using System.Threading.RateLimiting;

namespace StructuredConcurrency.Tests;

// The token buckets don't replenish on their own; each test calls TryReplenish, so no test depends on timing. Their
// period is one tick because TryReplenish adds nothing until a period has passed.
public class RateLimiterTests
{
    private static readonly TimeSpan Wait = TimeSpan.FromSeconds(10);

    private static TokenBucketRateLimiter Bucket(int tokens, int queueLimit) => new(new TokenBucketRateLimiterOptions
    {
        TokenLimit = tokens,
        TokensPerPeriod = tokens,
        ReplenishmentPeriod = TimeSpan.FromTicks(1),
        AutoReplenishment = false,
        QueueLimit = queueLimit,
        QueueProcessingOrder = QueueProcessingOrder.OldestFirst,
    });

    [Fact]
    public async Task Children_WaitForAPermit_BeforeTheyStart()
    {
        using TokenBucketRateLimiter limiter = Bucket(tokens: 2, queueLimit: 10);
        int started = 0;

        Task run = TaskGroup.RunAsync(group =>
        {
            for (int i = 0; i < 5; i++)
            {
                group.Run(_ =>
                {
                    Interlocked.Increment(ref started);
                    return Task.CompletedTask;
                });
            }
            return Task.CompletedTask;
        }, new TaskGroupOptions { RateLimiter = limiter });

        Assert.Equal(2, Volatile.Read(ref started));   // the tokens there were
        limiter.TryReplenish();
        await Until(() => Volatile.Read(ref started) == 4);
        Assert.False(run.IsCompleted);
        limiter.TryReplenish();

        await run.WaitAsync(Wait);
        Assert.Equal(5, started);
    }

    [Fact]
    public async Task RefusedChild_FailsWithRateLimitRejected_AndCancelsTheGroup()
    {
        using TokenBucketRateLimiter limiter = Bucket(tokens: 1, queueLimit: 0);
        bool siblingCancelled = false;

        var exception = await Assert.ThrowsAsync<RateLimitRejectedException>(() => TaskGroup.RunAsync(group =>
        {
            group.Run(async ct =>
            {
                try
                {
                    await Task.Delay(Timeout.Infinite, ct);
                }
                catch (OperationCanceledException)
                {
                    siblingCancelled = true;
                    throw;
                }
            });
            group.Run(_ => Task.CompletedTask);   // no token left and no room to queue
            return Task.CompletedTask;
        }, new TaskGroupOptions { RateLimiter = limiter }).WaitAsync(Wait));

        Assert.True(siblingCancelled);
        Assert.NotNull(exception.RetryAfter);
    }

    [Fact]
    public async Task Permit_IsGivenBack_WhenTheChildCompletes()
    {
        using var limiter = new ConcurrencyLimiter(new ConcurrencyLimiterOptions { PermitLimit = 1, QueueLimit = 10 });
        var gate = new TaskCompletionSource();
        int running = 0, peak = 0;

        Task run = TaskGroup.RunAsync(group =>
        {
            for (int i = 0; i < 3; i++)
            {
                group.Run(async _ =>
                {
                    int now = Interlocked.Increment(ref running);
                    InterlockedMax(ref peak, now);
                    await gate.Task;
                    Interlocked.Decrement(ref running);
                });
            }
            return Task.CompletedTask;
        }, new TaskGroupOptions { RateLimiter = limiter });

        gate.SetResult();
        await run.WaitAsync(Wait);
        Assert.Equal(1, peak);
        Assert.Equal(1, limiter.GetStatistics()!.CurrentAvailablePermits);
    }

    [Fact]
    public async Task WaitingForAPermit_EndsQuietly_WhenTheGroupIsCancelled()
    {
        using TokenBucketRateLimiter limiter = Bucket(tokens: 1, queueLimit: 10);
        bool lateChildRan = false;

        await TaskGroup.RunAsync(group =>
        {
            group.Run(_ =>
            {
                group.Cancel();
                return Task.CompletedTask;
            });
            group.Run(_ =>
            {
                lateChildRan = true;   // waits for a token that never comes
                return Task.CompletedTask;
            });
            return Task.CompletedTask;
        }, new TaskGroupOptions { RateLimiter = limiter }).WaitAsync(Wait);

        Assert.False(lateChildRan);
    }

    [Fact]
    public async Task TypedChild_ReturnsItsResult_ThroughTheLimiter()
    {
        using TokenBucketRateLimiter limiter = Bucket(tokens: 1, queueLimit: 1);

        int result = await TaskGroup.RunAsync(group => group.Run(_ => Task.FromResult(42)),
            new TaskGroupOptions { RateLimiter = limiter }).WaitAsync(Wait);

        Assert.Equal(42, result);
    }

    // Queued permits are granted on the limiter's continuations, which may run a moment after TryReplenish returns.
    private static async Task Until(Func<bool> condition)
    {
        using var timeout = new CancellationTokenSource(Wait);
        while (!condition())
        {
            await Task.Delay(1, timeout.Token);
        }
    }

    private static void InterlockedMax(ref int location, int value)
    {
        int current = Volatile.Read(ref location);
        while (value > current)
        {
            int seen = Interlocked.CompareExchange(ref location, value, current);
            if (seen == current)
            {
                return;
            }
            current = seen;
        }
    }
}
