using System.Threading.RateLimiting;

namespace StructuredConcurrency.Tests;

// group.TryRun: start a child only if the group's limits let it start now, like Go's errgroup.TryGo.
public class TryRunTests
{
    private static readonly TimeSpan Guard = TimeSpan.FromSeconds(10);

    private sealed class Boom() : Exception("boom");

    [Fact]
    public async Task WithoutLimits_StartsTheChild()
    {
        int result = await TaskGroup.RunAsync(async group =>
        {
            Assert.True(group.TryRun(_ => Task.FromResult(42), out Task<int>? child));
            Assert.True(group.TryRun(_ => Task.CompletedTask, out Task? other));
            await other;
            return await child;
        });

        Assert.Equal(42, result);
    }

    [Fact]
    public async Task AtMaxConcurrency_ReturnsFalse_WithoutInvokingTheWork_UntilASlotFrees()
    {
        var gate = new TaskCompletionSource();
        bool invoked = false;

        await TaskGroup.RunAsync(async group =>
        {
            Assert.True(group.TryRun(_ => gate.Task, out Task? running));

            Assert.False(group.TryRun(_ => { invoked = true; return Task.CompletedTask; }, out Task? rejected));
            Assert.Null(rejected);

            gate.SetResult();
            await running.WaitAsync(Guard);
            Assert.True(group.TryRun(_ => Task.CompletedTask, out _));
        }, new TaskGroupOptions { MaxConcurrency = 1 }).WaitAsync(Guard);

        Assert.False(invoked);
    }

    [Fact]
    public async Task ASlotTakenByTryRun_IsHandedToQueuedWork_WhenTheChildCompletes()
    {
        var gate = new TaskCompletionSource();
        bool queuedRan = false;

        await TaskGroup.RunAsync(group =>
        {
            Assert.True(group.TryRun(_ => gate.Task, out _));
            group.Run(_ => { queuedRan = true; return Task.CompletedTask; });   // queued behind the TryRun child
            Assert.False(queuedRan);
            gate.SetResult();
            return Task.CompletedTask;
        }, new TaskGroupOptions { MaxConcurrency = 1 }).WaitAsync(Guard);

        Assert.True(queuedRan);
    }

    [Fact]
    public async Task WithoutAPermit_ReturnsFalse_AndGivesTheSlotBack()
    {
        using var limiter = new TokenBucketRateLimiter(new TokenBucketRateLimiterOptions
        {
            TokenLimit = 1,
            TokensPerPeriod = 1,
            ReplenishmentPeriod = TimeSpan.FromTicks(1),
            AutoReplenishment = false,
            QueueLimit = 0,
        });

        await TaskGroup.RunAsync(async group =>
        {
            Assert.True(group.TryRun(_ => Task.CompletedTask, out Task? first));
            await first;
            Assert.False(group.TryRun(_ => Task.CompletedTask, out _));   // no token left

            // The failed attempt didn't keep the only slot.
            limiter.TryReplenish();
            Assert.True(group.TryRun(_ => Task.CompletedTask, out _));
        }, new TaskGroupOptions { MaxConcurrency = 1, RateLimiter = limiter }).WaitAsync(Guard);
    }

    [Fact]
    public async Task ThePermit_IsHeldUntilTheChildCompletes()
    {
        using var limiter = new ConcurrencyLimiter(new ConcurrencyLimiterOptions { PermitLimit = 1, QueueLimit = 0 });
        var gate = new TaskCompletionSource();

        await TaskGroup.RunAsync(async group =>
        {
            Assert.True(group.TryRun(_ => gate.Task, out Task? running));
            Assert.False(group.TryRun(_ => Task.CompletedTask, out _));

            gate.SetResult();
            await running.WaitAsync(Guard);
            Assert.True(group.TryRun(_ => Task.CompletedTask, out _));
        }, new TaskGroupOptions { RateLimiter = limiter }).WaitAsync(Guard);
    }

    [Fact]
    public async Task OnACancelledGroup_ReturnsACancelledTask_WithoutInvokingTheWork()
    {
        bool invoked = false;
        await TaskGroup.RunAsync(group =>
        {
            group.Cancel();
            Assert.True(group.TryRun(_ => { invoked = true; return Task.CompletedTask; }, out Task? child));
            Assert.True(child.IsCanceled);
            return Task.CompletedTask;
        }, new TaskGroupOptions { MaxConcurrency = 1 });

        Assert.False(invoked);
    }

    [Fact]
    public async Task AFailure_CancelsTheGroup_AsWithRun()
    {
        CancellationToken sibling = default;
        await Assert.ThrowsAsync<Boom>(() => TaskGroup.RunAsync(group =>
        {
            group.Run(ct => { sibling = ct; return Task.Delay(Timeout.Infinite, ct); });
            Assert.True(group.TryRun(_ => Task.FromException(new Boom()), out _));
            return Task.CompletedTask;
        }, new TaskGroupOptions { MaxConcurrency = 4 }).WaitAsync(Guard));

        Assert.True(sibling.IsCancellationRequested);
    }

    [Fact]
    public async Task OnACompletedGroup_Throws()
    {
        TaskGroup? done = null;
        await TaskGroup.RunAsync(group =>
        {
            done = group;
            return Task.CompletedTask;
        });

        Assert.Throws<InvalidOperationException>(() => done!.TryRun(_ => Task.CompletedTask, out _));
    }
}
