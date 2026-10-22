using Microsoft.Extensions.Time.Testing;

namespace StructuredConcurrency.Tests;

// Operations that time out wait on the token, so the outcome never depends on how long the timer actually takes.
public class TimeoutTests
{
    private static readonly TimeSpan Short = TimeSpan.FromMilliseconds(20);
    private static readonly TimeSpan Long = TimeSpan.FromMinutes(5);

    [Fact]
    public async Task WithTimeout_ReturnsTheResult_WhenInTime()
    {
        int result = await TaskGroup.WithTimeoutAsync(Long, _ => Task.FromResult(42));

        Assert.Equal(42, result);
    }

    [Fact]
    public async Task WithTimeout_CancelsTheOperation_AndWaitsForItBeforeThrowing()
    {
        bool sawCancellation = false, ended = false;

        var exception = await Assert.ThrowsAsync<TimeoutException>(() => TaskGroup.WithTimeoutAsync(Short, async ct =>
        {
            try
            {
                await Task.Delay(Timeout.Infinite, ct);
            }
            catch (OperationCanceledException)
            {
                sawCancellation = true;
                await Task.Yield();   // cleanup that takes a moment
                ended = true;
                throw;
            }
        }));

        Assert.IsAssignableFrom<OperationCanceledException>(exception.InnerException);
        Assert.True(sawCancellation);
        Assert.True(ended);
    }

    [Fact]
    public async Task WithTimeout_CallerCancellation_IsNotATimeout()
    {
        using var cts = new CancellationTokenSource();
        Task run = TaskGroup.WithTimeoutAsync(Long, ct => Task.Delay(Timeout.Infinite, ct), cts.Token);
        cts.Cancel();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => run);
    }

    [Fact]
    public async Task WithTimeout_OperationsOwnCancellation_IsNotATimeout()
    {
        using var own = new CancellationTokenSource();
        own.Cancel();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() =>
            TaskGroup.WithTimeoutAsync(Long, _ => Task.FromCanceled<int>(own.Token)));
    }

    [Fact]
    public async Task WithTimeout_Failure_IsThrownAsIs()
    {
        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            TaskGroup.WithTimeoutAsync(Long, _ => Task.FromException<int>(new InvalidOperationException())));
    }

    [Fact]
    public async Task WithTimeoutOrDefault_ReturnsTheDefault_WhenTheTimeIsUp()
    {
        int result = await TaskGroup.WithTimeoutOrDefaultAsync(Short, async ct =>
        {
            await Task.Delay(Timeout.Infinite, ct);
            return 1;
        }, defaultValue: -1);

        Assert.Equal(-1, result);
    }

    [Fact]
    public async Task WithTimeoutOrDefault_NestedTimeout_IsNotMistakenForItsOwn()
    {
        await Assert.ThrowsAsync<TimeoutException>(() => TaskGroup.WithTimeoutOrDefaultAsync(
            Long,
            ct => TaskGroup.WithTimeoutAsync(Short, async inner =>
            {
                await Task.Delay(Timeout.Infinite, inner);
                return 1;
            }, ct),
            defaultValue: -1));
    }

    [Fact]
    public async Task TryWithTimeout_ReportsWhetherTheOperationCompleted()
    {
        Assert.True(await TaskGroup.TryWithTimeoutAsync(Long, _ => Task.CompletedTask));
        Assert.False(await TaskGroup.TryWithTimeoutAsync(Short, ct => Task.Delay(Timeout.Infinite, ct)));
    }

    [Fact]
    public void WithTimeout_RejectsTimeoutsItCannotWaitFor()
    {
        // Thrown by the call itself, not by the task it returns.
        Assert.Throws<ArgumentOutOfRangeException>(() => { _ = TaskGroup.WithTimeoutAsync(TimeSpan.FromSeconds(-1), _ => Task.CompletedTask); });
        Assert.Throws<ArgumentOutOfRangeException>(() => { _ = TaskGroup.WithTimeoutAsync(TimeSpan.MaxValue, _ => Task.CompletedTask); });
    }

    [Fact]
    public async Task WithTimeout_InfiniteTimeout_NeverTimesOut()
    {
        Assert.Equal(7, await TaskGroup.WithTimeoutAsync(Timeout.InfiniteTimeSpan, _ => Task.FromResult(7)));
    }

    // ----------------------------------------------------------------------------------------------------------------
    // TimeProvider: the timeout fires when the fake clock is advanced, not when real time passes.

    [Fact]
    public async Task TimeProvider_TimesOutOnlyWhenItsTimePasses()
    {
        var time = new FakeTimeProvider();
        var started = new TaskCompletionSource();

        Task<int> run = TaskGroup.WithTimeoutAsync(TimeSpan.FromHours(1), time, async ct =>
        {
            started.SetResult();
            await Task.Delay(Timeout.Infinite, ct);
            return 1;
        });
        await started.Task;

        time.Advance(TimeSpan.FromMinutes(59));
        await Task.Delay(50);
        Assert.False(run.IsCompleted);

        time.Advance(TimeSpan.FromMinutes(1));
        await Assert.ThrowsAsync<TimeoutException>(() => run.WaitAsync(TimeSpan.FromSeconds(10)));
    }

    [Fact]
    public async Task TimeProvider_WorksWithEveryVariant()
    {
        var time = new FakeTimeProvider();
        using var caller = new CancellationTokenSource();   // a cancellable caller token takes the linked path

        Task<int> orDefault = TaskGroup.WithTimeoutOrDefaultAsync(TimeSpan.FromSeconds(1), time, async ct =>
        {
            await Task.Delay(Timeout.Infinite, ct);
            return 1;
        }, defaultValue: -1, caller.Token);
        Task<bool> tried = TaskGroup.TryWithTimeoutAsync(TimeSpan.FromSeconds(1), time, ct => Task.Delay(Timeout.Infinite, ct));
        Task plain = TaskGroup.WithTimeoutAsync(TimeSpan.FromSeconds(1), time, ct => Task.Delay(Timeout.Infinite, ct));

        time.Advance(TimeSpan.FromSeconds(1));

        Assert.Equal(-1, await orDefault.WaitAsync(TimeSpan.FromSeconds(10)));
        Assert.False(await tried.WaitAsync(TimeSpan.FromSeconds(10)));
        await Assert.ThrowsAsync<TimeoutException>(() => plain.WaitAsync(TimeSpan.FromSeconds(10)));
    }
}
