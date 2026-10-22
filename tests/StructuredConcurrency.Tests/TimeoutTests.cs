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
}
