using Microsoft.Extensions.Time.Testing;

namespace StructuredConcurrency.Tests;

// Attempts wait on gates the test opens, and the delay is measured by a FakeTimeProvider, so no test depends on timing.
public class HedgeTests
{
    private static readonly TimeSpan Delay = TimeSpan.FromSeconds(1);
    private static readonly TimeSpan Wait = TimeSpan.FromSeconds(10);

    private sealed class Boom(int id) : Exception($"boom {id}")
    {
        public int Id { get; } = id;
    }

    // One gate per attempt: Started completes when the attempt begins, and the attempt returns what Result is set to.
    private sealed class Attempts(int count)
    {
        public readonly TaskCompletionSource[] Started = Make(count, () => new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously));
        public readonly TaskCompletionSource<int>[] Result = Make(count, () => new TaskCompletionSource<int>(TaskCreationOptions.RunContinuationsAsynchronously));
        public readonly bool[] Cancelled = new bool[count];
        public int Count;

        public async Task<int> RunAsync(int attempt, CancellationToken ct)
        {
            Interlocked.Increment(ref Count);
            Started[attempt].SetResult();
            using (ct.Register(() => Cancelled[attempt] = Result[attempt].TrySetCanceled(ct)))
            {
                return await Result[attempt].Task;
            }
        }

        private static T[] Make<T>(int count, Func<T> create) => Enumerable.Range(0, count).Select(_ => create()).ToArray();
    }

    [Fact]
    public async Task FastFirstAttempt_IsTheOnlyOne()
    {
        var time = new FakeTimeProvider();
        var attempts = new Attempts(3);
        attempts.Result[0].SetResult(7);

        int result = await TaskGroup.HedgeAsync(Delay, time, attempts.RunAsync, maxAttempts: 3);

        Assert.Equal(7, result);
        Assert.Equal(1, attempts.Count);
    }

    [Fact]
    public async Task SlowAttempt_StartsAnotherAfterTheDelay_AndTheLoserIsCancelledBeforeReturning()
    {
        var time = new FakeTimeProvider();
        var attempts = new Attempts(3);

        Task<int> hedge = TaskGroup.HedgeAsync(Delay, time, attempts.RunAsync, maxAttempts: 3);
        await attempts.Started[0].Task.WaitAsync(Wait);
        Assert.False(attempts.Started[1].Task.IsCompleted);

        time.Advance(Delay);
        await attempts.Started[1].Task.WaitAsync(Wait);
        attempts.Result[1].SetResult(2);

        Assert.Equal(2, await hedge);
        Assert.True(attempts.Cancelled[0]);
        Assert.Equal(2, attempts.Count);
    }

    [Fact]
    public async Task FailedAttempt_StartsTheNextOneWithoutWaitingForTheDelay()
    {
        var time = new FakeTimeProvider();
        var attempts = new Attempts(2);

        Task<int> hedge = TaskGroup.HedgeAsync(Delay, time, attempts.RunAsync, maxAttempts: 2);
        attempts.Result[0].SetException(new Boom(0));

        await attempts.Started[1].Task.WaitAsync(Wait);   // the fake time never moved
        attempts.Result[1].SetResult(5);
        Assert.Equal(5, await hedge);
    }

    [Fact]
    public async Task FailureWhileAnotherAttemptRuns_WaitsForTheDelay()
    {
        var time = new FakeTimeProvider();
        var attempts = new Attempts(3);

        Task<int> hedge = TaskGroup.HedgeAsync(Delay, time, attempts.RunAsync, maxAttempts: 3);
        time.Advance(Delay);
        await attempts.Started[1].Task.WaitAsync(Wait);

        // The first attempt fails, but the second is still running: no reason to start a third yet.
        attempts.Result[0].SetException(new Boom(0));
        await Task.Delay(50);
        Assert.False(attempts.Started[2].Task.IsCompleted);

        attempts.Result[1].SetResult(1);
        Assert.Equal(1, await hedge);
    }

    [Fact]
    public async Task EveryAttemptFailing_ThrowsTheFailuresInAttemptOrder()
    {
        var exception = await Assert.ThrowsAsync<AggregateException>(() =>
            TaskGroup.HedgeAsync<int>(Delay, (attempt, _) => Task.FromException<int>(new Boom(attempt)), maxAttempts: 3));

        Assert.Equal(new[] { 0, 1, 2 }, exception.InnerExceptions.Select(inner => ((Boom)inner).Id));
    }

    [Fact]
    public async Task MaxAttempts_IsNeverExceeded()
    {
        var time = new FakeTimeProvider();
        var attempts = new Attempts(2);

        Task<int> hedge = TaskGroup.HedgeAsync(Delay, time, attempts.RunAsync, maxAttempts: 2);
        time.Advance(Delay);
        await attempts.Started[1].Task.WaitAsync(Wait);
        time.Advance(Delay * 10);

        attempts.Result[0].SetResult(3);
        Assert.Equal(3, await hedge);
        Assert.Equal(2, attempts.Count);
    }

    [Fact]
    public async Task InfiniteDelay_StartsTheNextAttemptOnlyOnFailure()
    {
        var attempts = new Attempts(2);

        Task<int> hedge = TaskGroup.HedgeAsync(Timeout.InfiniteTimeSpan, attempts.RunAsync, maxAttempts: 2);
        await attempts.Started[0].Task.WaitAsync(Wait);
        Assert.False(attempts.Started[1].Task.IsCompleted);

        attempts.Result[0].SetException(new Boom(0));
        await attempts.Started[1].Task.WaitAsync(Wait);
        attempts.Result[1].SetResult(9);
        Assert.Equal(9, await hedge);
    }

    [Fact]
    public async Task CallerCancellation_CancelsEveryAttempt()
    {
        var time = new FakeTimeProvider();
        var attempts = new Attempts(2);
        using var cts = new CancellationTokenSource();

        Task<int> hedge = TaskGroup.HedgeAsync(Delay, time, attempts.RunAsync, maxAttempts: 2, cts.Token);
        time.Advance(Delay);
        await attempts.Started[1].Task.WaitAsync(Wait);
        cts.Cancel();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => hedge);
        Assert.True(attempts.Cancelled[0]);
        Assert.True(attempts.Cancelled[1]);
    }

    [Fact]
    public async Task TokenOnlyOverload_RunsTheOperation()
    {
        int result = await TaskGroup.HedgeAsync(Delay, _ => Task.FromResult(4), maxAttempts: 2);

        Assert.Equal(4, result);
    }

    [Fact]
    public void InvalidArguments_Throw()
    {
        // Thrown by the call itself, not by the task it returns.
        Assert.Throws<ArgumentOutOfRangeException>(() => { _ = TaskGroup.HedgeAsync(Delay, _ => Task.FromResult(1), maxAttempts: 0); });
        Assert.Throws<ArgumentOutOfRangeException>(() => { _ = TaskGroup.HedgeAsync(TimeSpan.FromSeconds(-2), _ => Task.FromResult(1), maxAttempts: 2); });
        Assert.Throws<ArgumentNullException>(() => { _ = TaskGroup.HedgeAsync(Delay, (Func<CancellationToken, Task<int>>)null!, maxAttempts: 2); });
    }
}
