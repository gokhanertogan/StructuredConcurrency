namespace StructuredConcurrency.Tests;

// Operations wait on gates the test opens in a chosen order; no test depends on timing.
public class KeyedSequencerTests
{
    private static readonly TimeSpan Guard = TimeSpan.FromSeconds(10);

    private static TaskCompletionSource Gate() => new(TaskCreationOptions.RunContinuationsAsynchronously);

    [Fact]
    public async Task SameKey_RunsOneAtATime_InCallOrder()
    {
        var sequencer = new KeyedSequencer<string>();
        var log = new List<string>();
        var first = Gate();

        Task a = sequencer.RunAsync("k", async _ =>
        {
            log.Add("a start");
            await first.Task;
            log.Add("a end");
        });
        Task b = sequencer.RunAsync("k", _ =>
        {
            log.Add("b");
            return Task.CompletedTask;
        });
        Task c = sequencer.RunAsync("k", _ =>
        {
            log.Add("c");
            return Task.CompletedTask;
        });

        Assert.False(b.IsCompleted);
        first.SetResult();
        await Task.WhenAll(a, b, c).WaitAsync(Guard);

        Assert.Equal(new[] { "a start", "a end", "b", "c" }, log);
    }

    [Fact]
    public async Task DifferentKeys_RunConcurrently()
    {
        var sequencer = new KeyedSequencer<int>();
        var gate = Gate();
        var bothStarted = new CountdownEvent(2);

        Task a = sequencer.RunAsync(1, async _ =>
        {
            bothStarted.Signal();
            await gate.Task;
        });
        Task b = sequencer.RunAsync(2, async _ =>
        {
            bothStarted.Signal();
            await gate.Task;
        });

        Assert.True(bothStarted.Wait(Guard));
        gate.SetResult();
        await Task.WhenAll(a, b).WaitAsync(Guard);
    }

    [Fact]
    public async Task Result_AndFailure_GoToTheirOwnCaller()
    {
        var sequencer = new KeyedSequencer<string>();

        Task<int> failing = sequencer.RunAsync<int>("k", _ => throw new InvalidOperationException());
        Task<int> next = sequencer.RunAsync("k", _ => Task.FromResult(2));

        await Assert.ThrowsAsync<InvalidOperationException>(() => failing.WaitAsync(Guard));
        Assert.Equal(2, await next.WaitAsync(Guard));
    }

    [Fact]
    public async Task CallerGivingUpWhileWaiting_SkipsItsOperation_ButDoesNotLetTheNextOneOvertake()
    {
        var sequencer = new KeyedSequencer<string>();
        var first = Gate();
        bool firstRunning = false, skippedRan = false, overlapped = false;
        using var cts = new CancellationTokenSource();

        Task a = sequencer.RunAsync("k", async _ =>
        {
            firstRunning = true;
            await first.Task;
            firstRunning = false;
        });
        Task skipped = sequencer.RunAsync("k", _ =>
        {
            skippedRan = true;
            return Task.CompletedTask;
        }, cts.Token);
        Task c = sequencer.RunAsync("k", _ =>
        {
            overlapped = firstRunning;
            return Task.CompletedTask;
        });

        cts.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => skipped.WaitAsync(Guard));
        Assert.False(c.IsCompleted);

        first.SetResult();
        await Task.WhenAll(a, c).WaitAsync(Guard);
        Assert.False(skippedRan);
        Assert.False(overlapped);
    }

    [Fact]
    public async Task CallerCancellation_ReachesTheRunningOperation_AndTheCallWaitsForIt()
    {
        var sequencer = new KeyedSequencer<string>();
        using var cts = new CancellationTokenSource();
        var started = Gate();
        bool ended = false;

        Task run = sequencer.RunAsync("k", async ct =>
        {
            started.SetResult();
            try
            {
                await Task.Delay(Timeout.Infinite, ct);
            }
            finally
            {
                await Task.Yield();
                ended = true;
            }
        }, cts.Token);

        await started.Task.WaitAsync(Guard);
        cts.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => run.WaitAsync(Guard));
        Assert.True(ended);
    }

    [Fact]
    public async Task AlreadyCancelledToken_NeverRunsTheOperation()
    {
        var sequencer = new KeyedSequencer<string>();
        bool ran = false;

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => sequencer.RunAsync("k", _ =>
        {
            ran = true;
            return Task.CompletedTask;
        }, new CancellationToken(true)));

        Assert.False(ran);
        await sequencer.RunAsync("k", _ => Task.CompletedTask).WaitAsync(Guard);   // the key isn't left blocked
    }

    [Fact]
    public async Task Comparer_DecidesWhichKeysAreTheSame()
    {
        var sequencer = new KeyedSequencer<string>(StringComparer.OrdinalIgnoreCase);
        var gate = Gate();

        Task a = sequencer.RunAsync("Account", _ => gate.Task);
        Task b = sequencer.RunAsync("ACCOUNT", _ => Task.CompletedTask);

        Assert.False(b.IsCompleted);
        gate.SetResult();
        await Task.WhenAll(a, b).WaitAsync(Guard);
    }
}
