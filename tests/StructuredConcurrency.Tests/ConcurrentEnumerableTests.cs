using System.Runtime.CompilerServices;
using System.Threading.Channels;

namespace StructuredConcurrency.Tests;

public class ConcurrentEnumerableTests
{
    private sealed class Boom() : Exception("boom");

    // A bug that makes the enumeration wait forever fails the test after this long instead of hanging the run.
    private static readonly TimeSpan Guard = TimeSpan.FromSeconds(10);

    private static TaskCompletionSource<int>[] Gates(int count) =>
        Enumerable.Range(0, count).Select(_ => new TaskCompletionSource<int>()).ToArray();

    [Fact]
    public async Task PreserveOrder_YieldsInInputOrder_WhenItemsCompleteOutOfOrder()
    {
        var gates = Gates(2);
        await using var results = Enumerable.Range(0, 2)
            .SelectConcurrentAsync((i, _) => gates[i].Task, maxConcurrency: 2)
            .GetAsyncEnumerator();

        Task<bool> move = results.MoveNextAsync().AsTask();
        gates[1].SetResult(1);
        await Task.Delay(50);
        Assert.False(move.IsCompleted);

        gates[0].SetResult(0);
        Assert.True(await move.WaitAsync(Guard));
        Assert.Equal(0, results.Current);
        Assert.True(await results.MoveNextAsync());
        Assert.Equal(1, results.Current);
        Assert.False(await results.MoveNextAsync());
    }

    [Fact]
    public async Task Unordered_YieldsInCompletionOrder()
    {
        var gates = Gates(2);
        await using var results = Enumerable.Range(0, 2)
            .SelectConcurrentAsync((i, _) => gates[i].Task, maxConcurrency: 2, preserveOrder: false)
            .GetAsyncEnumerator();

        Task<bool> move = results.MoveNextAsync().AsTask();
        gates[1].SetResult(1);

        Assert.True(await move.WaitAsync(Guard));
        Assert.Equal(1, results.Current);
        gates[0].SetResult(0);
        Assert.True(await results.MoveNextAsync());
        Assert.Equal(0, results.Current);
    }

    [Fact]
    public async Task Limit_IsNeverExceeded_AndEveryItemIsYielded()
    {
        int running = 0, maxRunning = 0;
        var results = new List<int>();

        await foreach (int result in Enumerable.Range(0, 20).SelectConcurrentAsync(async (i, _) =>
        {
            int now = Interlocked.Increment(ref running);
            InterlockedMax(ref maxRunning, now);
            await Task.Delay(1);
            Interlocked.Decrement(ref running);
            return i;
        }, maxConcurrency: 3))
        {
            results.Add(result);
        }

        Assert.Equal(Enumerable.Range(0, 20), results);
        Assert.InRange(maxRunning, 1, 3);
    }

    [Fact]
    public async Task Source_IsReadOnlyAsSlotsFreeUp()
    {
        int read = 0;
        IEnumerable<int> Source()
        {
            for (int i = 0; i < 10; i++)
            {
                read++;
                yield return i;
            }
        }

        var gate = new TaskCompletionSource<int>();
        await using var results = Source().SelectConcurrentAsync((_, _) => gate.Task, maxConcurrency: 2).GetAsyncEnumerator();
        Task<bool> move = results.MoveNextAsync().AsTask();

        Assert.Equal(2, read);
        gate.SetResult(0);
        await move.WaitAsync(Guard);
    }

    [Fact]
    public async Task Break_CancelsTheRunningItems_AndWaitsForThem()
    {
        int started = 0, ended = 0;

        await foreach (int result in Enumerable.Range(0, 5).SelectConcurrentAsync(async (i, ct) =>
        {
            Interlocked.Increment(ref started);
            try
            {
                if (i > 0)
                {
                    await Task.Delay(Timeout.Infinite, ct);
                }
                return i;
            }
            finally
            {
                Interlocked.Increment(ref ended);
            }
        }, maxConcurrency: 3))
        {
            break;
        }

        Assert.Equal(3, started);
        Assert.Equal(3, ended);
    }

    [Fact]
    public async Task Failure_IsThrownAtOnce_AndTheOthersAreCancelledAndAwaited()
    {
        bool firstEnded = false;

        await Assert.ThrowsAsync<Boom>(async () =>
        {
            await foreach (int _ in Enumerable.Range(0, 2).SelectConcurrentAsync(async (i, ct) =>
            {
                if (i == 1)
                {
                    throw new Boom();
                }
                try
                {
                    await Task.Delay(Timeout.Infinite, ct);   // the first item would hold back the failure if order waited for it
                    return i;
                }
                finally
                {
                    firstEnded = true;
                }
            }, maxConcurrency: 2))
            {
            }
        }).WaitAsync(Guard);

        Assert.True(firstEnded);
    }

    [Fact]
    public async Task Cancellation_CancelsTheItems_AndThrows()
    {
        using var cts = new CancellationTokenSource();
        int ended = 0;
        Task enumerate = Task.Run(async () =>
        {
            await foreach (int _ in Enumerable.Range(0, 3)
                .SelectConcurrentAsync(async (i, ct) =>
                {
                    try
                    {
                        await Task.Delay(Timeout.Infinite, ct);
                        return i;
                    }
                    finally
                    {
                        Interlocked.Increment(ref ended);
                    }
                }, maxConcurrency: 2)
                .WithCancellation(cts.Token))
            {
            }
        });

        cts.Cancel();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => enumerate.WaitAsync(Guard));
        Assert.True(ended <= 2);
    }

    [Fact]
    public async Task Cancellation_IsReportedWithTheCallersToken()
    {
        using var cts = new CancellationTokenSource();
        var started = new TaskCompletionSource();
        Task enumerate = Task.Run(async () =>
        {
            await foreach (int _ in new[] { 1 }
                .SelectConcurrentAsync(async (i, ct) =>
                {
                    started.SetResult();
                    await Task.Delay(Timeout.Infinite, ct);
                    return i;
                }, maxConcurrency: 1)
                .WithCancellation(cts.Token))
            {
            }
        });

        await started.Task.WaitAsync(Guard);
        cts.Cancel();

        var exception = await Assert.ThrowsAnyAsync<OperationCanceledException>(() => enumerate.WaitAsync(Guard));
        Assert.Equal(cts.Token, exception.CancellationToken);
    }

    [Fact]
    public void InvalidArguments_Throw()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => new[] { 1 }.SelectConcurrentAsync((i, _) => Task.FromResult(i), 0));
        Assert.Throws<ArgumentNullException>(() => new[] { 1 }.SelectConcurrentAsync<int, int>(null!, 1));
    }

    // ----------------------------------------------------------------------------------------------------------------
    // IAsyncEnumerable sources

    // Yields 0..count-1, each after its gate opens, and records what the enumeration did to it.
    private sealed class Source(int count)
    {
        public TaskCompletionSource[] Gates { get; } = Enumerable.Range(0, count).Select(_ => new TaskCompletionSource()).ToArray();

        private int _read;
        private int _disposed;
        private int _sawCancellation;

        public int Read => Volatile.Read(ref _read);

        public bool Disposed => Volatile.Read(ref _disposed) == 1;

        public bool SawCancellation => Volatile.Read(ref _sawCancellation) == 1;

        public async IAsyncEnumerable<int> Items([EnumeratorCancellation] CancellationToken ct = default)
        {
            try
            {
                for (int i = 0; i < count; i++)
                {
                    await Gates[i].Task.WaitAsync(ct);
                    Interlocked.Increment(ref _read);
                    yield return i;
                }
            }
            finally
            {
                Volatile.Write(ref _sawCancellation, ct.IsCancellationRequested ? 1 : 0);
                Volatile.Write(ref _disposed, 1);
            }
        }

        public void OpenAll()
        {
            foreach (TaskCompletionSource gate in Gates)
            {
                gate.TrySetResult();
            }
        }
    }

    [Fact]
    public async Task AsyncSource_YieldsEveryResult_InInputOrder()
    {
        var source = new Source(10);
        source.OpenAll();
        int running = 0, maxRunning = 0;

        List<int> results = await source.Items().SelectConcurrentAsync(async (i, _) =>
        {
            InterlockedMax(ref maxRunning, Interlocked.Increment(ref running));
            await Task.Delay(1);
            Interlocked.Decrement(ref running);
            return i * 10;
        }, maxConcurrency: 3).ToListAsync().WaitAsync(Guard);

        Assert.Equal(Enumerable.Range(0, 10).Select(i => i * 10), results);
        Assert.InRange(maxRunning, 1, 3);
        Assert.True(source.Disposed);
    }

    [Fact]
    public async Task AsyncSource_IsReadOnlyAsSlotsFreeUp()
    {
        var source = new Source(10);
        source.OpenAll();
        var gate = new TaskCompletionSource<int>();

        await using var results = source.Items().SelectConcurrentAsync((_, _) => gate.Task, maxConcurrency: 2).GetAsyncEnumerator();
        Task<bool> move = results.MoveNextAsync().AsTask();

        // The open gates let the source complete synchronously, so the reads have happened by the time
        // MoveNextAsync returns: two, one per slot.
        Assert.Equal(2, source.Read);
        gate.SetResult(0);
        await move.WaitAsync(Guard);
    }

    [Fact]
    public async Task AsyncSource_ResultsAreYielded_WhileTheSourceIsSlow()
    {
        var source = new Source(3);
        source.Gates[0].SetResult();   // item 0 arrives at once; item 1 never does until we say so

        await using var results = source.Items()
            .SelectConcurrentAsync((i, _) => Task.FromResult(i), maxConcurrency: 4, preserveOrder: false)
            .GetAsyncEnumerator();

        Assert.True(await results.MoveNextAsync().AsTask().WaitAsync(Guard));
        Assert.Equal(0, results.Current);

        source.Gates[1].SetResult();
        source.Gates[2].SetResult();
        Assert.True(await results.MoveNextAsync().AsTask().WaitAsync(Guard));
        Assert.True(await results.MoveNextAsync().AsTask().WaitAsync(Guard));
        Assert.False(await results.MoveNextAsync().AsTask().WaitAsync(Guard));
    }

    [Fact]
    public async Task AsyncSource_Unordered_YieldsInCompletionOrder()
    {
        var source = new Source(2);
        source.OpenAll();
        var gates = Gates(2);

        await using var results = source.Items()
            .SelectConcurrentAsync((i, _) => gates[i].Task, maxConcurrency: 2, preserveOrder: false)
            .GetAsyncEnumerator();
        Task<bool> move = results.MoveNextAsync().AsTask();
        gates[1].SetResult(1);

        Assert.True(await move.WaitAsync(Guard));
        Assert.Equal(1, results.Current);
        gates[0].SetResult(0);
        Assert.True(await results.MoveNextAsync().AsTask().WaitAsync(Guard));
        Assert.Equal(0, results.Current);
    }

    [Fact]
    public async Task AsyncSource_Break_CancelsTheSourceAndTheItems_AndDisposesTheSource()
    {
        var source = new Source(5);
        source.Gates[0].SetResult();
        source.Gates[1].SetResult();   // items 2.. never arrive: the source is waiting when we break
        int ended = 0;

        await foreach (int result in source.Items().SelectConcurrentAsync(async (i, ct) =>
        {
            try
            {
                if (i > 0)
                {
                    await Task.Delay(Timeout.Infinite, ct);
                }
                return i;
            }
            finally
            {
                Interlocked.Increment(ref ended);
            }
        }, maxConcurrency: 3).WithCancellation(default))
        {
            break;
        }

        Assert.Equal(2, ended);
        Assert.True(source.Disposed);
        Assert.True(source.SawCancellation);
    }

    [Fact]
    public async Task AsyncSource_SourceFailure_IsThrown_AndTheItemsAreCancelled()
    {
        bool itemEnded = false;

        async IAsyncEnumerable<int> Failing()
        {
            yield return 1;
            await Task.Delay(10);
            throw new Boom();
        }

        await Assert.ThrowsAsync<Boom>(async () =>
        {
            await foreach (int _ in Failing().SelectConcurrentAsync(async (i, ct) =>
            {
                try
                {
                    await Task.Delay(Timeout.Infinite, ct);
                    return i;
                }
                finally
                {
                    itemEnded = true;
                }
            }, maxConcurrency: 2))
            {
            }
        }).WaitAsync(Guard);

        Assert.True(itemEnded);
    }

    [Fact]
    public async Task AsyncSource_OperationFailure_IsThrown_AndTheSourceIsDisposed()
    {
        var source = new Source(5);
        source.OpenAll();

        await Assert.ThrowsAsync<Boom>(async () =>
        {
            await foreach (int _ in source.Items().SelectConcurrentAsync<int, int>((i, _) =>
                i == 2 ? Task.FromException<int>(new Boom()) : Task.FromResult(i), maxConcurrency: 2))
            {
            }
        }).WaitAsync(Guard);

        Assert.True(source.Disposed);
    }

    [Fact]
    public async Task AsyncSource_Cancellation_IsReportedWithTheCallersToken()
    {
        var source = new Source(5);   // no gate open: the enumeration waits on the source
        using var cts = new CancellationTokenSource();
        Task enumerate = Task.Run(async () =>
        {
            await foreach (int _ in source.Items()
                .SelectConcurrentAsync((i, _) => Task.FromResult(i), maxConcurrency: 2)
                .WithCancellation(cts.Token))
            {
            }
        });
        await Task.Delay(50);

        cts.Cancel();

        var exception = await Assert.ThrowsAnyAsync<OperationCanceledException>(() => enumerate.WaitAsync(Guard));
        Assert.Equal(cts.Token, exception.CancellationToken);
        Assert.True(source.Disposed);
    }

    [Fact]
    public async Task AsyncSource_WorksWithAChannel()
    {
        var channel = Channel.CreateUnbounded<int>();
        for (int i = 0; i < 5; i++)
        {
            channel.Writer.TryWrite(i);
        }
        channel.Writer.Complete();

        List<int> results = await channel.Reader.ReadAllAsync()
            .SelectConcurrentAsync((i, _) => Task.FromResult(i + 1), maxConcurrency: 2)
            .ToListAsync().WaitAsync(Guard);

        Assert.Equal([1, 2, 3, 4, 5], results);
    }

    [Fact]
    public void AsyncSource_InvalidArguments_Throw()
    {
        IAsyncEnumerable<int> source = new Source(1).Items();
        Assert.Throws<ArgumentOutOfRangeException>(() => source.SelectConcurrentAsync((i, _) => Task.FromResult(i), 0));
        Assert.Throws<ArgumentNullException>(() => source.SelectConcurrentAsync<int, int>(null!, 1));
        Assert.Throws<ArgumentNullException>(() => ((IAsyncEnumerable<int>)null!).SelectConcurrentAsync((i, _) => Task.FromResult(i), 1));
    }

    private static void InterlockedMax(ref int target, int value)
    {
        int current = Volatile.Read(ref target);
        while (value > current)
        {
            int seen = Interlocked.CompareExchange(ref target, value, current);
            if (seen == current)
            {
                return;
            }
            current = seen;
        }
    }
}
