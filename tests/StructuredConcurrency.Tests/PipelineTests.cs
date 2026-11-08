using System.Collections.Concurrent;
using System.Runtime.CompilerServices;

namespace StructuredConcurrency.Tests;

// SelectConcurrentAsync with onFailure, and BufferAsync: the pieces that make chained stages a pipeline.
public class PipelineTests
{
    private static readonly TimeSpan Guard = TimeSpan.FromSeconds(10);

    private sealed class Boom(int id) : Exception($"boom {id}")
    {
        public int Id { get; } = id;
    }

    private static async IAsyncEnumerable<int> Numbers(int count, [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        for (int i = 1; i <= count; i++)
        {
            await Task.Yield();
            cancellationToken.ThrowIfCancellationRequested();
            yield return i;
        }
    }

    // ----------------------------------------------------------------------------------------------------------------
    // onFailure

    [Fact]
    public async Task OnFailure_SkipsAndReportsFailedItems_AndKeepsGoing()
    {
        var failed = new ConcurrentBag<(int Item, Exception Error)>();
        Func<int, CancellationToken, Task<int>> selector = async (i, _) =>
        {
            await Task.Yield();
            return i % 3 == 0 ? throw new Boom(i) : i * 10;
        };

        List<int> fromList = await Enumerable.Range(1, 7).SelectConcurrentAsync(selector, 3, (i, e) => failed.Add((i, e))).ToListAsync();
        List<int> fromStream = await Numbers(7).SelectConcurrentAsync(selector, 3, (i, e) => failed.Add((i, e))).ToListAsync();

        Assert.Equal([10, 20, 40, 50, 70], fromList);   // input order is kept around the gaps
        Assert.Equal([10, 20, 40, 50, 70], fromStream);
        Assert.Equal([3, 3, 6, 6], failed.Select(f => f.Item).Order());
        Assert.All(failed, f => Assert.Equal(f.Item, ((Boom)f.Error).Id));
    }

    [Fact]
    public async Task OnFailure_ReportsASelectorThatThrowsBeforeReturningATask()
    {
        var failed = new List<int>();
        List<int> results = await new[] { 1, 2, 3 }
            .SelectConcurrentAsync((i, _) => i == 2 ? throw new Boom(i) : Task.FromResult(i), 1, (i, _) => failed.Add(i))
            .ToListAsync();

        Assert.Equal([1, 3], results);
        Assert.Equal([2], failed);
    }

    [Fact]
    public async Task OnFailure_ThatThrows_EndsTheEnumeration()
    {
        IAsyncEnumerable<int> results = new[] { 1, 2, 3 }.SelectConcurrentAsync(
            (i, _) => i == 2 ? Task.FromException<int>(new Boom(i)) : Task.FromResult(i),
            1,
            (_, error) => throw new InvalidOperationException("stop", error));

        var thrown = await Assert.ThrowsAsync<InvalidOperationException>(() => results.ToListAsync());
        Assert.IsType<Boom>(thrown.InnerException);
    }

    [Fact]
    public async Task OnFailure_IsNotCalledForTheCancellationsThatEndingTheLoopCauses()
    {
        int reported = 0;
        await foreach (int result in new[] { 1, 2, 3, 4 }.SelectConcurrentAsync(
            async (i, ct) =>
            {
                if (i > 1)
                {
                    await Task.Delay(Timeout.Infinite, ct);
                }
                return i;
            },
            4,
            (_, _) => Interlocked.Increment(ref reported)))
        {
            Assert.Equal(1, result);
            break;
        }

        Assert.Equal(0, reported);
    }

    [Fact]
    public void OnFailure_Null_IsRejected()
    {
        Assert.Throws<ArgumentNullException>(() => new[] { 1 }.SelectConcurrentAsync((i, _) => Task.FromResult(i), 1, onFailure: null!));
    }

    // ----------------------------------------------------------------------------------------------------------------
    // BufferAsync

    [Fact]
    public async Task Buffer_ReadsAheadOfTheConsumer_UpToItsCapacity()
    {
        int read = 0;
        async IAsyncEnumerable<int> Counted([EnumeratorCancellation] CancellationToken ct = default)
        {
            for (int i = 1; i <= 100; i++)
            {
                await Task.Yield();
                Interlocked.Increment(ref read);
                yield return i;
            }
        }

        await using IAsyncEnumerator<int> items = Counted().BufferAsync(capacity: 5).GetAsyncEnumerator();
        Assert.True(await items.MoveNextAsync());
        Assert.Equal(1, items.Current);

        // While the consumer holds item 1, the buffer fills: 5 waiting, plus 1 read and waiting to be written.
        Assert.True(SpinWait.SpinUntil(() => Volatile.Read(ref read) == 7, Guard));
        await Task.Delay(20);
        Assert.Equal(7, Volatile.Read(ref read));
    }

    [Fact]
    public async Task Buffer_KeepsTheOrder_AndThrowsTheSourcesFailureAfterTheItemsBeforeIt()
    {
        async IAsyncEnumerable<int> Failing()
        {
            yield return 1;
            await Task.Yield();
            yield return 2;
            throw new Boom(3);
        }

        var seen = new List<int>();
        await Assert.ThrowsAsync<Boom>(async () =>
        {
            await foreach (int item in Failing().BufferAsync(capacity: 10))
            {
                seen.Add(item);
            }
        });
        Assert.Equal([1, 2], seen);
    }

    [Fact]
    public async Task Buffer_BreakOrCancellation_StopsAndDisposesTheSource()
    {
        bool disposed = false;
        async IAsyncEnumerable<int> Endless([EnumeratorCancellation] CancellationToken ct = default)
        {
            try
            {
                for (int i = 0; ; i++)
                {
                    await Task.Delay(1, ct);
                    yield return i;
                }
            }
            finally
            {
                disposed = true;
            }
        }

        await foreach (int _ in Endless().BufferAsync(capacity: 3))
        {
            break;
        }
        Assert.True(disposed);

        disposed = false;
        using var cts = new CancellationTokenSource();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
        {
            await foreach (int item in Endless().BufferAsync(capacity: 3).WithCancellation(cts.Token))
            {
                if (item == 2)
                {
                    cts.Cancel();
                }
            }
        });
        Assert.True(disposed);
    }

    [Fact]
    public void Buffer_CapacityBelowOne_IsRejected()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => Numbers(1).BufferAsync(0));
    }

    // ----------------------------------------------------------------------------------------------------------------
    // A whole pipeline

    [Fact]
    public async Task Pipeline_StagesBufferAndChunks_ComposeIntoOneStream()
    {
        var deadLetters = new ConcurrentBag<int>();
        var batches = new List<int[]>();

        await foreach (int[] batch in Numbers(20)
            .SelectConcurrentAsync(async (i, ct) => { await Task.Delay(1, ct); return i; }, maxConcurrency: 4)
            .BufferAsync(capacity: 8)
            .SelectConcurrentAsync(
                (i, _) => i % 7 == 0 ? Task.FromException<int>(new Boom(i)) : Task.FromResult(i * 2),
                maxConcurrency: 2,
                onFailure: (i, _) => deadLetters.Add(i))
            .ChunkAsync(maxSize: 5, maxDelay: TimeSpan.FromSeconds(5))
            .WithCancellation(CancellationToken.None))
        {
            batches.Add(batch);
        }

        int[] expected = Enumerable.Range(1, 20).Where(i => i % 7 != 0).Select(i => i * 2).ToArray();
        Assert.Equal(expected, batches.SelectMany(b => b));
        Assert.All(batches.SkipLast(1), b => Assert.Equal(5, b.Length));
        Assert.Equal([7, 14], deadLetters.Order());
    }
}
