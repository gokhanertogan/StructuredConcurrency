using System.Runtime.CompilerServices;
using System.Threading.Channels;
using Microsoft.Extensions.Time.Testing;

namespace StructuredConcurrency.Tests;

// Items come from a channel the test writes to, and the delay is measured by a FakeTimeProvider, so no test depends
// on timing.
public class ChunkTests
{
    private static readonly TimeSpan Guard = TimeSpan.FromSeconds(10);
    private static readonly TimeSpan Linger = TimeSpan.FromSeconds(1);

    private sealed class Boom() : Exception("boom");

    [Fact]
    public async Task ChunksBySize_AndYieldsTheRestAtTheEnd()
    {
        var channel = Channel.CreateUnbounded<int>();
        foreach (int item in Enumerable.Range(1, 5))
        {
            channel.Writer.TryWrite(item);
        }
        channel.Writer.Complete();

        var chunks = new List<int[]>();
        await foreach (int[] chunk in channel.Reader.ReadAllAsync().ChunkAsync(2, Timeout.InfiniteTimeSpan))
        {
            chunks.Add(chunk);
        }

        Assert.Equal(new[] { new[] { 1, 2 }, new[] { 3, 4 }, new[] { 5 } }, chunks);
    }

    [Fact]
    public async Task UnfinishedChunk_IsYieldedWhenItsFirstItemHasWaitedLongEnough()
    {
        var time = new FakeTimeProvider();
        var channel = Channel.CreateUnbounded<int>();
        await using IAsyncEnumerator<int[]> chunks = channel.Reader.ReadAllAsync().ChunkAsync(10, Linger, time).GetAsyncEnumerator();

        channel.Writer.TryWrite(1);
        ValueTask<bool> next = chunks.MoveNextAsync();
        time.Advance(Linger / 2);
        channel.Writer.TryWrite(2);   // doesn't restart the clock
        await Task.Delay(20);
        Assert.False(next.IsCompleted);

        time.Advance(Linger / 2);
        Assert.True(await next.AsTask().WaitAsync(Guard));
        Assert.Equal(new[] { 1, 2 }, chunks.Current);
    }

    [Fact]
    public async Task NoItems_NoChunk_HoweverLongItWaits()
    {
        var time = new FakeTimeProvider();
        var channel = Channel.CreateUnbounded<int>();
        await using IAsyncEnumerator<int[]> chunks = channel.Reader.ReadAllAsync().ChunkAsync(10, Linger, time).GetAsyncEnumerator();

        ValueTask<bool> next = chunks.MoveNextAsync();
        time.Advance(Linger * 10);
        await Task.Delay(20);
        Assert.False(next.IsCompleted);

        // The clock starts with the first item of a chunk.
        channel.Writer.TryWrite(7);
        await Task.Delay(20);
        Assert.False(next.IsCompleted);
        time.Advance(Linger);
        Assert.True(await next.AsTask().WaitAsync(Guard));
        Assert.Equal(new[] { 7 }, chunks.Current);
    }

    [Fact]
    public async Task FullChunk_DoesNotWaitForTheDelay_AndTheNextChunkGetsItsOwnClock()
    {
        var time = new FakeTimeProvider();
        var channel = Channel.CreateUnbounded<int>();
        await using IAsyncEnumerator<int[]> chunks = channel.Reader.ReadAllAsync().ChunkAsync(2, Linger, time).GetAsyncEnumerator();

        channel.Writer.TryWrite(1);
        channel.Writer.TryWrite(2);
        channel.Writer.TryWrite(3);
        Assert.True(await chunks.MoveNextAsync().AsTask().WaitAsync(Guard));
        Assert.Equal(new[] { 1, 2 }, chunks.Current);

        ValueTask<bool> next = chunks.MoveNextAsync();
        await Task.Delay(20);
        Assert.False(next.IsCompleted);
        time.Advance(Linger);
        Assert.True(await next.AsTask().WaitAsync(Guard));
        Assert.Equal(new[] { 3 }, chunks.Current);
    }

    [Fact]
    public async Task SourceFailure_IsThrown()
    {
        var channel = Channel.CreateUnbounded<int>();
        channel.Writer.TryWrite(1);
        channel.Writer.Complete(new Boom());

        await Assert.ThrowsAsync<Boom>(async () =>
        {
            await foreach (int[] _ in channel.Reader.ReadAllAsync().ChunkAsync(10, Linger))
            {
            }
        });
    }

    [Fact]
    public async Task Cancellation_StopsTheSource_AndTheEnumeration()
    {
        var source = new Source();
        using var cts = new CancellationTokenSource();
        await using IAsyncEnumerator<int[]> chunks = source.Items().ChunkAsync(10, Linger).GetAsyncEnumerator(cts.Token);

        ValueTask<bool> next = chunks.MoveNextAsync();
        await source.Reading.Task.WaitAsync(Guard);
        cts.Cancel();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => next.AsTask().WaitAsync(Guard));
        await chunks.DisposeAsync();
        Assert.True(source.Disposed);
    }

    [Fact]
    public async Task BreakingAfterAChunk_LeavesNothingRunning()
    {
        var source = new Source(first: 1);

        await foreach (int[] chunk in source.Items().ChunkAsync(1, Timeout.InfiniteTimeSpan))
        {
            Assert.Equal(new[] { 1 }, chunk);
            break;
        }

        Assert.True(source.Disposed);
    }

    [Fact]
    public void InvalidArguments_Throw()
    {
        IAsyncEnumerable<int> source = new Source().Items();
        Assert.Throws<ArgumentOutOfRangeException>(() => source.ChunkAsync(0, Linger));
        Assert.Throws<ArgumentOutOfRangeException>(() => source.ChunkAsync(1, TimeSpan.FromSeconds(-1)));
        Assert.Throws<ArgumentNullException>(() => ((IAsyncEnumerable<int>)null!).ChunkAsync(1, Linger));
    }

    // Yields `first` items, then waits for an item that never comes; records when it is disposed.
    private sealed class Source(int first = 0)
    {
        public TaskCompletionSource Reading { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public bool Disposed { get; private set; }

        public async IAsyncEnumerable<int> Items([EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            try
            {
                for (int i = 1; i <= first; i++)
                {
                    yield return i;
                }
                Reading.TrySetResult();
                await Task.Delay(Timeout.Infinite, cancellationToken);
                yield return -1;
            }
            finally
            {
                Disposed = true;
            }
        }
    }
}
