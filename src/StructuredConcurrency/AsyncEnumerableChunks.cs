using System.Runtime.CompilerServices;

namespace StructuredConcurrency;

/// <summary>
/// Groups the items of an asynchronous stream into chunks by size and by time, like Rx's <c>Buffer(timeSpan, count)</c>
/// and Kafka's <c>batch.size</c> / <c>linger.ms</c>: for batch inserts, bulk API calls and log shipping.
/// </summary>
public static class AsyncEnumerableChunks
{
    /// <summary>
    /// Yields the items of <paramref name="source"/> in chunks of up to <paramref name="maxSize"/> items, and yields a
    /// smaller chunk when <paramref name="maxDelay"/> has passed since its first item arrived.
    /// </summary>
    /// <typeparam name="T">The type of the items.</typeparam>
    /// <param name="source">The items. The source receives the enumeration's cancellation.</param>
    /// <param name="maxSize">The most items in a chunk; at least 1.</param>
    /// <param name="maxDelay">
    /// How long the first item of a chunk may wait for the chunk to fill up: up to about 49 days, or
    /// <see cref="Timeout.InfiniteTimeSpan"/> to cut chunks by size only.
    /// </param>
    /// <returns>
    /// The chunks, never empty, with the items in source order; the last one holds whatever is left when the source
    /// ends. A failure of the source is thrown from the enumeration, and the items of the unfinished chunk are lost.
    /// When enumeration ends for any reason, the read in progress is cancelled and awaited and the source is disposed
    /// before it returns.
    /// </returns>
    /// <exception cref="ArgumentOutOfRangeException">
    /// <paramref name="maxSize"/> is less than 1, or <paramref name="maxDelay"/> is negative or too long.
    /// </exception>
    public static IAsyncEnumerable<T[]> ChunkAsync<T>(this IAsyncEnumerable<T> source, int maxSize, TimeSpan maxDelay) =>
        ChunkAsync(source, maxSize, maxDelay, TimeProvider.System);

    /// <summary>
    /// Yields the items of <paramref name="source"/> in chunks of up to <paramref name="maxSize"/> items, and yields a
    /// smaller chunk when <paramref name="maxDelay"/> has passed since its first item arrived, as measured by
    /// <paramref name="timeProvider"/>.
    /// </summary>
    /// <typeparam name="T">The type of the items.</typeparam>
    /// <param name="source">The items. The source receives the enumeration's cancellation.</param>
    /// <param name="maxSize">The most items in a chunk; at least 1.</param>
    /// <param name="maxDelay">
    /// How long the first item of a chunk may wait for the chunk to fill up: up to about 49 days, or
    /// <see cref="Timeout.InfiniteTimeSpan"/> to cut chunks by size only.
    /// </param>
    /// <param name="timeProvider">Measures the delay; pass a fake one in tests to make time pass on demand.</param>
    /// <returns>
    /// The chunks, never empty, with the items in source order; the last one holds whatever is left when the source
    /// ends. A failure of the source is thrown from the enumeration, and the items of the unfinished chunk are lost.
    /// When enumeration ends for any reason, the read in progress is cancelled and awaited and the source is disposed
    /// before it returns.
    /// </returns>
    /// <exception cref="ArgumentOutOfRangeException">
    /// <paramref name="maxSize"/> is less than 1, or <paramref name="maxDelay"/> is negative or too long.
    /// </exception>
    public static IAsyncEnumerable<T[]> ChunkAsync<T>(
        this IAsyncEnumerable<T> source,
        int maxSize,
        TimeSpan maxDelay,
        TimeProvider timeProvider)
    {
        ArgumentNullException.ThrowIfNull(source);
        ArgumentNullException.ThrowIfNull(timeProvider);
        ArgumentOutOfRangeException.ThrowIfLessThan(maxSize, 1);
        Timeouts.Validate(maxDelay, nameof(maxDelay));
        return ChunkCore(source, maxSize, maxDelay, timeProvider);
    }

    private static async IAsyncEnumerable<T[]> ChunkCore<T>(
        IAsyncEnumerable<T> source,
        int maxSize,
        TimeSpan maxDelay,
        TimeProvider timeProvider,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        IAsyncEnumerator<T> items = source.GetAsyncEnumerator(cts.Token);
        var chunk = new List<T>(maxSize);
        Task<bool>? moving = null;              // the source's MoveNextAsync while it is in flight
        CancellationTokenSource? timer = null;  // cancels the current chunk's deadline once the chunk is yielded
        Task? deadline = null;
        try
        {
            while (true)
            {
                bool more = false;
                if (moving is null)
                {
                    // Only allocate a task for a read that has to wait.
                    ValueTask<bool> read = items.MoveNextAsync();
                    if (read.IsCompleted)
                    {
                        more = read.Result;   // rethrows the source's failure, if any
                    }
                    else
                    {
                        moving = read.AsTask();
                    }
                }

                if (moving is not null)
                {
                    // A chunk that has started can't wait for the next item longer than its deadline.
                    if (deadline is not null && !moving.IsCompleted)
                    {
                        await Task.WhenAny(moving, deadline).ConfigureAwait(false);
                        cancellationToken.ThrowIfCancellationRequested();
                        if (!moving.IsCompleted)
                        {
                            // The read stays in flight while the caller handles the chunk.
                            yield return Take(chunk, ref timer, ref deadline);
                            continue;
                        }
                    }
                    more = await moving.ConfigureAwait(false);
                    moving = null;
                }

                if (!more)
                {
                    break;
                }

                chunk.Add(items.Current);
                if (chunk.Count == maxSize)
                {
                    yield return Take(chunk, ref timer, ref deadline);
                }
                else if (chunk.Count == 1 && maxDelay != Timeout.InfiniteTimeSpan)
                {
                    timer = CancellationTokenSource.CreateLinkedTokenSource(cts.Token);
                    deadline = Task.Delay(maxDelay, timeProvider, timer.Token);
                }
            }

            if (chunk.Count > 0)
            {
                yield return Take(chunk, ref timer, ref deadline);
            }
        }
        finally
        {
            cts.Cancel();
            timer?.Dispose();

            // An enumerator can't be disposed while a MoveNextAsync is in flight.
            if (moving is not null)
            {
                await ((Task)moving).ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing);
            }
            await items.DisposeAsync().ConfigureAwait(false);
        }
    }

    private static T[] Take<T>(List<T> chunk, ref CancellationTokenSource? timer, ref Task? deadline)
    {
        T[] items = chunk.ToArray();
        chunk.Clear();
        if (timer is not null)
        {
            timer.Cancel();
            timer.Dispose();
            timer = null;
            deadline = null;
        }
        return items;
    }
}
