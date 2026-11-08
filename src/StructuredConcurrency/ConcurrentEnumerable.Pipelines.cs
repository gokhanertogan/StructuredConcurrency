using System.Runtime.CompilerServices;
using System.Threading.Channels;

namespace StructuredConcurrency;

// The pieces that turn chained SelectConcurrentAsync calls into a pipeline: per-item failure handling, so one bad item
// doesn't end the stream, and a buffer that lets a stage run ahead of the one after it.
public static partial class ConcurrentEnumerable
{
    /// <summary>
    /// Runs <paramref name="selector"/> for the items, at most <paramref name="maxConcurrency"/> at a time, and yields
    /// each result as soon as it can. An item whose operation fails is passed to <paramref name="onFailure"/> and
    /// skipped, instead of ending the enumeration.
    /// </summary>
    /// <typeparam name="TSource">The type of the items.</typeparam>
    /// <typeparam name="TResult">The type of the results.</typeparam>
    /// <param name="source">The items. They are read lazily, only as slots free up.</param>
    /// <param name="selector">The operation to run for each item.</param>
    /// <param name="maxConcurrency">The maximum number of operations that run at the same time; at least 1.</param>
    /// <param name="onFailure">
    /// Receives each item whose operation failed, and the exception, for example to log it or to put it on a
    /// dead-letter queue. It may be called from several operations at once. If it throws, that exception ends the
    /// enumeration. It isn't called for the cancellations that ending the enumeration causes.
    /// </param>
    /// <param name="preserveOrder">
    /// <see langword="true"/> to yield results in input order; <see langword="false"/> to yield them as they complete.
    /// </param>
    /// <returns>The results of the operations that succeeded. See <see cref="SelectConcurrentAsync{TSource, TResult}(IEnumerable{TSource}, Func{TSource, CancellationToken, Task{TResult}}, int, bool)"/>.</returns>
    public static IAsyncEnumerable<TResult> SelectConcurrentAsync<TSource, TResult>(
        this IEnumerable<TSource> source,
        Func<TSource, CancellationToken, Task<TResult>> selector,
        int maxConcurrency,
        Action<TSource, Exception> onFailure,
        bool preserveOrder = true)
    {
        ArgumentNullException.ThrowIfNull(source);
        ArgumentNullException.ThrowIfNull(selector);
        ArgumentNullException.ThrowIfNull(onFailure);
        ArgumentOutOfRangeException.ThrowIfLessThan(maxConcurrency, 1);
        return SkipFailed(SelectCore(source, Guarded(selector, onFailure), maxConcurrency, preserveOrder));
    }

    /// <summary>
    /// Runs <paramref name="selector"/> for the items of an asynchronous stream, at most
    /// <paramref name="maxConcurrency"/> at a time, and yields each result as soon as it can. An item whose operation
    /// fails is passed to <paramref name="onFailure"/> and skipped, instead of ending the enumeration.
    /// </summary>
    /// <typeparam name="TSource">The type of the items.</typeparam>
    /// <typeparam name="TResult">The type of the results.</typeparam>
    /// <param name="source">The items. They are read only as slots free up, and the source receives the enumeration's cancellation.</param>
    /// <param name="selector">The operation to run for each item.</param>
    /// <param name="maxConcurrency">The maximum number of operations that run at the same time; at least 1.</param>
    /// <param name="onFailure">
    /// Receives each item whose operation failed, and the exception, for example to log it or to put it on a
    /// dead-letter queue. It may be called from several operations at once. If it throws, that exception ends the
    /// enumeration. It isn't called for the cancellations that ending the enumeration causes, nor for a failure of the
    /// source itself, which still ends the enumeration.
    /// </param>
    /// <param name="preserveOrder">
    /// <see langword="true"/> to yield results in input order; <see langword="false"/> to yield them as they complete.
    /// </param>
    /// <returns>The results of the operations that succeeded. See <see cref="SelectConcurrentAsync{TSource, TResult}(IAsyncEnumerable{TSource}, Func{TSource, CancellationToken, Task{TResult}}, int, bool)"/>.</returns>
    public static IAsyncEnumerable<TResult> SelectConcurrentAsync<TSource, TResult>(
        this IAsyncEnumerable<TSource> source,
        Func<TSource, CancellationToken, Task<TResult>> selector,
        int maxConcurrency,
        Action<TSource, Exception> onFailure,
        bool preserveOrder = true)
    {
        ArgumentNullException.ThrowIfNull(source);
        ArgumentNullException.ThrowIfNull(selector);
        ArgumentNullException.ThrowIfNull(onFailure);
        ArgumentOutOfRangeException.ThrowIfLessThan(maxConcurrency, 1);
        return SkipFailed(SelectCore(source, Guarded(selector, onFailure), maxConcurrency, preserveOrder));
    }

    /// <summary>
    /// Reads <paramref name="source"/> ahead of the consumer into a buffer of up to <paramref name="capacity"/> items,
    /// so the stage before it keeps working while the stage after it is busy.
    /// </summary>
    /// <typeparam name="T">The type of the items.</typeparam>
    /// <param name="source">The items to read ahead.</param>
    /// <param name="capacity">How many items may wait in the buffer; at least 1. When it is full, reading pauses.</param>
    /// <returns>
    /// The same items, in the same order. Nothing is read until the sequence is enumerated. A failure of the source is
    /// thrown after the items read before it. When enumeration ends for any reason, the read in progress is cancelled
    /// and the source is disposed before it returns.
    /// </returns>
    public static IAsyncEnumerable<T> BufferAsync<T>(this IAsyncEnumerable<T> source, int capacity)
    {
        ArgumentNullException.ThrowIfNull(source);
        ArgumentOutOfRangeException.ThrowIfLessThan(capacity, 1);
        return BufferCore(source, capacity);
    }

    private static async IAsyncEnumerable<T> BufferCore<T>(
        IAsyncEnumerable<T> source,
        int capacity,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        Channel<T> buffer = Channel.CreateBounded<T>(new BoundedChannelOptions(capacity)
        {
            SingleReader = true,
            SingleWriter = true,
        });
        Task reading = FillAsync(source, buffer.Writer, cts.Token);
        try
        {
            // Throws the source's failure once the items before it have been read.
            while (await buffer.Reader.WaitToReadAsync(cancellationToken).ConfigureAwait(false))
            {
                while (buffer.Reader.TryRead(out T? item))
                {
                    yield return item;
                }
            }
        }
        finally
        {
            cts.Cancel();
            await reading.ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing);
        }
    }

    private static async Task FillAsync<T>(IAsyncEnumerable<T> source, ChannelWriter<T> writer, CancellationToken cancellationToken)
    {
        Exception? failure = null;
        try
        {
            await foreach (T item in source.WithCancellation(cancellationToken).ConfigureAwait(false))
            {
                await writer.WriteAsync(item, cancellationToken).ConfigureAwait(false);
            }
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            // The consumer stopped.
        }
        catch (Exception exception)
        {
            failure = exception;
        }
        finally
        {
            writer.TryComplete(failure);
        }
    }

    private readonly record struct Attempt<T>(bool Succeeded, T Value);

    // The selector, with each failure handed to onFailure instead of ending the enumeration.
    private static Func<TSource, CancellationToken, Task<Attempt<TResult>>> Guarded<TSource, TResult>(
        Func<TSource, CancellationToken, Task<TResult>> selector,
        Action<TSource, Exception> onFailure) =>
        async (item, cancellationToken) =>
        {
            Task<TResult>? task;
            try
            {
                task = selector(item, cancellationToken);
            }
            catch (Exception exception) when (!cancellationToken.IsCancellationRequested)
            {
                onFailure(item, exception);
                return default;
            }

            // A selector returning null is a bug in the pipeline, not a failure of this item.
            if (task is null)
            {
                throw new InvalidOperationException("The selector returned null.");
            }

            try
            {
                return new Attempt<TResult>(true, await task.ConfigureAwait(false));
            }
            catch (Exception exception) when (!cancellationToken.IsCancellationRequested)
            {
                onFailure(item, exception);
                return default;
            }
        };

    private static async IAsyncEnumerable<T> SkipFailed<T>(
        IAsyncEnumerable<Attempt<T>> attempts,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        await foreach (Attempt<T> attempt in attempts.WithCancellation(cancellationToken).ConfigureAwait(false))
        {
            if (attempt.Succeeded)
            {
                yield return attempt.Value;
            }
        }
    }
}
