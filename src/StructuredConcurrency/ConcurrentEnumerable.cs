using System.Runtime.CompilerServices;

namespace StructuredConcurrency;

/// <summary>
/// Runs an operation for every item with a concurrency limit and streams the results as they are ready, like Kotlin's
/// <c>flatMapMerge</c>, Rust's <c>buffered</c> / <c>buffer_unordered</c> and Python's <c>asyncio.as_completed</c>.
/// </summary>
public static class ConcurrentEnumerable
{
    /// <summary>
    /// Runs <paramref name="selector"/> for the items, at most <paramref name="maxConcurrency"/> at a time, and yields
    /// each result as soon as it can.
    /// </summary>
    /// <typeparam name="TSource">The type of the items.</typeparam>
    /// <typeparam name="TResult">The type of the results.</typeparam>
    /// <param name="source">The items. They are read lazily, only as slots free up.</param>
    /// <param name="selector">The operation to run for each item.</param>
    /// <param name="maxConcurrency">The maximum number of operations that run at the same time; at least 1.</param>
    /// <param name="preserveOrder">
    /// <see langword="true"/> to yield results in input order; a slow item then holds back the results after it, and
    /// it counts towards the limit until it is yielded. <see langword="false"/> to yield them as they complete.
    /// </param>
    /// <returns>
    /// The results. Operations start only while the sequence is enumerated. The first failure is thrown from the
    /// enumeration. When enumeration ends for any reason (all items done, <c>break</c>, a failure, or cancellation via
    /// <c>WithCancellation</c>), the operations still running are cancelled and awaited before it returns, so none
    /// of them outlives the loop.
    /// </returns>
    public static IAsyncEnumerable<TResult> SelectConcurrentAsync<TSource, TResult>(
        this IEnumerable<TSource> source,
        Func<TSource, CancellationToken, Task<TResult>> selector,
        int maxConcurrency,
        bool preserveOrder = true)
    {
        ArgumentNullException.ThrowIfNull(source);
        ArgumentNullException.ThrowIfNull(selector);
        ArgumentOutOfRangeException.ThrowIfLessThan(maxConcurrency, 1);
        return SelectCore(source, selector, maxConcurrency, preserveOrder);
    }

    /// <summary>
    /// Runs <paramref name="selector"/> for the items of an asynchronous stream, at most
    /// <paramref name="maxConcurrency"/> at a time, and yields each result as soon as it can.
    /// </summary>
    /// <typeparam name="TSource">The type of the items.</typeparam>
    /// <typeparam name="TResult">The type of the results.</typeparam>
    /// <param name="source">
    /// The items, such as rows from a database, pages from an API or a channel's <c>ReadAllAsync()</c>. They are read
    /// only as slots free up, and the source receives the enumeration's cancellation.
    /// </param>
    /// <param name="selector">The operation to run for each item.</param>
    /// <param name="maxConcurrency">The maximum number of operations that run at the same time; at least 1.</param>
    /// <param name="preserveOrder">
    /// <see langword="true"/> to yield results in input order; a slow item then holds back the results after it, and
    /// it counts towards the limit until it is yielded. <see langword="false"/> to yield them as they complete.
    /// </param>
    /// <returns>
    /// The results. Nothing is read or started until the sequence is enumerated. The first failure, of an operation
    /// or of the source, is thrown from the enumeration. When enumeration ends for any reason, the operations still
    /// running are cancelled and awaited, and the source is disposed, before it returns.
    /// </returns>
    /// <remarks>
    /// A source that ignores its cancellation token can't be stopped in the middle of a read, and it can't be disposed
    /// during one: ending the enumeration then waits for the read in progress to finish.
    /// </remarks>
    public static IAsyncEnumerable<TResult> SelectConcurrentAsync<TSource, TResult>(
        this IAsyncEnumerable<TSource> source,
        Func<TSource, CancellationToken, Task<TResult>> selector,
        int maxConcurrency,
        bool preserveOrder = true)
    {
        ArgumentNullException.ThrowIfNull(source);
        ArgumentNullException.ThrowIfNull(selector);
        ArgumentOutOfRangeException.ThrowIfLessThan(maxConcurrency, 1);
        return SelectCore(source, selector, maxConcurrency, preserveOrder);
    }

    private static async IAsyncEnumerable<TResult> SelectCore<TSource, TResult>(
        IEnumerable<TSource> source,
        Func<TSource, CancellationToken, Task<TResult>> selector,
        int maxConcurrency,
        bool preserveOrder,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);

        // In input order, so the first one is the next result when order is preserved.
        var running = new List<Task<TResult>>(maxConcurrency);
        using IEnumerator<TSource> items = source.GetEnumerator();
        bool more = true;
        try
        {
            while (true)
            {
                while (more && running.Count < maxConcurrency && !cts.IsCancellationRequested)
                {
                    more = items.MoveNext();
                    if (more)
                    {
                        running.Add(Start(selector, items.Current, cts.Token));
                    }
                }
                cancellationToken.ThrowIfCancellationRequested();
                if (running.Count == 0)
                {
                    yield break;
                }

                Task<TResult> next = await NextAsync(running, preserveOrder).ConfigureAwait(false);
                running.Remove(next);
                if (!next.IsCompletedSuccessfully && cancellationToken.IsCancellationRequested)
                {
                    // Report the caller's cancellation rather than the operation's reaction to it.
                    cancellationToken.ThrowIfCancellationRequested();
                }
                yield return await next.ConfigureAwait(false);
            }
        }
        finally
        {
            cts.Cancel();
            await AwaitQuietlyAsync(running).ConfigureAwait(false);
        }
    }

    private static async IAsyncEnumerable<TResult> SelectCore<TSource, TResult>(
        IAsyncEnumerable<TSource> source,
        Func<TSource, CancellationToken, Task<TResult>> selector,
        int maxConcurrency,
        bool preserveOrder,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);

        // In input order, so the first one is the next result when order is preserved.
        var running = new List<Task<TResult>>(maxConcurrency);

        // The source gets the same token as the operations, so ending the enumeration stops it reading too.
        IAsyncEnumerator<TSource> items = source.GetAsyncEnumerator(cts.Token);
        Task<bool>? moving = null;   // the source's MoveNextAsync while it is in flight
        bool more = true;
        try
        {
            while (true)
            {
                // Read items while there are free slots, as far as the source has them ready.
                while (more && running.Count < maxConcurrency && !cts.IsCancellationRequested)
                {
                    if (moving is null)
                    {
                        // Most sources (a channel with items queued, a buffered reader) complete synchronously: only
                        // allocate a task for a read that has to wait.
                        ValueTask<bool> read = items.MoveNextAsync();
                        if (!read.IsCompleted)
                        {
                            moving = read.AsTask();
                            break;
                        }
                        more = read.Result;   // rethrows the source's failure, if any
                    }
                    else if (moving.IsCompleted)
                    {
                        more = await moving.ConfigureAwait(false);
                        moving = null;
                    }
                    else
                    {
                        break;
                    }

                    if (more)
                    {
                        running.Add(Start(selector, items.Current, cts.Token));
                    }
                }
                cancellationToken.ThrowIfCancellationRequested();

                if (TryPick(running, preserveOrder) is Task<TResult> next)
                {
                    running.Remove(next);
                    if (!next.IsCompletedSuccessfully && cancellationToken.IsCancellationRequested)
                    {
                        // Report the caller's cancellation rather than the operation's reaction to it.
                        cancellationToken.ThrowIfCancellationRequested();
                    }
                    yield return await next.ConfigureAwait(false);
                    continue;
                }
                if (running.Count == 0 && !more)
                {
                    yield break;
                }

                // Wait for an operation to complete or, when a slot is free, for the source's next item.
                var waits = new List<Task>(running.Count + 1);
                waits.AddRange(running.Where(task => !task.IsCompleted));
                if (moving is not null)
                {
                    waits.Add(moving);
                }
                if (waits.Count > 0)
                {
                    await Task.WhenAny(waits).ConfigureAwait(false);
                }
            }
        }
        finally
        {
            cts.Cancel();
            await AwaitQuietlyAsync(running).ConfigureAwait(false);

            // An enumerator can't be disposed while a MoveNextAsync is in flight.
            if (moving is not null)
            {
                await ((Task)moving).ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing);
            }
            await items.DisposeAsync().ConfigureAwait(false);
        }
    }

    private static async Task AwaitQuietlyAsync<TResult>(List<Task<TResult>> running)
    {
        foreach (Task<TResult> task in running)
        {
            try
            {
                await task.ConfigureAwait(false);
            }
            catch
            {
                // The enumeration is already ending: with the first failure, a break, or cancellation. What the
                // cancelled operations end with is no longer of interest.
            }
        }
    }

    // The result to yield next: the first one to complete, or with preserveOrder the first in input order. A failure
    // anywhere is returned at once, so it ends the enumeration without waiting for the items before it.
    private static async Task<Task<TResult>> NextAsync<TResult>(List<Task<TResult>> running, bool preserveOrder)
    {
        while (true)
        {
            if (TryPick(running, preserveOrder) is Task<TResult> pick)
            {
                return pick;
            }

            // Only the tasks still running, so one that completed successfully out of order isn't picked again. They
            // may all have completed since the scan above; then scan again.
            Task<TResult>[] pending = running.Where(task => !task.IsCompleted).ToArray();
            if (pending.Length > 0)
            {
                await Task.WhenAny(pending).ConfigureAwait(false);
            }
        }
    }

    // The result that can be yielded now, if any; see NextAsync.
    private static Task<TResult>? TryPick<TResult>(List<Task<TResult>> running, bool preserveOrder)
    {
        Task<TResult>? pick = null;
        foreach (Task<TResult> task in running)
        {
            if (task.IsCompleted && (!task.IsCompletedSuccessfully || !preserveOrder || task == running[0]))
            {
                pick = task;
                if (!task.IsCompletedSuccessfully)
                {
                    break;
                }
            }
        }
        return pick;
    }

    private static Task<TResult> Start<TSource, TResult>(
        Func<TSource, CancellationToken, Task<TResult>> selector,
        TSource item,
        CancellationToken cancellationToken)
    {
        try
        {
            return selector(item, cancellationToken) ?? throw new InvalidOperationException("The selector returned null.");
        }
        catch (Exception exception)
        {
            return Task.FromException<TResult>(exception);
        }
    }
}
