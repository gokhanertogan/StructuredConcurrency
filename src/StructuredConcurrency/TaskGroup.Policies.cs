namespace StructuredConcurrency;

// One-shot policies over a fixed set of operations, named after JavaScript's Promise.all, Promise.any and
// Promise.allSettled. Each runs its operations as children of a TaskGroup, so none of them outlives the call.
public sealed partial class TaskGroup
{
    // ---------------------------------------------------------------------------------------------------------------
    // All: every operation must succeed; the first failure cancels the rest.

    /// <summary>
    /// Runs two operations concurrently and returns both results. The first failure cancels the other operation.
    /// </summary>
    /// <typeparam name="T1">The type of the first result.</typeparam>
    /// <typeparam name="T2">The type of the second result.</typeparam>
    /// <param name="first">The first operation.</param>
    /// <param name="second">The second operation.</param>
    /// <param name="cancellationToken">Cancels both operations.</param>
    /// <returns>Both results; awaiting it throws the first failure.</returns>
    public static Task<(T1, T2)> AllAsync<T1, T2>(
        Func<CancellationToken, Task<T1>> first,
        Func<CancellationToken, Task<T2>> second,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(first);
        ArgumentNullException.ThrowIfNull(second);
        return RunAsync(async group =>
        {
            Task<T1> a = group.Run(first);
            Task<T2> b = group.Run(second);
            return (await a.ConfigureAwait(false), await b.ConfigureAwait(false));
        }, cancellationToken);
    }

    /// <summary>
    /// Runs three operations concurrently and returns all results. The first failure cancels the other operations.
    /// </summary>
    /// <typeparam name="T1">The type of the first result.</typeparam>
    /// <typeparam name="T2">The type of the second result.</typeparam>
    /// <typeparam name="T3">The type of the third result.</typeparam>
    /// <param name="first">The first operation.</param>
    /// <param name="second">The second operation.</param>
    /// <param name="third">The third operation.</param>
    /// <param name="cancellationToken">Cancels all operations.</param>
    /// <returns>All results; awaiting it throws the first failure.</returns>
    public static Task<(T1, T2, T3)> AllAsync<T1, T2, T3>(
        Func<CancellationToken, Task<T1>> first,
        Func<CancellationToken, Task<T2>> second,
        Func<CancellationToken, Task<T3>> third,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(first);
        ArgumentNullException.ThrowIfNull(second);
        ArgumentNullException.ThrowIfNull(third);
        return RunAsync(async group =>
        {
            Task<T1> a = group.Run(first);
            Task<T2> b = group.Run(second);
            Task<T3> c = group.Run(third);
            return (await a.ConfigureAwait(false), await b.ConfigureAwait(false), await c.ConfigureAwait(false));
        }, cancellationToken);
    }

    /// <summary>
    /// Runs the operations concurrently and returns their results in input order. The first failure cancels the rest.
    /// </summary>
    /// <typeparam name="T">The type of the results.</typeparam>
    /// <param name="operations">The operations to run.</param>
    /// <param name="cancellationToken">Cancels all operations.</param>
    /// <returns>The results in input order; awaiting it throws the first failure.</returns>
    public static Task<T[]> AllAsync<T>(
        IEnumerable<Func<CancellationToken, Task<T>>> operations,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(operations);
        return AllCore(operations.ToArray(), static (operation, ct) => operation(ct), cancellationToken);
    }

    /// <summary>
    /// Runs <paramref name="operation"/> for every item concurrently and returns the results in input order. The first
    /// failure cancels the rest.
    /// </summary>
    /// <typeparam name="TSource">The type of the items.</typeparam>
    /// <typeparam name="TResult">The type of the results.</typeparam>
    /// <param name="source">The items to run the operation for.</param>
    /// <param name="operation">The operation to run for each item.</param>
    /// <param name="cancellationToken">Cancels all operations.</param>
    /// <returns>The results in input order; awaiting it throws the first failure.</returns>
    public static Task<TResult[]> AllAsync<TSource, TResult>(
        IEnumerable<TSource> source,
        Func<TSource, CancellationToken, Task<TResult>> operation,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(source);
        ArgumentNullException.ThrowIfNull(operation);
        return AllCore(source.ToArray(), operation, cancellationToken);
    }

    private static Task<TResult[]> AllCore<TSource, TResult>(
        TSource[] items,
        Func<TSource, CancellationToken, Task<TResult>> operation,
        CancellationToken cancellationToken)
    {
        var results = new TResult[items.Length];
        return RunAsync(group =>
        {
            for (int i = 0; i < items.Length; i++)
            {
                int index = i;
                group.Run(async ct => results[index] = await operation(items[index], ct).ConfigureAwait(false));
            }

            // The group completes only after every child has, so the array is filled when this result is observed.
            return Task.FromResult(results);
        }, cancellationToken);
    }

    // ---------------------------------------------------------------------------------------------------------------
    // Any: the first operation to succeed wins and the rest are cancelled.

    /// <summary>
    /// Runs two operations concurrently and returns the result of the first one to succeed, cancelling the other.
    /// </summary>
    /// <typeparam name="T">The type of the result.</typeparam>
    /// <param name="first">The first operation.</param>
    /// <param name="second">The second operation.</param>
    /// <param name="cancellationToken">Cancels both operations.</param>
    /// <returns>The first successful result.</returns>
    /// <exception cref="AggregateException">Every operation failed; holds the failures in input order.</exception>
    public static Task<T> AnyAsync<T>(
        Func<CancellationToken, Task<T>> first,
        Func<CancellationToken, Task<T>> second,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(first);
        ArgumentNullException.ThrowIfNull(second);
        return AnyCore(new[] { first, second }, static (operation, ct) => operation(ct), cancellationToken);
    }

    /// <summary>
    /// Runs three operations concurrently and returns the result of the first one to succeed, cancelling the others.
    /// </summary>
    /// <typeparam name="T">The type of the result.</typeparam>
    /// <param name="first">The first operation.</param>
    /// <param name="second">The second operation.</param>
    /// <param name="third">The third operation.</param>
    /// <param name="cancellationToken">Cancels all operations.</param>
    /// <returns>The first successful result.</returns>
    /// <exception cref="AggregateException">Every operation failed; holds the failures in input order.</exception>
    public static Task<T> AnyAsync<T>(
        Func<CancellationToken, Task<T>> first,
        Func<CancellationToken, Task<T>> second,
        Func<CancellationToken, Task<T>> third,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(first);
        ArgumentNullException.ThrowIfNull(second);
        ArgumentNullException.ThrowIfNull(third);
        return AnyCore(new[] { first, second, third }, static (operation, ct) => operation(ct), cancellationToken);
    }

    /// <summary>
    /// Runs the operations concurrently and returns the result of the first one to succeed, cancelling the rest.
    /// </summary>
    /// <typeparam name="T">The type of the result.</typeparam>
    /// <param name="operations">The operations to run; at least one.</param>
    /// <param name="cancellationToken">Cancels all operations.</param>
    /// <returns>The first successful result.</returns>
    /// <exception cref="ArgumentException"><paramref name="operations"/> is empty.</exception>
    /// <exception cref="AggregateException">Every operation failed; holds the failures in input order.</exception>
    public static Task<T> AnyAsync<T>(
        IEnumerable<Func<CancellationToken, Task<T>>> operations,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(operations);
        Func<CancellationToken, Task<T>>[] items = operations.ToArray();
        if (items.Length == 0)
        {
            throw new ArgumentException("At least one operation is required.", nameof(operations));
        }
        return AnyCore(items, static (operation, ct) => operation(ct), cancellationToken);
    }

    /// <summary>
    /// Runs <paramref name="operation"/> for every item concurrently and returns the first successful result,
    /// cancelling the rest.
    /// </summary>
    /// <typeparam name="TSource">The type of the items.</typeparam>
    /// <typeparam name="TResult">The type of the result.</typeparam>
    /// <param name="source">The items to run the operation for; at least one.</param>
    /// <param name="operation">The operation to run for each item.</param>
    /// <param name="cancellationToken">Cancels all operations.</param>
    /// <returns>The first successful result.</returns>
    /// <exception cref="ArgumentException"><paramref name="source"/> is empty.</exception>
    /// <exception cref="AggregateException">Every operation failed; holds the failures in input order.</exception>
    public static Task<TResult> AnyAsync<TSource, TResult>(
        IEnumerable<TSource> source,
        Func<TSource, CancellationToken, Task<TResult>> operation,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(source);
        ArgumentNullException.ThrowIfNull(operation);
        TSource[] items = source.ToArray();
        if (items.Length == 0)
        {
            throw new ArgumentException("At least one item is required.", nameof(source));
        }
        return AnyCore(items, operation, cancellationToken);
    }

    private static async Task<TResult> AnyCore<TSource, TResult>(
        TSource[] items,
        Func<TSource, CancellationToken, Task<TResult>> operation,
        CancellationToken cancellationToken)
    {
        var state = new AnyState<TResult>(items.Length);

        // Failures are kept per operation rather than failing the group, so one failure doesn't stop the race.
        // A caller cancellation still ends the group as cancelled, which the await below rethrows.
        await RunAsync(group =>
        {
            for (int i = 0; i < items.Length; i++)
            {
                int index = i;
                group.Run(async ct =>
                {
                    try
                    {
                        TResult result = await operation(items[index], ct).ConfigureAwait(false);
                        if (Interlocked.CompareExchange(ref state.Won, 1, 0) == 0)
                        {
                            state.Value = result;
                            group.Cancel();
                        }
                    }
                    catch (Exception exception)
                    {
                        state.Failures[index] = exception;
                    }
                });
            }
            return Task.CompletedTask;
        }, cancellationToken).ConfigureAwait(false);

        if (state.Won == 1)
        {
            return state.Value!;
        }
        throw new AggregateException("Every operation failed.", state.Failures.Where(failure => failure is not null)!);
    }

    private sealed class AnyState<TResult>(int count)
    {
        public int Won;
        public TResult? Value;
        public readonly Exception?[] Failures = new Exception?[count];
    }

    // ---------------------------------------------------------------------------------------------------------------
    // AllSettled: every operation runs to completion and each outcome is reported; failures don't cancel anything.

    /// <summary>Runs two operations concurrently and returns how each one ended. A failure doesn't cancel the other.</summary>
    /// <typeparam name="T1">The type of the first result.</typeparam>
    /// <typeparam name="T2">The type of the second result.</typeparam>
    /// <param name="first">The first operation.</param>
    /// <param name="second">The second operation.</param>
    /// <param name="cancellationToken">Cancels both operations; the call then ends as cancelled.</param>
    /// <returns>The outcome of each operation.</returns>
    public static Task<(TaskOutcome<T1>, TaskOutcome<T2>)> AllSettledAsync<T1, T2>(
        Func<CancellationToken, Task<T1>> first,
        Func<CancellationToken, Task<T2>> second,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(first);
        ArgumentNullException.ThrowIfNull(second);
        return RunAsync(async group =>
        {
            Task<TaskOutcome<T1>> a = group.Run(ct => SettleAsync(first, ct));
            Task<TaskOutcome<T2>> b = group.Run(ct => SettleAsync(second, ct));
            return (await a.ConfigureAwait(false), await b.ConfigureAwait(false));
        }, cancellationToken);
    }

    /// <summary>Runs three operations concurrently and returns how each one ended. Failures don't cancel the others.</summary>
    /// <typeparam name="T1">The type of the first result.</typeparam>
    /// <typeparam name="T2">The type of the second result.</typeparam>
    /// <typeparam name="T3">The type of the third result.</typeparam>
    /// <param name="first">The first operation.</param>
    /// <param name="second">The second operation.</param>
    /// <param name="third">The third operation.</param>
    /// <param name="cancellationToken">Cancels all operations; the call then ends as cancelled.</param>
    /// <returns>The outcome of each operation.</returns>
    public static Task<(TaskOutcome<T1>, TaskOutcome<T2>, TaskOutcome<T3>)> AllSettledAsync<T1, T2, T3>(
        Func<CancellationToken, Task<T1>> first,
        Func<CancellationToken, Task<T2>> second,
        Func<CancellationToken, Task<T3>> third,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(first);
        ArgumentNullException.ThrowIfNull(second);
        ArgumentNullException.ThrowIfNull(third);
        return RunAsync(async group =>
        {
            Task<TaskOutcome<T1>> a = group.Run(ct => SettleAsync(first, ct));
            Task<TaskOutcome<T2>> b = group.Run(ct => SettleAsync(second, ct));
            Task<TaskOutcome<T3>> c = group.Run(ct => SettleAsync(third, ct));
            return (await a.ConfigureAwait(false), await b.ConfigureAwait(false), await c.ConfigureAwait(false));
        }, cancellationToken);
    }

    /// <summary>
    /// Runs the operations concurrently and returns how each one ended, in input order. Failures don't cancel the others.
    /// </summary>
    /// <typeparam name="T">The type of the results.</typeparam>
    /// <param name="operations">The operations to run.</param>
    /// <param name="cancellationToken">Cancels all operations; the call then ends as cancelled.</param>
    /// <returns>The outcome of each operation, in input order.</returns>
    public static Task<TaskOutcome<T>[]> AllSettledAsync<T>(
        IEnumerable<Func<CancellationToken, Task<T>>> operations,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(operations);
        return AllSettledCore(operations.ToArray(), static (operation, ct) => operation(ct), cancellationToken);
    }

    /// <summary>
    /// Runs <paramref name="operation"/> for every item concurrently and returns how each one ended, in input order.
    /// Failures don't cancel the others.
    /// </summary>
    /// <typeparam name="TSource">The type of the items.</typeparam>
    /// <typeparam name="TResult">The type of the results.</typeparam>
    /// <param name="source">The items to run the operation for.</param>
    /// <param name="operation">The operation to run for each item.</param>
    /// <param name="cancellationToken">Cancels all operations; the call then ends as cancelled.</param>
    /// <returns>The outcome of each operation, in input order.</returns>
    public static Task<TaskOutcome<TResult>[]> AllSettledAsync<TSource, TResult>(
        IEnumerable<TSource> source,
        Func<TSource, CancellationToken, Task<TResult>> operation,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(source);
        ArgumentNullException.ThrowIfNull(operation);
        return AllSettledCore(source.ToArray(), operation, cancellationToken);
    }

    private static Task<TaskOutcome<TResult>[]> AllSettledCore<TSource, TResult>(
        TSource[] items,
        Func<TSource, CancellationToken, Task<TResult>> operation,
        CancellationToken cancellationToken)
    {
        var outcomes = new TaskOutcome<TResult>[items.Length];
        return RunAsync(group =>
        {
            for (int i = 0; i < items.Length; i++)
            {
                int index = i;
                group.Run(async ct => outcomes[index] = await SettleAsync(c => operation(items[index], c), ct).ConfigureAwait(false));
            }

            // The group completes only after every child has, so the array is filled when this result is observed.
            return Task.FromResult(outcomes);
        }, cancellationToken);
    }

    private static async Task<TaskOutcome<T>> SettleAsync<T>(Func<CancellationToken, Task<T>> operation, CancellationToken cancellationToken)
    {
        try
        {
            return TaskOutcome<T>.Success(await operation(cancellationToken).ConfigureAwait(false));
        }
        catch (Exception exception)
        {
            return TaskOutcome<T>.Failure(exception);
        }
    }
}
