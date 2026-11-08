namespace StructuredConcurrency;

public sealed partial class TaskGroup
{
    private static readonly TaskGroupOptions s_quorumOptions = new() { Name = "TaskGroup.QuorumAsync" };

    /// <summary>
    /// Runs the operations concurrently and returns the results of the first <paramref name="quorum"/> to succeed,
    /// cancelling the rest. With <paramref name="quorum"/> = 1 it is <see cref="FirstSuccessAsync{T}(IEnumerable{Func{CancellationToken, Task{T}}}, CancellationToken)"/>.
    /// </summary>
    /// <typeparam name="T">The type of the results.</typeparam>
    /// <param name="operations">The operations to run.</param>
    /// <param name="quorum">How many must succeed: at least 1 and at most the number of operations.</param>
    /// <param name="cancellationToken">Cancels all operations.</param>
    /// <returns>
    /// The first <paramref name="quorum"/> results, in the order the operations succeeded. The task completes only
    /// after every operation has ended.
    /// </returns>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="quorum"/> is less than 1 or more than the number of operations.</exception>
    /// <exception cref="AggregateException">
    /// So many operations failed that the quorum can't be reached; holds those failures in input order. The others are
    /// cancelled as soon as that is certain.
    /// </exception>
    public static Task<T[]> QuorumAsync<T>(
        IEnumerable<Func<CancellationToken, Task<T>>> operations,
        int quorum,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(operations);
        Func<CancellationToken, Task<T>>[] items = operations.ToArray();
        ValidateQuorum(quorum, items.Length);
        return QuorumCore(items, static (operation, ct) => operation(ct), quorum, cancellationToken);
    }

    /// <summary>
    /// Runs <paramref name="operation"/> for every item concurrently and returns the results of the first
    /// <paramref name="quorum"/> to succeed, cancelling the rest. Use it to read from enough replicas, or to take a
    /// majority of votes.
    /// </summary>
    /// <typeparam name="TSource">The type of the items.</typeparam>
    /// <typeparam name="TResult">The type of the results.</typeparam>
    /// <param name="source">The items to run the operation for.</param>
    /// <param name="operation">The operation to run for each item.</param>
    /// <param name="quorum">How many must succeed: at least 1 and at most the number of items.</param>
    /// <param name="cancellationToken">Cancels all operations.</param>
    /// <returns>
    /// The first <paramref name="quorum"/> results, in the order the operations succeeded. The task completes only
    /// after every operation has ended.
    /// </returns>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="quorum"/> is less than 1 or more than the number of items.</exception>
    /// <exception cref="AggregateException">
    /// So many operations failed that the quorum can't be reached; holds those failures in input order. The others are
    /// cancelled as soon as that is certain.
    /// </exception>
    public static Task<TResult[]> QuorumAsync<TSource, TResult>(
        IEnumerable<TSource> source,
        Func<TSource, CancellationToken, Task<TResult>> operation,
        int quorum,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(source);
        ArgumentNullException.ThrowIfNull(operation);
        TSource[] items = source.ToArray();
        ValidateQuorum(quorum, items.Length);
        return QuorumCore(items, operation, quorum, cancellationToken);
    }

    private static void ValidateQuorum(int quorum, int count)
    {
        if (quorum < 1 || quorum > count)
        {
            throw new ArgumentOutOfRangeException(nameof(quorum), quorum, $"The quorum must be between 1 and the number of operations ({count}).");
        }
    }

    private static async Task<TResult[]> QuorumCore<TSource, TResult>(
        TSource[] items,
        Func<TSource, CancellationToken, Task<TResult>> operation,
        int quorum,
        CancellationToken cancellationToken)
    {
        var state = new QuorumState<TResult>(items.Length, quorum);

        // As in FirstSuccessAsync, failures are kept rather than failing the group; the group is cancelled once the
        // outcome is decided either way. A caller cancellation ends the group as cancelled, which the await rethrows.
        await RunAsync(group =>
        {
            for (int i = 0; i < items.Length; i++)
            {
                int index = i;
                group.Run(async ct =>
                {
                    bool decided;
                    try
                    {
                        TResult result = await (operation(items[index], ct) ?? throw new InvalidOperationException("The operation returned null."))
                            .ConfigureAwait(false);
                        decided = state.Succeeded(result);
                    }
                    catch (Exception exception)
                    {
                        decided = state.Failed(index, exception);
                    }
                    if (decided)
                    {
                        group.Cancel();
                    }
                });
            }
            return Task.CompletedTask;
        }, s_quorumOptions, cancellationToken).ConfigureAwait(false);

        return state.Result();
    }

    private sealed class QuorumState<TResult>(int count, int quorum)
    {
        private readonly TResult[] _results = new TResult[quorum];
        private readonly Exception?[] _failures = new Exception?[count];
        private int _succeeded;
        private int _failed;
        private bool _decided;

        // True when this success reaches the quorum.
        public bool Succeeded(TResult result)
        {
            lock (this)
            {
                if (_decided)
                {
                    return false;
                }
                _results[_succeeded++] = result;
                return _decided = _succeeded == quorum;
            }
        }

        // True when this failure makes the quorum impossible. Once the outcome is decided, what the cancelled
        // operations throw is the cancellation's doing, not a failure.
        public bool Failed(int index, Exception exception)
        {
            lock (this)
            {
                if (_decided)
                {
                    return false;
                }
                _failures[index] = exception;
                _failed++;
                return _decided = _failed > count - quorum;
            }
        }

        public TResult[] Result()
        {
            lock (this)
            {
                if (_succeeded == quorum)
                {
                    return _results;
                }
                throw new AggregateException(
                    $"Only {_succeeded} of {count} operations succeeded; {quorum} were needed.",
                    _failures.Where(failure => failure is not null)!);
            }
        }
    }
}
