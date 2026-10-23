namespace StructuredConcurrency;

/// <summary>
/// Runs operations one at a time per key, in the order they were asked for, while operations for different keys run
/// concurrently. Use it where work on the same thing must not overlap, such as the updates to one account or the
/// messages of one conversation, without a lock that makes everything else wait too.
/// </summary>
/// <typeparam name="TKey">The type of the keys.</typeparam>
/// <remarks>
/// Each caller's token stops only that caller: before its operation has started, it stops waiting for its turn, and
/// its operation never runs; once the operation has started, the operation receives the token, and the call returns
/// only after the operation has ended. The next operation for the key never starts before the previous one has ended.
/// A key holds no memory once nothing is running or waiting for it.
/// </remarks>
public sealed class KeyedSequencer<TKey>
    where TKey : notnull
{
    private readonly Dictionary<TKey, Lane> _lanes;

    /// <summary>Initializes a new instance of the <see cref="KeyedSequencer{TKey}"/> class.</summary>
    /// <param name="comparer">Compares keys, or <see langword="null"/> for the default comparer.</param>
    public KeyedSequencer(IEqualityComparer<TKey>? comparer = null)
    {
        _lanes = new Dictionary<TKey, Lane>(comparer);
    }

    /// <summary>
    /// Runs <paramref name="operation"/> once every operation asked for earlier for <paramref name="key"/> has ended.
    /// </summary>
    /// <param name="key">The key whose operations must not overlap.</param>
    /// <param name="operation">The operation; it receives <paramref name="cancellationToken"/>.</param>
    /// <param name="cancellationToken">Stops waiting for the turn, or cancels the operation once it has started.</param>
    /// <returns>A task that completes when the operation has ended.</returns>
    public Task RunAsync(TKey key, Func<CancellationToken, Task> operation, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(operation);
        return RunAsync(key, async ct =>
        {
            await (operation(ct) ?? throw new InvalidOperationException("The operation returned null.")).ConfigureAwait(false);
            return true;
        }, cancellationToken);
    }

    /// <summary>
    /// Runs <paramref name="operation"/> once every operation asked for earlier for <paramref name="key"/> has ended,
    /// and returns its result.
    /// </summary>
    /// <typeparam name="T">The type of the result.</typeparam>
    /// <param name="key">The key whose operations must not overlap.</param>
    /// <param name="operation">The operation; it receives <paramref name="cancellationToken"/>.</param>
    /// <param name="cancellationToken">Stops waiting for the turn, or cancels the operation once it has started.</param>
    /// <returns>The operation's result.</returns>
    public Task<T> RunAsync<T>(TKey key, Func<CancellationToken, Task<T>> operation, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(key);
        ArgumentNullException.ThrowIfNull(operation);
        if (cancellationToken.IsCancellationRequested)
        {
            return Task.FromCanceled<T>(cancellationToken);
        }

        // The turn completes when this operation has ended or been skipped; the next one for the key waits for it.
        var turn = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        Lane? lane;
        Task previous;
        lock (_lanes)
        {
            if (!_lanes.TryGetValue(key, out lane))
            {
                lane = new Lane();
                _lanes.Add(key, lane);
            }
            previous = lane.Last;
            lane.Last = turn.Task;
            lane.Count++;
        }
        return RunInTurnAsync(key, lane, previous, turn, operation, cancellationToken);
    }

    private async Task<T> RunInTurnAsync<T>(
        TKey key,
        Lane lane,
        Task previous,
        TaskCompletionSource turn,
        Func<CancellationToken, Task<T>> operation,
        CancellationToken cancellationToken)
    {
        try
        {
            if (!previous.IsCompleted)
            {
                await previous.WaitAsync(cancellationToken).ConfigureAwait(false);
            }
            cancellationToken.ThrowIfCancellationRequested();
            return await (operation(cancellationToken) ?? throw new InvalidOperationException("The operation returned null."))
                .ConfigureAwait(false);
        }
        finally
        {
            // A caller that gave up waiting must not let the next one overtake the operation still running before it.
            if (previous.IsCompleted)
            {
                EndTurn(key, lane, turn);
            }
            else
            {
                _ = previous.ContinueWith(
                    static (_, state) =>
                    {
                        var (self, key, lane, turn) = ((KeyedSequencer<TKey>, TKey, Lane, TaskCompletionSource))state!;
                        self.EndTurn(key, lane, turn);
                    },
                    (this, key, lane, turn),
                    CancellationToken.None,
                    TaskContinuationOptions.ExecuteSynchronously,
                    TaskScheduler.Default);
            }
        }
    }

    private void EndTurn(TKey key, Lane lane, TaskCompletionSource turn)
    {
        lock (_lanes)
        {
            if (--lane.Count == 0)
            {
                _lanes.Remove(key);
            }
        }
        turn.SetResult();
    }

    // The operations running or waiting for one key. Guarded by the lock on the lanes dictionary.
    private sealed class Lane
    {
        // The turn of the last operation asked for; it never fails.
        public Task Last = Task.CompletedTask;

        public int Count;
    }
}
