namespace StructuredConcurrency;

/// <summary>
/// Runs at most one operation per key at a time: callers that ask for a key while its operation is running share its
/// result, like Go's <c>singleflight.Group</c>. Use it to stop a burst of identical requests (a cache stampede) from
/// all hitting the backend.
/// </summary>
/// <typeparam name="TKey">The type of the keys.</typeparam>
/// <typeparam name="TValue">The type of the results.</typeparam>
/// <remarks>
/// Results are not cached: once an operation has completed, the next call for its key starts a new one. Each caller's
/// token cancels only that caller's wait. The operation itself is cancelled only when every caller waiting for it has
/// given up, and the last caller to give up waits for it to end, so it never outlives all of its callers. A call that
/// comes while such a cancelled operation is still ending waits for it to end and then starts a new one, so the
/// operations for a key never overlap, except after <see cref="Forget(TKey)"/>.
/// </remarks>
public sealed class SingleFlight<TKey, TValue>
    where TKey : notnull
{
    private readonly Dictionary<TKey, Flight> _flights;

    /// <summary>Initializes a new instance of the <see cref="SingleFlight{TKey, TValue}"/> class.</summary>
    /// <param name="comparer">Compares keys, or <see langword="null"/> for the default comparer.</param>
    public SingleFlight(IEqualityComparer<TKey>? comparer = null)
    {
        _flights = new Dictionary<TKey, Flight>(comparer);
    }

    /// <summary>
    /// Returns the result of the operation running for <paramref name="key"/>, starting <paramref name="operation"/>
    /// if none is.
    /// </summary>
    /// <param name="key">Identifies the operation.</param>
    /// <param name="operation">Runs if no operation is running for <paramref name="key"/>; it receives a token that is
    /// cancelled when every caller has given up.</param>
    /// <param name="cancellationToken">Stops this caller waiting.</param>
    /// <returns>The shared result; awaiting it throws the shared failure.</returns>
    public Task<TValue> RunAsync(TKey key, Func<CancellationToken, Task<TValue>> operation, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(key);
        ArgumentNullException.ThrowIfNull(operation);
        if (cancellationToken.IsCancellationRequested)
        {
            return Task.FromCanceled<TValue>(cancellationToken);
        }

        Flight? flight;
        bool start = false;
        lock (_flights)
        {
            if (!_flights.TryGetValue(key, out flight) || flight.Abandoned)
            {
                // An abandoned flight's operation was cancelled and may still be running; the new one starts after it.
                flight = new Flight(key) { Previous = flight };
                _flights[key] = flight;
                start = true;
            }
            flight.Waiters++;
        }

        if (start)
        {
            Start(flight, operation);
        }
        return cancellationToken.CanBeCanceled ? WaitAsync(flight, cancellationToken) : flight.Result.Task;
    }

    /// <summary>
    /// Makes the next call for <paramref name="key"/> start a new operation, even if one is still running. Callers
    /// already waiting keep waiting for the running one.
    /// </summary>
    /// <param name="key">The key to forget.</param>
    public void Forget(TKey key)
    {
        ArgumentNullException.ThrowIfNull(key);
        lock (_flights)
        {
            _flights.Remove(key);
        }
    }

    private void Start(Flight flight, Func<CancellationToken, Task<TValue>> operation)
    {
        Task? previous = flight.Previous?.Result.Task;
        flight.Previous = null;
        if (previous is { IsCompleted: false })
        {
            previous.ContinueWith(
                static (_, state) =>
                {
                    var (self, flight, operation) = ((SingleFlight<TKey, TValue>, Flight, Func<CancellationToken, Task<TValue>>))state!;
                    self.StartNow(flight, operation);
                },
                (this, flight, operation),
                CancellationToken.None,
                TaskContinuationOptions.ExecuteSynchronously,
                TaskScheduler.Default);
        }
        else
        {
            StartNow(flight, operation);
        }
    }

    private void StartNow(Flight flight, Func<CancellationToken, Task<TValue>> operation)
    {
        Task<TValue> task;
        try
        {
            // Every caller may have given up while it waited for the previous operation; then it never runs.
            task = flight.Cancellation.IsCancellationRequested
                ? Task.FromCanceled<TValue>(flight.Cancellation.Token)
                : operation(flight.Cancellation.Token) ?? throw new InvalidOperationException("The operation returned null.");
        }
        catch (Exception exception)
        {
            task = Task.FromException<TValue>(exception);
        }

        task.ContinueWith(
            static (task, state) =>
            {
                var (self, flight) = ((SingleFlight<TKey, TValue>, Flight))state!;
                self.Land(flight, (Task<TValue>)task);
            },
            (this, flight),
            CancellationToken.None,
            TaskContinuationOptions.ExecuteSynchronously,
            TaskScheduler.Default);
    }

    private void Land(Flight flight, Task<TValue> task)
    {
        // Removed before the result is published, so a caller that reacts to it by calling again starts a new operation.
        Leave(flight, waiting: false);
        flight.Cancellation.Dispose();

        if (task.IsCompletedSuccessfully)
        {
            flight.Result.TrySetResult(task.Result);
        }
        else if (task.IsCanceled)
        {
            flight.Result.TrySetCanceled(TaskGroup.CancellationTokenOf(task));
        }
        else
        {
            flight.Result.TrySetException(task.Exception!.InnerExceptions);

            // Every caller may have given up already; the failure is theirs, not the finalizer's.
            _ = flight.Result.Task.Exception;
        }
    }

    private async Task<TValue> WaitAsync(Flight flight, CancellationToken cancellationToken)
    {
        try
        {
            return await flight.Result.Task.WaitAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested && !flight.Result.Task.IsCompleted)
        {
            if (Leave(flight, waiting: true))
            {
                // The last caller to give up waits for the cancelled operation to end.
                await ((Task)flight.Result.Task).ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing);
            }
            throw;
        }
    }

    // Removes a waiter (or, when the operation lands, the flight itself). Returns true if that cancelled the operation.
    // A cancelled flight stays in the dictionary, marked abandoned, until its operation lands, so that the next call
    // waits for it instead of overlapping it.
    private bool Leave(Flight flight, bool waiting)
    {
        bool cancel = false;
        lock (_flights)
        {
            if (waiting)
            {
                cancel = --flight.Waiters == 0;
                flight.Abandoned |= cancel;
            }
            if (!waiting && _flights.TryGetValue(flight.Key, out Flight? current) && current == flight)
            {
                _flights.Remove(flight.Key);
            }
        }

        if (cancel)
        {
            try
            {
                flight.Cancellation.Cancel();
            }
            catch (ObjectDisposedException)
            {
                // The operation landed at the same time.
            }
        }
        return cancel;
    }

    private sealed class Flight(TKey key)
    {
        public TKey Key { get; } = key;

        public CancellationTokenSource Cancellation { get; } = new();

        public TaskCompletionSource<TValue> Result { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        // Guarded by the lock on the flights dictionary.
        public int Waiters { get; set; }

        // Every caller gave up and the operation was cancelled. Guarded by the lock on the flights dictionary.
        public bool Abandoned { get; set; }

        // The abandoned flight this one must not overlap; read once, by Start.
        public Flight? Previous { get; set; }
    }
}
