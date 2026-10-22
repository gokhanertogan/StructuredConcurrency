namespace StructuredConcurrency;

// Scoped timeouts, like Kotlin's withTimeout, Python's asyncio.timeout and Trio's fail_after / move_on_after. Unlike
// Task.WaitAsync(timeout), the operation is cancelled when the time is up, and the call returns only after it has ended.
public sealed partial class TaskGroup
{
    /// <summary>
    /// Runs <paramref name="operation"/> and cancels it if it hasn't completed within <paramref name="timeout"/>.
    /// </summary>
    /// <typeparam name="T">The type of the result.</typeparam>
    /// <param name="timeout">How long the operation may run: up to about 49 days, or <see cref="Timeout.InfiniteTimeSpan"/>.</param>
    /// <param name="operation">The operation; it receives a token that is cancelled when the time is up.</param>
    /// <param name="cancellationToken">Cancels the operation.</param>
    /// <returns>
    /// The operation's result. The task completes only after the operation has, so nothing keeps running once it
    /// returns. Awaiting it throws <see cref="TimeoutException"/> if the operation ended because the time was up.
    /// </returns>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="timeout"/> is negative or too long.</exception>
    public static Task<T> WithTimeoutAsync<T>(
        TimeSpan timeout,
        Func<CancellationToken, Task<T>> operation,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(operation);
        Timeouts.Validate(timeout, nameof(timeout));
        return Core();

        async Task<T> Core()
        {
            (T result, OperationCanceledException? timedOut) =
                await WithTimeoutCore(timeout, operation, cancellationToken).ConfigureAwait(false);
            return timedOut is null
                ? result
                : throw new TimeoutException($"The operation did not complete within {timeout}.", timedOut);
        }
    }

    /// <summary>
    /// Runs <paramref name="operation"/> and cancels it if it hasn't completed within <paramref name="timeout"/>.
    /// </summary>
    /// <param name="timeout">How long the operation may run: up to about 49 days, or <see cref="Timeout.InfiniteTimeSpan"/>.</param>
    /// <param name="operation">The operation; it receives a token that is cancelled when the time is up.</param>
    /// <param name="cancellationToken">Cancels the operation.</param>
    /// <returns>
    /// A task that completes only after the operation has. Awaiting it throws <see cref="TimeoutException"/> if the
    /// operation ended because the time was up.
    /// </returns>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="timeout"/> is negative or too long.</exception>
    public static Task WithTimeoutAsync(
        TimeSpan timeout,
        Func<CancellationToken, Task> operation,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(operation);
        return WithTimeoutAsync(timeout, async ct =>
        {
            await Invoke(operation, ct).ConfigureAwait(false);
            return default(VoidResult);
        }, cancellationToken);
    }

    /// <summary>
    /// Runs <paramref name="operation"/>, and if it hasn't completed within <paramref name="timeout"/>, cancels it
    /// and returns <paramref name="defaultValue"/>, like Kotlin's <c>withTimeoutOrNull</c>.
    /// </summary>
    /// <typeparam name="T">The type of the result.</typeparam>
    /// <param name="timeout">How long the operation may run: up to about 49 days, or <see cref="Timeout.InfiniteTimeSpan"/>.</param>
    /// <param name="operation">The operation; it receives a token that is cancelled when the time is up.</param>
    /// <param name="defaultValue">The value to return when the time is up.</param>
    /// <param name="cancellationToken">Cancels the operation.</param>
    /// <returns>
    /// The operation's result, or <paramref name="defaultValue"/> if it ended because the time was up. The task
    /// completes only after the operation has.
    /// </returns>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="timeout"/> is negative or too long.</exception>
    public static Task<T> WithTimeoutOrDefaultAsync<T>(
        TimeSpan timeout,
        Func<CancellationToken, Task<T>> operation,
        T defaultValue,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(operation);
        Timeouts.Validate(timeout, nameof(timeout));
        return Core();

        async Task<T> Core()
        {
            (T result, OperationCanceledException? timedOut) =
                await WithTimeoutCore(timeout, operation, cancellationToken).ConfigureAwait(false);
            return timedOut is null ? result : defaultValue;
        }
    }

    /// <summary>
    /// Runs <paramref name="operation"/>, and if it hasn't completed within <paramref name="timeout"/>, cancels it,
    /// like Trio's <c>move_on_after</c>.
    /// </summary>
    /// <param name="timeout">How long the operation may run: up to about 49 days, or <see cref="Timeout.InfiniteTimeSpan"/>.</param>
    /// <param name="operation">The operation; it receives a token that is cancelled when the time is up.</param>
    /// <param name="cancellationToken">Cancels the operation.</param>
    /// <returns>
    /// <see langword="true"/> if the operation completed; <see langword="false"/> if it ended because the time was up.
    /// The task completes only after the operation has.
    /// </returns>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="timeout"/> is negative or too long.</exception>
    public static Task<bool> TryWithTimeoutAsync(
        TimeSpan timeout,
        Func<CancellationToken, Task> operation,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(operation);
        return WithTimeoutOrDefaultAsync(timeout, async ct =>
        {
            await Invoke(operation, ct).ConfigureAwait(false);
            return true;
        }, defaultValue: false, cancellationToken);
    }

    // Returns the cancellation that ended the operation when it came from the timer rather than from the caller, so
    // a TimeoutException the operation throws itself (say, from a nested timeout) is never mistaken for this one.
    private static async Task<(T Result, OperationCanceledException? TimedOut)> WithTimeoutCore<T>(
        TimeSpan timeout,
        Func<CancellationToken, Task<T>> operation,
        CancellationToken cancellationToken)
    {
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        cts.CancelAfter(timeout);
        try
        {
            return (await Invoke(operation, cts.Token).ConfigureAwait(false), null);
        }
        catch (OperationCanceledException exception) when (cts.IsCancellationRequested && !cancellationToken.IsCancellationRequested)
        {
            return (default!, exception);
        }
    }
}
