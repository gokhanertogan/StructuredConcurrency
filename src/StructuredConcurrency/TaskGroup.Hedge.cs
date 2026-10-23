namespace StructuredConcurrency;

// Hedged requests, from "The Tail at Scale": when an attempt is slow, start another one and take whichever succeeds
// first. Every attempt is a child of one TaskGroup, so the attempts that lose are cancelled and awaited before the
// call returns.
public sealed partial class TaskGroup
{
    private static readonly TaskGroupOptions s_hedgeOptions = new() { Name = "TaskGroup.HedgeAsync" };

    /// <summary>
    /// Runs <paramref name="operation"/>, and each time <paramref name="delay"/> passes without a result, starts
    /// another attempt, up to <paramref name="maxAttempts"/>. The first attempt to succeed wins and the others are
    /// cancelled.
    /// </summary>
    /// <typeparam name="T">The type of the result.</typeparam>
    /// <param name="delay">
    /// How long to wait for an attempt before starting the next one: up to about 49 days, or
    /// <see cref="Timeout.InfiniteTimeSpan"/> to start the next attempt only when the ones running have failed.
    /// </param>
    /// <param name="operation">The operation; every attempt receives a token that is cancelled once another attempt has won.</param>
    /// <param name="maxAttempts">The maximum number of attempts, including the first; at least 1.</param>
    /// <param name="cancellationToken">Cancels every attempt.</param>
    /// <returns>
    /// The first successful result. The task completes only after every attempt has ended. When every attempt running
    /// has failed, the next one starts at once rather than after the delay.
    /// </returns>
    /// <exception cref="ArgumentOutOfRangeException">
    /// <paramref name="delay"/> is negative or too long, or <paramref name="maxAttempts"/> is less than 1.
    /// </exception>
    /// <exception cref="AggregateException">Every attempt failed; holds the failures in the order the attempts started.</exception>
    public static Task<T> HedgeAsync<T>(
        TimeSpan delay,
        Func<CancellationToken, Task<T>> operation,
        int maxAttempts,
        CancellationToken cancellationToken = default) =>
        HedgeAsync(delay, TimeProvider.System, operation, maxAttempts, cancellationToken);

    /// <summary>
    /// Runs <paramref name="operation"/>, and each time <paramref name="delay"/> passes without a result, as measured
    /// by <paramref name="timeProvider"/>, starts another attempt, up to <paramref name="maxAttempts"/>. The first
    /// attempt to succeed wins and the others are cancelled.
    /// </summary>
    /// <typeparam name="T">The type of the result.</typeparam>
    /// <param name="delay">
    /// How long to wait for an attempt before starting the next one: up to about 49 days, or
    /// <see cref="Timeout.InfiniteTimeSpan"/> to start the next attempt only when the ones running have failed.
    /// </param>
    /// <param name="timeProvider">Measures the delay; pass a fake one in tests to make time pass on demand.</param>
    /// <param name="operation">The operation; every attempt receives a token that is cancelled once another attempt has won.</param>
    /// <param name="maxAttempts">The maximum number of attempts, including the first; at least 1.</param>
    /// <param name="cancellationToken">Cancels every attempt.</param>
    /// <returns>
    /// The first successful result. The task completes only after every attempt has ended. When every attempt running
    /// has failed, the next one starts at once rather than after the delay.
    /// </returns>
    /// <exception cref="ArgumentOutOfRangeException">
    /// <paramref name="delay"/> is negative or too long, or <paramref name="maxAttempts"/> is less than 1.
    /// </exception>
    /// <exception cref="AggregateException">Every attempt failed; holds the failures in the order the attempts started.</exception>
    public static Task<T> HedgeAsync<T>(
        TimeSpan delay,
        TimeProvider timeProvider,
        Func<CancellationToken, Task<T>> operation,
        int maxAttempts,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(operation);
        return HedgeAsync(delay, timeProvider, (_, ct) => operation(ct), maxAttempts, cancellationToken);
    }

    /// <summary>
    /// Runs <paramref name="operation"/>, and each time <paramref name="delay"/> passes without a result, starts
    /// another attempt, up to <paramref name="maxAttempts"/>. The first attempt to succeed wins and the others are
    /// cancelled. Each attempt receives its number, so it can go to a different replica.
    /// </summary>
    /// <typeparam name="T">The type of the result.</typeparam>
    /// <param name="delay">
    /// How long to wait for an attempt before starting the next one: up to about 49 days, or
    /// <see cref="Timeout.InfiniteTimeSpan"/> to start the next attempt only when the ones running have failed.
    /// </param>
    /// <param name="operation">
    /// The operation; it receives the attempt's number, starting at 0, and a token that is cancelled once another
    /// attempt has won.
    /// </param>
    /// <param name="maxAttempts">The maximum number of attempts, including the first; at least 1.</param>
    /// <param name="cancellationToken">Cancels every attempt.</param>
    /// <returns>
    /// The first successful result. The task completes only after every attempt has ended. When every attempt running
    /// has failed, the next one starts at once rather than after the delay.
    /// </returns>
    /// <exception cref="ArgumentOutOfRangeException">
    /// <paramref name="delay"/> is negative or too long, or <paramref name="maxAttempts"/> is less than 1.
    /// </exception>
    /// <exception cref="AggregateException">Every attempt failed; holds the failures in the order the attempts started.</exception>
    public static Task<T> HedgeAsync<T>(
        TimeSpan delay,
        Func<int, CancellationToken, Task<T>> operation,
        int maxAttempts,
        CancellationToken cancellationToken = default) =>
        HedgeAsync(delay, TimeProvider.System, operation, maxAttempts, cancellationToken);

    /// <summary>
    /// Runs <paramref name="operation"/>, and each time <paramref name="delay"/> passes without a result, as measured
    /// by <paramref name="timeProvider"/>, starts another attempt, up to <paramref name="maxAttempts"/>. The first
    /// attempt to succeed wins and the others are cancelled. Each attempt receives its number, so it can go to a
    /// different replica.
    /// </summary>
    /// <typeparam name="T">The type of the result.</typeparam>
    /// <param name="delay">
    /// How long to wait for an attempt before starting the next one: up to about 49 days, or
    /// <see cref="Timeout.InfiniteTimeSpan"/> to start the next attempt only when the ones running have failed.
    /// </param>
    /// <param name="timeProvider">Measures the delay; pass a fake one in tests to make time pass on demand.</param>
    /// <param name="operation">
    /// The operation; it receives the attempt's number, starting at 0, and a token that is cancelled once another
    /// attempt has won.
    /// </param>
    /// <param name="maxAttempts">The maximum number of attempts, including the first; at least 1.</param>
    /// <param name="cancellationToken">Cancels every attempt.</param>
    /// <returns>
    /// The first successful result. The task completes only after every attempt has ended. When every attempt running
    /// has failed, the next one starts at once rather than after the delay.
    /// </returns>
    /// <exception cref="ArgumentOutOfRangeException">
    /// <paramref name="delay"/> is negative or too long, or <paramref name="maxAttempts"/> is less than 1.
    /// </exception>
    /// <exception cref="AggregateException">Every attempt failed; holds the failures in the order the attempts started.</exception>
    public static Task<T> HedgeAsync<T>(
        TimeSpan delay,
        TimeProvider timeProvider,
        Func<int, CancellationToken, Task<T>> operation,
        int maxAttempts,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(timeProvider);
        ArgumentNullException.ThrowIfNull(operation);
        Timeouts.Validate(delay, nameof(delay));
        ArgumentOutOfRangeException.ThrowIfLessThan(maxAttempts, 1);
        return HedgeCore(delay, timeProvider, operation, maxAttempts, cancellationToken);
    }

    private static async Task<T> HedgeCore<T>(
        TimeSpan delay,
        TimeProvider timeProvider,
        Func<int, CancellationToken, Task<T>> operation,
        int maxAttempts,
        CancellationToken cancellationToken)
    {
        var state = new HedgeState<T>(maxAttempts);

        // As in AnyAsync, a failing attempt doesn't fail the group; only the caller's cancellation ends it early.
        await RunAsync(async group =>
        {
            for (int attempt = 0; attempt < maxAttempts; attempt++)
            {
                Task allFailed = state.OnStarting();
                int index = attempt;
                _ = group.Run(async ct =>
                {
                    try
                    {
                        T result = await (operation(index, ct) ?? throw new InvalidOperationException("The operation returned null."))
                            .ConfigureAwait(false);
                        if (Interlocked.CompareExchange(ref state.Won, 1, 0) == 0)
                        {
                            state.Value = result;
                            group.Cancel();
                        }
                    }
                    catch (Exception exception)
                    {
                        state.OnFailed(index, exception);
                    }
                });

                if (index == maxAttempts - 1)
                {
                    break;
                }

                // Wait for the delay, or less if every attempt so far has failed or one has won.
                using (var wait = CancellationTokenSource.CreateLinkedTokenSource(group.CancellationToken))
                {
                    await Task.WhenAny(Task.Delay(delay, timeProvider, wait.Token), allFailed).ConfigureAwait(false);
                    wait.Cancel();
                }
                if (group.CancellationToken.IsCancellationRequested)
                {
                    break;
                }
            }
        }, s_hedgeOptions, cancellationToken).ConfigureAwait(false);

        if (state.Won == 1)
        {
            return state.Value!;
        }
        throw new AggregateException("Every attempt failed.", state.Failures.Where(failure => failure is not null)!);
    }

    private sealed class HedgeState<TResult>(int count)
    {
        public int Won;
        public TResult? Value;
        public readonly Exception?[] Failures = new Exception?[count];

        private int _started;
        private int _failed;
        private TaskCompletionSource? _allFailed;

        // Counts the attempt about to start, and returns a task that completes if it and every earlier one fail.
        public Task OnStarting()
        {
            lock (Failures)
            {
                _started++;
                _allFailed = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
                return _allFailed.Task;
            }
        }

        public void OnFailed(int index, Exception exception)
        {
            TaskCompletionSource? allFailed = null;
            lock (Failures)
            {
                Failures[index] = exception;
                if (++_failed == _started)
                {
                    allFailed = _allFailed;
                }
            }
            allFailed?.TrySetResult();
        }
    }
}
