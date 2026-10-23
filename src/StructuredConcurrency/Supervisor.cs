namespace StructuredConcurrency;

/// <summary>
/// Restarts work that fails, like an Erlang/OTP supervisor: with a delay that grows on each restart, and only so often
/// within a window of time before it gives up and lets the failure through.
/// </summary>
/// <remarks>
/// Supervise one child of a group to restart only that child (Erlang's <c>one_for_one</c>):
/// <c>group.Run(ct => Supervisor.RunAsync(ConsumeAsync, policy, ct))</c>. Use <see cref="RunGroupAsync"/> to restart a
/// whole group when any of its children fails (<c>one_for_all</c>). Nothing keeps running between runs: the next run
/// starts only after the previous one has ended.
/// </remarks>
public static class Supervisor
{
    /// <summary>Runs <paramref name="work"/>, and restarts it when it fails, as <paramref name="policy"/> allows.</summary>
    /// <param name="work">The work; each run receives <paramref name="cancellationToken"/>.</param>
    /// <param name="policy">How often to restart and how long to wait before each restart.</param>
    /// <param name="cancellationToken">Stops the work, and the waits between runs. It is never restarted after this.</param>
    /// <returns>
    /// A task that completes when a run completes. It fails with the last failure when the policy gives up, and with
    /// a failure that <see cref="RestartPolicy.ShouldRestart"/> turns down at once.
    /// </returns>
    public static Task RunAsync(Func<CancellationToken, Task> work, RestartPolicy policy, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(work);
        return RunAsync(async ct =>
        {
            await (work(ct) ?? throw new InvalidOperationException("The work delegate returned null.")).ConfigureAwait(false);
            return true;
        }, policy, cancellationToken);
    }

    /// <summary>
    /// Runs <paramref name="work"/>, and restarts it when it fails, as <paramref name="policy"/> allows, until a run
    /// returns a result.
    /// </summary>
    /// <typeparam name="T">The type of the result.</typeparam>
    /// <param name="work">The work; each run receives <paramref name="cancellationToken"/>.</param>
    /// <param name="policy">How often to restart and how long to wait before each restart.</param>
    /// <param name="cancellationToken">Stops the work, and the waits between runs. It is never restarted after this.</param>
    /// <returns>
    /// The result of the first run that completes. The task fails with the last failure when the policy gives up, and
    /// with a failure that <see cref="RestartPolicy.ShouldRestart"/> turns down at once.
    /// </returns>
    public static Task<T> RunAsync<T>(Func<CancellationToken, Task<T>> work, RestartPolicy policy, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(work);
        ArgumentNullException.ThrowIfNull(policy);
        return RunCore(work, policy, cancellationToken);
    }

    /// <summary>
    /// Runs <paramref name="body"/> in a new <see cref="TaskGroup"/>, and when the group fails, starts a new group, as
    /// <paramref name="policy"/> allows: Erlang's <c>one_for_all</c>. A failing child cancels its siblings as usual, and
    /// they all start again together.
    /// </summary>
    /// <param name="body">Starts the group's children. It runs again for every new group.</param>
    /// <param name="policy">How often to restart and how long to wait before each restart.</param>
    /// <param name="options">The options for each group, or <see langword="null"/> for the default ones.</param>
    /// <param name="cancellationToken">Cancels the group, and the waits between groups.</param>
    /// <returns>
    /// A task that completes when a group completes. It fails with the group's first failure when the policy gives
    /// up, and with a failure that <see cref="RestartPolicy.ShouldRestart"/> turns down at once.
    /// </returns>
    public static Task RunGroupAsync(
        Func<TaskGroup, Task> body,
        RestartPolicy policy,
        TaskGroupOptions? options = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(body);
        TaskGroupOptions groupOptions = options ?? TaskGroupOptions.Default;
        return RunAsync(ct => TaskGroup.RunAsync(body, groupOptions, ct), policy, cancellationToken);
    }

    private static async Task<T> RunCore<T>(Func<CancellationToken, Task<T>> work, RestartPolicy policy, CancellationToken cancellationToken)
    {
        TimeProvider time = policy.TimeProvider;
        Queue<long>? restarts = null;   // when the restarts still in the window were made
        int total = 0;
        while (true)
        {
            try
            {
                return await (work(cancellationToken) ?? throw new InvalidOperationException("The work delegate returned null."))
                    .ConfigureAwait(false);
            }
            catch (Exception) when (cancellationToken.IsCancellationRequested)
            {
                // Stopped on purpose. Whatever the work threw on the way out, say an IOException from a closed
                // connection, it is not restarted.
                throw;
            }
            catch (Exception exception) when (policy.ShouldRestart?.Invoke(exception) ?? true)
            {
                long now = time.GetTimestamp();
                restarts ??= new Queue<long>();
                while (restarts.Count > 0
                    && policy.Window != Timeout.InfiniteTimeSpan
                    && time.GetElapsedTime(restarts.Peek(), now) >= policy.Window)
                {
                    restarts.Dequeue();
                }
                if (restarts.Count >= policy.MaxRestarts)
                {
                    throw;
                }

                restarts.Enqueue(now);
                TimeSpan delay = policy.DelayBefore(restarts.Count);
                policy.OnRestart?.Invoke(new RestartEvent(exception, ++total, delay));
                if (delay > TimeSpan.Zero)
                {
                    await Task.Delay(delay, time, cancellationToken).ConfigureAwait(false);
                }
            }
        }
    }
}
