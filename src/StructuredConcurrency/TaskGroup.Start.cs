namespace StructuredConcurrency;

// Starting a child and waiting until it is ready, like Trio's nursery.start().
public sealed partial class TaskGroup
{
    /// <summary>
    /// Starts a child and waits until it signals that it is ready, for example a server that has started listening.
    /// The child keeps running in the group afterwards.
    /// </summary>
    /// <param name="work">The child's work. It calls the action it receives once it is ready.</param>
    /// <returns>
    /// A task that completes when the child signals that it is ready. It fails with the child's exception if the child
    /// fails first, is cancelled if the child is cancelled first, and throws <see cref="InvalidOperationException"/>
    /// if the child completes without signalling.
    /// </returns>
    /// <exception cref="InvalidOperationException">The group has already completed.</exception>
    public Task StartAsync(Func<Action, CancellationToken, Task> work)
    {
        ArgumentNullException.ThrowIfNull(work);
        return StartAsync<VoidResult>((ready, ct) => work(() => ready(default), ct));
    }

    /// <summary>
    /// Starts a child and waits until it signals that it is ready, returning the value it passes, for example the port
    /// a server is listening on. The child keeps running in the group afterwards.
    /// </summary>
    /// <typeparam name="T">The type of the value the child passes when it is ready.</typeparam>
    /// <param name="work">The child's work. It calls the action it receives, once, when it is ready.</param>
    /// <returns>
    /// The value the child passes when it signals that it is ready. The task fails with the child's exception if the
    /// child fails first, is cancelled if the child is cancelled first, and throws
    /// <see cref="InvalidOperationException"/> if the child completes without signalling.
    /// </returns>
    /// <exception cref="InvalidOperationException">The group has already completed.</exception>
    public Task<T> StartAsync<T>(Func<Action<T>, CancellationToken, Task> work)
    {
        ArgumentNullException.ThrowIfNull(work);

        var ready = new TaskCompletionSource<T>(TaskCreationOptions.RunContinuationsAsynchronously);
        int signalled = 0;
        Task child = Run(ct => work(value =>
        {
            if (Interlocked.Exchange(ref signalled, 1) != 0)
            {
                throw new InvalidOperationException("The work has already signalled that it is ready.");
            }
            ready.TrySetResult(value);
        }, ct));

        child.ContinueWith(
            static (child, state) =>
            {
                var ready = (TaskCompletionSource<T>)state!;
                if (child.IsCompletedSuccessfully)
                {
                    ready.TrySetException(new InvalidOperationException("The work completed without signalling that it is ready."));
                }
                else if (child.IsCanceled)
                {
                    ready.TrySetCanceled(CancellationTokenOf(child));
                }
                else if (ready.TrySetException(child.Exception!.InnerExceptions))
                {
                    // The group reports the child's failure, so a caller that doesn't await this must not see it as unobserved.
                    _ = ready.Task.Exception;
                }
            },
            ready,
            CancellationToken.None,
            TaskContinuationOptions.ExecuteSynchronously,
            TaskScheduler.Default);

        return ready.Task;
    }
}
