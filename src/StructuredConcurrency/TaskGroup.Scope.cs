namespace StructuredConcurrency;

// A group owned by a using block instead of a body lambda, like Java's StructuredTaskScope: the owner joins it
// explicitly, and leaving the block without joining (an exception, an early return) cancels what is left.
public sealed partial class TaskGroup : IAsyncDisposable
{
    // Only for a group from Create: stands in for the body, and completes when the owner joins or disposes.
    private TaskCompletionSource? _owner;

    /// <summary>
    /// Creates a group owned by the caller rather than by a body lambda. Start children with
    /// <see cref="Run(Func{CancellationToken, Task})"/>, then call <see cref="JoinAsync"/>; dispose it with
    /// <c>await using</c> so that it is cancelled and awaited however the block is left.
    /// </summary>
    /// <param name="cancellationToken">Cancels the group and all of its children.</param>
    /// <returns>The new group.</returns>
    /// <remarks>
    /// <code>
    /// await using var group = TaskGroup.Create(cancellationToken);
    /// foreach (var url in urls)
    /// {
    ///     group.Run(ct => DownloadAsync(url, ct));
    /// }
    /// await group.JoinAsync();
    /// </code>
    /// Use it where a lambda doesn't fit, such as an async iterator that yields results as children produce them.
    /// </remarks>
    public static TaskGroup Create(CancellationToken cancellationToken = default) =>
        Create(TaskGroupOptions.Default, cancellationToken);

    /// <summary>
    /// Creates a group owned by the caller rather than by a body lambda. Start children with
    /// <see cref="Run(Func{CancellationToken, Task})"/>, then call <see cref="JoinAsync"/>; dispose it with
    /// <c>await using</c> so that it is cancelled and awaited however the block is left.
    /// </summary>
    /// <param name="options">Options that control how the group runs its children and reacts to failures.</param>
    /// <param name="cancellationToken">Cancels the group and all of its children.</param>
    /// <returns>The new group.</returns>
    public static TaskGroup Create(TaskGroupOptions options, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(options);
        var completer = new Completer<VoidResult>();

        // Not an async method, so the group's activity stays current for the caller until it joins or disposes, and
        // the children it starts nest under it.
        var group = new TaskGroup(completer, options, cancellationToken)
        {
            _owner = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously),
        };
        completer.Body = group._owner.Task;
        group.Track(group._owner.Task, isBody: true);
        return group;
    }

    /// <summary>
    /// Ends the owner's part and waits for every child to complete, for a group from <see cref="Create(CancellationToken)"/>.
    /// </summary>
    /// <returns>
    /// A task that faults with every failure in the order they occurred (awaiting it throws the first one), or is
    /// cancelled when the caller's token was cancelled and nothing failed.
    /// </returns>
    /// <exception cref="InvalidOperationException">The group was not created with <see cref="Create(CancellationToken)"/>.</exception>
    public Task JoinAsync()
    {
        TaskCompletionSource owner = Owner();

        // Not an async method, so giving the caller its own activity back flows to the caller.
        LeaveActivity();
        owner.TrySetResult();
        return _completer.Task;
    }

    /// <summary>
    /// For a group from <see cref="Create(CancellationToken)"/>: if it hasn't completed, cancels it, then waits for
    /// every child to complete.
    /// </summary>
    /// <returns>A task that completes when every child has completed. It never throws.</returns>
    /// <remarks>
    /// After <see cref="JoinAsync"/> has completed, this does nothing. Without it, the block was left early, by an
    /// exception or a return: the children are cancelled, and their failures are not reported, so that an exception
    /// already on its way out is not replaced.
    /// </remarks>
    /// <exception cref="InvalidOperationException">The group was not created with <see cref="Create(CancellationToken)"/>.</exception>
    public ValueTask DisposeAsync()
    {
        TaskCompletionSource owner = Owner();
        Task completion = _completer.Task;
        if (completion.IsCompleted)
        {
            // JoinAsync has seen the outcome.
            return default;
        }

        LeaveActivity();
        CancelCore(TaskGroupCancellationReason.Disposed);
        owner.TrySetResult();
        return new ValueTask(completion.ContinueWith(
            static task => _ = task.Exception,   // observed, so that it is not reported as unobserved
            CancellationToken.None,
            TaskContinuationOptions.ExecuteSynchronously,
            TaskScheduler.Default));
    }

    private TaskCompletionSource Owner() =>
        _owner ?? throw new InvalidOperationException("Only a group from TaskGroup.Create is joined or disposed by its owner; RunAsync owns this one.");
}
