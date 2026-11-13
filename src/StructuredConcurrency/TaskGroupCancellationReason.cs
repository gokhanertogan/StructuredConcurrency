namespace StructuredConcurrency;

/// <summary>Why a <see cref="TaskGroup"/> cancelled its children. See <see cref="TaskGroup.CancellationReason"/>.</summary>
/// <remarks>
/// When several things happen at once, the first one to cancel the group is the reason. A child that catches its
/// <see cref="OperationCanceledException"/> can read it to log why it stopped, or to skip work that only makes sense
/// on a particular kind of shutdown.
/// </remarks>
public enum TaskGroupCancellationReason
{
    /// <summary>The group has not been cancelled.</summary>
    None,

    /// <summary>
    /// A child or the body failed. The failure is thrown by the group; the children only see the cancellation.
    /// </summary>
    Failure,

    /// <summary>The group ran longer than <see cref="TaskGroupOptions.Timeout"/>.</summary>
    Timeout,

    /// <summary><see cref="TaskGroup.Cancel"/> was called, for example once a result was found.</summary>
    Requested,

    /// <summary>The <see cref="CancellationToken"/> passed when the group was created was cancelled.</summary>
    CallerToken,

    /// <summary>
    /// A group from <see cref="TaskGroup.Create(CancellationToken)"/> was disposed without being joined, because its
    /// owner left the <c>await using</c> block early.
    /// </summary>
    Disposed,
}
