namespace StructuredConcurrency;

/// <summary>Options that control how a <see cref="TaskGroup"/> reacts to failures.</summary>
public sealed class TaskGroupOptions
{
    /// <summary>Gets the default options: the first failure cancels the remaining children.</summary>
    public static TaskGroupOptions Default { get; } = new();

    /// <summary>
    /// Gets a value indicating whether a failing child cancels its siblings. When <see langword="false"/>, the group
    /// supervises its children: every child runs to completion and all failures are reported at the end. A failing
    /// body cancels the children either way.
    /// </summary>
    public bool CancelOnFailure { get; init; } = true;
}
