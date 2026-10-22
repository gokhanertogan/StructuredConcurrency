namespace StructuredConcurrency;

/// <summary>The outcome of one operation run by <see cref="TaskGroup.AllSettledAsync{T}(IEnumerable{Func{CancellationToken, Task{T}}}, CancellationToken)"/>.</summary>
/// <typeparam name="T">The type of the operation's result.</typeparam>
public readonly struct TaskOutcome<T>
{
    private readonly T _value;

    private TaskOutcome(TaskStatus status, T value, Exception? exception)
    {
        Status = status;
        _value = value;
        Exception = exception;
    }

    /// <summary>
    /// Gets how the operation ended: <see cref="TaskStatus.RanToCompletion"/>, <see cref="TaskStatus.Faulted"/>
    /// or <see cref="TaskStatus.Canceled"/>.
    /// </summary>
    public TaskStatus Status { get; }

    /// <summary>Gets a value indicating whether the operation succeeded.</summary>
    public bool IsSuccess => Status == TaskStatus.RanToCompletion;

    /// <summary>Gets the operation's result.</summary>
    /// <exception cref="InvalidOperationException">The operation did not succeed.</exception>
    public T Value => IsSuccess
        ? _value
        : throw new InvalidOperationException($"The operation did not succeed (status {Status}).", Exception);

    /// <summary>Gets the exception that ended the operation, or <see langword="null"/> if it succeeded.</summary>
    public Exception? Exception { get; }

    internal static TaskOutcome<T> Success(T value) => new(TaskStatus.RanToCompletion, value, null);

    internal static TaskOutcome<T> Failure(Exception exception) =>
        new(exception is OperationCanceledException ? TaskStatus.Canceled : TaskStatus.Faulted, default!, exception);

    /// <inheritdoc/>
    public override string ToString() => IsSuccess ? $"RanToCompletion: {_value}" : $"{Status}: {Exception!.Message}";
}
