using System.Diagnostics.CodeAnalysis;

namespace StructuredConcurrency;

/// <summary>
/// A scope for concurrent child tasks. The scope completes only after its body and every child have completed,
/// and the first failure cancels the remaining children.
/// </summary>
/// <remarks>
/// A group is created and owned by <see cref="RunAsync(Func{TaskGroup, Task}, CancellationToken)"/> and its overloads;
/// the body uses <see cref="Run(Func{CancellationToken, Task})"/> to start children.
/// </remarks>
public sealed partial class TaskGroup
{
    private const int StateOpen = 0;
    private const int StateCancelling = 1;
    private const int StateCompleted = 2;

    private static readonly Action<Task, object?> s_onChildCompleted =
        static (task, state) => ((TaskGroup)state!).OnCompleted(task, isBody: false);

    private static readonly Action<Task, object?> s_onBodyCompleted =
        static (task, state) => ((TaskGroup)state!).OnCompleted(task, isBody: true);

    private static readonly Action<Task, object?> s_onLimitedChildCompleted =
        static (task, state) =>
        {
            var group = (TaskGroup)state!;
            group.OnCompleted(task, isBody: false);
            if (group.TryTakeNext(out Task? next))
            {
                group.Pump(next);
            }
        };

    private readonly ICompleter _completer;
    private readonly CancellationTokenSource _cts;
    private readonly CancellationToken _callerToken;
    private readonly bool _cancelOnFailure;

    // Running children plus the body; the group completes on the transition to zero.
    private int _outstanding = 1;
    private int _state;
    private bool _bodyCanceled;
    private List<Exception>? _failures;

    // Only used with a concurrency limit: children waiting for a slot, guarded by locking the queue itself.
    private readonly int _maxConcurrency;
    private readonly Queue<QueuedChild>? _queue;
    private int _running;

    private TaskGroup(ICompleter completer, TaskGroupOptions options, CancellationToken callerToken)
    {
        _completer = completer;
        _cancelOnFailure = options.CancelOnFailure;
        _callerToken = callerToken;
        _cts = callerToken.CanBeCanceled
            ? CancellationTokenSource.CreateLinkedTokenSource(callerToken)
            : new CancellationTokenSource();
        CancellationToken = _cts.Token;

        if (options.MaxConcurrency is int maxConcurrency)
        {
            _maxConcurrency = maxConcurrency;
            _queue = new Queue<QueuedChild>();

            // However the group gets cancelled, queued work is dropped at once rather than when a slot frees up.
            CancellationToken.Register(static state => ((TaskGroup)state!).DrainQueue(), this);
        }
    }

    /// <summary>
    /// Gets the token passed to every child. It is cancelled when the group fails, when the caller's token is
    /// cancelled, or when <see cref="Cancel"/> is called.
    /// </summary>
    public CancellationToken CancellationToken { get; }

    /// <summary>Runs <paramref name="body"/> in a new group and completes after the body and all children complete.</summary>
    /// <param name="body">Starts the group's children.</param>
    /// <param name="cancellationToken">Cancels the group and all of its children.</param>
    /// <returns>
    /// A task that faults with every failure in the order they occurred (awaiting it throws the first one),
    /// or is cancelled when the caller's token was cancelled and nothing failed.
    /// </returns>
    public static Task RunAsync(Func<TaskGroup, Task> body, CancellationToken cancellationToken = default) =>
        RunAsync(body, TaskGroupOptions.Default, cancellationToken);

    /// <summary>Runs <paramref name="body"/> in a new group and completes after the body and all children complete.</summary>
    /// <param name="body">Starts the group's children.</param>
    /// <param name="options">Options that control how the group runs its children and reacts to failures.</param>
    /// <param name="cancellationToken">Cancels the group and all of its children.</param>
    /// <returns>
    /// A task that faults with every failure in the order they occurred (awaiting it throws the first one),
    /// or is cancelled when the caller's token was cancelled and nothing failed.
    /// </returns>
    public static Task RunAsync(Func<TaskGroup, Task> body, TaskGroupOptions options, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(body);
        ArgumentNullException.ThrowIfNull(options);
        if (cancellationToken.IsCancellationRequested)
        {
            return Task.FromCanceled(cancellationToken);
        }

        var completer = new Completer<VoidResult>();
        var group = new TaskGroup(completer, options, cancellationToken);

        Task task;
        try
        {
            task = body(group) ?? throw new InvalidOperationException("The task group body returned null.");
        }
        catch (Exception exception)
        {
            task = Task.FromException(exception);
        }

        completer.Body = task;
        group.Track(task, isBody: true);
        return completer.Task;
    }

    /// <summary>
    /// Runs <paramref name="body"/> in a new group and returns its result after the body and all children complete.
    /// </summary>
    /// <typeparam name="TResult">The type of the body's result.</typeparam>
    /// <param name="body">Starts the group's children and produces the result.</param>
    /// <param name="cancellationToken">Cancels the group and all of its children.</param>
    /// <returns>
    /// A task with the body's result; it faults with every failure in the order they occurred (awaiting it throws
    /// the first one), or is cancelled when the caller's token was cancelled and nothing failed.
    /// </returns>
    public static Task<TResult> RunAsync<TResult>(Func<TaskGroup, Task<TResult>> body, CancellationToken cancellationToken = default) =>
        RunAsync(body, TaskGroupOptions.Default, cancellationToken);

    /// <summary>
    /// Runs <paramref name="body"/> in a new group and returns its result after the body and all children complete.
    /// </summary>
    /// <typeparam name="TResult">The type of the body's result.</typeparam>
    /// <param name="body">Starts the group's children and produces the result.</param>
    /// <param name="options">Options that control how the group runs its children and reacts to failures.</param>
    /// <param name="cancellationToken">Cancels the group and all of its children.</param>
    /// <returns>
    /// A task with the body's result; it faults with every failure in the order they occurred (awaiting it throws
    /// the first one), or is cancelled when the caller's token was cancelled and nothing failed.
    /// </returns>
    public static Task<TResult> RunAsync<TResult>(Func<TaskGroup, Task<TResult>> body, TaskGroupOptions options, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(body);
        ArgumentNullException.ThrowIfNull(options);
        if (cancellationToken.IsCancellationRequested)
        {
            return Task.FromCanceled<TResult>(cancellationToken);
        }

        var completer = new Completer<TResult>();
        var group = new TaskGroup(completer, options, cancellationToken);

        Task<TResult> task;
        try
        {
            task = body(group) ?? throw new InvalidOperationException("The task group body returned null.");
        }
        catch (Exception exception)
        {
            task = Task.FromException<TResult>(exception);
        }

        completer.Body = task;
        group.Track(task, isBody: true);
        return completer.Task;
    }

    /// <summary>Starts a child that the group observes and waits for.</summary>
    /// <param name="work">The child's work; it receives <see cref="CancellationToken"/>.</param>
    /// <returns>
    /// The child's task. If the group is already cancelled, <paramref name="work"/> is not invoked and a cancelled
    /// task is returned. With a concurrency limit, work beyond the limit is queued and the task completes after it runs.
    /// </returns>
    /// <exception cref="InvalidOperationException">The group has already completed.</exception>
    public Task Run(Func<CancellationToken, Task> work)
    {
        ArgumentNullException.ThrowIfNull(work);
        if (!TryAddChild())
        {
            return Task.FromCanceled(CancellationToken);
        }

        if (_queue is null)
        {
            Task task = Invoke(work, CancellationToken);
            Track(task, isBody: false);
            return task;
        }

        QueuedVoidChild? queued = null;
        lock (_queue)
        {
            if (_running < _maxConcurrency)
            {
                _running++;
            }
            else
            {
                queued = new QueuedVoidChild(work);
                _queue.Enqueue(queued);
            }
        }
        if (queued is not null)
        {
            return queued.Task;
        }

        Task started = Invoke(work, CancellationToken);
        Pump(started);
        return started;
    }

    /// <summary>Starts a child that the group observes and waits for.</summary>
    /// <typeparam name="T">The type of the child's result.</typeparam>
    /// <param name="work">The child's work; it receives <see cref="CancellationToken"/>.</param>
    /// <returns>
    /// The child's task. If the group is already cancelled, <paramref name="work"/> is not invoked and a cancelled
    /// task is returned. With a concurrency limit, work beyond the limit is queued and the task completes after it runs.
    /// </returns>
    /// <exception cref="InvalidOperationException">The group has already completed.</exception>
    public Task<T> Run<T>(Func<CancellationToken, Task<T>> work)
    {
        ArgumentNullException.ThrowIfNull(work);
        if (!TryAddChild())
        {
            return Task.FromCanceled<T>(CancellationToken);
        }

        if (_queue is null)
        {
            Task<T> task = Invoke(work, CancellationToken);
            Track(task, isBody: false);
            return task;
        }

        QueuedChild<T>? queued = null;
        lock (_queue)
        {
            if (_running < _maxConcurrency)
            {
                _running++;
            }
            else
            {
                queued = new QueuedChild<T>(work);
                _queue.Enqueue(queued);
            }
        }
        if (queued is not null)
        {
            return queued.TypedTask;
        }

        Task<T> started = Invoke(work, CancellationToken);
        Pump(started);
        return started;
    }

    /// <summary>
    /// Cancels the remaining children without counting as a failure. Cancellations that result from it are not
    /// reported.
    /// </summary>
    public void Cancel() => CancelCore();

    private static Task Invoke(Func<CancellationToken, Task> work, CancellationToken cancellationToken)
    {
        try
        {
            return work(cancellationToken) ?? throw new InvalidOperationException("The work delegate returned null.");
        }
        catch (Exception exception)
        {
            return Task.FromException(exception);
        }
    }

    private static Task<T> Invoke<T>(Func<CancellationToken, Task<T>> work, CancellationToken cancellationToken)
    {
        try
        {
            return work(cancellationToken) ?? throw new InvalidOperationException("The work delegate returned null.");
        }
        catch (Exception exception)
        {
            return Task.FromException<T>(exception);
        }
    }

    private bool TryAddChild()
    {
        int count = Volatile.Read(ref _outstanding);
        while (true)
        {
            if (count == 0)
            {
                throw new InvalidOperationException("The task group has already completed.");
            }
            if (CancellationToken.IsCancellationRequested)
            {
                return false;
            }

            int seen = Interlocked.CompareExchange(ref _outstanding, count + 1, count);
            if (seen == count)
            {
                return true;
            }
            count = seen;
        }
    }

    private void Track(Task task, bool isBody)
    {
        if (task.IsCompleted)
        {
            OnCompleted(task, isBody);
        }
        else
        {
            task.ContinueWith(
                isBody ? s_onBodyCompleted : s_onChildCompleted,
                this,
                CancellationToken.None,
                TaskContinuationOptions.ExecuteSynchronously,
                TaskScheduler.Default);
        }
    }

    // Called with a concurrency slot held by `task`. When a child completes synchronously, the next queued child
    // starts in this loop rather than recursively, so a long queue of synchronous work can't overflow the stack.
    private void Pump(Task task)
    {
        while (true)
        {
            if (!task.IsCompleted)
            {
                task.ContinueWith(
                    s_onLimitedChildCompleted,
                    this,
                    CancellationToken.None,
                    TaskContinuationOptions.ExecuteSynchronously,
                    TaskScheduler.Default);
                return;
            }

            OnCompleted(task, isBody: false);
            if (!TryTakeNext(out Task? next))
            {
                return;
            }
            task = next;
        }
    }

    // Hands the caller's slot to the next queued child and starts it, or frees the slot when the queue is empty.
    private bool TryTakeNext([NotNullWhen(true)] out Task? next)
    {
        while (true)
        {
            QueuedChild item;
            lock (_queue!)
            {
                if (_queue.Count == 0)
                {
                    _running--;
                    next = null;
                    return false;
                }
                item = _queue.Dequeue();
            }

            if (CancellationToken.IsCancellationRequested)
            {
                item.Cancel(CancellationToken);
                OnCompleted(item.Task, isBody: false);
                continue;
            }

            next = item.Start(CancellationToken);
            return true;
        }
    }

    private void DrainQueue()
    {
        while (true)
        {
            QueuedChild item;
            lock (_queue!)
            {
                if (_queue.Count == 0)
                {
                    return;
                }
                item = _queue.Dequeue();
            }

            item.Cancel(CancellationToken);
            OnCompleted(item.Task, isBody: false);
        }
    }

    private void OnCompleted(Task task, bool isBody)
    {
        if (!task.IsCompletedSuccessfully)
        {
            if (IsCancellationByGroup(task))
            {
                if (isBody)
                {
                    _bodyCanceled = true;
                }
            }
            else
            {
                RecordFailure(task, isBody);
            }
        }

        if (Interlocked.Decrement(ref _outstanding) == 0)
        {
            Complete();
        }
    }

    // An OperationCanceledException that arrives after the group was cancelled is noise the group caused;
    // before that, it is a real failure (e.g. an HttpClient timeout or the child's own token).
    private bool IsCancellationByGroup(Task task)
    {
        if (!CancellationToken.IsCancellationRequested)
        {
            return false;
        }
        if (task.IsCanceled)
        {
            return true;
        }

        foreach (Exception exception in task.Exception!.InnerExceptions)
        {
            if (exception is not OperationCanceledException)
            {
                return false;
            }
        }
        return true;
    }

    private void RecordFailure(Task task, bool isBody)
    {
        if (task.IsCanceled)
        {
            // A cancellation the group didn't ask for: rethrow it to get the exception to report.
            try
            {
                task.GetAwaiter().GetResult();
            }
            catch (OperationCanceledException exception)
            {
                AddFailure(exception);
            }
        }
        else
        {
            foreach (Exception exception in task.Exception!.InnerExceptions)
            {
                AddFailure(exception);
            }
        }

        // The body failing always ends the group; a child failing does unless the group supervises its children.
        if (_cancelOnFailure || isBody)
        {
            CancelCore();
        }
    }

    private void AddFailure(Exception exception)
    {
        List<Exception> failures = _failures ?? Interlocked.CompareExchange(ref _failures, new List<Exception>(), null) ?? _failures!;
        lock (failures)
        {
            // The body often rethrows a child's exception after awaiting it; report it once.
            foreach (Exception existing in failures)
            {
                if (ReferenceEquals(existing, exception))
                {
                    return;
                }
            }
            failures.Add(exception);
        }
    }

    private void CancelCore()
    {
        if (Interlocked.CompareExchange(ref _state, StateCancelling, StateOpen) != StateOpen)
        {
            return;
        }

        try
        {
            _cts.Cancel();
        }
        catch (AggregateException exception)
        {
            // Callbacks registered on the group's token threw; report them rather than letting them escape a child.
            foreach (Exception inner in exception.InnerExceptions)
            {
                AddFailure(inner);
            }
        }
        catch (ObjectDisposedException)
        {
            // Cancel() was called on a group that completed at the same time.
        }
    }

    private void Complete()
    {
        Volatile.Write(ref _state, StateCompleted);
        _cts.Dispose();

        List<Exception>? failures = _failures;
        if (failures is not null)
        {
            Exception[] snapshot;
            lock (failures)
            {
                snapshot = failures.ToArray();
            }
            _completer.TrySetException(snapshot);
        }
        else if (_callerToken.IsCancellationRequested)
        {
            _completer.TrySetCanceled(_callerToken);
        }
        else if (_bodyCanceled)
        {
            _completer.TrySetCanceled(CancellationToken);
        }
        else
        {
            _completer.SetSuccess();
        }
    }

    private interface ICompleter
    {
        void SetSuccess();

        bool TrySetException(IEnumerable<Exception> exceptions);

        bool TrySetCanceled(CancellationToken cancellationToken);
    }

    // The task returned by RunAsync, and the knowledge of how to produce its result from the body.
    private sealed class Completer<TResult> : TaskCompletionSource<TResult>, ICompleter
    {
        public Completer() : base(TaskCreationOptions.RunContinuationsAsynchronously)
        {
        }

        public Task? Body { get; set; }

        public void SetSuccess() => TrySetResult(Body is Task<TResult> body ? body.Result : default!);
    }

    private readonly struct VoidResult
    {
    }

    // Work waiting for a concurrency slot. Run returns Task, which mirrors the work once it has started and completed.
    private abstract class QueuedChild
    {
        public abstract Task Task { get; }

        // Invokes the work and returns its task, which the group tracks like any other child.
        public abstract Task Start(CancellationToken cancellationToken);

        public abstract void Cancel(CancellationToken cancellationToken);
    }

    private sealed class QueuedVoidChild(Func<CancellationToken, Task> work) : QueuedChild
    {
        private readonly TaskCompletionSource _mirror = new();

        public override Task Task => _mirror.Task;

        public override Task Start(CancellationToken cancellationToken)
        {
            Task task = Invoke(work, cancellationToken);
            if (task.IsCompleted)
            {
                Mirror(task);
            }
            else
            {
                task.ContinueWith(
                    static (task, state) => ((QueuedVoidChild)state!).Mirror(task),
                    this,
                    CancellationToken.None,
                    TaskContinuationOptions.ExecuteSynchronously,
                    TaskScheduler.Default);
            }
            return task;
        }

        public override void Cancel(CancellationToken cancellationToken) => _mirror.TrySetCanceled(cancellationToken);

        private void Mirror(Task task)
        {
            if (task.IsCompletedSuccessfully)
            {
                _mirror.TrySetResult();
            }
            else if (task.IsCanceled)
            {
                _mirror.TrySetCanceled(CancellationTokenOf(task));
            }
            else
            {
                _mirror.TrySetException(task.Exception!.InnerExceptions);
            }
        }
    }

    private sealed class QueuedChild<T>(Func<CancellationToken, Task<T>> work) : QueuedChild
    {
        private readonly TaskCompletionSource<T> _mirror = new();

        public override Task Task => _mirror.Task;

        public Task<T> TypedTask => _mirror.Task;

        public override Task Start(CancellationToken cancellationToken)
        {
            Task<T> task = Invoke(work, cancellationToken);
            if (task.IsCompleted)
            {
                Mirror(task);
            }
            else
            {
                task.ContinueWith(
                    static (task, state) => ((QueuedChild<T>)state!).Mirror((Task<T>)task),
                    this,
                    CancellationToken.None,
                    TaskContinuationOptions.ExecuteSynchronously,
                    TaskScheduler.Default);
            }
            return task;
        }

        public override void Cancel(CancellationToken cancellationToken) => _mirror.TrySetCanceled(cancellationToken);

        private void Mirror(Task<T> task)
        {
            if (task.IsCompletedSuccessfully)
            {
                _mirror.TrySetResult(task.Result);
            }
            else if (task.IsCanceled)
            {
                _mirror.TrySetCanceled(CancellationTokenOf(task));
            }
            else
            {
                _mirror.TrySetException(task.Exception!.InnerExceptions);
            }
        }
    }

    private static CancellationToken CancellationTokenOf(Task canceledTask)
    {
        try
        {
            canceledTask.GetAwaiter().GetResult();
        }
        catch (OperationCanceledException exception)
        {
            return exception.CancellationToken;
        }
        return CancellationToken.None;
    }
}
