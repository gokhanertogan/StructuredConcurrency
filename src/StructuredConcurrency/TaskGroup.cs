using System.Diagnostics;
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

    /// <summary>
    /// The name of the <see cref="ActivitySource"/> that groups record their tracing activities with. Add it to your
    /// OpenTelemetry tracer, for example with <c>AddSource(TaskGroup.ActivitySourceName)</c>.
    /// </summary>
    public const string ActivitySourceName = "StructuredConcurrency";

    private static readonly ActivitySource s_activitySource =
        new(ActivitySourceName, typeof(TaskGroup).Assembly.GetName().Version?.ToString());

    private static readonly Action<Task, object?> s_onChildCompleted =
        static (task, state) => ((TaskGroup)state!).OnCompleted(task, isBody: false);

    private static readonly Action<Task, object?> s_onBodyCompleted =
        static (task, state) => ((TaskGroup)state!).OnCompleted(task, isBody: true);

    private static readonly Action<Task, object?> s_onLimitedChildCompleted =
        static (task, state) =>
        {
            var group = (TaskGroup)state!;
            group.OnCompleted(task, isBody: false);
            if (group.TryTakeNext(out QueuedChild? next))
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

    // Only while tracing, so a group that isn't traced stays small.
    private readonly Trace? _trace;

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

        // Becomes Activity.Current while the body runs (see LeaveActivity), so the children's activities nest under it.
        if (options.EnableTracing)
        {
            Activity? previous = Activity.Current;
            if (s_activitySource.StartActivity(options.Name ?? "TaskGroup") is Activity activity)
            {
                _trace = new Trace(activity, previous);
            }
        }

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
        finally
        {
            group.LeaveActivity();
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
        finally
        {
            group.LeaveActivity();
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

        if (_queue is not null)
        {
            var child = new QueuedChild<VoidResult>(work);
            RunLimited(child);
            return child.Task;
        }

        Task task = Invoke(work, CancellationToken);
        Track(task, isBody: false);
        return task;
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

        if (_queue is not null)
        {
            var child = new QueuedChild<T>(work);
            RunLimited(child);
            return child.TypedTask;
        }

        Task<T> task = Invoke(work, CancellationToken);
        Track(task, isBody: false);
        return task;
    }

    /// <summary>
    /// Cancels the remaining children without counting as a failure. Cancellations that result from it are not
    /// reported.
    /// </summary>
    public void Cancel() => CancelCore("Cancel");

    // The group's activity is current from its start until the body returns; the caller then gets its own back.
    private void LeaveActivity()
    {
        if (_trace is not null)
        {
            Activity.Current = _trace.Previous;
            if (Activity.Current == _trace.Activity)
            {
                // The setter ignores an activity that has stopped since; the group's must not stay current either way.
                Activity.Current = null;
            }
        }
    }

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
                if (_trace is not null)
                {
                    Interlocked.Increment(ref _trace.Children);
                }
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

    // Starts the child if a slot is free, otherwise queues it.
    private void RunLimited(QueuedChild child)
    {
        bool start;
        lock (_queue!)
        {
            start = _running < _maxConcurrency;
            if (start)
            {
                _running++;
            }
            else
            {
                _queue.Enqueue(child);
            }
        }

        if (start)
        {
            Pump(child);
        }
        else if (CancellationToken.IsCancellationRequested)
        {
            // The group was cancelled after TryAddChild, and its DrainQueue may have run before the child was queued.
            DrainQueue();
        }
    }

    // Called with a concurrency slot held for `child`. When a child completes synchronously, the next queued child
    // starts in this loop rather than recursively, so a long queue of synchronous work can't overflow the stack.
    private void Pump(QueuedChild first)
    {
        QueuedChild? child = first;
        do
        {
            if (CancellationToken.IsCancellationRequested)
            {
                child.Cancel(CancellationToken);
                OnCompleted(child.Task, isBody: false);
                continue;
            }

            Task task = child.Start(CancellationToken);
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
        }
        while (TryTakeNext(out child));
    }

    // Hands the caller's slot to the next queued child, or frees the slot when the queue is empty.
    private bool TryTakeNext([NotNullWhen(true)] out QueuedChild? next)
    {
        lock (_queue!)
        {
            if (_queue.TryDequeue(out next))
            {
                return true;
            }
            _running--;
            return false;
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
            CancelCore("failure");
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

    private void CancelCore(string reason)
    {
        if (Interlocked.CompareExchange(ref _state, StateCancelling, StateOpen) != StateOpen)
        {
            return;
        }
        if (_trace is not null)
        {
            // The caller's token cancels the group without coming through here; if it already has, it was first.
            _trace.CancelledBy = CancellationToken.IsCancellationRequested ? "caller" : reason;
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
        Exception[]? snapshot = null;
        if (failures is not null)
        {
            lock (failures)
            {
                snapshot = failures.ToArray();
            }
        }

        // Read once, so the activity records the same outcome the task ends with.
        bool callerCancelled = _callerToken.IsCancellationRequested;

        // Before the result is published, so the activity has ended by the time the caller continues.
        if (_trace is not null)
        {
            StopActivity(_trace, snapshot, callerCancelled);
        }

        if (snapshot is not null)
        {
            _completer.TrySetException(snapshot);
        }
        else if (callerCancelled)
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

    private void StopActivity(Trace trace, Exception[]? failures, bool callerCancelled)
    {
        Activity activity = trace.Activity;
        activity.SetTag("taskgroup.children", Volatile.Read(ref trace.Children));
        string? cancelledBy = trace.CancelledBy ?? (callerCancelled ? "caller" : null);
        if (cancelledBy is not null)
        {
            activity.SetTag("taskgroup.cancelled_by", cancelledBy);
        }

        if (failures is not null)
        {
            activity.SetTag("taskgroup.outcome", "failed");
            activity.SetStatus(ActivityStatusCode.Error, failures[0].Message);
            foreach (Exception failure in failures)
            {
                // The OpenTelemetry semantic conventions for a recorded exception.
                activity.AddEvent(new ActivityEvent("exception", tags: new ActivityTagsCollection
                {
                    ["exception.type"] = failure.GetType().FullName,
                    ["exception.message"] = failure.Message,
                    ["exception.stacktrace"] = failure.ToString(),
                }));
            }
        }
        else
        {
            activity.SetTag("taskgroup.outcome", callerCancelled || _bodyCanceled ? "cancelled" : "success");
        }

        // Stop() may reset Activity.Current; the thread completing the group keeps whatever it had, except the group's
        // own activity, which it can still have in a child's continuation registered while the body ran.
        Activity? current = Activity.Current;
        activity.Stop();
        Activity.Current = current == activity ? activity.Parent : current;
    }

    // The group's activity, the caller's current activity to give back, the number of children added with Run
    // (including queued ones that never started), and why the group was cancelled.
    private sealed class Trace(Activity activity, Activity? previous)
    {
        public readonly Activity Activity = activity;
        public readonly Activity? Previous = previous;
        public int Children;
        public string? CancelledBy;
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

    // A child of a group with a concurrency limit. It may wait in the queue and start later from another child's
    // completion, so it keeps the ExecutionContext Run was called in. Run returns a task that mirrors the work once it
    // has run. It doesn't go back to the caller's SynchronizationContext: if that context were blocked waiting for the
    // group, the child could never start.
    private abstract class QueuedChild
    {
        public abstract Task Task { get; }

        // Invokes the work and returns the task for the group to track.
        public abstract Task Start(CancellationToken cancellationToken);

        public abstract void Cancel(CancellationToken cancellationToken);
    }

    // T is VoidResult for work that returns a plain Task.
    private sealed class QueuedChild<T>(Func<CancellationToken, Task> work) : QueuedChild
    {
        private readonly ExecutionContext? _executionContext = ExecutionContext.Capture();

        // Continuations run asynchronously, so code awaiting the child never runs on the thread that holds its slot.
        private readonly TaskCompletionSource<T> _mirror = new(TaskCreationOptions.RunContinuationsAsynchronously);

        private CancellationToken _cancellationToken;
        private Task? _started;

        public override Task Task => _mirror.Task;

        public Task<T> TypedTask => _mirror.Task;

        public override Task Start(CancellationToken cancellationToken)
        {
            _cancellationToken = cancellationToken;
            if (_executionContext is null)
            {
                return InvokeAndMirror();
            }

            ExecutionContext.Run(_executionContext, static state => ((QueuedChild<T>)state!).InvokeAndMirror(), this);
            return _started!;
        }

        public override void Cancel(CancellationToken cancellationToken) => _mirror.TrySetCanceled(cancellationToken);

        private Task InvokeAndMirror()
        {
            Task task = Invoke(work, _cancellationToken);
            _started = task;
            if (task.IsCompleted)
            {
                Mirror(task);
            }
            else
            {
                task.ContinueWith(
                    static (task, state) => ((QueuedChild<T>)state!).Mirror(task),
                    this,
                    CancellationToken.None,
                    TaskContinuationOptions.ExecuteSynchronously,
                    TaskScheduler.Default);
            }
            return task;
        }

        private void Mirror(Task task)
        {
            if (task.IsCompletedSuccessfully)
            {
                _mirror.TrySetResult(task is Task<T> typed ? typed.Result : default!);
            }
            else if (task.IsCanceled)
            {
                _mirror.TrySetCanceled(CancellationTokenOf(task));
            }
            else
            {
                _mirror.TrySetException(task.Exception!.InnerExceptions);

                // The group reports the failure, so a caller that doesn't await the child must not see it as unobserved.
                _ = _mirror.Task.Exception;
            }
        }
    }

    internal static CancellationToken CancellationTokenOf(Task canceledTask)
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
