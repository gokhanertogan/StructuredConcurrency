using System.Collections.Concurrent;
using System.Diagnostics;
using StructuredConcurrency.Hosting;

namespace StructuredConcurrency.Tests;

// Tests run in parallel and every group records an activity while a listener exists, so each test gives its group a
// unique name and looks only at activities with that name.
public sealed class TracingTests : IDisposable
{
    private static readonly ActivitySource s_work = new("StructuredConcurrency.Tests.Work");

    private readonly ConcurrentQueue<Activity> _stopped = new();
    private readonly ActivityListener _listener;

    public TracingTests()
    {
        _listener = new ActivityListener
        {
            ShouldListenTo = source => source.Name is TaskGroup.ActivitySourceName or "StructuredConcurrency.Tests.Work",
            Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded,
            ActivityStopped = _stopped.Enqueue,
        };
        ActivitySource.AddActivityListener(_listener);
    }

    public void Dispose() => _listener.Dispose();

    private sealed class Boom() : Exception("boom");

    private static TaskGroupOptions Named(out string name, int? maxConcurrency = null)
    {
        name = "group-" + Guid.NewGuid().ToString("N");
        return new TaskGroupOptions { Name = name, MaxConcurrency = maxConcurrency };
    }

    private Activity Single(string displayName) => Assert.Single(_stopped, activity => activity.DisplayName == displayName);

    [Fact]
    public async Task Group_RecordsAnActivity_WithItsChildrenAndOutcome()
    {
        await TaskGroup.RunAsync(group =>
        {
            group.Run(_ => Task.CompletedTask);
            group.Run(async _ => await Task.Yield());
            return Task.CompletedTask;
        }, Named(out string name));

        Activity activity = Single(name);
        Assert.Equal(TaskGroup.ActivitySourceName, activity.Source.Name);
        Assert.Equal(2, activity.GetTagItem("taskgroup.children"));
        Assert.Equal("success", activity.GetTagItem("taskgroup.outcome"));
        Assert.Equal(ActivityStatusCode.Unset, activity.Status);
        Assert.Null(activity.GetTagItem("taskgroup.cancelled_by"));
    }

    [Fact]
    public async Task Failure_IsRecordedAsAnErrorWithTheException()
    {
        TaskGroupOptions options = Named(out string name);
        await Assert.ThrowsAsync<Boom>(() => TaskGroup.RunAsync(group =>
        {
            group.Run(ct => Task.Delay(Timeout.Infinite, ct));
            group.Run(_ => Task.FromException(new Boom()));
            return Task.CompletedTask;
        }, options));

        Activity activity = Single(name);
        Assert.Equal(ActivityStatusCode.Error, activity.Status);
        Assert.Equal("boom", activity.StatusDescription);
        Assert.Equal("failed", activity.GetTagItem("taskgroup.outcome"));
        Assert.Equal("failure", activity.GetTagItem("taskgroup.cancelled_by"));
        ActivityEvent exception = Assert.Single(activity.Events);
        Assert.Equal("exception", exception.Name);
        Assert.Equal(typeof(Boom).FullName, exception.Tags.Single(tag => tag.Key == "exception.type").Value);
    }

    [Fact]
    public async Task Cancel_And_CallerCancellation_AreRecorded()
    {
        await TaskGroup.RunAsync(group =>
        {
            group.Run(ct => Task.Delay(Timeout.Infinite, ct));
            group.Cancel();
            return Task.CompletedTask;
        }, Named(out string cancelled));

        using var cts = new CancellationTokenSource();
        Task run = TaskGroup.RunAsync(group =>
        {
            group.Run(ct => Task.Delay(Timeout.Infinite, ct));
            return Task.CompletedTask;
        }, Named(out string byCaller), cts.Token);
        cts.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => run);

        Assert.Equal("Cancel", Single(cancelled).GetTagItem("taskgroup.cancelled_by"));
        Activity callerActivity = Single(byCaller);
        Assert.Equal("caller", callerActivity.GetTagItem("taskgroup.cancelled_by"));
        Assert.Equal("cancelled", callerActivity.GetTagItem("taskgroup.outcome"));
    }

    [Fact]
    public async Task ChildActivities_NestUnderTheGroup_IncludingQueuedChildren()
    {
        var gate = new TaskCompletionSource();
        string childName = "child-" + Guid.NewGuid().ToString("N");

        Task run = TaskGroup.RunAsync(group =>
        {
            for (int i = 0; i < 3; i++)
            {
                group.Run(async _ =>
                {
                    using Activity? child = s_work.StartActivity(childName);
                    await gate.Task;
                });
            }
            return Task.CompletedTask;
        }, Named(out string name, maxConcurrency: 1));

        await Task.Run(gate.SetResult);   // the queued children start from a thread pool thread
        await run;

        Activity group = Single(name);
        Activity[] children = _stopped.Where(activity => activity.DisplayName == childName).ToArray();
        Assert.Equal(3, children.Length);
        Assert.All(children, child => Assert.Equal(group.SpanId, child.ParentSpanId));
    }

    [Fact]
    public async Task CallersCurrentActivity_IsRestoredWhenRunAsyncReturns()
    {
        using Activity? outer = s_work.StartActivity("outer-" + Guid.NewGuid().ToString("N"));
        Assert.NotNull(outer);
        Activity? insideBody = null;

        Task run = TaskGroup.RunAsync(group =>
        {
            insideBody = Activity.Current;
            group.Run(_ => Task.Delay(10));
            return Task.CompletedTask;
        }, Named(out string name));

        Assert.Same(outer, Activity.Current);   // right after the call, while the group is still running
        await run;

        Activity group = Single(name);
        Assert.Same(group, insideBody);
        Assert.Equal(outer!.SpanId, group.ParentSpanId);
    }

    [Fact]
    public async Task WithoutAName_TheActivityIsCalledTaskGroup()
    {
        var parent = new ActivityContext(ActivityTraceId.CreateRandom(), ActivitySpanId.CreateRandom(), ActivityTraceFlags.Recorded);
        using (Activity? outer = s_work.StartActivity("outer", ActivityKind.Internal, parent))
        {
            await TaskGroup.RunAsync(_ => Task.CompletedTask);
        }

        Assert.Contains(_stopped, activity => activity.DisplayName == "TaskGroup" && activity.TraceId == parent.TraceId);
    }

    [Fact]
    public async Task EnableTracingFalse_RecordsNoActivity()
    {
        string name = "untraced-" + Guid.NewGuid().ToString("N");
        await TaskGroup.RunAsync(_ => Task.CompletedTask, new TaskGroupOptions { Name = name, EnableTracing = false });

        Assert.DoesNotContain(_stopped, activity => activity.DisplayName == name);
    }

    [Fact]
    public async Task Policies_NameTheirGroups()
    {
        string marker = "policy-" + Guid.NewGuid().ToString("N");
        using (s_work.StartActivity(marker))
        {
            await TaskGroup.AllAsync(_ => Task.FromResult(1), _ => Task.FromResult(2));
            await TaskGroup.AnyAsync(_ => Task.FromResult(1), _ => Task.FromResult(2));
            await TaskGroup.AllSettledAsync(new[] { 1, 2 }, (i, _) => Task.FromResult(i), maxConcurrency: 1);
        }

        ActivityTraceId trace = _stopped.Single(activity => activity.DisplayName == marker).TraceId;
        string[] names = _stopped.Where(activity => activity.TraceId == trace && activity.Source.Name == TaskGroup.ActivitySourceName)
            .Select(activity => activity.DisplayName).OrderBy(n => n).ToArray();
        Assert.Equal(["TaskGroup.AllAsync", "TaskGroup.AllSettledAsync", "TaskGroup.AnyAsync"], names);
    }

    [Fact]
    public async Task CallerCancellation_StaysTheRecordedCause_WhenAFailureFollows()
    {
        using var cts = new CancellationTokenSource();
        TaskGroupOptions options = Named(out string name);
        Task run = TaskGroup.RunAsync(group =>
        {
            group.Run(async ct =>
            {
                try
                {
                    await Task.Delay(Timeout.Infinite, ct);
                }
                catch (OperationCanceledException)
                {
                    throw new Boom();   // cleanup that fails after the caller cancelled
                }
            });
            return Task.CompletedTask;
        }, options, cts.Token);

        cts.Cancel();
        await Assert.ThrowsAsync<Boom>(() => run);

        Assert.Equal("caller", Single(name).GetTagItem("taskgroup.cancelled_by"));
    }

    private sealed class TracedWorker : StructuredBackgroundService
    {
        public string ChildName { get; } = "worker-child-" + Guid.NewGuid().ToString("N");

        protected override Task ExecuteAsync(TaskGroup group)
        {
            group.Run(_ =>
            {
                using Activity? child = s_work.StartActivity(ChildName);
                return Task.CompletedTask;
            });
            return Task.CompletedTask;
        }
    }

    [Fact]
    public async Task BackgroundService_RecordsNoLongLivedActivity_ButItsChildrenAreTraced()
    {
        var worker = new TracedWorker();
        await worker.StartAsync(CancellationToken.None);
        await worker.ExecuteTask!.WaitAsync(TimeSpan.FromSeconds(10));

        Activity child = Single(worker.ChildName);
        Assert.DoesNotContain(_stopped, activity => activity.DisplayName == nameof(TracedWorker));
        Assert.Null(child.Parent);   // a root, not nested under a span that lasts as long as the host
    }
}
