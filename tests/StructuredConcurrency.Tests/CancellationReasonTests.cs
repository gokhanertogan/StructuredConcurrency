using Microsoft.Extensions.Time.Testing;

namespace StructuredConcurrency.Tests;

// TaskGroup.CancellationReason: a child that was cancelled can tell why, like Go's context.Cause.
public class CancellationReasonTests
{
    private static readonly TimeSpan Guard = TimeSpan.FromSeconds(10);

    private sealed class Boom() : Exception("boom");

    // Starts a child that waits to be cancelled and reports the reason it sees when it is.
    private static Task<TaskGroupCancellationReason> ReasonSeenByAChild(TaskGroup group) =>
        group.Run(async ct =>
        {
            try
            {
                await Task.Delay(Timeout.Infinite, ct);
            }
            catch (OperationCanceledException)
            {
            }
            return group.CancellationReason;
        });

    [Fact]
    public async Task IsNone_WhileRunning_AndAfterSuccess()
    {
        TaskGroup? seen = null;
        await TaskGroup.RunAsync(group =>
        {
            seen = group;
            Assert.Equal(TaskGroupCancellationReason.None, group.CancellationReason);
            return group.Run(_ => Task.CompletedTask);
        });

        Assert.Equal(TaskGroupCancellationReason.None, seen!.CancellationReason);
    }

    [Fact]
    public async Task Failure_IsSeenByTheCancelledSibling()
    {
        Task<TaskGroupCancellationReason>? reason = null;
        await Assert.ThrowsAsync<Boom>(() => TaskGroup.RunAsync(group =>
        {
            reason = ReasonSeenByAChild(group);
            group.Run(_ => Task.FromException(new Boom()));
            return Task.CompletedTask;
        }).WaitAsync(Guard));

        Assert.Equal(TaskGroupCancellationReason.Failure, await reason!);
    }

    [Fact]
    public async Task Timeout_IsSeenByTheChildren()
    {
        var time = new FakeTimeProvider();
        Task<TaskGroupCancellationReason>? reason = null;
        Task run = TaskGroup.RunAsync(group =>
        {
            reason = ReasonSeenByAChild(group);
            return Task.CompletedTask;
        }, new TaskGroupOptions { Timeout = TimeSpan.FromSeconds(1), TimeProvider = time });

        time.Advance(TimeSpan.FromSeconds(1));
        await Assert.ThrowsAsync<TimeoutException>(() => run.WaitAsync(Guard));
        Assert.Equal(TaskGroupCancellationReason.Timeout, await reason!);
    }

    [Fact]
    public async Task Cancel_IsRequested()
    {
        Task<TaskGroupCancellationReason>? reason = null;
        await TaskGroup.RunAsync(group =>
        {
            reason = ReasonSeenByAChild(group);
            group.Cancel();
            return Task.CompletedTask;
        }).WaitAsync(Guard);

        Assert.Equal(TaskGroupCancellationReason.Requested, await reason!);
    }

    [Fact]
    public async Task TheCallersToken_IsCallerToken_EvenWhenAFailureFollows()
    {
        using var cts = new CancellationTokenSource();
        TaskGroup? seen = null;
        Task<TaskGroupCancellationReason>? reason = null;
        Task run = TaskGroup.RunAsync(async group =>
        {
            seen = group;
            reason = ReasonSeenByAChild(group);
            await reason;
            throw new Boom();   // after the caller's cancellation: the caller came first
        }, cts.Token);

        cts.Cancel();
        await Assert.ThrowsAsync<Boom>(() => run.WaitAsync(Guard));
        Assert.Equal(TaskGroupCancellationReason.CallerToken, await reason!);
        Assert.Equal(TaskGroupCancellationReason.CallerToken, seen!.CancellationReason);
    }

    [Fact]
    public async Task LeavingACreatedGroupWithoutJoining_IsDisposed()
    {
        Task<TaskGroupCancellationReason> reason;
        await using (var group = TaskGroup.Create())
        {
            reason = ReasonSeenByAChild(group);
        }

        Assert.Equal(TaskGroupCancellationReason.Disposed, await reason.WaitAsync(Guard));
    }

    [Fact]
    public async Task TheFirstReasonWins()
    {
        var time = new FakeTimeProvider();
        TaskGroup? seen = null;
        await TaskGroup.RunAsync(group =>
        {
            seen = group;
            group.Cancel();
            time.Advance(TimeSpan.FromSeconds(1));   // the deadline passes after Cancel: not a timeout
            return Task.CompletedTask;
        }, new TaskGroupOptions { Timeout = TimeSpan.FromSeconds(1), TimeProvider = time }).WaitAsync(Guard);

        Assert.Equal(TaskGroupCancellationReason.Requested, seen!.CancellationReason);
    }
}
