namespace StructuredConcurrency.Tests;

// Some children use ConfigureAwait(false) on purpose, to run their continuations without the test's context.
#pragma warning disable xUnit1030

// Children are driven by TaskCompletionSources that each test completes in a chosen order; no test depends on timing.
public class ConcurrencyLimitTests
{
    private sealed class Boom(int id) : Exception($"boom {id}")
    {
        public int Id { get; } = id;
    }

    private static TaskGroupOptions Limit(int max) => new() { MaxConcurrency = max };

    [Fact]
    public async Task Limit_IsNeverExceeded_AndEveryChildRuns()
    {
        const int Children = 10;
        var gates = Enumerable.Range(0, Children).Select(_ => new TaskCompletionSource()).ToArray();
        int running = 0, maxRunning = 0, started = 0;

        Task run = TaskGroup.RunAsync(group =>
        {
            for (int i = 0; i < Children; i++)
            {
                int id = i;
                group.Run(async _ =>
                {
                    started++;
                    maxRunning = Math.Max(maxRunning, ++running);
                    await gates[id].Task;
                    running--;
                });
            }
            return Task.CompletedTask;
        }, Limit(3));

        Assert.Equal(3, started);
        for (int i = 0; i < Children; i++)
        {
            gates[i].SetResult();
        }

        await run;
        Assert.Equal(Children, started);
        Assert.Equal(3, maxRunning);
    }

    [Fact]
    public async Task QueuedChildren_StartInOrder()
    {
        var first = new TaskCompletionSource();
        var order = new List<int>();

        Task run = TaskGroup.RunAsync(group =>
        {
            group.Run(_ => first.Task);
            for (int i = 1; i <= 5; i++)
            {
                int id = i;
                group.Run(_ =>
                {
                    order.Add(id);
                    return Task.CompletedTask;
                });
            }
            return Task.CompletedTask;
        }, Limit(1));

        Assert.Empty(order);
        first.SetResult();
        await run;
        Assert.Equal(new[] { 1, 2, 3, 4, 5 }, order);
    }

    [Fact]
    public async Task QueuedChild_TaskReflectsItsResult_AndBodyCanAwaitIt()
    {
        var first = new TaskCompletionSource<int>();

        int sum = await TaskGroup.RunAsync(async group =>
        {
            // a observes cancellation, so a failed assertion in this body fails the test instead of hanging it.
            Task<int> a = group.Run(ct => first.Task.WaitAsync(ct));
            Task<int> b = group.Run(_ => Task.FromResult(2));  // queued behind a
            Assert.False(b.IsCompleted);

            first.SetResult(1);
            return await a + await b;
        }, Limit(1));

        Assert.Equal(3, sum);
    }

    [Fact]
    public async Task Failure_SkipsQueuedChildren_WithoutReportingThem()
    {
        var gate = new TaskCompletionSource();
        int invoked = 0;
        var queued = new List<Task>();

        Task run = TaskGroup.RunAsync(group =>
        {
            group.Run(async _ =>
            {
                invoked++;
                await gate.Task;
                throw new Boom(1);
            });
            for (int i = 0; i < 5; i++)
            {
                queued.Add(group.Run(_ =>
                {
                    invoked++;
                    return Task.CompletedTask;
                }));
            }
            return Task.CompletedTask;
        }, Limit(1));

        gate.SetResult();

        Boom thrown = await Assert.ThrowsAsync<Boom>(() => run);
        Assert.Equal(1, thrown.Id);
        Assert.Single(run.Exception!.InnerExceptions);
        Assert.Equal(1, invoked);
        Assert.All(queued, task => Assert.True(task.IsCanceled));
    }

    [Fact]
    public async Task CallerCancellation_DropsQueuedChildrenAtOnce()
    {
        using var cts = new CancellationTokenSource();
        var ignoring = new TaskCompletionSource();
        Task queued = null!;

        Task run = TaskGroup.RunAsync(group =>
        {
            group.Run(_ => ignoring.Task);  // holds the only slot and ignores cancellation
            queued = group.Run(_ => Task.CompletedTask);
            return Task.CompletedTask;
        }, Limit(1), cts.Token);

        cts.Cancel();
        Assert.True(queued.IsCanceled);
        Assert.False(run.IsCompleted);

        ignoring.SetResult();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => run);
    }

    [Fact]
    public async Task LongQueueOfSynchronousWork_DoesNotOverflowTheStack()
    {
        const int Children = 100_000;
        var first = new TaskCompletionSource();
        int completed = 0;

        Task run = TaskGroup.RunAsync(group =>
        {
            group.Run(_ => first.Task);
            for (int i = 0; i < Children; i++)
            {
                group.Run(_ =>
                {
                    completed++;
                    return Task.CompletedTask;
                });
            }
            return Task.CompletedTask;
        }, Limit(1));

        first.SetResult();
        await run;
        Assert.Equal(Children, completed);
    }

    [Fact]
    public void Limit_BelowOne_Throws()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => new TaskGroupOptions { MaxConcurrency = 0 });
    }

    [Fact]
    public async Task AllAsync_WithLimit_ReturnsResultsInOrder_WithinTheLimit()
    {
        int running = 0, maxRunning = 0;

        int[] results = await TaskGroup.AllAsync(Enumerable.Range(1, 20), async (n, _) =>
        {
            maxRunning = Math.Max(maxRunning, Interlocked.Increment(ref running));
            await Task.Yield();
            Interlocked.Decrement(ref running);
            return n * 10;
        }, maxConcurrency: 4);

        Assert.Equal(Enumerable.Range(1, 20).Select(n => n * 10), results);
        Assert.True(maxRunning <= 4, $"max running was {maxRunning}");
    }

    [Fact]
    public async Task AllSettledAsync_WithLimit_ReportsEveryOutcome()
    {
        TaskOutcome<int>[] outcomes = await TaskGroup.AllSettledAsync(Enumerable.Range(0, 6), (n, _) =>
            n % 2 == 0 ? Task.FromResult(n) : Task.FromException<int>(new Boom(n)), maxConcurrency: 2);

        Assert.Equal(new[] { true, false, true, false, true, false }, outcomes.Select(o => o.IsSuccess));
    }

    [Fact]
    public async Task QueuedChild_RunsWithTheExecutionContextRunWasCalledIn()
    {
        var local = new AsyncLocal<int>();
        var gate = new TaskCompletionSource();
        var seen = new int[3];

        Task run = TaskGroup.RunAsync(group =>
        {
            for (int i = 0; i < seen.Length; i++)
            {
                int id = i;
                local.Value = id;
                group.Run(async _ =>
                {
                    seen[id] = local.Value;
                    await gate.Task;
                });
            }
            return Task.CompletedTask;
        }, Limit(1));

        gate.SetResult();
        await run;

        Assert.Equal([0, 1, 2], seen);
    }

    [Fact]
    public async Task CodeAwaitingAQueuedChild_DoesNotHoldUpTheNextOne()
    {
        var gate = new TaskCompletionSource();
        var lastStarted = new ManualResetEventSlim();
        bool lastStartedInTime = false;

        Task run = TaskGroup.RunAsync(async group =>
        {
            _ = group.Run(_ => gate.Task);
            Task queued = group.Run(_ => Task.CompletedTask);
            _ = group.Run(_ =>
            {
                lastStarted.Set();
                return Task.CompletedTask;
            });

            await queued.ConfigureAwait(false);

            // Blocks the thread the awaited child completed on; the last child must still get its slot.
            lastStartedInTime = lastStarted.Wait(TimeSpan.FromSeconds(10));
        }, Limit(1));

        // From a thread pool thread: the test's SynchronizationContext would stop continuations from running inline.
        await Task.Run(gate.SetResult);
        await run;

        Assert.True(lastStartedInTime);
    }

    [Fact]
    public async Task QueuedChildFailure_IsNotReportedAsUnobserved_WhenNotAwaited()
    {
        var unobserved = new List<Exception>();
        EventHandler<UnobservedTaskExceptionEventArgs> handler = (_, e) =>
        {
            lock (unobserved)
            {
                unobserved.AddRange(e.Exception.InnerExceptions.OfType<Boom>().Where(boom => boom.Id == 4242));
            }
        };

        TaskScheduler.UnobservedTaskException += handler;
        try
        {
            await RunUnawaitedFailingChildren();
            GC.Collect();
            GC.WaitForPendingFinalizers();
            GC.Collect();

            Assert.Empty(unobserved);
        }
        finally
        {
            TaskScheduler.UnobservedTaskException -= handler;
        }
    }

    // In its own method, so nothing in the test keeps the children's tasks alive when it collects garbage.
    [System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.NoInlining)]
    private static async Task RunUnawaitedFailingChildren()
    {
        var gate = new TaskCompletionSource();
        Task run = TaskGroup.RunAsync(group =>
        {
            for (int i = 0; i < 3; i++)
            {
                group.Run(async _ =>
                {
                    await gate.Task;
                    throw new Boom(4242);
                });
            }
            return Task.CompletedTask;
        }, new TaskGroupOptions { MaxConcurrency = 1, CancelOnFailure = false });

        gate.SetResult();
        await Assert.ThrowsAsync<Boom>(() => run);
    }
}
