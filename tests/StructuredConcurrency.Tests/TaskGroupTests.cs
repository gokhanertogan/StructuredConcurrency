namespace StructuredConcurrency.Tests;

// Children are driven by TaskCompletionSources that each test completes in a chosen order; no test depends on timing.
// TaskCompletionSource runs continuations synchronously by default, so completing a gate runs the child to its end.
public class TaskGroupTests
{
    private sealed class Boom(int id) : Exception($"boom {id}")
    {
        public int Id { get; } = id;
    }

    private static int[] FailureIds(Task task) =>
        task.Exception!.InnerExceptions.Select(e => ((Boom)e).Id).ToArray();

    // Rule 1: the scope completes only after every child has completed.

    [Fact]
    public async Task Scope_WaitsForChildThatIgnoresCancellation()
    {
        var ignoring = new TaskCompletionSource();
        var failing = new TaskCompletionSource();

        Task run = TaskGroup.RunAsync(group =>
        {
            group.Run(_ => ignoring.Task);
            group.Run(async _ =>
            {
                await failing.Task;
                throw new Boom(1);
            });
            return Task.CompletedTask;
        });

        failing.SetResult();
        Assert.False(run.IsCompleted);

        ignoring.SetResult();
        await Assert.ThrowsAsync<Boom>(() => run);
    }

    [Fact]
    public async Task Scope_EmptyGroupCompletes()
    {
        await TaskGroup.RunAsync(_ => Task.CompletedTask);
    }

    [Fact]
    public async Task Scope_ChildStartedByChild_IsAwaited()
    {
        var grandchild = new TaskCompletionSource();

        Task run = TaskGroup.RunAsync(group =>
        {
            group.Run(_ =>
            {
                group.Run(_ => grandchild.Task);
                return Task.CompletedTask;
            });
            return Task.CompletedTask;
        });

        Assert.False(run.IsCompleted);
        grandchild.SetResult();
        await run;
    }

    // Rule 2: the first real failure cancels the group's token.

    [Fact]
    public async Task Failure_CancelsSiblings()
    {
        CancellationToken seen = default;

        Task run = TaskGroup.RunAsync(group =>
        {
            group.Run(async ct =>
            {
                seen = ct;
                await Task.Delay(Timeout.Infinite, ct);
            });
            group.Run(_ => Task.FromException(new Boom(1)));
            return Task.CompletedTask;
        });

        Boom thrown = await Assert.ThrowsAsync<Boom>(() => run);
        Assert.Equal(1, thrown.Id);
        Assert.True(seen.IsCancellationRequested);
    }

    [Fact]
    public async Task Failure_BodyThrowing_CancelsChildrenAndIsReportedFirst()
    {
        var bodyGate = new TaskCompletionSource();
        var childGate = new TaskCompletionSource();
        CancellationToken seen = default;

        Task run = TaskGroup.RunAsync(async group =>
        {
            _ = group.Run(async ct =>
            {
                seen = ct;
                await childGate.Task;
                throw new Boom(2);
            });
            await bodyGate.Task;
            throw new Boom(1);
        });

        bodyGate.SetResult();
        Assert.True(seen.IsCancellationRequested);
        Assert.False(run.IsCompleted);

        childGate.SetResult();
        Boom thrown = await Assert.ThrowsAsync<Boom>(() => run);
        Assert.Equal(1, thrown.Id);
        Assert.Equal(new[] { 1, 2 }, FailureIds(run));
    }

    [Fact]
    public async Task Failure_SynchronousThrow_RunReturnsFaultedTask()
    {
        Task child = null!;
        bool continued = false;

        Task run = TaskGroup.RunAsync(group =>
        {
            child = group.Run(_ => throw new Boom(1));
            continued = true;
            return Task.CompletedTask;
        });

        await Assert.ThrowsAsync<Boom>(() => run);
        Assert.True(continued);
        Assert.True(child.IsFaulted);
    }

    // Rule 3: cancellation flows down from the caller and from parent groups.

    [Fact]
    public async Task Cancellation_CallerToken_CancelsGroupWithCallersToken()
    {
        using var cts = new CancellationTokenSource();
        CancellationToken seen = default;

        Task run = TaskGroup.RunAsync(group =>
        {
            group.Run(async ct =>
            {
                seen = ct;
                await Task.Delay(Timeout.Infinite, ct);
            });
            return Task.CompletedTask;
        }, cts.Token);

        cts.Cancel();

        OperationCanceledException thrown = await Assert.ThrowsAnyAsync<OperationCanceledException>(() => run);
        Assert.True(run.IsCanceled);
        Assert.Equal(cts.Token, thrown.CancellationToken);
        Assert.True(seen.IsCancellationRequested);
    }

    [Fact]
    public async Task Cancellation_AlreadyCancelledToken_DoesNotInvokeBody()
    {
        bool invoked = false;

        Task run = TaskGroup.RunAsync(_ =>
        {
            invoked = true;
            return Task.CompletedTask;
        }, new CancellationToken(canceled: true));

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => run);
        Assert.False(invoked);
    }

    [Fact]
    public async Task Cancellation_FailureInOuterGroup_CancelsNestedGroup()
    {
        CancellationToken inner = default;

        Task run = TaskGroup.RunAsync(outer =>
        {
            outer.Run(ct => TaskGroup.RunAsync(group =>
            {
                group.Run(async c =>
                {
                    inner = c;
                    await Task.Delay(Timeout.Infinite, c);
                });
                return Task.CompletedTask;
            }, ct));
            outer.Run(_ => Task.FromException(new Boom(1)));
            return Task.CompletedTask;
        });

        await Assert.ThrowsAsync<Boom>(() => run);
        Assert.True(inner.IsCancellationRequested);
        Assert.Equal(new[] { 1 }, FailureIds(run));
    }

    [Fact]
    public async Task Cancellation_FailureInNestedGroup_FailsOuterGroup()
    {
        Task run = TaskGroup.RunAsync(outer =>
        {
            outer.Run(ct => TaskGroup.RunAsync(group =>
            {
                group.Run(_ => Task.FromException(new Boom(7)));
                return Task.CompletedTask;
            }, ct));
            return Task.CompletedTask;
        });

        Boom thrown = await Assert.ThrowsAsync<Boom>(() => run);
        Assert.Equal(7, thrown.Id);
    }

    // Rule 4: await throws the first failure; the task holds all of them in order.

    [Fact]
    public async Task Errors_AllFailuresReportedInOrder()
    {
        var first = new TaskCompletionSource();
        var second = new TaskCompletionSource();

        Task run = TaskGroup.RunAsync(group =>
        {
            group.Run(async _ =>
            {
                await first.Task;
                throw new Boom(1);
            });
            group.Run(async _ =>
            {
                await second.Task;
                throw new Boom(2);
            });
            return Task.CompletedTask;
        });

        second.SetResult();
        first.SetResult();

        Boom thrown = await Assert.ThrowsAsync<Boom>(() => run);
        Assert.Equal(2, thrown.Id);
        Assert.Equal(new[] { 2, 1 }, FailureIds(run));
    }

    [Fact]
    public async Task Errors_BodyRethrowingChildFailure_IsReportedOnce()
    {
        Task run = TaskGroup.RunAsync(async group =>
        {
            Task child = group.Run(_ => Task.FromException(new Boom(1)));
            await child;
        });

        await Assert.ThrowsAsync<Boom>(() => run);
        Assert.Equal(new[] { 1 }, FailureIds(run));
    }

    [Fact]
    public async Task Errors_ThrowingCancellationCallback_IsReported()
    {
        Task run = TaskGroup.RunAsync(group =>
        {
            group.Run(ct =>
            {
                ct.Register(() => throw new Boom(2));
                return Task.CompletedTask;
            });
            group.Run(_ => Task.FromException(new Boom(1)));
            return Task.CompletedTask;
        });

        await Assert.ThrowsAsync<Boom>(() => run);
        Assert.Equal(new[] { 1, 2 }, FailureIds(run));
    }

    // Rule 5: cancellations caused by the group are not failures; other cancellations are.

    [Fact]
    public async Task GroupCausedCancellation_IsNotReported()
    {
        var gate = new TaskCompletionSource();

        Task run = TaskGroup.RunAsync(group =>
        {
            group.Run(async _ =>
            {
                await gate.Task;
                throw new OperationCanceledException();
            });
            group.Run(_ => Task.FromException(new Boom(1)));
            return Task.CompletedTask;
        });

        gate.SetResult();

        await Assert.ThrowsAsync<Boom>(() => run);
        Assert.Equal(new[] { 1 }, FailureIds(run));
    }

    [Fact]
    public async Task ChildCancellation_WithoutGroupCancellation_IsAFailure()
    {
        Task run = TaskGroup.RunAsync(group =>
        {
            group.Run(_ => Task.FromCanceled(new CancellationToken(canceled: true)));
            return Task.CompletedTask;
        });

        await Assert.ThrowsAsync<TaskCanceledException>(() => run);
        Assert.True(run.IsFaulted);
    }

    [Fact]
    public async Task Cancel_IsNotAFailure_AndKeepsTheBodysResult()
    {
        int result = await TaskGroup.RunAsync(async group =>
        {
            _ = group.Run(ct => Task.Delay(Timeout.Infinite, ct));
            group.Cancel();
            await Task.Yield();
            return 42;
        });

        Assert.Equal(42, result);
    }

    // Starting work late.

    [Fact]
    public async Task Run_AfterCancellation_DoesNotInvokeWork()
    {
        bool invoked = false;
        Task late = null!;

        await TaskGroup.RunAsync(group =>
        {
            group.Cancel();
            late = group.Run(_ =>
            {
                invoked = true;
                return Task.CompletedTask;
            });
            return Task.CompletedTask;
        });

        Assert.False(invoked);
        Assert.True(late.IsCanceled);
    }

    [Fact]
    public async Task Run_AfterCompletion_Throws()
    {
        TaskGroup leaked = null!;
        await TaskGroup.RunAsync(group =>
        {
            leaked = group;
            return Task.CompletedTask;
        });

        // Run throws synchronously rather than returning a faulted task.
        InvalidOperationException? thrown = null;
        try
        {
            _ = leaked.Run(_ => Task.CompletedTask);
        }
        catch (InvalidOperationException exception)
        {
            thrown = exception;
        }
        Assert.NotNull(thrown);
    }

    // Results and supervision.

    [Fact]
    public async Task Results_ChildResultsFlowToBody()
    {
        var (number, text) = await TaskGroup.RunAsync(async group =>
        {
            Task<int> a = group.Run(_ => Task.FromResult(1));
            Task<string> b = group.Run(async _ =>
            {
                await Task.Yield();
                return "two";
            });
            return (await a, await b);
        });

        Assert.Equal(1, number);
        Assert.Equal("two", text);
    }

    [Fact]
    public async Task Supervisor_FailureDoesNotCancelSiblings()
    {
        var gate = new TaskCompletionSource();
        CancellationToken seen = default;

        Task run = TaskGroup.RunAsync(group =>
        {
            group.Run(async ct =>
            {
                seen = ct;
                await gate.Task;
            });
            group.Run(_ => Task.FromException(new Boom(1)));
            return Task.CompletedTask;
        }, new TaskGroupOptions { CancelOnFailure = false });

        Assert.False(seen.IsCancellationRequested);
        Assert.False(run.IsCompleted);

        gate.SetResult();
        await Assert.ThrowsAsync<Boom>(() => run);
    }

    // Every completion order of three children, each succeeding, failing or throwing its own cancellation,
    // checked against a reference model of the rules. 3^3 outcomes x 3! orders x 2 modes = 324 scenarios.
    [Fact]
    public void Permutations_MatchReferenceModel()
    {
        int[][] orders = [[0, 1, 2], [0, 2, 1], [1, 0, 2], [1, 2, 0], [2, 0, 1], [2, 1, 0]];
        int scenarios = 0;

        foreach (bool cancelOnFailure in new[] { true, false })
        {
            for (int mix = 0; mix < 27; mix++)
            {
                int[] outcomes = [mix % 3, mix / 3 % 3, mix / 9];  // 0 = success, 1 = failure, 2 = own cancellation
                foreach (int[] order in orders)
                {
                    var gates = new[] { new TaskCompletionSource(), new TaskCompletionSource(), new TaskCompletionSource() };

                    Task run = TaskGroup.RunAsync(group =>
                    {
                        for (int i = 0; i < 3; i++)
                        {
                            int id = i;
                            group.Run(async _ =>
                            {
                                await gates[id].Task;
                                if (outcomes[id] == 1)
                                {
                                    throw new Boom(id);
                                }
                                if (outcomes[id] == 2)
                                {
                                    throw new OperationCanceledException($"cancel {id}");
                                }
                            });
                        }
                        return Task.CompletedTask;
                    }, new TaskGroupOptions { CancelOnFailure = cancelOnFailure });

                    foreach (int id in order)
                    {
                        gates[id].SetResult();
                    }

                    // Reference model: walk the completion order, applying rules 2 and 5.
                    var expected = new List<string>();
                    bool cancelled = false;
                    foreach (int id in order)
                    {
                        if (outcomes[id] == 0 || (outcomes[id] == 2 && cancelled))
                        {
                            continue;
                        }
                        expected.Add(outcomes[id] == 1 ? $"boom {id}" : $"cancel {id}");
                        cancelled |= cancelOnFailure;
                    }

                    string scenario = $"outcomes [{string.Join(",", outcomes)}], order [{string.Join(",", order)}], cancelOnFailure {cancelOnFailure}";
                    Assert.True(run.IsCompleted, scenario);
                    if (expected.Count == 0)
                    {
                        Assert.True(run.IsCompletedSuccessfully, scenario);
                    }
                    else
                    {
                        Assert.True(run.IsFaulted, scenario);
                        Assert.Equal(expected, run.Exception!.InnerExceptions.Select(e => e.Message));
                    }
                    scenarios++;
                }
            }
        }

        Assert.Equal(324, scenarios);
    }
}
