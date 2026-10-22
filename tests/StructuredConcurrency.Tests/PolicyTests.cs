namespace StructuredConcurrency.Tests;

// Operations are driven by TaskCompletionSources that each test completes in a chosen order; no test depends on timing.
public class PolicyTests
{
    private sealed class Boom(int id) : Exception($"boom {id}")
    {
        public int Id { get; } = id;
    }

    // Completes when its gate is opened; observes the token so cancellation is visible.
    private static Func<CancellationToken, Task<T>> Gated<T>(TaskCompletionSource<T> gate, Action<CancellationToken>? seen = null) =>
        async ct =>
        {
            seen?.Invoke(ct);
            using (ct.Register(() => gate.TrySetCanceled(ct)))
            {
                return await gate.Task;
            }
        };

    // ----------------------------------------------------------------------------------------------------------------
    // AllAsync

    [Fact]
    public async Task All_Tuple_ReturnsBothResults()
    {
        var (number, text) = await TaskGroup.AllAsync(
            _ => Task.FromResult(1),
            async _ =>
            {
                await Task.Yield();
                return "two";
            });

        Assert.Equal(1, number);
        Assert.Equal("two", text);
    }

    [Fact]
    public async Task All_FirstFailure_CancelsTheOthersAndIsThrown()
    {
        var slow = new TaskCompletionSource<string>();
        CancellationToken seen = default;

        Task<(string, int)> all = TaskGroup.AllAsync(
            Gated(slow, ct => seen = ct),
            _ => Task.FromException<int>(new Boom(1)));

        Boom thrown = await Assert.ThrowsAsync<Boom>(() => all);
        Assert.Equal(1, thrown.Id);
        Assert.True(seen.IsCancellationRequested);
        Assert.Single(all.Exception!.InnerExceptions);
    }

    [Fact]
    public async Task All_Collection_ReturnsResultsInInputOrder()
    {
        var gates = new[] { new TaskCompletionSource<int>(), new TaskCompletionSource<int>(), new TaskCompletionSource<int>() };

        Task<int[]> all = TaskGroup.AllAsync(gates, (gate, ct) => Gated(gate)(ct));

        gates[2].SetResult(30);
        gates[0].SetResult(10);
        gates[1].SetResult(20);

        Assert.Equal(new[] { 10, 20, 30 }, await all);
    }

    [Fact]
    public async Task All_Empty_ReturnsEmptyArray()
    {
        int[] results = await TaskGroup.AllAsync(Array.Empty<Func<CancellationToken, Task<int>>>());
        Assert.Empty(results);
    }

    // ----------------------------------------------------------------------------------------------------------------
    // AnyAsync

    [Fact]
    public async Task Any_FirstSuccessWins_AfterEarlierFailure_AndCancelsTheRest()
    {
        var failing = new TaskCompletionSource<string>();
        var winning = new TaskCompletionSource<string>();
        var slow = new TaskCompletionSource<string>();
        CancellationToken slowToken = default;

        Task<string> any = TaskGroup.AnyAsync(Gated(failing), Gated(winning), Gated(slow, ct => slowToken = ct));

        failing.SetException(new Boom(0));
        Assert.False(any.IsCompleted);

        winning.SetResult("B");

        Assert.Equal("B", await any);
        Assert.True(slowToken.IsCancellationRequested);
        Assert.True(slow.Task.IsCanceled);
    }

    [Fact]
    public async Task Any_AllFail_ThrowsAggregateInInputOrder()
    {
        var gates = new[] { new TaskCompletionSource<int>(), new TaskCompletionSource<int>(), new TaskCompletionSource<int>() };

        Task<int> any = TaskGroup.AnyAsync(gates, (gate, ct) => Gated(gate)(ct));

        gates[2].SetException(new Boom(2));
        gates[0].SetException(new Boom(0));
        gates[1].SetException(new Boom(1));

        AggregateException thrown = await Assert.ThrowsAsync<AggregateException>(() => any);
        Assert.Equal(new[] { 0, 1, 2 }, thrown.InnerExceptions.Select(e => ((Boom)e).Id));
    }

    [Fact]
    public async Task Any_OwnCancellationWithoutWinner_IsReportedAsFailure()
    {
        Task<int> any = TaskGroup.AnyAsync(
            _ => Task.FromCanceled<int>(new CancellationToken(canceled: true)),
            _ => Task.FromException<int>(new Boom(1)));

        AggregateException thrown = await Assert.ThrowsAsync<AggregateException>(() => any);
        Assert.IsAssignableFrom<OperationCanceledException>(thrown.InnerExceptions[0]);
        Assert.IsType<Boom>(thrown.InnerExceptions[1]);
    }

    [Fact]
    public async Task Any_CallerCancellation_EndsAsCancelled()
    {
        using var cts = new CancellationTokenSource();
        var a = new TaskCompletionSource<int>();
        var b = new TaskCompletionSource<int>();

        Task<int> any = TaskGroup.AnyAsync(Gated(a), Gated(b), cts.Token);
        cts.Cancel();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => any);
        Assert.True(any.IsCanceled);
    }

    [Fact]
    public void Any_Empty_ThrowsArgumentException()
    {
        Assert.Throws<ArgumentException>(() => { _ = TaskGroup.AnyAsync(Array.Empty<Func<CancellationToken, Task<int>>>()); });
    }

    // ----------------------------------------------------------------------------------------------------------------
    // AllSettledAsync

    [Fact]
    public async Task AllSettled_ReportsEachOutcomeInInputOrder_WithoutCancellingOthers()
    {
        var gates = new[] { new TaskCompletionSource<int>(), new TaskCompletionSource<int>(), new TaskCompletionSource<int>() };
        var tokens = new CancellationToken[3];

        Task<TaskOutcome<int>[]> settled = TaskGroup.AllSettledAsync(
            Enumerable.Range(0, 3),
            (i, ct) => Gated(gates[i], c => tokens[i] = c)(ct));

        gates[1].SetException(new Boom(1));
        Assert.False(tokens[0].IsCancellationRequested);
        Assert.False(settled.IsCompleted);

        gates[0].SetResult(10);
        gates[2].SetCanceled();

        TaskOutcome<int>[] outcomes = await settled;
        Assert.Equal(TaskStatus.RanToCompletion, outcomes[0].Status);
        Assert.Equal(10, outcomes[0].Value);
        Assert.Equal(TaskStatus.Faulted, outcomes[1].Status);
        Assert.IsType<Boom>(outcomes[1].Exception);
        Assert.Equal(TaskStatus.Canceled, outcomes[2].Status);
        Assert.Throws<InvalidOperationException>(() => outcomes[1].Value);
    }

    [Fact]
    public async Task AllSettled_Tuple_MixedTypes()
    {
        var (number, text) = await TaskGroup.AllSettledAsync(
            _ => Task.FromResult(1),
            _ => Task.FromException<string>(new Boom(2)));

        Assert.True(number.IsSuccess);
        Assert.Equal(1, number.Value);
        Assert.False(text.IsSuccess);
        Assert.IsType<Boom>(text.Exception);
    }

    [Fact]
    public async Task AllSettled_CallerCancellation_EndsAsCancelled()
    {
        using var cts = new CancellationTokenSource();
        var gate = new TaskCompletionSource<int>();

        Task<TaskOutcome<int>[]> settled = TaskGroup.AllSettledAsync(new[] { Gated(gate) }, cts.Token);
        cts.Cancel();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => settled);
    }
}
