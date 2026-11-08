namespace StructuredConcurrency.Tests;

public class SingleFlightTests
{
    private sealed class Boom() : Exception("boom");

    private static readonly TimeSpan Guard = TimeSpan.FromSeconds(10);

    [Fact]
    public async Task ConcurrentCalls_ShareOneOperation()
    {
        var flights = new SingleFlight<string, int>();
        var gate = new TaskCompletionSource<int>();
        int calls = 0;
        Func<CancellationToken, Task<int>> load = _ =>
        {
            calls++;
            return gate.Task;
        };

        Task<int> first = flights.RunAsync("user:1", load);
        Task<int> second = flights.RunAsync("user:1", load);
        Task<int> other = flights.RunAsync("user:2", _ => Task.FromResult(2));
        gate.SetResult(1);

        Assert.Equal([1, 1], await Task.WhenAll(first, second).WaitAsync(Guard));
        Assert.Equal(2, await other);
        Assert.Equal(1, calls);
    }

    [Fact]
    public async Task ResultIsNotCached_TheNextCallStartsAgain()
    {
        var flights = new SingleFlight<string, int>();
        int calls = 0;

        await flights.RunAsync("k", _ => Task.FromResult(++calls));
        int second = await flights.RunAsync("k", _ => Task.FromResult(++calls));

        Assert.Equal(2, second);
    }

    [Fact]
    public async Task Failure_IsSharedByEveryCaller()
    {
        var flights = new SingleFlight<string, int>();
        var gate = new TaskCompletionSource<int>();

        Task<int> first = flights.RunAsync("k", _ => gate.Task);
        Task<int> second = flights.RunAsync("k", _ => throw new InvalidOperationException("not run"));
        gate.SetException(new Boom());

        await Assert.ThrowsAsync<Boom>(() => first.WaitAsync(Guard));
        await Assert.ThrowsAsync<Boom>(() => second.WaitAsync(Guard));
    }

    [Fact]
    public async Task OneCallerGivingUp_DoesNotCancelTheOperationForTheOthers()
    {
        var flights = new SingleFlight<string, int>();
        var gate = new TaskCompletionSource<int>();
        CancellationToken operationToken = default;
        using var impatient = new CancellationTokenSource();

        Task<int> leaving = flights.RunAsync("k", ct =>
        {
            operationToken = ct;
            return gate.Task;
        }, impatient.Token);
        Task<int> staying = flights.RunAsync("k", _ => throw new InvalidOperationException("not run"));

        impatient.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => leaving.WaitAsync(Guard));
        Assert.False(operationToken.IsCancellationRequested);

        gate.SetResult(5);
        Assert.Equal(5, await staying.WaitAsync(Guard));
    }

    [Fact]
    public async Task EveryCallerGivingUp_CancelsTheOperation_AndTheLastWaitsForIt()
    {
        var flights = new SingleFlight<string, int>();
        bool ended = false;
        using var a = new CancellationTokenSource();
        using var b = new CancellationTokenSource();
        Func<CancellationToken, Task<int>> load = async ct =>
        {
            try
            {
                await Task.Delay(Timeout.Infinite, ct);
                return 1;
            }
            finally
            {
                await Task.Yield();
                ended = true;
            }
        };

        Task<int> first = flights.RunAsync("k", load, a.Token);
        Task<int> second = flights.RunAsync("k", load, b.Token);

        a.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => first.WaitAsync(Guard));
        Assert.False(ended);

        b.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => second.WaitAsync(Guard));
        Assert.True(ended);

        // The cancelled flight is gone: the next call starts a new operation.
        Assert.Equal(9, await flights.RunAsync("k", _ => Task.FromResult(9)).WaitAsync(Guard));
    }

    [Fact]
    public async Task ACallWhileACancelledOperationIsStillEnding_StartsOnlyAfterItEnds()
    {
        var flights = new SingleFlight<string, int>();
        var ending = new TaskCompletionSource<int>();
        using var impatient = new CancellationTokenSource();

        // Ignores its token until the gate opens, as slow cleanup would.
        var cancelled = new TaskCompletionSource();
        Task<int> abandoned = flights.RunAsync("k", ct =>
        {
            ct.Register(() => cancelled.TrySetResult());
            return ending.Task;
        }, impatient.Token);
        impatient.Cancel();
        await cancelled.Task.WaitAsync(Guard);   // every caller has given up, so the operation is cancelled
        Assert.False(abandoned.IsCompleted);     // and the last caller to give up waits for it

        bool started = false;
        Task<int> next = flights.RunAsync("k", _ =>
        {
            started = true;
            return Task.FromResult(9);
        });
        await Task.Delay(20);
        Assert.False(started);

        ending.SetResult(1);
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => abandoned.WaitAsync(Guard));
        Assert.Equal(9, await next.WaitAsync(Guard));
        Assert.True(started);
    }

    [Fact]
    public async Task EveryCallerGivingUpWhileWaitingForACancelledOperation_EndsTheWaitAndLeavesNothingBehind()
    {
        var flights = new SingleFlight<string, int>();
        var ending = new TaskCompletionSource<int>();
        using var first = new CancellationTokenSource();
        using var second = new CancellationTokenSource();

        var cancelled = new TaskCompletionSource();
        Task<int> abandoned = flights.RunAsync("k", ct =>
        {
            ct.Register(() => cancelled.TrySetResult());
            return ending.Task;
        }, first.Token);
        first.Cancel();
        await cancelled.Task.WaitAsync(Guard);

        // Whether the next operation still starts depends on whether this caller's giving up is seen before the
        // previous one ends; either way the caller sees its own cancellation, and nothing outlives it.
        Task<int> next = flights.RunAsync("k", ct => Task.Delay(Timeout.Infinite, ct).ContinueWith(_ => 9), second.Token);
        second.Cancel();
        Assert.False(next.IsCompleted);   // it waits for the operation before it, which hasn't ended

        ending.SetResult(1);
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => abandoned.WaitAsync(Guard));
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => next.WaitAsync(Guard));

        // Nothing is left behind for the key.
        Assert.Equal(3, await flights.RunAsync("k", _ => Task.FromResult(3)).WaitAsync(Guard));
    }

    [Fact]
    public async Task Forget_MakesTheNextCallStartANewOperation()
    {
        var flights = new SingleFlight<string, int>();
        var gate = new TaskCompletionSource<int>();

        Task<int> stale = flights.RunAsync("k", _ => gate.Task);
        flights.Forget("k");
        int fresh = await flights.RunAsync("k", _ => Task.FromResult(2)).WaitAsync(Guard);
        gate.SetResult(1);

        Assert.Equal(2, fresh);
        Assert.Equal(1, await stale.WaitAsync(Guard));
    }

    [Fact]
    public async Task OperationThrowingSynchronously_FailsTheCall()
    {
        var flights = new SingleFlight<string, int>();

        await Assert.ThrowsAsync<Boom>(() => flights.RunAsync("k", _ => throw new Boom()));
        Assert.Equal(3, await flights.RunAsync("k", _ => Task.FromResult(3)));
    }

    [Fact]
    public async Task OperationsOwnCancellation_KeepsItsToken()
    {
        var flights = new SingleFlight<string, int>();
        using var own = new CancellationTokenSource();
        own.Cancel();

        var exception = await Assert.ThrowsAnyAsync<OperationCanceledException>(() =>
            flights.RunAsync("k", _ => Task.FromCanceled<int>(own.Token)));

        Assert.Equal(own.Token, exception.CancellationToken);
    }

    [Fact]
    public async Task Comparer_IsUsedForKeys()
    {
        var flights = new SingleFlight<string, int>(StringComparer.OrdinalIgnoreCase);
        var gate = new TaskCompletionSource<int>();
        int calls = 0;

        Task<int> lower = flights.RunAsync("key", _ => { calls++; return gate.Task; });
        Task<int> upper = flights.RunAsync("KEY", _ => { calls++; return gate.Task; });
        gate.SetResult(1);
        await Task.WhenAll(lower, upper).WaitAsync(Guard);

        Assert.Equal(1, calls);
    }
}
