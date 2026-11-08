using System.Runtime.CompilerServices;

namespace StructuredConcurrency.Tests;

// Groups from TaskGroup.Create: joined by their owner, cancelled and awaited when the owner leaves without joining.
public class ScopeTests
{
    private static readonly TimeSpan Guard = TimeSpan.FromSeconds(10);

    private sealed class Boom(int id) : Exception($"boom {id}")
    {
        public int Id { get; } = id;
    }

    [Fact]
    public async Task Join_WaitsForEveryChild()
    {
        var gate = new TaskCompletionSource();
        await using var group = TaskGroup.Create();
        Task<int> child = group.Run(async _ =>
        {
            await gate.Task;
            return 42;
        });

        Task join = group.JoinAsync();
        Assert.False(join.IsCompleted);

        gate.SetResult();
        await join.WaitAsync(Guard);
        Assert.Equal(42, await child);
    }

    [Fact]
    public async Task Join_ThrowsTheFirstFailure_AfterTheOthersWereCancelled()
    {
        CancellationToken siblingToken = default;
        await using var group = TaskGroup.Create();
        group.Run(ct =>
        {
            siblingToken = ct;
            return Task.Delay(Timeout.Infinite, ct);
        });
        group.Run(_ => Task.FromException(new Boom(1)));

        await Assert.ThrowsAsync<Boom>(() => group.JoinAsync().WaitAsync(Guard));
        Assert.True(siblingToken.IsCancellationRequested);
    }

    [Fact]
    public async Task LeavingByAnException_CancelsAndWaits_AndTheOriginalExceptionSurvives()
    {
        bool stopped = false;

        var thrown = await Assert.ThrowsAsync<InvalidOperationException>(async () =>
        {
            await using var group = TaskGroup.Create();
            group.Run(async ct =>
            {
                try
                {
                    await Task.Delay(Timeout.Infinite, ct);
                }
                finally
                {
                    await Task.Yield();
                    stopped = true;
                }
            });
            group.Run(_ => Task.FromException(new Boom(1)));   // not reported: the owner's exception wins
            await Task.Yield();
            throw new InvalidOperationException("owner");
        });

        Assert.Equal("owner", thrown.Message);
        Assert.True(stopped);
    }

    [Fact]
    public async Task ReturningEarly_CancelsTheChildren_AndWaitsForThem()
    {
        bool stopped = false;

        async Task<int> FirstAsync()
        {
            await using var group = TaskGroup.Create();
            group.Run(async ct =>
            {
                try
                {
                    await Task.Delay(Timeout.Infinite, ct);
                }
                finally
                {
                    stopped = true;
                }
            });
            return await group.Run(_ => Task.FromResult(7));
        }

        Assert.Equal(7, await FirstAsync().WaitAsync(Guard));
        Assert.True(stopped);
    }

    private static async IAsyncEnumerable<int> SquaresAsync(int[] items, Action onStopped, [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        await using var group = TaskGroup.Create(cancellationToken);
        List<Task<int>> squares = items.Select(item => group.Run(async ct =>
        {
            try
            {
                await Task.Delay(item == 3 ? Timeout.Infinite : 1, ct);
                return item * item;
            }
            catch (OperationCanceledException)
            {
                onStopped();
                throw;
            }
        })).ToList();

        // A lambda body can't yield; a group from Create can.
        foreach (Task<int> square in squares)
        {
            yield return await square;
        }
        await group.JoinAsync();
    }

    [Fact]
    public async Task AsyncIterator_YieldsTheChildrensResults_AndStoppingEarlyCancelsTheRest()
    {
        int stopped = 0;
        var seen = new List<int>();
        await foreach (int square in SquaresAsync([1, 2, 3], () => Interlocked.Increment(ref stopped)).WithCancellation(CancellationToken.None))
        {
            seen.Add(square);
            if (seen.Count == 2)
            {
                break;   // disposes the iterator, and with it the group
            }
        }

        Assert.Equal([1, 4], seen);
        Assert.Equal(1, stopped);
    }

    [Fact]
    public async Task CallerCancellation_CancelsTheGroup_AndJoinEndsCancelled()
    {
        using var cts = new CancellationTokenSource();
        await using var group = TaskGroup.Create(cts.Token);
        group.Run(ct => Task.Delay(Timeout.Infinite, ct));

        cts.Cancel();
        Task join = group.JoinAsync();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => join.WaitAsync(Guard));
        Assert.True(join.IsCanceled);
    }

    [Fact]
    public async Task Options_Apply_ToACreatedGroup()
    {
        int running = 0;
        int peak = 0;
        var gates = Enumerable.Range(0, 4).Select(_ => new TaskCompletionSource()).ToArray();
        await using var group = TaskGroup.Create(new TaskGroupOptions { MaxConcurrency = 2 });
        foreach (TaskCompletionSource gate in gates)
        {
            group.Run(async _ =>
            {
                peak = Math.Max(peak, Interlocked.Increment(ref running));
                await gate.Task;
                Interlocked.Decrement(ref running);
            });
        }

        Task join = group.JoinAsync();
        foreach (TaskCompletionSource gate in gates)
        {
            gate.SetResult();
        }
        await join.WaitAsync(Guard);
        Assert.Equal(2, peak);
    }

    [Fact]
    public async Task DisposeAfterJoin_DoesNothing_EvenWhenTheGroupFailed()
    {
        TaskGroup group = TaskGroup.Create();
        group.Run(_ => Task.FromException(new Boom(1)));
        await Assert.ThrowsAsync<Boom>(() => group.JoinAsync());

        await group.DisposeAsync();
        Assert.Throws<InvalidOperationException>(() => { _ = group.Run(_ => Task.CompletedTask); });
    }

    [Fact]
    public async Task JoinAndDispose_AreOnlyForGroupsFromCreate()
    {
        await TaskGroup.RunAsync(group =>
        {
            Assert.Throws<InvalidOperationException>(() => { _ = group.JoinAsync(); });
            Assert.Throws<InvalidOperationException>(() => { _ = group.DisposeAsync(); });
            return Task.CompletedTask;
        });
    }
}
