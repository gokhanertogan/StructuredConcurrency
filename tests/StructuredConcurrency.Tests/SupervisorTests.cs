using Microsoft.Extensions.Time.Testing;
using StructuredConcurrency.Hosting;

namespace StructuredConcurrency.Tests;

// Policies wait no time between runs unless a test is about the delays, which it then measures with a FakeTimeProvider.
public class SupervisorTests
{
    private static readonly TimeSpan Guard = TimeSpan.FromSeconds(10);

    private sealed class Boom(int run) : Exception($"boom {run}")
    {
        public int Run { get; } = run;
    }

    private static RestartPolicy NoDelay(int maxRestarts = 3, Action<RestartEvent>? onRestart = null, Func<Exception, bool>? shouldRestart = null) => new()
    {
        MaxRestarts = maxRestarts,
        InitialDelay = TimeSpan.Zero,
        OnRestart = onRestart,
        ShouldRestart = shouldRestart,
    };

    // Fails the first `failures` runs, then succeeds.
    private sealed class Flaky(int failures)
    {
        public int Runs;

        public Task<int> RunAsync(CancellationToken _)
        {
            int run = Interlocked.Increment(ref Runs);
            return run <= failures ? Task.FromException<int>(new Boom(run)) : Task.FromResult(run);
        }
    }

    [Fact]
    public async Task FailingWork_IsRestarted_UntilItSucceeds()
    {
        var work = new Flaky(failures: 2);
        var restarts = new List<RestartEvent>();

        int result = await Supervisor.RunAsync(work.RunAsync, NoDelay(onRestart: restarts.Add)).WaitAsync(Guard);

        Assert.Equal(3, result);
        Assert.Equal(new[] { 1, 2 }, restarts.Select(r => r.Restart));
        Assert.Equal(new[] { 1, 2 }, restarts.Select(r => ((Boom)r.Failure).Run));
    }

    [Fact]
    public async Task TooManyRestarts_GivesUp_WithTheLastFailure()
    {
        var work = new Flaky(failures: 100);

        Boom thrown = await Assert.ThrowsAsync<Boom>(() => Supervisor.RunAsync(work.RunAsync, NoDelay(maxRestarts: 2)).WaitAsync(Guard));

        Assert.Equal(3, thrown.Run);   // the first run and two restarts
        Assert.Equal(3, work.Runs);
    }

    [Fact]
    public async Task ZeroRestarts_RunsOnce()
    {
        var work = new Flaky(failures: 1);

        await Assert.ThrowsAsync<Boom>(() => Supervisor.RunAsync(work.RunAsync, NoDelay(maxRestarts: 0)).WaitAsync(Guard));
        Assert.Equal(1, work.Runs);
    }

    [Fact]
    public async Task RestartsOutsideTheWindow_AreForgotten()
    {
        var time = new FakeTimeProvider();
        int runs = 0;
        var policy = new RestartPolicy { MaxRestarts = 1, Window = TimeSpan.FromMinutes(1), InitialDelay = TimeSpan.Zero, TimeProvider = time };

        // Each failure comes two minutes after the last, so the window never holds more than one restart.
        Task run = Supervisor.RunAsync(_ =>
        {
            if (++runs < 5)
            {
                time.Advance(TimeSpan.FromMinutes(2));
                throw new Boom(runs);
            }
            return Task.CompletedTask;
        }, policy);

        await run.WaitAsync(Guard);
        Assert.Equal(5, runs);
    }

    [Fact]
    public async Task DelaysGrow_AndAreCapped()
    {
        var time = new FakeTimeProvider();
        var work = new Flaky(failures: 4);
        var delays = new List<TimeSpan>();
        var policy = new RestartPolicy
        {
            MaxRestarts = 10,
            InitialDelay = TimeSpan.FromSeconds(1),
            BackoffMultiplier = 2,
            MaxDelay = TimeSpan.FromSeconds(5),
            TimeProvider = time,
            OnRestart = restart => delays.Add(restart.Delay),
        };

        Task<int> run = Supervisor.RunAsync(work.RunAsync, policy);
        for (int restart = 1; restart <= 4; restart++)
        {
            await WaitUntil(() => delays.Count == restart);
            Assert.False(run.IsCompleted);
            time.Advance(delays[^1]);
        }

        Assert.Equal(5, await run.WaitAsync(Guard));
        Assert.Equal(new[] { 1, 2, 4, 5 }.Select(s => TimeSpan.FromSeconds(s)), delays);
    }

    [Fact]
    public async Task FailureTheFilterTurnsDown_IsThrownAtOnce()
    {
        int runs = 0;

        await Assert.ThrowsAsync<InvalidOperationException>(() => Supervisor.RunAsync(_ =>
        {
            runs++;
            throw new InvalidOperationException();
        }, NoDelay(shouldRestart: exception => exception is not InvalidOperationException)).WaitAsync(Guard));

        Assert.Equal(1, runs);
    }

    [Fact]
    public async Task CallerCancellation_IsNeverRestarted()
    {
        using var cts = new CancellationTokenSource();
        int runs = 0;

        Task run = Supervisor.RunAsync(async ct =>
        {
            runs++;
            await Task.Delay(Timeout.Infinite, ct);
        }, NoDelay(), cts.Token);
        cts.Cancel();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => run.WaitAsync(Guard));
        Assert.Equal(1, runs);
    }

    [Fact]
    public async Task AnyFailureAfterCancellation_IsNeverRestarted()
    {
        using var cts = new CancellationTokenSource();
        var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        int runs = 0, restarts = 0;
        var policy = NoDelay(onRestart: _ => restarts++);

        // Work that reacts to shutdown with something other than an OperationCanceledException, like a closed connection.
        Task run = Supervisor.RunAsync(async ct =>
        {
            runs++;
            started.TrySetResult();
            try
            {
                await Task.Delay(Timeout.Infinite, ct);
            }
            catch (OperationCanceledException)
            {
                throw new IOException("connection closed");
            }
        }, policy, cts.Token);
        await started.Task.WaitAsync(Guard);
        cts.Cancel();

        await Assert.ThrowsAsync<IOException>(() => run.WaitAsync(Guard));
        Assert.Equal(1, runs);
        Assert.Equal(0, restarts);
    }

    [Fact]
    public async Task CallerCancellation_StopsTheWaitBeforeARestart()
    {
        var time = new FakeTimeProvider();
        using var cts = new CancellationTokenSource();
        var waiting = new TaskCompletionSource();
        var policy = new RestartPolicy { InitialDelay = TimeSpan.FromHours(1), TimeProvider = time, OnRestart = _ => waiting.SetResult() };

        Task run = Supervisor.RunAsync(_ => throw new Boom(1), policy, cts.Token);
        await waiting.Task.WaitAsync(Guard);
        cts.Cancel();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => run.WaitAsync(Guard));
    }

    [Fact]
    public async Task OwnCancellation_CountsAsAFailure()
    {
        int runs = 0;

        await Supervisor.RunAsync(_ => ++runs == 1 ? Task.FromCanceled(new CancellationToken(true)) : Task.CompletedTask, NoDelay())
            .WaitAsync(Guard);

        Assert.Equal(2, runs);
    }

    [Fact]
    public async Task RunGroup_RestartsTheWholeGroup_WhenAChildFails()
    {
        int bodies = 0, siblingsCancelled = 0;

        await Supervisor.RunGroupAsync(group =>
        {
            int body = Interlocked.Increment(ref bodies);
            group.Run(async ct =>
            {
                try
                {
                    await Task.Delay(Timeout.Infinite, ct);
                }
                catch (OperationCanceledException)
                {
                    Interlocked.Increment(ref siblingsCancelled);
                    throw;
                }
            });
            group.Run(async _ =>
            {
                await Task.Yield();
                if (body < 3)
                {
                    throw new Boom(body);
                }
                group.Cancel();   // the third group is done
            });
            return Task.CompletedTask;
        }, NoDelay()).WaitAsync(Guard);

        Assert.Equal(3, bodies);
        Assert.Equal(3, siblingsCancelled);
    }

    [Fact]
    public void InvalidPolicies_Throw()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => new RestartPolicy { MaxRestarts = -1 });
        Assert.Throws<ArgumentOutOfRangeException>(() => new RestartPolicy { Window = TimeSpan.Zero });
        Assert.Throws<ArgumentOutOfRangeException>(() => new RestartPolicy { InitialDelay = Timeout.InfiniteTimeSpan });
        Assert.Throws<ArgumentOutOfRangeException>(() => new RestartPolicy { MaxDelay = TimeSpan.FromSeconds(-1) });
        Assert.Throws<ArgumentOutOfRangeException>(() => new RestartPolicy { BackoffMultiplier = 0.5 });
        Assert.Throws<ArgumentOutOfRangeException>(() => new RestartPolicy { BackoffMultiplier = double.NaN });
        _ = new RestartPolicy { Window = Timeout.InfiniteTimeSpan, MaxRestarts = 0, BackoffMultiplier = 1 };
    }

    // A worker whose first two groups fail; the third runs until the host stops.
    private sealed class Restarting : StructuredBackgroundService
    {
        public int Bodies;

        public TaskCompletionSource Settled { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        protected override RestartPolicy RestartPolicy => NoDelay();

        protected override Task ExecuteAsync(TaskGroup group)
        {
            int body = Interlocked.Increment(ref Bodies);
            group.Run(async ct =>
            {
                await Task.Yield();
                if (body < 3)
                {
                    throw new Boom(body);
                }
                Settled.SetResult();
                await Task.Delay(Timeout.Infinite, ct);
            });
            return Task.CompletedTask;
        }
    }

    [Fact]
    public async Task BackgroundService_WithARestartPolicy_RestartsItsWork()
    {
        var service = new Restarting();
        await service.StartAsync(CancellationToken.None);
        await service.Settled.Task.WaitAsync(Guard);

        await service.StopAsync(CancellationToken.None).WaitAsync(Guard);

        Assert.Equal(3, service.Bodies);
        Assert.True(service.ExecuteTask!.IsCanceled);
    }

    private static async Task WaitUntil(Func<bool> condition)
    {
        using var timeout = new CancellationTokenSource(Guard);
        while (!condition())
        {
            await Task.Delay(1, timeout.Token);
        }
    }
}
