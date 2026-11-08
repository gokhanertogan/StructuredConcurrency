namespace StructuredConcurrency.Tests;

// The other tests drive every interleaving by hand. These run the same guarantees on the real thread pool, many times
// over, with random delays, failures and cancellations, to catch races that only show up under some orderings.
// Set STRESS_ITERATIONS to run more iterations locally; CI uses the default.
[Trait("Category", "Stress")]
public class StressTests
{
    private static readonly TimeSpan Guard = TimeSpan.FromSeconds(10);

    private static readonly int Iterations =
        int.TryParse(Environment.GetEnvironmentVariable("STRESS_ITERATIONS"), out int n) && n > 0 ? n : 100;

    private sealed class Boom(int id) : Exception($"boom {id}")
    {
        public int Id { get; } = id;
    }

    // Yields, spins or briefly sleeps, so that each iteration interleaves differently.
    private static async Task Jitter(Random random, CancellationToken ct = default)
    {
        switch (random.Next(4))
        {
            case 0:
                await Task.Yield();
                break;
            case 1:
                Thread.SpinWait(random.Next(10, 2_000));
                break;
            case 2:
                await Task.Delay(1, ct);
                break;
        }
    }

    [Fact]
    public async Task Group_ConcurrentFailures_ThrowAFailureAndLeaveNothingRunning()
    {
        for (int i = 0; i < Iterations; i++)
        {
            var random = new Random(i);
            int active = 0;
            bool anyFails = false;

            Task run = TaskGroup.RunAsync(group =>
            {
                for (int child = 0; child < 20; child++)
                {
                    int id = child;
                    bool fails = random.Next(5) == 0;
                    anyFails |= fails;
                    var childRandom = new Random(random.Next());
                    group.Run(async ct =>
                    {
                        Interlocked.Increment(ref active);
                        try
                        {
                            await Task.Run(() => Jitter(childRandom, ct), CancellationToken.None);
                            if (fails)
                            {
                                throw new Boom(id);
                            }
                            await Task.Delay(childRandom.Next(0, 3), ct);
                        }
                        finally
                        {
                            Interlocked.Decrement(ref active);
                        }
                    });
                }
                return Task.CompletedTask;
            });

            Exception? thrown = await Record.ExceptionAsync(() => run.WaitAsync(Guard));

            Assert.Equal(0, Volatile.Read(ref active));
            if (anyFails)
            {
                Assert.IsType<Boom>(thrown);
                Assert.All(run.Exception!.InnerExceptions, e => Assert.IsType<Boom>(e));
            }
            else
            {
                Assert.Null(thrown);
            }
        }
    }

    [Fact]
    public async Task Group_CallerCancellationRacingAFailure_LeavesNothingRunning()
    {
        for (int i = 0; i < Iterations; i++)
        {
            var random = new Random(i);
            int active = 0;
            using var cts = new CancellationTokenSource();

            Task run = TaskGroup.RunAsync(group =>
            {
                for (int child = 0; child < 10; child++)
                {
                    int id = child;
                    var childRandom = new Random(random.Next());
                    group.Run(async ct =>
                    {
                        Interlocked.Increment(ref active);
                        try
                        {
                            await Task.Run(() => Jitter(childRandom, ct), CancellationToken.None);
                            if (id == 0)
                            {
                                throw new Boom(id);
                            }
                            await Task.Delay(Timeout.Infinite, ct);
                        }
                        finally
                        {
                            Interlocked.Decrement(ref active);
                        }
                    });
                }
                return Task.CompletedTask;
            }, cts.Token);

            _ = Task.Run(async () =>
            {
                await Jitter(new Random(i + 1));
                cts.Cancel();
            });

            Exception? thrown = await Record.ExceptionAsync(() => run.WaitAsync(Guard));

            Assert.Equal(0, Volatile.Read(ref active));
            // Whichever came first wins; the children cancelled by the group never surface their own cancellation.
            Assert.True(thrown is Boom or OperationCanceledException, $"Unexpected {thrown}");
            if (thrown is OperationCanceledException)
            {
                Assert.True(cts.IsCancellationRequested);
            }
        }
    }

    [Fact]
    public async Task Group_MaxConcurrency_IsNeverExceeded_AndQueuedWorkNeverStartsAfterReturn()
    {
        const int limit = 4;
        for (int i = 0; i < Iterations; i++)
        {
            var random = new Random(i);
            int running = 0;
            int peak = 0;
            int returned = 0;
            int startedAfterReturn = 0;

            Task run = TaskGroup.RunAsync(group =>
            {
                for (int child = 0; child < 30; child++)
                {
                    int id = child;
                    bool fails = id == 10;
                    var childRandom = new Random(random.Next());
                    group.Run(async ct =>
                    {
                        if (Volatile.Read(ref returned) == 1)
                        {
                            Interlocked.Increment(ref startedAfterReturn);
                        }
                        int now = Interlocked.Increment(ref running);
                        InterlockedMax(ref peak, now);
                        try
                        {
                            await Task.Run(() => Jitter(childRandom, ct), CancellationToken.None);
                            if (fails)
                            {
                                throw new Boom(id);
                            }
                        }
                        finally
                        {
                            Interlocked.Decrement(ref running);
                        }
                    });
                }
                return Task.CompletedTask;
            }, new TaskGroupOptions { MaxConcurrency = limit });

            await Assert.ThrowsAsync<Boom>(() => run.WaitAsync(Guard));
            Volatile.Write(ref returned, 1);
            await Task.Delay(5);

            Assert.InRange(peak, 1, limit);
            Assert.Equal(0, Volatile.Read(ref running));
            Assert.Equal(0, Volatile.Read(ref startedAfterReturn));
        }
    }

    [Fact]
    public async Task FirstSuccess_RacingSuccessesAndFailures_ReturnsASuccessAndStopsTheRest()
    {
        for (int i = 0; i < Iterations; i++)
        {
            var random = new Random(i);
            int active = 0;
            int[] ids = Enumerable.Range(0, 8).ToArray();
            var failing = ids.Where(_ => random.Next(2) == 0).ToHashSet();
            bool allFail = failing.Count == ids.Length;
            var seeds = ids.ToDictionary(id => id, _ => random.Next());

            Task<int> first = TaskGroup.FirstSuccessAsync(ids, async (id, ct) =>
            {
                Interlocked.Increment(ref active);
                try
                {
                    await Task.Run(() => Jitter(new Random(seeds[id]), ct), CancellationToken.None);
                    if (failing.Contains(id))
                    {
                        throw new Boom(id);
                    }
                    return id;
                }
                finally
                {
                    Interlocked.Decrement(ref active);
                }
            });

            Exception? thrown = await Record.ExceptionAsync(() => first.WaitAsync(Guard));

            Assert.Equal(0, Volatile.Read(ref active));
            if (allFail)
            {
                var aggregate = Assert.IsType<AggregateException>(thrown);
                Assert.Equal(ids.Length, aggregate.InnerExceptions.Count);
            }
            else
            {
                Assert.Null(thrown);
                Assert.DoesNotContain(await first, failing);
            }
        }
    }

    [Fact]
    public async Task SingleFlight_CallersGivingUpAtRandom_NeverOverlapAndNeverOutliveTheirCallers()
    {
        for (int i = 0; i < Iterations; i++)
        {
            var random = new Random(i);
            var flight = new SingleFlight<string, int>();
            int running = 0;
            int overlaps = 0;

            Task[] callers = Enumerable.Range(0, 16).Select(caller =>
            {
                var callerRandom = new Random(random.Next());
                bool givesUp = callerRandom.Next(3) == 0;
                return Task.Run(async () =>
                {
                    using var cts = new CancellationTokenSource();
                    if (givesUp)
                    {
                        cts.CancelAfter(callerRandom.Next(0, 3));
                    }
                    await Jitter(callerRandom);
                    try
                    {
                        int value = await flight.RunAsync("key", async ct =>
                        {
                            if (Interlocked.Increment(ref running) > 1)
                            {
                                Interlocked.Increment(ref overlaps);
                            }
                            try
                            {
                                await Task.Delay(callerRandom.Next(0, 3), ct);
                                return 42;
                            }
                            finally
                            {
                                Interlocked.Decrement(ref running);
                            }
                        }, cts.Token);
                        Assert.Equal(42, value);
                    }
                    catch (OperationCanceledException) when (givesUp)
                    {
                    }
                });
            }).ToArray();

            await Task.WhenAll(callers).WaitAsync(Guard);

            Assert.Equal(0, Volatile.Read(ref overlaps));
            Assert.Equal(0, Volatile.Read(ref running));
        }
    }

    [Fact]
    public async Task KeyedSequencer_SameKeyNeverOverlapsAndRunsInOrder_UnderLoad()
    {
        for (int i = 0; i < Iterations; i++)
        {
            var random = new Random(i);
            var sequencer = new KeyedSequencer<int>();
            const int keys = 4;
            int[] running = new int[keys];
            int overlaps = 0;
            var order = Enumerable.Range(0, keys).Select(_ => new List<int>()).ToArray();
            var calls = new List<Task>();

            // Asked for from one thread, so the order per key is well defined.
            for (int call = 0; call < 40; call++)
            {
                int key = random.Next(keys);
                int seq = call;
                var callRandom = new Random(random.Next());
                calls.Add(sequencer.RunAsync(key, async ct =>
                {
                    if (Interlocked.Increment(ref running[key]) > 1)
                    {
                        Interlocked.Increment(ref overlaps);
                    }
                    try
                    {
                        lock (order[key])
                        {
                            order[key].Add(seq);
                        }
                        await Task.Run(() => Jitter(callRandom, ct), CancellationToken.None);
                    }
                    finally
                    {
                        Interlocked.Decrement(ref running[key]);
                    }
                }));
            }

            await Task.WhenAll(calls).WaitAsync(Guard);

            Assert.Equal(0, overlaps);
            Assert.All(running, r => Assert.Equal(0, r));
            Assert.All(order, sequence => Assert.Equal(sequence.Order(), sequence));
        }
    }

    private static void InterlockedMax(ref int location, int value)
    {
        int current = Volatile.Read(ref location);
        while (value > current)
        {
            int previous = Interlocked.CompareExchange(ref location, value, current);
            if (previous == current)
            {
                return;
            }
            current = previous;
        }
    }
}
