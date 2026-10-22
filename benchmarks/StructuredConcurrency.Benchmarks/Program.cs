using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Running;
using StructuredConcurrency;

BenchmarkSwitcher.FromAssembly(typeof(FanOut).Assembly).Run(args);

// Starting N children and waiting for all of them. Plain Task.WhenAll is the lower bound: it neither cancels on failure
// nor guards against leaks. The hand-written version is what TaskGroup replaces: a CancellationTokenSource that the
// first failure cancels. Delegates are created once, so only the cost of the pattern itself is measured.
[MemoryDiagnoser]
public class FanOut
{
    [Params(2, 10, 100)]
    public int Children;

    [Params(false, true)]
    public bool Yield;

    private Func<CancellationToken, Task> _work = null!;
    private Func<TaskGroup, Task> _body = null!;

    [GlobalSetup]
    public void Setup()
    {
        _work = Yield ? static async _ => await Task.Yield() : static _ => Task.CompletedTask;
        _body = group =>
        {
            for (int i = 0; i < Children; i++)
            {
                group.Run(_work);
            }
            return Task.CompletedTask;
        };
    }

    [Benchmark(Baseline = true, Description = "Task.WhenAll (no cancellation)")]
    public Task WhenAll()
    {
        var tasks = new Task[Children];
        for (int i = 0; i < tasks.Length; i++)
        {
            tasks[i] = _work(CancellationToken.None);
        }
        return Task.WhenAll(tasks);
    }

    [Benchmark(Description = "Hand-written fail-fast")]
    public async Task HandWritten()
    {
        using var cts = new CancellationTokenSource();
        var tasks = new Task[Children];
        for (int i = 0; i < tasks.Length; i++)
        {
            Task task = _work(cts.Token);
            tasks[i] = task;
            if (task.IsFaulted)
            {
                cts.Cancel();
            }
            else if (!task.IsCompleted)
            {
                _ = task.ContinueWith(static (_, state) => ((CancellationTokenSource)state!).Cancel(), cts,
                    CancellationToken.None, TaskContinuationOptions.OnlyOnFaulted | TaskContinuationOptions.ExecuteSynchronously,
                    TaskScheduler.Default);
            }
        }
        await Task.WhenAll(tasks);
    }

    [Benchmark(Description = "TaskGroup.RunAsync")]
    public Task Group() => TaskGroup.RunAsync(_body);
}

// The first successful result of three operations that each yield once.
[MemoryDiagnoser]
public class FirstSuccess
{
    private static readonly Func<CancellationToken, Task<int>>[] s_operations =
    [
        static async _ => { await Task.Yield(); return 1; },
        static async _ => { await Task.Yield(); return 2; },
        static async _ => { await Task.Yield(); return 3; },
    ];

    [Benchmark(Baseline = true, Description = "Hand-written (WhenEach + CTS)")]
    public async Task<int> HandWritten()
    {
        using var cts = new CancellationTokenSource();
        var tasks = new Task<int>[s_operations.Length];
        for (int i = 0; i < tasks.Length; i++)
        {
            tasks[i] = s_operations[i](cts.Token);
        }

        int? winner = null;
        var failures = new List<Exception>();
        await foreach (Task<int> task in Task.WhenEach(tasks))
        {
            if (task.IsCompletedSuccessfully)
            {
                winner ??= task.Result;
                cts.Cancel();
            }
            else if (winner is null)
            {
                failures.Add(task.Exception?.InnerException ?? new TaskCanceledException(task));
            }
        }
        return winner ?? throw new AggregateException(failures);
    }

    [Benchmark(Description = "TaskGroup.AnyAsync")]
    public Task<int> Group() => TaskGroup.AnyAsync(s_operations);
}
