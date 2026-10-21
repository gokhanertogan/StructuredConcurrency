// A request calls three services in parallel. OrderService fails after 300 ms;
// the other two are slow (1.5 s and 2 s). The same request is written three ways.
using System.Diagnostics;
using StructuredConcurrency;

var clock = Stopwatch.StartNew();

void Log(string who, string what) =>
    Console.WriteLine($"[{clock.ElapsedMilliseconds,5} ms] {who,-16} {what}");

async Task<string> CallService(string name, int durationMs, bool fails, CancellationToken ct)
{
    Log(name, "started");
    try
    {
        await Task.Delay(durationMs, ct);
    }
    catch (OperationCanceledException)
    {
        Log(name, "cancelled, stopped early");
        throw;
    }

    if (fails)
    {
        Log(name, "FAILED");
        throw new HttpRequestException($"{name} is down");
    }

    Log(name, "finished");
    return $"{name} data";
}

// ---------------------------------------------------------------------------------------------
Console.WriteLine("AS-IS (A): Task.WhenAll");
Console.WriteLine("The request has already failed at 300 ms, but the caller waits for the slow calls anyway.\n");
clock.Restart();
try
{
    Task<string> user = CallService("UserService", 2000, fails: false, CancellationToken.None);
    Task<string> orders = CallService("OrderService", 300, fails: true, CancellationToken.None);
    Task<string> recs = CallService("Recommendations", 1500, fails: false, CancellationToken.None);
    await Task.WhenAll(user, orders, recs);
}
catch (Exception e)
{
    Log("Caller", $"got error: {e.Message}");
}

// ---------------------------------------------------------------------------------------------
Console.WriteLine("\nAS-IS (B): await the tasks one by one");
Console.WriteLine("The caller gets the error at 300 ms, but the other calls keep running with nobody waiting.\n");
clock.Restart();
try
{
    Task<string> user = CallService("UserService", 2000, fails: false, CancellationToken.None);
    Task<string> orders = CallService("OrderService", 300, fails: true, CancellationToken.None);
    Task<string> recs = CallService("Recommendations", 1500, fails: false, CancellationToken.None);
    string o = await orders;
    string u = await user;
    string r = await recs;
}
catch (Exception e)
{
    Log("Caller", $"got error: {e.Message}, returns to its caller");
}
await Task.Delay(2200); // just to show what keeps happening in the background

// ---------------------------------------------------------------------------------------------
Console.WriteLine("\nAS-IS (C): a hand-written CancellationTokenSource");
Console.WriteLine("Cancellation and cleanup are wired by hand. The calls stop early, but look at which error the caller gets.\n");
clock.Restart();
try
{
    using var cts = new CancellationTokenSource();
    var started = new List<Task>();

    Task<string> Start(Func<CancellationToken, Task<string>> work)
    {
        Task<string> task = work(cts.Token);
        started.Add(task);
        // Cancel the siblings as soon as any call fails.
        _ = task.ContinueWith(_ => cts.Cancel(), CancellationToken.None,
            TaskContinuationOptions.OnlyOnFaulted | TaskContinuationOptions.ExecuteSynchronously, TaskScheduler.Default);
        return task;
    }

    try
    {
        Task<string> user = Start(ct => CallService("UserService", 2000, fails: false, ct));
        Task<string> orders = Start(ct => CallService("OrderService", 300, fails: true, ct));
        Task<string> recs = Start(ct => CallService("Recommendations", 1500, fails: false, ct));
        string u = await user;
        string o = await orders;
        string r = await recs;
    }
    finally
    {
        // Never leave a call running: stop and wait for all of them, whatever happened.
        cts.Cancel();
        try { await Task.WhenAll(started); } catch { }
    }
}
catch (Exception e)
{
    Log("Caller", $"got error: {e.GetType().Name}: {e.Message}");
}

// ---------------------------------------------------------------------------------------------
Console.WriteLine("\nTO-BE: TaskGroup");
Console.WriteLine("The first failure cancels the rest, and the caller gets the error only after they have stopped.\n");
clock.Restart();
try
{
    var (u, o, r) = await TaskGroup.RunAsync(async group =>
    {
        Task<string> user = group.Run(ct => CallService("UserService", 2000, fails: false, ct));
        Task<string> orders = group.Run(ct => CallService("OrderService", 300, fails: true, ct));
        Task<string> recs = group.Run(ct => CallService("Recommendations", 1500, fails: false, ct));
        return (await user, await orders, await recs);
    });
}
catch (Exception e)
{
    Log("Caller", $"got error: {e.Message}");
}
await Task.Delay(2200); // nothing else will be logged: no work was left behind
Console.WriteLine("\n(no background activity after the TaskGroup returned)");
