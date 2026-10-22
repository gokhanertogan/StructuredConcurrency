// Three mirrors serve the same data. A fails after 100 ms, B succeeds after 300 ms, C succeeds after 2 s.
// We want the first successful answer, the same as JavaScript's Promise.any.
using System.Diagnostics;
using StructuredConcurrency;

var clock = Stopwatch.StartNew();

void Log(string who, string what) =>
    Console.WriteLine($"[{clock.ElapsedMilliseconds,5} ms] {who,-8} {what}");

async Task<string> Mirror(string name, int durationMs, bool fails, CancellationToken ct)
{
    try
    {
        await Task.Delay(durationMs, ct);
    }
    catch (OperationCanceledException)
    {
        Log(name, "cancelled");
        throw;
    }

    if (fails)
    {
        Log(name, "FAILED");
        throw new HttpRequestException($"mirror {name} is down");
    }

    Log(name, "succeeded");
    return $"data from {name}";
}

// ---------------------------------------------------------------------------------------------
Console.WriteLine("AS-IS (A): Task.WhenAny returns the first task to COMPLETE, even if it failed");
clock.Restart();
try
{
    Task<string> first = await Task.WhenAny(
        Mirror("A", 100, fails: true, CancellationToken.None),
        Mirror("B", 300, fails: false, CancellationToken.None),
        Mirror("C", 2000, fails: false, CancellationToken.None));
    Log("Caller", $"result: {await first}");
}
catch (Exception e)
{
    Log("Caller", $"got error: {e.Message}");
}
await Task.Delay(2100); // just to show what keeps happening in the background

// ---------------------------------------------------------------------------------------------
Console.WriteLine("\nAS-IS (B): Task.WhenEach (.NET 9) can skip failures, but nothing stops the losers");
clock.Restart();
{
    var tasks = new[]
    {
        Mirror("A", 100, fails: true, CancellationToken.None),
        Mirror("B", 300, fails: false, CancellationToken.None),
        Mirror("C", 2000, fails: false, CancellationToken.None),
    };
    await foreach (Task<string> task in Task.WhenEach(tasks))
    {
        if (task.IsCompletedSuccessfully)
        {
            Log("Caller", $"result: {task.Result}");
            break;
        }
    }
}
await Task.Delay(2100);

// ---------------------------------------------------------------------------------------------
Console.WriteLine("\nTO-BE: TaskGroup.AnyAsync returns the first SUCCESS and cancels the rest");
clock.Restart();
string data = await TaskGroup.AnyAsync(
    ct => Mirror("A", 100, fails: true, ct),
    ct => Mirror("B", 300, fails: false, ct),
    ct => Mirror("C", 2000, fails: false, ct));
Log("Caller", $"result: {data}");
await Task.Delay(2100); // nothing else will be logged: no work was left behind
Console.WriteLine("\n(no background activity after AnyAsync returned)");
