// A report takes 2.4 s to build, in three steps of 800 ms, but the caller only waits 1 s for it.
// We want the call to give up after 1 s AND the report to stop, like Kotlin's withTimeout.
using System.Diagnostics;
using StructuredConcurrency;

var clock = Stopwatch.StartNew();

void Log(string who, string what) =>
    Console.WriteLine($"[{clock.ElapsedMilliseconds,5} ms] {who,-8} {what}");

async Task<string> BuildReport(CancellationToken ct)
{
    try
    {
        for (int step = 1; step <= 3; step++)
        {
            await Task.Delay(800, ct);
            Log("Report", $"step {step} of 3 done");
        }
    }
    catch (OperationCanceledException)
    {
        Log("Report", "cancelled");
        throw;
    }
    return "report";
}

// ---------------------------------------------------------------------------------------------
Console.WriteLine("AS-IS (A): Task.WaitAsync stops WAITING after 1 s, but the report keeps running");
clock.Restart();
try
{
    Log("Caller", $"result: {await BuildReport(CancellationToken.None).WaitAsync(TimeSpan.FromSeconds(1))}");
}
catch (TimeoutException)
{
    Log("Caller", "timed out");
}
await Task.Delay(2500); // just to show what keeps happening in the background

// ---------------------------------------------------------------------------------------------
Console.WriteLine("\nAS-IS (B): CancelAfter stops the report, but the caller can't tell a timeout from a cancellation");
clock.Restart();
try
{
    using var cts = new CancellationTokenSource();
    cts.CancelAfter(TimeSpan.FromSeconds(1));
    Log("Caller", $"result: {await BuildReport(cts.Token)}");
}
catch (OperationCanceledException e)
{
    Log("Caller", $"got {e.GetType().Name}: did it time out, or did someone cancel?");
}
await Task.Delay(2500);

// ---------------------------------------------------------------------------------------------
Console.WriteLine("\nTO-BE: TaskGroup.WithTimeoutAsync cancels the report and throws TimeoutException once it has stopped");
clock.Restart();
try
{
    Log("Caller", $"result: {await TaskGroup.WithTimeoutAsync(TimeSpan.FromSeconds(1), ct => BuildReport(ct))}");
}
catch (TimeoutException)
{
    Log("Caller", "timed out");
}
await Task.Delay(2500); // nothing else will be logged: no work was left behind
Console.WriteLine("\n(no background activity after WithTimeoutAsync returned)");
