# StructuredConcurrency

**Structured concurrency for C# and .NET.** Run tasks in parallel inside a `TaskGroup`. The first failure cancels the other tasks, no task keeps running after the group returns, and you get the real error back instead of a `TaskCanceledException`.

```
dotnet add package StructuredConcurrency
```

## Promise.all, Promise.any and Promise.allSettled in C#

```csharp
using StructuredConcurrency;

// Promise.all: all results; the first failure cancels the rest
var (user, orders) = await TaskGroup.AllAsync(
    ct => GetUserAsync(id, ct),
    ct => GetOrdersAsync(id, ct));

// Promise.any: the first success; the others are cancelled
string data = await TaskGroup.AnyAsync(mirrors, (mirror, ct) => mirror.DownloadAsync(path, ct));

// Promise.allSettled: every outcome
TaskOutcome<Receipt>[] results = await TaskGroup.AllSettledAsync(orders, (order, ct) => ChargeAsync(order, ct));
```

## Why not Task.WhenAll?

Three parallel calls; one fails after 300 ms, the others take 1.5 s and 2 s:

| Approach | Caller gets the error at | Error the caller sees | Work left running |
| --- | --- | --- | --- |
| `Task.WhenAll` | 2,012 ms | OrderService is down | None, but 1.7 s of wasted calls |
| `await` one by one | 301 ms | OrderService is down | 2 calls nobody waits for |
| Hand-written `CancellationTokenSource` | 310 ms | A task was canceled. | None |
| `TaskGroup` | 320 ms | OrderService is down | None |

`Task.WhenAny` has the opposite problem: it returns the first task to *complete*, even if it failed, and leaves the rest running.

## TaskGroup

```csharp
await TaskGroup.RunAsync(async group =>
{
    foreach (var url in urls)
    {
        group.Run(ct => DownloadAsync(url, ct));
    }
}, new TaskGroupOptions { MaxConcurrency = 8 }, cancellationToken);
// Every download has finished here, or the first failure was thrown after the rest were cancelled.
```

## Timeouts that cancel the work

`task.WaitAsync(timeout)` stops waiting, but the task keeps running. `WithTimeoutAsync` cancels it and returns once it has stopped, like Kotlin's `withTimeout`:

```csharp
User user = await TaskGroup.WithTimeoutAsync(TimeSpan.FromSeconds(2), ct => LoadUserAsync(id, ct), cancellationToken);
```

## Also included

- **Hedged requests:** `TaskGroup.HedgeAsync` starts a second attempt when the first is slow
- **Supervisors:** `Supervisor.RunAsync` restarts failing work with backoff, like Erlang/OTP
- **Rate limiting:** `TaskGroupOptions.RateLimiter` with `System.Threading.RateLimiting`
- **Bounded parallel streams:** `SelectConcurrentAsync` over collections and `IAsyncEnumerable`, and `ChunkAsync`
- **Go's singleflight** (`SingleFlight<TKey, TValue>`) and per-key ordering (`KeyedSequencer<TKey>`)
- **Go's select** for `System.Threading.Channels`, plus merge and broadcast
- **OpenTelemetry** tracing and metrics
- **`StructuredBackgroundService`** in the [StructuredConcurrency.Hosting](https://www.nuget.org/packages/StructuredConcurrency.Hosting) package: a `BackgroundService` that shuts down cleanly
- **Analyzers with code fixes** that catch ignored cancellation tokens, work that escapes its group, and `Task.WhenAll` calls that don't cancel the rest on failure

Targets .NET 8 and later; trimming and NativeAOT compatible.

**Full documentation, FAQ and benchmarks: [github.com/gokhanertogan/StructuredConcurrency](https://github.com/gokhanertogan/StructuredConcurrency)**
