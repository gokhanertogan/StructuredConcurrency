# StructuredConcurrency: TaskGroup and Promise.all / Promise.any for C# and .NET

[![NuGet](https://img.shields.io/nuget/v/StructuredConcurrency.svg)](https://www.nuget.org/packages/StructuredConcurrency)
[![Downloads](https://img.shields.io/nuget/dt/StructuredConcurrency.svg)](https://www.nuget.org/packages/StructuredConcurrency)
[![CI](https://github.com/gokhanertogan/StructuredConcurrency/actions/workflows/ci.yml/badge.svg)](https://github.com/gokhanertogan/StructuredConcurrency/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

**Structured concurrency for C#.** Run tasks in parallel inside a `TaskGroup`. The first failure cancels the other tasks, no task keeps running after the group returns, and you get the real error back instead of a `TaskCanceledException`. It includes C# versions of JavaScript's `Promise.all`, `Promise.any` and `Promise.allSettled`, Kotlin's `withTimeout`, Go's `select` and `singleflight`, and the same model as Python's `asyncio.TaskGroup`, Kotlin's `coroutineScope`, Swift's task groups, Java's `StructuredTaskScope` and Go's `errgroup`.

```
dotnet add package StructuredConcurrency
```

## Promise.all, Promise.any and Promise.allSettled in C#

If you know JavaScript promises, you already know the API. Unlike JavaScript, the tasks that are no longer needed are actually cancelled.

**`Promise.all` → `TaskGroup.AllAsync`.** Run tasks in parallel and get all results; the first failure cancels the rest.

```js
// JavaScript
const [user, orders] = await Promise.all([getUser(id), getOrders(id)]);
```

```csharp
// C#
var (user, orders) = await TaskGroup.AllAsync(
    ct => GetUserAsync(id, ct),
    ct => GetOrdersAsync(id, ct));
```

**`Promise.any` → `TaskGroup.AnyAsync`.** Take the first task that succeeds; the others are cancelled.

```js
// JavaScript
const data = await Promise.any(mirrors.map(m => m.download(path)));
```

```csharp
// C#
string data = await TaskGroup.AnyAsync(mirrors, (mirror, ct) => mirror.DownloadAsync(path, ct));
```

**`Promise.allSettled` → `TaskGroup.AllSettledAsync`.** Run every task to completion and look at each outcome.

```js
// JavaScript
const results = await Promise.allSettled(orders.map(o => charge(o)));
```

```csharp
// C#
TaskOutcome<Receipt>[] results = await TaskGroup.AllSettledAsync(orders, (order, ct) => ChargeAsync(order, ct));
```

| JavaScript | StructuredConcurrency | Built-in .NET, and what it doesn't do |
| --- | --- | --- |
| `Promise.all` | `TaskGroup.AllAsync` | `Task.WhenAll` doesn't cancel the other tasks when one fails |
| `Promise.any` | `TaskGroup.AnyAsync` | No equivalent. `Task.WhenAny` returns the first task to complete, even if it failed |
| `Promise.allSettled` | `TaskGroup.AllSettledAsync` | `Task.WhenAll`, then inspect each task by hand |
| `Promise.race` | Not needed | `Task.WhenAny` |

## Why: the bugs in Task.WhenAll and Task.WhenAny code

Take a request that calls three services in parallel. One fails after 300 ms, and the other two take 1.5 s and 2 s:

| Approach | Caller gets the error at | Error the caller sees | Work left running |
| --- | --- | --- | --- |
| `Task.WhenAll` | 2,012 ms | OrderService is down | None, but 1.7 s of calls whose results are discarded |
| `await` one by one | 301 ms | OrderService is down | 2 calls keep running with nobody waiting |
| Hand-written `CancellationTokenSource` | 310 ms | A task was canceled. | None |
| `TaskGroup` | 320 ms | OrderService is down | None |

The hand-written version cancels correctly but reports the wrong error: it awaits a task that was cancelled *because* another one failed. See [`samples/BeforeAfter`](samples/BeforeAfter/Program.cs).

`Task.WhenAny` has the opposite problem. With three mirrors, where A fails after 100 ms, B succeeds after 300 ms and C succeeds after 2 s:

| Approach | Caller gets | At | Work left running |
| --- | --- | --- | --- |
| `Task.WhenAny` | A's error, even though B would succeed | 104 ms | B and C, with nobody waiting |
| `Task.WhenEach` + skip failures | B's data | 301 ms | C, with nobody waiting |
| `TaskGroup.AnyAsync` | B's data | 334 ms | None: C is cancelled |

See [`samples/AnyVsWhenAny`](samples/AnyVsWhenAny/Program.cs).

## TaskGroup: structured concurrency in C#

For anything beyond the three helpers, open a group and start children in it:

```csharp
using StructuredConcurrency;

await TaskGroup.RunAsync(async group =>
{
    foreach (var url in urls)
    {
        group.Run(ct => DownloadAsync(url, ct));
    }
}, cancellationToken);
// Every download has finished here, or the first failure was thrown after the rest were cancelled.
```

Each child receives the group's `CancellationToken`. Children can start more children, and groups can be nested.

### Limit how many tasks run at once

Set `MaxConcurrency` to run at most N children at the same time, like Go's `errgroup.SetLimit`. `group.Run` never blocks: work beyond the limit waits in a queue and starts in order as slots free up. If the group fails or is cancelled, queued work is never started.

```csharp
await TaskGroup.RunAsync(async group =>
{
    foreach (var url in urls)
    {
        group.Run(ct => DownloadAsync(url, ct));   // at most 8 downloads at a time
    }
}, new TaskGroupOptions { MaxConcurrency = 8 }, cancellationToken);

// Or get the results back, in input order:
byte[][] files = await TaskGroup.AllAsync(urls, (url, ct) => DownloadAsync(url, ct), maxConcurrency: 8, cancellationToken);
```

### Stream the results as they are ready

`SelectConcurrentAsync` runs an operation for every item with a limit and yields each result as soon as it can, like Kotlin's `flatMapMerge`, Rust's `buffered` / `buffer_unordered` and Python's `asyncio.as_completed`. Results come in input order by default; pass `preserveOrder: false` to get them as they complete. Items are read only as slots free up, so the input can be long or endless.

```csharp
await foreach (Page page in urls.SelectConcurrentAsync((url, ct) => FetchAsync(url, ct), maxConcurrency: 8)
                                .WithCancellation(cancellationToken))
{
    if (page.HasWhatWeNeed)
    {
        break;   // the fetches still running are cancelled and awaited before the loop exits
    }
}
```

`Parallel.ForEachAsync` returns no results, and .NET 9's `Task.WhenEach` has no limit and leaves the remaining tasks running when you `break`. Here, ending the loop for any reason (a `break`, the first failure or cancellation) cancels the operations still running and waits for them.

## Timeouts that cancel the work

`task.WaitAsync(timeout)` stops *waiting* when the time is up, but the task keeps running with nobody watching it. `TaskGroup.WithTimeoutAsync` cancels the operation's token when the time is up and returns only after the operation has stopped, like Kotlin's `withTimeout`, Python's `asyncio.timeout` and Trio's `fail_after`.

```csharp
// Throws TimeoutException after 2 s, once LoadUserAsync has stopped
User user = await TaskGroup.WithTimeoutAsync(TimeSpan.FromSeconds(2), ct => LoadUserAsync(id, ct), cancellationToken);

// Kotlin's withTimeoutOrNull: a fallback value instead of an exception
User? cached = await TaskGroup.WithTimeoutOrDefaultAsync(TimeSpan.FromMilliseconds(50), ct => cache.GetAsync(id, ct), defaultValue: null);

// Trio's move_on_after: false if the time ran out
bool finished = await TaskGroup.TryWithTimeoutAsync(TimeSpan.FromSeconds(30), ct => WarmUpAsync(ct));
```

To put a deadline on a whole group, wrap it: `TaskGroup.WithTimeoutAsync(timeout, ct => TaskGroup.RunAsync(body, ct))`. Only a cancellation that comes from the timer becomes a `TimeoutException`. Your own token's cancellation, or a timeout inside the operation, comes through unchanged.

## Start a task and wait until it's ready

`group.StartAsync` starts a child and returns once the child signals that it is ready. The child keeps running in the group afterwards. This is Trio's `nursery.start()`, for things like a server that has to be listening before you send it requests:

```csharp
await TaskGroup.RunAsync(async group =>
{
    int port = await group.StartAsync<int>(async (ready, ct) =>
    {
        using var listener = StartListener();
        ready(listener.Port);              // StartAsync returns here...
        await ServeAsync(listener, ct);    // ...while the server keeps running in the group
    });

    await RunClientAsync(port, group.CancellationToken);
    group.Cancel();                        // stop the server
}, cancellationToken);
```

If the child fails before it signals, `StartAsync` throws its exception (and the group fails as usual). If it completes without signalling, `StartAsync` throws `InvalidOperationException`.

## Deduplicate concurrent calls: singleflight

`SingleFlight<TKey, TValue>` is Go's `singleflight.Group`. When several callers ask for the same key at once, only one operation runs and they all get its result. This stops a cache stampede, where a popular entry expires and a hundred requests hit the database for it at the same moment.

```csharp
private readonly SingleFlight<string, Product> _loads = new();

public Task<Product> GetProductAsync(string id, CancellationToken cancellationToken) =>
    _loads.RunAsync(id, ct => _database.LoadProductAsync(id, ct), cancellationToken);
```

Results aren't cached: once the operation completes, the next call starts a new one, so put it behind your cache. Each caller's token stops only that caller's wait. The shared operation is cancelled only when *every* caller has given up, and the last one to give up waits for it to stop. `Forget(key)` makes the next call start fresh even while an operation is running.

## Go channels in C#: select, merge and broadcast

`System.Threading.Channels` gives .NET Go-style channels, but not Go's `select` statement or the fan-in and fan-out patterns built around it. The `StructuredConcurrency.Channels` namespace adds them. None of them leave work running in the background.

**`select` → `ChannelSelect`.** Wait on several channels and handle whichever has an item first. When several are ready, one is picked at random, as in Go, so no channel starves.

```go
// Go
for {
    select {
    case order := <-orders:
        handle(order)
    case id := <-cancels:
        cancel(id)
    case <-time.After(5 * time.Second):
        sendHeartbeat()
    }
}
```

```csharp
// C#
using StructuredConcurrency.Channels;

var select = new ChannelSelect()
    .Case(orders.Reader, order => HandleAsync(order))
    .Case(cancels.Reader, id => CancelAsync(id))
    .Timeout(TimeSpan.FromSeconds(5), () => SendHeartbeatAsync());

while (await select.RunAsync(cancellationToken)) { }   // false once every channel has completed
```

`Default(...)` adds Go's `default` case: it runs when nothing is ready, so `RunAsync` never waits. Once every channel has completed, `RunAsync` returns `false` instead of running the default or timeout case, so the loop above always ends.

**Fan-in → `MergeWith`.** Read several channels as one stream. It reads only while you enumerate, so there is no pump task to leak.

```csharp
await foreach (var e in clicks.Reader.MergeWith(keys.Reader, ticks.Reader).WithCancellation(cancellationToken))
{
    Handle(e);
}
```

**Fan-out → `BroadcastAsync`.** Copy every item to several channels, then complete them. A target whose consumer completes it early just stops receiving items. Run it as a TaskGroup child so it can't outlive the consumers:

```csharp
await TaskGroup.RunAsync(group =>
{
    group.Run(ct => events.Reader.BroadcastAsync([audit.Writer, metrics.Writer], ct));
    group.Run(ct => WriteAuditLogAsync(audit.Reader, ct));
    group.Run(ct => PublishMetricsAsync(metrics.Reader, ct));
    return Task.CompletedTask;
}, cancellationToken);
```

**Producers → `group.Produce`.** Kotlin's `produce { }`: start a producer in the group and get the reader of a new channel. The channel completes when the producer does, with its exception if it fails, so a reader never waits for a producer that has died. The group waits for the producer like any other child, so if you stop reading early, call `group.Cancel()`.

```csharp
await TaskGroup.RunAsync(async group =>
{
    ChannelReader<Order> orders = group.Produce<Order>(async (writer, ct) =>
    {
        await foreach (var order in queue.ReceiveAsync(ct))
        {
            await writer.WriteAsync(order, ct);
        }
    }, capacity: 100);

    await foreach (var order in orders.ReadAllAsync(cancellationToken))
    {
        await HandleAsync(order);
    }
}, cancellationToken);
```

### Coming from another language

| Language | API | In C# |
| --- | --- | --- |
| Python 3.11+ | `asyncio.TaskGroup` | `TaskGroup.RunAsync` |
| Kotlin | `coroutineScope { }` | `TaskGroup.RunAsync` |
| Kotlin | `supervisorScope { }` | `TaskGroup.RunAsync` with `new TaskGroupOptions { CancelOnFailure = false }` |
| Swift | `withThrowingTaskGroup` | `TaskGroup.RunAsync` |
| Java 21+ | `StructuredTaskScope` | `TaskGroup.RunAsync`, `TaskGroup.AnyAsync` |
| Go | `errgroup.WithContext` | `TaskGroup.RunAsync`, `TaskGroup.AllAsync` |
| Go | `errgroup.SetLimit` | `new TaskGroupOptions { MaxConcurrency = n }` |
| Go | `select { case ... }` | `ChannelSelect` |
| Go | Fan-in (merging channels) | `reader.MergeWith(...)` |
| Go | Fan-out (one channel to many) | `reader.BroadcastAsync(...)` |
| Go | `singleflight.Group` | `SingleFlight<TKey, TValue>` |
| Kotlin | `withTimeout`, `withTimeoutOrNull` | `TaskGroup.WithTimeoutAsync`, `TaskGroup.WithTimeoutOrDefaultAsync` |
| Python 3.11+ | `asyncio.timeout` | `TaskGroup.WithTimeoutAsync` |
| Trio | `fail_after`, `move_on_after` | `TaskGroup.WithTimeoutAsync`, `TaskGroup.TryWithTimeoutAsync` |
| Trio | `nursery.start()` | `group.StartAsync` |
| Kotlin | `flatMapMerge(concurrency)` | `items.SelectConcurrentAsync(..., preserveOrder: false)` |
| Rust | `buffered(n)`, `buffer_unordered(n)` | `items.SelectConcurrentAsync(..., maxConcurrency: n)` |
| Python | `asyncio.as_completed` | `items.SelectConcurrentAsync(..., preserveOrder: false)` |
| Kotlin | `produce { }` | `group.Produce(...)` |

## Guarantees

1. **Scope.** `RunAsync` completes only after the body and every child have completed, even when the body throws.
2. **Failure.** The first real failure cancels the group's token, which every child receives.
3. **Cancellation flows down.** The group's token is linked to the caller's token, and a nested group is linked to its parent's.
4. **Errors surface as `await` expects.** `await` rethrows the first failure. The returned task's `Exception` holds every failure in the order they occurred.
5. **Cancellations caused by the group are not failures.** An `OperationCanceledException` thrown after the group was cancelled is dropped, so you see why the group stopped. A child's own cancellation, such as an `HttpClient` timeout, is reported as a failure.

## FAQ

### How do I cancel the other tasks when one task fails in C#?

Use `TaskGroup.AllAsync` for a fixed set of tasks, or `TaskGroup.RunAsync` for a dynamic one. The first exception cancels the shared `CancellationToken`, and the call waits for the cancelled tasks to stop before it rethrows the original exception. `Task.WhenAll` can't do this, because it has no way to cancel tasks.

### What is the C# equivalent of Promise.any?

`TaskGroup.AnyAsync`. It returns the first successful result and cancels the rest. If every task fails, it throws an `AggregateException` with all the failures, the same as JavaScript's `AggregateError`. `Task.WhenAny` isn't an equivalent: it is `Promise.race`.

### Why does Task.WhenAll wait for all tasks after one has failed?

`Task.WhenAll` only observes tasks; it can't stop them. The tasks keep running until they finish, so the caller waits for work whose results are discarded. Pass the group's token to every task with `TaskGroup` and they stop at the first failure.

### Can't I do this with a CancellationTokenSource?

Yes, and TaskGroup uses one internally. Doing it by hand means cancelling on every failure, awaiting every task in a `finally`, and filtering out the `OperationCanceledException`s you caused. The table above shows the most common hand-written version reporting "A task was canceled." instead of the real error.

### How do I limit the number of concurrent tasks in C#?

Set `TaskGroupOptions.MaxConcurrency`, or pass `maxConcurrency` to `AllAsync` or `AllSettledAsync`. The usual alternatives each cover part of it. A `SemaphoreSlim` around each task limits concurrency, but a failure doesn't stop the rest. `Parallel.ForEachAsync` has `MaxDegreeOfParallelism` and stops on the first failure, but it returns no results and has no way to collect every outcome.

### How do I add a timeout to an async operation in C#?

Use `TaskGroup.WithTimeoutAsync(timeout, ct => OperationAsync(ct))`. It throws `TimeoutException` when the time is up, after the operation has observed the cancellation and stopped. `Task.WaitAsync(timeout)` throws too, but leaves the operation running. `CancellationTokenSource.CancelAfter` does cancel it, but you get an `OperationCanceledException` that you can't tell apart from the caller's own cancellation.

### How do I process items in parallel and get the results as they complete?

`items.SelectConcurrentAsync((item, ct) => ProcessAsync(item, ct), maxConcurrency: n, preserveOrder: false)` returns an `IAsyncEnumerable` of the results in completion order; leave out `preserveOrder` to keep input order. Leaving the `await foreach` early cancels whatever is still running.

### How do I prevent a cache stampede in C#?

Wrap the load in `SingleFlight<TKey, TValue>.RunAsync(key, ...)`. Concurrent misses for the same key then share one load instead of each hitting the backend. `Lazy<Task<T>>` in a `ConcurrentDictionary` does the sharing, but then you have to remove failed entries yourself, and one caller's cancellation either cancels everyone or nobody.

### What is the C# equivalent of Go's select statement?

`ChannelSelect` in the `StructuredConcurrency.Channels` namespace. Add a `Case` per `ChannelReader`, plus optional `Timeout` and `Default` cases, and call `RunAsync`. It runs exactly one case, picking at random among the ready channels like Go does. Hand-written versions built on `WaitToReadAsync` and `Task.WhenAny` tend to always favour the first channel and to leave waits pending after they return.

### Does it run tasks on the thread pool?

No. `group.Run` invokes the delegate on the calling thread, like calling an async method. Wrap CPU-bound work in `Task.Run`. With `MaxConcurrency`, queued work starts on the thread that freed the slot, but with the `ExecutionContext` `Run` was called in, so `AsyncLocal` values and tracing flow as if it had started right away. It doesn't go back to a UI `SynchronizationContext`.

### How do I stop the remaining tasks once I have the result I need?

Call `group.Cancel()`. It cancels the remaining children without counting as a failure.

## Performance

The benchmark compares TaskGroup with the code it replaces: a hand-written `CancellationTokenSource` that the first failure cancels, plus `Task.WhenAll`. It was run with BenchmarkDotNet on .NET 10 on an Apple M2 Pro; the source is in [`benchmarks`](benchmarks/StructuredConcurrency.Benchmarks/Program.cs).

| Children | Children complete | Hand-written | TaskGroup | Hand-written alloc. | TaskGroup alloc. |
| ---: | --- | ---: | ---: | ---: | ---: |
| 2 | asynchronously | 2.07 μs | 2.06 μs | 850 B | 638 B |
| 10 | asynchronously | 6.89 μs | 7.18 μs | 2,710 B | 2,277 B |
| 100 | asynchronously | 41.0 μs | 43.9 μs | 25,334 B | 20,918 B |
| 2 | synchronously | 49 ns | 66 ns | 160 B | 224 B |
| 10 | synchronously | 98 ns | 152 ns | 224 B | 224 B |
| 100 | synchronously | 876 ns | 1,231 ns | 944 B | 224 B |

When children do real asynchronous work, TaskGroup runs within the benchmark's noise (about ±10%) of the hand-written code and allocates 16–25% less. When children complete synchronously, it costs about 3.5 ns more per child, but its allocation stays at 224 B regardless of the number of children. `AnyAsync` over three operations takes 3.6 μs, against 4.1 μs for a hand-written `Task.WhenEach` loop, but allocates 1,840 B against 931 B.

## Status

Version 0.4 includes:

- `TaskGroup.RunAsync`, `Run`, `StartAsync`, `Cancel` and `TaskGroupOptions` (with `CancelOnFailure` and `MaxConcurrency`)
- `AllAsync`, `AnyAsync`, `AllSettledAsync` and `TaskOutcome<T>`
- `WithTimeoutAsync`, `WithTimeoutOrDefaultAsync` and `TryWithTimeoutAsync`
- `SelectConcurrentAsync` and `SingleFlight<TKey, TValue>`
- in `StructuredConcurrency.Channels`: `ChannelSelect`, `MergeWith`, `BroadcastAsync` and `group.Produce`

The API may change before 1.0.

Targets .NET 8 and later, and is trimming and NativeAOT compatible.

## License

[MIT](LICENSE)
