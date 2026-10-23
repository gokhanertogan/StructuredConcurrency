# StructuredConcurrency: TaskGroup and Promise.all / Promise.any for C# and .NET

[![NuGet](https://img.shields.io/nuget/v/StructuredConcurrency.svg)](https://www.nuget.org/packages/StructuredConcurrency)
[![Downloads](https://img.shields.io/nuget/dt/StructuredConcurrency.svg)](https://www.nuget.org/packages/StructuredConcurrency)
[![CI](https://github.com/gokhanertogan/StructuredConcurrency/actions/workflows/ci.yml/badge.svg)](https://github.com/gokhanertogan/StructuredConcurrency/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

**Structured concurrency for C#.** Run tasks in parallel inside a `TaskGroup`. The first failure cancels the other tasks, no task keeps running after the group returns, and you get the real error back instead of a `TaskCanceledException`. It includes C# versions of JavaScript's `Promise.all`, `Promise.any` and `Promise.allSettled`, Kotlin's `withTimeout`, Go's `select` and `singleflight`, Erlang's supervisors, hedged requests, and the same model as Python's `asyncio.TaskGroup`, Kotlin's `coroutineScope`, Swift's task groups, Java's `StructuredTaskScope` and Go's `errgroup`.

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

### Limit how many tasks start per second

`MaxConcurrency` limits how many children run at once. For an API that allows only so many calls per second, give the group a rate limiter from `System.Threading.RateLimiting`: every child takes a permit before it starts and gives it back when it completes.

```csharp
using var limiter = new TokenBucketRateLimiter(new TokenBucketRateLimiterOptions
{
    TokenLimit = 10, TokensPerPeriod = 10, ReplenishmentPeriod = TimeSpan.FromSeconds(1), QueueLimit = 1_000,
});

await TaskGroup.RunAsync(group =>
{
    foreach (var id in ids)
    {
        group.Run(ct => api.GetAsync(id, ct));   // at most 10 calls a second
    }
    return Task.CompletedTask;
}, new TaskGroupOptions { RateLimiter = limiter, MaxConcurrency = 4 }, cancellationToken);
```

A child waits for its permit while holding its concurrency slot, and a child the limiter refuses (its queue is full) fails with `RateLimitRejectedException`, which has the limiter's `RetryAfter` when it gives one. A `ConcurrencyLimiter` shared by several groups limits them together. The group doesn't dispose the limiter.

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

The items can also come from an `IAsyncEnumerable`: rows from a database, pages from an API, or a channel. The source is read only as slots free up, so a slow source doesn't hold back results that are already done, and a fast one doesn't pile up work. Ending the loop cancels the source as well, and disposes it after the running operations have stopped.

```csharp
await foreach (Receipt receipt in orders.Reader.ReadAllAsync(cancellationToken)
                                        .SelectConcurrentAsync((order, ct) => ChargeAsync(order, ct), maxConcurrency: 4))
{
    await SaveAsync(receipt);
}
```

> **Upgrading from 0.6:** a type that is both `IEnumerable<T>` and `IAsyncEnumerable<T>`, such as an EF Core `DbSet<T>` or query, now matches both overloads, and the call no longer compiles (CS0121). Add `.AsAsyncEnumerable()` to read it asynchronously, which is what you want for a database, or `.AsEnumerable()` to keep reading it synchronously as before.

### Batch a stream by size and time

`ChunkAsync` groups an `IAsyncEnumerable` into arrays of up to `maxSize` items, and cuts a smaller one when its first item has waited `maxDelay`, like Rx's `Buffer(timeSpan, count)` or Kafka's `batch.size` and `linger.ms`. Use it for batch inserts and bulk API calls, where waiting for a full batch would hold up the items that are already there.

```csharp
await foreach (Event[] batch in events.Reader.ReadAllAsync(cancellationToken).ChunkAsync(500, TimeSpan.FromSeconds(1)))
{
    await database.BulkInsertAsync(batch, cancellationToken);   // 500 events, or fewer after a second
}
```

Chunks are never empty, and the last one holds whatever is left when the source ends. The next read stays in flight while you handle a chunk, and ending the loop cancels it and disposes the source. `ChunkAsync` takes a `TimeProvider` too.

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

To put a deadline on a whole group, wrap it: `TaskGroup.WithTimeoutAsync(timeout, ct => TaskGroup.RunAsync(body, ct))`. Only a cancellation that comes from the timer becomes a `TimeoutException`. Your own token's cancellation, or a timeout inside the operation, comes through unchanged. See [`samples/TimeoutVsWaitAsync`](samples/TimeoutVsWaitAsync/Program.cs) for the three approaches side by side.

Every timeout method also takes a `TimeProvider`, and so does `new ChannelSelect(timeProvider)`. Pass a `FakeTimeProvider` in tests and time passes only when you advance it:

```csharp
var time = new FakeTimeProvider();   // Microsoft.Extensions.TimeProvider.Testing
Task<User> load = TaskGroup.WithTimeoutAsync(TimeSpan.FromSeconds(30), time, ct => LoadUserAsync(id, ct));

time.Advance(TimeSpan.FromSeconds(30));
await Assert.ThrowsAsync<TimeoutException>(() => load);   // no 30-second test
```

## Hedged requests

A few slow responses dominate tail latency. `TaskGroup.HedgeAsync` starts the operation, and if it hasn't succeeded after `delay`, starts another attempt alongside it, up to `maxAttempts`. The first attempt to succeed wins and the others are cancelled, so the call takes about as long as the fastest attempt. This is the hedged request from Google's ["The Tail at Scale"](https://research.google/pubs/the-tail-at-scale/).

```csharp
// A second attempt if the first takes longer than the 95th percentile, say 50 ms
Product product = await TaskGroup.HedgeAsync(TimeSpan.FromMilliseconds(50), ct => catalog.GetAsync(id, ct), maxAttempts: 2, cancellationToken);

// Each attempt gets its number, so it can go to another replica
string page = await TaskGroup.HedgeAsync(TimeSpan.FromMilliseconds(100), (attempt, ct) => replicas[attempt].GetAsync(path, ct), maxAttempts: replicas.Length);
```

When every attempt that is running has failed, the next one starts at once instead of after the delay; with `Timeout.InfiniteTimeSpan` as the delay, attempts start only on failure. If every attempt fails, you get an `AggregateException` with each failure, as with `AnyAsync`. Only hedge operations that are safe to run twice, such as reads.

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

## Run work for the same key one at a time

`KeyedSequencer<TKey>` runs operations one at a time per key, in the order they were asked for, while operations for different keys run concurrently. Updates to one account, or the messages of one conversation, then never overlap, without a global lock that makes every other account wait.

```csharp
private readonly KeyedSequencer<Guid> _accounts = new();

public Task<Balance> ApplyAsync(Transaction transaction, CancellationToken cancellationToken) =>
    _accounts.RunAsync(transaction.AccountId, ct => _ledger.ApplyAsync(transaction, ct), cancellationToken);
```

A caller that gives up while waiting for its turn just leaves the line: its operation never runs, and the next one still waits for the operation running before it. Once an operation has started, the caller's token cancels it, and the call returns only after it has ended. A key takes no memory once nothing is running or waiting for it.

## Background services that shut down cleanly

The `StructuredConcurrency.Hosting` package adds `StructuredBackgroundService`, a `BackgroundService` whose work runs in a `TaskGroup`.

```
dotnet add package StructuredConcurrency.Hosting
```

```csharp
using StructuredConcurrency.Hosting;

public sealed class OrderWorker(IQueue queue, IMetrics metrics) : StructuredBackgroundService
{
    protected override Task ExecuteAsync(TaskGroup group)
    {
        group.Run(ct => ConsumeOrdersAsync(queue, ct));
        group.Run(ct => PublishMetricsAsync(metrics, ct));
        return Task.CompletedTask;
    }
}
```

When the host stops, every child is cancelled and `StopAsync` returns only after all of them have ended. If a child fails, the others are cancelled and the service stops with that exception, which the host logs and (by default) uses to stop the application. A plain `BackgroundService` gives neither: work started with `Task.Run` keeps running after shutdown, and its exceptions disappear. Override `Options` to set `MaxConcurrency` or `CancelOnFailure = false`. The service's group doesn't record a tracing activity: it lives as long as the host, so its span would never end. What the children trace is recorded as usual.

## Restart work that fails: supervisors

`Supervisor.RunAsync` restarts work when it fails, like an Erlang/OTP supervisor: it waits a little longer before each restart, and if the work fails more than `MaxRestarts` times within `Window`, it gives up and lets the failure through. Failures further apart than the window are forgotten, so work that fails now and then keeps going, while work that keeps failing is stopped.

```csharp
var policy = new RestartPolicy
{
    MaxRestarts = 5,
    Window = TimeSpan.FromMinutes(1),
    InitialDelay = TimeSpan.FromSeconds(1),   // then 2 s, 4 s... up to MaxDelay
    ShouldRestart = exception => exception is not AuthenticationException,
    OnRestart = restart => logger.LogWarning(restart.Failure, "Restart {Restart} in {Delay}", restart.Restart, restart.Delay),
};

await TaskGroup.RunAsync(group =>
{
    group.Run(ct => Supervisor.RunAsync(ConsumeOrdersAsync, policy, ct));   // restarts only the consumer: one_for_one
    group.Run(ct => PublishMetricsAsync(ct));
    return Task.CompletedTask;
}, cancellationToken);

// Restart the whole group when any child fails: one_for_all
await Supervisor.RunGroupAsync(group =>
{
    group.Run(ct => ReadAsync(connection, ct));
    group.Run(ct => WriteAsync(connection, ct));
    return Task.CompletedTask;
}, policy, cancellationToken: cancellationToken);
```

Cancelling the token stops the work and the wait before a restart. Once it is cancelled, nothing is restarted, whatever the work throws on its way out. The next run starts only after the previous one has ended, so nothing runs twice at once. In a `StructuredBackgroundService`, override `RestartPolicy` and the service's work starts again in a new group when it fails, instead of stopping the service:

```csharp
public sealed class OrderWorker(IQueue queue) : StructuredBackgroundService
{
    protected override RestartPolicy RestartPolicy => new() { MaxRestarts = 10, Window = TimeSpan.FromMinutes(5) };

    protected override Task ExecuteAsync(TaskGroup group) { ... }   // runs again after each restart
}
```

## Tracing with OpenTelemetry

Every group records an `Activity`, and the activities of its children nest under it, including children that waited in a `MaxConcurrency` queue. Subscribe to the source to see them:

```csharp
builder.Services.AddOpenTelemetry().WithTracing(tracing => tracing.AddSource(TaskGroup.ActivitySourceName));

await TaskGroup.RunAsync(body, new TaskGroupOptions { Name = "load-dashboard" }, cancellationToken);
```

Each group's activity has these tags:

- `taskgroup.children`: how many children were added with `Run`, including queued ones that never started.
- `taskgroup.outcome`: `success`, `failed` or `cancelled`.
- `taskgroup.cancelled_by`: `failure`, `caller` or `Cancel`, when it was cancelled.

A failed group has error status and one `exception` event per failure, so a trace shows which child failed and which ones were cancelled because of it. `AllAsync`, `AnyAsync` and `AllSettledAsync` name their groups after themselves. Set `EnableTracing = false` on a group you don't want traced. With no listener, `ActivitySource` creates nothing.

## Metrics

Groups also record metrics with the `StructuredConcurrency` meter, for OpenTelemetry or `dotnet-counters`:

```csharp
builder.Services.AddOpenTelemetry().WithMetrics(metrics => metrics.AddMeter(TaskGroup.MeterName));
```

| Instrument | Type | What it measures |
| --- | --- | --- |
| `taskgroup.duration` | Histogram (s) | How long groups ran, tagged with `taskgroup.outcome` |
| `taskgroup.active_children` | UpDownCounter | Children that have started and not yet completed, including ones waiting for a `RateLimiter` permit |
| `taskgroup.queued_children` | UpDownCounter | Children waiting for a `MaxConcurrency` slot |
| `taskgroup.failures` | Counter | Failures, not counting the cancellations they cause |

Every measurement is tagged with `taskgroup.name`, the group's `Name` (`TaskGroup` by default), so give the groups you want to watch a name. With no listener, groups record nothing. A group checks for a listener when it starts, so one that was already running when a listener subscribed isn't measured.

## Analyzers

Structured concurrency only works if the work looks at the token it is given. The package includes compiler warnings that catch the code that doesn't, in your IDE and in `dotnet build`. No extra package is needed.

| Rule | Catches | Why it matters |
| --- | --- | --- |
| `SC0001` | `group.Run(ct => DownloadAsync(url))`: the token is never used | When a sibling fails or the time is up, this download keeps going and the group waits for it |
| `SC0002` | `group.Run(_ => DownloadAsync(url, cancellationToken))`: an outer token instead of the work's own | Only the caller can cancel it, so a failing sibling, a timeout or `group.Cancel()` doesn't stop it |
| `SC0003` | `Task.Run(() => DownloadAsync(url));` in a group's body or child, with the task thrown away | The work isn't a child: the group doesn't cancel it, doesn't wait for it, and never sees it fail |

`SC0001` and `SC0002` check every lambda that StructuredConcurrency passes a token to: `group.Run`, `StartAsync`, `Produce`, `AllAsync`, `AnyAsync`, `AllSettledAsync`, `HedgeAsync`, the timeouts, `SelectConcurrentAsync`, `SingleFlight`, `KeyedSequencer` and `Supervisor`. `SC0002` has a code fix that switches the work to its own token.

`SC0003` checks `Task.Run`, `Task.Factory.StartNew` and `ThreadPool.QueueUserWorkItem` calls whose task is thrown away, in a group's body, in its children and in methods that take a `TaskGroup`. Awaiting or returning the task is fine. Its code fix turns `Task.Run(() => WorkAsync())` into `group.Run(ct => WorkAsync())`; then pass `ct` on. Unlike `Task.Run`, `group.Run` starts the work on the calling thread, which is what you want for work that awaits I/O. For work that computes for a while before its first `await`, keep the thread-pool hop instead: `group.Run(ct => Task.Run(() => WorkAsync(ct), ct))`.

> **Upgrading from 0.7:** `SC0003` is new, so a build with `TreatWarningsAsErrors` can stop at code it now reports. Fix it, or set its severity in `.editorconfig` as shown below. The package now depends on `System.Threading.RateLimiting`, which ASP.NET Core apps already have.

Name the parameter `_` to say that ignoring the token is intended; `SC0001` then stays quiet. Work that can't be cancelled anyway, such as `ct => Task.FromResult(value)`, isn't reported. To change a rule's severity, use `.editorconfig`:

```ini
[*.cs]
dotnet_diagnostic.SC0001.severity = error
```

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

`Send(...)` adds Go's send case, `case out <- v`: it writes when the channel has room. Its `when` condition does what a `nil` channel does in Go, turning the case off, for instance while there is nothing to send. Here is Go's classic buffer between two channels:

```csharp
var pending = new Queue<Order>();
var select = new ChannelSelect()
    .Case(incoming.Reader, order => pending.Enqueue(order))
    .Send(outgoing.Writer, () => pending.Peek(), () => pending.Dequeue(), when: () => pending.Count > 0);

while (await select.RunAsync(cancellationToken)) { }   // false once incoming has completed and the buffer is empty
```

The item function runs each time the case is tried, so keep side effects in the handler. A writer that has been completed counts as completed, like a reader's channel.

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
| Go | `select { case ch <- v: }` | `ChannelSelect.Send` |
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
| Erlang/OTP | `supervisor` with `one_for_one` | `Supervisor.RunAsync(work, policy)` |
| Erlang/OTP | `supervisor` with `one_for_all` | `Supervisor.RunGroupAsync(body, policy)` |
| Rx | `Buffer(timeSpan, count)` | `source.ChunkAsync(count, timeSpan)` |
| gRPC, Envoy | Hedging policy | `TaskGroup.HedgeAsync` |

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

`items.SelectConcurrentAsync((item, ct) => ProcessAsync(item, ct), maxConcurrency: n, preserveOrder: false)` returns an `IAsyncEnumerable` of the results in completion order; leave out `preserveOrder` to keep input order. `items` can be a collection or an `IAsyncEnumerable`, such as a channel's `ReadAllAsync()`. Leaving the `await foreach` early cancels whatever is still running.

### How do I prevent a cache stampede in C#?

Wrap the load in `SingleFlight<TKey, TValue>.RunAsync(key, ...)`. Concurrent misses for the same key then share one load instead of each hitting the backend. `Lazy<Task<T>>` in a `ConcurrentDictionary` does the sharing, but then you have to remove failed entries yourself, and one caller's cancellation either cancels everyone or nobody.

### What is the C# equivalent of Go's select statement?

`ChannelSelect` in the `StructuredConcurrency.Channels` namespace. Add a `Case` per `ChannelReader`, plus optional `Timeout` and `Default` cases, and call `RunAsync`. It runs exactly one case, picking at random among the ready channels like Go does. Hand-written versions built on `WaitToReadAsync` and `Task.WhenAny` tend to always favour the first channel and to leave waits pending after they return.

### How do I stop all tasks in a BackgroundService on shutdown?

Derive from `StructuredBackgroundService` in `StructuredConcurrency.Hosting` and start the work with `group.Run`. On shutdown the group's token is cancelled, and the service waits for every child to end before `StopAsync` returns. A child that fails stops the service with its exception instead of failing silently.

### How do I test code that uses a timeout without waiting for it?

Pass a `FakeTimeProvider` from `Microsoft.Extensions.TimeProvider.Testing` to the `TimeProvider` overload of `WithTimeoutAsync`, `WithTimeoutOrDefaultAsync` or `TryWithTimeoutAsync`, or to `new ChannelSelect(timeProvider)`. Then call `Advance` to make the time pass.

### How do I find tasks that ignore cancellation?

Build with StructuredConcurrency 0.6 or later. Its analyzers report work that never uses the token it is given (`SC0001`) and work that uses an outer token instead of its own (`SC0002`). See [Analyzers](#analyzers).

### Does it run tasks on the thread pool?

No. `group.Run` invokes the delegate on the calling thread, like calling an async method. Wrap CPU-bound work in `Task.Run`. With `MaxConcurrency`, queued work starts on the thread that freed the slot, but with the `ExecutionContext` `Run` was called in, so `AsyncLocal` values and tracing flow as if it had started right away. It doesn't go back to a UI `SynchronizationContext`.

### How do I stop the remaining tasks once I have the result I need?

Call `group.Cancel()`. It cancels the remaining children without counting as a failure.

### How do I restart a background task that fails in C#?

Wrap it in `Supervisor.RunAsync(work, new RestartPolicy { ... })`. It restarts the work with a growing delay, and gives up with the failure when there are more than `MaxRestarts` restarts within `Window`, so a task that is broken for good doesn't restart forever. In a `StructuredBackgroundService`, override `RestartPolicy` to restart the whole service's work.

### How do I send a second request when the first one is slow?

`TaskGroup.HedgeAsync(delay, ct => SendAsync(ct), maxAttempts: 2)` starts a second attempt when the first hasn't succeeded within `delay`, takes whichever succeeds first and cancels the other. Pick a delay around the 95th percentile of the latency, so only the slowest requests are sent twice.

### How do I process messages for the same key in order but different keys in parallel?

Use `KeyedSequencer<TKey>.RunAsync(key, ...)`. Operations for one key run one after another in the order they arrive, and different keys don't wait for each other. A `SemaphoreSlim` per key in a `ConcurrentDictionary` does the same, but then you have to remove the semaphores yourself and keep them from being removed while someone still waits on them.

## Performance

The benchmark compares TaskGroup with the code it replaces: a hand-written `CancellationTokenSource` that the first failure cancels, plus `Task.WhenAll`. It was run with BenchmarkDotNet on .NET 10 on an Apple M2 Pro; the source is in [`benchmarks`](benchmarks/StructuredConcurrency.Benchmarks/Program.cs).

| Children | Children complete | Hand-written | TaskGroup | Hand-written alloc. | TaskGroup alloc. |
| ---: | --- | ---: | ---: | ---: | ---: |
| 2 | asynchronously | 2.49 μs | 2.47 μs | 849 B | 678 B |
| 10 | asynchronously | 8.08 μs | 8.18 μs | 2,725 B | 2,323 B |
| 100 | asynchronously | 43.3 μs | 50.4 μs | 25,600 B | 20,960 B |
| 2 | synchronously | 41 ns | 84 ns | 160 B | 264 B |
| 10 | synchronously | 106 ns | 194 ns | 224 B | 264 B |
| 100 | synchronously | 950 ns | 1,350 ns | 944 B | 264 B |

When children do real asynchronous work, TaskGroup takes about as long as the hand-written code for a few children and up to 17% longer for 100 in this run (these numbers move by about ±10% between runs), and allocates 15–20% less. When children complete synchronously, it costs about 4 ns more per child, but its allocation stays at 264 B regardless of the number of children. The numbers include the checks for tracing and metrics; with no listener subscribed, neither records anything. `AnyAsync` over three operations takes 3.6 μs, against 6.2 μs for a hand-written `Task.WhenEach` loop, but allocates 1,879 B against 926 B.

## Status

Version 0.8 includes:

- `TaskGroup.RunAsync`, `Run`, `StartAsync`, `Cancel` and `TaskGroupOptions` (with `CancelOnFailure`, `MaxConcurrency`, `RateLimiter` and `Name`)
- `AllAsync`, `AnyAsync`, `AllSettledAsync`, `HedgeAsync` and `TaskOutcome<T>`
- `WithTimeoutAsync`, `WithTimeoutOrDefaultAsync` and `TryWithTimeoutAsync`, with or without a `TimeProvider`
- `Supervisor.RunAsync`, `Supervisor.RunGroupAsync` and `RestartPolicy`
- OpenTelemetry tracing through the `TaskGroup.ActivitySourceName` source, and metrics through the `TaskGroup.MeterName` meter
- `SelectConcurrentAsync`, over a collection or an `IAsyncEnumerable`, `ChunkAsync`, `SingleFlight<TKey, TValue>` and `KeyedSequencer<TKey>`
- in `StructuredConcurrency.Channels`: `ChannelSelect` (with send cases), `MergeWith`, `BroadcastAsync` and `group.Produce`
- in the `StructuredConcurrency.Hosting` package: `StructuredBackgroundService`, with an optional `RestartPolicy`
- the `SC0001`, `SC0002` and `SC0003` analyzers, with code fixes for `SC0002` and `SC0003`

The API may change before 1.0, but each release is checked against the previous one with package validation, so a breaking change is always deliberate.

Targets .NET 8 and later, and is trimming and NativeAOT compatible.

## License

[MIT](LICENSE)
