# StructuredConcurrency

Structured concurrency for .NET. Child tasks run inside a scope that completes only after all of them have finished, and the first failure cancels the rest.

```
dotnet add package StructuredConcurrency
```

## Why

Running calls in parallel with `Task.WhenAll` or a hand-written `CancellationTokenSource` goes wrong in quiet ways. Take a request that calls three services, where one fails after 300 ms and the other two take 1.5 s and 2 s:

| Approach | Caller gets the error at | Error the caller sees | Work left running |
| --- | --- | --- | --- |
| `Task.WhenAll` | 2,012 ms | OrderService is down | None, but 1.7 s of calls whose results are discarded |
| `await` one by one | 301 ms | OrderService is down | 2 calls keep running with nobody waiting |
| Hand-written `CancellationTokenSource` | 310 ms | A task was canceled. | None |
| `TaskGroup` | 320 ms | OrderService is down | None |

The numbers come from [`samples/BeforeAfter`](samples/BeforeAfter/Program.cs), which runs the same request all four ways.

## Usage

```csharp
using StructuredConcurrency;

var (user, orders) = await TaskGroup.RunAsync(async group =>
{
    Task<User> user = group.Run(ct => api.GetUserAsync(id, ct));
    Task<Order[]> orders = group.Run(ct => api.GetOrdersAsync(id, ct));
    return (await user, await orders);
}, cancellationToken);
```

Each child receives the group's `CancellationToken`. If either call fails, the other is cancelled. `RunAsync` completes only after both have stopped, and `await` throws the original failure.

A dynamic number of children works the same way:

```csharp
await TaskGroup.RunAsync(async group =>
{
    foreach (var url in urls)
    {
        group.Run(ct => DownloadAsync(url, ct));
    }
}, cancellationToken);
```

## Policies

Three one-shot helpers cover the common shapes. They are named after JavaScript's `Promise` combinators and keep the same guarantee: none of the operations outlives the call.

```csharp
// All must succeed. The first failure cancels the rest (Promise.all).
var (user, orders) = await TaskGroup.AllAsync(
    ct => api.GetUserAsync(id, ct),
    ct => api.GetOrdersAsync(id, ct),
    cancellationToken);

// The first success wins and the rest are cancelled (Promise.any).
string data = await TaskGroup.AnyAsync(mirrors, (mirror, ct) => mirror.DownloadAsync(path, ct), cancellationToken);

// Every operation runs to completion, and each outcome is reported (Promise.allSettled).
TaskOutcome<Receipt>[] outcomes = await TaskGroup.AllSettledAsync(orders, (order, ct) => ChargeAsync(order, ct), cancellationToken);
```

Each helper takes either two or three operations, which may return different types (and return a tuple), or a collection of items with one operation (and return an array in input order). If every operation passed to `AnyAsync` fails, it throws an `AggregateException` with all failures in input order.

### `Task.WhenAny` is not `Promise.any`

`Task.WhenAny` returns the first task to *complete*, even if that task failed, and it doesn't stop the others. Take three mirrors where A fails after 100 ms, B succeeds after 300 ms and C succeeds after 2 s:

| Approach | Caller gets | At | Work left running |
| --- | --- | --- | --- |
| `Task.WhenAny` | A's error, even though B would succeed | 104 ms | B and C, with nobody waiting |
| `Task.WhenEach` + skip failures | B's data | 301 ms | C, with nobody waiting |
| `TaskGroup.AnyAsync` | B's data | 334 ms | None: C is cancelled |

See [`samples/AnyVsWhenAny`](samples/AnyVsWhenAny/Program.cs).

## Guarantees

1. **Scope.** `RunAsync` completes only after the body and every child have completed, even when the body throws.
2. **Failure.** The first real failure cancels the group's token, which every child receives.
3. **Cancellation flows down.** The group's token is linked to the caller's token, and a nested group is linked to its parent's.
4. **Errors surface as `await` expects.** `await` rethrows the first failure. The returned task's `Exception` holds every failure in the order they occurred.
5. **Cancellations caused by the group are not failures.** An `OperationCanceledException` thrown after the group was cancelled is dropped, so you see why the group stopped. A child's own cancellation, such as an `HttpClient` timeout, is reported as a failure.

## Options and control

- `group.Cancel()` stops the remaining children without counting as a failure, for example once you have the answer you need.
- `new TaskGroupOptions { CancelOnFailure = false }` puts the group in supervisor mode: every child runs to completion and all failures are reported at the end.
- `group.Run` invokes the delegate on the calling thread, like calling an async method. It does not offload to the thread pool, so wrap CPU-bound work in `Task.Run`.

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

Version 0.2 adds `AllAsync`, `AnyAsync`, `AllSettledAsync` and `TaskOutcome<T>` to the 0.1 core (`RunAsync`, `Run`, `Cancel` and `TaskGroupOptions`). The API may change before 1.0.

Targets .NET 8 and later, and is trimming and NativeAOT compatible.

## License

[MIT](LICENSE)
