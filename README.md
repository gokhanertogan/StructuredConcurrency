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

## Status

Version 0.1 contains `RunAsync`, `Run`, `Cancel` and `TaskGroupOptions`. Next on the roadmap are three one-shot policies (`WhenAllAsync`, `FirstSuccessfulAsync` and `SettleAllAsync`) and benchmarks. The API may change before 1.0.

Targets .NET 8 and later, and is trimming and NativeAOT compatible.

## License

[MIT](LICENSE)
