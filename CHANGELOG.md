# Changelog

All notable changes to the StructuredConcurrency and StructuredConcurrency.Hosting packages. They are versioned and
released together. Before 1.0 a minor version may break the API; every break is listed under **Breaking changes**.

## 0.10.0

### Breaking changes

- `TaskGroup.AnyAsync` is now `TaskGroup.FirstSuccessAsync`, with the same overloads and behavior. The old name looked
  too much like `Task.WhenAny`, which returns the first task to *complete*, even a failed one. There is no alias, so
  rename the calls.
- `RestartEvent.Restart` is now `RestartEvent.RestartNumber`. There is no alias, so rename the uses.

### New

- **Group timeouts:** `TaskGroupOptions.Timeout` cancels the whole group when it runs too long and throws a
  `TimeoutException`; `TaskGroupOptions.TimeProvider` makes it testable with `FakeTimeProvider`.
- **Quorums:** `TaskGroup.QuorumAsync` returns the first N successes and cancels the rest, for replica reads and
  majority votes.
- **Groups without a lambda:** `TaskGroup.Create` with `JoinAsync` and `await using`, like Java's
  `StructuredTaskScope`. Leaving the block without joining cancels the children and waits for them. It works where a
  lambda doesn't, such as an async iterator.
- **Pipelines:** `SelectConcurrentAsync` takes an `onFailure` callback that skips and reports a failed item instead of
  ending the stream, and `BufferAsync` lets a stage read ahead of the one after it.
- **Why a child was cancelled:** `group.CancellationReason` (`Failure`, `Timeout`, `Requested`, `CallerToken`,
  `Disposed`), like Go's `context.Cause`. It is set before the token is cancelled, so a child can read it in its
  `catch`.
- **Load shedding:** `group.TryRun(work, out task)` starts a child only if a concurrency slot and a rate-limiter
  permit are free right now, like Go's `errgroup.TryGo`; otherwise it returns `false` and queues nothing.
- **Docs:** per-item timeouts in a pipeline (Elixir's `Task.async_stream` `timeout:`) and cleanup that must run after
  cancellation (Kotlin's `NonCancellable`, Trio's shield), both built from `WithTimeoutAsync`.
- **Analyzers:** `SC0002` now also checks the body of `TaskGroup.RunAsync` and flags an outer token used where
  `group.CancellationToken` belongs, with a code fix. A new suppressor (`SCSP0001`) silences `CS4014` on
  `group.Run(...)`, whose task the group already awaits.

### Fixes

- `SingleFlight`: when every caller of a flight gave up, a new caller for the same key could start a second operation
  while the cancelled one was still running. The new caller now waits for the old operation to stop first.
- `default(TaskOutcome<T>).ToString()` no longer throws.
- Tracing: `taskgroup.cancelled_by` is now documented with all its values, including `timeout` and `disposed`.

### Other

- Samples: an ASP.NET Core minimal API (`samples/WebApi`) and a queue worker (`samples/Worker`).
- The public API is tracked with `PublicApiAnalyzers`; stress tests and Stryker.NET mutation testing were added.
- `CONTRIBUTING.md`, `SECURITY.md`, and issue and pull request templates.

## 0.9.0 and earlier

See the [GitHub releases](https://github.com/gokhanertogan/StructuredConcurrency/releases) and the commit history.
