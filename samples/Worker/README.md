# Worker sample

A queue worker built on `StructuredBackgroundService` from the `StructuredConcurrency.Hosting` package. Orders arrive every 50 ms and are charged through a simulated payment gateway.

```sh
dotnet run
```

Watch the log, then press Ctrl+C.

- **Per-customer order:** orders for the same customer are charged one at a time, in order (`KeyedSequencer`). Different customers are charged in parallel.
- **Limits:** at most 4 orders at once (`MaxConcurrency`) and 20 a second (`RateLimiter`). `group.Run` never blocks; orders past the limits wait in the group's queue.
- **Restarts:** about one charge in 150 finds the gateway connection gone. That fails the run: the orders in progress are cancelled, and the `RestartPolicy` starts the work again after 1 s, then 2 s, 4 s and so on, up to 5 times a minute.
- **Shutdown:** on Ctrl+C every order in progress is cancelled, and the host exits only after all of them have stopped.

The processing loop reads the queue with `processing.CancellationToken`, the group's own token. With the caller's token instead, the loop would keep feeding a group that has already failed, and the failure would never reach the supervisor. Analyzer `SC0002` reports that mistake.
