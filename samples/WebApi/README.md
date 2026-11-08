# WebApi sample

A minimal API whose endpoints call slow, flaky backends. The backends are simulated in [`Backends.cs`](Backends.cs) and log when their work is cancelled, so you can watch the console to see work stop as soon as nobody needs it.

```sh
dotnet run --urls http://localhost:5199
```

| Request | What happens |
| --- | --- |
| `curl localhost:5199/products/42` | Product, price and stock load in parallel (`AllAsync`) within a 1 s deadline (`WithTimeoutAsync`) |
| `curl localhost:5199/products/slow` | The stock call is too slow: after 1 s it is cancelled and the response is a 504 |
| `curl localhost:5199/products/broken` | The price call fails: the other calls are cancelled at once and the response is a 502 with the real error |
| `curl "localhost:5199/search?q=shoes"` | One search in five hits a slow replica; after 100 ms `HedgeAsync` asks again, and the slow attempt is cancelled |
| `curl localhost:5199/files/report.pdf` | `FirstSuccessAsync` over three mirrors: the EU mirror fails fast, the US one wins and the Asia one is cancelled |
| `for i in $(seq 20); do curl -s localhost:5199/products/7/details & done; wait; curl localhost:5199/stats` | 20 concurrent requests share one backend load through `SingleFlight` |

Every endpoint takes the request's `CancellationToken`, which ASP.NET Core cancels when the client disconnects. Press Ctrl+C on a `curl localhost:5199/products/slow` and the stock call stops right away.
