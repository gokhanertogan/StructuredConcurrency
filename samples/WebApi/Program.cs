// A minimal API whose endpoints call slow, flaky backends the way real services do. The backends are simulated in
// Backends.cs, so the sample runs on its own: `dotnet run`, then try the requests in README.md.
//
// Every endpoint takes the request's CancellationToken (ASP.NET Core binds it to HttpContext.RequestAborted), so when
// the client disconnects, all the work the request started is cancelled too, and the logs show it stopping.

using StructuredConcurrency;
using WebApi;

var builder = WebApplication.CreateBuilder(args);
builder.Services.AddSingleton<Catalog>();
builder.Services.AddSingleton<Search>();
builder.Services.AddSingleton<Mirrors>();
builder.Services.AddSingleton(new SingleFlight<string, Product>());
var app = builder.Build();

// Fan-out with a deadline: three backends in parallel, all within one second.
// The first failure cancels the other calls, and the response carries that failure, not a TaskCanceledException.
// Try /products/42 (fast), /products/slow (stock is too slow: 504) and /products/broken (pricing fails: 502).
app.MapGet("/products/{id}", async (string id, Catalog catalog, CancellationToken cancellationToken) =>
{
    try
    {
        var (product, price, stock) = await TaskGroup.WithTimeoutAsync(
            TimeSpan.FromSeconds(1),
            ct => TaskGroup.AllAsync(
                c => catalog.GetProductAsync(id, c),
                c => catalog.GetPriceAsync(id, c),
                c => catalog.GetStockAsync(id, c),
                ct),
            cancellationToken);
        return Results.Ok(new ProductPage(product, price, stock));
    }
    catch (TimeoutException)
    {
        // By now the slow call has been cancelled and has stopped; nothing is left running for this request.
        return Results.Problem("The product page took longer than 1 s.", statusCode: StatusCodes.Status504GatewayTimeout);
    }
    catch (BackendException exception)
    {
        return Results.Problem(exception.Message, statusCode: StatusCodes.Status502BadGateway);
    }
});

// Hedged requests: if the search backend hasn't answered within 100 ms, ask again in parallel, up to three times,
// and take the first answer. The slower attempts are cancelled. Most calls are fast, but one in five is very slow,
// so the hedge cuts the tail latency. Call /search?q=shoes a few times and watch the attempts in the log.
app.MapGet("/search", (string q, Search search, CancellationToken cancellationToken) =>
    TaskGroup.HedgeAsync(
        TimeSpan.FromMilliseconds(100),
        (attempt, ct) => search.QueryAsync(q, attempt, ct),
        maxAttempts: 3,
        cancellationToken));

// Deduplicated loads: concurrent requests for the same product share one backend call instead of each making their
// own (a cache stampede). Fire 20 requests at once and /stats shows far fewer loads than requests.
app.MapGet("/products/{id}/details", (string id, Catalog catalog, SingleFlight<string, Product> loads, CancellationToken cancellationToken) =>
    loads.RunAsync(id, ct => catalog.GetProductAsync(id, ct), cancellationToken));

// First success wins: download from whichever mirror succeeds first. One mirror always fails fast; with
// Task.WhenAny that failure would win. The other mirrors are cancelled as soon as one succeeds.
app.MapGet("/files/{name}", (string name, Mirrors mirrors, CancellationToken cancellationToken) =>
    TaskGroup.FirstSuccessAsync(mirrors.All, (mirror, ct) => mirrors.DownloadAsync(mirror, name, ct), cancellationToken));

app.MapGet("/stats", (Catalog catalog) => new { productLoads = catalog.ProductLoads });

app.Run();
