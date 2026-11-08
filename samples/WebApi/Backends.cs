namespace WebApi;

// Simulated backends. Each one honours its CancellationToken and logs when it is cancelled, so you can see in the
// console that work stops when it is no longer needed.

public sealed record Product(string Id, string Name);

public sealed record ProductPage(Product Product, decimal Price, int Stock);

public sealed class BackendException(string message) : Exception(message);

public sealed class Catalog(ILogger<Catalog> logger)
{
    private int _productLoads;

    public int ProductLoads => Volatile.Read(ref _productLoads);

    public async Task<Product> GetProductAsync(string id, CancellationToken cancellationToken)
    {
        Interlocked.Increment(ref _productLoads);
        await Simulate("product", TimeSpan.FromMilliseconds(Random.Shared.Next(100, 200)), cancellationToken);
        return new Product(id, $"Product {id}");
    }

    public async Task<decimal> GetPriceAsync(string id, CancellationToken cancellationToken)
    {
        await Simulate("price", TimeSpan.FromMilliseconds(80), cancellationToken);
        return id == "broken" ? throw new BackendException("The pricing service is down.") : 9.99m;
    }

    public async Task<int> GetStockAsync(string id, CancellationToken cancellationToken)
    {
        // "slow" takes longer than the product page's deadline.
        await Simulate("stock", TimeSpan.FromMilliseconds(id == "slow" ? 5_000 : 60), cancellationToken);
        return 12;
    }

    private async Task Simulate(string call, TimeSpan latency, CancellationToken cancellationToken)
    {
        try
        {
            await Task.Delay(latency, cancellationToken);
        }
        catch (OperationCanceledException)
        {
            logger.LogInformation("{Call} call cancelled after it was no longer needed", call);
            throw;
        }
    }
}

public sealed class Search(ILogger<Search> logger)
{
    public async Task<string[]> QueryAsync(string query, int attempt, CancellationToken cancellationToken)
    {
        // One call in five hits a slow replica.
        TimeSpan latency = TimeSpan.FromMilliseconds(Random.Shared.Next(5) == 0 ? 2_000 : 40);
        logger.LogInformation("Search attempt {Attempt} for '{Query}' started ({Latency} ms)", attempt + 1, query, latency.TotalMilliseconds);
        try
        {
            await Task.Delay(latency, cancellationToken);
        }
        catch (OperationCanceledException)
        {
            logger.LogInformation("Search attempt {Attempt} cancelled: another attempt answered first", attempt + 1);
            throw;
        }
        return [$"{query} #1", $"{query} #2", $"{query} #3"];
    }
}

public sealed class Mirrors(ILogger<Mirrors> logger)
{
    public string[] All { get; } = ["eu", "us", "asia"];

    public async Task<string> DownloadAsync(string mirror, string name, CancellationToken cancellationToken)
    {
        try
        {
            switch (mirror)
            {
                case "eu":
                    await Task.Delay(20, cancellationToken);
                    throw new BackendException("The EU mirror is down.");
                case "us":
                    await Task.Delay(150, cancellationToken);
                    return $"{name} from {mirror}";
                default:
                    await Task.Delay(3_000, cancellationToken);
                    return $"{name} from {mirror}";
            }
        }
        catch (OperationCanceledException)
        {
            logger.LogInformation("Download from {Mirror} cancelled: another mirror succeeded", mirror);
            throw;
        }
    }
}
