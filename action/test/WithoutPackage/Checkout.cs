using System.Threading;
using System.Threading.Tasks;

public static class Checkout
{
    static Task ReserveStockAsync(int orderId, CancellationToken cancellationToken) => Task.Delay(10, cancellationToken);
    static Task ChargeCardAsync(int orderId, CancellationToken cancellationToken) => Task.Delay(10, cancellationToken);
    static Task<string> LoadAddressAsync(int orderId, CancellationToken cancellationToken) => Task.FromResult("");

    // Reported: when the card is declined, the stock reservation keeps running and WhenAll waits for it.
    public static async Task PlaceOrderAsync(int orderId, CancellationToken cancellationToken)
    {
        await Task.WhenAll(ReserveStockAsync(orderId, cancellationToken), ChargeCardAsync(orderId, cancellationToken));
    }

    // Not reported: nothing here can be cancelled.
    public static Task WarmUpAsync() => Task.WhenAll(Task.Delay(1), Task.Delay(2));

    // Not reported: a single task.
    public static Task<string[]> AddressAsync(int orderId, CancellationToken cancellationToken) =>
        Task.WhenAll(LoadAddressAsync(orderId, cancellationToken));
}
