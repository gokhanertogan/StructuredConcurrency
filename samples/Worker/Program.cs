// A worker that processes a stream of orders. Run it with `dotnet run`, watch it work, then press Ctrl+C.
//
// - Orders for the same customer are processed one at a time, in order (KeyedSequencer); different customers in
//   parallel, at most 4 at once (MaxConcurrency) and at most 20 per second (RateLimiter).
// - Now and then the payment gateway connection drops. That fails the whole run, which the RestartPolicy restarts
//   after a back-off, instead of the service dying or carrying on half-broken.
// - On Ctrl+C every order in progress is cancelled, and the host exits only after all of them have stopped.

using System.Threading.Channels;
using System.Threading.RateLimiting;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using StructuredConcurrency;
using StructuredConcurrency.Hosting;
using Worker;

var builder = Host.CreateApplicationBuilder(args);
builder.Logging.AddSimpleConsole(options =>
{
    options.SingleLine = true;
    options.TimestampFormat = "HH:mm:ss.fff ";
});
builder.Services.AddSingleton(Channel.CreateBounded<Order>(100));
builder.Services.AddSingleton<PaymentGateway>();
builder.Services.AddHostedService<IncomingOrders>();
builder.Services.AddHostedService<OrderProcessor>();
builder.Build().Run();

namespace Worker
{
    public sealed record Order(int Id, string Customer, decimal Amount);

    public sealed class GatewayDisconnectedException() : Exception("The payment gateway connection dropped.");

    // Stands in for a message queue: puts an order on the channel every 50 ms.
    public sealed class IncomingOrders(Channel<Order> orders) : StructuredBackgroundService
    {
        private static readonly string[] s_customers = ["alice", "bob", "carol", "dave", "erin", "frank"];

        protected override Task ExecuteAsync(TaskGroup group)
        {
            group.Run(async ct =>
            {
                for (int id = 1; ; id++)
                {
                    string customer = s_customers[Random.Shared.Next(s_customers.Length)];
                    await orders.Writer.WriteAsync(new Order(id, customer, Random.Shared.Next(5, 500)), ct);
                    await Task.Delay(50, ct);
                }
            });
            return Task.CompletedTask;
        }
    }

    public sealed class OrderProcessor(Channel<Order> orders, PaymentGateway gateway, ILogger<OrderProcessor> logger)
        : StructuredBackgroundService
    {
        private readonly KeyedSequencer<string> _perCustomer = new();
        private readonly TokenBucketRateLimiter _limiter = new(new TokenBucketRateLimiterOptions
        {
            TokenLimit = 20,
            TokensPerPeriod = 20,
            ReplenishmentPeriod = TimeSpan.FromSeconds(1),
            QueueLimit = int.MaxValue,
        });
        private int _processed;

        // When the run fails, start it again after 1 s, 2 s, 4 s ... (at most 10 s), up to 5 times a minute.
        // A failure beyond that stops the service, and the host with it.
        protected override RestartPolicy RestartPolicy => new()
        {
            MaxRestarts = 5,
            Window = TimeSpan.FromMinutes(1),
            InitialDelay = TimeSpan.FromSeconds(1),
            MaxDelay = TimeSpan.FromSeconds(10),
            ShouldRestart = exception => exception is GatewayDisconnectedException,
            OnRestart = restart => logger.LogWarning(
                "Run failed ({Reason}); restart {Number} in {Delay}", restart.Failure.Message, restart.RestartNumber, restart.Delay),
        };

        protected override Task ExecuteAsync(TaskGroup group)
        {
            group.Run(ProcessOrdersAsync);
            group.Run(ReportAsync);
            return Task.CompletedTask;
        }

        private Task ProcessOrdersAsync(CancellationToken cancellationToken) =>
            TaskGroup.RunAsync(async processing =>
            {
                // The group's own token, not cancellationToken: when an order fails, the group cancels it, and the
                // loop must stop too. With the outer token the loop would go on feeding a group that has failed.
                await foreach (Order order in orders.Reader.ReadAllAsync(processing.CancellationToken))
                {
                    // Run never blocks: past the limits, orders wait in the group's queue in arrival order.
                    processing.Run(ct => _perCustomer.RunAsync(order.Customer, c => ProcessAsync(order, c), ct));
                }
            },
            new TaskGroupOptions { Name = "orders", MaxConcurrency = 4, RateLimiter = _limiter },
            cancellationToken);

        private async Task ProcessAsync(Order order, CancellationToken cancellationToken)
        {
            try
            {
                await gateway.ChargeAsync(order, cancellationToken);
                Interlocked.Increment(ref _processed);
                logger.LogInformation("Order {Id} for {Customer} charged {Amount}", order.Id, order.Customer, order.Amount);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                // Shutting down, or another order failed the run. The order is still unpaid; a real worker would
                // leave it unacknowledged on the queue so that it is delivered again.
                logger.LogInformation("Order {Id} for {Customer} stopped before it was charged", order.Id, order.Customer);
                throw;
            }
        }

        private async Task ReportAsync(CancellationToken cancellationToken)
        {
            using var timer = new PeriodicTimer(TimeSpan.FromSeconds(2));
            while (await timer.WaitForNextTickAsync(cancellationToken))
            {
                logger.LogInformation("{Processed} orders processed so far, {Queued} waiting", Volatile.Read(ref _processed), orders.Reader.Count);
            }
        }

        public override void Dispose()
        {
            _limiter.Dispose();
            base.Dispose();
        }
    }

    public sealed class PaymentGateway
    {
        public async Task ChargeAsync(Order order, CancellationToken cancellationToken)
        {
            await Task.Delay(Random.Shared.Next(100, 400), cancellationToken);

            // Roughly one charge in 150 finds the connection gone.
            if (Random.Shared.Next(150) == 0)
            {
                throw new GatewayDisconnectedException();
            }
        }
    }
}
