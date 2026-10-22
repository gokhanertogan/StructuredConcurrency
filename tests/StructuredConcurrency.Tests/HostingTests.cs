using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using StructuredConcurrency.Hosting;

namespace StructuredConcurrency.Tests;

public class HostingTests
{
    private sealed class Boom() : Exception("boom");

    private static readonly TimeSpan Guard = TimeSpan.FromSeconds(10);

    // Two workers that run until they are cancelled and take a moment to clean up; a third one fails on demand.
    private sealed class Workers : StructuredBackgroundService
    {
        private int _cleanedUp;

        public TaskCompletionSource Started { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public TaskCompletionSource Fail { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public int CleanedUp => Volatile.Read(ref _cleanedUp);

        protected override TaskGroupOptions Options => new() { Name = "workers" };

        protected override Task ExecuteAsync(TaskGroup group)
        {
            for (int i = 0; i < 2; i++)
            {
                group.Run(async ct =>
                {
                    try
                    {
                        await Task.Delay(Timeout.Infinite, ct);
                    }
                    finally
                    {
                        await Task.Delay(20, CancellationToken.None);   // cleanup that takes a moment
                        Interlocked.Increment(ref _cleanedUp);
                    }
                });
            }
            group.Run(async ct =>
            {
                await Fail.Task.WaitAsync(ct);
                throw new Boom();
            });
            Started.SetResult();
            return Task.CompletedTask;
        }
    }

    // Starts more children than its limit allows and records how many run at once.
    private sealed class Limited : StructuredBackgroundService
    {
        private int _running;

        public int MaxRunning { get; private set; }

        protected override TaskGroupOptions Options => new() { MaxConcurrency = 2 };

        protected override Task ExecuteAsync(TaskGroup group)
        {
            for (int i = 0; i < 6; i++)
            {
                group.Run(async _ =>
                {
                    int now = Interlocked.Increment(ref _running);
                    MaxRunning = Math.Max(MaxRunning, now);
                    await Task.Delay(5);
                    Interlocked.Decrement(ref _running);
                });
            }
            return Task.CompletedTask;
        }
    }

    [Fact]
    public async Task Stop_CancelsEveryChild_AndWaitsForThem()
    {
        var service = new Workers();
        await service.StartAsync(CancellationToken.None);
        await service.Started.Task.WaitAsync(Guard);

        await service.StopAsync(CancellationToken.None).WaitAsync(Guard);

        Assert.Equal(2, service.CleanedUp);
        Assert.True(service.ExecuteTask!.IsCanceled);
    }

    [Fact]
    public async Task ChildFailure_CancelsTheOthers_AndEndsTheServiceWithIt()
    {
        var service = new Workers();
        await service.StartAsync(CancellationToken.None);
        await service.Started.Task.WaitAsync(Guard);

        service.Fail.SetResult();

        await Assert.ThrowsAsync<Boom>(() => service.ExecuteTask!.WaitAsync(Guard));
        Assert.Equal(2, service.CleanedUp);
    }

    [Fact]
    public async Task ChildFailure_StopsTheHost()
    {
        using IHost host = Host.CreateDefaultBuilder()
            .ConfigureLogging(logging => logging.ClearProviders())
            .ConfigureServices(services => services.AddSingleton<Workers>().AddHostedService(sp => sp.GetRequiredService<Workers>()))
            .Build();
        var service = host.Services.GetRequiredService<Workers>();

        await host.StartAsync();
        await service.Started.Task.WaitAsync(Guard);
        service.Fail.SetResult();

        await host.WaitForShutdownAsync().WaitAsync(Guard);
        Assert.Equal(2, service.CleanedUp);
    }

    [Fact]
    public async Task OverriddenOptions_AreApplied()
    {
        var service = new Limited();
        await service.StartAsync(CancellationToken.None);
        await service.ExecuteTask!.WaitAsync(Guard);

        Assert.InRange(service.MaxRunning, 1, 2);
    }
}
