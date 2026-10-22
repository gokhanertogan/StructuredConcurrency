using System.Threading.Channels;
using StructuredConcurrency.Channels;

namespace StructuredConcurrency.Tests;

public class StartAndProduceTests
{
    private sealed class Boom() : Exception("boom");

    private static readonly TimeSpan Guard = TimeSpan.FromSeconds(10);

    // ----------------------------------------------------------------------------------------------------------------
    // StartAsync

    [Fact]
    public async Task Start_ReturnsTheReadyValue_AndTheChildKeepsRunningInTheGroup()
    {
        var stop = new TaskCompletionSource();
        bool childEnded = false;

        int port = await TaskGroup.RunAsync(async group =>
        {
            int port = await group.StartAsync<int>(async (ready, ct) =>
            {
                ready(8080);
                await stop.Task;
                childEnded = true;
            });

            Assert.False(childEnded);
            stop.SetResult();
            return port;
        }).WaitAsync(Guard);

        Assert.Equal(8080, port);
        Assert.True(childEnded);   // the group waited for the child
    }

    [Fact]
    public async Task Start_WithoutAValue_CompletesWhenReady()
    {
        bool ready = false;

        await TaskGroup.RunAsync(async group =>
        {
            await group.StartAsync(async (signal, ct) =>
            {
                await Task.Yield();
                ready = true;
                signal();
            });
            Assert.True(ready);
        }).WaitAsync(Guard);
    }

    [Fact]
    public async Task Start_ChildFailingBeforeReady_FailsTheStartAndTheGroup()
    {
        Task<int>? start = null;

        Task run = TaskGroup.RunAsync(group =>
        {
            start = group.StartAsync<int>(async (ready, ct) =>
            {
                await Task.Yield();
                throw new Boom();
            });
            return Task.CompletedTask;
        });

        await Assert.ThrowsAsync<Boom>(() => run.WaitAsync(Guard));
        await Assert.ThrowsAsync<Boom>(() => start!);
    }

    [Fact]
    public async Task Start_ChildCompletingWithoutSignalling_Throws()
    {
        await TaskGroup.RunAsync(async group =>
        {
            await Assert.ThrowsAsync<InvalidOperationException>(() => group.StartAsync<int>((ready, ct) => Task.CompletedTask));
        }).WaitAsync(Guard);
    }

    [Fact]
    public async Task Start_SignallingTwice_Throws()
    {
        Exception? second = null;

        await TaskGroup.RunAsync(async group =>
        {
            await group.StartAsync<int>((ready, ct) =>
            {
                ready(1);
                second = Record.Exception(() => ready(2));
                return Task.CompletedTask;
            });
        }).WaitAsync(Guard);

        Assert.IsType<InvalidOperationException>(second);
    }

    [Fact]
    public async Task Start_InACancelledGroup_IsCancelled()
    {
        await TaskGroup.RunAsync(async group =>
        {
            group.Cancel();
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => group.StartAsync<int>((ready, ct) => Task.CompletedTask));
        }).WaitAsync(Guard);
    }

    // ----------------------------------------------------------------------------------------------------------------
    // Produce

    [Fact]
    public async Task Produce_StreamsTheItems_AndCompletesTheChannel()
    {
        List<int> items = await TaskGroup.RunAsync(async group =>
        {
            ChannelReader<int> numbers = group.Produce<int>(async (writer, ct) =>
            {
                for (int i = 0; i < 5; i++)
                {
                    await writer.WriteAsync(i, ct);
                }
            });
            return await numbers.ReadAllAsync().ToListAsync();
        }).WaitAsync(Guard);

        Assert.Equal(Enumerable.Range(0, 5), items);
    }

    [Fact]
    public async Task Produce_ProducerFailure_ReachesTheReaderAndTheGroup()
    {
        Exception? readerSaw = null;

        Task run = TaskGroup.RunAsync(async group =>
        {
            ChannelReader<int> numbers = group.Produce<int>(async (writer, ct) =>
            {
                await writer.WriteAsync(1, ct);
                throw new Boom();
            });
            readerSaw = await Record.ExceptionAsync(() => numbers.ReadAllAsync().ToListAsync());
        });

        await Assert.ThrowsAsync<Boom>(() => run.WaitAsync(Guard));
        Assert.IsType<Boom>(readerSaw);
    }

    [Fact]
    public async Task Produce_InACancelledGroup_CompletesTheChannelInsteadOfHanging()
    {
        await TaskGroup.RunAsync(async group =>
        {
            group.Cancel();
            ChannelReader<int> numbers = group.Produce<int>((writer, ct) => throw new InvalidOperationException("not run"));
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => numbers.Completion.WaitAsync(Guard));
        }).WaitAsync(Guard);
    }

    [Fact]
    public async Task Produce_CapacityLimitsHowFarTheProducerRunsAhead()
    {
        int written = 0;

        await TaskGroup.RunAsync(async group =>
        {
            ChannelReader<int> numbers = group.Produce<int>(async (writer, ct) =>
            {
                for (int i = 0; i < 10; i++)
                {
                    await writer.WriteAsync(i, ct);
                    written++;
                }
            }, capacity: 2);

            await Task.Delay(50);
            Assert.Equal(2, written);
            await numbers.ReadAllAsync().ToListAsync();
        }).WaitAsync(Guard);

        Assert.Equal(10, written);
    }
}
