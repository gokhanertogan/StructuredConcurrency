using System.Threading.Channels;
using Microsoft.Extensions.Time.Testing;
using StructuredConcurrency.Channels;

namespace StructuredConcurrency.Tests;

public class ChannelTests
{
    private sealed class Boom() : Exception("boom");

    // A bug that makes a select wait forever fails the test after this long instead of hanging the run.
    private static readonly TimeSpan Guard = TimeSpan.FromSeconds(10);

    private static Channel<T> Unbounded<T>() => Channel.CreateUnbounded<T>();

    // ----------------------------------------------------------------------------------------------------------------
    // ChannelSelect

    [Fact]
    public async Task Select_RunsTheReadyCase()
    {
        var numbers = Unbounded<int>();
        var words = Unbounded<string>();
        words.Writer.TryWrite("hello");
        string? got = null;

        bool ran = await new ChannelSelect()
            .Case(numbers.Reader, (int _) => throw new InvalidOperationException("not ready"))
            .Case(words.Reader, (string word) => got = word)
            .RunAsync().WaitAsync(Guard);

        Assert.True(ran);
        Assert.Equal("hello", got);
    }

    [Fact]
    public async Task Select_WaitsForTheFirstItem()
    {
        var a = Unbounded<int>();
        var b = Unbounded<int>();
        int got = 0;

        Task<bool> run = new ChannelSelect()
            .Case(a.Reader, (int n) => got = n)
            .Case(b.Reader, (int n) => got = -n)
            .RunAsync().WaitAsync(Guard);

        Assert.False(run.IsCompleted);
        b.Writer.TryWrite(7);

        Assert.True(await run);
        Assert.Equal(-7, got);
    }

    [Fact]
    public async Task Select_AwaitsAsyncHandler()
    {
        var channel = Unbounded<int>();
        channel.Writer.TryWrite(1);
        var handlerGate = new TaskCompletionSource();

        Task<bool> run = new ChannelSelect()
            .Case(channel.Reader, (int _) => handlerGate.Task)
            .RunAsync().WaitAsync(Guard);

        Assert.False(run.IsCompleted);
        handlerGate.SetResult();
        Assert.True(await run);
    }

    [Fact]
    public async Task Select_Default_RunsWhenNothingIsReady()
    {
        var channel = Unbounded<int>();
        bool idle = false;

        bool ran = await new ChannelSelect()
            .Case(channel.Reader, (int _) => { })
            .Default(() => idle = true)
            .RunAsync().WaitAsync(Guard);

        Assert.True(ran);
        Assert.True(idle);
    }

    [Fact]
    public async Task Select_Timeout_RunsWhenNothingArrives()
    {
        var channel = Unbounded<int>();
        bool timedOut = false;

        bool ran = await new ChannelSelect()
            .Case(channel.Reader, (int _) => { })
            .Timeout(TimeSpan.FromMilliseconds(20), () => timedOut = true)
            .RunAsync().WaitAsync(Guard);

        Assert.True(ran);
        Assert.True(timedOut);
    }

    [Fact]
    public async Task Select_ReturnsFalse_WhenEveryChannelHasCompleted()
    {
        var a = Unbounded<int>();
        var b = Unbounded<int>();
        var select = new ChannelSelect()
            .Case(a.Reader, (int _) => { })
            .Case(b.Reader, (int _) => { });

        a.Writer.TryWrite(1);
        a.Writer.Complete();
        Task<bool> run = select.RunAsync().WaitAsync(Guard);
        Assert.True(await run);          // the buffered item is still delivered

        Task<bool> next = select.RunAsync().WaitAsync(Guard);
        Assert.False(next.IsCompleted);  // b is still open
        b.Writer.Complete();
        Assert.False(await next);
    }

    [Fact]
    public async Task Select_Cancellation_Throws()
    {
        var channel = Unbounded<int>();
        using var cts = new CancellationTokenSource();

        Task<bool> run = new ChannelSelect().Case(channel.Reader, (int _) => { }).RunAsync(cts.Token).WaitAsync(Guard);
        cts.Cancel();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => run);
    }

    [Fact]
    public async Task Select_ChannelCompletedWithError_Throws()
    {
        var channel = Unbounded<int>();
        Task<bool> run = new ChannelSelect().Case(channel.Reader, (int _) => { }).RunAsync().WaitAsync(Guard);

        channel.Writer.Complete(new Boom());

        await Assert.ThrowsAsync<Boom>(() => run);
    }

    [Fact]
    public async Task Select_PicksFairly_WhenSeveralChannelsAreReady()
    {
        const int Runs = 1000;
        var a = Unbounded<int>();
        var b = Unbounded<int>();
        for (int i = 0; i < Runs; i++)
        {
            a.Writer.TryWrite(i);
            b.Writer.TryWrite(i);
        }

        int fromA = 0, fromB = 0;
        var select = new ChannelSelect()
            .Case(a.Reader, (int _) => fromA++)
            .Case(b.Reader, (int _) => fromB++);
        for (int i = 0; i < Runs; i++)
        {
            await select.RunAsync().WaitAsync(Guard);
        }

        // Each run picks a or b with equal probability: 350..650 is about 9.5 standard deviations either side of 500.
        Assert.Equal(Runs, fromA + fromB);
        Assert.InRange(fromA, 350, 650);
    }

    [Fact]
    public async Task Select_InALoop_DrainsEveryChannel()
    {
        var a = Unbounded<int>();
        var b = Unbounded<int>();
        var seen = new List<int>();
        for (int i = 0; i < 50; i++)
        {
            a.Writer.TryWrite(i);
            b.Writer.TryWrite(100 + i);
        }
        a.Writer.Complete();
        b.Writer.Complete();

        var select = new ChannelSelect()
            .Case(a.Reader, (int n) => seen.Add(n))
            .Case(b.Reader, (int n) => seen.Add(n));
        while (await select.RunAsync().WaitAsync(Guard))
        {
        }

        Assert.Equal(100, seen.Count);
        Assert.Equal(Enumerable.Range(0, 50).Concat(Enumerable.Range(100, 50)), seen.Order());
    }

    // ----------------------------------------------------------------------------------------------------------------
    // ChannelSelect.Send

    [Fact]
    public async Task Send_WritesWhenThereIsRoom_AndRunsTheHandler()
    {
        var output = Channel.CreateBounded<int>(1);
        bool sent = false;

        bool ran = await new ChannelSelect()
            .Send(output.Writer, () => 5, () => sent = true)
            .RunAsync().WaitAsync(Guard);

        Assert.True(ran);
        Assert.True(sent);
        Assert.True(output.Reader.TryRead(out int item));
        Assert.Equal(5, item);
    }

    [Fact]
    public async Task Send_WaitsForRoom()
    {
        var output = Channel.CreateBounded<int>(1);
        output.Writer.TryWrite(1);
        bool sent = false;

        Task<bool> run = new ChannelSelect()
            .Send(output.Writer, () => 2, () => sent = true)
            .RunAsync().WaitAsync(Guard);

        Assert.False(run.IsCompleted);
        Assert.True(output.Reader.TryRead(out _));

        Assert.True(await run);
        Assert.True(sent);
        Assert.True(output.Reader.TryRead(out int item));
        Assert.Equal(2, item);
    }

    [Fact]
    public async Task Send_RacesWithReads_AndOnlyOneCaseRuns()
    {
        var input = Unbounded<int>();
        var output = Channel.CreateBounded<int>(1);
        output.Writer.TryWrite(0);   // full: only the read can run
        string? ran = null;

        Task<bool> run = new ChannelSelect()
            .Case(input.Reader, (int _) => ran = "read")
            .Send(output.Writer, () => 1, () => ran = "send")
            .RunAsync().WaitAsync(Guard);
        input.Writer.TryWrite(9);

        Assert.True(await run);
        Assert.Equal("read", ran);
        Assert.Equal(1, output.Reader.Count);
    }

    [Fact]
    public async Task Send_Disabled_IsSkipped()
    {
        var output = Unbounded<int>();
        bool ranDefault = false;

        bool ran = await new ChannelSelect()
            .Send(output.Writer, () => throw new InvalidOperationException("must not be asked for"), () => { }, when: () => false)
            .Default(() => ranDefault = true)
            .RunAsync().WaitAsync(Guard);

        // A disabled case can never run, so there is nothing left that could make progress.
        Assert.False(ran);
        Assert.False(ranDefault);
        Assert.Equal(0, output.Reader.Count);
    }

    [Fact]
    public async Task Send_BuffersBetweenChannels_UntilTheInputCompletesAndTheBufferIsEmpty()
    {
        // Go's classic buffer goroutine: the send case is enabled only while there is something to send.
        var input = Unbounded<int>();
        var output = Unbounded<int>();
        var pending = new Queue<int>();
        var select = new ChannelSelect()
            .Case(input.Reader, (int item) => pending.Enqueue(item))
            .Send(output.Writer, () => pending.Peek(), () => pending.Dequeue(), when: () => pending.Count > 0);

        foreach (int item in new[] { 1, 2, 3 })
        {
            input.Writer.TryWrite(item);
        }
        input.Writer.Complete();

        while (await select.RunAsync().WaitAsync(Guard)) { }

        output.Writer.Complete();
        Assert.Equal(new[] { 1, 2, 3 }, await output.Reader.ReadAllAsync().ToListAsync());
    }

    [Fact]
    public async Task Send_ToACompletedChannel_CountsAsCompleted()
    {
        var output = Unbounded<int>();
        output.Writer.Complete();

        bool ran = await new ChannelSelect()
            .Send(output.Writer, () => 1, () => throw new InvalidOperationException("not sent"))
            .RunAsync().WaitAsync(Guard);

        Assert.False(ran);
    }

    [Fact]
    public async Task Send_ToACompletedFullChannel_WithADefaultCase_EndsTheLoop()
    {
        // A default case means RunAsync never waits, so the case has to find out without waiting.
        var output = Channel.CreateBounded<int>(1);
        output.Writer.TryWrite(0);
        output.Writer.Complete();
        int defaults = 0;

        bool ran = await new ChannelSelect()
            .Send(output.Writer, () => 1, () => throw new InvalidOperationException("not sent"))
            .Default(() => defaults++)
            .RunAsync().WaitAsync(Guard);

        Assert.False(ran);
        Assert.Equal(0, defaults);
    }

    [Fact]
    public async Task Send_ToAFullChannel_WithADefaultCase_RunsTheDefault_AndLeavesNoWaiter()
    {
        var output = Channel.CreateBounded<int>(1);
        output.Writer.TryWrite(0);
        int defaults = 0;
        var select = new ChannelSelect().Send(output.Writer, () => 1, () => { }).Default(() => defaults++);

        Assert.True(await select.RunAsync().WaitAsync(Guard));
        Assert.True(await select.RunAsync().WaitAsync(Guard));
        Assert.Equal(2, defaults);

        Assert.True(output.Reader.TryRead(out _));
        Assert.True(await select.RunAsync().WaitAsync(Guard));
        Assert.Equal(2, defaults);   // it had room this time, so the send ran
        Assert.True(output.Reader.TryRead(out int item));
        Assert.Equal(1, item);
    }

    [Fact]
    public async Task Send_ToAChannelCompletedWithAnError_Throws()
    {
        var output = Channel.CreateBounded<int>(1);
        output.Writer.Complete(new Boom());
        var select = new ChannelSelect().Send(output.Writer, () => 1, () => { });

        await Assert.ThrowsAsync<Boom>(() => select.RunAsync().WaitAsync(Guard));
        await Assert.ThrowsAsync<Boom>(() => select.RunAsync().WaitAsync(Guard));
    }

    [Fact]
    public async Task Send_CallerCancellation_LeavesTheSelectUsable()
    {
        var output = Channel.CreateBounded<int>(1);
        output.Writer.TryWrite(0);
        var select = new ChannelSelect().Send(output.Writer, () => 1, () => { });
        using var cts = new CancellationTokenSource();

        Task<bool> run = select.RunAsync(cts.Token);
        cts.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => run.WaitAsync(Guard));

        Assert.True(output.Reader.TryRead(out _));
        Assert.True(await select.RunAsync().WaitAsync(Guard));
    }

    // ----------------------------------------------------------------------------------------------------------------
    // MergeWith

    [Fact]
    public async Task Merge_YieldsEveryItem_UntilAllChannelsComplete()
    {
        var a = Unbounded<int>();
        var b = Unbounded<int>();
        var c = Unbounded<int>();

        Task producers = TaskGroup.RunAsync(group =>
        {
            group.Run(async _ =>
            {
                for (int i = 0; i < 100; i++) { await a.Writer.WriteAsync(i); await Task.Yield(); }
                a.Writer.Complete();
            });
            group.Run(async _ =>
            {
                for (int i = 100; i < 200; i++) { await b.Writer.WriteAsync(i); }
                b.Writer.Complete();
            });
            group.Run(_ =>
            {
                c.Writer.Complete();  // an empty channel doesn't stop the others
                return Task.CompletedTask;
            });
            return Task.CompletedTask;
        });

        var items = new List<int>();
        await foreach (int item in a.Reader.MergeWith(b.Reader, c.Reader))
        {
            items.Add(item);
        }
        await producers;

        Assert.Equal(Enumerable.Range(0, 200), items.Order());
    }

    [Fact]
    public async Task Merge_ChannelCompletedWithError_Throws()
    {
        var a = Unbounded<int>();
        var b = Unbounded<int>();
        a.Writer.Complete(new Boom());

        await Assert.ThrowsAsync<Boom>(async () =>
        {
            await foreach (int _ in a.Reader.MergeWith(b.Reader))
            {
            }
        });
    }

    [Fact]
    public async Task Merge_Cancellation_Throws()
    {
        var a = Unbounded<int>();
        var b = Unbounded<int>();
        using var cts = new CancellationTokenSource();

        Task enumerate = Task.Run(async () =>
        {
            await foreach (int _ in a.Reader.MergeWith(b.Reader).WithCancellation(cts.Token))
            {
            }
        });
        cts.Cancel();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => enumerate);
    }

    // ----------------------------------------------------------------------------------------------------------------
    // BroadcastAsync

    [Fact]
    public async Task Broadcast_SendsEveryItemToEveryTarget_ThenCompletesThem()
    {
        var source = Unbounded<int>();
        var audit = Unbounded<int>();
        var metrics = Unbounded<int>();
        for (int i = 0; i < 10; i++)
        {
            source.Writer.TryWrite(i);
        }
        source.Writer.Complete();

        await source.Reader.BroadcastAsync([audit.Writer, metrics.Writer]);

        Assert.Equal(Enumerable.Range(0, 10), await audit.Reader.ReadAllAsync().ToListAsync());
        Assert.Equal(Enumerable.Range(0, 10), await metrics.Reader.ReadAllAsync().ToListAsync());
    }

    [Fact]
    public async Task Broadcast_SourceError_CompletesTargetsWithIt()
    {
        var source = Unbounded<int>();
        var target = Unbounded<int>();
        source.Writer.Complete(new Boom());

        await Assert.ThrowsAsync<Boom>(() => source.Reader.BroadcastAsync([target.Writer]));
        await Assert.ThrowsAsync<Boom>(() => target.Reader.Completion);
    }

    [Fact]
    public async Task Select_DefaultCase_ReturnsFalse_WhenEveryChannelHasCompleted()
    {
        var channel = Unbounded<int>();
        channel.Writer.Complete();
        int defaults = 0;

        bool ran = await new ChannelSelect()
            .Case(channel.Reader, (int _) => { })
            .Default(() => defaults++)
            .RunAsync().WaitAsync(Guard);

        Assert.False(ran);
        Assert.Equal(0, defaults);
    }

    [Fact]
    public async Task Select_TimeoutCase_ReturnsFalse_WhenEveryChannelHasCompleted()
    {
        var channel = Unbounded<int>();
        channel.Writer.Complete();
        int timeouts = 0;

        bool ran = await new ChannelSelect()
            .Case(channel.Reader, (int _) => { })
            .Timeout(TimeSpan.FromMinutes(5), () => timeouts++)
            .RunAsync().WaitAsync(Guard);

        Assert.False(ran);
        Assert.Equal(0, timeouts);
    }

    [Fact]
    public void Select_Timeout_RejectsValuesTaskDelayCannotWaitFor()
    {
        var select = new ChannelSelect();

        Assert.Throws<ArgumentOutOfRangeException>(() => select.Timeout(TimeSpan.FromMilliseconds(-2), () => { }));
        Assert.Throws<ArgumentOutOfRangeException>(() => select.Timeout(TimeSpan.MaxValue, () => { }));
        select.Timeout(Timeout.InfiniteTimeSpan, () => { });
    }

    [Fact]
    public async Task Select_LongestTimeout_IsAcceptedByTaskDelay()
    {
        var channel = Unbounded<int>();
        channel.Writer.TryWrite(1);
        using var cts = new CancellationTokenSource();
        var select = new ChannelSelect()
            .Case(channel.Reader, (int _) => { })
            .Timeout(TimeSpan.FromMilliseconds(uint.MaxValue - 1), () => { });

        Assert.True(await select.RunAsync().WaitAsync(Guard));   // reads the item
        Task<bool> waiting = select.RunAsync(cts.Token);         // has to wait, so starts the delay
        cts.Cancel();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => waiting.WaitAsync(Guard));
    }

    [Fact]
    public async Task Broadcast_TargetClosedByItsConsumer_DoesNotStopTheOthers()
    {
        var source = Unbounded<int>();
        var closed = Unbounded<int>();
        var healthy = Unbounded<int>();
        closed.Writer.Complete();
        for (int i = 0; i < 5; i++)
        {
            source.Writer.TryWrite(i);
        }
        source.Writer.Complete();

        await source.Reader.BroadcastAsync([closed.Writer, healthy.Writer]).WaitAsync(Guard);

        Assert.Equal(Enumerable.Range(0, 5), await healthy.Reader.ReadAllAsync().ToListAsync());
        Assert.True(healthy.Reader.Completion.IsCompletedSuccessfully);
    }

    [Fact]
    public async Task Broadcast_EveryTargetClosed_StopsWithoutReadingFurther()
    {
        var source = Unbounded<int>();
        var target = Unbounded<int>();
        target.Writer.Complete();
        source.Writer.TryWrite(1);
        source.Writer.TryWrite(2);

        await source.Reader.BroadcastAsync([target.Writer]).WaitAsync(Guard);

        Assert.Equal(1, source.Reader.Count);
    }

    [Fact]
    public async Task Broadcast_NullTarget_ThrowsBeforeReading()
    {
        var source = Unbounded<int>();
        var target = Unbounded<int>();
        source.Writer.TryWrite(1);

        await Assert.ThrowsAsync<ArgumentException>(() => source.Reader.BroadcastAsync([target.Writer, null!]));

        Assert.Equal(1, source.Reader.Count);
        Assert.False(target.Reader.Completion.IsCompleted);
    }

    [Fact]
    public async Task Select_ChannelCompletedWithCancellation_Throws()
    {
        var channel = Unbounded<int>();
        channel.Writer.Complete(new OperationCanceledException());

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() =>
            new ChannelSelect().Case(channel.Reader, (int _) => { }).RunAsync().WaitAsync(Guard));
    }

    [Fact]
    public async Task Merge_ChannelCompletedWithCancellation_Throws()
    {
        var cancelled = Unbounded<int>();
        var open = Unbounded<int>();
        cancelled.Writer.Complete(new OperationCanceledException());
        open.Writer.Complete();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => cancelled.Reader.MergeWith(open.Reader).ToListAsync().WaitAsync(Guard));
    }

    [Fact]
    public async Task Broadcast_TargetCompletedWithCancellation_DoesNotStopTheOthers()
    {
        var source = Unbounded<int>();
        var cancelled = Unbounded<int>();
        var healthy = Unbounded<int>();
        cancelled.Writer.Complete(new OperationCanceledException());
        source.Writer.TryWrite(1);
        source.Writer.TryWrite(2);
        source.Writer.Complete();

        await source.Reader.BroadcastAsync([cancelled.Writer, healthy.Writer]).WaitAsync(Guard);

        Assert.Equal([1, 2], await healthy.Reader.ReadAllAsync().ToListAsync());
    }

    [Fact]
    public async Task Select_TimeProvider_FiresTheTimeoutWhenItsTimePasses()
    {
        var time = new FakeTimeProvider();
        var channel = Unbounded<int>();
        bool timedOut = false;

        Task<bool> run = new ChannelSelect(time)
            .Case(channel.Reader, (int _) => { })
            .Timeout(TimeSpan.FromMinutes(5), () => timedOut = true)
            .RunAsync();

        await Task.Delay(50);
        Assert.False(run.IsCompleted);

        time.Advance(TimeSpan.FromMinutes(5));
        Assert.True(await run.WaitAsync(Guard));
        Assert.True(timedOut);
    }
}

internal static class AsyncEnumerableTestExtensions
{
    public static async Task<List<T>> ToListAsync<T>(this IAsyncEnumerable<T> source)
    {
        var list = new List<T>();
        await foreach (T item in source)
        {
            list.Add(item);
        }
        return list;
    }
}
