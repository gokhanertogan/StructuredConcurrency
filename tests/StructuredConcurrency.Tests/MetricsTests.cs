using System.Collections.Concurrent;
using System.Diagnostics.Metrics;

namespace StructuredConcurrency.Tests;

// Tests run in parallel and every group is measured while a listener exists, so each test gives its group a unique
// name and looks only at measurements tagged with that name.
public sealed class MetricsTests : IDisposable
{
    private readonly ConcurrentQueue<(string Instrument, double Value, string? Name, string? Outcome)> _measurements = new();
    private readonly MeterListener _listener = new();

    public MetricsTests()
    {
        _listener.InstrumentPublished = (instrument, listener) =>
        {
            if (instrument.Meter.Name == TaskGroup.MeterName)
            {
                listener.EnableMeasurementEvents(instrument);
            }
        };
        _listener.SetMeasurementEventCallback<long>((instrument, value, tags, _) => Record(instrument, value, tags));
        _listener.SetMeasurementEventCallback<double>((instrument, value, tags, _) => Record(instrument, value, tags));
        _listener.Start();
    }

    public void Dispose() => _listener.Dispose();

    private void Record(Instrument instrument, double value, ReadOnlySpan<KeyValuePair<string, object?>> tags)
    {
        string? name = null, outcome = null;
        foreach (KeyValuePair<string, object?> tag in tags)
        {
            if (tag.Key == "taskgroup.name")
            {
                name = (string?)tag.Value;
            }
            else if (tag.Key == "taskgroup.outcome")
            {
                outcome = (string?)tag.Value;
            }
        }
        _measurements.Enqueue((instrument.Name, value, name, outcome));
    }

    private static TaskGroupOptions Named(out string name, int? maxConcurrency = null, bool cancelOnFailure = true)
    {
        name = "group-" + Guid.NewGuid().ToString("N");
        return new TaskGroupOptions { Name = name, MaxConcurrency = maxConcurrency, CancelOnFailure = cancelOnFailure };
    }

    private (string Instrument, double Value, string? Name, string? Outcome)[] Of(string name, string instrument) =>
        _measurements.Where(m => m.Name == name && m.Instrument == instrument).ToArray();

    private double Sum(string name, string instrument) => Of(name, instrument).Sum(m => m.Value);

    [Fact]
    public async Task Group_RecordsItsDurationAndOutcome()
    {
        await TaskGroup.RunAsync(group =>
        {
            group.Run(async _ => await Task.Yield());
            return Task.CompletedTask;
        }, Named(out string name));

        var duration = Assert.Single(Of(name, "taskgroup.duration"));
        Assert.Equal("success", duration.Outcome);
        Assert.True(duration.Value >= 0);
    }

    [Fact]
    public async Task ActiveChildren_GoUpWhileRunning_AndBackToZero()
    {
        var gate = new TaskCompletionSource();
        Task run = TaskGroup.RunAsync(group =>
        {
            group.Run(_ => gate.Task);
            group.Run(_ => gate.Task);
            return Task.CompletedTask;
        }, Named(out string name));

        Assert.Equal(2, Sum(name, "taskgroup.active_children"));
        gate.SetResult();
        await run;
        Assert.Equal(0, Sum(name, "taskgroup.active_children"));
    }

    [Fact]
    public async Task QueuedChildren_AreCountedUntilTheyStart()
    {
        var gate = new TaskCompletionSource();
        Task run = TaskGroup.RunAsync(group =>
        {
            for (int i = 0; i < 3; i++)
            {
                group.Run(_ => gate.Task);
            }
            return Task.CompletedTask;
        }, Named(out string name, maxConcurrency: 1));

        Assert.Equal(1, Sum(name, "taskgroup.active_children"));
        Assert.Equal(2, Sum(name, "taskgroup.queued_children"));
        gate.SetResult();
        await run;
        Assert.Equal(0, Sum(name, "taskgroup.active_children"));
        Assert.Equal(0, Sum(name, "taskgroup.queued_children"));
    }

    [Fact]
    public async Task DroppedQueuedWork_LeavesTheCountsAtZero()
    {
        TaskGroupOptions options = Named(out string name, maxConcurrency: 1);

        // The first child holds the only slot and fails, which drops the two queued behind it without starting them.
        await Assert.ThrowsAsync<InvalidOperationException>(() => TaskGroup.RunAsync(group =>
        {
            group.Run(async _ =>
            {
                await Task.Yield();
                throw new InvalidOperationException();
            });
            group.Run(_ => Task.CompletedTask);
            group.Run(_ => Task.CompletedTask);
            return Task.CompletedTask;
        }, options));

        Assert.Equal(1, Of(name, "taskgroup.active_children").Count(m => m.Value > 0));
        Assert.Equal(0, Sum(name, "taskgroup.active_children"));
        Assert.Equal(0, Sum(name, "taskgroup.queued_children"));
    }

    [Fact]
    public async Task Failures_AreCounted_WithoutTheCancellationsTheyCause()
    {
        TaskGroupOptions options = Named(out string name, cancelOnFailure: false);

        await Assert.ThrowsAsync<InvalidOperationException>(() => TaskGroup.RunAsync(group =>
        {
            group.Run(_ => throw new InvalidOperationException());
            group.Run(_ => throw new ArgumentException());
            group.Run(_ => Task.CompletedTask);
            return Task.CompletedTask;
        }, options));

        Assert.Equal(2, Sum(name, "taskgroup.failures"));
        Assert.Equal("failed", Assert.Single(Of(name, "taskgroup.duration")).Outcome);
    }

    [Fact]
    public async Task CancelledGroup_RecordsTheCancelledOutcome()
    {
        using var cts = new CancellationTokenSource();
        TaskGroupOptions options = Named(out string name);

        Task run = TaskGroup.RunAsync(group =>
        {
            group.Run(ct => Task.Delay(Timeout.Infinite, ct));
            return Task.CompletedTask;
        }, options, cts.Token);
        cts.Cancel();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => run);
        Assert.Equal("cancelled", Assert.Single(Of(name, "taskgroup.duration")).Outcome);
        Assert.Equal(0, Sum(name, "taskgroup.failures"));
    }
}
