using System.Diagnostics;
using System.Diagnostics.Metrics;

namespace StructuredConcurrency;

// Metrics for OpenTelemetry and dotnet-counters. A group decides when it is created whether anyone listens; one that
// isn't measured keeps no state for it and pays one null check per child.
public sealed partial class TaskGroup
{
    /// <summary>
    /// The name of the <see cref="Meter"/> that groups record their metrics with. Add it to your OpenTelemetry meter
    /// provider, for example with <c>AddMeter(TaskGroup.MeterName)</c>.
    /// </summary>
    public const string MeterName = "StructuredConcurrency";

    private static readonly Meter s_meter = new(MeterName, typeof(TaskGroup).Assembly.GetName().Version?.ToString());

    private static readonly Histogram<double> s_duration = s_meter.CreateHistogram<double>(
        "taskgroup.duration", unit: "s", description: "How long task groups ran, from start until every child had completed.");

    private static readonly UpDownCounter<long> s_activeChildren = s_meter.CreateUpDownCounter<long>(
        "taskgroup.active_children", unit: "{child}", description: "Children that have started and not yet completed, including ones waiting for a rate limiter permit.");

    private static readonly UpDownCounter<long> s_queuedChildren = s_meter.CreateUpDownCounter<long>(
        "taskgroup.queued_children", unit: "{child}", description: "Children waiting for a MaxConcurrency slot.");

    private static readonly Counter<long> s_failures = s_meter.CreateCounter<long>(
        "taskgroup.failures", unit: "{failure}", description: "Failures recorded by task groups, not counting the cancellations they caused.");

    private static Measurements? StartMeasuring(TaskGroupOptions options) =>
        s_duration.Enabled || s_activeChildren.Enabled || s_queuedChildren.Enabled || s_failures.Enabled
            ? new Measurements(options.Name ?? "TaskGroup")
            : null;

    // The group's name as a tag, and when it started.
    private sealed class Measurements(string name)
    {
        private readonly KeyValuePair<string, object?> _name = new("taskgroup.name", name);
        private readonly long _started = Stopwatch.GetTimestamp();

        public void ChildStarted() => s_activeChildren.Add(1, _name);

        public void ChildCompleted() => s_activeChildren.Add(-1, _name);

        public void Queued(int delta) => s_queuedChildren.Add(delta, _name);

        public void Failed() => s_failures.Add(1, _name);

        public void Completed(string outcome) =>
            s_duration.Record(Stopwatch.GetElapsedTime(_started).TotalSeconds, _name, new("taskgroup.outcome", outcome));
    }
}
