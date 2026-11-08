using System.Threading.RateLimiting;

namespace StructuredConcurrency;

/// <summary>Options that control how a <see cref="TaskGroup"/> runs its children and reacts to failures.</summary>
public sealed class TaskGroupOptions
{
    private readonly int? _maxConcurrency;
    private readonly TimeSpan? _timeout;

    /// <summary>Gets the default options: no concurrency limit, and the first failure cancels the remaining children.</summary>
    public static TaskGroupOptions Default { get; } = new();

    /// <summary>
    /// Gets a value indicating whether a failing child cancels its siblings. When <see langword="false"/>, the group
    /// supervises its children: every child runs to completion and all failures are reported at the end. A failing
    /// body cancels the children either way.
    /// </summary>
    public bool CancelOnFailure { get; init; } = true;

    /// <summary>
    /// Gets the group's name, used as the display name of its tracing activity. Defaults to <c>TaskGroup</c>.
    /// </summary>
    /// <remarks>
    /// When tracing is enabled (an OpenTelemetry tracer or another <see cref="System.Diagnostics.ActivityListener"/>
    /// subscribes to the <c>StructuredConcurrency</c> source), each group records an activity that its children's
    /// activities nest under.
    /// </remarks>
    public string? Name { get; init; }

    /// <summary>
    /// Gets a value indicating whether the group records a tracing activity when tracing is enabled. Defaults to
    /// <see langword="true"/>. Turn it off for a group that lives as long as the application, whose activity would
    /// otherwise put everything its children trace into one trace that never ends.
    /// </summary>
    public bool EnableTracing { get; init; } = true;

    /// <summary>
    /// Gets the maximum number of children that run at the same time, or <see langword="null"/> for no limit.
    /// </summary>
    /// <remarks>
    /// Children started beyond the limit wait in a queue and start in order as running children complete.
    /// <see cref="TaskGroup.Run(Func{CancellationToken, Task})"/> never blocks; it returns a task that completes when
    /// the queued work has run. Queued work is not started once the group is cancelled. The body doesn't count towards
    /// the limit, but a child that waits for a queued sibling holds its slot while it waits.
    /// </remarks>
    /// <exception cref="ArgumentOutOfRangeException">The value is less than 1.</exception>
    public int? MaxConcurrency
    {
        get => _maxConcurrency;
        init
        {
            if (value < 1)
            {
                throw new ArgumentOutOfRangeException(nameof(MaxConcurrency), value, "The limit must be at least 1.");
            }
            _maxConcurrency = value;
        }
    }

    /// <summary>
    /// Gets a rate limiter that every child takes one permit from before it starts, or <see langword="null"/> for none.
    /// </summary>
    /// <remarks>
    /// <see cref="MaxConcurrency"/> limits how many children run at once; a rate limiter such as a
    /// <see cref="TokenBucketRateLimiter"/> limits how many start per second, for an API with a rate limit. A child
    /// waits for its permit while it holds its concurrency slot, and gives the permit back when it completes, so a
    /// <see cref="ConcurrencyLimiter"/> shared by several groups limits them together. A child that is refused a permit
    /// fails with <see cref="RateLimitRejectedException"/>. The group doesn't dispose the limiter.
    /// </remarks>
    public RateLimiter? RateLimiter { get; init; }

    /// <summary>
    /// Gets how long the group may run, or <see langword="null"/> for no limit. When the time is up, the group is
    /// cancelled, waits for its body and children to stop, and then throws <see cref="TimeoutException"/>.
    /// </summary>
    /// <remarks>
    /// It is the same as wrapping the group in <see cref="TaskGroup.WithTimeoutAsync(TimeSpan, Func{CancellationToken, Task}, CancellationToken)"/>:
    /// the group throws <see cref="TimeoutException"/> only when the timer is what cancelled it. A failure, a call to
    /// <see cref="TaskGroup.Cancel"/> or the caller's token that comes first is reported as usual. Up to about 49 days,
    /// or <see cref="System.Threading.Timeout.InfiniteTimeSpan"/>.
    /// </remarks>
    /// <exception cref="ArgumentOutOfRangeException">The value is negative or too long.</exception>
    public TimeSpan? Timeout
    {
        get => _timeout;
        init
        {
            if (value is TimeSpan timeout)
            {
                Timeouts.Validate(timeout, nameof(Timeout));
            }
            _timeout = value;
        }
    }

    /// <summary>
    /// Gets the time provider that measures <see cref="Timeout"/>, or <see langword="null"/> for
    /// <see cref="System.TimeProvider.System"/>. Pass a fake one in tests to make time pass on demand.
    /// </summary>
    public TimeProvider? TimeProvider { get; init; }
}
