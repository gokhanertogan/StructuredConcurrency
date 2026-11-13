using System.Diagnostics.CodeAnalysis;
using System.Threading.RateLimiting;

namespace StructuredConcurrency;

// Start a child only if the group's limits let it start now, like Go's errgroup.TryGo: for shedding load instead of
// queueing it.
public sealed partial class TaskGroup
{
    /// <summary>
    /// Starts a child only if it can start right away: when a <see cref="TaskGroupOptions.MaxConcurrency"/> slot is
    /// free and <see cref="TaskGroupOptions.RateLimiter"/> grants a permit without waiting. Otherwise
    /// <paramref name="work"/> is not invoked, and nothing is queued.
    /// </summary>
    /// <param name="work">The child's work; it receives <see cref="CancellationToken"/>.</param>
    /// <param name="task">
    /// When this method returns <see langword="true"/>, the child's task, as <see cref="Run(Func{CancellationToken, Task})"/>
    /// returns it; otherwise <see langword="null"/>.
    /// </param>
    /// <returns>
    /// <see langword="true"/> if the child was started, or if the group is already cancelled and <paramref name="task"/>
    /// is a cancelled task, as with <see cref="Run(Func{CancellationToken, Task})"/>; <see langword="false"/> if the
    /// group is at its limit.
    /// </returns>
    /// <exception cref="InvalidOperationException">The group has already completed.</exception>
    /// <remarks>
    /// <code>
    /// if (!group.TryRun(ct => HandleAsync(request, ct), out _))
    /// {
    ///     return Results.StatusCode(503);   // busy: reject now rather than queue
    /// }
    /// </code>
    /// The permit is taken when the child starts, rather than while it runs as with <c>Run</c>, and is held until the
    /// child completes.
    /// </remarks>
    public bool TryRun(Func<CancellationToken, Task> work, [NotNullWhen(true)] out Task? task)
    {
        ArgumentNullException.ThrowIfNull(work);
        if (!TryReserve(out RateLimitLease? lease, out bool slot))
        {
            task = null;
            return false;
        }
        if (!TryAddReserved(lease, slot))
        {
            task = Task.FromCanceled(CancellationToken);
            return true;
        }
        if (lease is not null)
        {
            work = WithLease(lease, work);
        }

        _measurements?.ChildStarted();
        task = Invoke(work, CancellationToken);
        TrackReserved(task, slot);
        return true;
    }

    /// <summary>
    /// Starts a child only if it can start right away: when a <see cref="TaskGroupOptions.MaxConcurrency"/> slot is
    /// free and <see cref="TaskGroupOptions.RateLimiter"/> grants a permit without waiting. Otherwise
    /// <paramref name="work"/> is not invoked, and nothing is queued.
    /// </summary>
    /// <typeparam name="T">The type of the child's result.</typeparam>
    /// <param name="work">The child's work; it receives <see cref="CancellationToken"/>.</param>
    /// <param name="task">
    /// When this method returns <see langword="true"/>, the child's task, as <see cref="Run{T}(Func{CancellationToken, Task{T}})"/>
    /// returns it; otherwise <see langword="null"/>.
    /// </param>
    /// <returns>
    /// <see langword="true"/> if the child was started, or if the group is already cancelled and <paramref name="task"/>
    /// is a cancelled task, as with <see cref="Run{T}(Func{CancellationToken, Task{T}})"/>; <see langword="false"/> if
    /// the group is at its limit.
    /// </returns>
    /// <exception cref="InvalidOperationException">The group has already completed.</exception>
    public bool TryRun<T>(Func<CancellationToken, Task<T>> work, [NotNullWhen(true)] out Task<T>? task)
    {
        ArgumentNullException.ThrowIfNull(work);
        if (!TryReserve(out RateLimitLease? lease, out bool slot))
        {
            task = null;
            return false;
        }
        if (!TryAddReserved(lease, slot))
        {
            task = Task.FromCanceled<T>(CancellationToken);
            return true;
        }
        if (lease is not null)
        {
            work = WithLease(lease, work);
        }

        _measurements?.ChildStarted();
        task = Invoke(work, CancellationToken);
        TrackReserved(task, slot);
        return true;
    }

    // Takes a concurrency slot, then a permit, without waiting for either. The slot goes first, so that a permit (a
    // token bucket's token, say) isn't spent when there is no slot to use it in.
    private bool TryReserve(out RateLimitLease? lease, out bool slot)
    {
        lease = null;
        slot = false;
        if (_queue is not null)
        {
            lock (_queue)
            {
                if (_running >= _maxConcurrency)
                {
                    return false;
                }
                _running++;
            }
            slot = true;
        }

        if (_rateLimiter is not null)
        {
            RateLimitLease attempt = _rateLimiter.AttemptAcquire(1);
            if (!attempt.IsAcquired)
            {
                attempt.Dispose();
                ReleaseSlot(slot);
                return false;
            }
            lease = attempt;
        }
        return true;
    }

    // Counts the child, or gives back what TryReserve took when the group is cancelled or has completed.
    private bool TryAddReserved(RateLimitLease? lease, bool slot)
    {
        bool added = false;
        try
        {
            added = TryAddChild();
            return added;
        }
        finally
        {
            if (!added)
            {
                lease?.Dispose();
                ReleaseSlot(slot);
            }
        }
    }

    private void ReleaseSlot(bool slot)
    {
        if (slot && TryTakeNext(out QueuedChild? next))
        {
            // Run queued work while a slot was held here: hand the slot on, as a completing child would.
            Pump(next);
        }
    }

    private void TrackReserved(Task task, bool slot)
    {
        if (!slot)
        {
            Track(task, isBody: false);
        }
        else if (task.IsCompleted)
        {
            s_onLimitedChildCompleted(task, this);
        }
        else
        {
            task.ContinueWith(
                s_onLimitedChildCompleted,
                this,
                CancellationToken.None,
                TaskContinuationOptions.ExecuteSynchronously,
                TaskScheduler.Default);
        }
    }

    private static Func<CancellationToken, Task> WithLease(RateLimitLease lease, Func<CancellationToken, Task> work) =>
        async cancellationToken =>
        {
            using (lease)
            {
                await Invoke(work, cancellationToken).ConfigureAwait(false);
            }
        };

    private static Func<CancellationToken, Task<T>> WithLease<T>(RateLimitLease lease, Func<CancellationToken, Task<T>> work) =>
        async cancellationToken =>
        {
            using (lease)
            {
                return await Invoke(work, cancellationToken).ConfigureAwait(false);
            }
        };
}
