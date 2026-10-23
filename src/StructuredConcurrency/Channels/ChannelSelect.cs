using System.Diagnostics.CodeAnalysis;
using System.Runtime.ExceptionServices;
using System.Threading.Channels;

namespace StructuredConcurrency.Channels;

/// <summary>
/// Waits on several channels at once and runs the first case that can proceed, like Go's <c>select</c> statement: a
/// read from a channel that has an item, or a write to a channel that has room.
/// </summary>
/// <remarks>
/// Build the cases once and call <see cref="RunAsync"/> as often as needed, typically in a loop:
/// <c>while (await select.RunAsync(ct)) { }</c>. When several cases are ready, one is picked at random, so no channel
/// is starved. Nothing keeps waiting in the background after <see cref="RunAsync"/> returns.
/// </remarks>
public sealed class ChannelSelect
{
    private readonly List<SelectCase> _cases = [];
    private readonly TimeProvider _timeProvider;
    private TimeSpan _timeout;
    private Func<Task>? _onTimeout;
    private Func<Task>? _onDefault;

    /// <summary>Initializes a new instance of the <see cref="ChannelSelect"/> class.</summary>
    public ChannelSelect()
        : this(TimeProvider.System)
    {
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="ChannelSelect"/> class that measures its timeout case with
    /// <paramref name="timeProvider"/>.
    /// </summary>
    /// <param name="timeProvider">Measures the timeout; pass a fake one in tests to make time pass on demand.</param>
    public ChannelSelect(TimeProvider timeProvider)
    {
        ArgumentNullException.ThrowIfNull(timeProvider);
        _timeProvider = timeProvider;
    }

    /// <summary>Adds a case that reads one item from <paramref name="reader"/> and passes it to <paramref name="handler"/>.</summary>
    /// <typeparam name="T">The type of the channel's items.</typeparam>
    /// <param name="reader">The channel to read from.</param>
    /// <param name="handler">Handles the item; <see cref="RunAsync"/> completes after it does.</param>
    /// <returns>This instance, to add more cases.</returns>
    public ChannelSelect Case<T>(ChannelReader<T> reader, Func<T, Task> handler)
    {
        ArgumentNullException.ThrowIfNull(reader);
        ArgumentNullException.ThrowIfNull(handler);
        _cases.Add(new ReadCase<T>(reader, handler));
        return this;
    }

    /// <summary>Adds a case that reads one item from <paramref name="reader"/> and passes it to <paramref name="handler"/>.</summary>
    /// <typeparam name="T">The type of the channel's items.</typeparam>
    /// <param name="reader">The channel to read from.</param>
    /// <param name="handler">Handles the item.</param>
    /// <returns>This instance, to add more cases.</returns>
    public ChannelSelect Case<T>(ChannelReader<T> reader, Action<T> handler)
    {
        ArgumentNullException.ThrowIfNull(handler);
        return Case(reader, item =>
        {
            handler(item);
            return Task.CompletedTask;
        });
    }

    /// <summary>
    /// Adds a case that writes an item to <paramref name="writer"/> when it has room and then runs
    /// <paramref name="handler"/>, like Go's <c>case ch &lt;- v</c>.
    /// </summary>
    /// <typeparam name="T">The type of the channel's items.</typeparam>
    /// <param name="writer">The channel to write to.</param>
    /// <param name="item">
    /// Returns the item to write. It is called each time the case is tried, before it is known whether the write
    /// succeeds, so keep it cheap and free of side effects; make those in <paramref name="handler"/>.
    /// </param>
    /// <param name="handler">Runs after the item was written; <see cref="RunAsync"/> completes after it does.</param>
    /// <param name="when">
    /// Returns whether the case is enabled, checked each time the case is tried; <see langword="null"/> for always. A
    /// disabled case is skipped, like a send to a <c>nil</c> channel in Go, for instance while there is nothing to send.
    /// </param>
    /// <returns>This instance, to add more cases.</returns>
    /// <remarks>
    /// A channel that has been completed counts as completed, like a reader's channel: the case no longer runs. If it
    /// was completed with an exception, that exception is thrown.
    /// </remarks>
    public ChannelSelect Send<T>(ChannelWriter<T> writer, Func<T> item, Func<Task> handler, Func<bool>? when = null)
    {
        ArgumentNullException.ThrowIfNull(writer);
        ArgumentNullException.ThrowIfNull(item);
        ArgumentNullException.ThrowIfNull(handler);
        _cases.Add(new SendCase<T>(writer, item, handler, when));
        return this;
    }

    /// <summary>
    /// Adds a case that writes an item to <paramref name="writer"/> when it has room and then runs
    /// <paramref name="handler"/>, like Go's <c>case ch &lt;- v</c>.
    /// </summary>
    /// <typeparam name="T">The type of the channel's items.</typeparam>
    /// <param name="writer">The channel to write to.</param>
    /// <param name="item">
    /// Returns the item to write. It is called each time the case is tried, before it is known whether the write
    /// succeeds, so keep it cheap and free of side effects; make those in <paramref name="handler"/>.
    /// </param>
    /// <param name="handler">Runs after the item was written.</param>
    /// <param name="when">
    /// Returns whether the case is enabled, checked each time the case is tried; <see langword="null"/> for always. A
    /// disabled case is skipped, like a send to a <c>nil</c> channel in Go, for instance while there is nothing to send.
    /// </param>
    /// <returns>This instance, to add more cases.</returns>
    public ChannelSelect Send<T>(ChannelWriter<T> writer, Func<T> item, Action handler, Func<bool>? when = null)
    {
        ArgumentNullException.ThrowIfNull(handler);
        return Send(writer, item, () =>
        {
            handler();
            return Task.CompletedTask;
        }, when);
    }

    /// <summary>
    /// Runs <paramref name="handler"/> if no channel has an item within <paramref name="after"/> of
    /// <see cref="RunAsync"/> being called, like Go's <c>case &lt;-time.After(d)</c>.
    /// </summary>
    /// <param name="after">
    /// How long to wait for an item: up to about 49 days, or <see cref="System.Threading.Timeout.InfiniteTimeSpan"/>.
    /// </param>
    /// <param name="handler">Runs when the time is up.</param>
    /// <returns>This instance, to add more cases.</returns>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="after"/> is negative or too long.</exception>
    public ChannelSelect Timeout(TimeSpan after, Func<Task> handler)
    {
        ArgumentNullException.ThrowIfNull(handler);
        Timeouts.Validate(after, nameof(after));
        _timeout = after;
        _onTimeout = handler;
        return this;
    }

    /// <summary>
    /// Runs <paramref name="handler"/> if no channel has an item within <paramref name="after"/> of
    /// <see cref="RunAsync"/> being called, like Go's <c>case &lt;-time.After(d)</c>.
    /// </summary>
    /// <param name="after">How long to wait for an item.</param>
    /// <param name="handler">Runs when the time is up.</param>
    /// <returns>This instance, to add more cases.</returns>
    public ChannelSelect Timeout(TimeSpan after, Action handler)
    {
        ArgumentNullException.ThrowIfNull(handler);
        return Timeout(after, () =>
        {
            handler();
            return Task.CompletedTask;
        });
    }

    /// <summary>
    /// Runs <paramref name="handler"/> immediately if no channel has an item ready, so <see cref="RunAsync"/> never
    /// waits, like Go's <c>default</c> case.
    /// </summary>
    /// <param name="handler">Runs when nothing is ready.</param>
    /// <returns>This instance, to add more cases.</returns>
    public ChannelSelect Default(Func<Task> handler)
    {
        ArgumentNullException.ThrowIfNull(handler);
        _onDefault = handler;
        return this;
    }

    /// <summary>
    /// Runs <paramref name="handler"/> immediately if no channel has an item ready, so <see cref="RunAsync"/> never
    /// waits, like Go's <c>default</c> case.
    /// </summary>
    /// <param name="handler">Runs when nothing is ready.</param>
    /// <returns>This instance, to add more cases.</returns>
    public ChannelSelect Default(Action handler)
    {
        ArgumentNullException.ThrowIfNull(handler);
        return Default(() =>
        {
            handler();
            return Task.CompletedTask;
        });
    }

    /// <summary>Waits for the first case that can run, runs it, and returns.</summary>
    /// <param name="cancellationToken">Stops waiting.</param>
    /// <returns>
    /// <see langword="true"/> when a case ran; <see langword="false"/> when no case can ever run again because every
    /// channel has completed (or its send case is disabled), so the loop can end. The timeout and default cases don't
    /// run then.
    /// </returns>
    /// <exception cref="OperationCanceledException"><paramref name="cancellationToken"/> was cancelled.</exception>
    /// <remarks>If a channel completed with an exception (including an <see cref="OperationCanceledException"/>), that exception is thrown.</remarks>
    public async Task<bool> RunAsync(CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();

        int count = _cases.Count;
        Task<bool>?[]? waits = null;
        CancellationTokenSource? waitCts = null;
        Task? timeout = null;
        try
        {
            while (true)
            {
                // Try every case once, starting at a random one so that a busy channel can't starve the others.
                int start = count > 1 ? Random.Shared.Next(count) : 0;
                bool anyOpen = false;
                for (int i = 0; i < count; i++)
                {
                    SelectCase selectCase = _cases[(start + i) % count];
                    if (selectCase.TryRun(out Task? handled))
                    {
                        await handled.ConfigureAwait(false);
                        return true;
                    }
                    anyOpen |= selectCase.IsOpen;
                }

                // Once no case can make progress, stop here rather than run the default or timeout case on every call,
                // which would turn the caller's loop into a busy or endless one.
                if (!anyOpen && (count > 0 || (_onDefault is null && _onTimeout is null)))
                {
                    return false;
                }
                if (_onDefault is not null)
                {
                    await _onDefault().ConfigureAwait(false);
                    return true;
                }

                // Nothing is ready: wait until a channel might have an item or room, the time is up, or the caller cancels.
                waitCts ??= CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
                waits ??= new Task<bool>?[count];
                if (_onTimeout is not null)
                {
                    timeout ??= Task.Delay(_timeout, _timeProvider, waitCts.Token);
                }

                var pending = new List<Task>(count + 1);
                for (int i = 0; i < count; i++)
                {
                    if (_cases[i].IsOpen)
                    {
                        waits[i] ??= _cases[i].WaitAsync(waitCts.Token);
                        pending.Add(waits[i]!);
                    }
                }
                if (timeout is not null)
                {
                    pending.Add(timeout);
                }

                Task done = await Task.WhenAny(pending).ConfigureAwait(false);
                cancellationToken.ThrowIfCancellationRequested();

                if (done == timeout)
                {
                    await _onTimeout!().ConfigureAwait(false);
                    return true;
                }

                // A channel has an item or room, or has completed (or completed with an error, which TryRun rethrows).
                // Another reader or writer may get there first, so go round again.
                for (int i = 0; i < count; i++)
                {
                    if (waits[i] is { IsCompleted: true } completed)
                    {
                        // A wait the caller's token cancelled says nothing about its channel.
                        if (!completed.IsCanceled || !waitCts.IsCancellationRequested)
                        {
                            _cases[i].Observe(completed);
                        }
                        waits[i] = null;
                    }
                }
            }
        }
        finally
        {
            // Stop the waits still pending, so nothing keeps waiting on the channels after this call.
            if (waitCts is not null)
            {
                waitCts.Cancel();
                waitCts.Dispose();
            }
        }
    }

    private abstract class SelectCase
    {
        // Whether the case could run later: its channel hasn't completed, and a send case is enabled. Valid after TryRun.
        public abstract bool IsOpen { get; }

        // Runs the case if it can proceed now; throws if the channel completed with an error.
        public abstract bool TryRun([NotNullWhen(true)] out Task? handled);

        public abstract Task<bool> WaitAsync(CancellationToken cancellationToken);

        // Takes note of how a wait ended; the error itself surfaces through TryRun, so it mustn't stay unobserved.
        public virtual void Observe(Task<bool> wait) => _ = wait.Exception;
    }

    private sealed class ReadCase<T>(ChannelReader<T> reader, Func<T, Task> handler) : SelectCase
    {
        public override bool IsOpen => !reader.Completion.IsCompleted;

        public override bool TryRun([NotNullWhen(true)] out Task? handled)
        {
            if (reader.TryRead(out T? item))
            {
                handled = handler(item) ?? Task.CompletedTask;
                return true;
            }

            // A channel completed with an OperationCanceledException ends up cancelled rather than faulted; rethrow both.
            if (reader.Completion.IsCompleted && !reader.Completion.IsCompletedSuccessfully)
            {
                reader.Completion.GetAwaiter().GetResult();
            }
            handled = null;
            return false;
        }

        public override Task<bool> WaitAsync(CancellationToken cancellationToken) =>
            reader.WaitToReadAsync(cancellationToken).AsTask();
    }

    // A writer has no Completion task; it says it has completed only through WaitToWriteAsync, which is remembered.
    private sealed class SendCase<T>(ChannelWriter<T> writer, Func<T> item, Func<Task> handler, Func<bool>? when) : SelectCase
    {
        private bool _completed;
        private bool _enabled = true;
        private ExceptionDispatchInfo? _error;

        public override bool IsOpen => !_completed && _enabled;

        public override bool TryRun([NotNullWhen(true)] out Task? handled)
        {
            _error?.Throw();
            _enabled = when?.Invoke() ?? true;
            handled = null;
            if (_completed || !_enabled)
            {
                return false;
            }
            if (writer.TryWrite(item()))
            {
                handled = handler() ?? Task.CompletedTask;
                return true;
            }

            // The write failed because the channel is full or has completed. Find out which without waiting, or a
            // select with a default case, which never waits, would never learn that the channel has completed.
            using (var probe = new CancellationTokenSource())
            {
                ValueTask<bool> wait = writer.WaitToWriteAsync(probe.Token);
                if (!wait.IsCompleted)
                {
                    probe.Cancel();   // full: take the waiter back off the channel
                }
                Task<bool> waited = wait.AsTask();
                if (!waited.IsCanceled)
                {
                    Observe(waited);
                }
            }
            _error?.Throw();
            return false;
        }

        public override Task<bool> WaitAsync(CancellationToken cancellationToken) =>
            writer.WaitToWriteAsync(cancellationToken).AsTask();

        public override void Observe(Task<bool> wait)
        {
            try
            {
                _completed = !wait.GetAwaiter().GetResult();
            }
            catch (Exception exception)
            {
                // Including an OperationCanceledException the channel was completed with.
                _error = ExceptionDispatchInfo.Capture(exception);
            }
        }
    }
}
