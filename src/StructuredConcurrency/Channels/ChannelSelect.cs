using System.Threading.Channels;

namespace StructuredConcurrency.Channels;

/// <summary>
/// Waits on several channels at once and handles the first item that arrives, like Go's <c>select</c> statement.
/// </summary>
/// <remarks>
/// Build the cases once and call <see cref="RunAsync"/> as often as needed, typically in a loop:
/// <c>while (await select.RunAsync(ct)) { }</c>. When several channels have an item ready, one is picked at random,
/// so no channel is starved. Nothing keeps waiting in the background after <see cref="RunAsync"/> returns.
/// </remarks>
public sealed class ChannelSelect
{
    private readonly List<SelectCase> _cases = [];
    private TimeSpan _timeout;
    private Func<Task>? _onTimeout;
    private Func<Task>? _onDefault;

    /// <summary>Adds a case that reads one item from <paramref name="reader"/> and passes it to <paramref name="handler"/>.</summary>
    /// <typeparam name="T">The type of the channel's items.</typeparam>
    /// <param name="reader">The channel to read from.</param>
    /// <param name="handler">Handles the item; <see cref="RunAsync"/> completes after it does.</param>
    /// <returns>This instance, to add more cases.</returns>
    public ChannelSelect Case<T>(ChannelReader<T> reader, Func<T, Task> handler)
    {
        ArgumentNullException.ThrowIfNull(reader);
        ArgumentNullException.ThrowIfNull(handler);
        _cases.Add(new SelectCase<T>(reader, handler));
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
    /// Runs <paramref name="handler"/> if no channel has an item within <paramref name="after"/> of
    /// <see cref="RunAsync"/> being called, like Go's <c>case &lt;-time.After(d)</c>.
    /// </summary>
    /// <param name="after">How long to wait for an item.</param>
    /// <param name="handler">Runs when the time is up.</param>
    /// <returns>This instance, to add more cases.</returns>
    public ChannelSelect Timeout(TimeSpan after, Func<Task> handler)
    {
        ArgumentNullException.ThrowIfNull(handler);
        ArgumentOutOfRangeException.ThrowIfLessThan(after, TimeSpan.Zero);
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
    /// <see langword="true"/> when a case ran; <see langword="false"/> when every channel has completed and there is
    /// no timeout or default case, so no case can ever run again.
    /// </returns>
    /// <exception cref="OperationCanceledException"><paramref name="cancellationToken"/> was cancelled.</exception>
    /// <remarks>If a channel completed with an exception, that exception is thrown.</remarks>
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
                // Try every channel once, starting at a random one so that a busy channel can't starve the others.
                int start = count > 1 ? Random.Shared.Next(count) : 0;
                bool anyOpen = false;
                for (int i = 0; i < count; i++)
                {
                    SelectCase selectCase = _cases[(start + i) % count];
                    if (selectCase.TryRead(out Task? handled))
                    {
                        await handled.ConfigureAwait(false);
                        return true;
                    }
                    anyOpen |= !selectCase.IsCompleted;
                }

                if (_onDefault is not null)
                {
                    await _onDefault().ConfigureAwait(false);
                    return true;
                }
                if (!anyOpen && _onTimeout is null)
                {
                    return false;
                }

                // Nothing is ready: wait until a channel might have an item, the time is up, or the caller cancels.
                waitCts ??= CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
                waits ??= new Task<bool>?[count];
                if (_onTimeout is not null)
                {
                    timeout ??= Task.Delay(_timeout, waitCts.Token);
                }

                var pending = new List<Task>(count + 1);
                for (int i = 0; i < count; i++)
                {
                    if (!_cases[i].IsCompleted)
                    {
                        waits[i] ??= _cases[i].WaitToReadAsync(waitCts.Token);
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

                // A channel has an item or has completed (or completed with an error, which TryRead rethrows).
                // Another reader may take the item first, so go round again.
                for (int i = 0; i < count; i++)
                {
                    if (waits[i] is { IsCompleted: true } completed)
                    {
                        _ = completed.Exception;  // the error itself surfaces through TryRead; don't leave it unobserved
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
        public abstract bool IsCompleted { get; }

        // Reads an item if one is ready and starts its handler; throws if the channel completed with an error.
        public abstract bool TryRead([System.Diagnostics.CodeAnalysis.NotNullWhen(true)] out Task? handled);

        public abstract Task<bool> WaitToReadAsync(CancellationToken cancellationToken);
    }

    private sealed class SelectCase<T>(ChannelReader<T> reader, Func<T, Task> handler) : SelectCase
    {
        public override bool IsCompleted => reader.Completion.IsCompleted;

        public override bool TryRead([System.Diagnostics.CodeAnalysis.NotNullWhen(true)] out Task? handled)
        {
            if (reader.TryRead(out T? item))
            {
                handled = handler(item) ?? Task.CompletedTask;
                return true;
            }

            if (reader.Completion.IsFaulted)
            {
                reader.Completion.GetAwaiter().GetResult();
            }
            handled = null;
            return false;
        }

        public override Task<bool> WaitToReadAsync(CancellationToken cancellationToken) =>
            reader.WaitToReadAsync(cancellationToken).AsTask();
    }
}
