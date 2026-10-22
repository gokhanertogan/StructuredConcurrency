using System.Runtime.CompilerServices;
using System.Threading.Channels;

namespace StructuredConcurrency.Channels;

/// <summary>Fan-in and fan-out for <see cref="ChannelReader{T}"/>, as commonly built from goroutines and channels in Go.</summary>
public static class ChannelReaderExtensions
{
    /// <summary>
    /// Reads from this channel and <paramref name="others"/> at the same time and yields every item as it arrives,
    /// until all of them have completed (fan-in).
    /// </summary>
    /// <typeparam name="T">The type of the items.</typeparam>
    /// <param name="first">The first channel.</param>
    /// <param name="others">The other channels.</param>
    /// <returns>
    /// The items from every channel. No background task is involved: channels are read only while the sequence is
    /// enumerated. Pass a cancellation token with <c>WithCancellation</c>. If a channel completes with an exception,
    /// enumeration throws it.
    /// </returns>
    public static IAsyncEnumerable<T> MergeWith<T>(this ChannelReader<T> first, params ChannelReader<T>[] others)
    {
        ArgumentNullException.ThrowIfNull(first);
        ArgumentNullException.ThrowIfNull(others);
        var readers = new ChannelReader<T>[others.Length + 1];
        readers[0] = first;
        for (int i = 0; i < others.Length; i++)
        {
            readers[i + 1] = others[i] ?? throw new ArgumentException("A channel is null.", nameof(others));
        }
        return MergeCore(readers);
    }

    private static async IAsyncEnumerable<T> MergeCore<T>(
        ChannelReader<T>[] readers,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        var waits = new Task<bool>?[readers.Length];
        using var waitCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        try
        {
            while (true)
            {
                cancellationToken.ThrowIfCancellationRequested();

                // Try every channel once, starting at a random one so that a busy channel can't starve the others.
                int start = Random.Shared.Next(readers.Length);
                bool anyOpen = false;
                bool found = false;
                T item = default!;
                for (int i = 0; i < readers.Length; i++)
                {
                    ChannelReader<T> reader = readers[(start + i) % readers.Length];
                    if (reader.TryRead(out item!))
                    {
                        found = true;
                        break;
                    }
                    if (reader.Completion.IsFaulted)
                    {
                        reader.Completion.GetAwaiter().GetResult();
                    }
                    anyOpen |= !reader.Completion.IsCompleted;
                }

                if (found)
                {
                    yield return item;
                    continue;
                }
                if (!anyOpen)
                {
                    yield break;
                }

                var pending = new List<Task>(readers.Length);
                for (int i = 0; i < readers.Length; i++)
                {
                    if (!readers[i].Completion.IsCompleted)
                    {
                        waits[i] ??= readers[i].WaitToReadAsync(waitCts.Token).AsTask();
                        pending.Add(waits[i]!);
                    }
                }

                await Task.WhenAny(pending).ConfigureAwait(false);
                cancellationToken.ThrowIfCancellationRequested();

                for (int i = 0; i < waits.Length; i++)
                {
                    if (waits[i] is { IsCompleted: true } completed)
                    {
                        _ = completed.Exception;  // the error itself surfaces through Completion; don't leave it unobserved
                        waits[i] = null;
                    }
                }
            }
        }
        finally
        {
            // Stop the waits still pending, so nothing keeps waiting on the channels after enumeration ends.
            waitCts.Cancel();
        }
    }

    /// <summary>
    /// Copies every item from this channel to all <paramref name="targets"/> (fan-out), then completes the targets.
    /// </summary>
    /// <typeparam name="T">The type of the items.</typeparam>
    /// <param name="source">The channel to read from.</param>
    /// <param name="targets">The channels that each receive every item.</param>
    /// <param name="cancellationToken">Stops copying.</param>
    /// <returns>
    /// A task that completes when the source has completed and every item has been written to every target. Writes
    /// wait for space, so the slowest target sets the pace. If the source fails or the copy is cancelled, the targets
    /// are completed with that exception and the task rethrows it. Run it as a child of a <see cref="TaskGroup"/>.
    /// </returns>
    public static async Task BroadcastAsync<T>(
        this ChannelReader<T> source,
        IReadOnlyList<ChannelWriter<T>> targets,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(source);
        ArgumentNullException.ThrowIfNull(targets);

        try
        {
            await foreach (T item in source.ReadAllAsync(cancellationToken).ConfigureAwait(false))
            {
                foreach (ChannelWriter<T> target in targets)
                {
                    await target.WriteAsync(item, cancellationToken).ConfigureAwait(false);
                }
            }
        }
        catch (Exception exception)
        {
            foreach (ChannelWriter<T> target in targets)
            {
                target.TryComplete(exception);
            }
            throw;
        }

        foreach (ChannelWriter<T> target in targets)
        {
            target.TryComplete();
        }
    }
}
