using System.Threading.Channels;

namespace StructuredConcurrency.Channels;

/// <summary>Channel producers that run as children of a <see cref="TaskGroup"/>.</summary>
public static class TaskGroupChannelExtensions
{
    /// <summary>
    /// Starts <paramref name="producer"/> as a child of the group, writing to a new channel, and returns the channel's
    /// reader, like Kotlin's <c>produce</c>.
    /// </summary>
    /// <typeparam name="T">The type of the items.</typeparam>
    /// <param name="group">The group the producer runs in.</param>
    /// <param name="producer">Writes the items. It doesn't need to complete the writer.</param>
    /// <param name="capacity">
    /// How many items the channel holds before the producer's writes wait for the reader; at least 1.
    /// </param>
    /// <returns>
    /// The reader. The channel completes when the producer does: normally when it succeeds, with its exception when it
    /// fails, and with an <see cref="OperationCanceledException"/> when it is cancelled or the group was cancelled
    /// before it started. A failing producer also fails the group, like any other child.
    /// </returns>
    /// <remarks>
    /// The producer is a child like any other, so the group waits for it. If you stop reading before the channel
    /// completes, call <see cref="TaskGroup.Cancel"/> (or cancel the producer's token another way); otherwise a producer
    /// waiting for space in the channel keeps the group from completing.
    /// </remarks>
    /// <exception cref="InvalidOperationException">The group has already completed.</exception>
    public static ChannelReader<T> Produce<T>(
        this TaskGroup group,
        Func<ChannelWriter<T>, CancellationToken, Task> producer,
        int capacity = 1)
    {
        ArgumentNullException.ThrowIfNull(group);
        ArgumentNullException.ThrowIfNull(producer);
        ArgumentOutOfRangeException.ThrowIfLessThan(capacity, 1);

        var channel = Channel.CreateBounded<T>(new BoundedChannelOptions(capacity) { SingleWriter = false });
        Task child = group.Run(ct => producer(channel.Writer, ct));
        child.ContinueWith(
            static (child, state) =>
            {
                var writer = (ChannelWriter<T>)state!;
                if (child.IsCompletedSuccessfully)
                {
                    writer.TryComplete();
                }
                else if (child.IsCanceled)
                {
                    writer.TryComplete(new OperationCanceledException("The producer was cancelled."));
                }
                else
                {
                    writer.TryComplete(child.Exception!.InnerException);
                }
            },
            channel.Writer,
            CancellationToken.None,
            TaskContinuationOptions.ExecuteSynchronously,
            TaskScheduler.Default);

        return channel.Reader;
    }
}
