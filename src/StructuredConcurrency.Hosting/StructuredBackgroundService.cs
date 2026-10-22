using Microsoft.Extensions.Hosting;

namespace StructuredConcurrency.Hosting;

/// <summary>
/// A <see cref="BackgroundService"/> whose work runs in a <see cref="TaskGroup"/>. Start the service's concurrent work
/// as children of the group: when the host stops, every child is cancelled and the service stops only after all of
/// them have ended, and when a child fails, the others are cancelled and the service stops with that exception.
/// </summary>
/// <remarks>
/// With a plain <see cref="BackgroundService"/>, work started with <c>Task.Run</c> or left un-awaited keeps running
/// after <c>StopAsync</c> returns, and its exceptions are lost. Here nothing outlives the service. A failure ends
/// <see cref="BackgroundService.ExecuteTask"/> with the child's exception; with the host's default
/// <c>BackgroundServiceExceptionBehavior.StopHost</c>, that is logged and stops the application.
/// </remarks>
public abstract class StructuredBackgroundService : BackgroundService
{
    /// <summary>
    /// Gets the options for the service's group, such as <see cref="TaskGroupOptions.MaxConcurrency"/> or
    /// <see cref="TaskGroupOptions.CancelOnFailure"/>. By default the first failure cancels the other children.
    /// </summary>
    /// <remarks>
    /// The service's group doesn't record a tracing activity, whatever <see cref="TaskGroupOptions.EnableTracing"/>
    /// says: it lives as long as the host, so its activity would gather everything the service ever traces into one
    /// trace that only ends at shutdown. Groups and activities the children start are traced as usual.
    /// </remarks>
    protected virtual TaskGroupOptions Options => TaskGroupOptions.Default;

    /// <summary>
    /// Starts the service's work in <paramref name="group"/>. Use the group's
    /// <see cref="TaskGroup.CancellationToken"/>, which is cancelled when the host stops or a child fails.
    /// </summary>
    /// <param name="group">The group to start children in.</param>
    /// <returns>A task that completes when the body has finished; the children may keep running after it.</returns>
    protected abstract Task ExecuteAsync(TaskGroup group);

    /// <inheritdoc/>
    protected sealed override Task ExecuteAsync(CancellationToken stoppingToken)
    {
        TaskGroupOptions options = Options ?? TaskGroupOptions.Default;
        return TaskGroup.RunAsync(ExecuteAsync, new TaskGroupOptions
        {
            Name = options.Name,
            CancelOnFailure = options.CancelOnFailure,
            MaxConcurrency = options.MaxConcurrency,
            EnableTracing = false,
        }, stoppingToken);
    }
}
