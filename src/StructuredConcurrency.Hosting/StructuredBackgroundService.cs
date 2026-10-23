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
    /// Gets how to restart the service's work when it fails, or <see langword="null"/> (the default) to let the first
    /// failure stop the service.
    /// </summary>
    /// <remarks>
    /// When a child fails, its siblings are cancelled as usual; then, as the policy allows, a new group is started and
    /// <see cref="ExecuteAsync(TaskGroup)"/> runs again, like Erlang's <c>one_for_all</c> supervisor. Once the policy
    /// gives up, the service stops with the failure. To restart one child on its own instead, wrap it in
    /// <see cref="Supervisor.RunAsync(Func{CancellationToken, Task}, StructuredConcurrency.RestartPolicy, CancellationToken)"/>.
    /// </remarks>
    protected virtual RestartPolicy? RestartPolicy => null;

    /// <summary>
    /// Starts the service's work in <paramref name="group"/>. Use the group's
    /// <see cref="TaskGroup.CancellationToken"/>, which is cancelled when the host stops or a child fails.
    /// </summary>
    /// <param name="group">The group to start children in.</param>
    /// <returns>A task that completes when the body has finished; the children may keep running after it.</returns>
    /// <remarks>With a <see cref="RestartPolicy"/>, this runs again, in a new group, after each restart.</remarks>
    protected abstract Task ExecuteAsync(TaskGroup group);

    /// <inheritdoc/>
    protected sealed override Task ExecuteAsync(CancellationToken stoppingToken)
    {
        TaskGroupOptions options = Options ?? TaskGroupOptions.Default;
        var groupOptions = new TaskGroupOptions
        {
            Name = options.Name,
            CancelOnFailure = options.CancelOnFailure,
            MaxConcurrency = options.MaxConcurrency,
            RateLimiter = options.RateLimiter,
            EnableTracing = false,
        };
        return RestartPolicy is RestartPolicy policy
            ? Supervisor.RunGroupAsync(ExecuteAsync, policy, groupOptions, stoppingToken)
            : TaskGroup.RunAsync(ExecuteAsync, groupOptions, stoppingToken);
    }
}
