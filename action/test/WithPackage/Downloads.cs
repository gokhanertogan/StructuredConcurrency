using System.Threading;
using System.Threading.Tasks;
using StructuredConcurrency;

public static class Downloads
{
    static Task DownloadAsync(string url, CancellationToken cancellationToken) => Task.Delay(10, cancellationToken);
    static Task DownloadAsync(string url) => Task.Delay(10);

    public static Task RunAsync(string[] urls, CancellationToken cancellationToken) =>
        TaskGroup.RunAsync(group =>
        {
            foreach (string url in urls)
            {
                group.Run(ct => DownloadAsync(url));   // SC0001: ct is never used
            }
            return Task.CompletedTask;
        }, cancellationToken);
}
