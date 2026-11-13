using System.Threading;
using System.Threading.Tasks;

public static class Greeter
{
    public static Task<string> GreetAsync(string name, CancellationToken cancellationToken) =>
        Task.FromResult($"Hello, {name}");
}
