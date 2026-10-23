using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp.Testing;
using Microsoft.CodeAnalysis.Testing;
using StructuredConcurrency.Analyzers;
using StructuredConcurrency.CodeFixes;

namespace StructuredConcurrency.Analyzers.Tests;

// Each test compiles a small program against StructuredConcurrency. {|SC0003:x|} marks where a diagnostic is
// expected; code without markup must produce none.
public class EscapingWorkAnalyzerTests
{
    private const string Usings = """
        using System;
        using System.Threading;
        using System.Threading.Tasks;
        using StructuredConcurrency;

        """;

    private static Task VerifyAsync(string body, string? fixedBody = null)
    {
        string Wrap(string code) => Usings + """
            class C
            {
                static Task Work() => Task.CompletedTask;
                static Task<int> Get() => Task.FromResult(1);
                static void Compute() { }
                static Task InBody() => Task.CompletedTask;
                event EventHandler Changed;

                async Task M(CancellationToken cancellationToken)
                {
            """ + code + """

                }

                Task Body(TaskGroup group)
                {
                    {|SC0003:Task.Run(() => InBody())|};   // a method given a TaskGroup counts as a group's code
                    return Task.CompletedTask;
                }
            }
            """;

        string WrapFixed(string code) => Wrap(code).Replace("{|SC0003:Task.Run(() => InBody())|};", "group.Run(ct => InBody());");

        var test = new CSharpCodeFixTest<EscapingWorkAnalyzer, StartInGroupCodeFix, DefaultVerifier>
        {
            TestCode = Wrap(body),
            FixedCode = WrapFixed(fixedBody ?? body),
            ReferenceAssemblies = ReferenceAssemblies.Net.Net80,
        };
        test.TestState.AdditionalReferences.Add(MetadataReference.CreateFromFile(typeof(TaskGroup).Assembly.Location));
        test.FixedState.AdditionalReferences.Add(MetadataReference.CreateFromFile(typeof(TaskGroup).Assembly.Location));

        // Calls without a fix keep their markup in the fixed code: they are still reported there.
        test.FixedState.MarkupHandling = MarkupMode.Allow;
        return test.RunAsync();
    }

    [Fact]
    public Task DiscardedWork_InTheBody_IsReported_AndFixed() => VerifyAsync(
        """
        await TaskGroup.RunAsync(async group =>
        {
            {|SC0003:Task.Run(async () => await Work())|};
            _ = {|SC0003:Task.Run(() => Get())|};
            await Task.Yield();
        });
        """,
        """
        await TaskGroup.RunAsync(async group =>
        {
            group.Run(async ct => await Work());
            _ = group.Run(ct => Get());
            await Task.Yield();
        });
        """);

    [Fact]
    public Task DiscardedWork_InAChild_IsFixedWithTheGroupAround() => VerifyAsync(
        """
        await TaskGroup.RunAsync(group =>
        {
            group.Run(async ct =>
            {
                {|SC0003:Task.Run(() => Work())|};
                await Work();
            });
            return Task.CompletedTask;
        });
        """,
        """
        await TaskGroup.RunAsync(group =>
        {
            group.Run(async ct =>
            {
                group.Run(ct2 => Work());
                await Work();
            });
            return Task.CompletedTask;
        });
        """);

    [Fact]
    public Task OtherStarters_AreReported_WithoutAFix() => VerifyAsync("""
        await TaskGroup.RunAsync(group =>
        {
            {|SC0003:Task.Run(() => Compute())|};
            {|SC0003:Task.Run(Work)|};
            {|SC0003:Task.Run(() => Work(), cancellationToken)|};
            {|SC0003:Task.Factory.StartNew(() => Compute())|};
            {|SC0003:ThreadPool.QueueUserWorkItem(_ => Compute())|};
            return Task.CompletedTask;
        });
        """);

    [Fact]
    public Task WorkWithoutAGroupInScope_IsReported_WithoutAFix() => VerifyAsync("""
        await TaskGroup.WithTimeoutAsync(TimeSpan.FromSeconds(1), ct =>
        {
            {|SC0003:Task.Run(() => Work())|};
            return Work();
        });
        """);

    [Fact]
    public Task AwaitedReturnedOrKeptWork_IsNotReported() => VerifyAsync("""
        await TaskGroup.RunAsync(async group =>
        {
            await Task.Run(() => Compute());
            group.Run(ct => Task.Run(() => Work(), ct));
            Task kept = Task.Run(() => Work());
            await kept;
        });
        """);

    [Fact]
    public Task CodeThatRunsLater_OrOutsideAGroup_IsNotReported() => VerifyAsync("""
        Task.Run(() => Work());
        await TaskGroup.RunAsync(group =>
        {
            Changed += (s, e) => Task.Run(() => Work());
            Action later = () => Task.Run(() => Work());
            return Task.CompletedTask;
        });
        """);
}
