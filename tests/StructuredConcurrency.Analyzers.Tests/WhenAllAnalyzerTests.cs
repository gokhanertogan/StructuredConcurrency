using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp.Testing;
using Microsoft.CodeAnalysis.Testing;
using StructuredConcurrency.Analyzers;
using StructuredConcurrency.CodeFixes;

namespace StructuredConcurrency.Analyzers.Tests;

// Each test compiles a small program against StructuredConcurrency. {|SC0004:x|} marks where a diagnostic is
// expected; code without markup must produce none.
public class WhenAllAnalyzerTests
{
    private const string Usings = """
        using System;
        using System.Threading;
        using System.Threading.Tasks;
        using StructuredConcurrency;

        """;

    private const string UsingsWithoutPackage = """
        using System;
        using System.Threading;
        using System.Threading.Tasks;

        """;

    private static Task VerifyAsync(string body, string? fixedBody = null, string usings = Usings, string fixedUsings = Usings)
    {
        string Wrap(string code) => """
            class C
            {
                static Task Save(int id, CancellationToken ct) => Task.CompletedTask;
                static Task<int> Get(int id, CancellationToken ct) => Task.FromResult(id);
                static Task<string> Name(CancellationToken ct) => Task.FromResult("");
                static Task Plain() => Task.CompletedTask;

                async Task M(CancellationToken cancellationToken)
                {
            """ + "\n" + code + """

                }
            }
            """;

        var test = new CSharpCodeFixTest<WhenAllAnalyzer, UseTaskGroupCodeFix, DefaultVerifier>
        {
            TestCode = usings + Wrap(body),
            FixedCode = fixedUsings + Wrap(fixedBody ?? body),
            ReferenceAssemblies = ReferenceAssemblies.Net.Net80,
        };
        test.TestState.AdditionalReferences.Add(MetadataReference.CreateFromFile(typeof(TaskGroup).Assembly.Location));
        test.FixedState.AdditionalReferences.Add(MetadataReference.CreateFromFile(typeof(TaskGroup).Assembly.Location));

        // Calls without a fix keep their markup in the fixed code: they are still reported there.
        test.FixedState.MarkupHandling = MarkupMode.Allow;
        return test.RunAsync();
    }

    [Fact]
    public Task WhenAll_OverCallsWithTheSameToken_IsReported_AndFixed() => VerifyAsync(
        """
                await {|SC0004:Task.WhenAll(Save(1, cancellationToken), Get(2, cancellationToken))|};
        """,
        """
                await TaskGroup.RunAsync(group =>
                {
                    group.Run(ct => Save(1, ct));
                    group.Run(ct => Get(2, ct));
                    return Task.CompletedTask;
                }, cancellationToken);
        """);

    [Fact]
    public Task DiscardedResults_AndConfigureAwait_AreFixed() => VerifyAsync(
        """
                using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
                await {|SC0004:Task.WhenAll(Get(1, cts.Token), Get(2, cts.Token))|}.ConfigureAwait(false);
        """,
        """
                using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
                await TaskGroup.RunAsync(group =>
                {
                    group.Run(ct => Get(1, ct));
                    group.Run(ct => Get(2, ct));
                    return Task.CompletedTask;
                }, cts.Token).ConfigureAwait(false);
        """);

    [Fact]
    public Task TheFix_AddsTheUsing_AndAvoidsTakenNames() => VerifyAsync(
        """
                int group = 0, ct = 0;
                await {|SC0004:Task.WhenAll(Save(group, cancellationToken), Save(ct, cancellationToken))|};
        """,
        """
                int group = 0, ct = 0;
                await TaskGroup.RunAsync(group2 =>
                {
                    group2.Run(ct2 => Save(group, ct2));
                    group2.Run(ct2 => Save(ct, ct2));
                    return Task.CompletedTask;
                }, cancellationToken);
        """,
        usings: UsingsWithoutPackage,
        fixedUsings: UsingsWithoutPackage.TrimEnd() + "\nusing StructuredConcurrency;\n\n");

    [Fact]
    public Task UsedResults_TasksInLocals_AndMixedTokens_AreReported_WithoutAFix() => VerifyAsync("""
                int[] ids = await {|SC0004:Task.WhenAll(Get(1, cancellationToken), Get(2, cancellationToken))|};
                Task<int> first = Get(1, cancellationToken);
                Task<string> name = Name(cancellationToken);
                await {|SC0004:Task.WhenAll(first, name)|};
                using var cts = new CancellationTokenSource();
                await {|SC0004:Task.WhenAll(Save(1, cancellationToken), Save(2, cts.Token))|};
        """);

    [Fact]
    public Task WhenAll_WithoutCancellableWork_IsNotReported() => VerifyAsync("""
                await Task.WhenAll(Plain(), Plain());
                await Task.WhenAll(Save(1, cancellationToken), Plain());
                await Task.WhenAll(Save(1, CancellationToken.None), Save(2, default));
                await Task.WhenAll(new[] { Save(1, cancellationToken), Save(2, cancellationToken) });
                await Task.WhenAll(Save(1, cancellationToken));
        """);
}
