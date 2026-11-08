using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp.Testing;
using Microsoft.CodeAnalysis.Testing;
using StructuredConcurrency.Analyzers;

namespace StructuredConcurrency.Analyzers.Tests;

// The compiler's CS4014 is expected in each test; the suppressor must mark it suppressed on group.Run and leave it
// alone everywhere else.
public class UnawaitedRunSuppressorTests
{
    private static Task VerifyAsync(string body, params DiagnosticResult[] expected)
    {
        var test = new CSharpCodeFixTest<UnawaitedRunSuppressor, EmptyCodeFixProvider, DefaultVerifier>
        {
            TestCode = """
                using System;
                using System.Threading;
                using System.Threading.Tasks;
                using StructuredConcurrency;

                class C
                {
                    static Task Save(CancellationToken ct) => Task.CompletedTask;
                    static Task<int> Get(CancellationToken ct) => Task.FromResult(1);

                    async Task M(CancellationToken cancellationToken)
                    {
                """ + "\n" + body + """

                    }
                }
                """,
            ReferenceAssemblies = ReferenceAssemblies.Net.Net80,
            CompilerDiagnostics = CompilerDiagnostics.Warnings,
        };
        test.TestState.AdditionalReferences.Add(MetadataReference.CreateFromFile(typeof(TaskGroup).Assembly.Location));
        test.ExpectedDiagnostics.AddRange(expected);
        return test.RunAsync();
    }

    private static DiagnosticResult Cs4014(int markupKey) => DiagnosticResult.CompilerWarning("CS4014").WithLocation(markupKey);

    [Fact]
    public Task Run_InAnAsyncBody_IsSuppressed() => VerifyAsync(
        """
                await TaskGroup.RunAsync(async group =>
                {
                    await Task.Yield();
                    {|#0:group.Run(ct => Save(ct))|};
                    {|#1:group.Run(ct => Get(ct))|};
                }, cancellationToken);
        """,
        Cs4014(0).WithIsSuppressed(true),
        Cs4014(1).WithIsSuppressed(true));

    [Fact]
    public Task OtherUnawaitedCalls_AreStillReported() => VerifyAsync(
        """
                {|#0:Save(cancellationToken)|};
                await TaskGroup.RunAsync(async group =>
                {
                    await Task.Yield();
                    {|#1:Task.Delay(1, cancellationToken)|};
                }, cancellationToken);
        """,
        Cs4014(0).WithIsSuppressed(false),
        Cs4014(1).WithIsSuppressed(false));

    [Fact]
    public Task AnotherTypesRunMethod_IsStillReported() => VerifyAsync(
        """
                await Task.Yield();
                {|#0:Task.Run(() => Save(cancellationToken))|};
        """,
        Cs4014(0).WithIsSuppressed(false));
}
