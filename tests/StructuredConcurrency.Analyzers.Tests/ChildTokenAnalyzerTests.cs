using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp.Testing;
using Microsoft.CodeAnalysis.Testing;
using StructuredConcurrency.Analyzers;
using StructuredConcurrency.CodeFixes;

namespace StructuredConcurrency.Analyzers.Tests;

// Each test compiles a small program against StructuredConcurrency. {|SC0001:x|} and {|SC0002:x|} mark where a
// diagnostic is expected; code without markup must produce none.
public class ChildTokenAnalyzerTests
{
    private const string Usings = """
        using System;
        using System.Collections.Generic;
        using System.Threading;
        using System.Threading.Channels;
        using System.Threading.Tasks;
        using StructuredConcurrency;
        using StructuredConcurrency.Channels;

        """;

    private static Task VerifyAsync(string body, string? fixedBody = null)
    {
        string Wrap(string code) => Usings + """
            class State { public CancellationToken Token; }

            class C
            {
                CancellationToken _field;
                CancellationToken Tok => _field;
                static Task Work() => Task.CompletedTask;
                static Task Work(CancellationToken ct) => Task.CompletedTask;
                static Task<int> Get(int item, CancellationToken ct) => Task.FromResult(item);

                async Task M(CancellationToken cancellationToken, TaskCompletionSource gate)
                {
            """ + code + """

                }
            }
            """;

        var test = new CSharpCodeFixTest<ChildTokenAnalyzer, UseGivenTokenCodeFix, DefaultVerifier>
        {
            TestCode = Wrap(body),
            FixedCode = Wrap(fixedBody ?? body),
            ReferenceAssemblies = ReferenceAssemblies.Net.Net80,
        };
        test.TestState.AdditionalReferences.Add(MetadataReference.CreateFromFile(typeof(TaskGroup).Assembly.Location));
        return test.RunAsync();
    }

    // ----------------------------------------------------------------------------------------------------------------
    // SC0001: the token is never used

    [Fact]
    public Task IgnoredToken_IsReported() => VerifyAsync("""
        await TaskGroup.RunAsync(group =>
        {
            group.Run({|SC0001:ct|} => Work());
            group.Run(async {|SC0001:ct|} => { await Task.Delay(100); });
            group.Run(async {|SC0001:ct|} => await gate.Task);   // waits without a way to stop waiting
            group.Run({|SC0001:_unused|} => Work());              // only '_' says it is intended
            group.Run({|SC0001:delegate|} { return Work(); });    // a parameterless anonymous method
            group.Run({|SC0001:ct|} => { var s = new State(); return Work(s.Token); });   // a token from inside the work
            return Task.CompletedTask;
        });
        """);

    [Fact]
    public Task UsedToken_IsNotReported() => VerifyAsync("""
        await TaskGroup.RunAsync(group =>
        {
            group.Run(ct => Work(ct));
            group.Run(async ct => { await Task.Delay(100, ct); });
            group.Run(ct => Task.Run(() => Work(ct)));   // used in a nested lambda
            return Task.CompletedTask;
        });
        """);

    [Fact]
    public Task Discard_And_WorkThatCantBeCancelled_AreNotReported() => VerifyAsync("""
        await TaskGroup.RunAsync(group =>
        {
            group.Run(_ => Work());
            group.Run(ct => gate.Task);
            group.Run(ct => Task.CompletedTask);
            group.Run(ct => Task.FromResult(Environment.ProcessorCount));
            group.Run(ct => { Console.WriteLine("synchronous"); return Task.CompletedTask; });
            return Task.CompletedTask;
        });
        """);

    [Fact]
    public Task EveryApiThatPassesAToken_IsChecked() => VerifyAsync("""
        int[] items = { 1, 2 };
        await TaskGroup.AllAsync({|SC0001:a|} => Get(1, default), {|SC0001:b|} => Get(2, default));
        await TaskGroup.AnyAsync(items, (item, {|SC0001:ct|}) => Get(item, default));
        await TaskGroup.AllSettledAsync(items, (item, {|SC0001:ct|}) => Get(item, default), maxConcurrency: 1);
        await TaskGroup.WithTimeoutAsync(TimeSpan.FromSeconds(1), {|SC0001:ct|} => Work());
        await new SingleFlight<string, int>().RunAsync("k", {|SC0001:ct|} => Get(1, default));
        await foreach (int x in items.SelectConcurrentAsync((item, {|SC0001:ct|}) => Get(item, default), 2)) { }
        async IAsyncEnumerable<int> Numbers() { await Task.Yield(); yield return 1; }
        await foreach (int x in Numbers().SelectConcurrentAsync((item, {|SC0001:ct|}) => Get(item, default), 2)) { }
        await TaskGroup.RunAsync(async group =>
        {
            await group.StartAsync((ready, {|SC0001:ct|}) => { ready(); return Work(); });
            ChannelReader<int> numbers = group.Produce<int>(async (writer, {|SC0001:ct|}) => await writer.WriteAsync(1));
        });
        """);

    [Fact]
    public Task OtherLibraries_AreNotChecked() => VerifyAsync("""
        Func<CancellationToken, Task> work = ct => Work();
        await Task.Run(() => Work());
        """);

    // ----------------------------------------------------------------------------------------------------------------
    // SC0002: an outer token instead of the work's own

    [Fact]
    public Task OuterParameter_IsReported_AndFixed() => VerifyAsync(
        """
        await TaskGroup.RunAsync(group =>
        {
            group.Run(_ => Work({|SC0002:cancellationToken|}));
            group.Run(token => Work({|SC0002:cancellationToken|}));
            return Task.CompletedTask;
        }, cancellationToken);
        """,
        """
        await TaskGroup.RunAsync(group =>
        {
            group.Run(ct => Work(ct));
            group.Run(token => Work(token));
            return Task.CompletedTask;
        }, cancellationToken);
        """);

    [Fact]
    public Task EveryUseOfTheOuterToken_IsReplaced() => VerifyAsync(
        """
        await TaskGroup.WithTimeoutAsync(TimeSpan.FromSeconds(1), async _ =>
        {
            await Work({|SC0002:cancellationToken|});
            if (cancellationToken.IsCancellationRequested) { return; }
            await Task.Delay(10, cancellationToken);
        });
        """,
        """
        await TaskGroup.WithTimeoutAsync(TimeSpan.FromSeconds(1), async ct =>
        {
            await Work(ct);
            if (ct.IsCancellationRequested) { return; }
            await Task.Delay(10, ct);
        });
        """);

    [Fact]
    public Task FieldsAndOuterSourceTokens_AreReported() => VerifyAsync(
        """
        using var cts = new CancellationTokenSource();
        await TaskGroup.RunAsync(group =>
        {
            group.Run(_ => Work({|SC0002:_field|}));
            group.Run(_ => Work({|SC0002:cts.Token|}));
            return Task.CompletedTask;
        });
        """,
        """
        using var cts = new CancellationTokenSource();
        await TaskGroup.RunAsync(group =>
        {
            group.Run(ct => Work(ct));
            group.Run(ct => Work(ct));
            return Task.CompletedTask;
        });
        """);

    [Fact]
    public Task TheGroupsToken_NoneAndInnerTokens_AreNotOuterTokens() => VerifyAsync("""
        await TaskGroup.RunAsync(group =>
        {
            group.Run(_ => Work(group.CancellationToken));
            group.Run(_ => Work(CancellationToken.None));
            group.Run(_ => Work(default));
            group.Run({|SC0001:ct|} =>
            {
                using var inner = new CancellationTokenSource();
                return Work(inner.Token);
            });
            return Task.CompletedTask;
        });
        """);

    [Fact]
    public Task Fix_PicksANameThatIsFree() => VerifyAsync(
        """
        CancellationToken ct = cancellationToken;
        await TaskGroup.RunAsync(group =>
        {
            group.Run(_ => Work({|SC0002:ct|}));
            return Task.CompletedTask;
        });
        """,
        """
        CancellationToken ct = cancellationToken;
        await TaskGroup.RunAsync(group =>
        {
            group.Run(ct2 => Work(ct2));
            return Task.CompletedTask;
        });
        """);

    [Fact]
    public Task UsingBothTokens_IsNotReported() => VerifyAsync("""
        await TaskGroup.RunAsync(group =>
        {
            group.Run(ct =>
            {
                using var linked = CancellationTokenSource.CreateLinkedTokenSource(ct, cancellationToken);
                return Work(linked.Token);
            });
            return Task.CompletedTask;
        });
        """);

    [Fact]
    public Task LambdasInAnArrayArgument_AreChecked() => VerifyAsync("""
        await TaskGroup.AllAsync(new Func<CancellationToken, Task<int>>[] { {|SC0001:ct|} => Get(1, default), ct => Get(2, ct) });
        """);

    [Fact]
    public Task TheGroupsTokenOrNone_KeptInALocal_IsNotAnOuterToken() => VerifyAsync("""
        await TaskGroup.RunAsync(group =>
        {
            var token = group.CancellationToken;
            var none = CancellationToken.None;
            group.Run(_ => Work(token));
            group.Run(_ => Work(none));
            return Task.CompletedTask;
        });
        """);

    [Fact]
    public Task APropertyOfTheEnclosingObject_IsAnOuterToken() => VerifyAsync(
        """
        await TaskGroup.RunAsync(group =>
        {
            group.Run(_ => Work({|SC0002:Tok|}));
            group.Run(_ => Work({|SC0002:this.Tok|}));
            return Task.CompletedTask;
        });
        """,
        """
        await TaskGroup.RunAsync(group =>
        {
            group.Run(ct => Work(ct));
            group.Run(ct => Work(ct));
            return Task.CompletedTask;
        });
        """);

    [Fact]
    public Task SeveralOuterTokens_AreAllReplaced() => VerifyAsync(
        """
        await TaskGroup.RunAsync(group =>
        {
            group.Run(_ => Task.WhenAll(Work({|SC0002:cancellationToken|}), Work(_field)));
            return Task.CompletedTask;
        });
        """,
        """
        await TaskGroup.RunAsync(group =>
        {
            group.Run(ct => Task.WhenAll(Work(ct), Work(ct)));
            return Task.CompletedTask;
        });
        """);

    [Fact]
    public Task NestedWork_IsFixedWithItsOwnToken() => VerifyAsync(
        """
        await TaskGroup.RunAsync(group =>
        {
            group.Run(token =>
            {
                group.Run(inner => Work({|SC0002:cancellationToken|}));
                return Work({|SC0002:cancellationToken|});
            });
            return Task.CompletedTask;
        });
        """,
        """
        await TaskGroup.RunAsync(group =>
        {
            group.Run(token =>
            {
                group.Run(inner => Work(inner));
                return Work(token);
            });
            return Task.CompletedTask;
        });
        """);

    [Fact]
    public Task ParameterlessAnonymousMethod_IsReportedWithoutAFix() => VerifyAsync("""
        await TaskGroup.RunAsync(group =>
        {
            group.Run(delegate { return Work({|SC0002:cancellationToken|}); });
            return Task.CompletedTask;
        });
        """);
}
