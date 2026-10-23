using System.Collections.Immutable;
using System.Composition;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CodeActions;
using Microsoft.CodeAnalysis.CodeFixes;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using StructuredConcurrency.Analyzers;

namespace StructuredConcurrency.CodeFixes;

/// <summary>
/// Fixes SC0003 by turning <c>Task.Run(() =&gt; WorkAsync())</c> into <c>group.Run(ct =&gt; WorkAsync())</c>. The new
/// token parameter is left for the work to use, which SC0001 then points out.
/// </summary>
[ExportCodeFixProvider(LanguageNames.CSharp, Name = nameof(StartInGroupCodeFix)), Shared]
public sealed class StartInGroupCodeFix : CodeFixProvider
{
    private const string Title = "Start the work with group.Run";

    public override ImmutableArray<string> FixableDiagnosticIds { get; } = ImmutableArray.Create(EscapingWorkAnalyzer.EscapingWorkId);

    public override FixAllProvider GetFixAllProvider() => WellKnownFixAllProviders.BatchFixer;

    public override async Task RegisterCodeFixesAsync(CodeFixContext context)
    {
        SyntaxNode? root = await context.Document.GetSyntaxRootAsync(context.CancellationToken).ConfigureAwait(false);
        SemanticModel? model = await context.Document.GetSemanticModelAsync(context.CancellationToken).ConfigureAwait(false);
        if (root is null || model is null)
        {
            return;
        }

        foreach (Diagnostic diagnostic in context.Diagnostics)
        {
            if (diagnostic.Properties.TryGetValue(EscapingWorkAnalyzer.GroupProperty, out string? group)
                && group is not null
                && root.FindNode(diagnostic.Location.SourceSpan, getInnermostNodeForTie: true) is InvocationExpressionSyntax invocation
                && AsyncWork(invocation, model, context.CancellationToken) is ParenthesizedLambdaExpressionSyntax work)
            {
                context.RegisterCodeFix(
                    CodeAction.Create(Title, _ => Task.FromResult(Fix(context.Document, root, model, invocation, work, group)), equivalenceKey: Title),
                    diagnostic);
            }
        }
    }

    // The work, when the call is Task.Run(() => ...) with a lambda that returns a task: group.Run takes that as it is.
    // Synchronous work, a method group or a token of its own would each need more than a rewrite.
    private static ParenthesizedLambdaExpressionSyntax? AsyncWork(InvocationExpressionSyntax invocation, SemanticModel model, CancellationToken cancellationToken)
    {
        if (invocation.ArgumentList.Arguments.Count != 1
            || invocation.ArgumentList.Arguments[0].Expression is not ParenthesizedLambdaExpressionSyntax { ParameterList.Parameters.Count: 0 } work
            || model.GetSymbolInfo(invocation, cancellationToken).Symbol is not IMethodSymbol { Name: "Run", Parameters.Length: 1 } method
            || method.Parameters[0].Type is not INamedTypeSymbol { Name: "Func", TypeArguments.Length: 1 } func)
        {
            return null;
        }
        return func.TypeArguments[0] is { Name: "Task", ContainingNamespace.Name: "Tasks" } ? work : null;
    }

    private static Document Fix(
        Document document,
        SyntaxNode root,
        SemanticModel model,
        InvocationExpressionSyntax invocation,
        ParenthesizedLambdaExpressionSyntax work,
        string group)
    {
        string name = FreeName(work, model);
        ParameterSyntax parameter = SyntaxFactory.Parameter(SyntaxFactory.Identifier(name));
        LambdaExpressionSyntax child = SyntaxFactory.SimpleLambdaExpression(parameter)
            .WithAsyncKeyword(work.AsyncKeyword)
            .WithArrowToken(work.ArrowToken)
            .WithBlock(work.Block)
            .WithExpressionBody(work.ExpressionBody)
            .WithTriviaFrom(work);

        InvocationExpressionSyntax run = SyntaxFactory.InvocationExpression(
                SyntaxFactory.MemberAccessExpression(
                    SyntaxKind.SimpleMemberAccessExpression,
                    SyntaxFactory.IdentifierName(group),
                    SyntaxFactory.IdentifierName("Run")),
                SyntaxFactory.ArgumentList(SyntaxFactory.SingletonSeparatedList(SyntaxFactory.Argument(child))))
            .WithTriviaFrom(invocation);
        return document.WithSyntaxRoot(root.ReplaceNode(invocation, run));
    }

    // "ct", or "ct2", "ct3"... if the work or its surroundings already use that name.
    private static string FreeName(ParenthesizedLambdaExpressionSyntax work, SemanticModel model)
    {
        for (int i = 1; ; i++)
        {
            string candidate = i == 1 ? "ct" : "ct" + i;
            bool taken = model.LookupSymbols(work.SpanStart, name: candidate).Any()
                || work.DescendantTokens().Any(token => token.IsKind(SyntaxKind.IdentifierToken) && token.ValueText == candidate);
            if (!taken)
            {
                return candidate;
            }
        }
    }
}
