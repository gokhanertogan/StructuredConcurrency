using System.Collections.Generic;
using System.Collections.Immutable;
using System.Composition;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CodeActions;
using Microsoft.CodeAnalysis.CodeFixes;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using Microsoft.CodeAnalysis.Formatting;
using StructuredConcurrency.Analyzers;

namespace StructuredConcurrency.CodeFixes;

/// <summary>
/// Fixes SC0004 by turning <c>Task.WhenAll(F(x, token), G(y, token))</c> into a <c>TaskGroup.RunAsync</c> whose
/// children make the same calls with the group's token, and which is given <c>token</c> itself.
/// </summary>
[ExportCodeFixProvider(LanguageNames.CSharp, Name = nameof(UseTaskGroupCodeFix)), Shared]
public sealed class UseTaskGroupCodeFix : CodeFixProvider
{
    private const string Title = "Run the operations in a TaskGroup";

    public override ImmutableArray<string> FixableDiagnosticIds { get; } = ImmutableArray.Create(WhenAllAnalyzer.WhenAllId);

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
            if (root.FindNode(diagnostic.Location.SourceSpan, getInnermostNodeForTie: true) is InvocationExpressionSyntax whenAll
                && SharedToken(whenAll, model, context.CancellationToken) is { } token
                && ResultUnused(whenAll, model, context.CancellationToken))
            {
                context.RegisterCodeFix(
                    CodeAction.Create(Title, _ => Task.FromResult(Fix(context.Document, root, model, whenAll, token)), equivalenceKey: Title),
                    diagnostic);
            }
        }
    }

    // The token every call passes, when each task is a call written right in WhenAll's arguments and they all pass the
    // same simple token, such as cancellationToken or cts.Token. Tasks kept in locals are awaited again later for their
    // results, which a rewrite of this one call can't follow.
    private static ExpressionSyntax? SharedToken(InvocationExpressionSyntax whenAll, SemanticModel model, CancellationToken cancellationToken)
    {
        ExpressionSyntax? shared = null;
        foreach (ArgumentSyntax argument in whenAll.ArgumentList.Arguments)
        {
            if (argument.Expression is not InvocationExpressionSyntax call)
            {
                return null;
            }
            List<ExpressionSyntax> tokens = TokenArguments(call, model, cancellationToken).ToList();
            if (tokens.Count != 1 || !IsSimple(tokens[0]) || (shared is not null && !shared.IsEquivalentTo(tokens[0])))
            {
                return null;
            }
            shared = tokens[0];
        }
        return shared;
    }

    private static IEnumerable<ExpressionSyntax> TokenArguments(InvocationExpressionSyntax call, SemanticModel model, CancellationToken cancellationToken) =>
        call.ArgumentList.Arguments
            .Select(argument => argument.Expression)
            .Where(expression => model.GetTypeInfo(expression, cancellationToken).Type is { Name: "CancellationToken", ContainingNamespace.Name: "Threading" });

    // A name or a chain of names, so moving it out of the calls doesn't change when or how often it is evaluated.
    private static bool IsSimple(ExpressionSyntax expression) => expression switch
    {
        IdentifierNameSyntax => true,
        MemberAccessExpressionSyntax { Name: IdentifierNameSyntax } access => IsSimple(access.Expression) || access.Expression is ThisExpressionSyntax,
        _ => false,
    };

    // RunAsync returns a plain Task, so it can stand in for WhenAll only where nobody looks at WhenAll's results.
    private static bool ResultUnused(InvocationExpressionSyntax whenAll, SemanticModel model, CancellationToken cancellationToken)
    {
        if (model.GetSymbolInfo(whenAll, cancellationToken).Symbol is IMethodSymbol { ReturnType: INamedTypeSymbol { IsGenericType: false } })
        {
            return true;
        }
        SyntaxNode awaited = whenAll;
        if (whenAll.Parent is MemberAccessExpressionSyntax { Name.Identifier.ValueText: "ConfigureAwait", Parent: InvocationExpressionSyntax configured })
        {
            awaited = configured;
        }
        return awaited.Parent is AwaitExpressionSyntax { Parent: ExpressionStatementSyntax };
    }

    private static Document Fix(Document document, SyntaxNode root, SemanticModel model, InvocationExpressionSyntax whenAll, ExpressionSyntax token)
    {
        string groupName = FreeName("group", whenAll, model);
        string tokenName = FreeName("ct", whenAll, model);
        string indent = whenAll.FirstAncestorOrSelf<StatementSyntax>()?.GetLeadingTrivia().LastOrDefault(t => t.IsKind(SyntaxKind.WhitespaceTrivia)).ToString() ?? "";
        string newLine = root.DescendantTrivia().FirstOrDefault(t => t.IsKind(SyntaxKind.EndOfLineTrivia)).ToString() is { Length: > 0 } eol ? eol : "\r\n";
        string task = whenAll.Expression is MemberAccessExpressionSyntax { Expression: var receiver } ? receiver.ToString() : "Task";

        var text = new StringBuilder();
        text.Append("TaskGroup.RunAsync(").Append(groupName).Append(" =>").Append(newLine);
        text.Append(indent).Append('{').Append(newLine);
        foreach (ArgumentSyntax argument in whenAll.ArgumentList.Arguments)
        {
            var call = (InvocationExpressionSyntax)argument.Expression;
            ExpressionSyntax own = TokenArguments(call, model, default).Single();
            InvocationExpressionSyntax child = call.ReplaceNode(own, SyntaxFactory.IdentifierName(tokenName).WithTriviaFrom(own));
            text.Append(indent).Append("    ").Append(groupName).Append(".Run(").Append(tokenName).Append(" => ").Append(child.WithoutTrivia().ToString()).Append(");").Append(newLine);
        }
        text.Append(indent).Append("    return ").Append(task).Append(".CompletedTask;").Append(newLine);
        text.Append(indent).Append("}, ").Append(token.WithoutTrivia().ToString()).Append(')');

        ExpressionSyntax runAsync = SyntaxFactory.ParseExpression(text.ToString()).WithTriviaFrom(whenAll);
        SyntaxNode newRoot = root.ReplaceNode(whenAll, runAsync);

        // The package is referenced, but the file may not import its namespace yet.
        bool imported = model.LookupNamespacesAndTypes(whenAll.SpanStart, name: "TaskGroup")
            .Any(symbol => symbol.ContainingNamespace?.ToDisplayString() == "StructuredConcurrency");
        if (!imported && newRoot is CompilationUnitSyntax unit)
        {
            UsingDirectiveSyntax directive = SyntaxFactory.UsingDirective(SyntaxFactory.ParseName("StructuredConcurrency"))
                .WithAdditionalAnnotations(Formatter.Annotation);
            newRoot = unit.AddUsings(directive);
        }
        return document.WithSyntaxRoot(newRoot);
    }

    // The name, or the name with 2, 3... appended if the code around the call already uses it.
    private static string FreeName(string name, InvocationExpressionSyntax whenAll, SemanticModel model)
    {
        for (int i = 1; ; i++)
        {
            string candidate = i == 1 ? name : name + i;
            bool taken = model.LookupSymbols(whenAll.SpanStart, name: candidate).Any()
                || whenAll.DescendantTokens().Any(token => token.IsKind(SyntaxKind.IdentifierToken) && token.ValueText == candidate);
            if (!taken)
            {
                return candidate;
            }
        }
    }
}
