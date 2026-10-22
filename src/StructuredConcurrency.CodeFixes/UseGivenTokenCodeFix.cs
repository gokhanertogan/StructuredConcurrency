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
using Microsoft.CodeAnalysis.Editing;
using StructuredConcurrency.Analyzers;

namespace StructuredConcurrency.CodeFixes;

/// <summary>
/// Fixes SC0002 by replacing the outer tokens with the work's own parameter, which it names <c>ct</c> when it was
/// <c>_</c>.
/// </summary>
[ExportCodeFixProvider(LanguageNames.CSharp, Name = nameof(UseGivenTokenCodeFix)), Shared]
public sealed class UseGivenTokenCodeFix : CodeFixProvider
{
    private const string Title = "Use the cancellation token passed to the work";

    public override ImmutableArray<string> FixableDiagnosticIds { get; } = ImmutableArray.Create(ChildTokenAnalyzer.OtherTokenId);

    public override FixAllProvider GetFixAllProvider() => WellKnownFixAllProviders.BatchFixer;

    public override Task RegisterCodeFixesAsync(CodeFixContext context)
    {
        foreach (Diagnostic diagnostic in context.Diagnostics)
        {
            // Without the parameter's location (a parameterless delegate { }), there is no parameter to switch to.
            if (diagnostic.AdditionalLocations.Count < 2)
            {
                continue;
            }
            context.RegisterCodeFix(
                CodeAction.Create(Title, cancellationToken => FixAsync(context.Document, diagnostic, cancellationToken), equivalenceKey: Title),
                diagnostic);
        }
        return Task.CompletedTask;
    }

    private static async Task<Document> FixAsync(Document document, Diagnostic diagnostic, CancellationToken cancellationToken)
    {
        SyntaxNode? root = await document.GetSyntaxRootAsync(cancellationToken).ConfigureAwait(false);
        SemanticModel? model = await document.GetSemanticModelAsync(cancellationToken).ConfigureAwait(false);
        if (root is null || model is null || diagnostic.AdditionalLocations.Count < 2)
        {
            return document;
        }

        // The analyzer puts the work's token parameter first, then every use of the outer token.
        ParameterSyntax? parameter = root.FindToken(diagnostic.AdditionalLocations[0].SourceSpan.Start)
            .Parent?.AncestorsAndSelf().OfType<ParameterSyntax>().FirstOrDefault();
        AnonymousFunctionExpressionSyntax? work = parameter?.Ancestors().OfType<AnonymousFunctionExpressionSyntax>().FirstOrDefault();
        if (parameter is null || work is null)
        {
            return document;
        }

        string name = parameter.Identifier.ValueText;
        if (name == "_")
        {
            name = FreeName(work, model);
        }

        var editor = new SyntaxEditor(root, document.Project.Solution.Workspace.Services);
        if (name != parameter.Identifier.ValueText)
        {
            editor.ReplaceNode(parameter, parameter.WithIdentifier(SyntaxFactory.Identifier(name).WithTriviaFrom(parameter.Identifier)));
        }
        foreach (Location use in diagnostic.AdditionalLocations.Skip(1))
        {
            SyntaxNode node = root.FindNode(use.SourceSpan, getInnermostNodeForTie: true);
            if (node is ArgumentSyntax argument)
            {
                node = argument.Expression;
            }
            editor.ReplaceNode(node, SyntaxFactory.IdentifierName(name).WithTriviaFrom(node));
        }
        return document.WithSyntaxRoot(editor.GetChangedRoot());
    }

    // "ct", or "ct2", "ct3"... if the work or its surroundings already use that name.
    private static string FreeName(AnonymousFunctionExpressionSyntax work, SemanticModel model)
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
