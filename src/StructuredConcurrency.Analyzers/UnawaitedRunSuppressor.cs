using System.Collections.Immutable;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using Microsoft.CodeAnalysis.Diagnostics;

namespace StructuredConcurrency.Analyzers;

/// <summary>
/// Suppresses CS4014 ("this call is not awaited") on <c>group.Run(...)</c> inside an async lambda or method. The group
/// awaits every child it starts, and the task <c>Run</c> returns is there for callers who want the child's result, so
/// not awaiting it is the normal way to use it, not a mistake.
/// </summary>
[DiagnosticAnalyzer(LanguageNames.CSharp)]
public sealed class UnawaitedRunSuppressor : DiagnosticSuppressor
{
    internal static readonly SuppressionDescriptor UnawaitedRun = new(
        id: "SCSP0001",
        suppressedDiagnosticId: "CS4014",
        justification: "The task group awaits every child it starts with Run; the task Run returns only needs awaiting to get the child's result.");

    public override ImmutableArray<SuppressionDescriptor> SupportedSuppressions { get; } = ImmutableArray.Create(UnawaitedRun);

    public override void ReportSuppressions(SuppressionAnalysisContext context)
    {
        INamedTypeSymbol? group = context.Compilation.GetTypeByMetadataName("StructuredConcurrency.TaskGroup");
        if (group is null)
        {
            return;
        }

        foreach (Diagnostic diagnostic in context.ReportedDiagnostics)
        {
            if (diagnostic.Location.SourceTree is not { } tree)
            {
                continue;
            }

            SyntaxNode node = tree.GetRoot(context.CancellationToken).FindNode(diagnostic.Location.SourceSpan, getInnermostNodeForTie: true);
            if (node is ExpressionStatementSyntax statement)
            {
                node = statement.Expression;
            }
            if (node is not InvocationExpressionSyntax invocation)
            {
                continue;
            }

            SemanticModel model = context.GetSemanticModel(tree);
            if (model.GetSymbolInfo(invocation, context.CancellationToken).Symbol is IMethodSymbol { Name: "Run" } method
                && SymbolEqualityComparer.Default.Equals(method.ContainingType, group))
            {
                context.ReportSuppression(Suppression.Create(UnawaitedRun, diagnostic));
            }
        }
    }
}
