using System.Collections.Immutable;
using System.Linq;
using System.Threading;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using Microsoft.CodeAnalysis.Diagnostics;
using Microsoft.CodeAnalysis.Operations;

namespace StructuredConcurrency.Analyzers;

/// <summary>
/// Finds <c>Task.WhenAll</c> over operations that each take a cancellation token: when one fails, the others keep
/// running and WhenAll waits for them, although they could have been cancelled.
/// </summary>
[DiagnosticAnalyzer(LanguageNames.CSharp)]
public sealed class WhenAllAnalyzer : DiagnosticAnalyzer
{
    public const string WhenAllId = "SC0004";

    private const string HelpLink = "https://github.com/gokhanertogan/StructuredConcurrency#analyzers";

    // A suggestion, not a warning: waiting for every task is sometimes what the code wants.
    internal static readonly DiagnosticDescriptor WhenAll = new(
        WhenAllId,
        title: "Cancel the other operations when one fails",
        messageFormat: "When one of these operations fails, Task.WhenAll lets the others run to the end and waits for them; run them in a TaskGroup so the first failure cancels the rest",
        category: "Reliability",
        DiagnosticSeverity.Info,
        isEnabledByDefault: true,
        description: "Each operation passed to Task.WhenAll here takes a cancellation token, but nothing cancels it when a sibling fails, so the caller gets the error only after the slowest operation has finished. TaskGroup.RunAsync and TaskGroup.AllAsync cancel the rest on the first failure and still throw the original error.",
        helpLinkUri: HelpLink);

    public override ImmutableArray<DiagnosticDescriptor> SupportedDiagnostics { get; } = ImmutableArray.Create(WhenAll);

    public override void Initialize(AnalysisContext context)
    {
        context.ConfigureGeneratedCodeAnalysis(GeneratedCodeAnalysisFlags.None);
        context.EnableConcurrentExecution();
        context.RegisterCompilationStartAction(start =>
        {
            INamedTypeSymbol? token = start.Compilation.GetTypeByMetadataName("System.Threading.CancellationToken");
            INamedTypeSymbol? task = start.Compilation.GetTypeByMetadataName("System.Threading.Tasks.Task");
            INamedTypeSymbol? group = start.Compilation.GetTypeByMetadataName("StructuredConcurrency.TaskGroup");
            if (token is null || task is null || group is null)
            {
                return;
            }
            start.RegisterOperationAction(operation => AnalyzeInvocation(operation, token, task), OperationKind.Invocation);
        });
    }

    private static void AnalyzeInvocation(OperationAnalysisContext context, INamedTypeSymbol token, INamedTypeSymbol task)
    {
        var invocation = (IInvocationOperation)context.Operation;
        if (invocation.TargetMethod.Name != "WhenAll"
            || !SymbolEqualityComparer.Default.Equals(invocation.TargetMethod.ContainingType, task)
            || invocation.Syntax is not InvocationExpressionSyntax { ArgumentList.Arguments.Count: >= 2 } syntax
            || invocation.SemanticModel is not { } model)
        {
            return;
        }

        // The tasks as written, one argument each. Read from the syntax, so the params array and the params span
        // overloads of newer .NET versions look the same.
        foreach (ArgumentSyntax argument in syntax.ArgumentList.Arguments)
        {
            if (!StartsWithToken(model.GetOperation(argument.Expression, context.CancellationToken), model, token, context.CancellationToken))
            {
                return;
            }
        }
        context.ReportDiagnostic(Diagnostic.Create(WhenAll, syntax.GetLocation()));
    }

    // The task comes from a call that is given a real cancellation token, either right there or in the initializer of
    // the local that holds it.
    private static bool StartsWithToken(IOperation? operation, SemanticModel model, INamedTypeSymbol token, CancellationToken cancellationToken)
    {
        while (operation is IConversionOperation conversion)
        {
            operation = conversion.Operand;
        }
        if (operation is ILocalReferenceOperation local
            && local.Local.DeclaringSyntaxReferences.FirstOrDefault()?.GetSyntax(cancellationToken) is VariableDeclaratorSyntax { Initializer.Value: { } initializer })
        {
            operation = model.GetOperation(initializer, cancellationToken);
            while (operation is IConversionOperation inner)
            {
                operation = inner.Operand;
            }
        }
        return operation is IInvocationOperation call && call.Arguments.Any(argument => IsRealToken(argument, token));
    }

    // CancellationToken.None and default can't cancel anything, so code passing them isn't relying on cancellation.
    private static bool IsRealToken(IArgumentOperation argument, INamedTypeSymbol token)
    {
        if (argument.ArgumentKind != ArgumentKind.Explicit || !SymbolEqualityComparer.Default.Equals(argument.Value.Type, token))
        {
            return false;
        }
        IOperation value = argument.Value;
        while (value is IConversionOperation conversion)
        {
            value = conversion.Operand;
        }
        return value is not (IDefaultValueOperation or IPropertyReferenceOperation { Property.Name: "None", Instance: null });
    }
}
