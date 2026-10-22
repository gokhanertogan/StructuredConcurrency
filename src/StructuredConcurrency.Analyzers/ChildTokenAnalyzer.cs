using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.Diagnostics;
using Microsoft.CodeAnalysis.Operations;

namespace StructuredConcurrency.Analyzers;

/// <summary>
/// Finds work passed to StructuredConcurrency that doesn't use the cancellation token it is given, so cancelling the
/// group (on the first failure, a timeout or shutdown) can't stop it.
/// </summary>
[DiagnosticAnalyzer(LanguageNames.CSharp)]
public sealed class ChildTokenAnalyzer : DiagnosticAnalyzer
{
    public const string IgnoredTokenId = "SC0001";
    public const string OtherTokenId = "SC0002";

    private const string HelpLink = "https://github.com/gokhanertogan/StructuredConcurrency#analyzers";

    internal static readonly DiagnosticDescriptor IgnoredToken = new(
        IgnoredTokenId,
        title: "Pass the cancellation token on",
        messageFormat: "'{0}' is never used, so this work keeps running when it is cancelled; pass it to the calls that can be cancelled, or name it '_' if ignoring it is intended",
        category: "Reliability",
        DiagnosticSeverity.Warning,
        isEnabledByDefault: true,
        description: "Work started by a TaskGroup, a policy such as AllAsync, a timeout or SingleFlight receives a token that is cancelled when its result is no longer needed. Work that never looks at the token runs to the end anyway, and the call waits for it.",
        helpLinkUri: HelpLink);

    internal static readonly DiagnosticDescriptor OtherToken = new(
        OtherTokenId,
        title: "Use the cancellation token passed to the work",
        messageFormat: "This work uses '{0}' instead of the token it is given, so it keeps running when a sibling fails, the time is up or the group is cancelled",
        category: "Reliability",
        DiagnosticSeverity.Warning,
        isEnabledByDefault: true,
        description: "The token passed to the work is the one that is cancelled on the first failure, a timeout or Cancel(). An outer token, such as the method's own cancellationToken, is only cancelled by the caller.",
        helpLinkUri: HelpLink);

    public override ImmutableArray<DiagnosticDescriptor> SupportedDiagnostics { get; } = ImmutableArray.Create(IgnoredToken, OtherToken);

    public override void Initialize(AnalysisContext context)
    {
        context.ConfigureGeneratedCodeAnalysis(GeneratedCodeAnalysisFlags.None);
        context.EnableConcurrentExecution();
        context.RegisterCompilationStartAction(start =>
        {
            INamedTypeSymbol? token = start.Compilation.GetTypeByMetadataName("System.Threading.CancellationToken");
            INamedTypeSymbol? group = start.Compilation.GetTypeByMetadataName("StructuredConcurrency.TaskGroup");
            if (token is null || group is null)
            {
                return;
            }
            start.RegisterOperationAction(operation => AnalyzeInvocation(operation, token, group), OperationKind.Invocation);
        });
    }

    private static void AnalyzeInvocation(OperationAnalysisContext context, INamedTypeSymbol token, INamedTypeSymbol group)
    {
        var invocation = (IInvocationOperation)context.Operation;
        if (!SymbolEqualityComparer.Default.Equals(invocation.TargetMethod.ContainingAssembly, group.ContainingAssembly))
        {
            return;
        }

        foreach (IArgumentOperation argument in invocation.Arguments)
        {
            foreach (IAnonymousFunctionOperation work in AnonymousFunctions(argument.Value))
            {
                foreach (IParameterSymbol parameter in work.Symbol.Parameters)
                {
                    if (SymbolEqualityComparer.Default.Equals(parameter.Type, token))
                    {
                        AnalyzeWork(context, work, parameter, token, group);
                    }
                }
            }
        }
    }

    private static void AnalyzeWork(
        OperationAnalysisContext context,
        IAnonymousFunctionOperation work,
        IParameterSymbol parameter,
        INamedTypeSymbol token,
        INamedTypeSymbol group)
    {
        bool doesWork = false;
        var outerTokens = new List<IOperation>();
        foreach (IOperation operation in work.Body.Descendants())
        {
            if (operation is IParameterReferenceOperation reference
                && SymbolEqualityComparer.Default.Equals(reference.Parameter, parameter))
            {
                return;   // it uses its token
            }

            doesWork |= operation is IAwaitOperation
                || (operation is IInvocationOperation call && StartsAsyncWork(call.TargetMethod));

            // A nested lambda that takes a token of its own (say, a nested group.Run) is checked on its own.
            if (SymbolEqualityComparer.Default.Equals(operation.Type, token)
                && !IsInNestedWork(operation, work, token)
                && IsOuterToken(operation, work, group))
            {
                outerTokens.Add(operation);
            }
        }

        // A parameterless anonymous method (delegate { ... }) has a token parameter with no location of its own.
        Location? parameterLocation = parameter.Locations.FirstOrDefault();
        if (outerTokens.Count > 0)
        {
            // The parameter, then every use of an outer token, so the code fix can replace them all at once.
            IEnumerable<Location> additional = outerTokens.Select(use => use.Syntax.GetLocation());
            context.ReportDiagnostic(Diagnostic.Create(
                OtherToken,
                outerTokens[0].Syntax.GetLocation(),
                additionalLocations: parameterLocation is null ? null : new[] { parameterLocation }.Concat(additional),
                messageArgs: string.Join("' and '", outerTokens.Select(use => use.Syntax.ToString()).Distinct())));
        }
        else if (doesWork && parameter.Name != "_")
        {
            context.ReportDiagnostic(Diagnostic.Create(
                IgnoredToken,
                parameterLocation ?? work.Syntax.GetFirstToken().GetLocation(),
                parameter.Name));
        }
    }

    private static bool IsInNestedWork(IOperation operation, IAnonymousFunctionOperation work, INamedTypeSymbol token)
    {
        for (IOperation? current = operation.Parent; current is not null && current != work; current = current.Parent)
        {
            if (current is IAnonymousFunctionOperation nested
                && nested.Symbol.Parameters.Any(p => SymbolEqualityComparer.Default.Equals(p.Type, token)))
            {
                return true;
            }
        }
        return false;
    }

    // Calls that could have been given the token: anything that returns a task, a ValueTask or an async stream,
    // except the Task.FromResult family, which only wraps a value that is already there.
    private static bool StartsAsyncWork(IMethodSymbol method)
    {
        if (method.ContainingType is { Name: "Task" or "ValueTask", ContainingNamespace: { Name: "Tasks" } }
            && method.Name.StartsWith("From"))
        {
            return false;
        }

        ITypeSymbol type = method.ReturnType;
        string? name = type.OriginalDefinition.ContainingNamespace?.ToDisplayString();
        return name == "System.Threading.Tasks" && type.Name is "Task" or "ValueTask"
            || name == "System.Collections.Generic" && type.Name == "IAsyncEnumerable";
    }

    // A token that comes from outside the work: the enclosing method's parameter, a field or property of the
    // enclosing object, or one read from something declared outside the work. CancellationToken.None, default and the
    // group's own CancellationToken (the same token the work receives) don't count, even when kept in a local first.
    private static bool IsOuterToken(IOperation operation, IAnonymousFunctionOperation work, INamedTypeSymbol group)
    {
        switch (operation)
        {
            case IParameterReferenceOperation parameter:
                return IsDeclaredOutside(parameter.Parameter, work);
            case ILocalReferenceOperation local:
                return IsDeclaredOutside(local.Local, work) && !HoldsHarmlessToken(local.Local, operation, group);
            case IFieldReferenceOperation field:
                return field.Field.IsStatic || IsOuterInstance(field.Instance, work);
            case IPropertyReferenceOperation property:
                return !IsHarmlessProperty(property, group) && IsOuterInstance(property.Instance, work);
            default:
                return false;
        }
    }

    private static bool IsOuterInstance(IOperation? instance, IAnonymousFunctionOperation work) => instance switch
    {
        IParameterReferenceOperation parameter => IsDeclaredOutside(parameter.Parameter, work),
        ILocalReferenceOperation local => IsDeclaredOutside(local.Local, work),
        IFieldReferenceOperation or IInstanceReferenceOperation => true,
        _ => false,
    };

    private static bool IsHarmlessProperty(IPropertyReferenceOperation property, INamedTypeSymbol group) =>
        property.Property.IsStatic || SymbolEqualityComparer.Default.Equals(property.Property.ContainingType, group);

    // A local initialized with default, CancellationToken.None or the group's token.
    private static bool HoldsHarmlessToken(ILocalSymbol local, IOperation reference, INamedTypeSymbol group)
    {
        foreach (SyntaxReference declaration in local.DeclaringSyntaxReferences)
        {
            if (reference.SemanticModel?.GetOperation(declaration.GetSyntax()) is IVariableDeclaratorOperation declarator)
            {
                IOperation? value = declarator.GetVariableInitializer()?.Value;
                while (value is IConversionOperation conversion)
                {
                    value = conversion.Operand;
                }
                return value is IDefaultValueOperation
                    || (value is IPropertyReferenceOperation property && IsHarmlessProperty(property, group));
            }
        }
        return false;
    }

    private static bool IsDeclaredOutside(ISymbol symbol, IAnonymousFunctionOperation work) =>
        symbol.DeclaringSyntaxReferences.All(declaration =>
            declaration.SyntaxTree != work.Syntax.SyntaxTree || !work.Syntax.Span.Contains(declaration.Span));

    // The lambdas in an argument: the argument itself, or the elements of an array or collection built in place.
    // Lambdas inside a lambda aren't arguments of this call, so they aren't searched.
    private static IEnumerable<IAnonymousFunctionOperation> AnonymousFunctions(IOperation value)
    {
        var pending = new Stack<IOperation>();
        pending.Push(value);
        while (pending.Count > 0)
        {
            IOperation current = pending.Pop();
            if (current is IAnonymousFunctionOperation function)
            {
                yield return function;
                continue;
            }
            foreach (IOperation child in current.ChildOperations)
            {
                pending.Push(child);
            }
        }
    }
}
