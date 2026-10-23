using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.Diagnostics;
using Microsoft.CodeAnalysis.Operations;

namespace StructuredConcurrency.Analyzers;

/// <summary>
/// Finds work that a task group's body or one of its children starts with <c>Task.Run</c> or the like and then lets go
/// of, so the group neither waits for it nor sees it fail.
/// </summary>
[DiagnosticAnalyzer(LanguageNames.CSharp)]
public sealed class EscapingWorkAnalyzer : DiagnosticAnalyzer
{
    public const string EscapingWorkId = "SC0003";

    // The name of the TaskGroup parameter the work can be started on instead, for the code fix.
    public const string GroupProperty = "group";

    private const string HelpLink = "https://github.com/gokhanertogan/StructuredConcurrency#analyzers";

    internal static readonly DiagnosticDescriptor EscapingWork = new(
        EscapingWorkId,
        title: "Start the work in the task group",
        messageFormat: "'{0}' starts work outside the task group, so it can outlive the group and its failure is lost; start it with group.Run instead",
        category: "Reliability",
        DiagnosticSeverity.Warning,
        isEnabledByDefault: true,
        description: "Work that a task group's body or children start with Task.Run, Task.Factory.StartNew or ThreadPool.QueueUserWorkItem, without awaiting or returning its task, isn't a child of the group. The group doesn't cancel it, doesn't wait for it, and never hears that it failed.",
        helpLinkUri: HelpLink);

    public override ImmutableArray<DiagnosticDescriptor> SupportedDiagnostics { get; } = ImmutableArray.Create(EscapingWork);

    public override void Initialize(AnalysisContext context)
    {
        context.ConfigureGeneratedCodeAnalysis(GeneratedCodeAnalysisFlags.None);
        context.EnableConcurrentExecution();
        context.RegisterCompilationStartAction(start =>
        {
            INamedTypeSymbol? group = start.Compilation.GetTypeByMetadataName("StructuredConcurrency.TaskGroup");
            if (group is null)
            {
                return;
            }
            start.RegisterOperationAction(operation => AnalyzeInvocation(operation, group), OperationKind.Invocation);
        });
    }

    private static void AnalyzeInvocation(OperationAnalysisContext context, INamedTypeSymbol group)
    {
        var invocation = (IInvocationOperation)context.Operation;
        string? name = StarterName(invocation.TargetMethod);
        if (name is null || !IsDiscarded(invocation))
        {
            return;
        }

        IOperation? function = EnclosingFunction(invocation);
        if (!RunsInGroup(function, context.ContainingSymbol, group))
        {
            return;
        }

        string? groupName = GroupParameter(invocation, context.ContainingSymbol, group)?.Name;
        ImmutableDictionary<string, string?> properties = groupName is null
            ? ImmutableDictionary<string, string?>.Empty
            : ImmutableDictionary<string, string?>.Empty.Add(GroupProperty, groupName);
        context.ReportDiagnostic(Diagnostic.Create(EscapingWork, invocation.Syntax.GetLocation(), properties, name));
    }

    // Task.Run, Task.Factory.StartNew and ThreadPool.QueueUserWorkItem, by the name the message shows.
    private static string? StarterName(IMethodSymbol method)
    {
        INamedTypeSymbol type = method.ContainingType;
        string? ns = type.ContainingNamespace?.ToDisplayString();
        return (ns, type.Name, method.Name) switch
        {
            ("System.Threading.Tasks", "Task", "Run") => "Task.Run",
            ("System.Threading.Tasks", "TaskFactory", "StartNew") => "Task.Factory.StartNew",
            ("System.Threading", "ThreadPool", "QueueUserWorkItem" or "UnsafeQueueUserWorkItem") => "ThreadPool." + method.Name,
            _ => null,
        };
    }

    // A call statement, or an assignment to the discard: nothing keeps the task to await it later.
    private static bool IsDiscarded(IInvocationOperation invocation)
    {
        IOperation? parent = invocation.Parent;
        while (parent is IConversionOperation)
        {
            parent = parent.Parent;
        }
        return parent is IExpressionStatementOperation
            || (parent is ISimpleAssignmentOperation { Target: IDiscardOperation } assignment
                && assignment.Parent is IExpressionStatementOperation);
    }

    // The lambda or local function the call is in, or null for the method body itself.
    private static IOperation? EnclosingFunction(IOperation operation)
    {
        for (IOperation? current = operation.Parent; current is not null; current = current.Parent)
        {
            if (current is IAnonymousFunctionOperation or ILocalFunctionOperation)
            {
                return current;
            }
        }
        return null;
    }

    // Code that runs as the body or a child of a group: a function that is given a TaskGroup, or a lambda passed to
    // StructuredConcurrency. Code in a lambda that only runs later, say an event handler, is left alone.
    private static bool RunsInGroup(IOperation? function, ISymbol containingSymbol, INamedTypeSymbol group)
    {
        IMethodSymbol? method = function switch
        {
            IAnonymousFunctionOperation lambda => lambda.Symbol,
            ILocalFunctionOperation local => local.Symbol,
            _ => containingSymbol as IMethodSymbol,
        };
        if (method is not null && method.Parameters.Any(p => SymbolEqualityComparer.Default.Equals(p.Type, group)))
        {
            return true;
        }
        if (function is not IAnonymousFunctionOperation)
        {
            return false;
        }

        IOperation? parent = function.Parent;
        while (parent is IDelegateCreationOperation or IConversionOperation)
        {
            parent = parent.Parent;
        }
        return parent is IArgumentOperation { Parent: IInvocationOperation call }
            && SymbolEqualityComparer.Default.Equals(call.TargetMethod.ContainingAssembly, group.ContainingAssembly);
    }

    // The nearest TaskGroup parameter in scope: the work can be started on it instead.
    private static IParameterSymbol? GroupParameter(IOperation operation, ISymbol containingSymbol, INamedTypeSymbol group)
    {
        var functions = new List<IMethodSymbol>();
        for (IOperation? current = operation.Parent; current is not null; current = current.Parent)
        {
            if (current is IAnonymousFunctionOperation lambda)
            {
                functions.Add(lambda.Symbol);
            }
            else if (current is ILocalFunctionOperation local)
            {
                functions.Add(local.Symbol);
            }
        }
        if (containingSymbol is IMethodSymbol method)
        {
            functions.Add(method);
        }

        return functions
            .SelectMany(function => function.Parameters)
            .FirstOrDefault(parameter => SymbolEqualityComparer.Default.Equals(parameter.Type, group));
    }
}
