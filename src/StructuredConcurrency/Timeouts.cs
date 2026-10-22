namespace StructuredConcurrency;

internal static class Timeouts
{
    // The longest wait Task.Delay and CancellationTokenSource.CancelAfter accept.
    public static readonly TimeSpan Max = TimeSpan.FromMilliseconds(uint.MaxValue - 1);

    public static void Validate(TimeSpan timeout, string paramName)
    {
        if (timeout != Timeout.InfiniteTimeSpan && (timeout < TimeSpan.Zero || timeout > Max))
        {
            throw new ArgumentOutOfRangeException(paramName, timeout, $"The timeout must be between zero and {Max}, or infinite.");
        }
    }
}
