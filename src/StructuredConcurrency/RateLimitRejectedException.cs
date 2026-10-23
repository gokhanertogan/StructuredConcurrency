namespace StructuredConcurrency;

/// <summary>
/// The exception a child fails with when the group's <see cref="TaskGroupOptions.RateLimiter"/> refuses it a permit,
/// for instance because the limiter's queue is full.
/// </summary>
public sealed class RateLimitRejectedException : Exception
{
    /// <summary>Initializes a new instance of the <see cref="RateLimitRejectedException"/> class.</summary>
    /// <param name="retryAfter">When the limiter suggests trying again, if it says.</param>
    public RateLimitRejectedException(TimeSpan? retryAfter = null)
        : base(retryAfter is TimeSpan after
            ? $"The rate limiter rejected the work; it suggests retrying after {after}."
            : "The rate limiter rejected the work.")
    {
        RetryAfter = retryAfter;
    }

    /// <summary>
    /// Gets how long the limiter suggests waiting before trying again, or <see langword="null"/> if it doesn't say.
    /// </summary>
    public TimeSpan? RetryAfter { get; }
}
