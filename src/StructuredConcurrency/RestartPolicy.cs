namespace StructuredConcurrency;

/// <summary>
/// How a <see cref="Supervisor"/> restarts work that fails: how often it may restart within a window of time, and how
/// long it waits before each restart. Modelled on Erlang/OTP's supervisor intensity and period.
/// </summary>
public sealed class RestartPolicy
{
    private readonly int _maxRestarts = 3;
    private readonly TimeSpan _window = TimeSpan.FromMinutes(1);
    private readonly TimeSpan _initialDelay = TimeSpan.FromMilliseconds(100);
    private readonly TimeSpan _maxDelay = TimeSpan.FromSeconds(30);
    private readonly double _backoffMultiplier = 2;
    private readonly TimeProvider _timeProvider = TimeProvider.System;

    /// <summary>
    /// Gets the default policy: at most 3 restarts a minute, waiting 100 ms before the first, doubling each time up to
    /// 30 seconds.
    /// </summary>
    public static RestartPolicy Default { get; } = new();

    /// <summary>
    /// Gets how many restarts are allowed within <see cref="Window"/>. When the work fails once more, the supervisor
    /// gives up and throws that failure. Defaults to 3; 0 never restarts.
    /// </summary>
    /// <exception cref="ArgumentOutOfRangeException">The value is negative.</exception>
    public int MaxRestarts
    {
        get => _maxRestarts;
        init
        {
            ArgumentOutOfRangeException.ThrowIfNegative(value);
            _maxRestarts = value;
        }
    }

    /// <summary>
    /// Gets the window that <see cref="MaxRestarts"/> counts restarts in: restarts longer ago than this are forgotten,
    /// so work that fails now and then keeps being restarted, while work that keeps failing is given up on. Defaults to
    /// one minute; <see cref="Timeout.InfiniteTimeSpan"/> counts every restart.
    /// </summary>
    /// <exception cref="ArgumentOutOfRangeException">The value isn't positive or infinite.</exception>
    public TimeSpan Window
    {
        get => _window;
        init
        {
            if (value != Timeout.InfiniteTimeSpan && value <= TimeSpan.Zero)
            {
                throw new ArgumentOutOfRangeException(nameof(Window), value, "The window must be positive, or infinite.");
            }
            _window = value;
        }
    }

    /// <summary>Gets how long to wait before the first restart in the window. Defaults to 100 milliseconds.</summary>
    /// <exception cref="ArgumentOutOfRangeException">The value is negative, infinite or longer than about 49 days.</exception>
    public TimeSpan InitialDelay
    {
        get => _initialDelay;
        init
        {
            ValidateDelay(value, nameof(InitialDelay));
            _initialDelay = value;
        }
    }

    /// <summary>Gets the longest wait before a restart, however many there have been. Defaults to 30 seconds.</summary>
    /// <exception cref="ArgumentOutOfRangeException">The value is negative, infinite or longer than about 49 days.</exception>
    public TimeSpan MaxDelay
    {
        get => _maxDelay;
        init
        {
            ValidateDelay(value, nameof(MaxDelay));
            _maxDelay = value;
        }
    }

    /// <summary>
    /// Gets what each wait is multiplied by for the next restart in the window. Defaults to 2; 1 waits
    /// <see cref="InitialDelay"/> every time.
    /// </summary>
    /// <exception cref="ArgumentOutOfRangeException">The value is less than 1, or not a finite number.</exception>
    public double BackoffMultiplier
    {
        get => _backoffMultiplier;
        init
        {
            if (!double.IsFinite(value) || value < 1)
            {
                throw new ArgumentOutOfRangeException(nameof(BackoffMultiplier), value, "The multiplier must be at least 1.");
            }
            _backoffMultiplier = value;
        }
    }

    /// <summary>
    /// Gets a function that decides whether a failure is worth a restart, or <see langword="null"/> to restart on every
    /// failure. A failure it turns down is thrown at once. If it throws, the failure is not restarted either.
    /// </summary>
    public Func<Exception, bool>? ShouldRestart { get; init; }

    /// <summary>Gets a callback that runs before each restart, for instance to log it.</summary>
    public Action<RestartEvent>? OnRestart { get; init; }

    /// <summary>
    /// Gets the time provider that measures <see cref="Window"/> and the delays; pass a fake one in tests to make time
    /// pass on demand. Defaults to <see cref="TimeProvider.System"/>.
    /// </summary>
    public TimeProvider TimeProvider
    {
        get => _timeProvider;
        init
        {
            ArgumentNullException.ThrowIfNull(value);
            _timeProvider = value;
        }
    }

    // The wait before the given restart in the window, counting from 1.
    internal TimeSpan DelayBefore(int restart)
    {
        double delay = _initialDelay.Ticks * Math.Pow(_backoffMultiplier, restart - 1);
        return delay >= _maxDelay.Ticks ? _maxDelay : TimeSpan.FromTicks((long)delay);
    }

    private static void ValidateDelay(TimeSpan value, string paramName)
    {
        if (value < TimeSpan.Zero || value > Timeouts.Max)
        {
            throw new ArgumentOutOfRangeException(paramName, value, $"The delay must be between zero and {Timeouts.Max}.");
        }
    }
}

/// <summary>Describes a restart that a <see cref="Supervisor"/> is about to make.</summary>
/// <param name="Failure">The failure that ended the run.</param>
/// <param name="Restart">How many restarts there have been so far, including this one.</param>
/// <param name="Delay">How long the supervisor waits before restarting.</param>
public readonly record struct RestartEvent(Exception Failure, int Restart, TimeSpan Delay);
