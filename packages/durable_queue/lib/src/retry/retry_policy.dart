/// How many times a task may run, and how long to wait between failures.
///
/// [maxAttempts] counts executions, not extra retries. A policy with
/// `maxAttempts: 1` runs once and does not retry.
///
/// [delayAfter] receives the number of attempts that have already started.
/// After attempt 1 fails, the queue waits `delayAfter(1, ...)`.
///
/// Computed delays are capped at [maxSchedulableDelay] so a retry cannot
/// overflow [DateTime].
sealed class RetryPolicy {
  const RetryPolicy();

  /// Largest delay the scheduler will store.
  static const Duration maxSchedulableDelay = Duration(days: 365);

  /// Total executions allowed for a task, including the first.
  int get maxAttempts;

  /// Delay before the next attempt after [completedAttempts] have started.
  ///
  /// [randomFraction] is in `[0, 1)` and is used only by policies with jitter.
  Duration delayAfter(int completedAttempts, double randomFraction);

  /// Serializes this policy so it can be stored with the task.
  Map<String, dynamic> toJson();

  /// Runs the task once. Further failures are permanent.
  static RetryPolicy none() => const NoRetryPolicy();

  /// Waits the same [delay] between attempts.
  static RetryPolicy fixed({
    required int maxAttempts,
    required Duration delay,
  }) {
    return FixedRetryPolicy(maxAttempts: maxAttempts, delay: delay);
  }

  /// Doubles [initialDelay] after each failure, up to [maxDelay].
  ///
  /// After attempt 1 the delay is [initialDelay], then double that, and so
  /// on. When [jitter] is true the delay is multiplied by a factor in
  /// `[1 - jitterFactor, 1 + jitterFactor)` and then capped at [maxDelay].
  static RetryPolicy exponential({
    required int maxAttempts,
    required Duration initialDelay,
    Duration? maxDelay,
    bool jitter = false,
    double jitterFactor = 0.2,
  }) {
    return ExponentialRetryPolicy(
      maxAttempts: maxAttempts,
      initialDelay: initialDelay,
      maxDelay: maxDelay,
      jitter: jitter,
      jitterFactor: jitterFactor,
    );
  }

  /// Restores a policy produced by [toJson].
  static RetryPolicy fromJson(Map<String, dynamic> json) {
    final type = json['type'];
    return switch (type) {
      'none' => const NoRetryPolicy(),
      'fixed' => FixedRetryPolicy(
        maxAttempts: _requireAttempts(json),
        delay: _requireDuration(json, 'delayMicros'),
      ),
      'exponential' => ExponentialRetryPolicy(
        maxAttempts: _requireAttempts(json),
        initialDelay: _requireDuration(json, 'initialDelayMicros'),
        maxDelay: _optionalDuration(json, 'maxDelayMicros'),
        jitter: _requireBool(json, 'jitter'),
        jitterFactor: _requireDouble(json, 'jitterFactor'),
      ),
      _ => throw FormatException('Unknown retry policy "$type"'),
    };
  }
}

/// A policy that allows a single attempt.
final class NoRetryPolicy extends RetryPolicy {
  /// Creates a policy that does not retry.
  const NoRetryPolicy();

  @override
  int get maxAttempts => 1;

  @override
  Duration delayAfter(int completedAttempts, double randomFraction) {
    _validateAttempt(completedAttempts);
    _validateFraction(randomFraction);
    return Duration.zero;
  }

  @override
  Map<String, dynamic> toJson() => {'type': 'none'};

  @override
  bool operator ==(Object other) => other is NoRetryPolicy;

  @override
  int get hashCode => 'none'.hashCode;
}

/// A policy that waits a constant duration between attempts.
final class FixedRetryPolicy extends RetryPolicy {
  /// Creates a fixed-delay policy.
  FixedRetryPolicy({required this.maxAttempts, required this.delay}) {
    _validateMaxAttempts(maxAttempts);
    _validateDelay(delay, 'delay');
  }

  @override
  final int maxAttempts;

  /// Delay applied after every failed attempt that still has a retry left.
  final Duration delay;

  @override
  Duration delayAfter(int completedAttempts, double randomFraction) {
    _validateAttempt(completedAttempts);
    _validateFraction(randomFraction);
    return delay;
  }

  @override
  Map<String, dynamic> toJson() => {
    'type': 'fixed',
    'maxAttempts': maxAttempts,
    'delayMicros': delay.inMicroseconds,
  };

  @override
  bool operator ==(Object other) =>
      other is FixedRetryPolicy &&
      other.maxAttempts == maxAttempts &&
      other.delay == delay;

  @override
  int get hashCode => Object.hash(maxAttempts, delay);
}

/// A policy that doubles the delay after each failed attempt.
final class ExponentialRetryPolicy extends RetryPolicy {
  /// Creates an exponential-backoff policy.
  ExponentialRetryPolicy({
    required this.maxAttempts,
    required this.initialDelay,
    this.maxDelay,
    this.jitter = false,
    this.jitterFactor = 0.2,
  }) {
    _validateMaxAttempts(maxAttempts);
    _validateDelay(initialDelay, 'initialDelay');
    if (maxDelay != null) {
      _validateDelay(maxDelay!, 'maxDelay');
      if (maxDelay! < initialDelay) {
        throw ArgumentError.value(
          maxDelay,
          'maxDelay',
          'Must be greater than or equal to initialDelay',
        );
      }
    }
    if (jitterFactor <= 0 || jitterFactor > 1) {
      throw ArgumentError.value(
        jitterFactor,
        'jitterFactor',
        'Must be in the range (0, 1]',
      );
    }
  }

  @override
  final int maxAttempts;

  /// Delay after the first failed attempt, before doubling.
  final Duration initialDelay;

  /// Upper bound applied before and after jitter. Null means no policy cap
  /// other than [RetryPolicy.maxSchedulableDelay].
  final Duration? maxDelay;

  /// Whether to spread the delay by [jitterFactor].
  final bool jitter;

  /// Half-width of the jitter window as a fraction of the base delay.
  ///
  /// `0.2` keeps the delay within ±20% of the backoff value.
  final double jitterFactor;

  @override
  Duration delayAfter(int completedAttempts, double randomFraction) {
    _validateAttempt(completedAttempts);
    _validateFraction(randomFraction);
    var delay = initialDelay;
    for (var i = 1; i < completedAttempts; i++) {
      if (delay >= _cap) return _applyJitter(_cap, randomFraction);
      final doubled = delay * 2;
      delay = doubled > _cap ? _cap : doubled;
    }
    if (delay > _cap) delay = _cap;
    return _applyJitter(delay, randomFraction);
  }

  Duration get _cap {
    final configured = maxDelay;
    if (configured == null || configured > RetryPolicy.maxSchedulableDelay) {
      return RetryPolicy.maxSchedulableDelay;
    }
    return configured;
  }

  Duration _applyJitter(Duration delay, double randomFraction) {
    if (!jitter || delay == Duration.zero) return delay;
    final factor = 1 + jitterFactor * (2 * randomFraction - 1);
    final micros = (delay.inMicroseconds * factor).round();
    var result = Duration(microseconds: micros < 0 ? 0 : micros);
    if (result > _cap) result = _cap;
    return result;
  }

  @override
  Map<String, dynamic> toJson() => {
    'type': 'exponential',
    'maxAttempts': maxAttempts,
    'initialDelayMicros': initialDelay.inMicroseconds,
    'maxDelayMicros': maxDelay?.inMicroseconds,
    'jitter': jitter,
    'jitterFactor': jitterFactor,
  };

  @override
  bool operator ==(Object other) =>
      other is ExponentialRetryPolicy &&
      other.maxAttempts == maxAttempts &&
      other.initialDelay == initialDelay &&
      other.maxDelay == maxDelay &&
      other.jitter == jitter &&
      other.jitterFactor == jitterFactor;

  @override
  int get hashCode =>
      Object.hash(maxAttempts, initialDelay, maxDelay, jitter, jitterFactor);
}

void _validateMaxAttempts(int maxAttempts) {
  if (maxAttempts < 1) {
    throw ArgumentError.value(maxAttempts, 'maxAttempts', 'Must be at least 1');
  }
}

void _validateDelay(Duration delay, String name) {
  if (delay.isNegative) {
    throw ArgumentError.value(delay, name, 'Must not be negative');
  }
  if (delay > RetryPolicy.maxSchedulableDelay) {
    throw ArgumentError.value(
      delay,
      name,
      'Must be at most ${RetryPolicy.maxSchedulableDelay}',
    );
  }
}

void _validateAttempt(int completedAttempts) {
  if (completedAttempts < 1) {
    throw ArgumentError.value(
      completedAttempts,
      'completedAttempts',
      'Must be at least 1',
    );
  }
}

void _validateFraction(double randomFraction) {
  if (randomFraction.isNaN || randomFraction < 0 || randomFraction >= 1) {
    throw ArgumentError.value(
      randomFraction,
      'randomFraction',
      'Must be in [0, 1)',
    );
  }
}

int _requireAttempts(Map<String, dynamic> json) {
  final value = json['maxAttempts'];
  if (value is! int) {
    throw FormatException('Retry policy maxAttempts must be an int');
  }
  return value;
}

Duration _requireDuration(Map<String, dynamic> json, String key) {
  final value = json[key];
  if (value is! int) {
    throw FormatException('Retry policy $key must be an int');
  }
  return Duration(microseconds: value);
}

Duration? _optionalDuration(Map<String, dynamic> json, String key) {
  final value = json[key];
  if (value == null) return null;
  if (value is! int) {
    throw FormatException('Retry policy $key must be an int or null');
  }
  return Duration(microseconds: value);
}

bool _requireBool(Map<String, dynamic> json, String key) {
  final value = json[key];
  if (value is bool) return value;
  throw FormatException('Retry policy $key must be a bool');
}

double _requireDouble(Map<String, dynamic> json, String key) {
  final value = json[key];
  if (value is int) return value.toDouble();
  if (value is double) return value;
  throw FormatException('Retry policy $key must be a number');
}
