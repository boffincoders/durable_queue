/// Diagnostic record for the latest failed attempt.
///
/// The queue stores [error] and [stackTrace] as text. It does not redact
/// them. Handlers should avoid throwing exceptions whose string form contains
/// secrets.
final class TaskFailure {
  /// Creates a failure record for one attempt.
  const TaskFailure({
    required this.error,
    required this.stackTrace,
    required this.failedAt,
    required this.attempt,
  });

  /// `error.toString()` from the failed attempt.
  final String error;

  /// `stackTrace.toString()` from the failed attempt, when one exists.
  final String? stackTrace;

  /// When the failure was recorded, according to the queue clock.
  final DateTime failedAt;

  /// 1-based attempt number that failed.
  final int attempt;

  /// Serializes this failure.
  Map<String, dynamic> toJson() => {
    'error': error,
    'stackTrace': stackTrace,
    'failedAt': failedAt.toIso8601String(),
    'attempt': attempt,
  };

  /// Restores a failure from [json].
  factory TaskFailure.fromJson(Map<String, dynamic> json) {
    return TaskFailure(
      error: _requireString(json, 'error'),
      stackTrace: _optionalString(json, 'stackTrace'),
      failedAt: _requireDateTime(json, 'failedAt'),
      attempt: _requireInt(json, 'attempt'),
    );
  }
}

String _requireString(Map<String, dynamic> json, String key) {
  final value = json[key];
  if (value is String) return value;
  throw FormatException('TaskFailure.$key must be a string');
}

String? _optionalString(Map<String, dynamic> json, String key) {
  final value = json[key];
  if (value == null) return null;
  if (value is String) return value;
  throw FormatException('TaskFailure.$key must be a string or null');
}

int _requireInt(Map<String, dynamic> json, String key) {
  final value = json[key];
  if (value is int) return value;
  throw FormatException('TaskFailure.$key must be an int');
}

DateTime _requireDateTime(Map<String, dynamic> json, String key) {
  final value = json[key];
  if (value is! String) {
    throw FormatException('TaskFailure.$key must be an ISO-8601 string');
  }
  return DateTime.parse(value);
}
