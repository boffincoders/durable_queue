/// @docImport '../errors.dart';
/// @docImport 'task_status.dart';
library;

/// What happens to a waiting task when one of its dependencies does not
/// complete.
///
/// A dependency "does not complete" when it ends [TaskStatus.failed] or
/// [TaskStatus.cancelled], or when its record is no longer in storage.
enum DependencyFailurePolicy {
  /// Cancel the dependent task. It never runs. This is the default.
  cancel,

  /// Fail the dependent task with a [DependencyFailedException]. It never
  /// runs.
  fail,

  /// Run the dependent task anyway, once every dependency has finished in any
  /// terminal state. Useful for cleanup or reporting steps.
  run,
}
