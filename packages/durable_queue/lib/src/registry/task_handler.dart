import '../retry/retry_policy.dart';
import '../task/durable_task.dart';
import '../task/task_context.dart';

/// Rebuilds a task from the JSON stored by `DurableTask.toJson`.
typedef TaskDecoder<T extends DurableTask> = T Function(
  Map<String, dynamic> json,
);

/// Executes one attempt.
///
/// The queue awaits the returned future. An error becomes a failed attempt.
/// Throw [TaskCancelledException] to acknowledge cooperative cancellation.
typedef TaskHandler<T extends DurableTask> = Future<void> Function(
  T task,
  TaskContext context,
);

/// Decides whether [error] should be retried.
///
/// Return false for permanent failures such as validation errors. When this
/// is omitted, every error except [TaskCancelledException] is retried until
/// the retry policy's attempt budget is spent.
///
/// A predicate that throws is treated as "do not retry".
typedef RetryPredicate = bool Function(Object error, StackTrace stackTrace);

/// Chooses the wait before the next attempt after [error], or null to use
/// the task's [RetryPolicy].
///
/// Use it when the error itself says how long to wait, for example an HTTP
/// 429 or 503 response with a `Retry-After` header. Only consulted when the
/// task is going to be retried; the attempt still counts toward
/// `RetryPolicy.maxAttempts`. Negative durations are treated as zero and
/// values above [RetryPolicy.maxSchedulableDelay] are capped. A resolver
/// that throws falls back to the policy's delay.
typedef RetryDelayResolver = Duration? Function(
  Object error,
  StackTrace stackTrace,
);

/// Attributes a handler error to a gate, for example a connection error to a
/// `network` gate. Return the gate's name to defer the task instead of
/// counting a failed attempt, or null to handle the error normally.
///
/// The returned gate must be one of the task type's `requires`; any other
/// value is ignored. A function that throws is treated as returning null.
typedef DeferOnError = String? Function(Object error, StackTrace stackTrace);
