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
