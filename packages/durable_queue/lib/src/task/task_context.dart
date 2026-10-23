/// @docImport '../queue/durable_queue.dart';
/// @docImport 'task_status.dart';
library;

import 'dart:async';

/// Information about the attempt currently executing.
///
/// Handlers may ignore this. It exists so a handler can read the idempotency
/// key, report the attempt, or cooperate with cancellation.
final class TaskContext {
  /// Creates a context. Application code receives instances from the queue.
  TaskContext({
    required this.taskId,
    required this.attempt,
    required this.maxAttempts,
    required this.idempotencyKey,
    required this.deduplicationKey,
    required bool Function() isCancellationRequested,
    Future<void>? whenCancellationRequested,
    void Function(String reference)? onHandOff,
  }) : _isCancellationRequested = isCancellationRequested,
       _onHandOff = onHandOff,
       whenCancellationRequested =
           whenCancellationRequested ?? Completer<void>().future;

  /// Id assigned when the task was enqueued.
  final String taskId;

  /// 1-based number of this attempt.
  final int attempt;

  /// Maximum attempts configured by the task's retry policy.
  final int maxAttempts;

  /// Application-defined idempotency key, when one was provided.
  final String? idempotencyKey;

  /// Deduplication key, when one was provided.
  final String? deduplicationKey;

  final bool Function() _isCancellationRequested;

  /// Whether [DurableQueue.cancel] was called for this running task.
  ///
  /// The queue cannot abort arbitrary Dart futures. Handlers that need to
  /// stop early should poll this and throw [TaskCancelledException].
  bool get isCancellationRequested => _isCancellationRequested();

  /// Completes when [DurableQueue.cancel] is called for this running task.
  ///
  /// Never completes if the task is not cancelled. Use it to abort work that
  /// accepts a cancellation signal, such as an HTTP request, instead of
  /// polling [isCancellationRequested]. It completes immediately if
  /// cancellation was requested before this attempt started.
  final Future<void> whenCancellationRequested;

  final void Function(String reference)? _onHandOff;

  /// Hands the task to work that continues outside this handler, such as an
  /// upload run by the operating system, under [reference].
  ///
  /// When the handler then returns normally, the task is not completed: it
  /// stays [TaskStatus.running] with `StoredTask.handOffReference` set, and
  /// the queue waits for [DurableQueue.completeHandOff] or
  /// [DurableQueue.failHandOff]. Meanwhile the task does not take a
  /// concurrency slot, a restart does not treat it as interrupted, and tasks
  /// that depend on it keep waiting. If the handler throws after calling
  /// this, the throw counts and the hand-off is dropped.
  ///
  /// [reference] names the outside work so it can be found again, for
  /// example after a restart. Call this at most once per attempt, while the
  /// handler runs.
  ///
  /// Throws [ArgumentError] if [reference] is blank, [StateError] if the
  /// attempt already handed off or its handler has returned, and
  /// [UnsupportedError] for a context that was not created by a queue.
  void handOff(String reference) {
    if (reference.trim().isEmpty) {
      throw ArgumentError.value(reference, 'reference', 'Must not be blank');
    }
    final onHandOff = _onHandOff;
    if (onHandOff == null) {
      throw UnsupportedError('This TaskContext does not support handOff');
    }
    onHandOff(reference);
  }
}
