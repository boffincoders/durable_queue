import '../task/task_status.dart';

/// Reported on `DurableQueue.holdChanges` when a task is held, released, or
/// stops running because of a hold.
///
/// It is not a `QueueEvent`, so code that switches over every event type is
/// unaffected. Listen to both streams to keep a view of tasks current.
final class TaskHoldChanged {
  /// Creates a hold change for [taskId].
  const TaskHoldChanged({
    required this.taskId,
    required this.taskType,
    required this.occurredAt,
    required this.held,
    required this.status,
    required this.isHandedOff,
  });

  /// The task whose hold changed.
  final String taskId;

  /// `DurableTask.type` for [taskId].
  final String taskType;

  /// When the change was recorded, according to the queue clock.
  final DateTime occurredAt;

  /// Whether the task is held after the change.
  final bool held;

  /// The task's status after the change.
  final TaskStatus status;

  /// Whether the task is handed off to outside work (see
  /// `TaskContext.handOff`). Code that runs that work pauses it when [held]
  /// is true and resumes it when [held] is false.
  final bool isHandedOff;

  @override
  String toString() =>
      'TaskHoldChanged($taskId, held: $held, ${status.name}'
      '${isHandedOff ? ', handed off' : ''})';
}
