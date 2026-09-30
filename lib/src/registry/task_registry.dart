import '../errors.dart';
import '../task/durable_task.dart';
import '../task/task_context.dart';
import 'task_handler.dart';

/// One registered task type.
///
/// This is an engine type. Application code registers through `DurableQueue`.
final class RegisteredTask {
  RegisteredTask._({
    required this.type,
    required this.decode,
    required this.handle,
    required this.retryIf,
  });

  /// Registry key.
  final String type;

  /// Rebuilds a task from persisted JSON.
  final DurableTask Function(Map<String, dynamic> json) decode;

  /// Runs one attempt.
  final Future<void> Function(DurableTask task, TaskContext context) handle;

  /// Optional retry predicate. Null retries every handler error.
  final RetryPredicate? retryIf;
}

/// Handlers keyed by task type.
final class TaskRegistry {
  final Map<String, RegisteredTask> _byType = {};

  /// Types currently registered, in registration order.
  List<String> get types => List<String>.unmodifiable(_byType.keys);

  /// Whether [type] has a handler.
  bool contains(String type) => _byType.containsKey(type);

  /// Returns the registration for [type], or null.
  RegisteredTask? find(String type) => _byType[type];

  /// Registers [type].
  ///
  /// Throws [ArgumentError] if [type] is empty and [StateError] if [type] is
  /// already registered.
  void register<T extends DurableTask>({
    required String type,
    required TaskDecoder<T> decoder,
    required TaskHandler<T> handler,
    RetryPredicate? retryIf,
  }) {
    if (type.isEmpty) {
      throw ArgumentError.value(type, 'type', 'Must not be empty');
    }
    if (_byType.containsKey(type)) {
      throw StateError('Task type "$type" is already registered');
    }
    _byType[type] = RegisteredTask._(
      type: type,
      decode: decoder,
      handle: (task, context) => handler(task as T, context),
      retryIf: retryIf,
    );
  }

  /// Exception used when [type] was not registered.
  UnknownTaskTypeException unknownType(String type) {
    return UnknownTaskTypeException(type, registered: types);
  }
}
