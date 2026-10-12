/// A serializable unit of work.
///
/// The queue persists [toJson], not the closure that performs the work.
/// After a restart the registered decoder rebuilds the task and the
/// registered handler runs it.
///
/// [toJson] must return JSON-encodable values: `null`, `bool`, `num`,
/// `String`, lists, and string-keyed maps. Do not put secrets in the payload
/// unless the configured [QueueStorage] is an appropriate place for them.
abstract class DurableTask {
  /// Creates a task base.
  const DurableTask();

  /// Stable type name used as the registry and storage key.
  ///
  /// This must match the `type` passed to `DurableQueue.register`.
  String get type;

  /// JSON-encodable data required to rebuild this task.
  Map<String, dynamic> toJson();
}
