/// Lifecycle of a [DurableQueue] instance.
enum QueueRunState {
  /// Created or fully stopped. Tasks may be stored, but none will start.
  idle,

  /// Eligible tasks are allowed to start, up to the concurrency limit.
  running,

  /// In-flight handlers continue. No further tasks start until [resume].
  paused,

  /// [stop] is waiting for in-flight handlers to finish.
  stopping,
}
