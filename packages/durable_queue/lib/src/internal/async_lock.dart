import 'dart:async';

/// Serializes asynchronous critical sections.
///
/// The lock is not reentrant. A section must not wait on work that needs the
/// same lock.
final class AsyncLock {
  Future<void> _tail = Future<void>.value();

  /// Runs [action] after previously scheduled actions have finished.
  Future<T> synchronized<T>(Future<T> Function() action) {
    final previous = _tail;
    final gate = Completer<void>();
    _tail = gate.future;
    return previous.then((_) async {
      try {
        return await action();
      } finally {
        if (!gate.isCompleted) gate.complete();
      }
    });
  }
}
