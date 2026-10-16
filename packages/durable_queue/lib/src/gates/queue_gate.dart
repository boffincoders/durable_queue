import 'dart:async';

/// A condition that must hold before tasks that require it may start, for
/// example "the network is reachable".
///
/// Add a gate with `DurableQueue.addGate` and list it in `register(requires:)`.
/// While a required gate is closed, matching tasks are deferred: they keep
/// their place and their attempt count, and start soon after the gate opens.
/// Tasks that are already running are not cancelled when a gate closes.
///
/// Implementations report the current state through [isOpen] and every change
/// through [changes]. [recheck] is called when a task failed in a way that
/// suggests the condition does not hold even though the gate is open (see
/// `register(deferOn:)`); implementations that can probe, such as a
/// connectivity check, should probe and update their state.
abstract class QueueGate {
  /// Whether tasks that require this gate may start now.
  bool get isOpen;

  /// Emits the new state each time it changes.
  Stream<bool> get changes;

  /// Asks the gate to check its condition again. Does nothing by default.
  void recheck() {}
}

/// A [QueueGate] opened and closed by the application.
///
/// Useful for conditions the app already tracks, for tests, and to connect a
/// connectivity listener without another package.
final class ManualQueueGate extends QueueGate {
  /// Creates a gate that starts [open] (default) or closed.
  ManualQueueGate({bool open = true}) : _open = open;

  final StreamController<bool> _changes = StreamController<bool>.broadcast(
    sync: true,
  );
  bool _open;

  @override
  bool get isOpen => _open;

  @override
  Stream<bool> get changes => _changes.stream;

  /// Opens the gate.
  void open() => set(true);

  /// Closes the gate.
  void close() => set(false);

  /// Opens or closes the gate. Does nothing when the state is unchanged.
  void set(bool open) {
    if (open == _open) return;
    _open = open;
    if (!_changes.isClosed) _changes.add(open);
  }

  /// Closes the [changes] stream. The gate keeps its last state.
  Future<void> dispose() => _changes.close();
}
