import 'dart:async';

import 'package:durable_queue/durable_queue.dart';

/// Answers "can the app reach the service it needs right now?".
///
/// Return true when it can. Errors and timeouts should return false; a probe
/// that throws is treated as false.
typedef ReachabilityProbe = Future<bool> Function();

/// A [QueueGate] that is open while the device has a network connection and,
/// when a [probe] is given, while that probe succeeds.
///
/// [networkChanges] reports whether any network interface is up (Wi-Fi,
/// mobile, Ethernet, ...); [checkNetwork] reads the current state. On Flutter,
/// `ConnectivityGate.create` wires both to `connectivity_plus`. Other sources
/// work too, which is how this class is tested without Flutter.
///
/// A network interface does not prove the internet is reachable, for example
/// behind a captive portal. With a [probe]:
///
/// * the gate opens only after the probe succeeds;
/// * while the interface is up but the probe fails, it probes again every
///   [probeRetryInterval];
/// * [recheck], which the queue calls when `register(deferOn:)` attributes a
///   handler error to this gate, probes again and closes the gate if the probe
///   fails.
///
/// Without a probe, [recheck] reads [checkNetwork] again.
///
/// Call [start] before handing the gate to `DurableQueue.addGate`, and
/// [dispose] when the queue is closed.
final class NetworkGate extends QueueGate {
  /// Creates a gate. It is closed until [start] completes.
  ///
  /// [probeRetryInterval] and [minRecheckInterval] must be positive.
  /// [minRecheckInterval] limits how often [recheck] probes, so a burst of
  /// failing tasks causes one probe, not one per task. [clock] defaults to the
  /// system clock.
  NetworkGate({
    required Stream<bool> networkChanges,
    required Future<bool> Function() checkNetwork,
    ReachabilityProbe? probe,
    this.probeRetryInterval = const Duration(seconds: 30),
    this.minRecheckInterval = const Duration(seconds: 2),
    QueueClock? clock,
  }) : _networkChanges = networkChanges,
       _checkNetwork = checkNetwork,
       _probe = probe,
       _clock = clock ?? const SystemQueueClock() {
    if (probeRetryInterval <= Duration.zero) {
      throw ArgumentError.value(
        probeRetryInterval,
        'probeRetryInterval',
        'Must be positive',
      );
    }
    if (minRecheckInterval <= Duration.zero) {
      throw ArgumentError.value(
        minRecheckInterval,
        'minRecheckInterval',
        'Must be positive',
      );
    }
  }

  final Stream<bool> _networkChanges;
  final Future<bool> Function() _checkNetwork;
  final ReachabilityProbe? _probe;
  final QueueClock _clock;

  /// Wait between probes while a network is up but the probe fails.
  final Duration probeRetryInterval;

  /// Shortest time between two probes started by [recheck].
  final Duration minRecheckInterval;

  final StreamController<bool> _changes = StreamController<bool>.broadcast(
    sync: true,
  );
  StreamSubscription<bool>? _subscription;
  QueueDelay? _retry;
  Future<void>? _starting;

  var _open = false;
  var _networkUp = false;
  var _known = false;
  var _disposed = false;

  /// Increases on every network change, so a probe that finishes after the
  /// network changed again cannot overwrite the newer state.
  var _generation = 0;
  Future<void>? _probing;
  DateTime? _lastRecheck;

  @override
  bool get isOpen => _open;

  @override
  Stream<bool> get changes => _changes.stream;

  /// Whether a network interface is currently up, regardless of the probe.
  bool get hasNetwork => _networkUp;

  /// Reads the current state, runs the probe if needed, and starts listening
  /// for changes. Calling it again returns the same future.
  Future<void> start() => _starting ??= _start();

  Future<void> _start() async {
    _subscription = _networkChanges.listen(
      _onNetwork,
      // A failing source keeps the last known state.
      onError: (Object error, StackTrace stackTrace) {},
    );
    final bool up;
    try {
      up = await _checkNetwork();
    } on Object {
      // Unknown: let the stream decide.
      return;
    }
    _onNetwork(up);
    await _probing;
  }

  void _onNetwork(bool up) {
    if (_disposed) return;
    // Sources often repeat the current state, for example when a listener
    // subscribes. Nothing changed, so keep the probe result and any retry.
    if (_known && up == _networkUp) return;
    _known = true;
    _generation++;
    _networkUp = up;
    _cancelRetry();
    if (!up) {
      _set(false);
      return;
    }
    if (_probe == null) {
      _set(true);
      return;
    }
    _runProbe();
  }

  /// Probes and updates the state. Concurrent calls share one probe.
  Future<void> _runProbe() {
    final running = _probing;
    if (running != null) return running;
    final generation = _generation;
    return _probing = () async {
      bool reachable;
      try {
        reachable = await _probe!();
      } on Object {
        reachable = false;
      }
      _probing = null;
      if (_disposed) return;
      if (generation != _generation) {
        // The network changed while probing; probe the newer state.
        if (_networkUp && _probe != null) await _runProbe();
        return;
      }
      if (!_networkUp) return;
      _set(reachable);
      if (!reachable) _scheduleRetry();
    }();
  }

  void _scheduleRetry() {
    _cancelRetry();
    final delay = _clock.delayUntil(_clock.now().add(probeRetryInterval));
    _retry = delay;
    delay.future.then((_) {
      if (!identical(_retry, delay) || _disposed) return;
      _retry = null;
      if (_networkUp) _runProbe();
    });
  }

  void _cancelRetry() {
    _retry?.cancel();
    _retry = null;
  }

  @override
  void recheck() {
    if (_disposed || _starting == null) return;
    final now = _clock.now();
    final last = _lastRecheck;
    if (last != null && now.difference(last) < minRecheckInterval) return;
    _lastRecheck = now;
    if (_probe != null) {
      if (_networkUp) _runProbe();
      return;
    }
    final generation = _generation;
    _checkNetwork().then((up) {
      if (generation == _generation && up != _networkUp) _onNetwork(up);
    }, onError: (Object error, StackTrace stackTrace) {});
  }

  void _set(bool open) {
    if (open == _open) return;
    _open = open;
    if (!_changes.isClosed) _changes.add(open);
  }

  /// Stops listening and probing and closes [changes]. The gate stays in its
  /// last state.
  Future<void> dispose() async {
    if (_disposed) return;
    _disposed = true;
    _cancelRetry();
    await _subscription?.cancel();
    await _changes.close();
  }
}
