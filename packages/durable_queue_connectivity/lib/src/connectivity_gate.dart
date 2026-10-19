import 'package:connectivity_plus/connectivity_plus.dart';
import 'package:durable_queue/durable_queue.dart';

import 'network_gate.dart';

/// Builds [NetworkGate]s on `connectivity_plus`.
abstract final class ConnectivityGate {
  /// Creates and starts a network gate driven by `connectivity_plus`.
  ///
  /// The gate is open while at least one connection whose type is in
  /// [accept] is up. By default every type except
  /// [ConnectivityResult.none] counts. Pass
  /// `accept: {ConnectivityResult.wifi, ConnectivityResult.ethernet}` for
  /// Wi-Fi-only work.
  ///
  /// [probe] adds a reachability check, for example
  /// `HttpProbe(Uri.parse('https://api.example.com/health')).call`, so a
  /// connection without internet access (a captive portal) keeps the gate
  /// closed. See [NetworkGate] for [probeRetryInterval] and
  /// [minRecheckInterval].
  ///
  /// [connectivity] is for tests; it defaults to `Connectivity()`.
  static Future<NetworkGate> create({
    Set<ConnectivityResult>? accept,
    ReachabilityProbe? probe,
    Duration probeRetryInterval = const Duration(seconds: 30),
    Duration minRecheckInterval = const Duration(seconds: 2),
    Connectivity? connectivity,
    QueueClock? clock,
  }) async {
    if (accept != null &&
        (accept.isEmpty || accept.contains(ConnectivityResult.none))) {
      throw ArgumentError.value(
        accept,
        'accept',
        'Must name at least one connection type other than none',
      );
    }
    final source = connectivity ?? Connectivity();
    bool usable(List<ConnectivityResult> results) =>
        hasUsableConnection(results, accept: accept);

    final gate = NetworkGate(
      networkChanges: source.onConnectivityChanged.map(usable).distinct(),
      checkNetwork: () async => usable(await source.checkConnectivity()),
      probe: probe,
      probeRetryInterval: probeRetryInterval,
      minRecheckInterval: minRecheckInterval,
      clock: clock,
    );
    await gate.start();
    return gate;
  }

  /// Whether [results] from `connectivity_plus` include a connection whose
  /// type is in [accept], or any connection when [accept] is null.
  static bool hasUsableConnection(
    List<ConnectivityResult> results, {
    Set<ConnectivityResult>? accept,
  }) {
    for (final result in results) {
      if (result == ConnectivityResult.none) continue;
      if (accept == null || accept.contains(result)) return true;
    }
    return false;
  }
}
