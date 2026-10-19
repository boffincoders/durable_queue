/// A network gate for `durable_queue`: tasks that need the network wait
/// while the device is offline, without using attempts, and start as soon as
/// it is back online.
library;

export 'src/connection_errors.dart'
    show deferOnConnectionError, isConnectionError;
export 'src/connectivity_gate.dart';
export 'src/http_probe.dart';
export 'src/network_gate.dart';
