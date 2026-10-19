import 'dart:async';

import 'package:durable_queue/durable_queue.dart';
import 'package:http/http.dart' as http;

import 'errors/io_errors_stub.dart'
    if (dart.library.io) 'errors/io_errors_io.dart';

/// Whether [error] looks like a lost or missing connection rather than a
/// problem with the request itself.
///
/// True for `dart:io` `SocketException` and `HttpException`, for
/// `package:http` `ClientException` (which `package:http` throws for
/// connection failures on every platform), and, when [includeTimeouts] is
/// true, for [TimeoutException]. Errors with an HTTP status, such as a 500
/// response turned into an exception by your own code, are not connection
/// errors.
///
/// Dio reports connection failures as `DioException` with type
/// `connectionError`; see the README for a matching rule.
bool isConnectionError(Object error, {bool includeTimeouts = true}) {
  if (isIoConnectionError(error)) return true;
  if (error is http.ClientException) return true;
  if (includeTimeouts && error is TimeoutException) return true;
  return false;
}

/// A `register(deferOn:)` rule that sends connection errors to [gate].
///
/// ```dart
/// queue.register<UploadTask>(
///   ...,
///   requires: {'network'},
///   deferOn: deferOnConnectionError('network'),
/// );
/// ```
DeferOnError deferOnConnectionError(
  String gate, {
  bool includeTimeouts = true,
}) {
  if (gate.isEmpty) {
    throw ArgumentError.value(gate, 'gate', 'Must not be empty');
  }
  return (error, stackTrace) =>
      isConnectionError(error, includeTimeouts: includeTimeouts) ? gate : null;
}
