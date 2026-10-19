import 'dart:async';
import 'dart:io';

import 'package:durable_queue_connectivity/durable_queue_connectivity.dart';
import 'package:http/http.dart' as http;
import 'package:test/test.dart';

void main() {
  test('recognizes connection errors', () {
    expect(isConnectionError(const SocketException('no route')), isTrue);
    expect(isConnectionError(const HttpException('closed early')), isTrue);
    expect(isConnectionError(http.ClientException('failed')), isTrue);
    expect(isConnectionError(TimeoutException('slow')), isTrue);
    expect(
      isConnectionError(TimeoutException('slow'), includeTimeouts: false),
      isFalse,
    );
    expect(isConnectionError(StateError('bug')), isFalse);
    expect(isConnectionError(const FormatException('bad json')), isFalse);
  });

  test('deferOnConnectionError returns the gate for connection errors', () {
    final rule = deferOnConnectionError('network');
    expect(rule(const SocketException('x'), StackTrace.current), 'network');
    expect(rule(StateError('x'), StackTrace.current), isNull);
    final noTimeouts = deferOnConnectionError('net', includeTimeouts: false);
    expect(noTimeouts(TimeoutException('x'), StackTrace.current), isNull);
    expect(() => deferOnConnectionError(''), throwsArgumentError);
  });
}
