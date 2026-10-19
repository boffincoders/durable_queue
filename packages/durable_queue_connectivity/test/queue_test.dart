import 'dart:async';
import 'dart:io';

import 'package:durable_queue/durable_queue.dart';
import 'package:durable_queue_connectivity/durable_queue_connectivity.dart';
import 'package:test/test.dart';

final class Upload extends DurableTask {
  Upload(this.name);

  final String name;

  @override
  String get type => 'upload';

  @override
  Map<String, dynamic> toJson() => {'name': name};

  static Upload fromJson(Map<String, dynamic> json) =>
      Upload(json['name'] as String);
}

Future<void> until(bool Function() condition) async {
  for (var i = 0; i < 100 && !condition(); i++) {
    await Future<void>.delayed(Duration.zero);
  }
  expect(condition(), isTrue);
}

void main() {
  test('offline, captive portal, and back online', () async {
    final clock = FakeQueueClock(DateTime.utc(2026, 10, 19, 9));
    final network = StreamController<bool>();
    var up = false;
    var reachable = true;
    final gate = NetworkGate(
      networkChanges: network.stream,
      checkNetwork: () async => up,
      probe: () async => reachable,
      clock: clock,
    );
    await gate.start();

    final queue = DurableQueue(storage: MemoryQueueStorage(), clock: clock);
    final events = <QueueEvent>[];
    queue.events.listen(events.add);
    queue.addGate('network', gate);

    final sent = <String>[];
    var failNext = false;
    queue.register<Upload>(
      type: 'upload',
      decoder: Upload.fromJson,
      requires: {'network'},
      deferOn: deferOnConnectionError('network'),
      handler: (task, context) async {
        if (failNext) {
          failNext = false;
          // Wi-Fi is up but the internet is not: the request fails.
          reachable = false;
          throw const SocketException('Connection failed');
        }
        sent.add('${task.name}#${context.attempt}');
      },
    );

    // 1. Offline: the upload waits.
    await queue.enqueue(Upload('a'));
    await queue.start();
    await until(() => events.whereType<TaskDeferred>().isNotEmpty);
    expect(
      events.whereType<TaskDeferred>().single.reason,
      DeferralReason.gateClosed,
    );
    expect(sent, isEmpty);

    // 2. Online: it is sent at once.
    up = true;
    network.add(true);
    await until(() => sent.length == 1);
    expect(sent, ['a#1']);

    // 3. Captive portal: Wi-Fi up, request fails, probe closes the gate.
    failNext = true;
    await queue.enqueue(Upload('b'));
    await until(() => !gate.isOpen);
    final deferred = events.whereType<TaskDeferred>().last;
    expect(deferred.reason, DeferralReason.gateError);
    expect(sent, ['a#1']);

    // 4. Internet returns: the next probe opens the gate and b is sent,
    //    still on its first attempt.
    reachable = true;
    clock.advance(gate.probeRetryInterval);
    await until(() => sent.length == 2);
    expect(sent, ['a#1', 'b#1']);
    expect(events.whereType<TaskFailed>(), isEmpty);

    await queue.close();
    await gate.dispose();
  });
}
