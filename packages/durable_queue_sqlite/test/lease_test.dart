@TestOn('vm')
library;

import 'dart:io';
import 'dart:isolate';

import 'package:durable_queue/durable_queue.dart';
import 'package:durable_queue_sqlite/durable_queue_sqlite.dart';
import 'package:test/test.dart';

void main() {
  late Directory dir;
  late String path;
  final open = <SqliteQueueLease>[];

  setUp(() {
    dir = Directory.systemTemp.createTempSync('dq_lease_');
    path = '${dir.path}/queue.db';
  });

  tearDown(() {
    for (final lease in open) {
      lease.close();
    }
    open.clear();
    dir.deleteSync(recursive: true);
  });

  SqliteQueueLease lease({DateTime Function()? now}) {
    final created = SqliteQueueLease.open(
      path,
      ttl: const Duration(minutes: 2),
      now: now,
    );
    open.add(created);
    return created;
  }

  test('one holder at a time', () async {
    final a = lease();
    final b = lease();
    expect(await a.tryAcquire(), isTrue);
    expect(await a.tryAcquire(), isTrue, reason: 'already held');
    expect(await b.tryAcquire(), isFalse);
    expect(await b.renew(), isFalse);
    expect(await a.renew(), isTrue);

    await b.release();
    expect(await b.tryAcquire(), isFalse, reason: 'b could not release a');

    await a.release();
    expect(await b.tryAcquire(), isTrue);
    expect(await a.tryAcquire(), isFalse);
  });

  test('an abandoned lease is taken over, and its holder lost it', () async {
    var now = DateTime(2026, 1, 1);
    final a = lease(now: () => now);
    final b = lease(now: () => now);
    expect(await a.tryAcquire(), isTrue);

    now = now.add(const Duration(minutes: 1));
    expect(await b.tryAcquire(), isFalse);
    expect(await a.renew(), isTrue);

    now = now.add(const Duration(minutes: 2));
    expect(await b.tryAcquire(), isTrue);
    expect(await a.renew(), isFalse);
    await a.release();
    expect(await b.renew(), isTrue, reason: "a's release left b's lease");
  });

  test('only one of several isolates racing for it gets it', () async {
    lease(); // Creates the table before the race.
    final results = await Future.wait([
      for (var i = 0; i < 8; i++) Isolate.run(_tryAcquireIn(path)),
    ]);
    expect(results.where((won) => won), hasLength(1));
  });

  test('another isolate sees the lease held', () async {
    final a = lease();
    expect(await a.tryAcquire(), isTrue);
    final other = await Isolate.run(_tryAcquireIn(path));
    expect(other, isFalse);
  });

  test('lives next to the tasks without touching them', () async {
    final storage = SqliteQueueStorage.open(path);
    addTearDown(storage.close);
    final queue = DurableQueue(storage: storage)
      ..register<_Task>(
        type: 'task',
        decoder: (_) => _Task(),
        handler: (task, context) async {},
      );
    await queue.enqueue(_Task());

    final a = lease();
    expect(await a.tryAcquire(), isTrue);
    expect(await queue.getTasks(), hasLength(1));
    expect(await storage.readState('lease'), isNull);
    expect((await queue.drain()).completed, 1);
    await queue.close();
  });

  test('rejects a bad ttl or prefix, and use after close', () async {
    expect(
      () => SqliteQueueLease.open(path, ttl: Duration.zero),
      throwsArgumentError,
    );
    expect(
      () => SqliteQueueLease.open(path, tablePrefix: 'no-dashes'),
      throwsArgumentError,
    );
    final a = SqliteQueueLease.open(path)..close();
    expect(a.tryAcquire, throwsStateError);
  });
}

/// Takes the lease from another isolate. Top level, so the closure it
/// returns captures only [path].
Future<bool> Function() _tryAcquireIn(String path) => () async {
  final lease = SqliteQueueLease.open(path);
  try {
    return await lease.tryAcquire();
  } finally {
    lease.close();
  }
};

final class _Task extends DurableTask {
  @override
  String get type => 'task';

  @override
  Map<String, dynamic> toJson() => {};
}
