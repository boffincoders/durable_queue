import 'dart:convert';
import 'dart:io';
import 'dart:isolate';

import 'package:durable_queue_workmanager/durable_queue_workmanager.dart';
import 'package:flutter_test/flutter_test.dart';

void main() {
  late Directory dir;
  late String path;
  late DateTime now;

  FileQueueLease lease({Duration ttl = const Duration(minutes: 2)}) =>
      FileQueueLease(path, ttl: ttl, now: () => now);

  Map<String, Object?> stored() =>
      jsonDecode(File(path).readAsStringSync()) as Map<String, Object?>;

  setUp(() {
    dir = Directory.systemTemp.createTempSync('dq_lease_');
    path = '${dir.path}/queue.lease';
    now = DateTime.utc(2026, 10, 22, 12);
  });

  tearDown(() => dir.deleteSync(recursive: true));

  test('one holder at a time; release frees it', () async {
    final a = lease();
    final b = lease();
    expect(await a.tryAcquire(), isTrue);
    expect(await b.tryAcquire(), isFalse);
    expect(await a.tryAcquire(), isTrue);
    await b.release();
    expect(File(path).existsSync(), isTrue);
    await a.release();
    expect(File(path).existsSync(), isFalse);
    expect(await b.tryAcquire(), isTrue);
  });

  test('the file names the holder and the expiry', () async {
    final a = lease();
    await a.tryAcquire();
    final first = stored();
    expect(
      first['expiresAt'],
      now.add(const Duration(minutes: 2)).millisecondsSinceEpoch,
    );
    now = now.add(const Duration(minutes: 1));
    expect(await a.renew(), isTrue);
    final second = stored();
    expect(second['owner'], first['owner']);
    expect(
      second['expiresAt'],
      now.add(const Duration(minutes: 2)).millisecondsSinceEpoch,
    );
    expect(dir.listSync().length, 1, reason: 'no temp file left behind');
  });

  test('acquiring again renews', () async {
    final a = lease();
    await a.tryAcquire();
    now = now.add(const Duration(minutes: 1));
    expect(await a.tryAcquire(), isTrue);
    expect(
      stored()['expiresAt'],
      now.add(const Duration(minutes: 2)).millisecondsSinceEpoch,
    );
  });

  test('a renewed lease is not taken over', () async {
    final a = lease();
    final b = lease();
    await a.tryAcquire();
    now = now.add(const Duration(seconds: 90));
    await a.renew();
    now = now.add(const Duration(seconds: 90));
    expect(await b.tryAcquire(), isFalse);
  });

  test('an expired lease is taken over and the old holder loses it', () async {
    final a = lease();
    final b = lease();
    await a.tryAcquire();
    now = now.add(const Duration(minutes: 2));
    expect(await b.tryAcquire(), isTrue);
    expect(await a.renew(), isFalse);
    expect(await a.tryAcquire(), isFalse);
    await a.release();
    expect(
      await b.renew(),
      isTrue,
      reason: 'release by the old holder is ignored',
    );
    expect(File('$path.takeover').existsSync(), isFalse);
  });

  test('a holder that let the lease expire has lost it', () async {
    final a = lease();
    final b = lease();
    await a.tryAcquire();
    now = now.add(const Duration(minutes: 3));
    expect(await a.renew(), isFalse);
    await a.release();
    expect(
      File(path).existsSync(),
      isTrue,
      reason: 'expired leases are left for a takeover',
    );
    expect(await b.tryAcquire(), isTrue);
  });

  test('a file that cannot be read is held until it is ttl old', () async {
    File(path).writeAsStringSync('');
    File(path).setLastModifiedSync(now.subtract(const Duration(seconds: 30)));
    final b = lease();
    expect(await b.tryAcquire(), isFalse);
    File(path).setLastModifiedSync(now.subtract(const Duration(minutes: 2)));
    expect(await b.tryAcquire(), isTrue);
    expect(await b.renew(), isTrue);
  });

  for (final content in [
    '{"owner":1,"expiresAt":${DateTime.utc(2030).millisecondsSinceEpoch}}',
    '{"owner": 1}',
    '[1]',
    'not json',
    '{"owner":"x","expiresAt":"soon"}',
  ]) {
    test('content $content counts as unreadable', () async {
      File(path).writeAsStringSync(content);
      File(path).setLastModifiedSync(now);
      expect(await lease().tryAcquire(), isFalse);
    });
  }

  test('a takeover in progress blocks another one', () async {
    final a = lease();
    await a.tryAcquire();
    now = now.add(const Duration(minutes: 5));
    final guard = File('$path.takeover')..writeAsStringSync('');
    guard.setLastModifiedSync(now.subtract(const Duration(seconds: 1)));
    final b = lease();
    expect(await b.tryAcquire(), isFalse);
    expect(guard.existsSync(), isTrue);
  });

  test('an abandoned takeover guard is cleared', () async {
    final a = lease();
    await a.tryAcquire();
    now = now.add(const Duration(minutes: 5));
    final guard = File('$path.takeover')..writeAsStringSync('');
    guard.setLastModifiedSync(now.subtract(const Duration(minutes: 2)));
    final b = lease();
    expect(await b.tryAcquire(), isFalse);
    expect(guard.existsSync(), isFalse);
    expect(await b.tryAcquire(), isTrue);
  });

  test('missing directories are created', () async {
    path = '${dir.path}/a/b/queue.lease';
    expect(await lease().tryAcquire(), isTrue);
  });

  test('ttl must be positive', () {
    expect(() => FileQueueLease(path, ttl: Duration.zero), throwsArgumentError);
  });

  test('isolates racing for a free lease: one wins', () async {
    final target = path;
    final results = await Future.wait([
      for (var i = 0; i < 8; i++)
        Isolate.run(() => FileQueueLease(target).tryAcquire()),
    ]);
    expect(results.where((won) => won).length, 1);
  });

  test('isolates racing for an expired lease: one wins', () async {
    final target = path;
    for (var round = 0; round < 5; round++) {
      File(target)
          .writeAsStringSync(jsonEncode({'owner': 'gone', 'expiresAt': 0}));
      final results = await Future.wait([
        for (var i = 0; i < 8; i++)
          Isolate.run(() => FileQueueLease(target).tryAcquire()),
      ]);
      expect(results.where((won) => won).length, 1);
      final owner =
          (jsonDecode(File(target).readAsStringSync()) as Map)['owner'];
      expect(owner, isNot('gone'));
      expect(File('$target.takeover').existsSync(), isFalse);
      File(target).deleteSync();
    }
  });
}
