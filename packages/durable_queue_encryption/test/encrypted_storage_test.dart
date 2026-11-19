import 'dart:convert';

import 'package:durable_queue/durable_queue.dart';
import 'package:durable_queue_encryption/durable_queue_encryption.dart';
import 'package:durable_queue_test_support/durable_queue_test_support.dart';
import 'package:test/test.dart';

final _key1 = List<int>.generate(32, (i) => i);
final _key2 = List<int>.generate(32, (i) => 100 + i);

final _failure = TaskFailure(
  error: 'token=abc123 rejected',
  stackTrace: '#0 secretFunction (secret.dart:1)',
  failedAt: DateTime.utc(2026, 1, 1),
  attempt: 1,
);

StoredTask _secretTask(String id, {DateTime? createdAt}) => storedTask(
  id: id,
  payload: {
    'card': '4111-secret',
    'nested': {
      'list': [1, 'two', null],
    },
  },
  deduplicationKey: 'user@example.com',
  group: 'private-group',
  lastFailure: _failure,
  status: TaskStatus.retryScheduled,
  createdAt: createdAt,
);

const _secrets = [
  '4111-secret',
  'user@example.com',
  'private-group',
  'abc123',
  'secretFunction',
];

void main() {
  test('passes the call guard on to the wrapped storage', () {
    final guarded = _GuardedStorage();
    final wrapper = EncryptedQueueStorage(guarded, cipher: AesGcmCipher(_key1));
    wrapper.checkCall();
    guarded.refuse = true;
    expect(wrapper.checkCall, throwsStateError);
    // A storage without a guard needs no check.
    final plain = EncryptedQueueStorage(
      MemoryQueueStorage(),
      cipher: AesGcmCipher(_key1),
    );
    plain.checkCall();
  });

  late MemoryQueueStorage inner;
  late EncryptedQueueStorage storage;

  setUp(() {
    inner = MemoryQueueStorage();
    storage = EncryptedQueueStorage(inner, cipher: AesGcmCipher(_key1));
  });

  Future<String> rawJson(String id) async =>
      jsonEncode((await inner.get(id))!.toJson());

  test('the wrapped storage holds none of the protected values', () async {
    await storage.save(_secretTask('a'));
    final raw = await rawJson('a');
    for (final secret in _secrets) {
      expect(raw.contains(secret), isFalse, reason: secret);
    }
    final record = (await inner.get('a'))!;
    expect(record.payload.keys.toSet(), {
      EncryptedQueueStorage.envelopeKey,
      'key',
      'data',
    });
    expect(record.payload['key'], 'k1');
    expect(record.lastFailure!.error, EncryptedQueueStorage.redactedError);
    expect(record.lastFailure!.stackTrace, isNull);
    expect(record.lastFailure!.attempt, 1);
    expect(record.status, TaskStatus.retryScheduled);
  });

  test('the failure history is encrypted too', () async {
    final earlier = TaskFailure(
      error: 'password=hunter2 rejected',
      stackTrace: '#0 loginSecretly (auth.dart:9)',
      failedAt: DateTime.utc(2025, 12, 31),
      attempt: 1,
    );
    final task = _secretTask('h').copyWith(failures: [earlier, _failure]);
    await storage.save(task);
    final raw = await rawJson('h');
    for (final secret in ['hunter2', 'loginSecretly', 'token=abc123']) {
      expect(raw.contains(secret), isFalse, reason: secret);
    }
    final record = (await inner.get('h'))!;
    expect(record.failures.map((f) => f.error), [
      EncryptedQueueStorage.redactedError,
      EncryptedQueueStorage.redactedError,
    ]);
    expect(record.failures.first.attempt, 1);

    final read = (await storage.get('h'))!;
    expect(read.failures.map((f) => f.error), [
      'password=hunter2 rejected',
      _failure.error,
    ]);
    expect(read.failures.first.stackTrace, '#0 loginSecretly (auth.dart:9)');
    expect(read.failures.first.failedAt, DateTime.utc(2025, 12, 31));
  });

  test('reading returns the original task', () async {
    final task = _secretTask('a');
    await storage.save(task);
    expect((await storage.get('a'))!.toJson(), task.toJson());
    expect((await storage.getAll()).single.toJson(), task.toJson());
  });

  test('update re-encrypts with a new nonce', () async {
    final task = _secretTask('a');
    await storage.save(task);
    final first = (await inner.get('a'))!.payload['data'];
    await storage.update(task);
    final second = (await inner.get('a'))!.payload['data'];
    expect(second, isNot(first));
    expect((await storage.get('a'))!.toJson(), task.toJson());
  });

  test('lookups find tasks by their original values', () async {
    await storage.save(_secretTask('a'));
    expect(
      (await storage.findActiveByDeduplicationKey('user@example.com'))?.id,
      'a',
    );
    expect(await storage.findActiveByDeduplicationKey('other'), isNull);
    final grouped = await storage.getByGroup('private-group');
    expect(grouped.single.payload['card'], '4111-secret');
    expect(grouped.single.group, 'private-group');
  });

  test('a record copied onto another task does not decrypt', () async {
    await storage.save(_secretTask('a'));
    await storage.save(storedTask(id: 'b'));
    final a = (await inner.get('a'))!;
    final b = (await inner.get('b'))!;
    await inner.update(b.copyWith(payload: a.payload));
    expect(await storage.get('b'), isNull);
    expect(await storage.unreadableTaskIds(), ['b']);
  });

  test('a changed record does not decrypt', () async {
    await storage.save(_secretTask('a'));
    final record = (await inner.get('a'))!;
    final data = base64.decode(record.payload['data'] as String);
    data[data.length ~/ 2] ^= 1;
    await inner.update(
      record.copyWith(
        payload: {...record.payload, 'data': base64.encode(data)},
      ),
    );
    expect(await storage.get('a'), isNull);
    expect(await storage.unreadableTaskIds(), ['a']);
  });

  test('a malformed record or unknown version does not decrypt', () async {
    await storage.save(_secretTask('a'));
    final record = (await inner.get('a'))!;
    await inner.update(
      record.copyWith(payload: {...record.payload, 'data': 7}),
    );
    expect(await storage.get('a'), isNull);
    await inner.update(
      record.copyWith(
        payload: {...record.payload, EncryptedQueueStorage.envelopeKey: 2},
      ),
    );
    expect(await storage.get('a'), isNull);
    expect(await storage.unreadableTaskIds(), ['a']);
  });

  test('the wrong key does not decrypt; unreadable tasks can be found and '
      'deleted', () async {
    await storage.save(_secretTask('a'));
    await inner.save(storedTask(id: 'plain-ok'));
    final other = EncryptedQueueStorage(inner, cipher: AesGcmCipher(_key2));
    await other.save(storedTask(id: 'b'));

    expect(await other.get('a'), isNull);
    expect((await other.getAll()).map((t) => t.id), ['b', 'plain-ok']);
    expect(await other.unreadableTaskIds(), ['a']);
    await other.delete('a');
    expect((await other.getAll()).map((t) => t.id), ['b', 'plain-ok']);
  });

  test('a new key reads tasks written with a previous key', () async {
    final earlier = DateTime.utc(2026, 1, 1);
    final later = DateTime.utc(2026, 1, 2);
    await storage.save(_secretTask('old', createdAt: earlier));

    final rotated = EncryptedQueueStorage(
      inner,
      cipher: AesGcmCipher(_key2, keyId: 'k2', previousKeys: {'k1': _key1}),
    );
    await rotated.save(_secretTask('new', createdAt: later));

    expect((await inner.get('new'))!.payload['key'], 'k2');
    expect((await rotated.get('old'))!.payload['card'], '4111-secret');
    expect(
      (await rotated.findActiveByDeduplicationKey('user@example.com'))?.id,
      'old',
    );
    expect((await rotated.getByGroup('private-group')).map((t) => t.id), [
      'old',
      'new',
    ]);

    expect(await rotated.reencryptAll(), 2);
    expect((await inner.get('old'))!.payload['key'], 'k2');

    final onlyNew = EncryptedQueueStorage(
      inner,
      cipher: AesGcmCipher(_key2, keyId: 'k2'),
    );
    expect(await onlyNew.unreadableTaskIds(), isEmpty);
    expect(
      (await onlyNew.findActiveByDeduplicationKey('user@example.com'))?.id,
      'old',
    );
    expect((await onlyNew.getByGroup('private-group')).length, 2);
  });

  test('tasks stored before encryption are read and then encrypted', () async {
    await inner.save(_secretTask('plain'));

    expect((await storage.get('plain'))!.payload['card'], '4111-secret');
    expect(
      (await storage.findActiveByDeduplicationKey('user@example.com'))?.id,
      'plain',
    );
    expect((await storage.getByGroup('private-group')).map((t) => t.id), [
      'plain',
    ]);

    final strict = EncryptedQueueStorage(
      inner,
      cipher: AesGcmCipher(_key1),
      readPlaintext: false,
    );
    expect(await strict.get('plain'), isNull);
    expect(
      await strict.findActiveByDeduplicationKey('user@example.com'),
      isNull,
    );
    expect(await strict.unreadableTaskIds(), ['plain']);

    expect(await storage.reencryptAll(), 1);
    final raw = await rawJson('plain');
    for (final secret in _secrets) {
      expect(raw.contains(secret), isFalse, reason: secret);
    }
    expect(
      (await strict.get('plain'))!.toJson(),
      _secretTask('plain').toJson(),
    );
  });

  test('with protectLookupKeys off, only the payload and failure are '
      'encrypted', () async {
    final readable = EncryptedQueueStorage(
      inner,
      cipher: AesGcmCipher(_key1),
      protectLookupKeys: false,
    );
    final task = _secretTask('a');
    await readable.save(task);
    final record = (await inner.get('a'))!;
    expect(record.deduplicationKey, 'user@example.com');
    expect(record.group, 'private-group');
    final raw = await rawJson('a');
    expect(raw.contains('4111-secret'), isFalse);
    expect(raw.contains('abc123'), isFalse);
    expect((await readable.get('a'))!.toJson(), task.toJson());

    // A storage that hides lookup keys still reads these records.
    expect((await storage.get('a'))!.toJson(), task.toJson());
  });

  test('a payload with values JSON cannot encode is refused', () async {
    final task = storedTask(id: 'a', payload: {'when': DateTime(2026)});
    expect(storage.save(task), throwsArgumentError);
    expect(await inner.get('a'), isNull);
  });

  test('a queue runs on encrypted storage', () async {
    final queue = DurableQueue(storage: storage);
    final seen = <String>[];
    var attempt = 0;
    queue.register<_Note>(
      type: _Note.taskType,
      decoder: (json) => _Note(json['text'] as String),
      handler: (task, context) async {
        attempt++;
        if (attempt == 1) throw StateError('secret-failure ${task.text}');
        seen.add(task.text);
      },
    );
    final id = await queue.enqueue(
      const _Note('hello secret'),
      deduplicationKey: 'note-user@example.com',
      group: 'notes',
      retryPolicy: RetryPolicy.none(),
    );
    expect(
      await queue.enqueue(
        const _Note('again'),
        deduplicationKey: 'note-user@example.com',
      ),
      id,
    );
    final raw = await rawJson(id);
    expect(raw.contains('hello secret'), isFalse);
    expect(raw.contains('note-user@example.com'), isFalse);
    expect(raw.contains('notes'), isFalse);

    // The first attempt fails; its error is readable through the queue only.
    await queue.drain();
    final failed = (await queue.getTask(id))!;
    expect(failed.status, TaskStatus.failed);
    expect(failed.lastFailure?.error, contains('secret-failure hello secret'));
    expect((await rawJson(id)).contains('secret-failure'), isFalse);

    await queue.retryFailed();
    await queue.drain();
    expect(seen, ['hello secret']);
    expect((await queue.getTask(id))!.status, TaskStatus.completed);
    expect((await queue.getTasks(group: 'notes')).single.id, id);
    await queue.close();
  });

  test(
    'queue state is encrypted, and unreadable state reads as none',
    () async {
      const state = '{"version":1,"shared":{"api.example.com":1}}';
      await storage.writeState('rate_limits', state);
      final raw = (await inner.readState('rate_limits'))!;
      expect(raw.contains('api.example.com'), isFalse);
      expect(await storage.readState('rate_limits'), state);

      final other = EncryptedQueueStorage(inner, cipher: AesGcmCipher(_key2));
      expect(await other.readState('rate_limits'), isNull);

      await storage.writeState('rate_limits', null);
      expect(await inner.readState('rate_limits'), isNull);
    },
  );

  group('a task that cannot be decrypted', () {
    DurableQueue noteQueue(QueueStorage on, List<String> seen) {
      return DurableQueue(storage: on)..register<_Note>(
        type: _Note.taskType,
        decoder: (json) => _Note(json['text'] as String),
        handler: (task, context) async => seen.add(task.text),
      );
    }

    Future<void> corrupt(String id) async {
      final record = (await inner.get(id))!;
      final data = base64.decode(record.payload['data'] as String);
      data[data.length ~/ 2] ^= 1;
      await inner.update(
        record.copyWith(
          payload: {...record.payload, 'data': base64.encode(data)},
        ),
      );
    }

    test('is set aside, and the queue runs the tasks behind it', () async {
      final seen = <String>[];
      final queue = noteQueue(storage, seen);
      final setAside = <PayloadDecryptionException>[];
      final subscription = storage.tasksSetAside.listen(setAside.add);
      final storageErrors = <QueueStorageFailure>[];
      queue.storageErrors.listen(storageErrors.add);

      final bad = await queue.enqueue(const _Note('first'));
      await queue.enqueue(const _Note('second'));
      await queue.enqueue(const _Note('third'));
      await corrupt(bad);

      final result = await queue.drain().timeout(const Duration(seconds: 5));
      expect(result.completed, 2);
      expect(seen, ['second', 'third']);
      expect(storageErrors, isEmpty);
      expect(setAside.map((e) => e.taskId), [bad]);
      expect('${setAside.single}', contains('task $bad'));

      // Its data is kept, held so that it is not selected again.
      final raw = (await inner.get(bad))!;
      expect(raw.held, isTrue);
      expect(raw.status, TaskStatus.pending);
      expect(raw.nextAttemptAt, StoredTask.heldNextAttemptAt);
      expect(await storage.getNextReady(DateTime.utc(3000)), isNull);

      // The queue does not list it.
      expect(await queue.getTask(bad), isNull);
      expect((await queue.getTasks()).length, 2);
      expect(await storage.unreadableTaskIds(), [bad]);
      expect(await storage.recoveredTaskIds(), isEmpty);

      await subscription.cancel();
      await queue.close();
    });

    test('runs once its key is restored and it is released', () async {
      final lostKey = EncryptedQueueStorage(inner, cipher: AesGcmCipher(_key2));
      final seen = <String>[];
      final writer = noteQueue(storage, seen);
      final id = await writer.enqueue(
        const _Note('kept'),
        delay: Duration.zero,
      );
      await writer.close();

      // Opened with the wrong key: set aside, nothing runs.
      final withoutKey = noteQueue(lostKey, seen);
      await withoutKey.drain().timeout(const Duration(seconds: 5));
      expect(seen, isEmpty);
      expect(await lostKey.unreadableTaskIds(), [id]);
      expect(await lostKey.recoveredTaskIds(), isEmpty);
      await withoutKey.close();

      // The right key again: the task can be read and released.
      final restored = EncryptedQueueStorage(
        inner,
        cipher: AesGcmCipher(_key1),
      );
      expect(await restored.unreadableTaskIds(), isEmpty);
      expect(await restored.recoveredTaskIds(), [id]);
      final task = (await restored.get(id))!;
      expect(task.held, isTrue);
      expect(task.payload, {'text': 'kept'});

      final queue = noteQueue(restored, seen);
      expect(await queue.release(id), isTrue);
      await queue.drain();
      expect(seen, ['kept']);
      expect((await queue.getTask(id))!.status, TaskStatus.completed);
      expect(await restored.recoveredTaskIds(), isEmpty);
      expect(
        jsonEncode((await inner.get(id))!.toJson()),
        isNot(contains('set_aside')),
      );
      await queue.close();
    });

    test('does not stop a task it depends on from finishing', () async {
      final seen = <String>[];
      final queue = noteQueue(storage, seen);
      final first = await queue.enqueue(const _Note('first'));
      final dependent = await queue.enqueue(
        const _Note('dependent'),
        dependsOn: [first],
      );
      final other = await queue.enqueue(const _Note('other'));
      await corrupt(dependent);

      await queue.drain().timeout(const Duration(seconds: 5));
      expect(seen, ['first', 'other']);
      expect((await queue.getTask(first))!.status, TaskStatus.completed);
      expect((await queue.getTask(other))!.status, TaskStatus.completed);
      expect(await storage.unreadableTaskIds(), [dependent]);
      await queue.close();
    });

    test('does not use up a limited status query', () async {
      final created = DateTime.utc(2026, 1, 1);
      for (var i = 0; i < 5; i++) {
        await storage.save(
          storedTask(
            id: 't$i',
            status: TaskStatus.running,
            attempts: 1,
            createdAt: created.add(Duration(seconds: i)),
            sequence: i,
          ),
        );
      }
      await corrupt('t0');
      await corrupt('t1');
      expect(
        (await storage.getByStatus(
          TaskStatus.running,
          limit: 2,
        )).map((t) => t.id),
        ['t2', 't3'],
      );
      expect(
        (await storage.getByStatus(
          TaskStatus.running,
          limit: 10,
        )).map((t) => t.id),
        ['t2', 't3', 't4'],
      );
    });

    test('is skipped by deduplication lookups', () async {
      await storage.save(
        storedTask(
          id: 'a',
          deduplicationKey: 'same',
          status: TaskStatus.pending,
        ),
      );
      await corrupt('a');
      expect(await storage.findActiveByDeduplicationKey('same'), isNull);
    });

    test('an unencrypted task is set aside and read again without the '
        'marker', () async {
      await inner.save(storedTask(id: 'plain', payload: {'v': 1}));
      final strict = EncryptedQueueStorage(
        inner,
        cipher: AesGcmCipher(_key1),
        readPlaintext: false,
      );
      expect(await strict.getNextReady(DateTime.utc(2027)), isNull);
      expect((await inner.get('plain'))!.held, isTrue);

      expect(await storage.recoveredTaskIds(), ['plain']);
      expect((await storage.get('plain'))!.payload, {'v': 1});
    });
  });
}

final class _Note extends DurableTask {
  const _Note(this.text);

  static const taskType = 'note';

  final String text;

  @override
  String get type => taskType;

  @override
  Map<String, dynamic> toJson() => {'text': text};
}

/// A storage that can refuse calls, like durable_queue_drift.
final class _GuardedStorage implements QueueStorage, QueueStorageCallGuard {
  final _inner = MemoryQueueStorage();
  var refuse = false;

  @override
  void checkCall() {
    if (refuse) throw StateError('refused');
  }

  @override
  dynamic noSuchMethod(Invocation invocation) => super.noSuchMethod(invocation);

  @override
  Future<StoredTask?> get(String id) => _inner.get(id);
}
