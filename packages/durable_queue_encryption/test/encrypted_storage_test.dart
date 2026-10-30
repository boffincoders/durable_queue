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
    expect(
      storage.get('b'),
      throwsA(
        isA<PayloadDecryptionException>().having(
          (e) => e.taskId,
          'taskId',
          'b',
        ),
      ),
    );
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
    expect(storage.get('a'), throwsA(isA<PayloadDecryptionException>()));
  });

  test('a malformed record or unknown version does not decrypt', () async {
    await storage.save(_secretTask('a'));
    final record = (await inner.get('a'))!;
    await inner.update(
      record.copyWith(payload: {...record.payload, 'data': 7}),
    );
    expect(storage.get('a'), throwsA(isA<PayloadDecryptionException>()));
    await inner.update(
      record.copyWith(
        payload: {...record.payload, EncryptedQueueStorage.envelopeKey: 2},
      ),
    );
    expect(storage.get('a'), throwsA(isA<PayloadDecryptionException>()));
  });

  test('the wrong key does not decrypt; unreadable tasks can be found and '
      'deleted', () async {
    await storage.save(_secretTask('a'));
    await inner.save(storedTask(id: 'plain-ok'));
    final other = EncryptedQueueStorage(inner, cipher: AesGcmCipher(_key2));
    await other.save(storedTask(id: 'b'));

    final error = await other
        .get('a')
        .then<Object?>((_) => null, onError: (Object e) => e);
    expect(error, isA<PayloadDecryptionException>());
    expect('$error', contains('task a'));
    expect('$error', contains('key k1'));

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
    expect(strict.get('plain'), throwsA(isA<PayloadDecryptionException>()));
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
