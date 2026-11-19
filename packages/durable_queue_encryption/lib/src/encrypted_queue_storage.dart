/// @docImport 'aes_gcm_cipher.dart';
library;

import 'dart:async';
import 'dart:convert';

import 'package:durable_queue/durable_queue.dart';

import 'payload_cipher.dart';

/// A [QueueStorage] that encrypts what tasks contain before another storage
/// writes it.
///
/// Wrap the storage you already use:
///
/// ```dart
/// final storage = EncryptedQueueStorage(
///   SqliteQueueStorage.open(path),
///   cipher: AesGcmCipher(key),
/// );
/// final queue = DurableQueue(storage: storage);
/// ```
///
/// Encrypted, with the task id as associated data:
///
/// * the payload;
/// * the error message and stack trace of the last failure;
/// * the deduplication key and group, when [protectLookupKeys] is true. The
///   wrapped storage then holds a keyed hash of each, used for lookups, and
///   the original value comes back when a task is read.
///
/// Not encrypted, because the queue queries or orders by them: the id, type,
/// status, attempt counts, times, priority, dependencies, idempotency key,
/// and hand-off reference. Keep secrets out of those.
///
/// ## Tasks that cannot be decrypted
///
/// A task can become unreadable, for example after its key was lost or its
/// record was changed. Such a task is left out, so the queue keeps running
/// the tasks it can read:
///
/// * [get] returns null for it, and the lists ([getAll], [getByStatus],
///   [getByGroup], ...) and [findActiveByDeduplicationKey] skip it.
/// * When it is next in line to start, [getNextReady] sets it aside: it is
///   held, as by `DurableQueue.hold`, and [tasksSetAside] reports it. Its
///   encrypted data is kept unchanged.
///
/// [unreadableTaskIds] lists the tasks that cannot be read now. Delete them
/// if their key is gone for good. If you restore the key, the tasks become
/// readable again, and [recoveredTaskIds] lists the ones that were set aside
/// so you can release them with `DurableQueue.release`.
///
/// Every queue or connection that opens the same storage must use this
/// wrapper with the same keys.
final class EncryptedQueueStorage
    implements QueueStorage, QueueStorageCallGuard, QueueStateStorage {
  /// Wraps [inner], encrypting with [cipher].
  ///
  /// When [readPlaintext] is true, tasks stored before encryption was added
  /// are read as they are, and lookups also find their plain deduplication
  /// keys and groups. Every write is encrypted; [reencryptAll] encrypts the
  /// remaining old tasks at once. Set it to false to treat unencrypted
  /// records as unreadable.
  EncryptedQueueStorage(
    QueueStorage inner, {
    required PayloadCipher cipher,
    bool protectLookupKeys = true,
    bool readPlaintext = true,
  }) : _inner = inner,
       _cipher = cipher,
       _protectLookupKeys = protectLookupKeys,
       _readPlaintext = readPlaintext;

  /// The payload key that marks an encrypted record.
  static const envelopeKey = 'durable_queue_encryption';

  /// Stored in place of the error message of an encrypted failure.
  static const redactedError = '[encrypted]';

  static const _version = 1;
  static const _tagPrefix = 'dqe1:';

  /// Added to the stored payload of a task set aside by [getNextReady].
  /// Removed when the task is read, so the next write drops it.
  static const _setAsideKey = 'durable_queue_encryption_set_aside';

  final QueueStorage _inner;
  final PayloadCipher _cipher;
  final bool _protectLookupKeys;
  final bool _readPlaintext;
  final StreamController<PayloadDecryptionException> _setAside =
      StreamController<PayloadDecryptionException>.broadcast();

  /// Reports each task that [getNextReady] set aside because it could not be
  /// decrypted, with the reason.
  Stream<PayloadDecryptionException> get tasksSetAside => _setAside.stream;

  /// The wrapped storage. Records read from it directly are encrypted.
  QueueStorage get inner => _inner;

  /// Passes the check on to [inner] when it has one, for example
  /// `durable_queue_drift`'s refusal of calls inside a drift transaction.
  @override
  void checkCall() {
    if (_inner case final QueueStorageCallGuard guard) guard.checkCall();
  }

  /// Reads queue state, such as rate limits, from [inner] and decrypts it.
  /// Returns null when [inner] keeps no state or the value cannot be
  /// decrypted, so the queue starts that state empty.
  @override
  Future<String?> readState(String key) async {
    if (_inner case final QueueStateStorage state) {
      final stored = await state.readState(key);
      if (stored == null) return null;
      try {
        final envelope = jsonDecode(stored);
        if (envelope is! Map || envelope[envelopeKey] != _version) {
          return _readPlaintext ? stored : null;
        }
        final plaintext = await _cipher.decrypt(
          base64.decode(envelope['data'] as String),
          keyId: envelope['key'] as String,
          associatedData: _stateAssociatedData(key),
        );
        return utf8.decode(plaintext);
      } on Object {
        return _readPlaintext && !stored.contains(envelopeKey) ? stored : null;
      }
    }
    return null;
  }

  /// Encrypts queue state, such as rate limits, and keeps it in [inner].
  /// Does nothing when [inner] keeps no state.
  @override
  Future<void> writeState(String key, String? value) async {
    if (_inner case final QueueStateStorage state) {
      if (value == null) return state.writeState(key, null);
      final data = await _cipher.encrypt(
        utf8.encode(value),
        associatedData: _stateAssociatedData(key),
      );
      await state.writeState(
        key,
        jsonEncode({
          envelopeKey: _version,
          'key': _cipher.keyId,
          'data': base64.encode(data),
        }),
      );
    }
  }

  @override
  Future<void> save(StoredTask task) async => _inner.save(await _seal(task));

  @override
  Future<void> update(StoredTask task) async =>
      _inner.update(await _seal(task));

  @override
  Future<void> delete(String id) => _inner.delete(id);

  /// Returns the task with [id], or null if it is not stored or cannot be
  /// decrypted. See [unreadableTaskIds].
  @override
  Future<StoredTask?> get(String id) async => _openOrNull(await _inner.get(id));

  @override
  Future<List<StoredTask>> getPending() async =>
      _readable(await _inner.getPending());

  @override
  Future<List<StoredTask>> getByStatus(TaskStatus status, {int? limit}) async {
    if (limit == null) {
      return _readable(await _inner.getByStatus(status));
    }
    // Skipped tasks must not use up the limit: ask for more until [limit]
    // tasks can be read or the wrapped storage has no more.
    var want = limit;
    while (true) {
      final stored = await _inner.getByStatus(status, limit: want);
      final readable = await _readable(stored);
      if (readable.length >= limit || stored.length < want) {
        return readable.length > limit ? readable.sublist(0, limit) : readable;
      }
      want = limit + stored.length - readable.length;
    }
  }

  /// Returns the next ready task that can be decrypted.
  ///
  /// A ready task that cannot be decrypted is set aside first: it is held,
  /// as by `DurableQueue.hold`, so it is not returned again, and
  /// [tasksSetAside] reports it.
  @override
  Future<StoredTask?> getNextReady(DateTime now) async {
    while (true) {
      final stored = await _inner.getNextReady(now);
      if (stored == null) return null;
      try {
        return await _open(stored);
      } on PayloadDecryptionException catch (error) {
        await _inner.update(
          stored.copyWith(
            payload: {...stored.payload, _setAsideKey: true},
            held: true,
            releaseAt: stored.nextAttemptAt,
            nextAttemptAt: StoredTask.heldNextAttemptAt,
            updatedAt: now,
          ),
        );
        if (!_setAside.isClosed) _setAside.add(error);
      }
    }
  }

  @override
  Future<DateTime?> getNextWakeAt() => _inner.getNextWakeAt();

  @override
  Future<int> getMaxSequence() => _inner.getMaxSequence();

  @override
  Future<List<StoredTask>> getAll() async => _readable(await _inner.getAll());

  @override
  Future<List<StoredTask>> getWaitingDependents(String id) async =>
      _readable(await _inner.getWaitingDependents(id));

  @override
  Future<StoredTask?> findActiveByDeduplicationKey(String key) async {
    final found = <StoredTask>[];
    for (final stored in _lookupValues(key)) {
      final task = await _inner.findActiveByDeduplicationKey(stored);
      if (task != null) found.add(task);
    }
    found.sort(_compare);
    final readable = await _readable(found);
    return readable.isEmpty ? null : readable.first;
  }

  @override
  Future<List<StoredTask>> getByGroup(
    String group, {
    TaskStatus? status,
  }) async {
    final byId = <String, StoredTask>{};
    for (final stored in _lookupValues(group)) {
      for (final task in await _inner.getByGroup(stored, status: status)) {
        byId[task.id] = task;
      }
    }
    final tasks = byId.values.toList()..sort(_compare);
    return _readable(tasks);
  }

  /// Rewrites every stored task with the cipher's current key and lookup
  /// tags, and encrypts tasks stored before encryption was added. Returns how
  /// many tasks were rewritten.
  ///
  /// Use it after changing keys or turning [protectLookupKeys] on or off.
  /// Call it before the queue starts, not while it runs.
  Future<int> reencryptAll() async {
    var count = 0;
    for (final stored in await _inner.getAll()) {
      await _inner.update(await _seal(await _open(stored)));
      count++;
    }
    return count;
  }

  /// Returns the ids of stored tasks that cannot be decrypted, for example
  /// after the key was lost. The queue leaves them out; delete them with
  /// [delete] if they cannot be recovered.
  Future<List<String>> unreadableTaskIds() async {
    final ids = <String>[];
    for (final stored in await _inner.getAll()) {
      if (await _tryOpen(stored) == null) ids.add(stored.id);
    }
    return ids;
  }

  /// Returns the ids of tasks that [getNextReady] set aside because they
  /// could not be decrypted and that can be read again, for example because
  /// the key was restored.
  ///
  /// They are still held. Release each one to let it run:
  ///
  /// ```dart
  /// for (final id in await storage.recoveredTaskIds()) {
  ///   await queue.release(id);
  /// }
  /// ```
  Future<List<String>> recoveredTaskIds() async {
    final ids = <String>[];
    for (final stored in await _inner.getAll()) {
      if (stored.payload[_setAsideKey] != true || !stored.held) continue;
      if (await _tryOpen(stored) != null) ids.add(stored.id);
    }
    return ids;
  }

  List<String> _lookupValues(String value) {
    if (!_protectLookupKeys) {
      return [value];
    }
    return [
      for (final tag in _cipher.lookupTags(value)) '$_tagPrefix$tag',
      if (_readPlaintext) value,
    ];
  }

  Future<StoredTask> _seal(StoredTask task) async {
    final failure = task.lastFailure;
    final dedup = task.deduplicationKey;
    final group = task.group;
    final hideKeys = _protectLookupKeys;
    final secret = <String, Object?>{
      'payload': task.payload,
      if (failure != null) 'error': failure.error,
      if (failure != null) 'stackTrace': failure.stackTrace,
      if (hideKeys && dedup != null) 'deduplicationKey': dedup,
      if (hideKeys && group != null) 'group': group,
      if (task.failures.isNotEmpty)
        'failures': [
          for (final earlier in task.failures)
            {'error': earlier.error, 'stackTrace': earlier.stackTrace},
        ],
    };
    final String plaintext;
    try {
      plaintext = jsonEncode(secret);
    } on JsonUnsupportedObjectError catch (error) {
      throw ArgumentError.value(
        error.unsupportedObject,
        'payload',
        'Task ${task.id}: encrypted payloads must contain only JSON values',
      );
    }
    final data = await _cipher.encrypt(
      utf8.encode(plaintext),
      associatedData: _associatedData(task.id),
    );
    return task.copyWith(
      payload: {
        envelopeKey: _version,
        'key': _cipher.keyId,
        'data': base64.encode(data),
      },
      lastFailure: failure == null
          ? null
          : TaskFailure(
              error: redactedError,
              stackTrace: null,
              failedAt: failure.failedAt,
              attempt: failure.attempt,
            ),
      failures: [
        for (final earlier in task.failures)
          TaskFailure(
            error: redactedError,
            stackTrace: null,
            failedAt: earlier.failedAt,
            attempt: earlier.attempt,
          ),
      ],
      deduplicationKey: hideKeys && dedup != null
          ? '$_tagPrefix${_cipher.lookupTags(dedup).first}'
          : dedup,
      group: hideKeys && group != null
          ? '$_tagPrefix${_cipher.lookupTags(group).first}'
          : group,
    );
  }

  Future<StoredTask?> _openOrNull(StoredTask? stored) async =>
      stored == null ? null : _tryOpen(stored);

  /// The decrypted task, or null if [stored] cannot be decrypted.
  Future<StoredTask?> _tryOpen(StoredTask stored) async {
    try {
      return await _open(stored);
    } on PayloadDecryptionException {
      return null;
    }
  }

  /// The tasks in [stored] that can be decrypted, in the same order.
  Future<List<StoredTask>> _readable(List<StoredTask> stored) async => [
    for (final task in stored) ?await _tryOpen(task),
  ];

  Future<StoredTask> _open(StoredTask stored) async {
    final payload = stored.payload;
    final version = payload[envelopeKey];
    if (version == null) {
      if (!_readPlaintext) {
        throw PayloadDecryptionException(
          stored.id,
          'The task is not encrypted',
        );
      }
      if (!payload.containsKey(_setAsideKey)) return stored;
      return stored.copyWith(payload: {...payload}..remove(_setAsideKey));
    }
    if (version != _version) {
      throw PayloadDecryptionException(
        stored.id,
        'Unsupported format version $version',
      );
    }
    final keyId = payload['key'];
    final data = payload['data'];
    if (keyId is! String || data is! String) {
      throw PayloadDecryptionException(stored.id, 'Malformed encrypted record');
    }
    final Map<String, dynamic> secret;
    try {
      final plaintext = await _cipher.decrypt(
        base64.decode(data),
        keyId: keyId,
        associatedData: _associatedData(stored.id),
      );
      secret = jsonDecode(utf8.decode(plaintext)) as Map<String, dynamic>;
    } on Object catch (error) {
      throw PayloadDecryptionException(
        stored.id,
        'Decryption failed',
        keyId: keyId,
        cause: error,
      );
    }
    final failure = stored.lastFailure;
    final secretFailures = secret['failures'] as List<dynamic>? ?? const [];
    return stored.copyWith(
      failures: [
        for (var i = 0; i < stored.failures.length; i++)
          TaskFailure(
            error: i < secretFailures.length
                ? (secretFailures[i] as Map)['error'] as String
                : stored.failures[i].error,
            stackTrace: i < secretFailures.length
                ? (secretFailures[i] as Map)['stackTrace'] as String?
                : stored.failures[i].stackTrace,
            failedAt: stored.failures[i].failedAt,
            attempt: stored.failures[i].attempt,
          ),
      ],
      payload: secret['payload'] as Map<String, dynamic>,
      lastFailure: failure == null
          ? null
          : TaskFailure(
              error: secret['error'] as String? ?? failure.error,
              stackTrace: secret['stackTrace'] as String?,
              failedAt: failure.failedAt,
              attempt: failure.attempt,
            ),
      deduplicationKey: secret.containsKey('deduplicationKey')
          ? secret['deduplicationKey'] as String?
          : stored.deduplicationKey,
      group: secret.containsKey('group')
          ? secret['group'] as String?
          : stored.group,
    );
  }

  static List<int> _associatedData(String taskId) =>
      utf8.encode('durable_queue_encryption/v1/task/$taskId');

  static List<int> _stateAssociatedData(String key) =>
      utf8.encode('durable_queue_encryption/v1/state/$key');

  // The storage contract's order: createdAt, then sequence, then id.
  static int _compare(StoredTask a, StoredTask b) {
    final byTime = a.createdAt.compareTo(b.createdAt);
    if (byTime != 0) return byTime;
    final bySequence = a.sequence.compareTo(b.sequence);
    if (bySequence != 0) return bySequence;
    return a.id.compareTo(b.id);
  }
}

/// A stored task could not be decrypted: the key is unknown or wrong, the
/// record was changed, or it was copied from another task.
final class PayloadDecryptionException implements Exception {
  /// Creates the exception for the task [taskId].
  PayloadDecryptionException(
    this.taskId,
    this.message, {
    this.keyId,
    this.cause,
  });

  /// The id of the task that could not be read.
  final String taskId;

  /// What went wrong.
  final String message;

  /// The key id stored with the record, when it has one.
  final String? keyId;

  /// The error the cipher threw, when there was one.
  final Object? cause;

  @override
  String toString() {
    final key = keyId == null ? '' : ' (key $keyId)';
    final reason = cause == null ? '' : ': $cause';
    return 'PayloadDecryptionException: task $taskId$key: $message$reason';
  }
}
