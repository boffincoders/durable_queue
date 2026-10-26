/// @docImport 'aes_gcm_cipher.dart';
library;

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
/// Reading a task that cannot be decrypted throws
/// [PayloadDecryptionException]. A queue that hits one reports it on
/// `DurableQueue.storageErrors` and keeps retrying, so a lost key stops the
/// queue: find such tasks with [unreadableTaskIds] and delete them.
///
/// Every queue or connection that opens the same storage must use this
/// wrapper with the same keys.
final class EncryptedQueueStorage implements QueueStorage {
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

  final QueueStorage _inner;
  final PayloadCipher _cipher;
  final bool _protectLookupKeys;
  final bool _readPlaintext;

  /// The wrapped storage. Records read from it directly are encrypted.
  QueueStorage get inner => _inner;

  @override
  Future<void> save(StoredTask task) async => _inner.save(await _seal(task));

  @override
  Future<void> update(StoredTask task) async =>
      _inner.update(await _seal(task));

  @override
  Future<void> delete(String id) => _inner.delete(id);

  @override
  Future<StoredTask?> get(String id) async => _openOrNull(await _inner.get(id));

  @override
  Future<List<StoredTask>> getPending() async =>
      _openAll(await _inner.getPending());

  @override
  Future<List<StoredTask>> getByStatus(TaskStatus status, {int? limit}) async =>
      _openAll(await _inner.getByStatus(status, limit: limit));

  @override
  Future<StoredTask?> getNextReady(DateTime now) async =>
      _openOrNull(await _inner.getNextReady(now));

  @override
  Future<DateTime?> getNextWakeAt() => _inner.getNextWakeAt();

  @override
  Future<int> getMaxSequence() => _inner.getMaxSequence();

  @override
  Future<List<StoredTask>> getAll() async => _openAll(await _inner.getAll());

  @override
  Future<List<StoredTask>> getWaitingDependents(String id) async =>
      _openAll(await _inner.getWaitingDependents(id));

  @override
  Future<StoredTask?> findActiveByDeduplicationKey(String key) async {
    StoredTask? oldest;
    for (final stored in _lookupValues(key)) {
      final found = await _inner.findActiveByDeduplicationKey(stored);
      if (found != null && (oldest == null || _compare(found, oldest) < 0)) {
        oldest = found;
      }
    }
    return _openOrNull(oldest);
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
    return _openAll(tasks);
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
  /// after the key was lost. Delete them with [delete] so the queue can
  /// continue.
  Future<List<String>> unreadableTaskIds() async {
    final ids = <String>[];
    for (final stored in await _inner.getAll()) {
      try {
        await _open(stored);
      } on PayloadDecryptionException {
        ids.add(stored.id);
      }
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
      deduplicationKey: hideKeys && dedup != null
          ? '$_tagPrefix${_cipher.lookupTags(dedup).first}'
          : dedup,
      group: hideKeys && group != null
          ? '$_tagPrefix${_cipher.lookupTags(group).first}'
          : group,
    );
  }

  Future<StoredTask?> _openOrNull(StoredTask? stored) async =>
      stored == null ? null : _open(stored);

  Future<List<StoredTask>> _openAll(List<StoredTask> stored) async => [
    for (final task in stored) await _open(task),
  ];

  Future<StoredTask> _open(StoredTask stored) async {
    final payload = stored.payload;
    final version = payload[envelopeKey];
    if (version == null) {
      if (_readPlaintext) return stored;
      throw PayloadDecryptionException(stored.id, 'The task is not encrypted');
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
    return stored.copyWith(
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
