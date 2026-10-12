// A realistic photo-upload flow, runnable with `dart run example/image_upload.dart`.
//
// It shows:
// * retrying only temporary failures (`retryIf`) with exponential backoff;
// * failing fast on a permanent error;
// * an idempotency key the server uses to ignore duplicate uploads;
// * deduplication when the user taps "upload" twice;
// * tasks surviving an app restart via a file-backed storage adapter.
//
// The "server" is simulated so the example runs offline. In an app, replace
// FakePhotoApi with your HTTP client and keep everything else.
import 'dart:io';

import 'package:durable_queue/durable_queue.dart';

import 'json_file_storage.dart';

/// The work to do, stored as JSON. No file bytes, no closures.
final class UploadPhotoTask extends DurableTask {
  UploadPhotoTask({required this.path, required this.albumId});

  final String path;
  final String albumId;

  @override
  String get type => 'upload_photo';

  @override
  Map<String, dynamic> toJson() => {'path': path, 'albumId': albumId};

  static UploadPhotoTask fromJson(Map<String, dynamic> json) {
    return UploadPhotoTask(
      path: json['path'] as String,
      albumId: json['albumId'] as String,
    );
  }
}

/// A network error worth retrying (timeouts, 5xx, 429).
final class TemporaryUploadError implements Exception {
  TemporaryUploadError(this.reason);
  final String reason;
  @override
  String toString() => 'TemporaryUploadError: $reason';
}

/// An error retrying cannot fix (400, file rejected).
final class PermanentUploadError implements Exception {
  PermanentUploadError(this.reason);
  final String reason;
  @override
  String toString() => 'PermanentUploadError: $reason';
}

/// Simulated photo API: flaky at first, rejects one file, and ignores
/// repeated requests that carry an idempotency key it has already seen.
final class FakePhotoApi {
  final _seenKeys = <String>{};
  var _calls = 0;

  Future<void> upload(String path, {required String idempotencyKey}) async {
    await Future<void>.delayed(const Duration(milliseconds: 20));
    _calls++;
    if (_seenKeys.contains(idempotencyKey)) {
      print('  server: duplicate $idempotencyKey ignored');
      return;
    }
    if (path.endsWith('.heic')) {
      throw PermanentUploadError('unsupported format');
    }
    if (_calls <= 2) throw TemporaryUploadError('connection reset');
    _seenKeys.add(idempotencyKey);
    print('  server: stored $path');
  }
}

/// Builds a queue the same way on every launch.
DurableQueue buildQueue(QueueStorage storage, FakePhotoApi api) {
  final queue = DurableQueue(storage: storage, maxConcurrentTasks: 2);
  queue.register<UploadPhotoTask>(
    type: 'upload_photo',
    decoder: UploadPhotoTask.fromJson,
    handler: (task, context) async {
      print('upload ${task.path} (attempt ${context.attempt})');
      await api.upload(task.path, idempotencyKey: context.idempotencyKey!);
    },
    // Only network-style failures are retried. Anything else fails at once.
    retryIf: (error, stackTrace) => error is TemporaryUploadError,
  );
  queue.events.listen((event) {
    switch (event) {
      case TaskRetryScheduled(:final delay, :final error):
        print('  retry in ${delay.inMilliseconds} ms after $error');
      case TaskFailed(:final failure):
        print('  gave up: ${failure.error}');
      default:
    }
  });
  return queue;
}

Future<void> enqueuePhoto(DurableQueue queue, String path) {
  return queue.enqueue(
    UploadPhotoTask(path: path, albumId: 'holiday'),
    // One active upload per file, however many times the user taps.
    deduplicationKey: 'upload:$path',
    // Sent to the server so a retried request is not stored twice.
    idempotencyKey: 'upload:$path:v1',
    group: 'album:holiday',
    retryPolicy: RetryPolicy.exponential(
      maxAttempts: 5,
      initialDelay: const Duration(milliseconds: 100),
      maxDelay: const Duration(seconds: 2),
      jitter: true,
    ),
  );
}

Future<void> main() async {
  final directory = await Directory.systemTemp.createTemp('photo_queue_');
  final path = '${directory.path}/queue.json';
  final api = FakePhotoApi();

  // First launch: the user picks photos while offline, then closes the app.
  print('--- first launch (offline) ---');
  final firstQueue = buildQueue(JsonFileStorage(path), api);
  await enqueuePhoto(firstQueue, '/photos/beach.jpg');
  await enqueuePhoto(firstQueue, '/photos/beach.jpg'); // double tap: no-op
  await enqueuePhoto(firstQueue, '/photos/sunset.jpg');
  await enqueuePhoto(firstQueue, '/photos/raw.heic');
  final stored = await firstQueue.getTasks(group: 'album:holiday');
  print('stored ${stored.length} uploads; app closed before starting');
  await firstQueue.close();

  // Second launch: the same file is opened and the queue picks up the work.
  print('--- second launch ---');
  final queue = buildQueue(JsonFileStorage(path), api);
  await queue.start();
  while ((await queue.getTasks(group: 'album:holiday'))
      .any((task) => task.status.isActive)) {
    await Future<void>.delayed(const Duration(milliseconds: 50));
  }

  print('--- result ---');
  for (final task in await queue.getTasks(group: 'album:holiday')) {
    final error = task.lastFailure == null
        ? ''
        : ' (${task.lastFailure!.error})';
    print(
      '${task.payload['path']}: ${task.status.name}, '
      '${task.attempts} attempt(s)$error',
    );
  }
  await queue.close();
  await directory.delete(recursive: true);
}
