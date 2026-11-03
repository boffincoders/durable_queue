// Recipe: upload files to Firebase Storage, retried across restarts, with
// progress, pause, and resume.
//
// The upload runs in the app, so it stops while the app is closed and
// continues on the next launch. For uploads that must go on while the app
// is closed, upload to a signed URL with durable_queue_transfer instead.
import 'dart:async';
import 'dart:io';

import 'package:durable_queue/durable_queue.dart';
import 'package:firebase_storage/firebase_storage.dart';

final class FirebaseUpload extends DurableTask {
  FirebaseUpload({required this.localPath, required this.storagePath});

  /// A file the app keeps until the upload is done, for example in its
  /// support directory. Not a temporary file the system may delete.
  final String localPath;

  /// Where the file goes in the bucket. The same path on every attempt, so
  /// a repeated upload overwrites the object instead of adding a copy.
  final String storagePath;

  static const taskType = 'firebase_upload';

  @override
  String get type => taskType;

  @override
  Map<String, dynamic> toJson() => {
    'localPath': localPath,
    'storagePath': storagePath,
  };

  static FirebaseUpload fromJson(Map<String, dynamic> json) => FirebaseUpload(
    localPath: json['localPath'] as String,
    storagePath: json['storagePath'] as String,
  );
}

/// The file was deleted before it could be uploaded. Not retried.
final class MissingUploadFile implements Exception {
  const MissingUploadFile(this.path);

  final String path;

  @override
  String toString() => 'MissingUploadFile: $path';
}

/// Errors that another attempt will not fix.
const _permanentCodes = {
  'unauthenticated',
  'unauthorized',
  'invalid-checksum',
  'bucket-not-found',
  'project-not-found',
};

void registerFirebaseUploads(DurableQueue queue, FirebaseStorage storage) {
  queue.register<FirebaseUpload>(
    type: FirebaseUpload.taskType,
    decoder: FirebaseUpload.fromJson,
    // Two uploads at a time; other work keeps running.
    maxConcurrent: 2,
    retryIf: (error, stackTrace) => switch (error) {
      MissingUploadFile() => false,
      FirebaseException(:final code) => !_permanentCodes.contains(code),
      _ => true,
    },
    handler: (task, context) async {
      final file = File(task.localPath);
      if (!file.existsSync()) throw MissingUploadFile(task.localPath);

      final upload = storage.ref(task.storagePath).putFile(file);
      final progress = upload.snapshotEvents.listen((snapshot) {
        if (snapshot.totalBytes > 0) {
          context.reportProgress(
            snapshot.bytesTransferred / snapshot.totalBytes,
          );
        }
      }, onError: (Object _) {}); // the upload's own future reports errors
      // queue.cancel(id) and queue.hold(id) stop the upload. After a hold
      // the task goes back to the queue and starts again on release.
      unawaited(context.whenCancellationRequested.then((_) => upload.cancel()));
      try {
        await upload;
      } finally {
        await progress.cancel();
      }
    },
  );
}

/// Queues an upload and returns its task id, for example to show its
/// progress with QueueProgressBuilder.
Future<String> uploadPhoto(DurableQueue queue, File photo, String userId) {
  final name = photo.uri.pathSegments.last;
  return queue.enqueue(
    FirebaseUpload(localPath: photo.path, storagePath: 'users/$userId/$name'),
    group: 'uploads',
    // One upload per file at a time, however often the user taps.
    deduplicationKey: 'upload:${photo.path}',
    retryPolicy: RetryPolicy.exponential(
      maxAttempts: 8,
      initialDelay: const Duration(seconds: 5),
      maxDelay: const Duration(minutes: 10),
      jitter: true,
    ),
  );
}
