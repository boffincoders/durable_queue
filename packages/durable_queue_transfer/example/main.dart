// Photo uploads that keep going after the app is minimised or closed, then
// a follow-up request once each upload is done.
import 'package:background_downloader/background_downloader.dart';
import 'package:durable_queue/durable_queue.dart' hide TaskStatus;
import 'package:durable_queue_transfer/durable_queue_transfer.dart';
import 'package:flutter/widgets.dart';

final class PhotoUpload extends DurableTask {
  PhotoUpload(this.fileName);

  final String fileName;

  @override
  String get type => 'photo_upload';

  @override
  Map<String, dynamic> toJson() => {'fileName': fileName};

  static PhotoUpload fromJson(Map<String, dynamic> json) =>
      PhotoUpload(json['fileName'] as String);
}

final class TagPhoto extends DurableTask {
  TagPhoto(this.fileName);

  final String fileName;

  @override
  String get type => 'tag_photo';

  @override
  Map<String, dynamic> toJson() => {'fileName': fileName};

  static TagPhoto fromJson(Map<String, dynamic> json) =>
      TagPhoto(json['fileName'] as String);
}

Future<void> main() async {
  WidgetsFlutterBinding.ensureInitialized();

  // Use durable_queue_sqlite in a real app, so queued work survives restarts.
  final queue = DurableQueue(storage: MemoryQueueStorage());
  final transfers = QueueTransfers(
    queue,
    onComplete: (result) {
      // For example, keep the server's photo id from result.responseBody.
    },
  );

  queue
    ..register<PhotoUpload>(
      type: 'photo_upload',
      decoder: PhotoUpload.fromJson,
      retryIf: isRetryableTransferError,
      handler: (task, context) => transfers.start(
        context,
        UploadTask(
          url: 'https://example.com/photos',
          filename: task.fileName, // in the app's documents directory
        ),
      ),
    )
    ..register<TagPhoto>(
      type: 'tag_photo',
      decoder: TagPhoto.fromJson,
      handler: (task, context) async {
        // A normal request; runs after the upload completed.
      },
    );

  await transfers.attach();
  await queue.start();

  await queue.enqueueChain(
    [PhotoUpload('beach.jpg'), TagPhoto('beach.jpg')],
    retryPolicy: RetryPolicy.exponential(
      maxAttempts: 5,
      initialDelay: const Duration(seconds: 10),
    ),
  );

  runApp(
    const Directionality(
      textDirection: TextDirection.ltr,
      child: Text('Uploading; you can close the app.'),
    ),
  );
}
