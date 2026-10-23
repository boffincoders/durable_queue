// Recipe: upload a file with the operating system's background transfer
// service, then tell your server, even if the user closes the app.
//
// The full app is examples/durable_queue_demo.
import 'package:background_downloader/background_downloader.dart'
    show BaseDirectory, UploadTask;
import 'package:durable_queue/durable_queue.dart';
import 'package:durable_queue_dio/durable_queue_dio.dart';
import 'package:durable_queue_transfer/durable_queue_transfer.dart';

final class UploadFile extends DurableTask {
  UploadFile(this.fileName);

  final String fileName;

  @override
  String get type => 'upload_file';

  @override
  Map<String, dynamic> toJson() => {'fileName': fileName};

  static UploadFile fromJson(Map<String, dynamic> json) =>
      UploadFile(json['fileName'] as String);
}

Future<QueueTransfers> registerUploads(DurableQueue queue) async {
  final transfers = QueueTransfers(queue);
  queue.register<UploadFile>(
    type: 'upload_file',
    decoder: UploadFile.fromJson,
    retryIf: isRetryableTransferError,
    handler: (task, context) async {
      await transfers.start(
        context,
        UploadTask(
          url: 'https://api.example.com/files',
          filename: task.fileName,
          baseDirectory: BaseDirectory.applicationSupport,
          headers: {'Idempotency-Key': context.taskId},
        ),
      );
    },
  );
  await transfers.attach(); // once per launch, before or after queue.start()
  return transfers;
}

Future<void> uploadAndNotify(DurableQueue queue, String fileName) async {
  await queue.enqueueChain([
    UploadFile(fileName),
    HttpRequestTask(method: 'POST', url: '/files/$fileName/uploaded'),
  ], group: 'uploads');
}
