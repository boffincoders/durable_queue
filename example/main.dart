import 'package:durable_queue/durable_queue.dart';

/// Sample task. The queue stores [toJson], not the upload closure.
final class UploadPhotoTask extends DurableTask {
  UploadPhotoTask({required this.path});

  final String path;

  @override
  String get type => 'upload_photo';

  @override
  Map<String, dynamic> toJson() => {'path': path};

  static UploadPhotoTask fromJson(Map<String, dynamic> json) {
    return UploadPhotoTask(path: json['path'] as String);
  }
}

Future<void> main() async {
  final queue = DurableQueue(
    storage: MemoryQueueStorage(),
    maxConcurrentTasks: 3,
  );

  queue.register<UploadPhotoTask>(
    type: 'upload_photo',
    decoder: UploadPhotoTask.fromJson,
    handler: (task, context) async {
      print('upload ${task.path} attempt ${context.attempt}');
      if (context.attempt == 1) {
        throw StateError('temporary failure');
      }
    },
    retryIf: (error, stackTrace) => error is StateError,
  );

  queue.events.listen((event) => print(event.runtimeType));

  await queue.start();
  final done = queue.events
      .where((event) => event is TaskCompleted || event is TaskFailed)
      .first;
  await queue.enqueue(
    UploadPhotoTask(path: '/storage/avatar.jpg'),
    idempotencyKey: 'avatar',
    retryPolicy: RetryPolicy.exponential(
      maxAttempts: 3,
      initialDelay: const Duration(milliseconds: 20),
    ),
  );

  await done.timeout(const Duration(seconds: 2));
  await queue.stop();
}
