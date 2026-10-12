import 'package:durable_queue/durable_queue.dart';

/// One step of a photo pipeline. The queue stores [toJson] only.
final class PhotoStep extends DurableTask {
  PhotoStep(this.step, this.photo);

  final String step;
  final String photo;

  @override
  String get type => 'photo_step';

  @override
  Map<String, dynamic> toJson() => {'step': step, 'photo': photo};

  static PhotoStep fromJson(Map<String, dynamic> json) {
    return PhotoStep(json['step'] as String, json['photo'] as String);
  }
}

Future<void> main() async {
  final queue = DurableQueue(
    storage: MemoryQueueStorage(),
    maxConcurrentTasks: 3,
  );

  queue.register<PhotoStep>(
    type: 'photo_step',
    decoder: PhotoStep.fromJson,
    handler: (task, context) async {
      print('${task.step} ${task.photo}');
      if (task.photo == 'broken.jpg' && task.step == 'resize') {
        throw StateError('corrupt image');
      }
    },
  );

  // A chain: each step waits for the previous one, even with three slots.
  final good = await queue.enqueueChain([
    PhotoStep('upload', 'beach.jpg'),
    PhotoStep('resize', 'beach.jpg'),
    PhotoStep('notify', 'beach.jpg'),
  ], group: 'photos');

  // When resize fails, notify is cancelled instead of running.
  final broken = await queue.enqueueChain([
    PhotoStep('upload', 'broken.jpg'),
    PhotoStep('resize', 'broken.jpg'),
    PhotoStep('notify', 'broken.jpg'),
  ], group: 'photos');

  // A cleanup step that runs after both chains, however they ended.
  final cleanup = await queue.enqueue(
    PhotoStep('cleanup', 'tmp'),
    dependsOn: [good.last, broken.last],
    onDependencyFailure: DependencyFailurePolicy.run,
  );

  // Higher priority starts first among tasks that are ready.
  await queue.enqueue(PhotoStep('thumbnail', 'profile.jpg'), priority: 10);

  final finished = queue.events
      .where((event) => event.taskId == cleanup && event is TaskCompleted)
      .first;
  await queue.start();
  await finished.timeout(const Duration(seconds: 2));

  for (final task in await queue.getTasks(group: 'photos')) {
    print(
      '${task.payload['photo']} ${task.payload['step']}: ${task.status.name}',
    );
  }
  await queue.close();
}
