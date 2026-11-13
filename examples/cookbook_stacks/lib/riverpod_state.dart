// Recipe: read queued tasks and their progress with Riverpod.
//
// The queue is created in main() and handed to Riverpod with an override.
// DurableQueueScope still starts it with the app and pauses it in the
// background.
import 'package:durable_queue/durable_queue.dart';
import 'package:durable_queue_flutter/durable_queue_flutter.dart';
import 'package:flutter_riverpod/flutter_riverpod.dart';
import 'package:material_ui/material_ui.dart';

final queueProvider = Provider<DurableQueue>(
  (ref) => throw UnimplementedError('Override queueProvider in main()'),
);

/// The tasks of the 'uploads' group, updated as they change.
final uploadsProvider = StreamProvider<List<StoredTask>>(
  (ref) => ref.watch(queueProvider).watchGroup('uploads'),
);

/// The progress of one task's current attempt.
final progressProvider = StreamProvider.family<TaskProgress?, String>(
  (ref, taskId) => ref.watch(queueProvider).watchProgress(taskId),
);

void runWithRiverpod(DurableQueue queue, Widget app) {
  runApp(
    ProviderScope(
      overrides: [queueProvider.overrideWithValue(queue)],
      child: DurableQueueScope(queue: queue, child: app),
    ),
  );
}

class UploadsList extends ConsumerWidget {
  const UploadsList({super.key});

  @override
  Widget build(BuildContext context, WidgetRef ref) {
    final queue = ref.watch(queueProvider);
    return ref
        .watch(uploadsProvider)
        .when(
          loading: () => const Center(child: CircularProgressIndicator()),
          error: (error, _) => Center(child: Text('$error')),
          data: (tasks) => ListView(
            children: [
              for (final task in tasks)
                ListTile(
                  title: Text(task.id),
                  subtitle: _UploadProgress(taskId: task.id),
                  trailing: task.held
                      ? TextButton(
                          onPressed: () => queue.release(task.id),
                          child: const Text('Resume'),
                        )
                      : task.status.isActive
                      ? TextButton(
                          onPressed: () => queue.hold(task.id),
                          child: const Text('Pause'),
                        )
                      : Text(task.status.name),
                ),
            ],
          ),
        );
  }
}

class _UploadProgress extends ConsumerWidget {
  const _UploadProgress({required this.taskId});

  final String taskId;

  @override
  Widget build(BuildContext context, WidgetRef ref) {
    final progress = ref.watch(progressProvider(taskId)).value;
    return LinearProgressIndicator(value: progress?.fraction);
  }
}
