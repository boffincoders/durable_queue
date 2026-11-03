// Recipe: read queued tasks with a Bloc cubit, and pause, resume, or retry
// them from the UI.
import 'dart:async';

import 'package:durable_queue/durable_queue.dart';
import 'package:durable_queue_flutter/durable_queue_flutter.dart';
import 'package:flutter/material.dart';
import 'package:flutter_bloc/flutter_bloc.dart';

/// The tasks of one group, updated as they change.
class QueueGroupCubit extends Cubit<List<StoredTask>> {
  QueueGroupCubit(this.queue, String group) : super(const []) {
    _subscription = queue.watchGroup(group).listen(emit);
  }

  final DurableQueue queue;
  late final StreamSubscription<List<StoredTask>> _subscription;

  Future<void> pause(String id) => queue.hold(id);

  Future<void> resume(String id) => queue.release(id);

  Future<void> retry(String id) => queue.retry(id);

  @override
  Future<void> close() async {
    await _subscription.cancel();
    return super.close();
  }
}

class UploadsScreen extends StatelessWidget {
  const UploadsScreen({super.key, required this.queue});

  final DurableQueue queue;

  @override
  Widget build(BuildContext context) {
    return BlocProvider(
      create: (_) => QueueGroupCubit(queue, 'uploads'),
      child: BlocBuilder<QueueGroupCubit, List<StoredTask>>(
        builder: (context, tasks) {
          final cubit = context.read<QueueGroupCubit>();
          return ListView(
            children: [
              for (final task in tasks)
                ListTile(
                  title: Text(task.id),
                  subtitle: QueueProgressBuilder(
                    taskId: task.id,
                    queue: queue,
                    builder: (context, progress) =>
                        LinearProgressIndicator(value: progress?.fraction),
                  ),
                  trailing: switch (task) {
                    StoredTask(held: true) => TextButton(
                      onPressed: () => cubit.resume(task.id),
                      child: const Text('Resume'),
                    ),
                    StoredTask(status: TaskStatus.failed) => TextButton(
                      onPressed: () => cubit.retry(task.id),
                      child: const Text('Retry'),
                    ),
                    StoredTask(:final status) when status.isActive =>
                      TextButton(
                        onPressed: () => cubit.pause(task.id),
                        child: const Text('Pause'),
                      ),
                    StoredTask(:final status) => Text(status.name),
                  },
                ),
            ],
          );
        },
      ),
    );
  }
}
