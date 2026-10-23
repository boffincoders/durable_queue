// Recipe: show the state of queued work in the UI, updated as it changes.
import 'package:durable_queue/durable_queue.dart';
import 'package:durable_queue_flutter/durable_queue_flutter.dart';
import 'package:flutter/widgets.dart';

/// "3 of 5 uploaded", for everything in [group].
class GroupProgress extends StatelessWidget {
  const GroupProgress({super.key, required this.group});

  final String group;

  @override
  Widget build(BuildContext context) {
    return QueueGroupBuilder(
      group: group,
      builder: (context, tasks) {
        final done = tasks.where((t) => t.status == TaskStatus.completed);
        final failed = tasks.where((t) => t.status == TaskStatus.failed);
        return Text(
          '${done.length} of ${tasks.length} done'
          '${failed.isEmpty ? '' : ', ${failed.length} failed'}',
        );
      },
    );
  }
}

/// The status of one task, for example next to a sent message.
class TaskBadge extends StatelessWidget {
  const TaskBadge({super.key, required this.taskId});

  final String taskId;

  @override
  Widget build(BuildContext context) {
    return QueueTaskBuilder(
      taskId: taskId,
      builder: (context, task) => Text(switch (task?.status) {
        null => '',
        TaskStatus.completed => 'sent',
        TaskStatus.failed => 'not sent',
        TaskStatus.retryScheduled => 'retrying',
        _ => 'sending',
      }),
    );
  }
}
