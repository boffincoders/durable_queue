// Recipe: look at work that failed for good, retry it, and clean up.
//
// Failed tasks stay stored with their last error (a dead-letter queue)
// until you retry, delete, or purge them.
import 'package:durable_queue/durable_queue.dart';

Future<String> describeFailures(DurableQueue queue) async {
  final failed = await queue.getTasks(status: TaskStatus.failed);
  return [
    for (final task in failed)
      '${task.type} ${task.id}: ${task.lastFailure?.error} '
          '(attempt ${task.attempts})',
  ].join('\n');
}

/// For a "Try again" button: every failed task gets a fresh attempt budget.
Future<int> retryEverythingThatFailed(DurableQueue queue) async {
  final ids = await queue.retryFailed();
  return ids.length;
}

/// Run now and then, for example at startup: drop finished work older than a
/// week so storage stays small.
Future<int> cleanUp(DurableQueue queue) {
  return queue.purge(
    statuses: {TaskStatus.completed, TaskStatus.cancelled},
    olderThan: const Duration(days: 7),
  );
}
