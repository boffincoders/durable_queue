// Recipe: send chat messages in the order they were written, even across
// restarts and while offline.
//
// Each message depends on the previous one in its conversation, so message
// 2 never goes out before message 1, while other conversations are not held
// up.
import 'package:dio/dio.dart';
import 'package:durable_queue/durable_queue.dart';
import 'package:durable_queue_dio/durable_queue_dio.dart';

final class ChatOutbox {
  ChatOutbox(this.queue);

  final DurableQueue queue;

  /// Registers the request handler; call once before `queue.start()`.
  static void register(DurableQueue queue, Dio api) {
    queue.registerHttpRequests(api);
  }

  /// Queues [text] after the last unsent message of [conversationId].
  ///
  /// Await each call before the next one for the same conversation, so the
  /// new message sees the one before it.
  Future<String> send({
    required String conversationId,
    required String clientMessageId,
    required String text,
  }) async {
    final group = 'chat:$conversationId';
    final pending = [
      for (final task in await queue.getTasks(group: group))
        if (task.status.isActive) task,
    ];
    return queue.enqueue(
      HttpRequestTask(
        method: 'POST',
        url: '/conversations/$conversationId/messages',
        body: {'id': clientMessageId, 'text': text},
      ),
      group: group,
      dependsOn: [if (pending.isNotEmpty) pending.last.id],
      // If an earlier message fails for good, later ones still go out.
      onDependencyFailure: DependencyFailurePolicy.run,
      deduplicationKey: 'message:$clientMessageId',
      idempotencyKey: clientMessageId,
      retryPolicy: RetryPolicy.exponential(
        maxAttempts: 20,
        initialDelay: const Duration(seconds: 2),
        maxDelay: const Duration(minutes: 5),
      ),
    );
  }
}
