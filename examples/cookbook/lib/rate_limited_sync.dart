// Recipe: sync many records with an API that allows 10 requests per second
// and sometimes answers 429 Too Many Requests.
//
// The task stores only the record id; the handler reads the latest version
// when it runs, so edits made while the task waits are not lost.
import 'package:dio/dio.dart';
import 'package:durable_queue/durable_queue.dart';
import 'package:durable_queue_dio/durable_queue_dio.dart';

final class SyncRecord extends DurableTask {
  SyncRecord(this.recordId);

  final String recordId;

  static const taskType = 'sync_record';

  @override
  String get type => taskType;

  @override
  Map<String, dynamic> toJson() => {'recordId': recordId};

  static SyncRecord fromJson(Map<String, dynamic> json) =>
      SyncRecord(json['recordId'] as String);
}

void registerSync(
  DurableQueue queue,
  Dio api,
  Future<Map<String, Object?>> Function(String id) readRecord,
) {
  const rules = DioRetryRules();
  queue.register<SyncRecord>(
    type: SyncRecord.taskType,
    decoder: SyncRecord.fromJson,
    // At most 10 starts per second. (Limits are kept in memory.)
    rateLimit: RateLimit(10, per: const Duration(seconds: 1)),
    retryIf: rules.shouldRetry, // 408, 429, 5xx, timeouts; not other 4xx
    retryDelay: rules.retryDelay, // honours Retry-After
    handler: (task, context) async {
      final record = await readRecord(task.recordId);
      try {
        await api.put<void>('/records/${task.recordId}', data: record);
      } on DioException catch (error) {
        final wait = parseRetryAfter(
          error.response?.headers.value('retry-after'),
        );
        if (error.response?.statusCode == 429 && wait != null) {
          // Slow down every queued sync, not only this one.
          queue.holdRateLimit('type:${SyncRecord.taskType}', duration: wait);
        }
        rethrow;
      }
    },
  );
}

/// Queues a sync for each id. An id that is already waiting is not queued
/// twice; its handler will read the newest data anyway.
Future<void> syncRecords(DurableQueue queue, Iterable<String> ids) async {
  for (final id in ids) {
    await queue.enqueue(
      SyncRecord(id),
      deduplicationKey: 'sync:$id',
      group: 'sync',
    );
  }
}
