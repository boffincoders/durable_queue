import 'dart:math';

import 'package:durable_queue/durable_queue.dart';
import 'package:test/test.dart';

import 'support/harness.dart';
import 'support/probe_storage.dart';

void main() {
  test(
    'large backlogs use bounded worker queries and batched recovery',
    () async {
      final storage = ProbeStorage();
      final h = QueueHarness(storage: storage);
      var calls = 0;
      h.register(
        handler: (_, _) async {
          calls++;
        },
      );
      for (var i = 0; i < 3000; i++) {
        await storage.inner.save(
          storedTask(
            id: 'task-$i',
            sequence: i + 1,
            status: i < 250
                ? TaskStatus.running
                : i < 1250
                ? TaskStatus.pending
                : i < 2250
                ? TaskStatus.retryScheduled
                : TaskStatus.completed,
            attempts: i < 250 ? 1 : 0,
            nextAttemptAt: i >= 1250 && i < 2250
                ? h.clock.now().add(const Duration(days: 1))
                : null,
          ),
        );
      }
      var largestBatch = 0;
      storage.before = (method, argument) async {
        if (method == 'getAll' || method == 'getPending') {
          fail('Worker must not load all records through $method');
        }
        if (method == 'getByStatus') {
          expect(argument, isA<int>());
          expect(argument as int, lessThanOrEqualTo(100));
        }
      };
      storage.after = (method, result) async {
        if (method == 'getByStatus') {
          largestBatch = max(largestBatch, (result as List).length);
        }
      };
      final added = await h.queue.enqueue(ValueTask('new'));
      expect((await storage.inner.get(added))!.sequence, 3001);
      await h.queue.start();
      await h.until(() => h.of<TaskCompleted>().length == 1001);
      expect(calls, 1001);
      expect(h.of<TaskFailed>(), hasLength(250));
      expect(largestBatch, 100);
      expect(storage.calls['getByStatus'], 4);
      expect(storage.calls['getMaxSequence'], 1);
      expect(storage.calls['getAll'], isNull);
      expect(storage.calls['getNextReady'], lessThanOrEqualTo(1003));
      await h.queue.stop();
    },
  );

  test(
    'memory indexes match a scan across updates, deletions and clock changes',
    () async {
      final storage = MemoryQueueStorage();
      final records = <String, StoredTask>{};
      final random = Random(42);
      final epoch = DateTime.utc(2026);
      for (var step = 0; step < 500; step++) {
        final id = 'id-${random.nextInt(35)}';
        if (random.nextInt(5) == 0) {
          records.remove(id);
          await storage.delete(id);
        } else {
          final record = storedTask(
            id: id,
            sequence: random.nextInt(50),
            status: TaskStatus.values[random.nextInt(TaskStatus.values.length)],
            createdAt: epoch.add(Duration(seconds: random.nextInt(5))),
            nextAttemptAt: random.nextBool()
                ? null
                : epoch.add(Duration(seconds: random.nextInt(10))),
            deduplicationKey: 'key-${random.nextInt(3)}',
          );
          if (records.containsKey(id)) {
            await storage.update(record);
          } else {
            await storage.save(record);
          }
          records[id] = record;
        }
        final now = epoch.add(Duration(seconds: random.nextInt(10)));
        final ordered = records.values.toList()..sort(compareStoredTasks);
        final active = ordered
            .where(
              (t) =>
                  t.status == TaskStatus.pending ||
                  t.status == TaskStatus.retryScheduled,
            )
            .toList();
        final ready = active
            .where(
              (t) => t.nextAttemptAt == null || !t.nextAttemptAt!.isAfter(now),
            )
            .toList();
        final times =
            active.map((t) => t.nextAttemptAt).whereType<DateTime>().toList()
              ..sort();
        expect(
          (await storage.getNextReady(now))?.id,
          ready.firstOrNull?.id,
          reason: 'step $step',
        );
        expect(await storage.getNextWakeAt(), times.firstOrNull);
        expect(
          await storage.getMaxSequence(),
          records.values.fold<int>(0, (n, t) => max(n, t.sequence)),
        );
        for (final status in TaskStatus.values) {
          expect(
            (await storage.getByStatus(status, limit: 3)).map((t) => t.id),
            ordered.where((t) => t.status == status).take(3).map((t) => t.id),
          );
        }
        for (var key = 0; key < 3; key++) {
          final expected = ordered
              .where(
                (t) =>
                    t.deduplicationKey == 'key-$key' &&
                    [
                      TaskStatus.pending,
                      TaskStatus.retryScheduled,
                      TaskStatus.running,
                    ].contains(t.status),
              )
              .firstOrNull;
          expect(
            (await storage.findActiveByDeduplicationKey('key-$key'))?.id,
            expected?.id,
          );
        }
      }
    },
  );
}
