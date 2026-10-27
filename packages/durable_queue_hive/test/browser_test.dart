@TestOn('browser')
library;

import 'package:durable_queue/durable_queue.dart';
import 'package:durable_queue_hive/durable_queue_hive.dart';
import 'package:test/test.dart';

// On the web, Hive keeps the box in IndexedDB.
void main() {
  test('tasks survive closing and reopening the box', () async {
    final name = 'dq_browser_${DateTime.now().microsecondsSinceEpoch}';
    final now = DateTime.utc(2026);
    final long = 'é' * 200;
    var storage = await HiveQueueStorage.open(name);
    for (final (i, id) in ['a', 'b', long].indexed) {
      await storage.save(
        StoredTask(
          id: id,
          type: 't',
          payload: {'i': i},
          status: i == 1 ? TaskStatus.completed : TaskStatus.pending,
          attempts: 0,
          retryPolicy: RetryPolicy.none(),
          createdAt: now,
          updatedAt: now,
          sequence: i,
          group: 'g',
        ),
      );
    }
    await storage.delete('a');
    await storage.close();

    storage = await HiveQueueStorage.open(name);
    expect((await storage.getAll()).map((t) => t.id), ['b', long]);
    expect((await storage.getNextReady(DateTime.utc(2030)))?.id, long);
    expect((await storage.get(long))!.payload, {'i': 2});
    await storage.box.deleteFromDisk();
  });
}
