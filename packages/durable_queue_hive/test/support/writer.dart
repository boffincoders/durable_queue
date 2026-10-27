// Child process for kill_test.dart: saves tasks one after another and
// prints the id of each save that completed, until it is killed.
import 'dart:io';

import 'package:durable_queue/durable_queue.dart';
import 'package:durable_queue_hive/durable_queue_hive.dart';

Future<void> main(List<String> args) async {
  final storage = await HiveQueueStorage.open(args[1], path: args[0]);
  final start = await storage.getMaxSequence() + 1;
  for (var i = start; ; i++) {
    final now = DateTime.now().toUtc();
    await storage.save(
      StoredTask(
        id: 'task-$i',
        type: 'kill',
        payload: {'i': i, 'filler': 'x' * 200},
        status: TaskStatus.pending,
        attempts: 0,
        retryPolicy: RetryPolicy.none(),
        createdAt: now,
        updatedAt: now,
        sequence: i,
      ),
    );
    stdout.writeln('saved task-$i');
  }
}
