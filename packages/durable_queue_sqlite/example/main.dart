// Tasks stored in SQLite survive the app being closed.
//
// Run with `dart run example/main.dart` from packages/durable_queue_sqlite.
import 'dart:io';

import 'package:durable_queue/durable_queue.dart';
import 'package:durable_queue_sqlite/durable_queue_sqlite.dart';

final class SendMessageTask extends DurableTask {
  SendMessageTask(this.text);

  final String text;

  @override
  String get type => 'send_message';

  @override
  Map<String, dynamic> toJson() => {'text': text};

  static SendMessageTask fromJson(Map<String, dynamic> json) =>
      SendMessageTask(json['text'] as String);
}

/// Builds the queue the same way on every launch.
DurableQueue buildQueue(SqliteQueueStorage storage) {
  return DurableQueue(storage: storage)..register<SendMessageTask>(
    type: 'send_message',
    decoder: SendMessageTask.fromJson,
    handler: (task, context) async => print('sent: ${task.text}'),
  );
}

Future<void> main() async {
  final directory = await Directory.systemTemp.createTemp('chat_queue_');
  final path = '${directory.path}/queue.db';

  // First launch: offline, so messages are only stored. Then the app closes.
  final firstStorage = SqliteQueueStorage.open(path);
  final first = buildQueue(firstStorage);
  await first.enqueue(SendMessageTask('Hello'));
  await first.enqueue(SendMessageTask('Are you there?'));
  print('stored ${(await first.getTasks()).length} messages, app closed');
  await first.close();
  await firstStorage.close();

  // Second launch: same file, new process in a real app.
  final storage = SqliteQueueStorage.open(path);
  final queue = buildQueue(storage);
  final done = queue.events
      .where((event) => event is TaskCompleted)
      .take(2)
      .drain<void>();
  await queue.start();
  await done;
  print(
    'pending after restart: '
    '${(await queue.getTasks(status: TaskStatus.pending)).length}',
  );
  await queue.close();
  await storage.close();
  await directory.delete(recursive: true);
}
