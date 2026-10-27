// A queue on Hive: tasks are stored in a Hive box and survive a restart.
//
// Run with `dart run example/main.dart` from packages/durable_queue_hive.
// In a Flutter app, call `await Hive.initFlutter()` (from `hive_ce_flutter`)
// instead of `Hive.init`.
import 'dart:io';

import 'package:durable_queue/durable_queue.dart';
import 'package:durable_queue_hive/durable_queue_hive.dart';
import 'package:hive_ce/hive_ce.dart';

Future<void> main() async {
  final dir = await Directory.systemTemp.createTemp('hive_queue_');
  Hive.init(dir.path);

  // First launch, offline: a message is stored, then the app is closed.
  var storage = await HiveQueueStorage.open('durable_queue');
  var queue = _buildQueue(storage);
  await queue.enqueue(const SendMessage('See you at 7'));
  print('stored ${(await storage.getAll()).length} task');
  await queue.close();
  await storage.close();

  // Next launch: the queue finds the message and sends it.
  storage = await HiveQueueStorage.open('durable_queue');
  queue = _buildQueue(storage);
  await queue.drain();
  await queue.close();
  await storage.close();
  await dir.delete(recursive: true);
}

DurableQueue _buildQueue(QueueStorage storage) {
  final queue = DurableQueue(storage: storage);
  queue.register<SendMessage>(
    type: SendMessage.taskType,
    decoder: (json) => SendMessage(json['text'] as String),
    handler: (message, context) async => print('sent: ${message.text}'),
  );
  return queue;
}

final class SendMessage extends DurableTask {
  const SendMessage(this.text);

  static const taskType = 'send_message';

  final String text;

  @override
  String get type => taskType;

  @override
  Map<String, dynamic> toJson() => {'text': text};
}
