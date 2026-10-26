// Encrypted task payloads in SQLite.
//
// Run with `dart run example/main.dart` from packages/durable_queue_encryption.
// The key is generated here; in an app, keep it in the platform's secure
// storage (see the README).
import 'dart:io';

import 'package:durable_queue/durable_queue.dart';
import 'package:durable_queue_encryption/durable_queue_encryption.dart';
import 'package:durable_queue_sqlite/durable_queue_sqlite.dart';

Future<void> main() async {
  final dir = await Directory.systemTemp.createTemp('encrypted_queue_');
  final path = '${dir.path}/queue.db';
  final key = AesGcmCipher.generateKey();

  // First launch: a message is queued, then the app is closed.
  var sqlite = SqliteQueueStorage.open(path);
  var queue = _buildQueue(
    EncryptedQueueStorage(sqlite, cipher: AesGcmCipher(key)),
  );
  final id = await queue.enqueue(
    const SendMessage(to: 'ana@example.com', text: 'The door code is 4711'),
    deduplicationKey: 'message-ana-1',
  );
  await queue.close();

  // What is written to the database file.
  final raw = (await sqlite.get(id))!;
  print('stored payload: ${raw.payload}');
  print('stored deduplication key: ${raw.deduplicationKey}');
  await sqlite.close();

  // Next launch, with the same key: the task runs with the original data.
  sqlite = SqliteQueueStorage.open(path);
  queue = _buildQueue(EncryptedQueueStorage(sqlite, cipher: AesGcmCipher(key)));
  await queue.drain();
  await queue.close();
  await sqlite.close();
  await dir.delete(recursive: true);
}

DurableQueue _buildQueue(QueueStorage storage) {
  final queue = DurableQueue(storage: storage);
  queue.register<SendMessage>(
    type: SendMessage.taskType,
    decoder: SendMessage.fromJson,
    handler: (task, context) async =>
        print('sending to ${task.to}: "${task.text}"'),
  );
  return queue;
}

final class SendMessage extends DurableTask {
  const SendMessage({required this.to, required this.text});

  factory SendMessage.fromJson(Map<String, dynamic> json) =>
      SendMessage(to: json['to'] as String, text: json['text'] as String);

  static const taskType = 'send_message';

  final String to;
  final String text;

  @override
  String get type => taskType;

  @override
  Map<String, dynamic> toJson() => {'to': to, 'text': text};
}
