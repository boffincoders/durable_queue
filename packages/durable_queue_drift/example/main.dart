// A queue whose tasks live in the app's drift database.
//
// In an app, use your generated database class (for example `AppDatabase`)
// instead of `ExampleDatabase`; the queue adds its own tables to it.
import 'package:drift/drift.dart';
import 'package:drift/native.dart';
import 'package:durable_queue/durable_queue.dart';
import 'package:durable_queue_drift/durable_queue_drift.dart';

final class ExampleDatabase extends GeneratedDatabase {
  ExampleDatabase(super.executor);

  @override
  Iterable<TableInfo<Table, Object?>> get allTables => const [];

  @override
  int get schemaVersion => 1;
}

final class SendMessage extends DurableTask {
  SendMessage(this.text);

  final String text;

  @override
  String get type => 'send_message';

  @override
  Map<String, dynamic> toJson() => {'text': text};

  static SendMessage fromJson(Map<String, dynamic> json) =>
      SendMessage(json['text'] as String);
}

Future<void> main() async {
  final db = ExampleDatabase(NativeDatabase.memory());
  final queue = DurableQueue(storage: await DriftQueueStorage.open(db))
    ..register<SendMessage>(
      type: 'send_message',
      decoder: SendMessage.fromJson,
      handler: (task, context) async => print('sending "${task.text}"'),
    );

  final id = await queue.enqueue(
    SendMessage('hello'),
    retryPolicy: RetryPolicy.exponential(
      maxAttempts: 5,
      initialDelay: const Duration(seconds: 2),
    ),
  );
  await queue.start();
  final done = await queue.waitFor(id);
  print('task ${done.status.name}');

  await queue.close();
  await db.close();
}
