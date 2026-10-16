// Rate limits and gates, runnable with
// `dart run example/rate_limits_and_gates.dart`.
//
// It shows:
// * a per-type rate limit (2 uploads per second; every attempt counts);
// * a shared limit per API host, so each host has its own budget;
// * a `network` gate: uploads wait while it is closed, other work runs, and
//   waiting uploads start as soon as it opens;
// * `deferOn`: a connection error defers the task instead of using an
//   attempt.
//
// In a Flutter app, open and close the gate from a connectivity listener.
import 'package:durable_queue/durable_queue.dart';

final class UploadTask extends DurableTask {
  UploadTask(this.name, this.host);

  final String name;
  final String host;

  @override
  String get type => 'upload';

  @override
  Map<String, dynamic> toJson() => {'name': name, 'host': host};

  static UploadTask fromJson(Map<String, dynamic> json) =>
      UploadTask(json['name'] as String, json['host'] as String);
}

final class LogTask extends DurableTask {
  LogTask(this.message);

  final String message;

  @override
  String get type => 'log';

  @override
  Map<String, dynamic> toJson() => {'message': message};

  static LogTask fromJson(Map<String, dynamic> json) =>
      LogTask(json['message'] as String);
}

final class ConnectionLost implements Exception {
  @override
  String toString() => 'ConnectionLost';
}

Future<void> main() async {
  final started = DateTime.now();
  String at() =>
      '${DateTime.now().difference(started).inMilliseconds.toString().padLeft(5)} ms';

  final network = ManualQueueGate(open: false);
  final queue = DurableQueue(
    storage: MemoryQueueStorage(),
    maxConcurrentTasks: 4,
    gateErrorDelay: const Duration(milliseconds: 300),
  );
  queue.addGate('network', network);
  queue.addRateLimit(
    'per-host',
    RateLimit(1, per: const Duration(milliseconds: 500)),
    keyOf: (task) => task.payload['host'] as String?,
  );

  var failedOnce = false;
  queue.register<UploadTask>(
    type: 'upload',
    decoder: UploadTask.fromJson,
    requires: {'network'},
    rateLimit: RateLimit(2, per: const Duration(seconds: 1)),
    deferOn: (error, stackTrace) => error is ConnectionLost ? 'network' : null,
    handler: (task, context) async {
      if (task.name == 'c.jpg' && !failedOnce) {
        failedOnce = true;
        print('${at()}  upload ${task.name}: connection lost');
        throw ConnectionLost();
      }
      print('${at()}  upload ${task.name} to ${task.host}');
    },
  );
  queue.register<LogTask>(
    type: 'log',
    decoder: LogTask.fromJson,
    handler: (task, context) async => print('${at()}  log: ${task.message}'),
  );
  queue.events.listen((event) {
    if (event is TaskDeferred) {
      final wait = event.until.difference(event.occurredAt).inMilliseconds;
      print(
        '${at()}  deferred ${event.taskType} (${event.reason.name}: '
        '${event.name}, up to $wait ms)',
      );
    }
  });

  for (final (name, host) in [
    ('a.jpg', 'eu.example.com'),
    ('b.jpg', 'eu.example.com'),
    ('c.jpg', 'us.example.com'),
    ('d.jpg', 'us.example.com'),
  ]) {
    await queue.enqueue(UploadTask(name, host));
  }
  await queue.enqueue(LogTask('offline work still runs'));

  print('${at()}  start while offline');
  await queue.start();
  await Future<void>.delayed(const Duration(milliseconds: 300));

  print('${at()}  network is back');
  network.open();

  while ((await queue.getTasks(status: TaskStatus.completed)).length < 5) {
    await Future<void>.delayed(const Duration(milliseconds: 50));
  }
  final c = (await queue.getTasks()).firstWhere(
    (task) => task.payload['name'] == 'c.jpg',
  );
  print('${at()}  done; c.jpg used ${c.attempts} attempt(s)');
  await queue.close();
}
