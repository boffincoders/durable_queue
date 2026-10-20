import 'package:durable_queue/durable_queue.dart';
import 'package:durable_queue_flutter/durable_queue_flutter.dart';
import 'package:flutter_test/flutter_test.dart';

import 'support/delegating_storage.dart';

final class Note extends DurableTask {
  Note(this.text);

  final String text;

  @override
  String get type => 'note';

  @override
  Map<String, dynamic> toJson() => {'text': text};

  static Note fromJson(Map<String, dynamic> json) =>
      Note(json['text'] as String);
}

DurableQueue newQueue(List<String> ran) {
  return DurableQueue(
    storage: MemoryQueueStorage(),
    clock: FakeQueueClock(DateTime.utc(2026, 10, 20)),
  )..register<Note>(
    type: 'note',
    decoder: Note.fromJson,
    handler: (task, context) async => ran.add(task.text),
  );
}

void main() {
  late List<String> ran;
  late DurableQueue queue;

  setUp(() {
    ran = [];
    queue = newQueue(ran);
  });

  tearDown(() => queue.close());

  test('start starts an idle queue once', () async {
    final controller = QueueLifecycleController(queue);
    await controller.start();
    expect(queue.runState, QueueRunState.running);
    await controller.start();
    expect(queue.runState, QueueRunState.running);
  });

  test('background pauses, foreground resumes', () async {
    final controller = QueueLifecycleController(queue);
    await controller.start();

    await controller.setVisibility(AppVisibility.background);
    expect(queue.runState, QueueRunState.paused);
    expect(controller.isPausedByLifecycle, isTrue);

    await queue.enqueue(Note('while hidden'));
    await pumpEventQueue();
    expect(ran, isEmpty);

    await controller.setVisibility(AppVisibility.foreground);
    expect(queue.runState, QueueRunState.running);
    await pumpEventQueue();
    expect(ran, ['while hidden']);
  });

  test('starting in the background leaves the queue paused', () async {
    final controller = QueueLifecycleController(queue);
    await controller.setVisibility(AppVisibility.background);
    await controller.start();
    expect(queue.runState, QueueRunState.paused);
    await controller.setVisibility(AppVisibility.foreground);
    expect(queue.runState, QueueRunState.running);
  });

  test('a pause made by the app is not undone', () async {
    final controller = QueueLifecycleController(queue);
    await controller.start();
    await queue.pause();
    await controller.setVisibility(AppVisibility.background);
    await controller.setVisibility(AppVisibility.foreground);
    expect(queue.runState, QueueRunState.paused);
    expect(controller.isPausedByLifecycle, isFalse);
  });

  test('pauseInBackground: false keeps running', () async {
    final controller = QueueLifecycleController(
      queue,
      pauseInBackground: false,
    );
    await controller.start();
    await controller.setVisibility(AppVisibility.background);
    expect(queue.runState, QueueRunState.running);
  });

  test('quick switches are applied in order', () async {
    final controller = QueueLifecycleController(queue);
    await controller.start();
    final done = [
      controller.setVisibility(AppVisibility.background),
      controller.setVisibility(AppVisibility.foreground),
      controller.setVisibility(AppVisibility.background),
    ];
    await Future.wait(done);
    expect(queue.runState, QueueRunState.paused);
    await controller.setVisibility(AppVisibility.foreground);
    expect(queue.runState, QueueRunState.running);
  });

  test('a failed start is reported and can be retried', () async {
    final failing = _FailOnceStorage();
    final q = DurableQueue(storage: failing);
    final controller = QueueLifecycleController(q);
    await expectLater(controller.start(), throwsA(isA<StateError>()));
    expect(q.runState, QueueRunState.idle);
    await controller.start();
    expect(q.runState, QueueRunState.running);
    await q.close();
  });

  test('dispose resumes a queue it paused and then ignores changes', () async {
    final controller = QueueLifecycleController(queue);
    await controller.start();
    await controller.setVisibility(AppVisibility.background);
    await controller.dispose();
    expect(queue.runState, QueueRunState.running);
    await controller.setVisibility(AppVisibility.background);
    expect(queue.runState, QueueRunState.running);
  });

  test('a closed queue is ignored', () async {
    final controller = QueueLifecycleController(queue);
    await controller.start();
    await queue.close();
    await controller.setVisibility(AppVisibility.background);
    await controller.start();
    expect(queue.isClosed, isTrue);
  });
}

/// Memory storage whose first recovery query fails.
final class _FailOnceStorage extends DelegatingStorage {
  var _failed = false;

  @override
  Future<List<StoredTask>> getByStatus(TaskStatus status, {int? limit}) {
    if (!_failed) {
      _failed = true;
      throw StateError('disk unavailable');
    }
    return super.getByStatus(status, limit: limit);
  }
}
