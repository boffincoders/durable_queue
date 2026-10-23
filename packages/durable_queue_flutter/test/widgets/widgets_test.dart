// Widget tests. They need the Flutter SDK and run in CI with `flutter test`.
import 'package:durable_queue/durable_queue.dart';
import 'package:durable_queue_flutter/durable_queue_flutter.dart';
import 'package:flutter/widgets.dart';
import 'package:flutter_test/flutter_test.dart';

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

DurableQueue newQueue() {
  // A fake clock keeps the test free of real timers.
  return DurableQueue(
    storage: MemoryQueueStorage(),
    clock: FakeQueueClock(DateTime.utc(2026, 10, 20)),
  )..register<Note>(
    type: 'note',
    decoder: Note.fromJson,
    handler: (task, context) async {},
  );
}

const _order = [
  AppLifecycleState.resumed,
  AppLifecycleState.inactive,
  AppLifecycleState.hidden,
  AppLifecycleState.paused,
];

/// Moves the app lifecycle to [target] one legal step at a time.
Future<void> moveTo(WidgetTester tester, AppLifecycleState target) async {
  final current = tester.binding.lifecycleState;
  void set(AppLifecycleState state) {
    // ignore: invalid_use_of_protected_member
    tester.binding.handleAppLifecycleStateChanged(state);
  }

  if (current == null || !_order.contains(current)) {
    set(target);
  } else {
    var i = _order.indexOf(current);
    final j = _order.indexOf(target);
    while (i != j) {
      i += j > i ? 1 : -1;
      set(_order[i]);
    }
  }
  await settle(tester);
}

Future<void> settle(WidgetTester tester) async {
  for (var i = 0; i < 5; i++) {
    await tester.pump();
  }
}

Widget app(DurableQueue queue, Widget child, {bool pause = true}) {
  return Directionality(
    textDirection: TextDirection.ltr,
    child: DurableQueueScope(
      queue: queue,
      pauseInBackground: pause,
      child: child,
    ),
  );
}

void main() {
  testWidgets('the scope starts the queue and provides it', (tester) async {
    final queue = newQueue();
    DurableQueue? found;
    await tester.pumpWidget(
      app(
        queue,
        Builder(
          builder: (context) {
            found = DurableQueueScope.of(context);
            return const SizedBox();
          },
        ),
      ),
    );
    await settle(tester);
    expect(found, same(queue));
    expect(queue.runState, QueueRunState.running);
    await tester.pumpWidget(const SizedBox());
    await queue.close();
  });

  testWidgets('with runQueue false the scope only provides the queue', (
    tester,
  ) async {
    final queue = newQueue();
    DurableQueue? found;
    await tester.pumpWidget(
      Directionality(
        textDirection: TextDirection.ltr,
        child: DurableQueueScope(
          queue: queue,
          runQueue: false,
          child: Builder(
            builder: (context) {
              found = DurableQueueScope.of(context);
              return const SizedBox();
            },
          ),
        ),
      ),
    );
    await settle(tester);
    expect(found, same(queue));
    expect(queue.runState, QueueRunState.idle);
    await tester.pumpWidget(const SizedBox());
    await settle(tester);
    expect(queue.isClosed, isFalse);
    await queue.close();
  });

  testWidgets('maybeOf is null without a scope', (tester) async {
    DurableQueue? found;
    var called = false;
    await tester.pumpWidget(
      Builder(
        builder: (context) {
          found = DurableQueueScope.maybeOf(context);
          called = true;
          return const SizedBox();
        },
      ),
    );
    expect(called, isTrue);
    expect(found, isNull);
  });

  testWidgets('the queue pauses in the background and resumes', (tester) async {
    addTearDown(() => moveTo(tester, AppLifecycleState.resumed));
    await moveTo(tester, AppLifecycleState.resumed);
    final queue = newQueue();
    await tester.pumpWidget(app(queue, const SizedBox()));
    await settle(tester);
    expect(queue.runState, QueueRunState.running);

    await moveTo(tester, AppLifecycleState.paused);
    expect(queue.runState, QueueRunState.paused);

    await moveTo(tester, AppLifecycleState.resumed);
    expect(queue.runState, QueueRunState.running);

    await tester.pumpWidget(const SizedBox());
    await settle(tester);
    await queue.close();
  });

  testWidgets('QueueTaskBuilder shows the latest status', (tester) async {
    final queue = newQueue();
    final id = await queue.enqueue(Note('a'));
    await tester.pumpWidget(
      app(
        queue,
        QueueTaskBuilder(
          taskId: id,
          builder: (context, task) => Text(task?.status.name ?? 'loading'),
        ),
      ),
    );
    await settle(tester);
    expect(find.text('completed'), findsOneWidget);
    await tester.pumpWidget(const SizedBox());
    await settle(tester);
    await queue.close();
  });

  testWidgets('QueueGroupBuilder lists the group', (tester) async {
    final queue = newQueue();
    await queue.enqueue(Note('a'), group: 'album');
    await queue.enqueue(Note('b'), group: 'album');
    await tester.pumpWidget(
      app(
        queue,
        QueueGroupBuilder(
          group: 'album',
          builder: (context, tasks) => Text('${tasks.length} tasks'),
        ),
      ),
    );
    await settle(tester);
    expect(find.text('2 tasks'), findsOneWidget);
    await tester.pumpWidget(const SizedBox());
    await settle(tester);
    await queue.close();
  });
}
