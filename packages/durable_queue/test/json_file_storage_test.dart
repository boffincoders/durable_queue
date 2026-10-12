import 'dart:io';

import 'package:durable_queue/durable_queue.dart';
import 'package:test/test.dart';

import '../example/json_file_storage.dart';
import 'storage_contract.dart';
import 'support/harness.dart';

void main() {
  final directory = Directory.systemTemp.createTempSync('durable_queue_');
  var files = 0;
  String nextPath() => '${directory.path}/queue-${files++}.json';

  tearDownAll(() => directory.deleteSync(recursive: true));

  group('JsonFileStorage contract', () {
    queueStorageContractTests(() => JsonFileStorage(nextPath()));
  });

  test('tasks survive reopening the file', () async {
    final path = nextPath();
    final first = JsonFileStorage(path);
    await first.save(
      storedTask(id: 'a', priority: 2, group: 'photos', sequence: 1),
    );
    await first.save(
      storedTask(
        id: 'b',
        status: TaskStatus.waiting,
        dependsOn: ['a'],
        sequence: 2,
      ),
    );
    await first.update(
      (await first.get('a'))!.copyWith(status: TaskStatus.running),
    );
    await first.delete('missing');

    final reopened = JsonFileStorage(path);
    expect((await reopened.get('a'))?.status, TaskStatus.running);
    expect((await reopened.get('a'))?.group, 'photos');
    expect((await reopened.getWaitingDependents('a')).map((task) => task.id), [
      'b',
    ]);
    expect(await reopened.getMaxSequence(), 2);
  });

  test('a queue resumes interrupted work from the file', () async {
    final path = nextPath();
    final before = QueueHarness(storage: JsonFileStorage(path));
    before.register();
    final id = await before.queue.enqueue(ValueTask('photo'));

    final after = QueueHarness(storage: JsonFileStorage(path));
    after.register();
    await after.queue.start();
    // Real file I/O takes milliseconds, so poll on wall-clock time.
    for (var i = 0; i < 300 && after.of<TaskCompleted>().isEmpty; i++) {
      await Future<void>.delayed(const Duration(milliseconds: 10));
    }
    expect(after.of<TaskCompleted>(), hasLength(1));
    await after.queue.close();
    expect((await JsonFileStorage(path).get(id))?.status, TaskStatus.completed);
  });

  test('a corrupt file is reported on first use', () async {
    final path = nextPath();
    File(path).writeAsStringSync('{"tasks": 3}');
    await expectLater(
      JsonFileStorage(path).get('a'),
      throwsA(isA<FormatException>()),
    );
  });

  test('a failed file write leaves memory unchanged', () async {
    final path = '${directory.path}/missing-dir/queue.json';
    final storage = JsonFileStorage(path);
    await expectLater(
      storage.save(storedTask(id: 'a')),
      throwsA(isA<FileSystemException>()),
    );
    expect(await storage.get('a'), isNull);
    await expectLater(
      storage.save(storedTask(id: 'b')),
      throwsA(isA<FileSystemException>()),
    );
    expect(await storage.getAll(), isEmpty);
  });
}
