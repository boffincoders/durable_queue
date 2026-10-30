import 'dart:async';

import 'package:background_downloader/background_downloader.dart';
import 'package:durable_queue/durable_queue.dart' as dq;
import 'package:durable_queue_transfer/durable_queue_transfer.dart';
import 'package:flutter_test/flutter_test.dart';

import 'support/fake_downloader.dart';

final class PhotoUpload extends dq.DurableTask {
  PhotoUpload(this.file);

  final String file;

  @override
  String get type => 'photo_upload';

  @override
  Map<String, dynamic> toJson() => {'file': file};

  static PhotoUpload fromJson(Map<String, dynamic> json) =>
      PhotoUpload(json['file'] as String);
}

final class Note extends dq.DurableTask {
  Note(this.text);

  final String text;

  @override
  String get type => 'note';

  @override
  Map<String, dynamic> toJson() => {'text': text};

  static Note fromJson(Map<String, dynamic> json) =>
      Note(json['text'] as String);
}

Future<void> until(bool Function() condition) async {
  for (var i = 0; i < 200; i++) {
    if (condition()) return;
    await Future<void>.delayed(Duration.zero);
  }
  if (!condition()) throw StateError('Condition not reached');
}

final class FileDownload extends dq.DurableTask {
  FileDownload(this.name);

  final String name;

  @override
  String get type => 'download';

  @override
  Map<String, dynamic> toJson() => {'name': name};

  static FileDownload fromJson(Map<String, dynamic> json) =>
      FileDownload(json['name'] as String);
}

void main() {
  late dq.FakeQueueClock clock;
  late FakeDownloader files;
  late dq.DurableQueue queue;
  late QueueTransfers transfers;
  late List<TransferResult> results;
  late List<Object> errors;
  late List<String> notes;
  FutureOr<void> Function(TransferResult)? onComplete;

  setUp(() async {
    clock = dq.FakeQueueClock(DateTime.utc(2026, 10, 22));
    files = FakeDownloader();
    queue = dq.DurableQueue(storage: dq.MemoryQueueStorage(), clock: clock);
    results = [];
    errors = [];
    notes = [];
    onComplete = null;
    transfers = QueueTransfers(
      queue,
      downloader: files,
      clock: clock,
      onComplete: (result) {
        results.add(result);
        return onComplete?.call(result);
      },
      onError: (error, _) => errors.add(error),
    );
    queue
      ..register<PhotoUpload>(
        type: 'photo_upload',
        decoder: PhotoUpload.fromJson,
        retryIf: isRetryableTransferError,
        handler: (task, context) => transfers.start(
          context,
          UploadTask(
            url: 'https://example.com/upload',
            filename: task.file,
            group: 'ignored',
          ),
        ),
      )
      ..register<Note>(
        type: 'note',
        decoder: Note.fromJson,
        handler: (task, context) async => notes.add(task.text),
      );
  });

  tearDown(() async {
    await transfers.dispose();
    await queue.close();
  });

  Future<String> enqueueUpload({int maxAttempts = 3}) {
    return queue.enqueue(
      PhotoUpload('a.jpg'),
      retryPolicy: dq.RetryPolicy.fixed(
        maxAttempts: maxAttempts,
        delay: Duration.zero,
      ),
    );
  }

  Future<dq.StoredTask> stored(String id) async => (await queue.getTask(id))!;

  Future<void> startAndWaitForTransfer([int count = 1]) async {
    if (queue.runState == dq.QueueRunState.idle) await queue.start();
    await until(() => files.enqueued.length >= count);
    await pumpEventQueue();
  }

  test('start enqueues the transfer and hands the task off', () async {
    await transfers.attach();
    final id = await enqueueUpload();
    await startAndWaitForTransfer();
    final transfer = files.enqueued.single;
    expect(transfer.taskId, 'dq-1-$id');
    expect(transfer.group, QueueTransfers.defaultGroup);
    expect(transfer.url, 'https://example.com/upload');
    expect(transfer.filename, 'a.jpg');
    final task = await stored(id);
    expect(task.isHandedOff, isTrue);
    expect(task.handOffReference, 'dq-1-$id');
  });

  test(
    'a completed transfer completes the task and passes the response',
    () async {
      await transfers.attach();
      final ids = await queue.enqueueChain([
        PhotoUpload('a.jpg'),
        Note('sent'),
      ]);
      await startAndWaitForTransfer();
      final transfer = files.enqueued.single;
      files.emit(
        TaskStatusUpdate(
          transfer,
          TaskStatus.complete,
          null,
          '{"photoId": 7}',
          {'content-type': 'application/json'},
          201,
        ),
      );
      await until(() => notes.isNotEmpty);
      expect((await stored(ids.first)).status, dq.TaskStatus.completed);
      final result = results.single;
      expect(result.queueTaskId, ids.first);
      expect(result.transfer.taskId, transfer.taskId);
      expect(result.httpStatusCode, 201);
      expect(result.responseBody, '{"photoId": 7}');
      expect(result.responseHeaders, {'content-type': 'application/json'});
      expect(notes, ['sent']);
    },
  );

  test('an onComplete error fails the task with that error', () async {
    await transfers.attach();
    final id = await enqueueUpload(maxAttempts: 2);
    onComplete = (_) => throw StateError('could not save the photo id');
    await startAndWaitForTransfer();
    files.emit(TaskStatusUpdate(files.enqueued.single, TaskStatus.complete));
    await startAndWaitForTransfer(2);
    expect(files.enqueued.last.taskId, 'dq-2-$id');
    final task = await stored(id);
    expect(task.lastFailure!.error, contains('could not save the photo id'));
    expect(task.handOffReference, 'dq-2-$id');
  });

  test('retryable failures retry with a new transfer', () async {
    await transfers.attach();
    final id = await enqueueUpload();
    await startAndWaitForTransfer();
    files.emit(
      TaskStatusUpdate(
        files.enqueued.single,
        TaskStatus.failed,
        TaskHttpException('Service unavailable', 503),
      ),
    );
    await startAndWaitForTransfer(2);
    final task = await stored(id);
    expect(task.attempts, 2);
    expect(task.lastFailure!.error, contains('HTTP 503'));
    expect(task.isHandedOff, isTrue);
  });

  test('permanent failures fail the task', () async {
    await transfers.attach();
    final id = await enqueueUpload();
    await startAndWaitForTransfer();
    files.emit(
      TaskStatusUpdate(
        files.enqueued.single,
        TaskStatus.failed,
        TaskException('Forbidden'),
        'denied',
        null,
        403,
      ),
    );
    await pumpEventQueue();
    final task = await stored(id);
    expect(task.status, dq.TaskStatus.failed);
    expect(task.lastFailure!.error, contains('HTTP 403'));
    expect(files.enqueued, hasLength(1));
  });

  test('notFound fails the task', () async {
    await transfers.attach();
    final id = await enqueueUpload();
    await startAndWaitForTransfer();
    files.emit(TaskStatusUpdate(files.enqueued.single, TaskStatus.notFound));
    await pumpEventQueue();
    expect((await stored(id)).status, dq.TaskStatus.failed);
  });

  test('a cancelled transfer cancels the task', () async {
    await transfers.attach();
    final id = await enqueueUpload();
    await startAndWaitForTransfer();
    files.emit(TaskStatusUpdate(files.enqueued.single, TaskStatus.canceled));
    await pumpEventQueue();
    expect((await stored(id)).status, dq.TaskStatus.cancelled);
  });

  test('cancelling the task cancels the transfer', () async {
    await transfers.attach();
    final id = await enqueueUpload();
    await startAndWaitForTransfer();
    await queue.cancel(id);
    await until(() => files.cancelled.isNotEmpty);
    expect(files.cancelled, ['dq-1-$id']);
    // background_downloader then reports the cancellation; nothing changes.
    files.emit(TaskStatusUpdate(files.enqueued.single, TaskStatus.canceled));
    await pumpEventQueue();
    expect((await stored(id)).status, dq.TaskStatus.cancelled);
    expect(errors, isEmpty);
  });

  test('other tasks, other states, and old attempts are ignored', () async {
    await transfers.attach();
    final id = await enqueueUpload();
    await startAndWaitForTransfer();
    final transfer = files.enqueued.single;
    for (final status in [
      TaskStatus.enqueued,
      TaskStatus.running,
      TaskStatus.waitingToRetry,
      TaskStatus.paused,
    ]) {
      files.emit(TaskStatusUpdate(transfer, status));
    }
    files.emit(
      TaskStatusUpdate(
        DownloadTask(
          taskId: 'not-ours',
          url: 'https://example.com',
          group: QueueTransfers.defaultGroup,
        ),
        TaskStatus.complete,
      ),
    );
    files.emit(
      TaskStatusUpdate(
        transfer.copyWith(taskId: 'dq-9-$id'),
        TaskStatus.complete,
      ),
    );
    await pumpEventQueue();
    expect((await stored(id)).isHandedOff, isTrue);
    expect(results, isEmpty, reason: 'onComplete only for the current attempt');
    expect(errors, isEmpty);
  });

  test('a transfer that could not be enqueued is a normal failure', () async {
    await transfers.attach();
    files.enqueueResult = false;
    final id = await enqueueUpload(maxAttempts: 1);
    await queue.start();
    await until(() => files.enqueued.isNotEmpty);
    await pumpEventQueue();
    final task = await stored(id);
    expect(task.status, dq.TaskStatus.failed);
    expect(task.lastFailure!.error, contains('TransferEnqueueException'));
  });

  test('attach registers, tracks, resumes, then reconciles', () async {
    await transfers.attach();
    expect(files.callbacks.keys, [QueueTransfers.defaultGroup]);
    expect(files.tracked, [QueueTransfers.defaultGroup]);
    expect(files.resumeCalls, 1);
    expect(files.rescheduleCalls, 0);
    clock.advance(const Duration(seconds: 5));
    await until(() => files.rescheduleCalls == 1);

    await transfers.attach();
    expect(files.tracked, hasLength(1), reason: 'attach twice does nothing');
  });

  test('rescheduleKilled can be turned off', () async {
    final quiet = QueueTransfers(
      queue,
      downloader: files,
      clock: clock,
      rescheduleKilled: false,
    );
    await quiet.attach();
    clock.advance(const Duration(seconds: 5));
    await pumpEventQueue();
    expect(files.rescheduleCalls, 0);
    await quiet.dispose();
  });

  group('reconcile', () {
    Future<String> handedOffUpload() async {
      final id = await enqueueUpload();
      await queue.drain();
      expect((await stored(id)).isHandedOff, isTrue);
      return id;
    }

    test('applies outcomes recorded while the app was closed', () async {
      final done = await handedOffUpload();
      final failed = await handedOffUpload();
      final going = await handedOffUpload();
      final byQueueId = {
        for (final t in files.enqueued)
          t.taskId.split('-').skip(2).join('-'): t,
      };
      files.fakeDatabase.records['dq-1-$done'] = TaskRecord(
        byQueueId[done]!,
        TaskStatus.complete,
        1,
        10,
      );
      files.fakeDatabase.records['dq-1-$failed'] = TaskRecord(
        byQueueId[failed]!,
        TaskStatus.failed,
        -1,
        10,
        TaskUrlException('bad url'),
      );
      files.fakeDatabase.records['dq-1-$going'] = TaskRecord(
        byQueueId[going]!,
        TaskStatus.running,
        0.5,
        10,
      );
      await transfers.reconcile();
      expect((await stored(done)).status, dq.TaskStatus.completed);
      expect(results.single.responseBody, isNull);
      expect((await stored(failed)).status, dq.TaskStatus.failed);
      expect((await stored(failed)).lastFailure!.error, contains('bad url'));
      expect((await stored(going)).isHandedOff, isTrue);
    });

    test('a transfer nobody knows is lost after lostAfter', () async {
      final old = await handedOffUpload();
      clock.advance(const Duration(minutes: 3));
      await transfers.reconcile();
      expect((await stored(old)).isHandedOff, isTrue, reason: 'still queued');

      final recent = await handedOffUpload();
      files.native.clear();
      await transfers.reconcile();
      expect((await stored(recent)).isHandedOff, isTrue, reason: 'too recent');
      final lost = await stored(old);
      expect(lost.status, dq.TaskStatus.retryScheduled);
      expect(lost.lastFailure!.error, contains('TransferLostException'));
    });

    test('hand-offs of other kinds are left alone', () async {
      queue.register<_Other>(
        type: 'other',
        decoder: (json) => _Other(),
        handler: (task, context) async => context.handOff('elsewhere'),
      );
      final other = await queue.enqueue(_Other());
      await queue.drain();
      clock.advance(const Duration(hours: 1));
      await transfers.reconcile();
      expect((await stored(other)).isHandedOff, isTrue);
      await transfers.attach();
      await queue.cancel(other);
      await pumpEventQueue();
      expect(files.cancelled, isEmpty);
    });

    test('errors are reported and the rest continues', () async {
      final id = await handedOffUpload();
      await queue.close();
      files.native.clear();
      clock.advance(const Duration(minutes: 5));
      await transfers.reconcile();
      expect(errors.single, isA<StateError>());
      expect((await stored(id)).isHandedOff, isTrue);
    });
  });

  test('after dispose, updates are ignored', () async {
    await transfers.attach();
    final id = await enqueueUpload();
    await startAndWaitForTransfer();
    final transfer = files.enqueued.single;
    final callback = files.callbacks[QueueTransfers.defaultGroup]!;
    await transfers.dispose();
    expect(files.unregistered, [QueueTransfers.defaultGroup]);
    callback(TaskStatusUpdate(transfer, TaskStatus.complete));
    await queue.cancel(id);
    await pumpEventQueue();
    expect(files.cancelled, isEmpty);
    expect(results, isEmpty);
  });

  test('without onError, errors go to the zone', () async {
    final caught = <Object>[];
    await runZonedGuarded(() async {
      final plain = QueueTransfers(queue, downloader: files, clock: clock);
      await plain.attach();
      await queue.close();
      files.emit(
        TaskStatusUpdate(
          DownloadTask(
            taskId: 'dq-1-x',
            url: 'https://example.com',
            group: QueueTransfers.defaultGroup,
          ),
          TaskStatus.failed,
        ),
      );
      await pumpEventQueue();
    }, (error, _) => caught.add(error));
    expect(caught.single, isA<StateError>());
  });

  test('group must not be blank', () {
    expect(
      () => QueueTransfers(queue, downloader: files, group: ' '),
      throwsArgumentError,
    );
  });

  test(
    'a transfer that ends before its handler returns still counts',
    () async {
      final release = Completer<void>();
      queue.register<_Quick>(
        type: 'quick',
        decoder: (json) => _Quick(),
        handler: (task, context) async {
          final id = await transfers.start(
            context,
            DownloadTask(url: 'https://example.com/a'),
          );
          // The transfer finishes at once, before this handler returns.
          files.emit(
            TaskStatusUpdate(
              files.enqueued.singleWhere((t) => t.taskId == id),
              TaskStatus.complete,
            ),
          );
          await release.future;
        },
      );
      await transfers.attach();
      final id = await queue.enqueue(_Quick());
      await queue.start();
      await until(() => results.isNotEmpty);
      await pumpEventQueue();
      expect((await stored(id)).status, dq.TaskStatus.running);
      release.complete();
      await pumpEventQueue();
      expect((await stored(id)).status, dq.TaskStatus.completed);
    },
  );

  group('hold and release', () {
    late List<dq.TaskHoldChanged> holds;

    setUp(() {
      holds = [];
      queue.holdChanges.listen(holds.add);
      queue.register<FileDownload>(
        type: 'download',
        decoder: FileDownload.fromJson,
        handler: (task, context) => transfers.start(
          context,
          DownloadTask(
            url: 'https://example.com/${task.name}',
            allowPause: task.name.startsWith('pausable'),
          ),
        ),
      );
    });

    Future<String> enqueueDownload(String name) => queue.enqueue(
      FileDownload(name),
      retryPolicy: dq.RetryPolicy.fixed(maxAttempts: 3, delay: Duration.zero),
    );

    test('a held upload is stopped and starts again once released', () async {
      await transfers.attach();
      final id = await enqueueUpload();
      await startAndWaitForTransfer();
      final first = files.enqueued.single;
      // background_downloader keeps a record of the stopped transfer.
      files.fakeDatabase.records[first.taskId] = TaskRecord(
        first,
        TaskStatus.canceled,
        0,
        10,
      );

      expect(await queue.hold(id), isTrue);
      await until(() => files.cancelled.isNotEmpty);
      await pumpEventQueue();
      expect(files.cancelled, ['dq-1-$id']);
      var task = await stored(id);
      expect(task.status, dq.TaskStatus.pending);
      expect(task.held, isTrue);
      expect(task.attempts, 0);

      // The late report of the stopped transfer changes nothing.
      files.emit(TaskStatusUpdate(first, TaskStatus.canceled));
      await pumpEventQueue();
      expect((await stored(id)).status, dq.TaskStatus.pending);

      await queue.release(id);
      await startAndWaitForTransfer(2);
      final second = files.enqueued.last;
      expect(second.taskId, 'dq-1.1-$id');
      task = await stored(id);
      expect(task.isHandedOff, isTrue);
      expect(task.handOffReference, second.taskId);

      files.emit(TaskStatusUpdate(second, TaskStatus.complete));
      await pumpEventQueue();
      expect((await stored(id)).status, dq.TaskStatus.completed);
      expect(errors, isEmpty);
    });

    test('a pausable download is paused and resumed', () async {
      await transfers.attach();
      final id = await enqueueDownload('pausable.zip');
      await startAndWaitForTransfer();
      final transfer = files.enqueued.single;

      await queue.hold(id);
      await until(() => files.pauses.isNotEmpty);
      await pumpEventQueue();
      expect(files.pauses, [transfer.taskId]);
      expect(files.cancelled, isEmpty);
      var task = await stored(id);
      expect(task.status, dq.TaskStatus.running);
      expect(task.isHandedOff, isTrue);
      expect(task.held, isTrue);

      await queue.release(id);
      await until(() => files.resumes.isNotEmpty);
      await pumpEventQueue();
      expect(files.resumes, [transfer.taskId]);
      expect(files.enqueued, hasLength(1));
      task = await stored(id);
      expect(task.held, isFalse);
      expect(task.isHandedOff, isTrue);

      files.emit(TaskStatusUpdate(transfer, TaskStatus.complete));
      await pumpEventQueue();
      expect((await stored(id)).status, dq.TaskStatus.completed);
    });

    test('a download that cannot resume is downloaded again', () async {
      await transfers.attach();
      final id = await enqueueDownload('pausable.zip');
      await startAndWaitForTransfer();
      await queue.hold(id);
      await until(() => files.pauses.isNotEmpty);
      files.resumeResult = false;
      await queue.release(id);
      await until(() => files.enqueued.length == 2);
      expect(files.enqueued.last.taskId, files.enqueued.first.taskId);
      expect((await stored(id)).isHandedOff, isTrue);
    });

    test('a download without allowPause is stopped like an upload', () async {
      await transfers.attach();
      final id = await enqueueDownload('plain.zip');
      await startAndWaitForTransfer();
      await queue.hold(id);
      await until(() => files.cancelled.isNotEmpty);
      await pumpEventQueue();
      expect(files.pauses, hasLength(1));
      final task = await stored(id);
      expect(task.status, dq.TaskStatus.pending);
      expect(task.held, isTrue);
    });

    test('reconcile pauses a held transfer the app did not get to', () async {
      final id = await enqueueDownload('pausable.zip');
      await startAndWaitForTransfer();
      await queue.hold(id);
      await pumpEventQueue();
      expect(files.pauses, isEmpty, reason: 'not attached');
      await transfers.reconcile();
      expect(files.pauses, hasLength(1));
    });

    test('reconcile resumes a released transfer still paused', () async {
      final id = await enqueueDownload('pausable.zip');
      await startAndWaitForTransfer();
      final transfer = files.enqueued.single;
      files.fakeDatabase.records[transfer.taskId] = TaskRecord(
        transfer,
        TaskStatus.paused,
        0.5,
        10,
      );
      await transfers.reconcile();
      expect(files.resumes, [transfer.taskId]);
      expect((await stored(id)).isHandedOff, isTrue);
    });

    test('a running transfer that was never paused is left alone', () async {
      await transfers.attach();
      final id = await enqueueDownload('pausable.zip');
      await startAndWaitForTransfer();
      // Simulate the pause not having taken effect yet.
      files.resumeResult = false;
      await transfers.dispose();
      await queue.hold(id);
      await queue.release(id);
      await transfers.reconcile();
      expect(files.resumes, isEmpty);
      expect(files.enqueued, hasLength(1));
    });

    test('nothing happens after dispose', () async {
      await transfers.attach();
      final id = await enqueueUpload();
      await startAndWaitForTransfer();
      await transfers.dispose();
      await queue.hold(id);
      await pumpEventQueue();
      expect(files.cancelled, isEmpty);
      expect(holds, hasLength(1));
    });
  });
}

final class _Quick extends dq.DurableTask {
  @override
  String get type => 'quick';

  @override
  Map<String, dynamic> toJson() => {'text': 'q'};
}

final class _Other extends dq.DurableTask {
  @override
  String get type => 'other';

  @override
  Map<String, dynamic> toJson() => {'text': 'x'};
}
