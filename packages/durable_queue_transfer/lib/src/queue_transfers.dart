import 'dart:async';

import 'package:background_downloader/background_downloader.dart'
    show
        DownloadTask,
        FileDownloader,
        Task,
        TaskException,
        TaskHttpException,
        TaskStatus,
        TaskStatusUpdate;
import 'package:durable_queue/durable_queue.dart'
    show
        DurableQueue,
        QueueClock,
        QueueDelay,
        QueueEvent,
        StoredTask,
        SystemQueueClock,
        TaskCancelled,
        TaskCancelledException,
        TaskContext,
        TaskHoldChanged;
import 'package:durable_queue/durable_queue.dart' as dq show TaskStatus;

import 'transfer_errors.dart';
import 'transfer_ids.dart';

/// The outcome of a successful transfer, passed to
/// [QueueTransfers.onComplete].
final class TransferResult {
  /// Creates a result.
  const TransferResult({
    required this.queueTaskId,
    required this.transfer,
    this.httpStatusCode,
    this.responseBody,
    this.responseHeaders,
  });

  /// The queue task that started the transfer.
  final String queueTaskId;

  /// The background_downloader task, with the id and group set by
  /// [QueueTransfers.start].
  final Task transfer;

  /// The HTTP status code, when background_downloader passed it on.
  final int? httpStatusCode;

  /// The response body, when background_downloader passed it on. It is not
  /// available for a transfer that finished while the app was not running
  /// and is found later in background_downloader's records.
  final String? responseBody;

  /// The response headers, when background_downloader passed them on.
  final Map<String, String>? responseHeaders;
}

/// Runs uploads and downloads for a [DurableQueue] through
/// background_downloader, so they continue while the app is in the
/// background or closed.
///
/// A handler starts the transfer with [start] and returns. The queue task is
/// handed off (see `TaskContext.handOff`) and stays running until the
/// transfer ends; then it is completed, or failed so the task type's retry
/// rules apply. Tasks that depend on it run after it completes.
///
/// Call [attach] once per app launch, in the app's main isolate, so results
/// that arrived while the app was not running are applied.
///
/// `queue.hold(id)` pauses a transfer and `queue.release(id)` resumes it.
/// A download created with `allowPause: true` is paused by
/// background_downloader and continues where it stopped, when the server
/// supports it. Any other transfer is cancelled, its queue task goes back to
/// the queue held, and it starts again from the beginning once released.
///
/// ```dart
/// final transfers = QueueTransfers(queue);
/// queue.register<PhotoUpload>(
///   type: 'photo_upload',
///   decoder: PhotoUpload.fromJson,
///   retryIf: isRetryableTransferError,
///   handler: (task, context) => transfers.start(
///     context,
///     UploadTask(url: uploadUrl, filename: task.fileName),
///   ),
/// );
/// await transfers.attach();
/// ```
final class QueueTransfers {
  /// Creates transfers for [queue].
  ///
  /// * [downloader] defaults to `FileDownloader()`.
  /// * [group] is the background_downloader group used for every transfer
  ///   started here. Do not use it for other transfers.
  /// * [onComplete] runs when a transfer succeeds, before the queue task is
  ///   completed, for example to store the server's response. If it throws,
  ///   the queue task fails with that error.
  /// * [onError] receives errors that have no caller, such as a failed
  ///   update of the queue from a background_downloader callback.
  /// * [lostAfter]: a handed-off task whose transfer background_downloader
  ///   does not know is failed with [TransferLostException] once it has been
  ///   handed off for this long.
  /// * [settleDelay]: how long [attach] waits for background_downloader to
  ///   deliver stored updates before it reschedules killed transfers and
  ///   reconciles.
  /// * [rescheduleKilled]: whether [attach] calls
  ///   `FileDownloader.rescheduleKilledTasks`, which restarts tracked
  ///   transfers that were killed with the app.
  QueueTransfers(
    this.queue, {
    FileDownloader? downloader,
    this.group = defaultGroup,
    this.onComplete,
    this.onError,
    this.lostAfter = const Duration(minutes: 2),
    this.settleDelay = const Duration(seconds: 5),
    this.rescheduleKilled = true,
    QueueClock clock = const SystemQueueClock(),
  }) : _downloader = downloader,
       _clock = clock {
    if (group.trim().isEmpty) {
      throw ArgumentError.value(group, 'group', 'Must not be blank');
    }
  }

  /// The default background_downloader group.
  static const defaultGroup = 'durable_queue';

  /// The queue whose tasks run the transfers.
  final DurableQueue queue;

  /// The background_downloader group of the transfers started here.
  final String group;

  /// Runs when a transfer succeeds, before its queue task completes.
  final FutureOr<void> Function(TransferResult result)? onComplete;

  /// Receives errors that have no caller.
  final void Function(Object error, StackTrace stackTrace)? onError;

  /// How long a hand-off may go without a known transfer before it counts as
  /// lost.
  final Duration lostAfter;

  /// How long [attach] waits before reconciling.
  final Duration settleDelay;

  /// Whether [attach] reschedules transfers killed with the app.
  final bool rescheduleKilled;

  final FileDownloader? _downloader;
  final QueueClock _clock;
  StreamSubscription<QueueEvent>? _events;
  StreamSubscription<TaskHoldChanged>? _holds;
  QueueDelay? _settle;
  var _attached = false;
  var _disposed = false;

  FileDownloader get _files => _downloader ?? FileDownloader();

  /// Starts [transfer] and hands the queue task of [context] off to it.
  /// Call it from a handler and return.
  ///
  /// The transfer gets an id that names the queue task and attempt, and
  /// [group]; its other settings, such as the file, URL, headers, and
  /// notifications, are kept. Returns the transfer id.
  ///
  /// Throws [TransferEnqueueException] if background_downloader does not
  /// enqueue it, which the handler passes on as a normal failure.
  Future<String> start(TaskContext context, Task transfer) async {
    final id = await _freshId(context);
    final task = transfer.copyWith(taskId: id, group: group);
    // Saved before the transfer starts, so a crash in between is reconciled
    // as a lost transfer rather than starting a second one.
    context.handOff(id);
    if (!await _files.enqueue(task)) throw TransferEnqueueException(id);
    return id;
  }

  /// The transfer id for [context]'s attempt. An attempt that starts again
  /// after a hold stopped its transfer gets a new id, so late updates of the
  /// stopped transfer cannot be mistaken for the new one.
  Future<String> _freshId(TaskContext context) async {
    for (var restart = 0; ; restart++) {
      final id = TransferIds.build(context.taskId, context.attempt, restart);
      if (await _files.database.recordForId(id) == null) return id;
    }
  }

  /// Starts applying transfer results to the queue: registers for
  /// background_downloader updates of [group], turns on its record keeping
  /// for [group], and asks for updates stored while the app was not running.
  /// After [settleDelay] it reschedules killed transfers (see
  /// [rescheduleKilled]) and runs [reconcile].
  ///
  /// Also cancels a transfer when its queue task is cancelled, and pauses or
  /// resumes it when its queue task is held or released (see
  /// `DurableQueue.hold`). Calling it again does nothing.
  Future<void> attach() async {
    if (_attached || _disposed) return;
    _attached = true;
    _files.registerCallbacks(group: group, taskStatusCallback: _onStatus);
    _events = queue.events.listen(_onQueueEvent);
    _holds = queue.holdChanges.listen(_onHoldChange);
    await _files.trackTasksInGroup(group);
    await _files.resumeFromBackground();
    final settle = _settle = _clock.delayUntil(_clock.now().add(settleDelay));
    unawaited(
      settle.future
          .then((_) async {
            if (_disposed) return;
            if (rescheduleKilled) await _files.rescheduleKilledTasks();
            await reconcile();
          })
          .catchError(_report),
    );
  }

  /// Applies the outcome of every handed-off transfer that background_downloader
  /// recorded as finished, and fails hand-offs whose transfer it does not
  /// know (see [lostAfter]). Pauses transfers of held tasks and resumes
  /// paused transfers of released ones, in case the app stopped before it
  /// could. Other transfers still running are left alone.
  ///
  /// [attach] runs it once. Call it again, for example when the app returns
  /// to the foreground, if you like; it is safe to repeat.
  Future<void> reconcile() async {
    final running = await queue.getTasks(status: dq.TaskStatus.running);
    for (final task in running) {
      final reference = task.handOffReference;
      if (!task.isHandedOff || reference == null) continue;
      if (TransferIds.queueTaskId(reference) != task.id) continue;
      try {
        final record = await _files.database.recordForId(reference);
        if (record != null) {
          if (record.status.isFinalState) {
            await _apply(record.task, record.status, record.exception, null);
          } else {
            await _syncHold(task);
          }
          continue;
        }
        if (await _files.taskForId(reference) != null) {
          await _syncHold(task);
          continue;
        }
        if (_clock.now().difference(task.updatedAt) < lostAfter) continue;
        await queue.failHandOff(
          task.id,
          TransferLostException(reference),
          reference: reference,
        );
      } on Object catch (error, stackTrace) {
        _report(error, stackTrace);
      }
    }
  }

  /// Stops applying results and cancelling transfers. Transfers keep
  /// running; the next [attach], for example on the next launch, picks up
  /// their results.
  Future<void> dispose() async {
    _disposed = true;
    _settle?.cancel();
    await _events?.cancel();
    await _holds?.cancel();
    if (_attached) {
      _files.unregisterCallbacks(group: group, callback: _onStatus);
    }
  }

  void _onStatus(TaskStatusUpdate update) {
    if (_disposed) return;
    unawaited(
      _apply(
        update.task,
        update.status,
        update.exception,
        update,
      ).catchError(_report),
    );
  }

  Future<void> _apply(
    Task transfer,
    TaskStatus status,
    TaskException? exception,
    TaskStatusUpdate? update,
  ) async {
    final reference = transfer.taskId;
    final id = TransferIds.queueTaskId(reference);
    if (id == null) return;
    switch (status) {
      case TaskStatus.complete:
        final complete = onComplete;
        if (complete != null) {
          final current = await queue.getTask(id);
          // Only for the attempt that is still waiting for this transfer.
          // Its reference may not be stored yet if the transfer ended
          // before the handler returned, so compare the attempt.
          if (current == null ||
              current.status != dq.TaskStatus.running ||
              current.attempts != TransferIds.attempt(reference)) {
            return;
          }
          try {
            await complete(
              TransferResult(
                queueTaskId: id,
                transfer: transfer,
                httpStatusCode: update?.responseStatusCode,
                responseBody: update?.responseBody,
                responseHeaders: update?.responseHeaders,
              ),
            );
          } on Object catch (error, stackTrace) {
            await queue.failHandOff(
              id,
              error,
              stackTrace: stackTrace,
              reference: reference,
            );
            return;
          }
        }
        await queue.completeHandOff(id, reference: reference);
      case TaskStatus.canceled:
        await queue.failHandOff(
          id,
          const TaskCancelledException(),
          reference: reference,
        );
      case TaskStatus.failed:
      case TaskStatus.notFound:
        await queue.failHandOff(
          id,
          TransferException(
            reference,
            status,
            httpStatusCode: _httpStatusCode(update, exception),
            cause: exception,
            responseBody: update?.responseBody,
          ),
          reference: reference,
        );
      case TaskStatus.enqueued:
      case TaskStatus.running:
      case TaskStatus.waitingToRetry:
      case TaskStatus.paused:
        return;
    }
  }

  static int? _httpStatusCode(
    TaskStatusUpdate? update,
    TaskException? exception,
  ) {
    final code = update?.responseStatusCode;
    if (code != null && code > 0) return code;
    if (exception is TaskHttpException && exception.httpResponseCode > 0) {
      return exception.httpResponseCode;
    }
    return null;
  }

  void _onQueueEvent(QueueEvent event) {
    if (event is! TaskCancelled || !event.wasRunning) return;
    unawaited(_cancelTransfer(event.taskId).catchError(_report));
  }

  Future<void> _cancelTransfer(String id) async {
    final task = await queue.getTask(id);
    final reference = task?.handOffReference;
    if (reference == null || TransferIds.queueTaskId(reference) != id) return;
    await _files.cancelTaskWithId(reference);
  }

  void _onHoldChange(TaskHoldChanged change) {
    if (_disposed || !change.isHandedOff) return;
    unawaited(
      queue
          .getTask(change.taskId)
          .then((task) async {
            if (task != null) await _syncHold(task);
          })
          .catchError(_report),
    );
  }

  /// Pauses the transfer of a held, handed-off [task], or resumes the paused
  /// transfer of a released one.
  Future<void> _syncHold(StoredTask task) async {
    final reference = task.handOffReference;
    if (!task.isHandedOff || reference == null) return;
    if (TransferIds.queueTaskId(reference) != task.id) return;
    final record = await _files.database.recordForId(reference);
    final paused = record?.status == TaskStatus.paused;
    final transfer = record?.task ?? await _files.taskForId(reference);
    if (task.held) {
      if (paused) return;
      if (transfer is DownloadTask && await _files.pause(transfer)) return;
      // Not pausable: stop it. The queue task goes back held and starts a
      // new transfer once released.
      await _files.cancelTaskWithId(reference);
      await queue.failHandOff(
        task.id,
        const TaskCancelledException(),
        reference: reference,
      );
      return;
    }
    if (!paused || transfer is! DownloadTask) return;
    if (await _files.resume(transfer)) return;
    // Nothing to resume from: download it again.
    if (!await _files.enqueue(transfer)) {
      await queue.failHandOff(
        task.id,
        TransferEnqueueException(reference),
        reference: reference,
      );
    }
  }

  void _report(Object error, StackTrace stackTrace) {
    final report = onError;
    if (report != null) {
      report(error, stackTrace);
    } else {
      Zone.current.handleUncaughtError(error, stackTrace);
    }
  }
}
