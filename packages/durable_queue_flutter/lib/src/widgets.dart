import 'dart:async';

import 'package:durable_queue/durable_queue.dart';
import 'package:flutter/widgets.dart';

import 'lifecycle.dart';
import 'watch.dart';

/// Makes a [DurableQueue] available to a widget subtree and runs it with
/// the app lifecycle.
///
/// When inserted, it starts the queue (if idle) and attaches a
/// [DurableQueueLifecycle] that pauses the queue in the background when
/// [pauseInBackground] is true. When removed, it detaches; with
/// [closeOnDispose] it also closes the queue.
///
/// ```dart
/// runApp(DurableQueueScope(queue: queue, child: const MyApp()));
///
/// // Anywhere below:
/// final queue = DurableQueueScope.of(context);
/// ```
///
/// [onStartError] receives errors from starting the queue, such as a failed
/// recovery. Without it they are reported through [FlutterError].
///
/// With [runQueue] set to false the scope only provides the queue and does
/// not start, pause, or resume it. Use that when something else runs the
/// queue, such as a `LeasedQueueRunner` from `durable_queue_workmanager`.
class DurableQueueScope extends StatefulWidget {
  /// Creates a scope for [queue].
  const DurableQueueScope({
    super.key,
    required this.queue,
    required this.child,
    this.pauseInBackground = true,
    this.closeOnDispose = false,
    this.onStartError,
    this.runQueue = true,
  });

  /// The queue provided to [child].
  final DurableQueue queue;

  /// The widget below this scope.
  final Widget child;

  /// Whether the queue is paused while the app is in the background.
  final bool pauseInBackground;

  /// Whether the queue is closed when this scope is removed.
  final bool closeOnDispose;

  /// Whether this scope starts the queue and follows the app lifecycle.
  /// When false, [pauseInBackground] and [onStartError] are not used.
  final bool runQueue;

  /// Called when starting the queue fails.
  final void Function(Object error, StackTrace stackTrace)? onStartError;

  /// The queue of the closest enclosing scope.
  ///
  /// Throws a [FlutterError] if there is none.
  static DurableQueue of(BuildContext context) {
    final queue = maybeOf(context);
    if (queue == null) {
      throw FlutterError(
        'DurableQueueScope.of() was called without a DurableQueueScope above '
        'the given context.',
      );
    }
    return queue;
  }

  /// The queue of the closest enclosing scope, or null.
  static DurableQueue? maybeOf(BuildContext context) {
    return context
        .dependOnInheritedWidgetOfExactType<_DurableQueueInherited>()
        ?.queue;
  }

  @override
  State<DurableQueueScope> createState() => _DurableQueueScopeState();
}

class _DurableQueueScopeState extends State<DurableQueueScope> {
  Future<DurableQueueLifecycle?>? _lifecycle;

  @override
  void initState() {
    super.initState();
    _attach();
  }

  @override
  void didUpdateWidget(DurableQueueScope oldWidget) {
    super.didUpdateWidget(oldWidget);
    if (!identical(oldWidget.queue, widget.queue) ||
        oldWidget.pauseInBackground != widget.pauseInBackground ||
        oldWidget.runQueue != widget.runQueue) {
      _detach(oldWidget.queue, close: oldWidget.closeOnDispose);
      _attach();
    }
  }

  void _attach() {
    if (!widget.runQueue) return;
    final queue = widget.queue;
    _lifecycle =
        DurableQueueLifecycle.attach(
          queue,
          pauseInBackground: widget.pauseInBackground,
        ).then<DurableQueueLifecycle?>(
          (lifecycle) => lifecycle,
          onError: (Object error, StackTrace stackTrace) {
            final report = widget.onStartError;
            if (report != null) {
              report(error, stackTrace);
            } else {
              FlutterError.reportError(
                FlutterErrorDetails(
                  exception: error,
                  stack: stackTrace,
                  library: 'durable_queue_flutter',
                  context: ErrorDescription('while starting a DurableQueue'),
                ),
              );
            }
            return null;
          },
        );
  }

  void _detach(DurableQueue queue, {required bool close}) {
    final pending = _lifecycle;
    _lifecycle = null;
    unawaited(() async {
      await (await pending)?.dispose();
      if (close) await queue.close();
    }());
  }

  @override
  void dispose() {
    _detach(widget.queue, close: widget.closeOnDispose);
    super.dispose();
  }

  @override
  Widget build(BuildContext context) {
    return _DurableQueueInherited(queue: widget.queue, child: widget.child);
  }
}

class _DurableQueueInherited extends InheritedWidget {
  const _DurableQueueInherited({required this.queue, required super.child});

  final DurableQueue queue;

  @override
  bool updateShouldNotify(_DurableQueueInherited oldWidget) =>
      !identical(queue, oldWidget.queue);
}

/// Rebuilds with the stored record of one task.
///
/// Uses [DurableQueueWatch.watchTask]. [queue] defaults to
/// `DurableQueueScope.of(context)`. The record is null while loading and
/// when the task is not stored.
///
/// ```dart
/// QueueTaskBuilder(
///   taskId: uploadId,
///   builder: (context, task) => Text(task?.status.name ?? '...'),
/// )
/// ```
class QueueTaskBuilder extends StatefulWidget {
  /// Creates a builder for [taskId].
  const QueueTaskBuilder({
    super.key,
    required this.taskId,
    required this.builder,
    this.queue,
  });

  /// The task to show.
  final String taskId;

  /// Builds the UI for the current record.
  final Widget Function(BuildContext context, StoredTask? task) builder;

  /// The queue; defaults to the enclosing [DurableQueueScope].
  final DurableQueue? queue;

  @override
  State<QueueTaskBuilder> createState() => _QueueTaskBuilderState();
}

class _QueueTaskBuilderState extends State<QueueTaskBuilder> {
  Stream<StoredTask?>? _stream;
  DurableQueue? _queue;

  @override
  void didChangeDependencies() {
    super.didChangeDependencies();
    _subscribe();
  }

  @override
  void didUpdateWidget(QueueTaskBuilder oldWidget) {
    super.didUpdateWidget(oldWidget);
    if (oldWidget.taskId != widget.taskId || oldWidget.queue != widget.queue) {
      _queue = null;
      _subscribe();
    }
  }

  void _subscribe() {
    final queue = widget.queue ?? DurableQueueScope.of(context);
    if (identical(queue, _queue) && _stream != null) return;
    _queue = queue;
    _stream = queue.watchTask(widget.taskId);
  }

  @override
  Widget build(BuildContext context) {
    return StreamBuilder<StoredTask?>(
      stream: _stream,
      builder: (context, snapshot) => widget.builder(context, snapshot.data),
    );
  }
}

/// Rebuilds with the tasks of one group, for example to show upload
/// progress for an album.
///
/// Uses [DurableQueueWatch.watchGroup]. [queue] defaults to
/// `DurableQueueScope.of(context)`. The list is empty while loading.
class QueueGroupBuilder extends StatefulWidget {
  /// Creates a builder for [group], optionally only tasks with [status].
  const QueueGroupBuilder({
    super.key,
    required this.group,
    required this.builder,
    this.status,
    this.queue,
  });

  /// The group to show.
  final String group;

  /// Only tasks with this status, or every task in the group when null.
  final TaskStatus? status;

  /// Builds the UI for the current tasks, oldest first.
  final Widget Function(BuildContext context, List<StoredTask> tasks) builder;

  /// The queue; defaults to the enclosing [DurableQueueScope].
  final DurableQueue? queue;

  @override
  State<QueueGroupBuilder> createState() => _QueueGroupBuilderState();
}

class _QueueGroupBuilderState extends State<QueueGroupBuilder> {
  Stream<List<StoredTask>>? _stream;
  DurableQueue? _queue;

  @override
  void didChangeDependencies() {
    super.didChangeDependencies();
    _subscribe();
  }

  @override
  void didUpdateWidget(QueueGroupBuilder oldWidget) {
    super.didUpdateWidget(oldWidget);
    if (oldWidget.group != widget.group ||
        oldWidget.status != widget.status ||
        oldWidget.queue != widget.queue) {
      _queue = null;
      _subscribe();
    }
  }

  void _subscribe() {
    final queue = widget.queue ?? DurableQueueScope.of(context);
    if (identical(queue, _queue) && _stream != null) return;
    _queue = queue;
    _stream = queue.watchGroup(widget.group, status: widget.status);
  }

  @override
  Widget build(BuildContext context) {
    return StreamBuilder<List<StoredTask>>(
      stream: _stream,
      builder: (context, snapshot) =>
          widget.builder(context, snapshot.data ?? const []),
    );
  }
}
