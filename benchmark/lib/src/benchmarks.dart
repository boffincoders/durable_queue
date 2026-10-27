import 'dart:io';

import 'package:durable_queue/durable_queue.dart';

import 'stats.dart';
import 'storages.dart';

/// How much work each benchmark does.
final class Sizes {
  const Sizes({
    required this.rounds,
    required this.enqueueWarmup,
    required this.enqueueCount,
    required this.drainCount,
    required this.recoveryCompleted,
    required this.recoveryRunning,
    required this.historyCompleted,
    required this.historyPending,
    required this.nextReadyCalls,
  });

  /// The sizes reported in docs/benchmarks.md.
  static const full = Sizes(
    rounds: 5,
    enqueueWarmup: 200,
    enqueueCount: 2000,
    drainCount: 5000,
    recoveryCompleted: 9000,
    recoveryRunning: 1000,
    historyCompleted: 100000,
    historyPending: 1000,
    nextReadyCalls: 1000,
  );

  /// A fast run that checks that every benchmark works.
  static const quick = Sizes(
    rounds: 1,
    enqueueWarmup: 20,
    enqueueCount: 200,
    drainCount: 300,
    recoveryCompleted: 450,
    recoveryRunning: 50,
    historyCompleted: 2000,
    historyPending: 50,
    nextReadyCalls: 100,
  );

  final int rounds;
  final int enqueueWarmup;
  final int enqueueCount;
  final int drainCount;
  final int recoveryCompleted;
  final int recoveryRunning;
  final int historyCompleted;
  final int historyPending;
  final int nextReadyCalls;

  Map<String, Object> toJson() => {
    'rounds': rounds,
    'enqueueWarmup': enqueueWarmup,
    'enqueueCount': enqueueCount,
    'drainCount': drainCount,
    'recoveryCompleted': recoveryCompleted,
    'recoveryRunning': recoveryRunning,
    'historyCompleted': historyCompleted,
    'historyPending': historyPending,
    'nextReadyCalls': nextReadyCalls,
  };
}

/// Runs the benchmarks in a temporary directory.
final class BenchmarkRunner {
  BenchmarkRunner(this.sizes, {void Function(String)? log})
    : _log = log ?? ((_) {});

  final Sizes sizes;
  final void Function(String) _log;
  late final Directory _root = Directory.systemTemp.createTempSync('dq_bench_');
  var _files = 0;

  String _nextFile() => 'q${_files++}';

  Future<void> dispose() async => _root.deleteSync(recursive: true);

  /// Time for one `enqueue`, measured call by call.
  Future<Map<String, Object>> enqueue(StorageKind kind) async {
    final micros = <int>[];
    final totals = <int>[];
    for (var round = 0; round < sizes.rounds; round++) {
      final storage = await kind.open(_root, _nextFile());
      final queue = _queue(storage);
      for (var i = 0; i < sizes.enqueueWarmup; i++) {
        await queue.enqueue(const _Noop());
      }
      final total = Stopwatch()..start();
      final each = Stopwatch();
      for (var i = 0; i < sizes.enqueueCount; i++) {
        each
          ..reset()
          ..start();
        await queue.enqueue(const _Noop());
        micros.add(each.elapsedMicroseconds);
      }
      totals.add(total.elapsedMicroseconds);
      await queue.close();
      await kind.close(storage);
    }
    final samples = Samples(micros);
    final perSecond =
        sizes.enqueueCount /
        (Samples(totals).median / Duration.microsecondsPerSecond);
    _log('enqueue ${kind.name}: median ${formatMicros(samples.median)}');
    return {...samples.toJson(), 'perSecond': perSecond};
  }

  /// Time for `drain` to run stored no-op tasks, one at a time
  /// (`maxConcurrentTasks: 1`, the default).
  Future<Map<String, Object>> drain(StorageKind kind) async {
    final times = <int>[];
    for (var round = 0; round < sizes.rounds; round++) {
      final file = _nextFile();
      await kind.fill(_root, file, _tasks(pending: sizes.drainCount));
      final storage = await kind.open(_root, file);
      final queue = _queue(storage);
      final watch = Stopwatch()..start();
      final result = await queue.drain();
      times.add(watch.elapsedMicroseconds);
      if (result.completed != sizes.drainCount) {
        throw StateError('drain completed ${result.completed} tasks');
      }
      await queue.close();
      await kind.close(storage);
    }
    final median = Samples(times).median;
    _log('drain ${kind.name}: ${formatMicros(median)}');
    return {
      'medianMicros': median,
      'perSecond': sizes.drainCount / (median / Duration.microsecondsPerSecond),
    };
  }

  /// After a crash: opening the storage, then `start`, which recovers every
  /// task left running. Recovered tasks are scheduled to retry an hour
  /// later, so no handler runs during the measurement.
  Future<Map<String, Object>> recovery(StorageKind kind) async {
    final opens = <int>[];
    final starts = <int>[];
    for (var round = 0; round < sizes.rounds; round++) {
      final file = _nextFile();
      await kind.fill(
        _root,
        file,
        _tasks(
          completed: sizes.recoveryCompleted,
          running: sizes.recoveryRunning,
        ),
      );
      final open = Stopwatch()..start();
      final storage = await kind.open(_root, file);
      opens.add(open.elapsedMicroseconds);
      final queue = _queue(storage);
      final start = Stopwatch()..start();
      await queue.start();
      starts.add(start.elapsedMicroseconds);
      final scheduled = await storage.getByStatus(TaskStatus.retryScheduled);
      if (scheduled.length != sizes.recoveryRunning) {
        throw StateError('recovered ${scheduled.length} tasks');
      }
      await queue.close();
      await kind.close(storage);
    }
    final open = Samples(opens).median;
    final start = Samples(starts).median;
    _log(
      'recovery ${kind.name}: open ${formatMicros(open)}, '
      'start ${formatMicros(start)}',
    );
    return {
      'openMicros': open,
      'startMicros': start,
      'persistent': kind.persistent,
    };
  }

  /// A queue with a long history of completed tasks: opening it, finding
  /// the next task, and running the pending ones.
  Future<Map<String, Object>> history(StorageKind kind) async {
    final opens = <int>[];
    final nextReady = <double>[];
    final drains = <int>[];
    for (var round = 0; round < sizes.rounds; round++) {
      final file = _nextFile();
      await kind.fill(
        _root,
        file,
        _tasks(
          completed: sizes.historyCompleted,
          pending: sizes.historyPending,
        ),
      );
      final open = Stopwatch()..start();
      final storage = await kind.open(_root, file);
      opens.add(open.elapsedMicroseconds);
      // One call takes well under a microsecond with some storages, so the
      // calls are timed in batches of 10 and each batch gives the average.
      final now = DateTime.now().toUtc();
      final batch = Stopwatch();
      // The first batch warms up and is not recorded.
      for (var i = -1; i < sizes.nextReadyCalls ~/ 10; i++) {
        batch
          ..reset()
          ..start();
        for (var j = 0; j < 10; j++) {
          if (await storage.getNextReady(now) == null) {
            throw StateError('no ready task');
          }
        }
        if (i >= 0) nextReady.add(batch.elapsedMicroseconds / 10);
      }
      final queue = _queue(storage);
      final drain = Stopwatch()..start();
      final result = await queue.drain();
      drains.add(drain.elapsedMicroseconds);
      if (result.completed != sizes.historyPending) {
        throw StateError('drain completed ${result.completed} tasks');
      }
      await queue.close();
      await kind.close(storage);
    }
    final next = Samples(nextReady);
    final drain = Samples(drains).median;
    _log('history ${kind.name}: next ready ${formatMicros(next.median)}');
    return {
      'openMicros': Samples(opens).median,
      'persistent': kind.persistent,
      'nextReadyMedianMicros': next.median,
      'nextReadyP95Micros': next.p95,
      'drainMicros': drain,
      'drainPerSecond':
          sizes.historyPending / (drain / Duration.microsecondsPerSecond),
    };
  }

  static DurableQueue _queue(QueueStorage storage) =>
      DurableQueue(storage: storage)..register<_Noop>(
        type: _Noop.taskType,
        decoder: (_) => const _Noop(),
        handler: (task, context) async {},
      );

  /// Task records as the queue would store them: completed ones first
  /// (oldest), then running, then pending.
  static Iterable<StoredTask> _tasks({
    int completed = 0,
    int running = 0,
    int pending = 0,
  }) sync* {
    final base = DateTime.now().toUtc().subtract(const Duration(days: 1));
    final retry = RetryPolicy.fixed(
      maxAttempts: 3,
      delay: const Duration(hours: 1),
    );
    var n = 0;
    StoredTask make(TaskStatus status, int attempts) {
      n++;
      final at = base.add(Duration(milliseconds: n));
      return StoredTask(
        id: 'task-$n',
        type: _Noop.taskType,
        payload: {'n': n, 'note': 'benchmark task'},
        status: status,
        attempts: attempts,
        retryPolicy: retry,
        createdAt: at,
        updatedAt: at,
        sequence: n,
      );
    }

    for (var i = 0; i < completed; i++) {
      yield make(TaskStatus.completed, 1);
    }
    for (var i = 0; i < running; i++) {
      yield make(TaskStatus.running, 1);
    }
    for (var i = 0; i < pending; i++) {
      yield make(TaskStatus.pending, 0);
    }
  }
}

final class _Noop extends DurableTask {
  const _Noop();

  static const taskType = 'noop';

  @override
  String get type => taskType;

  @override
  Map<String, dynamic> toJson() => const {};
}
