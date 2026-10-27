import 'dart:io';

import 'benchmarks.dart';
import 'stats.dart';

/// Where and how a run happened.
Map<String, Object?> environment(Sizes sizes, {required bool quick}) => {
  'date': DateTime.now().toUtc().toIso8601String(),
  'dart': Platform.version.split(' ').first,
  'os': Platform.operatingSystem,
  'osVersion': Platform.operatingSystemVersion,
  'processors': Platform.numberOfProcessors,
  'runner': Platform.environment['RUNNER_OS'] == null
      ? null
      : '${Platform.environment['ImageOS'] ?? Platform.environment['RUNNER_OS']}'
            ' (${Platform.environment['RUNNER_ARCH'] ?? '?'})',
  'commit': Platform.environment['GITHUB_SHA'],
  'quick': quick,
  'sizes': sizes.toJson(),
};

/// The results as Markdown tables.
String markdown(
  Map<String, Object?> env,
  Map<String, Map<String, Map<String, Object>>> results,
  Sizes sizes,
) {
  final out = StringBuffer();
  String n(int value) => value.toString().replaceAllMapped(
    RegExp(r'(\d)(?=(\d{3})+$)'),
    (m) => '${m[1]},',
  );
  double v(Map<String, Object> row, String key) =>
      (row[key]! as num).toDouble();
  // Nothing is read back when an in-memory storage "opens".
  String open(Map<String, Object> row) =>
      row['persistent'] == false ? '—' : formatMicros(v(row, 'openMicros'));
  Iterable<MapEntry<String, Map<String, Object>>> rows(String benchmark) =>
      (results[benchmark] ?? const {}).entries;

  out
    ..writeln(
      'Dart ${env['dart']} on ${env['runner'] ?? env['os']}, '
      '${env['processors']} processors. ${env['osVersion']}.',
    )
    ..writeln()
    ..writeln(
      'Commit ${env['commit'] ?? '(local)'}, ${env['date']}. '
      '${sizes.rounds} rounds; times are the median round unless noted.'
      '${env['quick'] == true ? ' QUICK RUN: sizes reduced, not for publishing.' : ''}',
    )
    ..writeln();

  if (rows('enqueue').isNotEmpty) {
    out
      ..writeln('### Enqueue')
      ..writeln()
      ..writeln(
        '${n(sizes.enqueueCount)} calls to `enqueue` per round, '
        'each until the task is stored; percentiles over all calls.',
      )
      ..writeln()
      ..writeln('| Storage | Median | p95 | Per second |')
      ..writeln('| --- | --- | --- | --- |');
    for (final MapEntry(key: name, value: row) in rows('enqueue')) {
      out.writeln(
        '| $name | ${formatMicros(v(row, 'medianMicros'))} | '
        '${formatMicros(v(row, 'p95Micros'))} | '
        '${formatRate(v(row, 'perSecond'))} |',
      );
    }
    out.writeln();
  }
  if (rows('drain').isNotEmpty) {
    out
      ..writeln('### Running tasks')
      ..writeln()
      ..writeln(
        '`drain` runs ${n(sizes.drainCount)} stored tasks whose '
        'handler does nothing, one at a time. Each task is claimed and '
        'completed in storage.',
      )
      ..writeln()
      ..writeln('| Storage | Time | Tasks per second |')
      ..writeln('| --- | --- | --- |');
    for (final MapEntry(key: name, value: row) in rows('drain')) {
      out.writeln(
        '| $name | ${formatMicros(v(row, 'medianMicros'))} | '
        '${formatRate(v(row, 'perSecond'))} |',
      );
    }
    out.writeln();
  }
  if (rows('recovery').isNotEmpty) {
    out
      ..writeln('### Recovery after a crash')
      ..writeln()
      ..writeln(
        '${n(sizes.recoveryCompleted + sizes.recoveryRunning)} '
        'stored tasks, ${n(sizes.recoveryRunning)} of them left running '
        'by a crash. Open the storage, then `start`, which records each '
        'interrupted attempt and schedules a retry.',
      )
      ..writeln()
      ..writeln('| Storage | Open | `start` (recovery) |')
      ..writeln('| --- | --- | --- |');
    for (final MapEntry(key: name, value: row) in rows('recovery')) {
      out.writeln(
        '| $name | ${open(row)} | '
        '${formatMicros(v(row, 'startMicros'))} |',
      );
    }
    out.writeln();
  }
  if (rows('history').isNotEmpty) {
    out
      ..writeln('### A long history')
      ..writeln()
      ..writeln(
        '${n(sizes.historyCompleted)} completed tasks and '
        '${n(sizes.historyPending)} pending ones. Open the storage, find '
        'the next task to run (${n(sizes.nextReadyCalls)} calls, timed in '
        'batches of 10), then '
        'run the pending tasks.',
      )
      ..writeln()
      ..writeln(
        '| Storage | Open | Next task, median | Next task, p95 | '
        'Run ${n(sizes.historyPending)} tasks |',
      )
      ..writeln('| --- | --- | --- | --- | --- |');
    for (final MapEntry(key: name, value: row) in rows('history')) {
      out.writeln(
        '| $name | ${open(row)} | '
        '${formatMicros(v(row, 'nextReadyMedianMicros'))} | '
        '${formatMicros(v(row, 'nextReadyP95Micros'))} | '
        '${formatMicros(v(row, 'drainMicros'))} |',
      );
    }
    out.writeln();
  }
  return out.toString();
}
