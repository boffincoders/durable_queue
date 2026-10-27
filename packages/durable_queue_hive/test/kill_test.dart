@TestOn('vm')
library;

import 'dart:async';
import 'dart:convert';
import 'dart:io';
import 'dart:isolate';

import 'package:durable_queue_hive/durable_queue_hive.dart';
import 'package:hive_ce/hive_ce.dart';
import 'package:test/test.dart';

// Kills a process that is writing tasks (SIGKILL, no clean-up) and checks
// that every save it reported as complete is there after reopening.
void main() {
  test('every completed save survives the writer being killed', () async {
    final directory = Directory.systemTemp.createTempSync('dq_hive_kill_');
    addTearDown(() => directory.deleteSync(recursive: true));
    final packages = (await Isolate.packageConfig)!.toFilePath();
    final acknowledged = <String>{};

    for (var round = 0; round < 3; round++) {
      final process = await Process.start(Platform.resolvedExecutable, [
        '--packages=$packages',
        'test/support/writer.dart',
        directory.path,
        'queue',
      ]);
      final enough = Completer<void>();
      var seen = 0;
      process.stdout
          .transform(utf8.decoder)
          .transform(const LineSplitter())
          .listen((line) {
            if (!line.startsWith('saved ')) return;
            acknowledged.add(line.substring(6));
            if (++seen == 150 && !enough.isCompleted) enough.complete();
          });
      final errors = StringBuffer();
      process.stderr.transform(utf8.decoder).listen(errors.write);
      await enough.future.timeout(
        const Duration(minutes: 1),
        onTimeout: () => fail('writer did not start: $errors'),
      );
      process.kill(ProcessSignal.sigkill);
      await process.exitCode;

      final storage = await HiveQueueStorage.open(
        'queue',
        path: directory.path,
      );
      final stored = {for (final t in await storage.getAll()) t.id};
      expect(
        acknowledged.difference(stored),
        isEmpty,
        reason: 'round $round: acknowledged but missing',
      );
      await storage.close();
      await Hive.close();
    }
    expect(acknowledged.length, greaterThanOrEqualTo(450));
  });
}
