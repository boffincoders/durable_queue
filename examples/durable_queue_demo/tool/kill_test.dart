// Kills the demo app while uploads are running and checks that every photo
// is still uploaded and announced exactly once after the next launch.
//
// Needs a built and installed debug app (see tool/ci_android.sh and
// tool/ci_ios.sh) and a running emulator or simulator.
//
//   dart run tool/kill_test.dart --platform android --app-id <application id>
//   dart run tool/kill_test.dart --platform ios --app-id <bundle id>
//
// Steps:
//  1. Start the demo server; hold every upload for 60 seconds.
//  2. Launch the app with the scenario "enqueue N photos".
//  3. Once uploads are running and handed off, send the app to the
//     background and kill its process (Android: like the system killing it
//     for memory; iOS simulator: terminate).
//  4. Android only: relaunch with the network off; nothing may be announced
//     while offline. Then turn the network back on.
//  5. Relaunch and wait until every task completed.
//  6. Check: every photo uploaded at least once and announced exactly once,
//     and the app reports all tasks completed.
import 'dart:async';
import 'dart:convert';
import 'dart:io';

import 'demo_server.dart';

late final String platform;
late final String appId;

Future<void> main(List<String> args) async {
  String option(String name, [String? fallback]) {
    final index = args.indexOf('--$name');
    if (index >= 0 && index + 1 < args.length) return args[index + 1];
    if (fallback != null) return fallback;
    stderr.writeln('Missing --$name');
    exit(64);
  }

  platform = option('platform');
  appId = option('app-id');
  final count = int.parse(option('photos', '6'));
  final port = int.parse(option('port', '8080'));
  if (platform != 'android' && platform != 'ios') {
    stderr.writeln('--platform must be android or ios');
    exit(64);
  }

  final server = await DemoServer.start(port: port);
  log('server on port ${server.port}');
  var failed = false;
  try {
    await run(server, count);
  } on Object catch (error, stackTrace) {
    failed = true;
    log('FAILED: $error\n$stackTrace');
    log('stats: ${jsonEncode(server.stats())}');
    log('report: ${jsonEncode(server.report)}');
  } finally {
    await server.close();
  }
  exit(failed ? 1 : 0);
}

Future<void> run(DemoServer server, int count) async {
  await stopApp();

  // 1–2. Enqueue with slow uploads.
  // Long enough that the kill below lands while uploads are in flight; the
  // kill checks that it did.
  server.uploadDelay = const Duration(seconds: 60);
  server.scenario = {'action': 'enqueue', 'count': count, 'sizeKb': 256};
  await launchApp();
  await waitFor(
    'uploads running and handed off',
    const Duration(minutes: 3),
    () =>
        server.uploadsInFlight > 0 &&
        ((server.report?['handedOff'] as int?) ?? 0) >= 2,
  );
  log('in flight: ${server.uploadsInFlight}, report: ${server.report}');

  // 3. Kill it in the middle.
  server.scenario = {'action': 'observe'};
  await killApp(server);
  log(
    'killed; uploads continue without the app: ${jsonEncode(server.stats())}',
  );
  server.uploadDelay = Duration.zero;
  await Future<void>.delayed(const Duration(seconds: 5));

  // 4. Offline launch (Android only).
  if (platform == 'android') {
    await setNetwork(false);
    final doneBefore = sum(server.done);
    await launchApp();
    await Future<void>.delayed(const Duration(seconds: 15));
    final doneOffline = sum(server.done);
    log('after 15 s offline: ${jsonEncode(server.stats())}');
    if (doneOffline != doneBefore) {
      throw StateError('Announcements went out while offline');
    }
    await setNetwork(true);
    log('network back on');
  } else {
    await launchApp();
  }

  // 5. Everything completes.
  await waitFor('all tasks completed', const Duration(minutes: 6), () {
    final report = server.report;
    if (report == null) return false;
    final byStatus = (report['byStatus'] as Map?) ?? {};
    return report['total'] == count * 2 && byStatus['completed'] == count * 2;
  });

  // 6. Check the server's view.
  final stats = server.stats();
  log('final stats: ${jsonEncode(stats)}');
  if (server.done.length != count) {
    throw StateError('Expected $count photos announced, got ${server.done}');
  }
  final repeated = server.done.entries.where((e) => e.value != 1).toList();
  if (repeated.isNotEmpty) {
    throw StateError('Announced more than once: $repeated');
  }
  for (final name in server.done.keys) {
    if ((server.uploads[name] ?? 0) < 1) {
      throw StateError('$name was announced but never uploaded');
    }
  }
  final reuploaded = server.uploads.entries.where((e) => e.value > 1).length;
  log(
    'PASSED: $count photos uploaded and announced once each '
    '($reuploaded uploaded again after the kill, allowed: at least once)',
  );
}

int sum(Map<String, int> counts) => counts.values.fold(0, (a, b) => a + b);

void log(String message) =>
    stdout.writeln('[kill_test ${DateTime.now().toIso8601String()}] $message');

Future<void> waitFor(
  String what,
  Duration timeout,
  bool Function() condition,
) async {
  log('waiting for $what');
  final deadline = DateTime.now().add(timeout);
  while (!condition()) {
    if (DateTime.now().isAfter(deadline)) {
      throw TimeoutException('Timed out waiting for $what', timeout);
    }
    await Future<void>.delayed(const Duration(seconds: 1));
  }
}

Future<String> sh(String executable, List<String> arguments) async {
  final result = await Process.run(executable, arguments);
  final out = '${result.stdout}'.trim();
  if (result.exitCode != 0) {
    log(
      '$executable ${arguments.join(' ')} -> ${result.exitCode}: '
              '${result.stderr}'
          .trim(),
    );
  }
  return out;
}

Future<void> launchApp() async {
  log('launch');
  if (platform == 'android') {
    await sh('adb', [
      'shell',
      'monkey',
      '-p',
      appId,
      '-c',
      'android.intent.category.LAUNCHER',
      '1',
    ]);
  } else {
    await sh('xcrun', ['simctl', 'launch', 'booted', appId]);
  }
}

Future<void> stopApp() async {
  if (platform == 'android') {
    await sh('adb', ['shell', 'am', 'force-stop', appId]);
  } else {
    await sh('xcrun', ['simctl', 'terminate', 'booted', appId]);
  }
}

/// Fails the test if no upload is in flight right before the kill, so a slow
/// step cannot turn this into a test of an idle app.
void expectInFlight(DemoServer server) {
  final inFlight = server.uploadsInFlight;
  if (inFlight == 0) {
    throw StateError('No upload in flight at the kill; the kill came too late');
  }
  log('killing with $inFlight uploads in flight');
}

Future<void> killApp(DemoServer server) async {
  if (platform == 'android') {
    // Background first, then kill the process the way the system does when
    // it needs memory. WorkManager keeps the transfers scheduled.
    await sh('adb', ['shell', 'input', 'keyevent', 'KEYCODE_HOME']);
    await Future<void>.delayed(const Duration(seconds: 2));
    final pid = await sh('adb', ['shell', 'pidof', appId]);
    expectInFlight(server);
    if (pid.isNotEmpty) {
      await sh('adb', ['shell', 'run-as', appId, 'kill', '-9', pid]);
    }
    await Future<void>.delayed(const Duration(seconds: 1));
    final still = await sh('adb', ['shell', 'pidof', appId]);
    if (still.isNotEmpty) {
      await sh('adb', ['shell', 'am', 'kill', appId]);
    }
    log('process ${pid.isEmpty ? '(none)' : pid} killed');
  } else {
    // Send it to the background, then terminate it.
    await sh('xcrun', ['simctl', 'launch', 'booted', 'com.apple.Preferences']);
    await Future<void>.delayed(const Duration(seconds: 2));
    expectInFlight(server);
    await sh('xcrun', ['simctl', 'terminate', 'booted', appId]);
    log('terminated');
  }
}

Future<void> setNetwork(bool on) async {
  final state = on ? 'enable' : 'disable';
  await sh('adb', ['shell', 'svc', 'wifi', state]);
  await sh('adb', ['shell', 'svc', 'data', state]);
  log('network ${on ? 'on' : 'off'}');
  await Future<void>.delayed(const Duration(seconds: 3));
}
