import 'dart:async';
import 'dart:convert';
import 'dart:io';

import 'demo.dart';
import 'demo_config.dart';

/// Lets `tool/kill_test.dart` drive the app: reads the scenario from the
/// demo server once at startup and reports the queue every second.
final class Autopilot {
  Autopilot(this.demo);

  final Demo demo;
  final _client = HttpClient()..connectionTimeout = const Duration(seconds: 3);
  Timer? _timer;

  Future<void> start() async {
    try {
      final scenario = await _get('/scenario');
      if (scenario['action'] == 'enqueue') {
        await demo.addPhotos(
          count: (scenario['count'] as int?) ?? 6,
          sizeKb: (scenario['sizeKb'] as int?) ?? 256,
        );
      }
    } on Object catch (error) {
      stderr.writeln('autopilot: no scenario ($error)');
    }
    _timer = Timer.periodic(const Duration(seconds: 1), (_) => _report());
  }

  Future<void> _report() async {
    try {
      final tasks = await demo.queue.getTasks(group: photosGroup);
      final byStatus = <String, int>{};
      for (final task in tasks) {
        byStatus.update(task.status.name, (n) => n + 1, ifAbsent: () => 1);
      }
      await _post('/report', {
        'total': tasks.length,
        'byStatus': byStatus,
        'handedOff': tasks.where((task) => task.isHandedOff).length,
        'at': DateTime.now().toIso8601String(),
      });
    } on Object {
      // Offline or the server is gone; try again next second.
    }
  }

  Future<Map<String, Object?>> _get(String path) async {
    final request = await _client.getUrl(Uri.parse('$demoServer$path'));
    final response = await request.close();
    final text = await utf8.decoder.bind(response).join();
    return jsonDecode(text) as Map<String, Object?>;
  }

  Future<void> _post(String path, Object body) async {
    final request = await _client.postUrl(Uri.parse('$demoServer$path'));
    request.headers.contentType = ContentType.json;
    request.write(jsonEncode(body));
    final response = await request.close();
    await response.drain<void>();
  }

  void dispose() {
    _timer?.cancel();
    _client.close(force: true);
  }
}
