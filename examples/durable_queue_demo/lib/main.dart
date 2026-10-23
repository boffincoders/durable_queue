// durable_queue demo: photo uploads that finish even if the app is closed.
//
// Each "photo" is queued as two tasks: an upload that the operating system
// runs (durable_queue_transfer), then a normal request that tells the
// server it is done (durable_queue_dio), which waits for the network
// (durable_queue_connectivity). Everything is stored in SQLite
// (durable_queue_sqlite) and shown with durable_queue_flutter.
//
// Start the server first: dart run tool/demo_server.dart
import 'package:durable_queue/durable_queue.dart';
import 'package:durable_queue_flutter/durable_queue_flutter.dart';
import 'package:flutter/widgets.dart';

import 'src/autopilot.dart';
import 'src/demo.dart';
import 'src/demo_config.dart';
import 'src/photo_upload.dart';

Future<void> main() async {
  WidgetsFlutterBinding.ensureInitialized();
  final demo = await Demo.open();
  runApp(
    DurableQueueScope(
      queue: demo.queue,
      child: DemoApp(demo: demo),
    ),
  );
  if (demoAutopilot) await Autopilot(demo).start();
}

const _ink = Color(0xFF1B1B1F);
const _muted = Color(0xFF5E5E66);
const _accent = Color(0xFF3A5BD9);
const _paper = Color(0xFFF7F7FA);

class DemoApp extends StatelessWidget {
  const DemoApp({super.key, required this.demo});

  final Demo demo;

  @override
  Widget build(BuildContext context) {
    return WidgetsApp(
      title: 'durable_queue demo',
      color: _accent,
      textStyle: const TextStyle(color: _ink, fontSize: 15),
      builder: (context, _) => _Home(demo: demo),
    );
  }
}

class _Home extends StatelessWidget {
  const _Home({required this.demo});

  final Demo demo;

  @override
  Widget build(BuildContext context) {
    final padding = MediaQuery.paddingOf(context);
    return ColoredBox(
      color: _paper,
      child: Padding(
        padding: EdgeInsets.fromLTRB(16, padding.top + 16, 16, padding.bottom),
        child: Column(
          crossAxisAlignment: CrossAxisAlignment.stretch,
          children: [
            const Text(
              'Photo outbox',
              style: TextStyle(fontSize: 24, fontWeight: FontWeight.w700),
            ),
            const SizedBox(height: 4),
            const Text(
              'Add photos, then close the app. Uploads keep going; the '
              'server is told about each one once it is uploaded.',
              style: TextStyle(color: _muted),
            ),
            const SizedBox(height: 4),
            const Text(
              'Server: $demoServer',
              style: TextStyle(color: _muted, fontSize: 12),
            ),
            const SizedBox(height: 16),
            Wrap(
              spacing: 8,
              runSpacing: 8,
              children: [
                _Button('Add 3 photos', () => demo.addPhotos()),
                _Button(
                  'Add a 20 MB photo',
                  () => demo.addPhotos(count: 1, sizeKb: 20 * 1024),
                ),
                _Button('Retry failed', () => demo.queue.retryFailed()),
                _Button('Clear finished', demo.clearFinished),
              ],
            ),
            const SizedBox(height: 16),
            Expanded(
              child: QueueGroupBuilder(
                group: photosGroup,
                builder: (context, tasks) => _TaskList(tasks: tasks),
              ),
            ),
          ],
        ),
      ),
    );
  }
}

class _Button extends StatelessWidget {
  const _Button(this.label, this.onTap);

  final String label;
  final Future<Object?> Function() onTap;

  @override
  Widget build(BuildContext context) {
    return Semantics(
      button: true,
      label: label,
      child: GestureDetector(
        onTap: onTap,
        child: Container(
          padding: const EdgeInsets.symmetric(horizontal: 14, vertical: 10),
          decoration: BoxDecoration(
            color: _accent,
            borderRadius: BorderRadius.circular(8),
          ),
          child: Text(label, style: const TextStyle(color: Color(0xFFFFFFFF))),
        ),
      ),
    );
  }
}

class _TaskList extends StatelessWidget {
  const _TaskList({required this.tasks});

  final List<StoredTask> tasks;

  @override
  Widget build(BuildContext context) {
    if (tasks.isEmpty) {
      return const Center(
        child: Text('No photos queued.', style: TextStyle(color: _muted)),
      );
    }
    final done = tasks.where((t) => t.status == TaskStatus.completed).length;
    return Column(
      crossAxisAlignment: CrossAxisAlignment.stretch,
      children: [
        Text('$done of ${tasks.length} steps done'),
        const SizedBox(height: 8),
        Expanded(
          child: ListView.builder(
            itemCount: tasks.length,
            itemBuilder: (context, index) => _TaskRow(task: tasks[index]),
          ),
        ),
      ],
    );
  }
}

class _TaskRow extends StatelessWidget {
  const _TaskRow({required this.task});

  final StoredTask task;

  @override
  Widget build(BuildContext context) {
    final upload = task.type == PhotoUpload.taskType;
    final what = upload
        ? 'Upload ${task.payload['fileName']}'
        : 'Tell server: ${task.payload['url']}';
    final failure = task.lastFailure?.error;
    return Padding(
      padding: const EdgeInsets.symmetric(vertical: 6),
      child: Row(
        crossAxisAlignment: CrossAxisAlignment.start,
        children: [
          SizedBox(width: 96, child: Text(_label(task))),
          Expanded(
            child: Column(
              crossAxisAlignment: CrossAxisAlignment.start,
              children: [
                Text(what, maxLines: 1, overflow: TextOverflow.ellipsis),
                if (failure != null && task.status != TaskStatus.completed)
                  Text(
                    'attempt ${task.attempts}: $failure',
                    maxLines: 2,
                    overflow: TextOverflow.ellipsis,
                    style: const TextStyle(color: _muted, fontSize: 12),
                  ),
              ],
            ),
          ),
        ],
      ),
    );
  }

  static String _label(StoredTask task) {
    if (task.isHandedOff) return 'uploading';
    return switch (task.status) {
      TaskStatus.pending => 'queued',
      TaskStatus.waiting => 'waiting',
      TaskStatus.running => 'running',
      TaskStatus.retryScheduled => 'will retry',
      TaskStatus.completed => 'done',
      TaskStatus.failed => 'failed',
      TaskStatus.cancelled => 'cancelled',
    };
  }
}
