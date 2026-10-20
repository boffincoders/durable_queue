// A minimal Flutter app: tap to queue an upload, watch its status, and let
// the queue pause while the app is in the background.
import 'package:durable_queue/durable_queue.dart';
import 'package:durable_queue_flutter/durable_queue_flutter.dart';
import 'package:flutter/widgets.dart';

final class UploadTask extends DurableTask {
  UploadTask(this.path);

  final String path;

  @override
  String get type => 'upload';

  @override
  Map<String, dynamic> toJson() => {'path': path};

  static UploadTask fromJson(Map<String, dynamic> json) =>
      UploadTask(json['path'] as String);
}

Future<void> main() async {
  WidgetsFlutterBinding.ensureInitialized();

  // Use durable_queue_sqlite (or another persistent storage) in a real app.
  final queue = DurableQueue(storage: MemoryQueueStorage());
  queue.register<UploadTask>(
    type: 'upload',
    decoder: UploadTask.fromJson,
    handler: (task, context) async {
      await Future<void>.delayed(const Duration(seconds: 1)); // the upload
    },
  );

  runApp(
    DurableQueueScope(
      queue: queue,
      child: const Directionality(
        textDirection: TextDirection.ltr,
        child: Uploads(),
      ),
    ),
  );
}

class Uploads extends StatefulWidget {
  const Uploads({super.key});

  @override
  State<Uploads> createState() => _UploadsState();
}

class _UploadsState extends State<Uploads> {
  var _count = 0;

  Future<void> _add() async {
    await DurableQueueScope.of(context)
        .enqueue(UploadTask('/photos/${_count++}.jpg'), group: 'photos');
  }

  @override
  Widget build(BuildContext context) {
    return GestureDetector(
      onTap: _add,
      child: Center(
        child: QueueGroupBuilder(
          group: 'photos',
          builder: (context, tasks) {
            final done = tasks
                .where((task) => task.status == TaskStatus.completed)
                .length;
            return Text('$done of ${tasks.length} uploaded. Tap to add.');
          },
        ),
      ),
    );
  }
}
