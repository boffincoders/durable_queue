import 'dart:io';
import 'dart:math';

import 'package:background_downloader/background_downloader.dart'
    show BaseDirectory, UploadTask;
import 'package:dio/dio.dart';
import 'package:durable_queue/durable_queue.dart';
import 'package:durable_queue_connectivity/durable_queue_connectivity.dart';
import 'package:durable_queue_dio/durable_queue_dio.dart';
import 'package:durable_queue_sqlite/durable_queue_sqlite.dart';
import 'package:durable_queue_transfer/durable_queue_transfer.dart';
import 'package:path_provider/path_provider.dart';

import 'demo_config.dart';
import 'photo_upload.dart';

/// Everything the demo app runs: the queue, its storage, the network gate,
/// and background transfers.
final class Demo {
  Demo._(this.queue, this.transfers, this.network, this._photos);

  /// Opens the queue on SQLite, registers the task types, and starts
  /// applying transfer results. Call once at startup.
  static Future<Demo> open() async {
    final support = await getApplicationSupportDirectory();
    final photos = Directory('${support.path}/photos');
    await photos.create(recursive: true);

    final queue = DurableQueue(
      storage: SqliteQueueStorage.open('${support.path}/demo_queue.db'),
    );

    // Requests that need the network wait while the device is offline.
    final network = await ConnectivityGate.create();
    queue.addGate('network', network);

    // Step 2 of each photo: a normal HTTP request, queued as data.
    final dio = Dio(
      BaseOptions(
        baseUrl: demoServer,
        connectTimeout: const Duration(seconds: 10),
        receiveTimeout: const Duration(seconds: 30),
      ),
    );
    const rules = DioRetryRules();
    queue.registerHttpRequests(
      dio,
      requires: {'network'},
      deferOn: rules.deferOn('network'),
    );

    // Step 1 of each photo: an upload the operating system runs, so it
    // continues while the app is in the background or closed.
    final transfers = QueueTransfers(queue);
    queue.register<PhotoUpload>(
      type: PhotoUpload.taskType,
      decoder: PhotoUpload.fromJson,
      retryIf: isRetryableTransferError,
      handler: (task, context) async {
        await transfers.start(
          context,
          UploadTask(
            url: '$demoServer/upload',
            urlQueryParameters: {'name': task.fileName},
            filename: task.fileName,
            baseDirectory: BaseDirectory.applicationSupport,
            directory: 'photos',
            post: 'binary',
            headers: {'Idempotency-Key': context.taskId},
          ),
        );
      },
    );
    await transfers.attach();
    return Demo._(queue, transfers, network, photos);
  }

  final DurableQueue queue;
  final QueueTransfers transfers;
  final NetworkGate network;
  final Directory _photos;
  final _random = Random();

  /// Creates [count] files of [sizeKb] KiB standing in for photos, and
  /// queues "upload, then announce" for each.
  Future<void> addPhotos({int count = 3, int sizeKb = 512}) async {
    final stamp = DateTime.now().millisecondsSinceEpoch;
    for (var i = 0; i < count; i++) {
      final name = 'photo_${stamp}_$i.jpg';
      final bytes = List<int>.generate(
        sizeKb * 1024,
        (_) => _random.nextInt(256),
      );
      await File('${_photos.path}/$name').writeAsBytes(bytes, flush: true);
      await queue.enqueueChain(
        [
          PhotoUpload(name),
          HttpRequestTask(method: 'POST', url: '/photos/$name/done'),
        ],
        group: photosGroup,
        retryPolicy: RetryPolicy.exponential(
          maxAttempts: 8,
          initialDelay: const Duration(seconds: 5),
          maxDelay: const Duration(minutes: 5),
          jitter: true,
        ),
      );
    }
  }

  /// Removes finished tasks from the list.
  Future<int> clearFinished() => queue.purge();
}
