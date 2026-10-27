# durable_queue_hive

[Hive](https://pub.dev/packages/hive_ce) storage for [`durable_queue`](https://pub.dev/packages/durable_queue): tasks are kept in a Hive box, so they survive app restarts and crashes. Use it when your app already uses Hive and you do not want a second database. It runs on Android, iOS, macOS, Windows, Linux, and the web (IndexedDB).

It is built on `hive_ce`, the maintained continuation of Hive.

## Install

```
dart pub add durable_queue_hive
```

In a Flutter app, also add `hive_ce_flutter` for `Hive.initFlutter()`.

## Use

```dart
import 'package:durable_queue/durable_queue.dart';
import 'package:durable_queue_hive/durable_queue_hive.dart';
import 'package:hive_ce_flutter/hive_ce_flutter.dart';

await Hive.initFlutter();
final storage = await HiveQueueStorage.open('durable_queue');
final queue = DurableQueue(storage: storage);
```

In plain Dart, call `Hive.init(directory)` instead of `Hive.initFlutter()`. To use a box you opened yourself, pass it to `HiveQueueStorage(box)`; it must be a `Box<String>` used only by the queue.

## How it stores tasks

Each task is one entry in the box: the key is the task id, the value is the JSON of the task record. Ids longer than Hive's 255-byte key limit are stored under a SHA-256 hash of the id.

When the storage opens, it reads every task into an indexed view in memory, so finding the next task to run, deduplication, groups, and dependencies do not scan the box. Every write goes to the view and to the box. If the box write fails, the view is changed back.

## Durability

By default each write is flushed to disk before `enqueue` (or any other write) completes, so a stored task survives a power loss as well as the app being killed. A test kills a writing process with `SIGKILL` three times and checks that every completed save is there afterwards.

`HiveQueueStorage.open('durable_queue', flushEachWrite: false)` skips the flush: writes are faster and still survive the app being killed or crashing, but the last ones can be lost if the device loses power. On the web, IndexedDB decides when data reaches disk.

If the app dies in the middle of a write, Hive drops the incomplete entry when the box is next opened; everything written before it is kept.

## Limits

* **One isolate.** Open the box in one isolate only. Hive must not open a box from two isolates at once, and the view lives in the isolate that opened it. A Workmanager background job runs in another isolate, so with `durable_queue_workmanager` use [`durable_queue_sqlite`](https://pub.dev/packages/durable_queue_sqlite) instead.
* **Memory.** All tasks are held in memory while the storage is open. That suits queues of hundreds or a few thousand tasks; purge finished tasks you no longer need (`DurableQueue.purge`). For very large queues, use `durable_queue_sqlite`.
* **A dedicated box.** Opening a box that holds anything other than task records throws `FormatException`.

## Encryption

Hive's own box encryption (`encryptionCipher` in `open`) encrypts the whole box with AES. Alternatively, wrap the storage with [`durable_queue_encryption`](https://pub.dev/packages/durable_queue_encryption), which encrypts each task's payload and error messages.

## Tests

The shared storage contract runs against real Hive files, with and without flushing. Further tests cover reopening, a write cut off at the end of the file, long and non-ASCII ids, failed writes, the process kill, and a run in a browser.
