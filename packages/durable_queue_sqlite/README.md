# durable_queue_sqlite

SQLite storage for [`durable_queue`](https://pub.dev/packages/durable_queue). Tasks are written to a database file in transactions, so queued work survives the app being closed, killed, or the device restarting.

```dart
final queue = DurableQueue(
  storage: SqliteQueueStorage.open('${dir.path}/queue.db'),
);
```

Everything else (tasks, retries, priorities, dependencies, groups) works exactly as described in the [`durable_queue` README](https://pub.dev/packages/durable_queue).

## Install

```yaml
dependencies:
  durable_queue: ^0.3.1
  durable_queue_sqlite: ^0.1.0
```

SQLite itself comes from [`package:sqlite3`](https://pub.dev/packages/sqlite3) 3.x, which bundles a compiled SQLite with your app through Dart build hooks. You do not need `sqlite3_flutter_libs`. To use the operating system's SQLite or an encrypted build instead, see the [`sqlite3` hook options](https://github.com/simolus3/sqlite3.dart/blob/main/sqlite3/doc/hook.md).

Platforms: Android, iOS, macOS, Windows, and Linux (Flutter or plain Dart). The web is not supported.

## Usage in Flutter

```dart
import 'package:durable_queue/durable_queue.dart';
import 'package:durable_queue_sqlite/durable_queue_sqlite.dart';
import 'package:path_provider/path_provider.dart';

Future<DurableQueue> openQueue() async {
  final dir = await getApplicationSupportDirectory();
  final queue = DurableQueue(
    storage: SqliteQueueStorage.open('${dir.path}/durable_queue.db'),
  );
  queue.register<UploadPhotoTask>(
    type: 'upload_photo',
    decoder: UploadPhotoTask.fromJson,
    handler: (task, context) => api.upload(task.path),
  );
  await queue.start(); // recovers tasks interrupted by the last shutdown
  return queue;
}
```

Create the queue once, at startup. Only one `DurableQueue` should use a database (and table prefix) at a time.

## Opening a database

| Constructor | Use it when |
|---|---|
| `SqliteQueueStorage.open(path)` | The queue gets its own database file. Enables write-ahead logging and full synchronous writes. |
| `SqliteQueueStorage.fromDatabase(db)` | Your app already has a `sqlite3` `Database`. The queue adds its own tables and leaves connection settings and closing to you. |
| `SqliteQueueStorage.inMemory()` | Tests. Nothing is kept after the process ends. |

All three take `tablePrefix` (default `durable_queue_`). Give each queue its own prefix if several queues share one database.

Call `close()` when you are done with a storage. For `open` and `inMemory` this closes the database too.

## What is stored

Each task is one row holding the full `StoredTask` JSON, plus indexed columns for status, priority, timestamps, sequence, deduplication key, and group. A second table records which waiting tasks depend on which tasks. Every write runs in one transaction, so a crash never leaves half a task behind.

Every query the worker runs uses an index (the package's tests check SQLite's query plans), so claiming the next task stays fast as the queue grows.

The schema is versioned in a `<prefix>meta` table. Opening tables created by a newer release of this package throws `StateError` rather than risking your data.

## Things to know

* `package:sqlite3` is synchronous. Each call runs on the calling isolate. Queue operations touch one or a few rows, so this is quick; listing or purging very large numbers of tasks blocks that isolate while it runs.
* Payloads are stored as plain JSON. The database is not encrypted unless you configure an encrypted SQLite build.
* Persistence is not background execution: the queue runs while your app runs. Call `start()` on the next launch.

## Example

```sh
dart run example/main.dart
```

## Contributing

Issues and pull requests: [github.com/boffincoders/durable_queue](https://github.com/boffincoders/durable_queue). See [CONTRIBUTING.md](https://github.com/boffincoders/durable_queue/blob/main/CONTRIBUTING.md).
