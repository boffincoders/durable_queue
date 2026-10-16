# durable_queue_sqlite

SQLite storage for [`durable_queue`](https://pub.dev/packages/durable_queue) on Android, iOS, macOS, Windows, Linux, and the web. Tasks are written in transactions, so queued work survives the app being closed, killed, or the device restarting, and on the web a page reload or a closed tab.

```dart
final queue = DurableQueue(
  storage: SqliteQueueStorage.open('${dir.path}/queue.db'),
);
```

Everything else (tasks, retries, priorities, dependencies, groups) works exactly as described in the [`durable_queue` README](https://pub.dev/packages/durable_queue).

## Install

```yaml
dependencies:
  durable_queue: ^0.5.0
  durable_queue_sqlite: ^0.2.1
```

SQLite itself comes from [`package:sqlite3`](https://pub.dev/packages/sqlite3) 3.x, which bundles a compiled SQLite with your app through Dart build hooks. You do not need `sqlite3_flutter_libs`. To use the operating system's SQLite or an encrypted build instead, see the [`sqlite3` hook options](https://github.com/simolus3/sqlite3.dart/blob/main/sqlite3/doc/hook.md).

Platforms: Android, iOS, macOS, Windows, and Linux (Flutter or plain Dart), and the web (see [Web](#web)).

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

For an app that also runs on the web:

```dart
import 'package:flutter/foundation.dart' show kIsWeb;

Future<SqliteQueueStorage> openStorage() async {
  if (kIsWeb) return SqliteQueueStorage.openWeb('durable_queue');
  final dir = await getApplicationSupportDirectory();
  return SqliteQueueStorage.open('${dir.path}/durable_queue.db');
}
```

## Web

On the web, SQLite runs as WebAssembly and the database is kept in the browser's IndexedDB.

1. Download `sqlite3.wasm` from the [`package:sqlite3` releases](https://github.com/simolus3/sqlite3.dart/releases). Pick the release that matches the `sqlite3` version in your `pubspec.lock` (for `sqlite3` 3.7.0, the `sqlite3-3.7.0` release).
2. Put it in your app's `web/` folder, next to `index.html`.
3. Open the storage with `SqliteQueueStorage.openWeb('durable_queue')`. Pass `sqliteWasm: Uri.parse('/assets/sqlite3.wasm')` if you serve the file somewhere else.

Each write is flushed to IndexedDB before its future completes, so a task that `enqueue` has returned survives a reload or a closed tab. Use a different name per queue, and open a queue in one tab at a time. `openWeb('name', inMemory: true)` keeps nothing and is handy for tests.

IndexedDB writes cost a few milliseconds each and get slower as a browser database sees heavy use, so keep web queues small: call `purge` regularly to remove finished tasks, and close storages you no longer need.

When you upgrade `package:sqlite3`, replace `sqlite3.wasm` with the matching release too.

## Opening a database

| Constructor | Use it when |
|---|---|
| `SqliteQueueStorage.open(path)` | The queue gets its own database file. Enables write-ahead logging and full synchronous writes. Native only. |
| `await SqliteQueueStorage.openWeb(name)` | Web. SQLite as WebAssembly, stored in IndexedDB. |
| `SqliteQueueStorage.fromDatabase(db)` | Your app already has a `sqlite3` database (native or web). The queue adds its own tables and leaves connection settings and closing to you. Pass `flush:` if your web file system buffers writes. |
| `SqliteQueueStorage.inMemory()` | Tests on native platforms. Nothing is kept after the process ends. |

All of them take `tablePrefix` (default `durable_queue_`). Give each queue its own prefix if several queues share one database.

Call `close()` when you are done with a storage. For `open` and `inMemory` this closes the database too.

## What is stored

Each task is one row holding the full `StoredTask` JSON, plus indexed columns for status, priority, timestamps, sequence, deduplication key, and group. A second table records which waiting tasks depend on which tasks. Every write runs in one transaction, so a crash never leaves half a task behind.

Every query the worker runs uses an index (the package's tests check SQLite's query plans), so claiming the next task stays fast as the queue grows.

The schema is versioned in a `<prefix>meta` table. Opening tables created by a newer release of this package throws `StateError` rather than risking your data.

## Things to know

* `package:sqlite3` is synchronous. Each call runs on the calling isolate (on the web, the page's main thread). Queue operations touch one or a few rows, so this is quick; listing or purging very large numbers of tasks blocks while it runs.
* Payloads are stored as plain JSON. The database is not encrypted unless you configure an encrypted SQLite build.
* Persistence is not background execution: the queue runs while your app runs. Call `start()` on the next launch.

## Example

```sh
dart run example/main.dart
```

## Contributing

Issues and pull requests: [github.com/boffincoders/durable_queue](https://github.com/boffincoders/durable_queue). See [CONTRIBUTING.md](https://github.com/boffincoders/durable_queue/blob/main/CONTRIBUTING.md).
