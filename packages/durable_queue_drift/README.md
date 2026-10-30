# durable_queue_drift

[drift](https://pub.dev/packages/drift) storage for [`durable_queue`](https://pub.dev/packages/durable_queue): queued tasks are kept in your app's own drift database, so they survive app restarts and crashes without a second database file. Use it when your app already uses drift.

## Install

```
dart pub add durable_queue_drift
```

## Use

```dart
import 'package:durable_queue/durable_queue.dart';
import 'package:durable_queue_drift/durable_queue_drift.dart';

final db = AppDatabase(); // your drift database
final storage = await DriftQueueStorage.open(db);
final queue = DurableQueue(storage: storage);
```

`open` creates the queue's tables in `db` if they are missing. They are created with plain SQL, are not part of your drift schema, and need no migration in your `MigrationStrategy`. Pass `tablePrefix` to rename them, for example to keep two queues in one database. `storage.close()` stops using the database; closing the database stays with your app.

## How it stores tasks

The tables are the same as [`durable_queue_sqlite`](https://pub.dev/packages/durable_queue_sqlite)'s: one row per task with the task's JSON record, next to indexed columns for the fields the queue searches by, and a table of dependency links. Every query the worker runs uses an index, so claiming work and settling dependencies stay fast as the backlog grows. Because the tables are the same, a database written by one package can be opened by the other.

Each write runs in its own drift transaction.

Do not call the queue from inside one of your drift transactions. The queue may be busy with a write that waits for your transaction to finish, while your transaction waits for the queue, and neither would ever finish. So such calls throw a `StateError` at once. Enqueue after the transaction commits:

```dart
await db.transaction(() async {
  await db.into(db.orders).insert(order);
});
await queue.enqueue(UploadOrder(order.id));
```

A crash between the two leaves an order without its task. If that matters, enqueue a task for each unsent order at startup, with the order id as `deduplicationKey`, so an order whose task already exists does not get a second one.

## Durability and platforms

A stored task is as durable as the rest of your drift database: with drift's defaults on native platforms, a committed transaction survives the app being killed. It runs wherever your drift database runs, including the web. It needs drift 2.35.1 or later: earlier versions could lose writes made in transactions on the web (IndexedDB), and every queue write runs in one.

## Limits

* Only one `DurableQueue` should use a database and table prefix at a time.
* drift's stream queries (`watch()`) on your own tables are not affected by queue writes, and queue tables do not appear in drift's generated code. Read tasks through the queue (`getTasks`, `watchGroup` from `durable_queue_flutter`).
* If you check your schema with `drift_dev`'s schema validation, the queue tables show up as extra tables.

## Encryption

Wrap the storage with [`durable_queue_encryption`](https://pub.dev/packages/durable_queue_encryption) to encrypt each task's payload and error messages, or encrypt the whole database with SQLCipher as drift documents.

## Tests

The shared storage contract runs against file and in-memory drift databases. Further tests cover reopening the app, opening the same file with `durable_queue_sqlite`, refusing calls made inside app transactions, table prefixes, newer schema versions, and the query plans of the worker queries.
