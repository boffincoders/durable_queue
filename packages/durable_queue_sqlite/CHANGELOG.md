## 0.2.0

* Web support. `SqliteQueueStorage.openWeb(name)` runs SQLite as WebAssembly
  (`sqlite3.wasm` from `package:sqlite3`) and stores the database in
  IndexedDB. Every write is flushed to IndexedDB before it completes, so
  queued tasks survive a page reload or a closed tab.
* The package now compiles for the web. 0.1.0 imported `dart:ffi` and broke web
  builds of any app that depended on it.
* `SqliteQueueStorage.fromDatabase` accepts any `CommonDatabase` (native or
  web) and an optional `flush` callback.
* `open` and `inMemory` throw `UnsupportedError` on the web; `openWeb` throws
  it on native platforms.
* Allow `durable_queue` 0.4.x (`>=0.3.1 <0.5.0`). 0.1.0 accepted only 0.3.x.
* No schema change: databases written by 0.1.0 open unchanged.

## 0.1.0

* Initial release: `SqliteQueueStorage`, a `QueueStorage` for `durable_queue`
  backed by SQLite through `package:sqlite3` 3.x.
* Tasks survive process death. Writes run in transactions; file databases use
  write-ahead logging with full synchronous writes.
* Every worker query (next ready task, next wake time, deduplication,
  dependents, groups, recovery batches, sequence) uses an index; tests check
  the query plans.
* Open a file (`SqliteQueueStorage.open`), an in-memory database
  (`SqliteQueueStorage.inMemory`), or tables inside a database the app already
  uses (`SqliteQueueStorage.fromDatabase`, with a configurable table prefix).
* Versioned schema: opening tables written by a newer release fails clearly
  instead of corrupting them.
* Passes the full `durable_queue` storage contract suite.
