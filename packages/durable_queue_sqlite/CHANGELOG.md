## 0.2.2

* Several connections to the same database file, for example the app and a
  background isolate, can now write at the same time. Writes take the write
  lock up front (`BEGIN IMMEDIATE`) and wait for each other instead of
  failing with `database is locked`. Inside a transaction the app already
  opened, writes still use a savepoint.
* The busy timeout is set before the other settings, so opening a new file
  from two isolates at once waits instead of failing.

## 0.2.1

* Allow `durable_queue` 0.5.x (`>=0.3.1 <0.6.0`). No code or schema change.

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
