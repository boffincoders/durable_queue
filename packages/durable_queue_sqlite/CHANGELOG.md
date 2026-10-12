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
