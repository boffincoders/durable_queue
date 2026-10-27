## 0.1.0

* Initial release.
* `HiveQueueStorage`: `QueueStorage` on a `hive_ce` `Box<String>`, one JSON
  entry per task, with an indexed in-memory view for queries.
* `HiveQueueStorage.open` opens the box; `flushEachWrite` (on by default)
  flushes every write to disk.
* Ids longer than Hive's 255-byte key limit are stored under a SHA-256
  hash.
* A failed box write is undone in the view.
* Tested with the shared storage contract on real Hive files, a writer
  process killed with SIGKILL, a write cut off at the end of the file, and
  in a browser (IndexedDB).
