## 0.1.0

* Initial release.
* `DriftQueueStorage.open(database, tablePrefix:)`: a `QueueStorage` that
  keeps tasks in tables inside an app's drift database. Each write runs in
  its own drift transaction. Calls made inside a drift transaction of the
  app are refused with a `StateError` instead of risking a deadlock.
* Uses the same tables as `durable_queue_sqlite`.
* Requires drift 2.35.1 or later. Earlier versions could lose writes made
  in transactions on the web (IndexedDB), and every queue write is one.
