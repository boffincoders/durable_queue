## 1.1.0

* Keeps queue state next to the tasks (`QueueStateStorage`), so rate
  limits still hold after a restart. The values live in the existing
  metadata table; the schema is unchanged.
* Requires `durable_queue` 1.1.0 or later.

## 1.0.0

* First stable release, for `durable_queue` 1.0. No API changes from
  0.1.0; breaking changes now wait for 2.0.

## 0.1.0

* Initial release.
* `DriftQueueStorage.open(database, tablePrefix:)`: a `QueueStorage` that
  keeps tasks in tables inside an app's drift database. Each write runs in
  its own drift transaction. Calls made inside a drift transaction of the
  app are refused with a `StateError` instead of risking a deadlock.
* Uses the same tables as `durable_queue_sqlite`.
* Requires drift 2.35.1 or later. Earlier versions could lose writes made
  in transactions on the web (IndexedDB), and every queue write is one.
