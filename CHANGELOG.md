## 0.1.0

* Initial release of the durable task queue.
* Serializable tasks with immutable payload snapshots and isolated execution attempts.
* Indexed in-memory storage, bounded worker queries, and a documented storage contract.
* Constant-delay and exponential retries, jitter, retry predicates, and attempt limits.
* Concurrency limits, deduplication, idempotency metadata, and cancellation.
* Pause, resume, and stop controls.
* Lifecycle events, task queries, and failure metadata.
* Batched startup recovery for interrupted tasks.
* Automatic retries of worker storage operations without rerunning handlers,
  with a storage error stream and configurable retry delay.
* An injectable clock so retry tests do not wait on real time.
