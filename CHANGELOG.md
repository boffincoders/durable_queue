## 0.1.0

* Initial release of the durable task queue.
* Serializable tasks, an in-memory storage adapter, and a storage contract.
* Fixed and exponential retries, jitter, retry predicates, and max attempts.
* Concurrency limits, deduplication, idempotency metadata, and cancellation.
* Pause, resume, and stop controls.
* Lifecycle events, task queries, and failure metadata.
* Restart recovery for tasks left in the running state.
* An injectable clock so retry tests do not wait on real time.
