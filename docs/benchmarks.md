# Benchmarks

What the durable_queue packages cost: storing a task, running it, recovering after a crash, and working with a long history. The numbers below come from one run of the [Benchmarks workflow](https://github.com/boffincoders/durable_queue/actions/runs/40456305352) on GitHub's hosted runners, at commit [`0c61136`](https://github.com/boffincoders/durable_queue/commit/0c6113679be9504b3a3f8245a03f36d1f7faf9fd) (27 October 2026).

## Method

* **Code:** [`benchmark/`](../benchmark) in this repository. Run it yourself with `dart run benchmark/bin/benchmark.dart` from the repository root (`--quick` for a short run).
* **Machines:** GitHub's `ubuntu-latest` runner (Ubuntu 24.04, x64, 4 processors) and `macos-latest` runner (macOS 26.6, Apple silicon ARM64, 3 processors). Both are virtual machines; their disks and timings differ from a phone's.
* **Dart:** 3.13.7, run with `dart run` (JIT compiled). Flutter release builds are compiled ahead of time, which changes the numbers.
* **Storages:** `MemoryQueueStorage`; `durable_queue_sqlite` on a file; the same wrapped with `durable_queue_encryption` (AES-256-GCM in pure Dart); `durable_queue_hive`, with `flushEachWrite` on (the default) and off.
* **Repetition:** every benchmark runs 5 rounds on new files. Times are the median round; latency percentiles are over all calls of all rounds.
* **Set-up:** large data sets are written quickly before the timer starts (one SQLite transaction, or Hive without flushing), then the storage is opened again with its normal settings for the measurement.
* **Handlers do nothing,** so the numbers are the queue's and the storage's own cost. In an app, a handler that makes a network request usually takes far longer than this.

## Results

### Enqueue

2,000 calls to `enqueue` per round, each until the task is stored.

| Storage | Ubuntu: median | Ubuntu: p95 | macOS: median | macOS: p95 |
| --- | --- | --- | --- | --- |
| Memory | 27 µs | 56 µs | 9 µs | 50 µs |
| SQLite | 224 µs | 444 µs | 157 µs | 450 µs |
| SQLite + encryption | 249 µs | 399 µs | 183 µs | 439 µs |
| Hive | 307 µs | 412 µs | 215 µs | 550 µs |
| Hive (no flush) | 73 µs | 94 µs | 63 µs | 232 µs |

### Running tasks

`drain` runs 5,000 stored tasks, one at a time (`maxConcurrentTasks: 1`). Each task is claimed and then completed in storage, so it costs two writes.

| Storage | Ubuntu: time | Ubuntu: per second | macOS: time | macOS: per second |
| --- | --- | --- | --- | --- |
| Memory | 75.5 ms | 66,266 | 121 ms | 41,175 |
| SQLite | 3.14 s | 1,591 | 2.90 s | 1,723 |
| SQLite + encryption | 3.47 s | 1,440 | 3.29 s | 1,522 |
| Hive | 2.95 s | 1,695 | 2.40 s | 2,080 |
| Hive (no flush) | 831 ms | 6,014 | 1.18 s | 4,239 |

### Recovery after a crash

10,000 stored tasks, 1,000 of them left running by a crash. Open the storage, then `start`, which records each interrupted attempt and schedules a retry.

| Storage | Ubuntu: open | Ubuntu: `start` | macOS: open | macOS: `start` |
| --- | --- | --- | --- | --- |
| Memory | — | 9.86 ms | — | 6.01 ms |
| SQLite | 1.15 ms | 266 ms | 2.15 ms | 581 ms |
| SQLite + encryption | 1.26 ms | 325 ms | 1.98 ms | 463 ms |
| Hive | 148 ms | 289 ms | 156 ms | 254 ms |
| Hive (no flush) | 153 ms | 71.0 ms | 163 ms | 73.7 ms |

### A long history

100,000 completed tasks and 1,000 pending ones. Open the storage, find the next task to run (1,000 calls, timed in batches of 10), then run the 1,000 pending tasks.

| Storage | Ubuntu: open | Ubuntu: next task | Ubuntu: run 1,000 | macOS: open | macOS: next task | macOS: run 1,000 |
| --- | --- | --- | --- | --- | --- | --- |
| Memory | — | 0.10 µs | 25.2 ms | — | 0.10 µs | 27.6 ms |
| SQLite | 1.13 ms | 21 µs | 611 ms | 1.95 ms | 15 µs | 492 ms |
| SQLite + encryption | 1.27 ms | 31 µs | 724 ms | 1.36 ms | 22 µs | 416 ms |
| Hive | 1.55 s | 0.10 µs | 572 ms | 1.41 s | 0.10 µs | 323 ms |
| Hive (no flush) | 1.55 s | 0.10 µs | 162 ms | 1.45 s | 0.10 µs | 136 ms |

## What the numbers say

* **Storing a task durably costs about 0.15 to 0.3 ms** with SQLite or Hive, most of it waiting for the disk. Hive without flushing is about four times faster, at the price described in its README: the last writes can be lost on power loss.
* **Encryption adds about 10 to 25 %** to SQLite here, with AES in pure Dart. In Flutter apps, `cryptography_flutter` uses the platform's native AES.
* **The queue runs about 1,500 to 2,000 tasks per second** on durable storage when handlers do nothing. With real work, the handler sets the pace.
* **Recovery takes one write per interrupted task:** about 0.25 to 0.6 s for 1,000 of them.
* **A long history does not slow SQLite down:** with 100,000 completed tasks, opening takes about 1 to 2 ms and finding the next task about 15 to 31 µs, because its queries use indexes.
* **Hive reads every task when it opens:** about 1.5 s with 101,000 tasks, then lookups are in memory. Purge finished tasks you no longer need (`DurableQueue.purge`), or use SQLite for very large queues.

Numbers vary between machines and runs. To compare two versions, run both on the same machine.
