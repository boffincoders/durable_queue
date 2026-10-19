# durable_queue

Persistent, retryable task execution for Dart and Flutter.

Work such as uploads, messages, and sync is described as data, stored through a pluggable storage interface, and run by a worker that owns retries with backoff, concurrency and rate limits, priorities, dependencies, deduplication, waiting for the network, and crash recovery.

## Packages

| Package | Description | Version |
|---|---|---|
| [`durable_queue`](packages/durable_queue) | Core engine. Pure Dart, no runtime dependencies. | [![pub package](https://img.shields.io/pub/v/durable_queue.svg)](https://pub.dev/packages/durable_queue) |
| [`durable_queue_sqlite`](packages/durable_queue_sqlite) | SQLite storage: tasks survive restarts and crashes. | [![pub package](https://img.shields.io/pub/v/durable_queue_sqlite.svg)](https://pub.dev/packages/durable_queue_sqlite) |
| [`durable_queue_dio`](packages/durable_queue_dio) | Dio integration: HTTP retry rules, `Retry-After`, idempotency keys, cancellation, queued requests. | [![pub package](https://img.shields.io/pub/v/durable_queue_dio.svg)](https://pub.dev/packages/durable_queue_dio) |
| [`durable_queue_connectivity`](packages/durable_queue_connectivity) | Network gate for Flutter: tasks wait while offline and start when connectivity returns, with an optional reachability check. | [![pub package](https://img.shields.io/pub/v/durable_queue_connectivity.svg)](https://pub.dev/packages/durable_queue_connectivity) |

Each integration is a separate package, so the core never forces a database, an HTTP client, or Flutter on you.

## Get started

```yaml
dependencies:
  durable_queue: ^0.5.0
```

The [`durable_queue` README](packages/durable_queue/README.md) has the quick start, an architecture diagram, examples, and the full feature guide. Adapter authors should read the [storage contract](packages/durable_queue/STORAGE.md).

## Repository layout

```text
packages/
  durable_queue/                core engine (published to pub.dev)
  durable_queue_connectivity/   network gate, Flutter (published to pub.dev; outside the workspace)
  durable_queue_dio/            Dio integration (published to pub.dev)
  durable_queue_sqlite/         SQLite storage (published to pub.dev)
  durable_queue_test_support/   shared test helpers (not published)
pubspec.yaml                    Dart pub workspace root (not published)
CONTRIBUTING.md                 how to contribute
```

## Contributing and feedback

* Questions and ideas: [GitHub Discussions](https://github.com/boffincoders/durable_queue/discussions)
* Bugs: [issues](https://github.com/boffincoders/durable_queue/issues)
* Code and adapters: [CONTRIBUTING.md](CONTRIBUTING.md)

Licensed under the [MIT license](LICENSE).
