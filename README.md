# durable_queue

Persistent, retryable task execution for Dart and Flutter.

Work such as uploads, messages, and sync is described as data, stored through a pluggable storage interface, and run by a worker that owns retries with backoff, concurrency limits, priorities, dependencies, deduplication, and crash recovery.

## Packages

| Package | Description | Version |
|---|---|---|
| [`durable_queue`](packages/durable_queue) | Core engine. Pure Dart, no runtime dependencies. | [![pub package](https://img.shields.io/pub/v/durable_queue.svg)](https://pub.dev/packages/durable_queue) |

Planned in this repository: SQLite storage, Flutter lifecycle integration, Dio integration, Hive storage, and a DevTools extension. Each will be a separate package, so the core never forces a database, an HTTP client, or Flutter on you.

## Get started

```yaml
dependencies:
  durable_queue: ^0.3.1
```

The [`durable_queue` README](packages/durable_queue/README.md) has the quick start, an architecture diagram, examples, and the full feature guide. Adapter authors should read the [storage contract](packages/durable_queue/STORAGE.md).

## Repository layout

```text
packages/
  durable_queue/     core engine (published to pub.dev)
pubspec.yaml         Dart pub workspace root (not published)
CONTRIBUTING.md      how to contribute
```

## Contributing and feedback

* Questions and ideas: [GitHub Discussions](https://github.com/boffincoders/durable_queue/discussions)
* Bugs: [issues](https://github.com/boffincoders/durable_queue/issues)
* Code and adapters: [CONTRIBUTING.md](CONTRIBUTING.md)

Licensed under the [MIT license](LICENSE).
