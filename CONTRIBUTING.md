# Contributing

Thanks for helping. `durable_queue` is a small execution engine, and the goal is to keep the core free of HTTP clients, databases, and Flutter. Integrations belong in separate packages.

## Where to start

* **Questions, ideas, and integration problems:** open a thread in [GitHub Discussions](https://github.com/boffincoders/durable_queue/discussions). Good topics: a storage backend you need, a workflow the queue cannot express yet, or an API that confused you.
* **Bugs:** open an [issue](https://github.com/boffincoders/durable_queue/issues) with the package version, Dart or Flutter version, a minimal reproduction, and what you expected.
* **Larger changes:** start a discussion first, so the design can be agreed before you write code.

## Setup

```sh
git clone https://github.com/boffincoders/durable_queue.git
cd durable_queue
dart pub get
```

## Checks

Every pull request must pass:

```sh
dart format --output=none --set-exit-if-changed .
dart analyze
dart test
```

## Pull requests

* Keep each pull request to one change, with tests.
* Add an entry under the next version in `CHANGELOG.md`.
* Document behavior users can observe in the README and in the public API docs.
* Call out breaking changes explicitly: new `TaskStatus` values, `QueueStorage` methods, or changed ordering all affect adapter authors.

## Engine rules

* Time goes through `QueueClock`. Tests use `FakeQueueClock` instead of real delays.
* Worker queries stay bounded. Never load the whole queue to claim work or recover; `test/scalability_test.dart` enforces this.
* Do not promise exactly-once execution or execution after the process has been killed.

## Storage adapters

Adapters are separate packages (for example `durable_queue_<backend>`). To build one:

1. Implement `QueueStorage` following [STORAGE.md](STORAGE.md).
2. Copy `test/storage_contract.dart` and run `queueStorageContractTests` against your adapter.
3. Use `example/json_file_storage.dart` as a working reference.
4. Announce it in Discussions so it can be listed in the README.

## License

By contributing, you agree that your contributions are licensed under the MIT license in [LICENSE](LICENSE).
