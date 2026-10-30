# Contributing

Thanks for helping. `durable_queue` is a small execution engine, and the goal is to keep the core free of HTTP clients, databases, and Flutter. Integrations belong in separate packages.

## Where to start

* **Questions, ideas, and integration problems:** open a thread in [GitHub Discussions](https://github.com/boffincoders/durable_queue/discussions). Good topics: a storage backend you need, a workflow the queue cannot express yet, or an API that confused you.
* **Bugs:** open an [issue](https://github.com/boffincoders/durable_queue/issues) with the package version, Dart or Flutter version, a minimal reproduction, and what you expected.
* **Security problems:** do not open an issue. Report them privately as described in [SECURITY.md](SECURITY.md).
* **Larger changes:** start a discussion first, so the design can be agreed before you write code.

## Setup

```sh
git clone https://github.com/boffincoders/durable_queue.git
cd durable_queue
dart pub get   # resolves every package in the workspace
```

The repository is a Dart pub workspace. Each published package lives in `packages/`:

| Folder | Package |
|---|---|
| `packages/durable_queue` | Core engine, pure Dart |
| `packages/durable_queue_dio` | Dio integration |
| `packages/durable_queue_sqlite` | SQLite storage |
| `packages/durable_queue_hive` | Hive storage on `hive_ce`, pure Dart |
| `packages/durable_queue_drift` | drift storage, in the app's drift database |
| `packages/durable_queue_encryption` | Encryption at rest for any storage, pure Dart |
| `packages/durable_queue_test_support` | Shared test helpers, including the storage contract suite. Not published. |
| `packages/durable_queue_connectivity` | Network gate for Flutter. Needs the Flutter SDK, so it is not part of the workspace: run `flutter pub get` inside its folder. |
| `packages/durable_queue_flutter` | Flutter integration (lifecycle, widgets). Also outside the workspace: run `flutter pub get` inside its folder. |
| `packages/durable_queue_transfer` | Background uploads and downloads on `background_downloader`. Also outside the workspace: run `flutter pub get` inside its folder. Needs Flutter 3.47 or later. |
| `website` | The documentation site on GitHub Pages: `dart run website/build.dart` writes it to `website/dist`. Most pages are the READMEs and docs, so editing those updates the site. Not published. |
| `benchmark` | Benchmarks (`dart run benchmark/bin/benchmark.dart`, add `--quick` for a short run). Not published. |
| `examples/cookbook` | The recipes of `docs/cookbook.md`. Change a recipe here and in the doc together. |
| `examples/durable_queue_demo` | Demo app and the device kill tests. Uses the packages in this repository through path overrides. See its README. |
| `packages/durable_queue_workmanager` | Workmanager background jobs and the queue lease. Also outside the workspace: run `flutter pub get` inside its folder. Tests under `test/widgets/` need the Flutter test binding. |

Run the checks below from inside the package you changed. For the Flutter packages (`durable_queue_connectivity`, `durable_queue_flutter`, `durable_queue_transfer`, `durable_queue_workmanager`), use `flutter analyze` and `flutter test` instead of the `dart` commands.

## Checks

Every pull request must pass:

```sh
dart format --output=none --set-exit-if-changed .
dart analyze
dart test
```

## Pull requests

* Keep each pull request to one change, with tests.
* Add an entry under the next version in that package's `CHANGELOG.md`.
* Document behavior users can observe in the README and in the public API docs.
* Call out breaking changes explicitly: new `TaskStatus` values, `QueueStorage` methods, or changed ordering all affect adapter authors.

## Engine rules

* Time goes through `QueueClock`. Tests use `FakeQueueClock` instead of real delays.
* Worker queries stay bounded. Never load the whole queue to claim work or recover; `packages/durable_queue/test/scalability_test.dart` enforces this.
* Do not promise exactly-once execution or execution after the process has been killed.

## Storage adapters

Adapters are separate packages (for example `durable_queue_<backend>`). To build one:

1. Implement `QueueStorage` following [STORAGE.md](packages/durable_queue/STORAGE.md).
2. Run `queueStorageContractTests` from `packages/durable_queue_test_support` against your adapter: add it as a `path` dev dependency inside this repository, or copy `lib/src/storage_contract.dart` elsewhere.
3. Use `packages/durable_queue_sqlite` (database), `packages/durable_queue_drift` (an app's existing database), `packages/durable_queue_hive` (key-value store), or `packages/durable_queue/example/json_file_storage.dart` (file) as a working reference.
4. Announce it in Discussions so it can be listed in the README.

## License

By contributing, you agree that your contributions are licensed under the MIT license in [LICENSE](LICENSE).
