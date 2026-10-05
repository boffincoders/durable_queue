# Contributing

`durable_queue` is a small execution engine. Changes should keep the core free of HTTP clients, databases, and Flutter.

## Setup

```sh
dart pub get
```

## Checks

```sh
dart format --output=none --set-exit-if-changed .
dart analyze
dart test
```

## Design notes

* Document behavior that users can observe in the README and in the public API docs.
* Storage adapters must follow `STORAGE.md` and the scenarios in `test/storage_contract.dart`.
* Time in the engine goes through `QueueClock`. Tests should use `FakeQueueClock` instead of real delays.
* Do not promise exactly-once execution or execution after the process has been killed.
