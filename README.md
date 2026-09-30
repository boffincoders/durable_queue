# durable_queue

A lightweight, backend-agnostic, persistent task queue for Dart and Flutter.

`durable_queue` lets applications enqueue work that must eventually execute — even when the app temporarily loses connectivity, the operation fails, or the application process is restarted.

```dart
await queue.enqueue(
  UploadPhotoTask(path: imagePath),
);
```

The queue takes responsibility for persistence, execution, retries, backoff, deduplication, concurrency, and failure tracking.

---

# Package Information

| Property | Value |
|---|---|
| Package name | `durable_queue` |
| Initial version | `0.1.0` |
| Language | Dart |
| Flutter dependency | No |
| Minimum Dart SDK | Decide before implementation based on APIs used |
| License | MIT |
| Repository | GitHub |
| Target | Dart and Flutter applications |
| Primary use case | Reliable persistent background-style task execution |

## Package tagline

> A persistent, retryable task queue for Dart and Flutter.

## Short pub.dev description

A backend-agnostic persistent task queue for Dart and Flutter with retries, exponential backoff, concurrency control, deduplication, cancellation, and observable queue state.

---

# 1. Why durable_queue?

Applications regularly need to perform work that should not disappear just because the first attempt failed.

Examples include:

- uploading an image;
- sending a chat message;
- submitting analytics;
- updating a user profile;
- synchronizing local changes;
- sending an API mutation;
- processing a file;
- retrying temporary server failures;
- performing work after connectivity returns.

A typical implementation starts simply:

```dart
try {
  await api.uploadImage(file);
} catch (_) {
  // What now?
}
```

But production requirements quickly become more complicated.

What if:

- the internet disappears?
- the server returns `503`?
- the application closes?
- the task needs five retry attempts?
- 100 tasks are queued?
- two identical tasks are added?
- a task permanently fails?
- only three tasks should execute concurrently?
- the developer wants to observe queue state?
- the user wants to cancel queued work?

Every application should not have to build this infrastructure from scratch.

`durable_queue` provides that infrastructure.

---

# 2. What durable_queue is

`durable_queue` is a **durable task execution engine**.

The basic lifecycle is:

```text
Application
    │
    │ enqueue()
    ▼
┌───────────────┐
│ Durable Queue │
└───────┬───────┘
        │
        ▼
    Persist Task
        │
        ▼
      Pending
        │
        ▼
      Running
      /     \
     /       \
Success      Failure
  │             │
  ▼             ▼
Completed    Retry?
                │
          ┌─────┴─────┐
          │           │
         Yes          No
          │           │
          ▼           ▼
       Waiting      Failed
          │
          ▼
        Retry
```

The important word is **durable**.

Tasks should be representable as persisted data rather than only an in-memory callback.

---

# 3. What durable_queue is NOT

The package will deliberately remain independent from application frameworks and networking libraries.

The core package will NOT depend directly on:

- Dio
- `package:http`
- GraphQL
- Firebase
- Riverpod
- Bloc
- Provider
- GetX
- Drift
- Hive
- Isar
- connectivity_plus
- WorkManager

This is intentional.

`durable_queue` manages **tasks**.

It does not manage HTTP requests specifically.

For example:

```dart
class UploadPhotoTask extends DurableTask {
  // ...
}
```

The task itself can internally use anything the application wants.

```text
DurableTask
     │
     ├── REST API
     ├── GraphQL
     ├── Firebase
     ├── File processing
     ├── Analytics
     └── Custom application logic
```

---

# 4. Package architecture

The long-term project can become an ecosystem.

```text
durable_queue
│
├── durable_queue_flutter
│
├── durable_queue_drift
│
├── durable_queue_hive
│
├── durable_queue_dio
│
├── durable_queue_workmanager
│
└── durable_queue_inspector
```

However, these packages will NOT all be created for the first release.

Version `0.1.0` focuses on making the core engine reliable.

---

# 5. Version 0.1.0 goal

The objective of `0.1.0` is:

> Provide a small, reliable and testable persistent task queue with retries and no dependency on a particular networking, database, or state-management framework.

Version `0.1.0` should prove the architecture.

We should resist adding every possible feature.

---

# 6. Version 0.1.0 features

The first version will deliver:

### Persistent tasks

Queued tasks can be serialized.

For example:

```dart
class UploadPhotoTask extends DurableTask {
  UploadPhotoTask({
    required this.path,
  });

  final String path;

  @override
  String get type => 'upload_photo';

  @override
  Map<String, dynamic> toJson() {
    return {
      'path': path,
    };
  }
}
```

The queue stores the task definition rather than attempting to persist a Dart closure.

---

### Task registration

Applications register handlers for task types.

Conceptual API:

```dart
queue.register<UploadPhotoTask>(
  type: 'upload_photo',
  decoder: UploadPhotoTask.fromJson,
  handler: (task) async {
    await uploader.upload(task.path);
  },
);
```

This allows persisted tasks to be reconstructed after an application restart.

---

### Enqueue tasks

```dart
final taskId = await queue.enqueue(
  UploadPhotoTask(
    path: '/images/avatar.jpg',
  ),
);
```

Every task receives a unique identifier.

---

### Persistent storage abstraction

The core package will define an abstraction such as:

```dart
abstract interface class QueueStorage {
  Future<void> save(StoredTask task);

  Future<StoredTask?> get(String id);

  Future<List<StoredTask>> getPending();

  Future<void> update(StoredTask task);

  Future<void> delete(String id);
}
```

The engine will depend on this interface rather than a particular database.

An in-memory implementation will be included:

```dart
final queue = DurableQueue(
  storage: MemoryQueueStorage(),
);
```

This is useful for:

- testing;
- examples;
- applications that do not require restart persistence.

A production persistence adapter can be introduced separately.

---

# 7. Task states

Version `0.1.0` will define an explicit task state machine.

```dart
enum TaskStatus {
  pending,
  running,
  retryScheduled,
  completed,
  failed,
  cancelled,
}
```

Typical lifecycle:

```text
pending
   │
   ▼
running
   │
   ├─────────────── success ──────────────► completed
   │
   └─────────────── failure
                         │
                  retry available?
                    /          \
                  yes           no
                   │             │
                   ▼             ▼
           retryScheduled      failed
                   │
                   ▼
                pending
```

Explicit states make debugging and observation much easier.

---

# 8. Retry system

Retries are a core feature of `0.1.0`.

Example:

```dart
await queue.enqueue(
  UploadPhotoTask(path: path),
  retryPolicy: RetryPolicy.exponential(
    maxAttempts: 5,
    initialDelay: Duration(seconds: 1),
    maxDelay: Duration(minutes: 1),
  ),
);
```

The package will support at least:

```dart
RetryPolicy.none()
```

```dart
RetryPolicy.fixed(
  maxAttempts: 3,
  delay: Duration(seconds: 2),
)
```

and:

```dart
RetryPolicy.exponential(
  maxAttempts: 5,
  initialDelay: Duration(seconds: 1),
  maxDelay: Duration(minutes: 1),
)
```

---

# 9. Exponential backoff

Repeated failures should not immediately hammer the server.

Instead of:

```text
FAIL
1 second
FAIL
1 second
FAIL
1 second
FAIL
```

we can use:

```text
Attempt 1
    │
  failure
    │
    ▼
   1 sec

Attempt 2
    │
  failure
    │
    ▼
   2 sec

Attempt 3
    │
  failure
    │
    ▼
   4 sec

Attempt 4
    │
  failure
    │
    ▼
   8 sec
```

The retry delay will respect a configurable maximum.

---

# 10. Retry jitter

`0.1.0` should support jitter.

Without jitter, thousands of clients recovering simultaneously can retry at approximately the same time.

Instead of every client doing:

```text
retry after exactly 8 seconds
```

jitter introduces controlled randomness around the retry timing.

Conceptual configuration:

```dart
RetryPolicy.exponential(
  maxAttempts: 5,
  initialDelay: Duration(seconds: 1),
  jitter: true,
)
```

The implementation should also make randomness injectable/testable so tests remain deterministic.

---

# 11. Retry predicates

Not every error should be retried.

For example:

```text
Timeout              → retry
Temporary network    → retry
503                   → application may choose retry
401                   → probably don't retry blindly
Validation failure    → don't retry
Malformed payload     → don't retry
```

The package should therefore allow the application to make the decision.

Conceptual API:

```dart
queue.register<UploadPhotoTask>(
  type: 'upload_photo',
  decoder: UploadPhotoTask.fromJson,
  handler: uploadHandler,
  retryIf: (error, stackTrace) {
    return error is TemporaryUploadException;
  },
);
```

`durable_queue` itself should not attempt to understand Dio exceptions or HTTP status codes.

That belongs to integrations.

---

# 12. Concurrency control

The queue should prevent unlimited simultaneous task execution.

Example:

```dart
final queue = DurableQueue(
  storage: storage,
  maxConcurrentTasks: 3,
);
```

If ten tasks exist:

```text
Task 1 ─── Running
Task 2 ─── Running
Task 3 ─── Running

Task 4 ─── Waiting
Task 5 ─── Waiting
Task 6 ─── Waiting
...
```

As soon as one running task completes, another pending task can start.

---

# 13. Deduplication

Applications frequently enqueue the same logical work multiple times.

Example:

```dart
await queue.enqueue(
  SyncUserTask(userId: '123'),
  deduplicationKey: 'sync-user:123',
);
```

Then another call:

```dart
await queue.enqueue(
  SyncUserTask(userId: '123'),
  deduplicationKey: 'sync-user:123',
);
```

The queue can detect that equivalent pending work already exists.

The exact duplicate strategy must be explicitly documented.

For `0.1.0`, keep the behavior simple and predictable rather than introducing complicated merging.

---

# 14. Idempotency

Deduplication and idempotency are related but different concepts.

Deduplication prevents unnecessary duplicate queued work.

Idempotency helps identify a logical operation when interacting with an external system.

Tasks should therefore be able to carry an idempotency key:

```dart
await queue.enqueue(
  CreateOrderTask(order),
  idempotencyKey: order.id,
);
```

The queue stores and exposes the key.

The application or future network adapter decides how that key is sent to a backend.

The core package will not assume an HTTP implementation.

---

# 15. Cancellation

Queued work should be cancellable.

```dart
final id = await queue.enqueue(task);

await queue.cancel(id);
```

Pending work can move immediately to:

```text
pending → cancelled
```

Cancellation behavior for currently running asynchronous work needs to be clearly documented.

For `0.1.0`, cancellation should not pretend that Dart can magically terminate arbitrary asynchronous code.

The initial contract should distinguish between:

```text
queued cancellation
```

and:

```text
cooperative cancellation of running work
```

if cooperative cancellation is implemented.

---

# 16. Queue controls

The queue should expose basic lifecycle controls.

Conceptual API:

```dart
await queue.start();
```

```dart
await queue.pause();
```

```dart
await queue.resume();
```

```dart
await queue.stop();
```

The exact semantics must be documented carefully.

For example, `pause()` should prevent new tasks from starting without unexpectedly terminating tasks already running.

---

# 17. Queue observation

Applications need to react to queue changes without depending on Riverpod, Bloc or another state-management system.

The core package can expose Dart streams.

Example:

```dart
queue.events.listen((event) {
  print(event);
});
```

Possible events:

```dart
sealed class QueueEvent {}

class TaskEnqueued extends QueueEvent {}

class TaskStarted extends QueueEvent {}

class TaskRetryScheduled extends QueueEvent {}

class TaskCompleted extends QueueEvent {}

class TaskFailed extends QueueEvent {}

class TaskCancelled extends QueueEvent {}
```

Applications can then connect those streams to whatever state-management system they use.

---

# 18. Querying tasks

Developers should be able to inspect the queue.

Examples:

```dart
final task = await queue.getTask(taskId);
```

```dart
final pending = await queue.getTasks(
  status: TaskStatus.pending,
);
```

```dart
final failed = await queue.getTasks(
  status: TaskStatus.failed,
);
```

This API will later power debugging tools such as `durable_queue_inspector`.

---

# 19. Failure information

Failed tasks should retain useful diagnostic information.

Conceptually:

```dart
class TaskFailure {
  final String error;
  final String? stackTrace;
  final DateTime failedAt;
  final int attempt;
}
```

Care must be taken not to encourage blindly persisting sensitive application data.

Documentation should explain that developers are responsible for deciding what task payloads and error details are safe to persist.

---

# 20. Task metadata

Internally, a persisted task may resemble:

```dart
class StoredTask {
  final String id;

  final String type;

  final Map<String, dynamic> payload;

  final TaskStatus status;

  final int attempts;

  final DateTime createdAt;

  final DateTime? updatedAt;

  final DateTime? nextAttemptAt;

  final String? deduplicationKey;

  final String? idempotencyKey;
}
```

This model is internal infrastructure and should not unnecessarily expose implementation details as permanent public API.

---

# 21. Example usage

The intended developer experience should eventually be approximately:

```dart
final queue = DurableQueue(
  storage: MemoryQueueStorage(),
  maxConcurrentTasks: 3,
);

queue.register<UploadPhotoTask>(
  type: 'upload_photo',
  decoder: UploadPhotoTask.fromJson,
  handler: (task) async {
    await photoRepository.upload(task.path);
  },
);

await queue.start();

await queue.enqueue(
  UploadPhotoTask(
    path: '/storage/avatar.jpg',
  ),
  retryPolicy: RetryPolicy.exponential(
    maxAttempts: 5,
    initialDelay: Duration(seconds: 1),
  ),
);
```

The API should optimize for this simplicity.

---

# 22. Serialization

Tasks must be serializable because closures cannot reliably survive process termination.

Bad architecture:

```dart
queue.enqueue(() async {
  await api.upload(file);
});
```

This can be useful for an in-memory queue but is not sufficient for a durable queue.

Preferred architecture:

```dart
class UploadPhotoTask extends DurableTask {
  final String path;

  @override
  Map<String, dynamic> toJson() => {
    'path': path,
  };
}
```

After restart:

```text
Persisted JSON
      │
      ▼
Task registry
      │
      ▼
UploadPhotoTask
      │
      ▼
Registered handler
      │
      ▼
Execute
```

This is one of the fundamental design decisions of the package.

---

# 23. Crash recovery

Durability introduces an important problem.

Suppose the application dies here:

```text
Task
  │
  ▼
RUNNING
  │
  X  ← application killed
```

When the application starts again, the persisted database may still say:

```text
status = running
```

The queue must have a recovery policy.

For `0.1.0`, startup recovery should detect tasks left in an interrupted running state and safely make them eligible for execution again according to a documented policy.

This is also why idempotent task handlers are strongly recommended.

---

# 24. At-least-once execution semantics

`durable_queue` should explicitly document its delivery semantics.

The practical guarantee should be:

> Tasks are executed with at-least-once semantics unless otherwise documented.

A crash can happen after an external side effect succeeds but before the queue persists the task as completed.

For example:

```text
Create order request
        │
        ▼
Server creates order
        │
        ▼
Application crashes
        │
        X
Queue never writes "completed"
        │
        ▼
Task may execute again
```

No local task queue can universally provide exactly-once execution for arbitrary external systems.

Applications should use idempotent operations or idempotency keys where duplicate side effects would be dangerous.

This limitation must be prominent in the documentation.

---

# 25. Clock abstraction

Retry scheduling relies heavily on time.

Using `DateTime.now()` everywhere would make tests slow and unreliable.

The package should internally use an injectable clock abstraction.

Conceptually:

```dart
abstract interface class QueueClock {
  DateTime now();
}
```

Production uses the real clock.

Tests can use:

```dart
FakeQueueClock(...)
```

This allows retry behavior to be tested instantly and deterministically.

---

# 26. Testability

Testing should be one of the package's main selling points.

Developers should be able to write:

```dart
final queue = DurableQueue(
  storage: MemoryQueueStorage(),
  clock: fakeClock,
);
```

Then simulate:

```text
enqueue
↓
execute
↓
failure
↓
advance fake time
↓
retry
↓
success
```

without waiting real seconds.

---

# 27. Error isolation

One broken task must not crash the entire worker.

```text
Task A → success
Task B → throws exception
Task C → should still execute
Task D → should still execute
```

Every task execution must have an error boundary around it.

Unexpected handler errors should become task failures/retries rather than worker failures.

---

# 28. Public API philosophy

The package should follow several rules.

### Small API

Avoid dozens of classes just because the implementation is complicated.

Simple things should remain simple.

### Framework independent

Core APIs should use Dart concepts such as:

```text
Future
Stream
Duration
Map<String, dynamic>
```

rather than framework-specific abstractions.

### Explicit behavior

Avoid hidden magic.

A developer should understand:

```text
when a task executes
why it retries
when it permanently fails
what happens after restart
```

### Testable

Time, storage and other external dependencies should be replaceable.

### Extensible

Future integrations should not require rewriting the engine.

---

# 29. Proposed source structure

A possible initial structure:

```text
durable_queue/
│
├── lib/
│   ├── durable_queue.dart
│   │
│   └── src/
│       ├── queue/
│       │   ├── durable_queue.dart
│       │   ├── queue_config.dart
│       │   └── queue_state.dart
│       │
│       ├── task/
│       │   ├── durable_task.dart
│       │   ├── stored_task.dart
│       │   ├── task_status.dart
│       │   └── task_failure.dart
│       │
│       ├── registry/
│       │   ├── task_registry.dart
│       │   └── task_handler.dart
│       │
│       ├── retry/
│       │   ├── retry_policy.dart
│       │   ├── fixed_retry.dart
│       │   └── exponential_retry.dart
│       │
│       ├── storage/
│       │   ├── queue_storage.dart
│       │   └── memory_queue_storage.dart
│       │
│       ├── events/
│       │   └── queue_event.dart
│       │
│       └── internal/
│           ├── worker.dart
│           ├── scheduler.dart
│           └── clock.dart
│
├── test/
│   ├── queue_test.dart
│   ├── retry_test.dart
│   ├── recovery_test.dart
│   ├── concurrency_test.dart
│   ├── deduplication_test.dart
│   ├── cancellation_test.dart
│   └── storage_contract_test.dart
│
├── example/
│   └── main.dart
│
├── README.md
├── CHANGELOG.md
├── LICENSE
├── CONTRIBUTING.md
├── analysis_options.yaml
└── pubspec.yaml
```

Internal structure can change during implementation.

Public API stability matters more than keeping this exact directory structure.

---

# 30. Storage contract tests

A future storage adapter should not require manually rediscovering the expected behavior.

The core package should provide or document storage contract tests.

For example, every implementation should behave correctly for:

```text
save
get
update
delete
pending query
status query
deduplication lookup
restart recovery
concurrent access assumptions
```

This will make future adapters much safer.

---

# 31. Version 0.1.0 deliverables

The first release should include all of the following.

## Core

- [ ] `DurableQueue`
- [ ] `DurableTask`
- [ ] unique task IDs
- [ ] task registry
- [ ] task serialization/deserialization
- [ ] explicit task states
- [ ] worker/scheduler
- [ ] start/pause/resume/stop semantics

## Persistence

- [ ] `QueueStorage` interface
- [ ] `MemoryQueueStorage`
- [ ] persisted task model
- [ ] startup recovery behavior
- [ ] storage contract documentation/tests

## Reliability

- [ ] retry policies
- [ ] fixed retry
- [ ] exponential backoff
- [ ] jitter
- [ ] maximum attempts
- [ ] retry predicate
- [ ] task timeout if included in the final v0.1 API
- [ ] worker error isolation

## Queue management

- [ ] concurrency limit
- [ ] deduplication key
- [ ] idempotency metadata
- [ ] queued-task cancellation
- [ ] query tasks by state

## Observability

- [ ] queue event stream
- [ ] task lifecycle events
- [ ] failure metadata

## Testing

- [ ] injectable clock
- [ ] deterministic retry tests
- [ ] concurrency tests
- [ ] recovery tests
- [ ] cancellation tests
- [ ] deduplication tests
- [ ] serialization tests
- [ ] malformed/unknown task tests

## Documentation

- [ ] README
- [ ] API documentation
- [ ] runnable example
- [ ] CHANGELOG
- [ ] LICENSE
- [ ] CONTRIBUTING guide
- [ ] limitations section
- [ ] execution-semantics documentation

---

# 32. What will NOT be in 0.1.0

Scope discipline is important.

Version `0.1.0` will NOT attempt to provide:

- Dio integration;
- HTTP interception;
- connectivity detection;
- automatic network reachability checks;
- Flutter UI;
- Riverpod integration;
- Bloc integration;
- Drift adapter;
- Hive adapter;
- Isar adapter;
- Android WorkManager;
- iOS BGTaskScheduler;
- background isolates;
- task dependency graphs;
- DAG scheduling;
- task priorities;
- optimistic UI;
- mutation compaction;
- synchronization conflict resolution;
- remote synchronization engine;
- debug inspector;
- distributed queues;
- exactly-once guarantees.

These can be considered after the core is stable.

---

# 33. Important Flutter limitation

`durable_queue` should not claim:

> Tasks execute even when your application is terminated.

Persistence and background execution are different problems.

Version `0.1.0` can persist tasks across application restarts, but actually waking a terminated mobile application requires platform-specific background execution mechanisms.

Therefore:

```text
Persistence
    ≠
OS background execution
```

For `0.1.0`:

```text
App running
     ↓
Queue executes tasks

App terminated
     ↓
Tasks remain persisted

App starts again
     ↓
Queue recovers
     ↓
Tasks continue
```

A future package such as:

```text
durable_queue_workmanager
```

can handle supported background-execution scenarios.

---

# 34. Roadmap

## v0.1.0 — Core Engine

Focus:

> Make durable task execution reliable.

Deliver:

```text
Task registry
Persistence abstraction
Memory storage
Retries
Backoff
Jitter
Concurrency
Deduplication
Cancellation
Events
Crash recovery
Tests
Documentation
```

---

## v0.2.0 — Production persistence

Potential focus:

```text
durable_queue_drift
```

Features:

- SQLite persistence;
- migrations;
- indexed task queries;
- transactional state updates;
- production restart recovery.

Keeping this separate avoids forcing Drift on core users.

---

## v0.3.0 — Task orchestration

Potential features:

```dart
await queue.enqueue(
  UpdateProfileTask(),
  dependsOn: uploadTaskId,
);
```

Possible additions:

- task dependencies;
- dependency failure behavior;
- priorities;
- groups;
- chains.

Example:

```text
Upload image
     │
     ▼
Create post
     │
     ▼
Send notification
```

---

## v0.4.0 — Dio integration

Potential package:

```text
durable_queue_dio
```

This integration could understand:

```text
timeouts
connection failures
429
Retry-After
5xx
Dio cancellation
```

without putting Dio into the core package.

---

## v0.5.0 — Flutter integration

Potential package:

```text
durable_queue_flutter
```

Potential capabilities:

- app lifecycle integration;
- resume queue when application returns;
- Flutter-friendly bindings;
- optional connectivity hooks.

---

## v0.6.0 — Inspector

Potential package:

```text
durable_queue_inspector
```

A developer-only Flutter interface.

Example:

```text
┌─────────────────────────────────┐
│ Durable Queue Inspector         │
├─────────────────────────────────┤
│ Running                       2 │
│ Pending                       7 │
│ Retry Scheduled               3 │
│ Failed                        1 │
├─────────────────────────────────┤
│ upload_photo                    │
│ Running • attempt 2/5           │
│                                 │
│ sync_profile                    │
│ Retry in 13s • attempt 3/5      │
└─────────────────────────────────┘
```

Possible controls:

```text
Retry
Cancel
Delete
Inspect payload
Pause queue
Resume queue
```

---

# 35. Future ecosystem

Long term:

```text
                   durable_queue
                         │
        ┌────────────────┼────────────────┐
        │                │                │
        ▼                ▼                ▼
      Drift             Dio            Flutter
        │                │                │
        └──────────┬─────┴─────┬──────────┘
                   │           │
                   ▼           ▼
              WorkManager   Inspector
```

Each integration remains optional.

---

# 36. Example real-world use cases

### Chat application

```dart
await queue.enqueue(
  SendMessageTask(
    conversationId: conversationId,
    messageId: message.id,
    text: message.text,
  ),
  idempotencyKey: message.id,
);
```

If sending fails temporarily:

```text
Message created locally
        │
        ▼
Queued
        │
        ▼
Send attempt
        │
        X
     Failure
        │
        ▼
     Backoff
        │
        ▼
      Retry
        │
        ▼
     Delivered
```

---

### Image upload

```dart
await queue.enqueue(
  UploadImageTask(
    imageId: image.id,
    path: image.path,
  ),
);
```

The application does not need to manually build retry loops around every upload.

---

### Analytics

```dart
await queue.enqueue(
  SendAnalyticsBatchTask(events),
);
```

Temporary backend failure does not immediately discard the work.

---

### Profile synchronization

```dart
await queue.enqueue(
  SyncProfileTask(userId),
  deduplicationKey: 'profile:$userId',
);
```

Repeated requests to synchronize the same user do not necessarily need to create unlimited duplicate pending jobs.

---

# 37. Safety and data considerations

Tasks may contain sensitive information.

Applications should avoid storing unnecessary secrets in queue payloads.

For example, avoid:

```dart
{
  "password": "...",
  "accessToken": "...",
}
```

when the handler can obtain current credentials from a secure authentication system at execution time.

The package documentation should clearly explain:

> `durable_queue` persists whatever task payload the application gives it. Applications are responsible for choosing an appropriately secure storage implementation and deciding which data is safe to persist.

Encryption should not be falsely implied by the core package.

---

# 38. Performance principles

The package should avoid:

- busy polling;
- unnecessary timers;
- loading an unlimited queue into memory;
- unnecessary serialization;
- uncontrolled parallelism.

The scheduler should sleep until there is useful work or the next scheduled retry whenever practical.

Example:

```text
No tasks
   │
   ▼
Idle

Task arrives
   │
   ▼
Wake scheduler
   │
   ▼
Execute
```

For delayed retries:

```text
nextAttemptAt = 12:30:15
          │
          ▼
Scheduler waits
          │
          ▼
Task becomes eligible
```

---

# 39. Quality target

Before publishing `0.1.0`, the following should be true:

```text
dart analyze
```

passes without errors.

```text
dart test
```

passes completely.

The package should also have:

- documented public APIs;
- formatting applied;
- meaningful examples;
- no unnecessary dependencies;
- strong static typing;
- deterministic tests;
- clear exception behavior;
- clear execution guarantees.

---

# 40. Dependency philosophy

For the core package:

> Fewer dependencies are better.

Do not add a dependency simply to save a few lines of implementation.

Every dependency creates:

```text
version constraints
maintenance burden
potential conflicts
security considerations
API compatibility concerns
```

The core should ideally remain very lightweight.

---

# 41. Naming

Recommended package name:

# `durable_queue`

Why:

**Durable** communicates that work survives beyond an individual execution attempt.

**Queue** immediately communicates the fundamental abstraction.

The name also leaves room for use cases beyond networking.

Alternative names, if unavailable on pub.dev:

```text
durable_task_queue
persistent_task_queue
reliable_queue
task_queue_dart
resilient_queue
```

Before publication, the final name must be checked on pub.dev and against similarly named repositories/packages.

---

# 42. Versioning

Start with:

```yaml
version: 0.1.0
```

Do not publish as `1.0.0` immediately.

The `0.x` period gives us room to improve the API based on real-world usage.

Suggested progression:

```text
0.1.0  Core architecture
0.1.x  Bug fixes/documentation
0.2.0  First production persistence adapter
0.3.0  Orchestration capabilities
0.4.0  Network integration
0.5.0  Flutter integration
0.6.0  Inspector/debug tooling
...
1.0.0  Stable public API
```

The roadmap is directional rather than a promise. Versions should be driven by API readiness and user needs.

---

# 43. pubspec.yaml concept

The initial package should remain Dart-first.

Conceptually:

```yaml
name: durable_queue
description: A backend-agnostic persistent task queue for Dart and Flutter with retries, backoff, concurrency control, and recovery.
version: 0.1.0

environment:
  sdk: <choose supported SDK range before release>

dependencies:
  # Keep minimal.

dev_dependencies:
  test:
  lints:
```

Exact SDK and dependency versions should be chosen when implementation begins rather than guessed in this design document.

---

# 44. README quick-start target

The final public README should allow a developer to understand the package in roughly one minute.

It should begin approximately like this:

```text
durable_queue

Persistent, retryable task execution for Dart and Flutter.

✓ Survives application restarts with persistent storage
✓ Exponential retry with jitter
✓ Concurrency control
✓ Deduplication
✓ Cancellation
✓ Observable task lifecycle
✓ Backend agnostic
✓ State-management agnostic
```

Then immediately show working code.

The deeper architectural material in this document can live in `/doc`, contribution documentation, or design documents rather than overwhelming the eventual pub.dev landing page.

---

# 45. Development phases

## Phase 1 — Models

Implement:

```text
DurableTask
StoredTask
TaskStatus
TaskFailure
TaskId
```

Tests first.

---

## Phase 2 — Storage

Implement:

```text
QueueStorage
MemoryQueueStorage
```

Create reusable storage contract tests.

---

## Phase 3 — Registry

Implement:

```text
TaskRegistry
Task decoder
Task handler
Unknown-task behavior
```

Verify persisted JSON can reconstruct tasks.

---

## Phase 4 — Worker

Implement:

```text
pending → running → completed
```

Initially without retries.

Make the simplest lifecycle rock solid.

---

## Phase 5 — Retry scheduler

Add:

```text
fixed delay
exponential delay
maximum attempts
jitter
retry predicates
nextAttemptAt
```

Use fake time in tests.

---

## Phase 6 — Concurrency

Add worker concurrency:

```dart
maxConcurrentTasks: 3
```

Test race conditions heavily.

---

## Phase 7 — Deduplication

Implement deterministic behavior for:

```dart
deduplicationKey
```

Document exactly when a key is considered active.

---

## Phase 8 — Cancellation

Implement:

```dart
queue.cancel(taskId)
```

Clearly define queued vs running cancellation semantics.

---

## Phase 9 — Recovery

Simulate:

```text
Task persisted as running
Application crashes
Application starts
Queue recovers
Task becomes executable
```

This phase is critical before calling the queue durable.

---

## Phase 10 — Observability

Implement:

```text
QueueEvent
TaskStarted
TaskCompleted
TaskFailed
TaskRetryScheduled
TaskCancelled
```

---

## Phase 11 — Documentation

Create:

```text
README.md
CHANGELOG.md
LICENSE
CONTRIBUTING.md
example/
```

Every public API should have Dart documentation.

---

## Phase 12 — Release

Before publishing:

```text
dart format .
dart analyze
dart test
dart pub publish --dry-run
```

Review the generated pub.dev presentation before the actual release.

---

# 46. Definition of done for 0.1.0

Version `0.1.0` is ready when this scenario works reliably:

```text
1. Developer creates DurableQueue

2. Developer registers task handler

3. Developer enqueues serializable task

4. Queue persists task

5. Worker executes task

6. Temporary failure occurs

7. Queue schedules exponential retry

8. Application can restart

9. Queue reconstructs persisted work

10. Task retries

11. Task succeeds

12. Queue records completion

13. Application receives lifecycle events
```

And all of those transitions have automated tests.

---

# 47. Core design promise

`durable_queue` should make one promise and make it well:

> Give me serializable work, and I will reliably manage its local execution lifecycle.

It should NOT promise:

> Give me any closure and I guarantee exactly-once execution anywhere, even when the operating system terminates the application.

The second promise is impossible to provide generically.

Being precise about this distinction will make the package more trustworthy.

---

# 48. Project vision

The initial release is not trying to become another massive offline-first framework.

We are building a small primitive:

```text
             Your Application
                    │
                    ▼
             durable_queue
                    │
        ┌───────────┼───────────┐
        ▼           ▼           ▼
    Persistence   Retry      Scheduling
        │           │           │
        └───────────┼───────────┘
                    ▼
             Reliable Tasks
```

Applications remain responsible for their business logic.

`durable_queue` handles the repetitive reliability infrastructure.

If the core succeeds, integrations can gradually build around it:

```text
              durable_queue
                    │
        ┌───────────┼────────────┐
        ▼           ▼            ▼
      Storage     Network      Flutter
        │           │            │
        ▼           ▼            ▼
       Drift        Dio      WorkManager
                    │
                    ▼
                 Inspector
```

The long-term objective is not to create the largest queue package.

It is to create a queue primitive that developers can understand, test, extend, and trust.

---

# 49. Final v0.1.0 scope

To keep the first release realistic, this is the final target:

```text
durable_queue v0.1.0
────────────────────────────────

CORE
✓ Serializable tasks
✓ Task registry
✓ Task handlers
✓ Task lifecycle

STORAGE
✓ Storage abstraction
✓ Memory implementation
✓ Restart/recovery semantics

RELIABILITY
✓ Fixed retry
✓ Exponential backoff
✓ Jitter
✓ Retry predicates
✓ Maximum attempts

QUEUE
✓ Concurrency limits
✓ Deduplication
✓ Idempotency metadata
✓ Cancellation
✓ Pause/resume

OBSERVABILITY
✓ Event stream
✓ Task queries
✓ Failure information

ENGINEERING
✓ Fake clock
✓ Unit tests
✓ Storage contract tests
✓ Documentation
✓ Example project

NOT YET
✗ Dio
✗ Drift
✗ Hive
✗ WorkManager
✗ Flutter UI
✗ Connectivity
✗ Dependency graphs
✗ Priority scheduling
✗ Inspector
```

That is enough functionality for `0.1.0` to be useful while keeping the architecture manageable.

---

# 50. Next milestone

The immediate milestone is not writing integrations.

It is implementing this smallest reliable path:

```text
DurableTask
     ↓
QueueStorage
     ↓
TaskRegistry
     ↓
DurableQueue.enqueue()
     ↓
Worker
     ↓
Handler
     ↓
Completed
```

Once that lifecycle is correct and thoroughly tested, retry scheduling can be layered on top.

That should be the foundation of `durable_queue`.
