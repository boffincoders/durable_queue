## 0.1.2

* `watchTask`, `watchGroup`, `QueueTaskBuilder`, and `QueueGroupBuilder` also
  update when a task is held or released (`DurableQueue.holdChanges`).
* Requires `durable_queue` 0.5.9 or later.

## 0.1.1

* `DurableQueueScope(runQueue: false)`: the scope only provides the queue
  and does not start, pause, or resume it. Use it when something else runs
  the queue, such as `LeasedQueueRunner` from `durable_queue_workmanager`.

## 0.1.0

* Initial release.
* `DurableQueueScope`: provides a `DurableQueue` to a widget subtree
  (`DurableQueueScope.of(context)`), starts it, pauses it while the app is
  hidden or paused, and resumes it in the foreground. A pause made by the app
  is never undone. Optional `closeOnDispose` and `onStartError`.
* `DurableQueueLifecycle.attach(queue)`: the same lifecycle handling without a
  widget.
* `QueueLifecycleController`: the platform-independent rules, driven by
  `AppVisibility`.
* `queue.watchTask(id)` and `queue.watchGroup(group, status:)`: streams of
  stored tasks that update on queue events.
* `QueueTaskBuilder` and `QueueGroupBuilder`: widgets built on those streams.
* Requires `durable_queue` 0.5.0 or later and Flutter 3.13 or later.
