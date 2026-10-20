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
