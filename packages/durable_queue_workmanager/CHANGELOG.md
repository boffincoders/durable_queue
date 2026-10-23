## 0.1.0

* Initial release.
* `QueueBackgroundWork`: registers a periodic Workmanager job (`schedule`,
  `cancel`) and runs the queue in it (`handles`, `run`): takes the lease,
  drains for up to `timeLimit` (20 seconds on iOS, 8 minutes elsewhere by
  default), and closes the queue. On Android it also registers a one-off
  job for the next due task, or soon after a run that hit the time limit.
* `LeasedQueueRunner`: runs the app's queue only while it holds the lease;
  waits for the lease while a background job drains, and releases it on
  `stop`.
* `LeasedQueueLifecycle.attach(runner)`: starts the runner in the foreground
  and stops it when the app is hidden or paused.
* `drainWithLease`: the background part without Workmanager, returning a
  `LeasedDrainResult`.
* `foregroundService`: run the jobs as Android foreground services for longer
  runs.
* `QueueLease` interface and `FileQueueLease`, a lease kept in a file that
  works between isolates of one process, renewed by its holder and taken
  over when abandoned.
* Requires `durable_queue` 0.5.3 or later, `workmanager` 0.10.10 or later,
  and Flutter 3.38 or later. Android and iOS.
