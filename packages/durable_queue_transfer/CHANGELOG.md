## Unreleased

* README: links to demo videos on a real iPhone and Android phone.

## 0.1.0

* Initial release.
* `QueueTransfers`: a handler starts an upload or download with
  `transfers.start(context, task)` and returns; background_downloader runs
  it while the app is in the background or closed, and the queue task waits
  for the result (handed off, needs `durable_queue` 0.5.4).
* Results become queue outcomes: success completes the task (after an
  optional `onComplete` with the response), failure fails it so the task
  type's retry rules apply, and a cancelled transfer cancels it. Cancelling
  the queue task cancels the transfer.
* `attach()` applies results that arrived while the app was closed, reschedules
  transfers killed with the app, and fails hand-offs whose transfer is lost
  (`TransferLostException`), so they retry.
* `isRetryableTransferError`: a `retryIf` rule for transfers.
* `TransferException`, `TransferLostException`, `TransferEnqueueException`.
* Requires `durable_queue` 0.5.4 or later, `background_downloader` 9.6.4 or
  later, and Flutter 3.47 or later.
