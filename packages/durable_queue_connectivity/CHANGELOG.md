## 0.1.0

* Initial release.
* `ConnectivityGate.create()`: a `durable_queue` gate driven by
  `connectivity_plus`, open while a connection is up. `accept` limits the
  connection types (for example Wi-Fi only).
* Optional reachability `probe`: the gate opens only when the probe succeeds,
  probes again every `probeRetryInterval` while connected but unreachable, and
  probes when the queue reports a connection error (`recheck`), at most once
  per `minRecheckInterval`.
* `HttpProbe`: one HTTP request to your own server; 2xx and 4xx count as
  reachable, redirects (captive portals), 5xx, timeouts, and connection errors
  do not.
* `deferOnConnectionError(gate)` and `isConnectionError`: a
  `register(deferOn:)` rule for `SocketException`, `HttpException`,
  `ClientException`, and `TimeoutException`.
* `NetworkGate`: the same gate for any network source, without
  `connectivity_plus`.
* Requires `durable_queue` 0.5.0 or later.
