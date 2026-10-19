## 0.2.0

* `registerHttpRequests` takes `requires`, `deferOn`, and `rateLimit` and
  passes them to `DurableQueue.register`, so queued requests can wait for a
  network gate and be rate limited.
* `DioRetryRules.deferOn(gate)`: a `register(deferOn:)` rule that turns
  connection errors (`connectionError`, `connectionTimeout`) into a wait for
  the gate instead of a failed attempt. `DioRetryRules.isConnectionError`
  exposes the same check.
* `queue.addHttpHostRateLimit(limit, dio:)`: a rate limit per API host for
  queued requests. Relative URLs are resolved against the Dio `baseUrl`.
* `HttpRequestTask.hostOf(task, baseUrl:)`: the host a stored request goes
  to.
* Requires `durable_queue` 0.5.0 or later (`>=0.5.0 <0.6.0`).

## 0.1.1

* Allow `durable_queue` 0.5.x (`>=0.4.0 <0.6.0`). No code change.

## 0.1.0

* Initial release.
* `DioRetryRules`: retries timeouts, connection failures, and HTTP 408, 425,
  429, 500, 502, 503, 504 by default; honors `Retry-After` (seconds or
  HTTP-date) up to a configurable maximum. Use `rules.shouldRetry` as
  `retryIf` and `rules.retryDelay` as `retryDelay`.
* `parseRetryAfter` for `Retry-After` header values. Two-digit years in
  RFC 850 dates follow the RFC 9110 50-year rule with every `http_parser` 4.x.
* `TaskContext` helpers: `dioCancelToken()` aborts the request when the task is
  cancelled; `withIdempotencyKey()` sends the task's idempotency key as a
  header (default `Idempotency-Key`).
* `HttpRequestTask` and `queue.registerHttpRequests(dio)`: queue HTTP requests
  (method, URL, headers, query, JSON or text body) without writing a task
  class, with an optional `onResponse` callback.
* Requires `durable_queue` 0.4.0 and `dio` 5.11.1 or later.
