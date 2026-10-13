## 0.1.0

* Initial release.
* `DioRetryRules`: retries timeouts, connection failures, and HTTP 408, 425,
  429, 500, 502, 503, 504 by default; honors `Retry-After` (seconds or
  HTTP-date) up to a configurable maximum. Use `rules.shouldRetry` as
  `retryIf` and `rules.retryDelay` as `retryDelay`.
* `parseRetryAfter` for `Retry-After` header values.
* `TaskContext` helpers: `dioCancelToken()` aborts the request when the task is
  cancelled; `withIdempotencyKey()` sends the task's idempotency key as a
  header (default `Idempotency-Key`).
* `HttpRequestTask` and `queue.registerHttpRequests(dio)`: queue HTTP requests
  (method, URL, headers, query, JSON or text body) without writing a task
  class, with an optional `onResponse` callback.
* Requires `durable_queue` 0.4.0 and `dio` 5.11.1 or later.
