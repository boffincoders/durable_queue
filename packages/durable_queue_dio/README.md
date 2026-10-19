# durable_queue_dio

[Dio](https://pub.dev/packages/dio) integration for [`durable_queue`](https://pub.dev/packages/durable_queue): retry the HTTP failures worth retrying, wait as long as the server asks, send idempotency keys, cancel requests in flight, and queue plain HTTP requests that survive app restarts.

## Install

```yaml
dependencies:
  dio: ^5.11.1
  durable_queue: ^0.5.0
  durable_queue_dio: ^0.2.0
  durable_queue_sqlite: ^0.2.1 # optional: keep requests across restarts
```

## Queue an HTTP request

```dart
final queue = DurableQueue(
  storage: SqliteQueueStorage.open('${dir.path}/queue.db'),
);
queue.registerHttpRequests(Dio(BaseOptions(baseUrl: 'https://api.example.com')));
await queue.start();

await queue.enqueue(
  HttpRequestTask(method: 'POST', url: '/messages', body: {'text': 'hi'}),
  idempotencyKey: messageId, // sent as the Idempotency-Key header
  retryPolicy: RetryPolicy.exponential(
    maxAttempts: 5,
    initialDelay: Duration(seconds: 2),
  ),
);
```

`HttpRequestTask` stores the method, URL, headers, query parameters, and a JSON or text body. Each attempt:

* sends the task's idempotency key in the `Idempotency-Key` header, the same value on every attempt;
* is aborted if the task is cancelled with `queue.cancel(id)`;
* is retried on timeouts, connection failures, and HTTP 408, 425, 429, 500, 502, 503, and 504, waiting for `Retry-After` when the server sends one;
* fails without retrying on other errors, such as a 400 or 404.

Pass `onResponse` to `registerHttpRequests` to read successful responses. It is not stored, so register it on every launch.

Everything in an `HttpRequestTask` is stored as plain JSON. **Do not put credentials in its headers.** Add them when the request is sent, with a Dio interceptor:

```dart
final dio = Dio()
  ..interceptors.add(InterceptorsWrapper(
    onRequest: (options, handler) async {
      options.headers['Authorization'] = 'Bearer ${await auth.token()}';
      handler.next(options);
    },
  ));
```

## Use the rules in your own tasks

For tasks you write yourself, use the same retry rules and helpers:

```dart
const rules = DioRetryRules();

queue.register<UploadPhotoTask>(
  type: 'upload_photo',
  decoder: UploadPhotoTask.fromJson,
  handler: (task, context) async {
    await dio.post(
      '/photos',
      data: FormData.fromMap({'file': await MultipartFile.fromFile(task.path)}),
      options: context.withIdempotencyKey(null),
      cancelToken: context.dioCancelToken(),
    );
  },
  retryIf: rules.shouldRetry,
  retryDelay: rules.retryDelay,
);
```

Store the file path in the task, not the bytes; the file is read again on each attempt.

## Wait for the network

Queued requests can wait while the device is offline instead of using attempts, and go out as soon as it is back. Add a gate named `network`, list it in `requires`, and let `rules.deferOn` turn connection errors into a wait:

```dart
const rules = DioRetryRules();

final network = await ConnectivityGate.create(); // durable_queue_connectivity
queue.addGate('network', network);

queue.registerHttpRequests(
  dio,
  rules: rules,
  requires: {'network'},
  deferOn: rules.deferOn('network'),
);
```

* While the gate is closed, requests are deferred (`TaskDeferred`, reason `gateClosed`) and keep their attempt count.
* A connection error (`DioExceptionType.connectionError` or `connectionTimeout`) defers the request instead of failing it, and asks the gate to check again. Send and receive timeouts still count as attempts, because the connection was made.
* [`durable_queue_connectivity`](https://pub.dev/packages/durable_queue_connectivity) provides a ready-made `network` gate for Flutter. `ManualQueueGate` from `durable_queue` works too, if you track connectivity yourself.

The same options work for your own task types: pass `requires: {'network'}` and `deferOn: rules.deferOn('network')` to `queue.register`.

## Rate limits per host

Give each API host its own budget, so one busy service cannot take every slot:

```dart
queue.addHttpHostRateLimit(
  RateLimit(5, per: Duration(seconds: 1)),
  dio: dio, // resolves relative URLs against dio.options.baseUrl
);
```

* Every attempt counts, retries included. A request that has to wait is deferred (`TaskDeferred`, reason `rateLimit`, name `http-host`) and keeps its place in line.
* Hosts are compared by name and explicit port (`api.example.com`, `127.0.0.1:8080`).
* To limit all queued requests together instead, pass `rateLimit:` to `registerHttpRequests`. For your own task types, use `queue.addRateLimit` with a `keyOf` function; `HttpRequestTask.hostOf` shows how to read a host.

## Rules

| Error | Default |
|---|---|
| Connect, send, or receive timeout | Retry |
| Connection error (offline, DNS, refused, reset) | Retry |
| HTTP 408, 425, 429, 500, 502, 503, 504 | Retry |
| Other HTTP statuses | Fail |
| Cancelled, bad certificate, transform timeout, unknown | Fail |
| Errors that are not `DioException` | Fail (combine with your own `retryIf`) |

`Retry-After` (seconds or an HTTP-date) sets the wait before the next attempt, capped by `maxRetryAfter` (one hour by default). Without it, the task's `RetryPolicy` decides. Retries always count toward `maxAttempts`.

```dart
const rules = DioRetryRules(
  retryStatusCodes: {429, 503},
  retryTimeouts: true,
  retryConnectionErrors: true,
  useRetryAfter: true,
  maxRetryAfter: Duration(minutes: 10),
);
queue.registerHttpRequests(dio, rules: rules, idempotencyHeader: 'X-Request-Id');
```

## Things to know

* Delivery is at least once. If the app dies after the server handled a request but before the result was saved, the request is sent again. The idempotency key lets a server that supports it ignore the duplicate.
* The queue runs while your app runs. Requests stored with `durable_queue_sqlite` are sent the next time the app starts the queue.
* Responses outside Dio's accepted range (2xx by default) count as failures.

## Example

```sh
dart run example/main.dart
```

A message is stored while "offline", the app restarts, the server answers 503 with `Retry-After: 1`, and the message is delivered on the next attempt.

## Contributing

Issues and pull requests: [github.com/boffincoders/durable_queue](https://github.com/boffincoders/durable_queue). See [CONTRIBUTING.md](https://github.com/boffincoders/durable_queue/blob/main/CONTRIBUTING.md).
