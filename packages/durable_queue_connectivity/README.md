# durable_queue_connectivity

A network gate for [`durable_queue`](https://pub.dev/packages/durable_queue). Tasks that need the network wait while the device is offline, without using attempts, and start as soon as it is back online. Tasks that do not need the network keep running.

Built on [`connectivity_plus`](https://pub.dev/packages/connectivity_plus), with an optional reachability check against your own server.

## Install

```yaml
dependencies:
  durable_queue: ^0.5.0
  durable_queue_connectivity: ^0.1.0
```

Requires Flutter, because `connectivity_plus` is a Flutter plugin. Follow its platform notes (for example the Android `ACCESS_NETWORK_STATE` permission, which it adds for you).

## Use it

```dart
final network = await ConnectivityGate.create();
queue.addGate('network', network); // before registering types that need it

queue.register<UploadTask>(
  type: 'upload',
  decoder: UploadTask.fromJson,
  handler: upload,
  requires: {'network'},
  deferOn: deferOnConnectionError('network'),
);

await queue.start();

// When the app shuts the queue down:
await queue.close();
await network.dispose();
```

* While no connection is up, `upload` tasks are deferred (`TaskDeferred` with reason `gateClosed`). They keep their attempt count and start the moment a connection comes back.
* `deferOnConnectionError` turns connection failures into a deferral instead of a failed attempt, and makes the gate check again. It matches `SocketException`, `HttpException`, `package:http`'s `ClientException`, and `TimeoutException` (pass `includeTimeouts: false` to leave timeouts to your retry policy). With Dio, use `DioRetryRules().deferOn('network')` from [`durable_queue_dio`](https://pub.dev/packages/durable_queue_dio) 0.2.0 instead; it also covers queued `HttpRequestTask`s.
* Tasks already running when the connection drops are not cancelled.

## Connected is not the same as online

`connectivity_plus` reports which connections are up (Wi-Fi, mobile, Ethernet), not whether the internet is reachable. Behind a captive portal, or on Wi-Fi without internet, it still says "Wi-Fi". Add a probe so the gate opens only when your server answers:

```dart
final network = await ConnectivityGate.create(
  probe: HttpProbe(Uri.parse('https://api.example.com/health')).call,
);
```

With a probe:

* the gate opens only after the probe succeeds;
* while a connection is up but the probe fails, it probes again every `probeRetryInterval` (default 30 seconds);
* when a task fails with a connection error, the queue asks the gate to check again; at most one probe runs every `minRecheckInterval` (default two seconds), however many tasks fail.

Probing happens only at those moments, not on a timer while everything works.

`HttpProbe` sends one `HEAD` request (5-second timeout). Any 2xx or 4xx status counts as reachable; 5xx, redirects, timeouts, and connection errors do not. Redirects are not followed because captive portals usually answer with one. Point it at your own API, ideally an endpoint that returns a fixed status such as 204, and pass `isReachable` if your endpoint behaves differently. On the web, the endpoint must allow the request with CORS.

Any `Future<bool> Function()` works as a probe. For example, with [`internet_connection_checker_plus`](https://pub.dev/packages/internet_connection_checker_plus), which you add to your own app:

```dart
final checker = InternetConnection.createInstance();
final network = await ConnectivityGate.create(
  probe: () => checker.hasInternetAccess,
);
```

Its default checks contact third-party hosts; this package itself sends no traffic anywhere except to the URL you give `HttpProbe`.

## Options

```dart
// Wi-Fi or Ethernet only, for large downloads:
final unmetered = await ConnectivityGate.create(
  accept: {ConnectivityResult.wifi, ConnectivityResult.ethernet},
);
queue.addGate('unmetered', unmetered);
```

| `ConnectivityGate.create` | Default | Meaning |
|---|---|---|
| `accept` | every type except `none` | Connection types that open the gate. |
| `probe` | none | Reachability check run before opening. |
| `probeRetryInterval` | 30 seconds | Wait between probes while connected but unreachable. |
| `minRecheckInterval` | 2 seconds | Shortest time between probes started by failing tasks. |

`gate.isOpen` and `gate.changes` show the state, for example for an offline banner. `gate.hasNetwork` is true while a connection is up, even if the probe fails.

## Without Flutter or `connectivity_plus`

`NetworkGate` takes any source of network state, so it also works with another connectivity package or in tests:

```dart
final gate = NetworkGate(
  networkChanges: myNetworkStream,      // Stream<bool>
  checkNetwork: () => myNetworkIsUp(),  // Future<bool>
  probe: HttpProbe(healthUrl).call,     // optional
);
await gate.start();
```

`durable_queue` itself also has `ManualQueueGate`, which you open and close yourself.

## Things to know

* The queue runs while your app runs. Going back online does not wake a closed app.
* With [`durable_queue_flutter`](https://pub.dev/packages/durable_queue_flutter), add the gate and register your task types before `DurableQueueScope` starts the queue.
* Deferred tasks are released when the gate opens. If the gate never reports opening, the queue checks again every `gateRecheckInterval` (five minutes by default; a `DurableQueue` setting).
* A task deferred by a connection error waits `gateErrorDelay` (30 seconds by default) unless the gate closes and reopens sooner. Deferrals do not use attempts, so if requests keep failing while the gate stays open, for example with no probe on a captive portal, the task keeps waiting instead of failing. A probe avoids that.

## Contributing

Issues and pull requests: [github.com/boffincoders/durable_queue](https://github.com/boffincoders/durable_queue). See [CONTRIBUTING.md](https://github.com/boffincoders/durable_queue/blob/main/CONTRIBUTING.md).
