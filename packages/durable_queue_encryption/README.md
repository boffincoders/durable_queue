# durable_queue_encryption

Encrypts what [`durable_queue`](https://pub.dev/packages/durable_queue) tasks contain before they are written: payloads, error messages, and by default deduplication keys and groups. It wraps any storage, including [`durable_queue_sqlite`](https://pub.dev/packages/durable_queue_sqlite), and the queue works as before.

Tasks often carry personal data: a message to send, an address, a form the user filled in offline. Without this package they are stored as plain JSON, readable by anyone who gets a copy of the database file, for example from a backup or a rooted device.

## What is encrypted

| Field | Stored as |
| --- | --- |
| Payload | AES-256-GCM ciphertext |
| Error message and stack trace of the last failure | Inside the ciphertext; the stored error reads `[encrypted]` |
| Deduplication key and group | Inside the ciphertext, plus an HMAC-SHA256 tag used for lookups (turn off with `protectLookupKeys: false`) |
| Id, type, status, attempts, times, priority, dependencies, idempotency key, hand-off reference | Plain, because the queue queries and orders by them. Keep secrets out of them. |

Each record is encrypted with a new random nonce, and the task id is authenticated with it: a record that was changed, or copied onto another task, does not decrypt.

## Install

```
dart pub add durable_queue_encryption
```

## Use

```dart
import 'package:durable_queue/durable_queue.dart';
import 'package:durable_queue_encryption/durable_queue_encryption.dart';
import 'package:durable_queue_sqlite/durable_queue_sqlite.dart';

final storage = EncryptedQueueStorage(
  SqliteQueueStorage.open(path),
  cipher: AesGcmCipher(key), // 32 bytes, see below
);
final queue = DurableQueue(storage: storage);
```

Everything else stays the same: handlers receive the original payload, and `getTask`, `getTasks`, deduplication, and groups work with the original values. Every queue that opens the same database, including one in a background job, must wrap it the same way.

## Keeping the key

The key is 32 random bytes. Create it once with `AesGcmCipher.generateKey()` and keep it in the platform's secure storage, not next to the database. With [`flutter_secure_storage`](https://pub.dev/packages/flutter_secure_storage) (Keychain on iOS and macOS, Keystore on Android):

```dart
import 'dart:convert';

import 'package:flutter_secure_storage/flutter_secure_storage.dart';

Future<List<int>> queueKey() async {
  const secure = FlutterSecureStorage();
  const name = 'durable_queue_key';
  final stored = await secure.read(key: name);
  if (stored != null) return base64.decode(stored);
  final key = AesGcmCipher.generateKey();
  await secure.write(key: name, value: base64.encode(key));
  return key;
}
```

If the key is lost (for example, the database came back from a backup but the key did not), the tasks it encrypted cannot be read. Reading one throws `PayloadDecryptionException`, and a running queue reports it on `storageErrors` and keeps retrying. Before starting the queue, delete such tasks:

```dart
for (final id in await storage.unreadableTaskIds()) {
  await storage.delete(id);
}
```

## Adding encryption to an existing app

Tasks stored before you added the wrapper are still read, and every write from then on is encrypted. To encrypt the remaining ones at once, call `reencryptAll()` before starting the queue. Pass `readPlaintext: false` to treat unencrypted records as unreadable instead.

## Changing the key

Give the new key a new id and keep the old one for reading:

```dart
final cipher = AesGcmCipher(
  newKey,
  keyId: 'k2',
  previousKeys: {'k1': oldKey},
);
final storage = EncryptedQueueStorage(sqlite, cipher: cipher);
await storage.reencryptAll(); // rewrites every task with k2
```

After `reencryptAll()`, the old key is no longer needed.

## Speed and platforms

Encryption runs through [`cryptography`](https://pub.dev/packages/cryptography): pure Dart on the Dart VM and the Web Crypto API in browsers. In Flutter apps, also add [`cryptography_flutter`](https://pub.dev/packages/cryptography_flutter) to use the operating system's native AES. Each read and write of a task encrypts or decrypts it once.

To use another algorithm, or keys that never leave the platform's keystore, implement `PayloadCipher`.

## What it does not do

* It does not hide that tasks exist, how many there are, their types, or when they run.
* It does not stop someone who can write the database from changing a task's status or deleting it.
* It does not protect data while the app runs: handlers receive decrypted payloads.

## Report a vulnerability

See [SECURITY.md](https://github.com/boffincoders/durable_queue/blob/main/SECURITY.md).
