## 0.1.1

* Passes `QueueStorageCallGuard` checks on to the wrapped storage, so
  `durable_queue_drift` still refuses calls made inside a drift transaction
  when it is encrypted.
* Requires `durable_queue` 0.5.10 or later.

## 0.1.0

* Initial release.
* `EncryptedQueueStorage`: wraps any `QueueStorage` and encrypts each
  task's payload and last error message and stack trace, with the task id
  as associated data. Deduplication keys and groups are stored as
  HMAC-SHA256 tags for lookups and restored on read
  (`protectLookupKeys`, on by default).
* `AesGcmCipher`: AES-256-GCM with a random nonce per write, subkeys derived
  with HMAC-SHA256, `generateKey()`, and key rotation through
  `previousKeys`.
* `PayloadCipher`: the interface for other ciphers.
* Tasks stored before encryption are still read (`readPlaintext`);
  `reencryptAll()` encrypts them, or moves every task to a new key.
* `PayloadDecryptionException` for records that cannot be decrypted, and
  `unreadableTaskIds()` to find them after a key is lost.
* Tested with the shared storage contract over memory and SQLite storage,
  and against data encrypted by an independent AES-GCM implementation.
