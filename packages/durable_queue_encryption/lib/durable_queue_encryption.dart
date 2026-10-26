/// Encryption at rest for `durable_queue`.
///
/// * [EncryptedQueueStorage]: wraps any `QueueStorage` and encrypts task
///   payloads, error messages, and (by default) deduplication keys and
///   groups before they are written.
/// * [AesGcmCipher]: AES-256-GCM with keys you keep in the platform's secure
///   storage, key rotation, and HMAC-SHA256 lookup tags.
/// * [PayloadCipher]: the interface to plug in another cipher.
library;

export 'src/aes_gcm_cipher.dart';
export 'src/encrypted_queue_storage.dart';
export 'src/payload_cipher.dart';
