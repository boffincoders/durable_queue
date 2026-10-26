/// @docImport 'aes_gcm_cipher.dart';
/// @docImport 'encrypted_queue_storage.dart';
library;

/// Encrypts the protected fields of a task for [EncryptedQueueStorage].
///
/// [AesGcmCipher] is the implementation shipped with this package. Implement
/// this interface to use another algorithm or keys held by the platform.
abstract interface class PayloadCipher {
  /// Identifies the key [encrypt] uses. It is stored next to each encrypted
  /// record and passed back to [decrypt].
  String get keyId;

  /// Encrypts [plaintext] with the key [keyId].
  ///
  /// [associatedData] must be authenticated but not encrypted: [decrypt]
  /// fails when it is given different associated data. The storage passes
  /// the task id, so a record copied onto another task does not decrypt.
  Future<List<int>> encrypt(
    List<int> plaintext, {
    required List<int> associatedData,
  });

  /// Decrypts data produced by [encrypt] with the key [keyId].
  ///
  /// Throws when the key is unknown, the data was changed, or
  /// [associatedData] differs from the data passed to [encrypt].
  Future<List<int>> decrypt(
    List<int> ciphertext, {
    required String keyId,
    required List<int> associatedData,
  });

  /// Keyed hashes of [value], used to look tasks up by deduplication key and
  /// group without storing those values in plain text.
  ///
  /// The same value and key must always give the same hash. Return one hash
  /// per known key, the hash for [keyId] first, so tasks stored under an
  /// earlier key are still found.
  List<String> lookupTags(String value);
}
