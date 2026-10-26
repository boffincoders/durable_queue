import 'dart:convert';
import 'dart:math';
import 'dart:typed_data';

import 'package:crypto/crypto.dart' as crypto;
import 'package:cryptography/cryptography.dart';

import 'payload_cipher.dart';

/// AES-256-GCM encryption with HMAC-SHA256 lookup tags.
///
/// Each key is 32 random bytes; create one with [generateKey] and keep it in
/// the platform's secure storage (Keychain, Android Keystore). Two subkeys
/// are derived from it, one for encryption and one for lookup tags, so the
/// key itself is never used for both.
///
/// Every [encrypt] call uses a new random 96-bit nonce. The output is the
/// nonce, the ciphertext, and the 128-bit authentication tag.
///
/// Encryption runs through `package:cryptography`: pure Dart on the Dart VM,
/// the Web Crypto API in browsers. In Flutter apps, adding
/// `cryptography_flutter` makes it use the platform's native implementation.
final class AesGcmCipher implements PayloadCipher {
  /// Creates a cipher that encrypts with [key], identified as [keyId].
  ///
  /// [previousKeys] maps the ids of earlier keys to the keys, so records
  /// encrypted before a key change can still be read. See
  /// `EncryptedQueueStorage.reencryptAll` to move them to the new key.
  ///
  /// Every key must be 32 bytes. Key ids must be non-empty and distinct.
  AesGcmCipher(
    List<int> key, {
    String keyId = 'k1',
    Map<String, List<int>> previousKeys = const {},
  }) : _keyId = keyId {
    if (keyId.isEmpty) {
      throw ArgumentError.value(keyId, 'keyId', 'Must not be empty');
    }
    if (previousKeys.containsKey(keyId)) {
      throw ArgumentError.value(
        keyId,
        'previousKeys',
        'Must not contain the current key id',
      );
    }
    _keys[keyId] = _DerivedKeys(_checkKey(key, 'key'));
    for (final MapEntry(key: id, value: bytes) in previousKeys.entries) {
      if (id.isEmpty) {
        throw ArgumentError.value(
          id,
          'previousKeys',
          'Key ids must not be empty',
        );
      }
      _keys[id] = _DerivedKeys(_checkKey(bytes, 'previousKeys[$id]'));
    }
  }

  /// Returns 32 random bytes from a cryptographically secure generator.
  static Uint8List generateKey() {
    final random = Random.secure();
    return Uint8List.fromList([
      for (var i = 0; i < 32; i++) random.nextInt(256),
    ]);
  }

  static final _algorithm = AesGcm.with256bits();

  final String _keyId;

  // The current key first.
  final _keys = <String, _DerivedKeys>{};

  @override
  String get keyId => _keyId;

  @override
  Future<List<int>> encrypt(
    List<int> plaintext, {
    required List<int> associatedData,
  }) async {
    final box = await _algorithm.encrypt(
      plaintext,
      secretKey: _keys[_keyId]!.encryption,
      aad: associatedData,
    );
    return box.concatenation();
  }

  @override
  Future<List<int>> decrypt(
    List<int> ciphertext, {
    required String keyId,
    required List<int> associatedData,
  }) async {
    final keys = _keys[keyId];
    if (keys == null) {
      throw ArgumentError.value(keyId, 'keyId', 'Unknown key');
    }
    final box = SecretBox.fromConcatenation(
      ciphertext,
      nonceLength: _algorithm.nonceLength,
      macLength: _algorithm.macAlgorithm.macLength,
    );
    return _algorithm.decrypt(
      box,
      secretKey: keys.encryption,
      aad: associatedData,
    );
  }

  @override
  List<String> lookupTags(String value) {
    final bytes = utf8.encode(value);
    return [
      for (final keys in _keys.values)
        base64Url.encode(keys.lookup.convert(bytes).bytes).replaceAll('=', ''),
    ];
  }

  static List<int> _checkKey(List<int> key, String name) {
    if (key.length != 32) {
      throw ArgumentError.value(
        '${key.length} bytes',
        name,
        'Must be 32 bytes (AES-256)',
      );
    }
    for (final byte in key) {
      if (byte < 0 || byte > 255) {
        throw ArgumentError.value(byte, name, 'Must contain bytes (0-255)');
      }
    }
    return List<int>.unmodifiable(key);
  }
}

final class _DerivedKeys {
  _DerivedKeys(List<int> key)
    : encryption = SecretKey(_derive(key, 'encryption')),
      lookup = crypto.Hmac(crypto.sha256, _derive(key, 'lookup'));

  final SecretKey encryption;
  final crypto.Hmac lookup;

  static List<int> _derive(List<int> key, String purpose) => crypto.Hmac(
    crypto.sha256,
    key,
  ).convert(utf8.encode('durable_queue_encryption/v1/$purpose')).bytes;
}
