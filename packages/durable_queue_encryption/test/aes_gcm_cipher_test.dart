import 'dart:convert';

import 'package:durable_queue_encryption/durable_queue_encryption.dart';
import 'package:test/test.dart';

final _key = List<int>.generate(32, (i) => i);
final _aad = utf8.encode('task-1');

void main() {
  test('generateKey returns 32 different random bytes each time', () {
    final a = AesGcmCipher.generateKey();
    final b = AesGcmCipher.generateKey();
    expect(a.length, 32);
    expect(a, isNot(b));
  });

  test('keys must be 32 bytes and key ids distinct and non-empty', () {
    expect(() => AesGcmCipher(List.filled(16, 0)), throwsArgumentError);
    expect(
      () => AesGcmCipher([...List.filled(31, 0), 256]),
      throwsArgumentError,
    );
    expect(() => AesGcmCipher(_key, keyId: ''), throwsArgumentError);
    expect(
      () => AesGcmCipher(_key, previousKeys: {'k1': _key}),
      throwsArgumentError,
    );
    expect(
      () => AesGcmCipher(_key, previousKeys: {'': _key}),
      throwsArgumentError,
    );
    expect(
      () => AesGcmCipher(_key, previousKeys: {'old': List.filled(8, 0)}),
      throwsArgumentError,
    );
  });

  test('round trip; output is nonce, ciphertext, and tag', () async {
    final cipher = AesGcmCipher(_key);
    final plaintext = utf8.encode('hello');
    final sealed = await cipher.encrypt(plaintext, associatedData: _aad);
    expect(sealed.length, 12 + plaintext.length + 16);
    expect(
      await cipher.decrypt(sealed, keyId: 'k1', associatedData: _aad),
      plaintext,
    );
    expect(
      await cipher.encrypt(plaintext, associatedData: _aad),
      isNot(sealed),
    );
  });

  test('decryption fails with other associated data, an unknown key, or a '
      'changed byte', () async {
    final cipher = AesGcmCipher(_key);
    final sealed = await cipher.encrypt([1, 2, 3], associatedData: _aad);
    expect(
      cipher.decrypt(sealed, keyId: 'k1', associatedData: utf8.encode('x')),
      throws,
    );
    expect(
      cipher.decrypt(sealed, keyId: 'k9', associatedData: _aad),
      throwsArgumentError,
    );
    final changed = [...sealed]..[0] ^= 1;
    expect(cipher.decrypt(changed, keyId: 'k1', associatedData: _aad), throws);
  });

  test('lookup tags are stable, keyed, and list the current key first', () {
    final one = AesGcmCipher(_key);
    final two = AesGcmCipher(
      List.filled(32, 7),
      keyId: 'k2',
      previousKeys: {'k1': _key},
    );
    final tag = one.lookupTags('user@example.com').single;
    expect(one.lookupTags('user@example.com').single, tag);
    expect(one.lookupTags('user@example.org').single, isNot(tag));
    final tags = two.lookupTags('user@example.com');
    expect(tags.length, 2);
    expect(tags.last, tag);
    expect(tags.first, isNot(tag));
    expect(tag.contains('='), isFalse);
  });

  // The stored format must not change: data written by 0.1.0 has to stay
  // readable. These values were computed with another AES-GCM and HMAC
  // implementation (Python's `cryptography` and `hmac`), from key bytes
  // 0..31 and nonce bytes 200..211.
  test('reads data in the 0.1.0 format', () async {
    final cipher = AesGcmCipher(_key);
    expect(cipher.lookupTags('user@example.com').single, _frozenTag);
    expect(
      utf8.decode(
        await cipher.decrypt(
          base64.decode(_frozenSealed),
          keyId: 'k1',
          associatedData: _aad,
        ),
      ),
      'durable_queue_encryption',
    );
  });
}

const _frozenTag = 'dadNbOACsTRFoWMhHXdgVwNPf0NfPkbrZWnNlcP8iGY';
const _frozenSealed =
    'yMnKy8zNzs/Q0dLT6Ti+BYDXeXciMyla+ijXLPh0Qba7uFrVfn+MuLbYhxTVqW5dgA12mg==';
