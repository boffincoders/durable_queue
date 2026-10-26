import 'package:durable_queue/durable_queue.dart';
import 'package:durable_queue_encryption/durable_queue_encryption.dart';
import 'package:durable_queue_test_support/durable_queue_test_support.dart';
import 'package:test/test.dart';

// The wrapped storage must still behave like any other storage.
void main() {
  final key = List<int>.generate(32, (i) => i);

  group('lookup keys hidden', () {
    queueStorageContractTests(
      () => EncryptedQueueStorage(
        MemoryQueueStorage(),
        cipher: AesGcmCipher(key),
      ),
    );
  });

  group('lookup keys readable', () {
    queueStorageContractTests(
      () => EncryptedQueueStorage(
        MemoryQueueStorage(),
        cipher: AesGcmCipher(key),
        protectLookupKeys: false,
      ),
    );
  });

  group('plaintext records refused', () {
    queueStorageContractTests(
      () => EncryptedQueueStorage(
        MemoryQueueStorage(),
        cipher: AesGcmCipher(key),
        readPlaintext: false,
      ),
    );
  });
}
