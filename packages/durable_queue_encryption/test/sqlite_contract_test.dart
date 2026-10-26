@TestOn('vm')
library;

import 'package:durable_queue_encryption/durable_queue_encryption.dart';
import 'package:durable_queue_sqlite/durable_queue_sqlite.dart';
import 'package:durable_queue_test_support/durable_queue_test_support.dart';
import 'package:test/test.dart';

// The storage contract over real SQLite, with encryption in between.
void main() {
  final key = List<int>.generate(32, (i) => i);
  final opened = <SqliteQueueStorage>[];

  tearDownAll(() async {
    for (final storage in opened) {
      await storage.close();
    }
  });

  queueStorageContractTests(() {
    final sqlite = SqliteQueueStorage.inMemory();
    opened.add(sqlite);
    return EncryptedQueueStorage(sqlite, cipher: AesGcmCipher(key));
  });
}
