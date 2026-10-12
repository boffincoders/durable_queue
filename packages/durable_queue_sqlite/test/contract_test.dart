import 'dart:io';

import 'package:durable_queue_sqlite/durable_queue_sqlite.dart';
import 'package:durable_queue_test_support/durable_queue_test_support.dart';
import 'package:test/test.dart';

void main() {
  final directory = Directory.systemTemp.createTempSync('dq_sqlite_contract_');
  var files = 0;
  final opened = <SqliteQueueStorage>[];

  SqliteQueueStorage track(SqliteQueueStorage storage) {
    opened.add(storage);
    return storage;
  }

  tearDownAll(() async {
    for (final storage in opened) {
      await storage.close();
    }
    directory.deleteSync(recursive: true);
  });

  group('file database', () {
    queueStorageContractTests(
      () => track(
        SqliteQueueStorage.open('${directory.path}/queue-${files++}.db'),
      ),
    );
  });

  group('in-memory database', () {
    queueStorageContractTests(() => track(SqliteQueueStorage.inMemory()));
  });
}
