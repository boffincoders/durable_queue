import 'dart:io';

import 'package:drift/native.dart';
import 'package:durable_queue_drift/durable_queue_drift.dart';
import 'package:durable_queue_test_support/durable_queue_test_support.dart';
import 'package:test/test.dart';

import 'support.dart';

void main() {
  final directory = Directory.systemTemp.createTempSync('dq_drift_contract_');
  var files = 0;
  final databases = <TestDatabase>[];

  Future<DriftQueueStorage> openOn(TestDatabase database) {
    databases.add(database);
    return DriftQueueStorage.open(database);
  }

  tearDownAll(() async {
    for (final database in databases) {
      await database.close();
    }
    directory.deleteSync(recursive: true);
  });

  group('file database', () {
    queueStorageContractTests(
      () => openOn(
        TestDatabase(
          NativeDatabase(File('${directory.path}/queue-${files++}.db')),
        ),
      ),
    );
  });

  group('in-memory database', () {
    queueStorageContractTests(() => openOn(TestDatabase.memory()));
  });
}
