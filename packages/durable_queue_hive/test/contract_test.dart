@TestOn('vm')
library;

import 'package:durable_queue_test_support/durable_queue_test_support.dart';
import 'package:test/test.dart';

import 'support/open_box.dart';

void main() {
  final hive = HiveFixture();
  tearDownAll(hive.dispose);

  group('flushing each write', () {
    queueStorageContractTests(() => OpeningStorage(hive.open(hive.nextName())));
  });

  group('without flushing', () {
    queueStorageContractTests(
      () => OpeningStorage(hive.open(hive.nextName(), flushEachWrite: false)),
    );
  });
}
