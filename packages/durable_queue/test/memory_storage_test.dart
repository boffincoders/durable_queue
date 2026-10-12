import 'package:durable_queue/durable_queue.dart';

import 'storage_contract.dart';

void main() {
  queueStorageContractTests(MemoryQueueStorage.new);
}
