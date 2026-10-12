import 'package:durable_queue/durable_queue.dart';
import 'package:durable_queue_test_support/durable_queue_test_support.dart';

void main() {
  queueStorageContractTests(MemoryQueueStorage.new);
}
