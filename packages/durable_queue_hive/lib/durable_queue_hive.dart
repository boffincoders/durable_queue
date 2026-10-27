/// Hive storage for `durable_queue`, on `hive_ce`.
///
/// [HiveQueueStorage] keeps every task in a Hive box, so the queue survives
/// app restarts and crashes, with indexed queries in memory.
library;

export 'src/hive_queue_storage.dart';
