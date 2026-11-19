/// `QueueLease` lives in `durable_queue` since 1.1.0, so that storages such
/// as `durable_queue_sqlite` can implement it. It is exported here too, so
/// existing imports keep working.
library;

export 'package:durable_queue/durable_queue.dart' show QueueLease;
