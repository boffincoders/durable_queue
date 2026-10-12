/// SQLite storage for `durable_queue`.
///
/// ```dart
/// final queue = DurableQueue(
///   storage: SqliteQueueStorage.open('${dir.path}/queue.db'),
/// );
/// ```
library;

export 'src/sqlite_queue_storage.dart';
