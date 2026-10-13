import 'package:sqlite3/sqlite3.dart';

import 'opened_database.dart';

/// Opens a database file with write-ahead logging and full syncs.
OpenedDatabase openFile(String path) {
  final db = sqlite3.open(path);
  try {
    db.execute('PRAGMA journal_mode = WAL');
    db.execute('PRAGMA synchronous = FULL');
    db.execute('PRAGMA busy_timeout = 5000');
    return OpenedDatabase(db);
  } catch (_) {
    db.close();
    rethrow;
  }
}

/// Opens a private in-memory database.
OpenedDatabase openMemory() => OpenedDatabase(sqlite3.openInMemory());

/// Web only.
Future<OpenedDatabase> openWeb(
  String name, {
  required Uri sqliteWasm,
  required bool inMemory,
}) async {
  throw UnsupportedError(
    'SqliteQueueStorage.openWeb is only available on the web. '
    'Use SqliteQueueStorage.open on native platforms.',
  );
}
