import 'opened_database.dart';

Never _unsupported() => throw UnsupportedError(
  'durable_queue_sqlite needs dart:ffi (native) or the web platform.',
);

/// Opens a database file. Native only.
OpenedDatabase openFile(String path) => _unsupported();

/// Opens a private in-memory database. Native only.
OpenedDatabase openMemory() => _unsupported();

/// Opens an IndexedDB-backed database. Web only.
Future<OpenedDatabase> openWeb(
  String name, {
  required Uri sqliteWasm,
  required bool inMemory,
}) async => _unsupported();
