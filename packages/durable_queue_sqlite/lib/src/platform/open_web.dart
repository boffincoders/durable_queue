import 'package:sqlite3/wasm.dart';

import 'opened_database.dart';

/// Native only.
OpenedDatabase openFile(String path) => throw UnsupportedError(
  'SqliteQueueStorage.open needs a file system and is not available on the '
  'web. Use SqliteQueueStorage.openWeb.',
);

/// Native only.
OpenedDatabase openMemory() => throw UnsupportedError(
  'SqliteQueueStorage.inMemory is not available on the web. '
  'Use SqliteQueueStorage.openWeb(name, inMemory: true).',
);

/// One loaded SQLite module per URL, shared by every storage on the page.
final _modules = <String, Future<WasmSqlite3>>{};

/// Opens `name` in IndexedDB (or in memory) with the SQLite module at
/// [sqliteWasm].
Future<OpenedDatabase> openWeb(
  String name, {
  required Uri sqliteWasm,
  required bool inMemory,
}) async {
  final url = sqliteWasm.toString();
  final sqlite = await (_modules[url] ??= WasmSqlite3.loadFromUrl(sqliteWasm));
  final vfsName = 'durable_queue_${inMemory ? 'memory' : 'idb'}_$name';
  if (inMemory) {
    final fs = InMemoryFileSystem(name: vfsName);
    sqlite.registerVirtualFileSystem(fs);
    final db = sqlite.open('/$name.db', vfs: vfsName);
    return OpenedDatabase(
      db,
      release: () async => sqlite.unregisterVirtualFileSystem(fs),
    );
  }
  final fs = await IndexedDbFileSystem.open(dbName: name, vfsName: vfsName);
  sqlite.registerVirtualFileSystem(fs);
  try {
    final db = sqlite.open('/$name.db', vfs: vfsName);
    return OpenedDatabase(
      db,
      flush: fs.flush,
      release: () async {
        sqlite.unregisterVirtualFileSystem(fs);
        await fs.close();
      },
    );
  } catch (_) {
    sqlite.unregisterVirtualFileSystem(fs);
    await fs.close();
    rethrow;
  }
}
