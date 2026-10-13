import 'package:sqlite3/common.dart';

/// A database opened by this package, and what to do around it.
final class OpenedDatabase {
  /// Wraps [database].
  OpenedDatabase(this.database, {this.flush, this.release});

  /// The open database.
  final CommonDatabase database;

  /// Persists completed writes, if the platform buffers them (web).
  final Future<void> Function()? flush;

  /// Releases platform resources after the database is closed (web).
  final Future<void> Function()? release;
}
