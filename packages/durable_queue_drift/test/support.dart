import 'package:drift/drift.dart';
import 'package:drift/native.dart';

/// A drift database with no tables of its own, like an app's database
/// before it adds the queue.
final class TestDatabase extends GeneratedDatabase {
  TestDatabase(super.executor);

  TestDatabase.memory() : this(NativeDatabase.memory());

  @override
  Iterable<TableInfo<Table, Object?>> get allTables => const [];

  @override
  int get schemaVersion => 1;
}
