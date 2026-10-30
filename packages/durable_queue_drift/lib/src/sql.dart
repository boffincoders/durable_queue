/// SQL used by `DriftQueueStorage`. The same schema as
/// `durable_queue_sqlite`, so either package can open the tables.
///
/// Kept in one place so tests can inspect query plans. Not exported.
library;

/// Statuses the worker may claim.
const _schedulable = "('pending', 'retryScheduled')";

/// Statuses that hold a deduplication key.
const _active = "('pending', 'waiting', 'running', 'retryScheduled')";

/// Enqueue order: oldest first, then sequence, then id.
const _order = 'created_at_us, sequence, id';

/// Schema version written by this release.
const int currentSchemaVersion = 1;

/// SQL statements for one table prefix.
final class QueueSql {
  /// Creates statements for tables named with [prefix].
  QueueSql(String prefix)
    : tasks = '${prefix}tasks',
      edges = '${prefix}waiting_edges',
      meta = '${prefix}meta',
      _p = prefix;

  final String _p;

  /// Table holding one row per task.
  final String tasks;

  /// Table holding (dependency, waiting task) pairs.
  final String edges;

  /// Table holding the schema version.
  final String meta;

  /// Creates the metadata table.
  String get createMeta =>
      'CREATE TABLE IF NOT EXISTS $meta '
      '(key TEXT NOT NULL PRIMARY KEY, value TEXT NOT NULL)';

  /// Reads the schema version.
  String get selectVersion => "SELECT value FROM $meta WHERE key = 'schema'";

  /// Writes the schema version.
  String get upsertVersion =>
      "INSERT INTO $meta (key, value) VALUES ('schema', ?) "
      'ON CONFLICT (key) DO UPDATE SET value = excluded.value';

  /// Schema version 1.
  List<String> get schemaV1 => [
    'CREATE TABLE IF NOT EXISTS $tasks ('
        'id TEXT NOT NULL PRIMARY KEY, '
        'status TEXT NOT NULL, '
        'priority INTEGER NOT NULL, '
        'created_at_us INTEGER NOT NULL, '
        'sequence INTEGER NOT NULL, '
        'next_attempt_at_us INTEGER, '
        'deduplication_key TEXT, '
        'task_group TEXT, '
        'record TEXT NOT NULL)',
    'CREATE TABLE IF NOT EXISTS $edges ('
        'dependency_id TEXT NOT NULL, '
        'task_id TEXT NOT NULL, '
        'PRIMARY KEY (dependency_id, task_id)) WITHOUT ROWID',
    'CREATE INDEX IF NOT EXISTS ${_p}tasks_by_status '
        'ON $tasks (status, $_order)',
    'CREATE INDEX IF NOT EXISTS ${_p}tasks_ready '
        'ON $tasks (priority DESC, $_order) '
        'WHERE status IN $_schedulable',
    'CREATE INDEX IF NOT EXISTS ${_p}tasks_wake '
        'ON $tasks (next_attempt_at_us) '
        'WHERE status IN $_schedulable AND next_attempt_at_us IS NOT NULL',
    'CREATE INDEX IF NOT EXISTS ${_p}tasks_dedup '
        'ON $tasks (deduplication_key, $_order) '
        'WHERE deduplication_key IS NOT NULL AND status IN $_active',
    'CREATE INDEX IF NOT EXISTS ${_p}tasks_group '
        'ON $tasks (task_group, $_order) WHERE task_group IS NOT NULL',
    'CREATE INDEX IF NOT EXISTS ${_p}tasks_sequence ON $tasks (sequence)',
    'CREATE INDEX IF NOT EXISTS ${_p}edges_by_task ON $edges (task_id)',
  ];

  /// Columns written for every task, in [insertTask] / [updateTask] order.
  static const columns = [
    'status',
    'priority',
    'created_at_us',
    'sequence',
    'next_attempt_at_us',
    'deduplication_key',
    'task_group',
    'record',
  ];

  /// Checks whether an id exists.
  String get exists => 'SELECT 1 FROM $tasks WHERE id = ?';

  /// Inserts a task: id, then [columns].
  String get insertTask =>
      'INSERT INTO $tasks (id, ${columns.join(', ')}) '
      'VALUES (?, ${List.filled(columns.length, '?').join(', ')})';

  /// Replaces a task: [columns], then id.
  String get updateTask =>
      'UPDATE $tasks SET ${columns.map((c) => '$c = ?').join(', ')} '
      'WHERE id = ?';

  /// Deletes a task by id.
  String get deleteTask => 'DELETE FROM $tasks WHERE id = ?';

  /// Deletes a task's dependency edges.
  String get deleteEdges => 'DELETE FROM $edges WHERE task_id = ?';

  /// Inserts one dependency edge.
  String get insertEdge =>
      'INSERT OR IGNORE INTO $edges (dependency_id, task_id) VALUES (?, ?)';

  /// One task by id.
  String get selectById => 'SELECT record FROM $tasks WHERE id = ?';

  /// Every task in enqueue order.
  String get selectAll => 'SELECT record FROM $tasks ORDER BY $_order';

  /// Tasks in one status, in enqueue order. Append [limitClause] if needed.
  String get selectByStatus =>
      'SELECT record FROM $tasks WHERE status = ? ORDER BY $_order';

  /// Appended to a query to bound it.
  static const limitClause = ' LIMIT ?';

  // `INDEXED BY` and `CROSS JOIN` pin the plans the tests check. Without
  // them SQLite may pick the status index and sort the whole backlog.

  /// Next claimable task: highest priority, then enqueue order.
  String get selectNextReady =>
      'SELECT record FROM $tasks INDEXED BY ${_p}tasks_ready '
      'WHERE status IN $_schedulable '
      'AND (next_attempt_at_us IS NULL OR next_attempt_at_us <= ?) '
      'ORDER BY priority DESC, $_order LIMIT 1';

  /// Task with the earliest scheduled time.
  String get selectNextWake =>
      'SELECT record FROM $tasks INDEXED BY ${_p}tasks_wake '
      'WHERE status IN $_schedulable AND next_attempt_at_us IS NOT NULL '
      'ORDER BY next_attempt_at_us LIMIT 1';

  /// Largest sequence, or null when empty.
  String get selectMaxSequence => 'SELECT MAX(sequence) AS max FROM $tasks';

  /// Oldest active task with a deduplication key.
  String get selectActiveByKey =>
      'SELECT record FROM $tasks INDEXED BY ${_p}tasks_dedup '
      'WHERE deduplication_key = ? AND deduplication_key IS NOT NULL '
      'AND status IN $_active '
      'ORDER BY $_order LIMIT 1';

  /// Waiting tasks that depend on an id, in enqueue order.
  String get selectWaitingDependents =>
      'SELECT t.record FROM $edges e CROSS JOIN $tasks t ON t.id = e.task_id '
      "WHERE e.dependency_id = ? AND t.status = 'waiting' "
      'ORDER BY t.created_at_us, t.sequence, t.id';

  /// Tasks in a group, in enqueue order.
  String get selectByGroup =>
      'SELECT record FROM $tasks INDEXED BY ${_p}tasks_group '
      'WHERE task_group = ? AND task_group IS NOT NULL ORDER BY $_order';

  /// Tasks in a group and status, in enqueue order.
  String get selectByGroupAndStatus =>
      'SELECT record FROM $tasks INDEXED BY ${_p}tasks_group '
      'WHERE task_group = ? AND task_group IS NOT NULL AND status = ? '
      'ORDER BY $_order';
}
