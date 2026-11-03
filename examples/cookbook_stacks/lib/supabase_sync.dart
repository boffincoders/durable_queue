// Recipe: sync records to Supabase, so edits made offline reach the server.
//
// A task stores only the record's id. When it runs, it reads the latest
// version from the app's local database and upserts it, so edits made while
// it waited are not lost, and a repeated attempt writes the same row again.
import 'package:durable_queue/durable_queue.dart';
import 'package:supabase_flutter/supabase_flutter.dart'
    show PostgrestException, SupabaseClient;

final class SyncNote extends DurableTask {
  SyncNote(this.noteId);

  final String noteId;

  static const taskType = 'sync_note';

  @override
  String get type => taskType;

  @override
  Map<String, dynamic> toJson() => {'noteId': noteId};

  static SyncNote fromJson(Map<String, dynamic> json) =>
      SyncNote(json['noteId'] as String);
}

/// Reads a note from the app's own database, or null if it was deleted.
typedef LoadNote = Future<Map<String, Object?>?> Function(String id);

void registerNoteSync(
  DurableQueue queue,
  SupabaseClient supabase,
  LoadNote loadNote,
) {
  queue.register<SyncNote>(
    type: SyncNote.taskType,
    decoder: SyncNote.fromJson,
    // Supabase answered with an error: the row is invalid or not allowed,
    // and sending it again will not help. Network errors are retried.
    retryIf: (error, stackTrace) => error is! PostgrestException,
    handler: (task, context) async {
      final note = await loadNote(task.noteId);
      if (note == null) {
        await supabase.from('notes').delete().eq('id', task.noteId);
      } else {
        await supabase.from('notes').upsert(note);
      }
    },
  );
}

/// Call after every local save or delete of a note.
Future<String> noteChanged(DurableQueue queue, String noteId) {
  // Each change adds a task. They all send the latest version, so extra
  // ones are cheap; the last one always runs after the last change.
  return queue.enqueue(
    SyncNote(noteId),
    group: 'sync',
    retryPolicy: RetryPolicy.exponential(
      maxAttempts: 10,
      initialDelay: const Duration(seconds: 5),
      maxDelay: const Duration(minutes: 15),
      jitter: true,
    ),
  );
}
