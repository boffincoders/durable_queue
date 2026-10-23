/// @docImport 'package:durable_queue/durable_queue.dart';
/// @docImport 'file_queue_lease.dart';
library;

/// Permission for one runner at a time to work through a queue's storage.
///
/// A [DurableQueue] that starts treats every task still marked running as
/// interrupted by a crash, and runs it again. If an app's foreground queue
/// and a background job open the same storage at the same time, each would
/// see the other's running tasks that way. A lease keeps them apart: only
/// the holder starts the queue.
///
/// A holder keeps the lease by calling [renew] well within [ttl]. A lease
/// that was not renewed for [ttl] counts as abandoned (its holder was
/// killed or frozen) and can be taken over.
///
/// [FileQueueLease] is the implementation for apps. Implement this to keep
/// the lease somewhere else, for example in the same database as the tasks.
abstract interface class QueueLease {
  /// How long the lease stays valid without [renew].
  Duration get ttl;

  /// Takes the lease if it is free or abandoned, and returns whether this
  /// object holds it now. Returns true if it already held it.
  Future<bool> tryAcquire();

  /// Extends the lease by [ttl]. Returns false if this object no longer
  /// holds it, for example because it was not renewed in time and another
  /// runner took it over.
  Future<bool> renew();

  /// Gives up the lease if this object holds it.
  Future<void> release();
}
