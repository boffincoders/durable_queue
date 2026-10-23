import 'package:background_downloader/background_downloader.dart'
    show
        TaskConnectionException,
        TaskException,
        TaskFileSystemException,
        TaskResumeException,
        TaskStatus,
        TaskUrlException;

/// A transfer ended without success.
///
/// `QueueTransfers` reports it to the queue with `failHandOff`, so the task
/// type's `retryIf` decides whether the queue task is retried.
/// [isRetryableTransferError] is a ready-made rule.
final class TransferException implements Exception {
  /// Creates an exception for transfer [transferId].
  const TransferException(
    this.transferId,
    this.status, {
    this.httpStatusCode,
    this.cause,
    this.responseBody,
  });

  /// The background_downloader task id.
  final String transferId;

  /// The final status: [TaskStatus.failed] or [TaskStatus.notFound].
  final TaskStatus status;

  /// The HTTP status code of the response, if there was one.
  final int? httpStatusCode;

  /// What background_downloader reported, if anything.
  final TaskException? cause;

  /// The response body, if background_downloader passed one on.
  final String? responseBody;

  @override
  String toString() {
    final code = httpStatusCode == null ? '' : ' (HTTP $httpStatusCode)';
    final reason = cause == null ? '' : ': ${cause!.description}';
    return 'TransferException: transfer $transferId ${status.name}$code$reason';
  }
}

/// A handed-off transfer that background_downloader no longer knows about,
/// for example because its records were cleared. Retrying starts it again.
final class TransferLostException implements Exception {
  /// Creates an exception for transfer [transferId].
  const TransferLostException(this.transferId);

  /// The background_downloader task id.
  final String transferId;

  @override
  String toString() =>
      'TransferLostException: transfer $transferId is not known to '
      'background_downloader';
}

/// background_downloader refused to enqueue a transfer.
final class TransferEnqueueException implements Exception {
  /// Creates an exception for transfer [transferId].
  const TransferEnqueueException(this.transferId);

  /// The background_downloader task id.
  final String transferId;

  @override
  String toString() =>
      'TransferEnqueueException: background_downloader did not enqueue '
      'transfer $transferId';
}

/// A `retryIf` rule for task types that run transfers.
///
/// Retries:
///
/// * lost transfers and transfers that could not be enqueued;
/// * connection errors and interrupted resumes;
/// * HTTP 408, 425, 429, and 5xx responses;
/// * any other error, such as one thrown by `onComplete`.
///
/// Does not retry: [TaskStatus.notFound], other HTTP 4xx responses, and
/// invalid URLs or file system errors (for example a missing file to
/// upload), which fail the same way again.
bool isRetryableTransferError(Object error, StackTrace stackTrace) {
  if (error is! TransferException) return true;
  if (error.status == TaskStatus.notFound) return false;
  final code = error.httpStatusCode;
  if (code != null && code >= 400) {
    return code == 408 || code == 425 || code == 429 || code >= 500;
  }
  final cause = error.cause;
  if (cause is TaskUrlException || cause is TaskFileSystemException) {
    return false;
  }
  if (cause is TaskConnectionException || cause is TaskResumeException) {
    return true;
  }
  return true;
}
