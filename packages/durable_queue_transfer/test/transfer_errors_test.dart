import 'package:background_downloader/background_downloader.dart';
import 'package:durable_queue_transfer/durable_queue_transfer.dart';
import 'package:durable_queue_transfer/src/transfer_ids.dart';
import 'package:flutter_test/flutter_test.dart';

void main() {
  test('transfer ids name the queue task and attempt', () {
    expect(TransferIds.build('abc-123', 2), 'dq-2-abc-123');
    expect(TransferIds.queueTaskId('dq-2-abc-123'), 'abc-123');
    expect(TransferIds.queueTaskId('dq-10-x'), 'x');
    expect(TransferIds.attempt('dq-10-x'), 10);
    expect(TransferIds.attempt('dq-2-abc-123'), 2);
    expect(TransferIds.attempt('other'), isNull);
    for (final other in [
      'abc',
      'dq-',
      'dq-2',
      'dq-2-',
      'dq--x',
      'dq-a-x',
      'xdq-1-x',
    ]) {
      expect(TransferIds.queueTaskId(other), isNull, reason: other);
    }
  });

  bool retry(Object error) => isRetryableTransferError(error, StackTrace.empty);

  TransferException failure({
    TaskStatus status = TaskStatus.failed,
    int? code,
    TaskException? cause,
  }) => TransferException('t', status, httpStatusCode: code, cause: cause);

  test('retry rules', () {
    expect(retry(const TransferLostException('t')), isTrue);
    expect(retry(const TransferEnqueueException('t')), isTrue);
    expect(retry(StateError('from onComplete')), isTrue);
    expect(retry(failure()), isTrue);
    expect(retry(failure(status: TaskStatus.notFound)), isFalse);
    for (final code in [408, 425, 429, 500, 503, 599]) {
      expect(retry(failure(code: code)), isTrue, reason: '$code');
    }
    for (final code in [400, 401, 403, 404, 409, 413, 422]) {
      expect(retry(failure(code: code)), isFalse, reason: '$code');
    }
    expect(retry(failure(code: 200)), isTrue);
    expect(retry(failure(cause: TaskUrlException('bad'))), isFalse);
    expect(retry(failure(cause: TaskFileSystemException('missing'))), isFalse);
    expect(retry(failure(cause: TaskConnectionException('offline'))), isTrue);
    expect(retry(failure(cause: TaskResumeException('etag'))), isTrue);
  });

  test('messages', () {
    expect(
      failure(code: 503, cause: TaskHttpException('busy', 503)).toString(),
      'TransferException: transfer t failed (HTTP 503): busy',
    );
    expect(
      failure(status: TaskStatus.notFound).toString(),
      'TransferException: transfer t notFound',
    );
    expect(
      const TransferLostException('t').toString(),
      contains('t is not known'),
    );
    expect(
      const TransferEnqueueException('t').toString(),
      contains('transfer t'),
    );
    final full = TransferException(
      't',
      TaskStatus.failed,
      httpStatusCode: 500,
      responseBody: 'oops',
    );
    expect(full.transferId, 't');
    expect(full.responseBody, 'oops');
  });
}
