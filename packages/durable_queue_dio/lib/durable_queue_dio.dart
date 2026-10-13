/// Dio integration for `durable_queue`.
///
/// * [DioRetryRules]: which Dio errors to retry, and `Retry-After` delays.
/// * [DurableQueueDioContext]: cancel tokens and idempotency headers for a
///   running task.
/// * [HttpRequestTask] and [DurableQueueDio.registerHttpRequests]: queue
///   plain HTTP requests without writing a task class.
library;

export 'src/http_request_task.dart';
export 'src/retry_after.dart';
export 'src/retry_rules.dart';
export 'src/task_context_dio.dart';
