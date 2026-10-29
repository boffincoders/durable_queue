import 'package:dio/dio.dart';
import 'package:durable_queue/durable_queue.dart';
import 'package:durable_queue_dio/durable_queue_dio.dart';
import 'package:test/test.dart';

DioException dioError(
  DioExceptionType type, {
  int? status,
  Map<String, List<String>> headers = const {},
}) {
  final options = RequestOptions(path: '/x');
  return DioException(
    requestOptions: options,
    type: type,
    response: status == null
        ? null
        : Response<Object?>(
            requestOptions: options,
            statusCode: status,
            headers: Headers.fromMap(headers),
          ),
  );
}

const queueTimeout = TaskTimeoutException(
  taskId: 't',
  timeout: Duration(seconds: 1),
);

void main() {
  const rules = DioRetryRules();
  final trace = StackTrace.empty;

  group('shouldRetry', () {
    test('retries timeouts and connection failures', () {
      for (final type in [
        DioExceptionType.connectionTimeout,
        DioExceptionType.sendTimeout,
        DioExceptionType.receiveTimeout,
        DioExceptionType.connectionError,
      ]) {
        expect(
          rules.shouldRetry(dioError(type), trace),
          isTrue,
          reason: '$type',
        );
      }
    });

    test('does not retry cancellation, certificates, or unknown errors', () {
      for (final type in [
        DioExceptionType.cancel,
        DioExceptionType.badCertificate,
        DioExceptionType.unknown,
        DioExceptionType.transformTimeout,
      ]) {
        expect(
          rules.shouldRetry(dioError(type), trace),
          isFalse,
          reason: '$type',
        );
      }
    });

    test('retries only the listed status codes', () {
      for (final status in [408, 425, 429, 500, 502, 503, 504]) {
        expect(
          rules.shouldRetry(
            dioError(DioExceptionType.badResponse, status: status),
            trace,
          ),
          isTrue,
          reason: '$status',
        );
      }
      for (final status in [400, 401, 403, 404, 409, 422, 501]) {
        expect(
          rules.shouldRetry(
            dioError(DioExceptionType.badResponse, status: status),
            trace,
          ),
          isFalse,
          reason: '$status',
        );
      }
    });

    test('can be configured', () {
      const custom = DioRetryRules(
        retryStatusCodes: {409},
        retryTimeouts: false,
        retryConnectionErrors: false,
      );
      expect(
        custom.shouldRetry(
          dioError(DioExceptionType.badResponse, status: 409),
          trace,
        ),
        isTrue,
      );
      expect(
        custom.shouldRetry(
          dioError(DioExceptionType.badResponse, status: 503),
          trace,
        ),
        isFalse,
      );
      expect(
        custom.shouldRetry(dioError(DioExceptionType.receiveTimeout), trace),
        isFalse,
      );
      expect(custom.shouldRetry(queueTimeout, trace), isFalse);
      expect(rules.shouldRetry(queueTimeout, trace), isTrue);
      expect(
        custom.shouldRetry(dioError(DioExceptionType.connectionError), trace),
        isFalse,
      );
    });

    test('ignores errors that are not DioExceptions', () {
      expect(rules.shouldRetry(StateError('x'), trace), isFalse);
    });
  });

  group('connection errors', () {
    test('isConnectionError covers connection failures only', () {
      expect(
        DioRetryRules.isConnectionError(
          dioError(DioExceptionType.connectionError),
        ),
        isTrue,
      );
      expect(
        DioRetryRules.isConnectionError(
          dioError(DioExceptionType.connectionTimeout),
        ),
        isTrue,
      );
      for (final type in [
        DioExceptionType.sendTimeout,
        DioExceptionType.receiveTimeout,
        DioExceptionType.cancel,
        DioExceptionType.unknown,
      ]) {
        expect(DioRetryRules.isConnectionError(dioError(type)), isFalse);
      }
      expect(
        DioRetryRules.isConnectionError(
          dioError(DioExceptionType.badResponse, status: 503),
        ),
        isFalse,
      );
      expect(DioRetryRules.isConnectionError(StateError('x')), isFalse);
    });

    test('deferOn names the gate for connection errors', () {
      final rule = rules.deferOn('network');
      expect(
        rule(dioError(DioExceptionType.connectionError), trace),
        'network',
      );
      expect(
        rule(dioError(DioExceptionType.badResponse, status: 500), trace),
        isNull,
      );
      expect(() => rules.deferOn(''), throwsArgumentError);
    });
  });

  group('HttpRequestTask.hostOf', () {
    StoredTask stored(String url, {String type = HttpRequestTask.taskType}) {
      final now = DateTime.utc(2026);
      return StoredTask(
        id: url,
        type: type,
        payload: HttpRequestTask(method: 'GET', url: url).toJson(),
        status: TaskStatus.pending,
        attempts: 0,
        retryPolicy: RetryPolicy.none(),
        createdAt: now,
        updatedAt: now,
        sequence: 1,
      );
    }

    test('reads the host and explicit port', () {
      expect(
        HttpRequestTask.hostOf(stored('https://API.example.com/a')),
        'api.example.com',
      );
      expect(
        HttpRequestTask.hostOf(stored('http://127.0.0.1:8080/a')),
        '127.0.0.1:8080',
      );
    });

    test('resolves relative URLs against baseUrl', () {
      expect(HttpRequestTask.hostOf(stored('/a')), isNull);
      expect(
        HttpRequestTask.hostOf(
          stored('/a'),
          baseUrl: 'https://api.example.com/v1/',
        ),
        'api.example.com',
      );
      expect(
        HttpRequestTask.hostOf(
          stored('https://other.example.com/a'),
          baseUrl: 'https://api.example.com',
        ),
        'other.example.com',
      );
    });

    test('ignores other task types', () {
      expect(
        HttpRequestTask.hostOf(stored('https://a.example.com', type: 'x')),
        isNull,
      );
    });
  });

  group('retryDelay', () {
    final now = DateTime.utc(2026, 10, 13, 12);
    final fixed = DioRetryRules(now: () => now);

    DioException withRetryAfter(String value, {int status = 429}) => dioError(
      DioExceptionType.badResponse,
      status: status,
      headers: {
        'retry-after': [value],
      },
    );

    test('reads seconds and HTTP dates', () {
      expect(
        fixed.retryDelay(withRetryAfter('7'), trace),
        const Duration(seconds: 7),
      );
      expect(
        fixed.retryDelay(
          withRetryAfter('Tue, 13 Oct 2026 12:02:00 GMT', status: 503),
          trace,
        ),
        const Duration(minutes: 2),
      );
    });

    test('caps at maxRetryAfter', () {
      expect(
        fixed.retryDelay(withRetryAfter('86400'), trace),
        const Duration(hours: 1),
      );
      const short = DioRetryRules(maxRetryAfter: Duration(seconds: 5));
      expect(
        short.retryDelay(withRetryAfter('60'), trace),
        const Duration(seconds: 5),
      );
    });

    test('returns null when the policy should decide', () {
      expect(
        fixed.retryDelay(
          dioError(DioExceptionType.badResponse, status: 503),
          trace,
        ),
        isNull,
      );
      expect(
        fixed.retryDelay(dioError(DioExceptionType.connectionError), trace),
        isNull,
      );
      expect(fixed.retryDelay(withRetryAfter('soon'), trace), isNull);
      expect(fixed.retryDelay(StateError('x'), trace), isNull);
      const off = DioRetryRules(useRetryAfter: false);
      expect(off.retryDelay(withRetryAfter('7'), trace), isNull);
    });
  });

  group('parseRetryAfter', () {
    final now = DateTime.utc(2026, 10, 13, 12);

    test('delay-seconds', () {
      expect(parseRetryAfter('0'), Duration.zero);
      expect(parseRetryAfter(' 120 '), const Duration(seconds: 120));
    });

    test('HTTP-date formats', () {
      expect(
        parseRetryAfter('Tue, 13 Oct 2026 12:00:30 GMT', now: now),
        const Duration(seconds: 30),
      );
      expect(
        parseRetryAfter('Tuesday, 13-Oct-26 12:00:30 GMT', now: now),
        const Duration(seconds: 30),
      );
      expect(
        parseRetryAfter('Tue Oct 13 12:00:30 2026', now: now),
        const Duration(seconds: 30),
      );
    });

    test('two-digit years follow the 50-year rule', () {
      // 99 is more than 50 years ahead of 2026, so it means 1999.
      expect(
        parseRetryAfter('Friday, 31-Dec-99 23:59:59 GMT', now: now),
        Duration.zero,
      );
      // 76 is exactly 50 years ahead, so it stays in this century.
      expect(
        parseRetryAfter('Tuesday, 13-Oct-76 12:00:30 GMT', now: now),
        RetryPolicy.maxSchedulableDelay,
      );
    });

    test('a date in the past means now', () {
      expect(
        parseRetryAfter('Mon, 12 Oct 2026 12:00:00 GMT', now: now),
        Duration.zero,
      );
    });

    test('rejects invalid values', () {
      for (final value in [
        null,
        '',
        '  ',
        '-5',
        '+5',
        '1.5',
        'later',
        '5 seconds',
      ]) {
        expect(parseRetryAfter(value, now: now), isNull, reason: '$value');
      }
    });

    test('caps huge values instead of overflowing', () {
      expect(
        parseRetryAfter('99999999999999999999999'),
        RetryPolicy.maxSchedulableDelay,
      );
      expect(
        parseRetryAfter('${400 * 24 * 3600}'),
        RetryPolicy.maxSchedulableDelay,
      );
    });
  });
}
