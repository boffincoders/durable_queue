import 'package:durable_queue/durable_queue.dart';
import 'package:test/test.dart';

void main() {
  const fraction = 0.5;

  group('RetryPolicy.none', () {
    test('allows one attempt and does not wait', () {
      final policy = RetryPolicy.none();
      expect(policy.maxAttempts, 1);
      expect(policy.delayAfter(1, fraction), Duration.zero);
    });
  });

  group('RetryPolicy.fixed', () {
    test('returns the same delay after every attempt', () {
      final policy = RetryPolicy.fixed(
        maxAttempts: 3,
        delay: const Duration(seconds: 2),
      );

      expect(policy.delayAfter(1, fraction), const Duration(seconds: 2));
      expect(policy.delayAfter(2, 0), const Duration(seconds: 2));
    });

    test('rejects an empty budget or a negative delay', () {
      expect(
        () => RetryPolicy.fixed(maxAttempts: 0, delay: Duration.zero),
        throwsArgumentError,
      );
      expect(
        () => RetryPolicy.fixed(
          maxAttempts: 1,
          delay: const Duration(seconds: -1),
        ),
        throwsArgumentError,
      );
    });
  });

  group('RetryPolicy.exponential', () {
    test('doubles the delay and honors the maximum', () {
      final policy = RetryPolicy.exponential(
        maxAttempts: 6,
        initialDelay: const Duration(seconds: 1),
        maxDelay: const Duration(seconds: 5),
      );

      expect(policy.delayAfter(1, fraction), const Duration(seconds: 1));
      expect(policy.delayAfter(2, fraction), const Duration(seconds: 2));
      expect(policy.delayAfter(3, fraction), const Duration(seconds: 4));
      expect(policy.delayAfter(4, fraction), const Duration(seconds: 5));
      expect(policy.delayAfter(5, fraction), const Duration(seconds: 5));
    });

    test('jitter spreads the delay around the backoff value', () {
      final policy = RetryPolicy.exponential(
        maxAttempts: 4,
        initialDelay: const Duration(seconds: 10),
        jitter: true,
        jitterFactor: 0.2,
      );

      expect(policy.delayAfter(1, 0.5), const Duration(seconds: 10));
      expect(policy.delayAfter(1, 0), const Duration(seconds: 8));
      expect(policy.delayAfter(2, 0), const Duration(seconds: 16));
      final nearUpper = policy.delayAfter(1, 0.999999);
      expect(nearUpper, greaterThan(const Duration(seconds: 11)));
      expect(nearUpper, lessThan(const Duration(seconds: 12)));
    });

    test('jitter does not exceed maxDelay', () {
      final policy = RetryPolicy.exponential(
        maxAttempts: 4,
        initialDelay: const Duration(seconds: 10),
        maxDelay: const Duration(seconds: 10),
        jitter: true,
        jitterFactor: 1,
      );

      expect(policy.delayAfter(3, 0.999999), const Duration(seconds: 10));
    });

    test('rejects maxDelay below the initial delay', () {
      expect(
        () => RetryPolicy.exponential(
          maxAttempts: 2,
          initialDelay: const Duration(seconds: 5),
          maxDelay: const Duration(seconds: 1),
        ),
        throwsArgumentError,
      );
    });

    test('rejects a jitter factor outside (0, 1]', () {
      expect(
        () => RetryPolicy.exponential(
          maxAttempts: 2,
          initialDelay: const Duration(seconds: 1),
          jitterFactor: 0,
        ),
        throwsArgumentError,
      );
    });
  });

  test('policies round-trip through json', () {
    final policies = [
      RetryPolicy.none(),
      RetryPolicy.fixed(
        maxAttempts: 3,
        delay: const Duration(milliseconds: 250),
      ),
      RetryPolicy.exponential(
        maxAttempts: 5,
        initialDelay: const Duration(seconds: 1),
        maxDelay: const Duration(minutes: 1),
        jitter: true,
        jitterFactor: 0.2,
      ),
    ];

    for (final policy in policies) {
      expect(RetryPolicy.fromJson(policy.toJson()), policy);
    }
  });

  test('unknown policy json is rejected', () {
    expect(
      () => RetryPolicy.fromJson({'type': 'linear'}),
      throwsFormatException,
    );
  });
}
