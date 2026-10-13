import 'package:durable_queue/durable_queue.dart';
import 'package:http_parser/http_parser.dart';

final _digits = RegExp(r'^\d+$');

/// Parses an HTTP `Retry-After` header value.
///
/// Accepts delay-seconds (`"120"`) and an HTTP-date
/// (`"Wed, 21 Oct 2026 07:28:00 GMT"`). A date in the past gives
/// [Duration.zero]. Returns null for a missing or unparseable value.
/// Results are capped at [RetryPolicy.maxSchedulableDelay].
///
/// [now] is used for HTTP-dates and defaults to the current time.
Duration? parseRetryAfter(String? value, {DateTime? now}) {
  if (value == null) return null;
  final text = value.trim();
  if (text.isEmpty) return null;
  const cap = RetryPolicy.maxSchedulableDelay;
  if (_digits.hasMatch(text)) {
    // Compare digit counts first so huge values cannot overflow.
    final seconds = text.length > 12 ? null : int.parse(text);
    if (seconds == null || seconds > cap.inSeconds) return cap;
    return Duration(seconds: seconds);
  }
  final DateTime date;
  try {
    date = parseHttpDate(text);
  } on FormatException {
    return null;
  }
  final wait = date.difference((now ?? DateTime.now()).toUtc());
  if (wait.isNegative) return Duration.zero;
  return wait > cap ? cap : wait;
}
