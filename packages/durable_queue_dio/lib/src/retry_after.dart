import 'package:durable_queue/durable_queue.dart';
import 'package:http_parser/http_parser.dart';

final _digits = RegExp(r'^\d+$');

// RFC 850 dates, such as "Sunday, 06-Nov-94 08:49:37 GMT", carry a
// two-digit year.
final _rfc850 = RegExp(r'^[A-Za-z]+, \d{2}-[A-Za-z]{3}-\d{2} ');

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
  final reference = (now ?? DateTime.now()).toUtc();
  DateTime date;
  try {
    date = parseHttpDate(text);
  } on FormatException {
    return null;
  }
  if (_rfc850.hasMatch(text)) date = _withRfc9110Year(date, reference);
  final wait = date.difference(reference);
  if (wait.isNegative) return Duration.zero;
  return wait > cap ? cap : wait;
}

/// Applies the RFC 9110 rule to a two-digit year: it is the year in the
/// current century unless that is more than 50 years ahead of [reference],
/// in which case it is the year a century earlier.
///
/// http_parser 4.1.2 and earlier always read two-digit years as 19xx, and
/// later versions apply the rule against the system clock, so the year is
/// recomputed here from [reference].
DateTime _withRfc9110Year(DateTime date, DateTime reference) {
  var year = reference.year - reference.year % 100 + date.year % 100;
  if (year > reference.year + 50) year -= 100;
  return DateTime.utc(
    year,
    date.month,
    date.day,
    date.hour,
    date.minute,
    date.second,
  );
}
