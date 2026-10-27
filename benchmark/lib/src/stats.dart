/// Summary of repeated measurements, in microseconds.
final class Samples {
  Samples(Iterable<num> micros) : _sorted = micros.toList()..sort() {
    if (_sorted.isEmpty) throw ArgumentError('No samples');
  }

  final List<num> _sorted;

  int get count => _sorted.length;

  double get median => percentile(50);

  double get p95 => percentile(95);

  /// Nearest-rank percentile.
  double percentile(double p) {
    final rank = (p / 100 * _sorted.length).ceil().clamp(1, _sorted.length);
    return _sorted[rank - 1].toDouble();
  }

  Map<String, Object> toJson() => {
    'count': count,
    'medianMicros': median,
    'p95Micros': p95,
  };
}

/// Formats microseconds for a table: "85 µs", "1.4 ms", "2.31 s".
String formatMicros(num micros) {
  if (micros < 10) return '${micros.toStringAsFixed(2)} µs';
  if (micros < 1000) return '${micros.round()} µs';
  if (micros < 1000000) return '${_digits(micros / 1000)} ms';
  return '${_digits(micros / 1000000)} s';
}

/// Formats a rate: "12,300/s".
String formatRate(num perSecond) {
  final rounded = perSecond >= 100
      ? perSecond.round()
      : double.parse(perSecond.toStringAsFixed(1));
  final text = '$rounded';
  final whole = text.split('.');
  final grouped = whole.first.replaceAllMapped(
    RegExp(r'(\d)(?=(\d{3})+$)'),
    (m) => '${m[1]},',
  );
  return '${whole.length > 1 ? '$grouped.${whole[1]}' : grouped}/s';
}

String _digits(num value) => value >= 100
    ? value.toStringAsFixed(0)
    : value >= 10
    ? value.toStringAsFixed(1)
    : value.toStringAsFixed(2);
