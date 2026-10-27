// Runs the durable_queue benchmarks and prints Markdown tables.
//
//   dart run benchmark/bin/benchmark.dart [--quick] [--only Memory,Hive]
//       [--benchmarks enqueue,drain,recovery,history] [--out <directory>]
//
// --out writes results.md and results.json. See docs/benchmarks.md for the
// method.
import 'dart:convert';
import 'dart:io';

import 'package:durable_queue_benchmark/durable_queue_benchmark.dart';

Future<void> main(List<String> args) async {
  String? option(String name) {
    final index = args.indexOf('--$name');
    return index >= 0 && index + 1 < args.length ? args[index + 1] : null;
  }

  final quick = args.contains('--quick');
  final sizes = quick ? Sizes.quick : Sizes.full;
  final only = option('only')?.split(',').map((s) => s.trim()).toSet();
  final kinds = [
    for (final kind in allKinds)
      if (only == null || only.contains(kind.name)) kind,
  ];
  const all = ['enqueue', 'drain', 'recovery', 'history'];
  final benchmarks = option('benchmarks')?.split(',') ?? all;

  final runner = BenchmarkRunner(sizes, log: stderr.writeln);
  final results = <String, Map<String, Map<String, Object>>>{};
  try {
    for (final benchmark in all.where(benchmarks.contains)) {
      for (final kind in kinds) {
        final row = switch (benchmark) {
          'enqueue' => await runner.enqueue(kind),
          'drain' => await runner.drain(kind),
          'recovery' => await runner.recovery(kind),
          _ => await runner.history(kind),
        };
        results.putIfAbsent(benchmark, () => {})[kind.name] = row;
      }
    }
  } finally {
    await runner.dispose();
  }

  final env = environment(sizes, quick: quick);
  final text = markdown(env, results, sizes);
  stdout.write(text);
  final out = option('out');
  if (out != null) {
    Directory(out).createSync(recursive: true);
    File('$out/results.md').writeAsStringSync(text);
    File('$out/results.json').writeAsStringSync(
      const JsonEncoder.withIndent('  ')
          .convert({'environment': env, 'results': results}),
    );
  }
}
