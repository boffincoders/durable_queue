// Checks that install snippets in the READMEs and docs ask for the current
// version of each package, for example `durable_queue: ^1.1.1` while
// packages/durable_queue/pubspec.yaml says `version: 1.1.1`.
//
//   dart run website/check_versions.dart        report, exit 1 if stale
//   dart run website/check_versions.dart --fix  rewrite stale snippets
//
// Run from the repository root. CI runs it in the website job.
import 'dart:io';

void main(List<String> args) {
  final fix = args.contains('--fix');
  final root = Directory.current;
  final versions = <String, String>{};
  for (final dir in Directory('${root.path}/packages').listSync()) {
    final pubspec = File('${dir.path}/pubspec.yaml');
    if (!pubspec.existsSync()) continue;
    final text = pubspec.readAsStringSync();
    final name = RegExp(r'^name:\s*(\S+)', multiLine: true).firstMatch(text);
    final version = RegExp(
      r'^version:\s*(\S+)',
      multiLine: true,
    ).firstMatch(text);
    if (name != null && version != null) {
      versions[name.group(1)!] = version.group(1)!;
    }
  }

  final files = [
    File('${root.path}/README.md'),
    for (final dir in Directory('${root.path}/packages').listSync())
      File('${dir.path}/README.md'),
    for (final entry in Directory('${root.path}/docs').listSync())
      if (entry is File && entry.path.endsWith('.md')) entry,
  ].where((file) => file.existsSync());

  // `  durable_queue_x: ^1.2.3`, with an optional trailing comment.
  final snippet = RegExp(
    r'^(\s+)(durable_queue[a-z_]*):(\s*)\^(\d+\.\d+\.\d+)',
    multiLine: true,
  );
  var stale = 0;
  for (final file in files) {
    final text = file.readAsStringSync();
    final relative = file.path.substring(root.path.length + 1);
    final updated = text.replaceAllMapped(snippet, (match) {
      final name = match.group(2)!;
      final current = versions[name];
      if (current == null || match.group(4) == current) return match.group(0)!;
      stale++;
      final line = '\n'.allMatches(text.substring(0, match.start)).length + 1;
      stdout.writeln(
        '$relative:$line: $name ^${match.group(4)} should be ^$current',
      );
      return '${match.group(1)}$name:${match.group(3)}^$current';
    });
    if (fix && updated != text) file.writeAsStringSync(updated);
  }

  if (stale == 0) {
    stdout.writeln('Install snippets match the package versions.');
  } else if (fix) {
    stdout.writeln('Updated $stale snippet(s).');
  } else {
    stdout.writeln(
      '$stale stale snippet(s). Run: dart run website/check_versions.dart --fix',
    );
    exitCode = 1;
  }
}
