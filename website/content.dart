// What the documentation site contains. build.dart turns it into pages.
//
// Most pages are rendered from the Markdown files in this repository, so the
// site always says what the READMEs and docs say. Only the home page is
// written here.

/// Site-wide settings.
abstract final class Site {
  static const name = 'durable_queue';
  static const tagline = 'Persistent, retryable tasks for Dart and Flutter';
  static const description =
      'Persistent, retryable task execution for Dart and Flutter: tasks are '
      'stored before enqueue returns, retried with backoff across restarts '
      'and crashes, wait for the network, and can hand uploads to the '
      'operating system so they finish after the app is closed.';
  static const url = 'https://boffincoders.github.io/durable_queue/';
  static const basePath = '/durable_queue/';
  static const repo = 'https://github.com/boffincoders/durable_queue';
  static const pub = 'https://pub.dev/packages/durable_queue';
  static const maintainer = 'https://boffincoders.com';
  static const explainer = 'https://youtu.be/6L7gFN60PmY';
  static const demoGif =
      'https://raw.githubusercontent.com/boffincoders/durable_queue/main/docs/media/durable_queue_demo_android.gif';
  static const iphoneVideo =
      'https://github.com/boffincoders/durable_queue/blob/main/docs/media/durable_queue_demo.mp4';
  static const androidVideo =
      'https://github.com/boffincoders/durable_queue/blob/main/docs/media/durable_queue_demo_android.mp4';
}

/// A published package, shown on the home page and in the sidebar.
final class Package {
  const Package(this.name, this.summary, {this.flutter = false});

  final String name;
  final String summary;

  /// Needs the Flutter SDK.
  final bool flutter;

  String get dir => 'packages/$name';
  String get url => 'packages/$name/';
}

const packages = [
  Package(
    'durable_queue',
    'The engine: tasks, retries, priorities, dependencies, rate limits, '
        'gates. Pure Dart, no runtime dependencies.',
  ),
  Package(
    'durable_queue_sqlite',
    'SQLite storage: tasks survive restarts and crashes, on mobile, desktop, '
        'and the web.',
  ),
  Package(
    'durable_queue_hive',
    'Hive storage on hive_ce, for apps that already use Hive.',
  ),
  Package(
    'durable_queue_encryption',
    'Encrypts stored payloads and error messages with AES-256-GCM. Wraps any '
        'storage.',
  ),
  Package(
    'durable_queue_dio',
    'Dio integration: HTTP retry rules, Retry-After, idempotency keys, '
        'queued requests.',
  ),
  Package(
    'durable_queue_flutter',
    'Starts the queue with the app, pauses it in the background, and '
        'rebuilds widgets from stored tasks.',
    flutter: true,
  ),
  Package(
    'durable_queue_connectivity',
    'A network gate: tasks wait while offline and start when the '
        'connection returns.',
    flutter: true,
  ),
  Package(
    'durable_queue_transfer',
    'Uploads and downloads the operating system finishes after the app is '
        'minimised or closed.',
    flutter: true,
  ),
  Package(
    'durable_queue_workmanager',
    'Runs the queue from Workmanager background jobs on Android and iOS.',
    flutter: true,
  ),
];

/// A page rendered from a Markdown file in the repository.
final class DocPage {
  const DocPage(this.source, this.url, {required this.section, this.title});

  /// Path of the Markdown file, relative to the repository root.
  final String source;

  /// Path of the page on the site, ending in `/`.
  final String url;

  /// The sidebar group, also shown above the title.
  final String section;

  /// Overrides the file's first heading in the sidebar.
  final String? title;
}

final docPages = [
  DocPage(
    'packages/durable_queue/README.md',
    'guide/',
    section: 'Start here',
    title: 'Guide',
  ),
  DocPage(
    'examples/durable_queue_demo/README.md',
    'demo/',
    section: 'Start here',
    title: 'Demo app',
  ),
  DocPage(
    'docs/cookbook.md',
    'cookbook/',
    section: 'Start here',
    title: 'Cookbook',
  ),
  DocPage(
    'docs/guarantees.md',
    'guarantees/',
    section: 'Trust',
    title: 'Guarantees',
  ),
  DocPage(
    'docs/benchmarks.md',
    'benchmarks/',
    section: 'Trust',
    title: 'Benchmarks',
  ),
  DocPage(
    'SECURITY.md',
    'security/',
    section: 'Trust',
    title: 'Security policy',
  ),
  DocPage(
    'packages/durable_queue/STORAGE.md',
    'storage-contract/',
    section: 'Reference',
    title: 'Storage contract',
  ),
  DocPage(
    'CONTRIBUTING.md',
    'contributing/',
    section: 'Reference',
    title: 'Contributing',
  ),
  for (final package in packages)
    DocPage(
      '${package.dir}/README.md',
      package.url,
      section: 'Packages',
      title: package.name,
    ),
];

/// Top bar links.
const topNav = [
  ('guide/', 'Guide'),
  ('packages/', 'Packages'),
  ('cookbook/', 'Cookbook'),
  ('guarantees/', 'Guarantees'),
  ('benchmarks/', 'Benchmarks'),
];
