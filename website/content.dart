// What the documentation site contains. build.dart turns it into pages.
//
// Most pages are rendered from the Markdown files in this repository, so the
// site always says what the READMEs and docs say. Only the home page is
// written here.

/// Site-wide settings.
abstract final class Site {
  static const name = 'durable_queue';
  static const tagline = 'Persistent, retryable tasks for Dart and Flutter';

  /// The home page's `<title>`.
  static const homeTitle =
      'durable_queue: a persistent Flutter and Dart task queue with retries';
  static const description =
      'A persistent task queue for Flutter and Dart: with SQLite or Hive '
      'storage, tasks are stored before enqueue returns and retried with '
      'backoff across restarts and crashes; they wait for the network, and '
      'can hand uploads to the operating system so they finish after the app '
      'is closed.';
  static const url = 'https://boffincoders.github.io/durable_queue/';
  static const basePath = '/durable_queue/';
  static const repo = 'https://github.com/boffincoders/durable_queue';
  static const pub = 'https://pub.dev/packages/durable_queue';
  static const explainer = 'https://youtu.be/6L7gFN60PmY';
  static const demoGif =
      'https://raw.githubusercontent.com/boffincoders/durable_queue/main/docs/media/durable_queue_demo_android.gif';
  static const iphoneVideo =
      'https://github.com/boffincoders/durable_queue/blob/main/docs/media/durable_queue_demo.mp4';
  static const androidVideo =
      'https://github.com/boffincoders/durable_queue/blob/main/docs/media/durable_queue_demo_android.mp4';
}

/// The maintainer, as boffincoders.com describes itself (structured data,
/// the home page and the footer).
abstract final class Org {
  static const name = 'Boffin Coders';
  static const url = 'https://boffincoders.com';
  static const description =
      'We design and build websites, web applications and mobile apps, and '
      'run their SEO, for businesses and agencies.';
  static const sameAs = [
    'https://www.linkedin.com/company/boffincoders/',
    'https://x.com/boffincoders',
    'https://www.instagram.com/boffincoders/',
    'https://www.facebook.com/boffincoders',
    'https://github.com/boffincoders',
  ];

  /// A link to boffincoders.com, tagged so visits from these docs show up in
  /// analytics.
  static String link(String path, String medium) =>
      '$url$path${path.contains('?') ? '&' : '?'}utm_source=docs&utm_medium=$medium&utm_campaign=durable_queue';
}

/// The Boffin Coders open-source hub, which lists every project.
const hubUrl = 'https://boffincoders.github.io/';

/// Other Boffin Coders open-source docs, linked from the footer and llms.txt.
const sisterSites = [
  (
    name: 'fundamental-analysis',
    url: 'https://boffincoders.github.io/fundamental-analysis/',
    summary:
        'Financial ratios and fundamental analysis for JavaScript and '
        'TypeScript',
  ),
  (
    name: 'ts-netstorageapi-akamai',
    url: 'https://boffincoders.github.io/akamai-netstorageapi-typescript/',
    summary: 'TypeScript client and CLI for the Akamai NetStorage HTTP API',
  ),
];

/// A published package, shown on the home page and in the sidebar.
final class Package {
  const Package(
    this.name,
    this.summary, {
    this.flutter = false,
    this.metaTitle,
    this.metaDescription,
  });

  final String name;
  final String summary;

  /// Overrides the page's `<title>`; the README's heading stays the same.
  final String? metaTitle;

  /// Overrides the page's meta description (by default the README's first
  /// paragraph).
  final String? metaDescription;

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
    metaTitle:
        'durable_queue_connectivity: Flutter offline sync with a network gate',
    metaDescription:
        'A network gate for durable_queue, for Flutter offline sync: tasks '
        'that need the network wait while the device is offline, without '
        'using attempts, and start as soon as it is back online.',
  ),
  Package(
    'durable_queue_transfer',
    'Uploads and downloads the operating system finishes after the app is '
        'minimised or closed.',
    flutter: true,
    metaTitle:
        'durable_queue_transfer: Flutter background upload and download that '
        'survive closing the app',
    metaDescription:
        'Flutter background uploads and downloads for durable_queue that keep '
        'going after the user minimises or closes the app.',
  ),
  Package(
    'durable_queue_workmanager',
    'Runs the queue from Workmanager background jobs on Android and iOS.',
    flutter: true,
    metaTitle:
        'durable_queue_workmanager: Flutter Workmanager background tasks with '
        'a persistent queue',
    metaDescription:
        'Run durable_queue tasks in Flutter Workmanager background jobs on '
        'Android and iOS, so queued background tasks keep going after the user '
        'leaves the app.',
  ),
];

/// A page rendered from a Markdown file in the repository.
final class DocPage {
  const DocPage(
    this.source,
    this.url, {
    required this.section,
    this.title,
    this.metaTitle,
    this.metaDescription,
  });

  /// Path of the Markdown file, relative to the repository root.
  final String source;

  /// Path of the page on the site, ending in `/`.
  final String url;

  /// The sidebar group, also shown above the title.
  final String section;

  /// Overrides the file's first heading in the sidebar.
  final String? title;

  /// Overrides the page's `<title>`.
  final String? metaTitle;

  /// Overrides the page's meta description.
  final String? metaDescription;
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
      metaTitle: package.metaTitle,
      metaDescription: package.metaDescription,
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

/// Questions people ask about durable_queue, answered from the docs. Shown on
/// the home page (with FAQPage structured data) and in llms.txt. Answers are
/// HTML with `<code>` only.
const faq = [
  (
    q: 'How do I make uploads survive the app being closed in Flutter?',
    a:
        'Hand them to the operating system with <code>durable_queue_transfer</code>: '
        'in the task\'s handler, call <code>transfers.start(context, UploadTask(...))</code> '
        'on a <code>QueueTransfers</code>. The upload continues while the app is in the '
        'background or closed, and its queue task completes, with the tasks after it, the '
        'next time the app runs. If the user force-quits the app, the system may stop the '
        'upload until the app is opened again; it is restarted or retried then.',
  ),
  (
    q: 'How do I retry failed uploads with exponential backoff in Flutter?',
    a:
        'Enqueue the task with <code>retryPolicy: RetryPolicy.exponential(maxAttempts: 5, '
        'initialDelay: Duration(seconds: 1))</code>. With SQLite or Hive storage the task '
        'is stored before <code>enqueue</code> returns, so its retries continue across '
        'restarts and crashes; a <code>retryIf</code> filter on <code>register</code> '
        'decides which errors are worth retrying.',
  ),
  (
    q: 'How do I queue requests while offline in Flutter?',
    a:
        'Add a network gate from <code>durable_queue_connectivity</code> with '
        '<code>queue.addGate(\'network\', await ConnectivityGate.create())</code>, then '
        'register the task type with <code>requires: {\'network\'}</code> and '
        '<code>deferOn: deferOnConnectionError(\'network\')</code>. Tasks wait while the '
        'device is offline without using attempts, and start as soon as a connection comes '
        'back.',
  ),
  (
    q: 'How do I run background tasks with Workmanager and a queue?',
    a:
        'Use <code>durable_queue_workmanager</code>: create a <code>QueueBackgroundWork</code> '
        'with <code>openQueue</code> and <code>openLease</code>, call '
        '<code>background.schedule()</code> in <code>main</code> and '
        '<code>background.run(taskName)</code> in Workmanager\'s callback, and run the app\'s '
        'queue through a <code>LeasedQueueRunner</code>. The lease keeps the app and the job '
        'from running the queue at once. Android runs the jobs at most every 15 minutes, for '
        'up to about 10 minutes; iOS decides when, and gives each about 30 seconds.',
  ),
  (
    q: 'Does durable_queue run each task exactly once?',
    a:
        'No. Every task runs at least once, until it succeeds or its attempts run out. If the '
        'app dies after a handler\'s side effect but before the result is written, the task '
        'runs again, so send the task\'s <code>context.idempotencyKey</code> to your server '
        'and let it drop the repeat.',
  ),
  (
    q: 'How do I encrypt queued data?',
    a:
        'Wrap the storage with <code>durable_queue_encryption</code>: '
        '<code>EncryptedQueueStorage(SqliteQueueStorage.open(path), cipher: '
        'AesGcmCipher(key))</code>, with a 32-byte key from '
        '<code>AesGcmCipher.generateKey()</code> kept in the platform\'s secure storage. '
        'Payloads, error messages, deduplication keys and groups are encrypted with '
        'AES-256-GCM.',
  ),
  (
    q: 'Does durable_queue need Flutter?',
    a:
        'No. The core package, <code>durable_queue</code>, is pure Dart with no runtime '
        'dependencies, and runs on Android, iOS, the web and desktop. The Flutter packages '
        'add app lifecycle and widgets, connectivity, background transfers and Workmanager.',
  ),
];
