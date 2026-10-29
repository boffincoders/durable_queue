// Builds the documentation site into website/dist/ for GitHub Pages.
//
//   dart run website/build.dart
//
// Plain HTML pages, so every page is fast, works without JavaScript and can
// be indexed by search engines. Pages other than the home page are rendered
// from the Markdown files in this repository (see content.dart), with links
// between them rewritten to point at the site.
import 'dart:convert';
import 'dart:io';

import 'package:markdown/markdown.dart' as md;

import 'content.dart';

final _website = File.fromUri(Platform.script).parent;
final _root = _website.parent;
final _out = Directory('${_website.path}/dist');

final _pages = <({String file, String html})>[];
final _search = <Map<String, String>>[];

/// Rendered Markdown pages, by their site path.
final _rendered = <String, _Rendered>{};

/// Site path of each Markdown source, by repository path.
final _bySource = {for (final page in docPages) page.source: page.url};

void main() {
  for (final page in docPages) {
    _rendered[page.url] = _render(page);
  }
  _home();
  _packagesIndex();
  for (final page in docPages) {
    _docPage(page, _rendered[page.url]!);
  }
  _notFound();
  _write();
  stdout.writeln('website/dist: ${_pages.length} pages');
}

// ------------------------------------------------------------- utilities

String _esc(String s) => s
    .replaceAll('&', '&amp;')
    .replaceAll('<', '&lt;')
    .replaceAll('>', '&gt;')
    .replaceAll('"', '&quot;');

String _unesc(String s) => s
    .replaceAll('&lt;', '<')
    .replaceAll('&gt;', '>')
    .replaceAll('&quot;', '"')
    .replaceAll('&#39;', "'")
    .replaceAll('&amp;', '&');

String _text(String html) =>
    _unesc(html.replaceAll(RegExp(r'<[^>]+>'), '')).trim();

/// Relative prefix from a page at [pageUrl] (`''`, `guide/`,
/// `packages/x/`) to the site root.
String _rootFrom(String pageUrl) {
  final depth = pageUrl.split('/').where((s) => s.isNotEmpty).length;
  return depth == 0 ? './' : '../' * depth;
}

String _version(String packageDir) {
  final pubspec = File('${_root.path}/$packageDir/pubspec.yaml');
  final match = RegExp(
    r'^version:\s*(\S+)',
    multiLine: true,
  ).firstMatch(pubspec.readAsStringSync());
  return match?.group(1) ?? '';
}

// ----------------------------------------------------------- highlighting

const _dartKeywords = {
  'abstract',
  'as',
  'assert',
  'async',
  'await',
  'base',
  'break',
  'case',
  'catch',
  'class',
  'const',
  'continue',
  'default',
  'do',
  'else',
  'enum',
  'extends',
  'extension',
  'factory',
  'false',
  'final',
  'finally',
  'for',
  'get',
  'if',
  'implements',
  'import',
  'in',
  'interface',
  'is',
  'late',
  'library',
  'mixin',
  'new',
  'null',
  'on',
  'override',
  'part',
  'required',
  'rethrow',
  'return',
  'sealed',
  'set',
  'show',
  'static',
  'super',
  'switch',
  'this',
  'throw',
  'true',
  'try',
  'typedef',
  'var',
  'void',
  'when',
  'while',
  'with',
  'yield',
  'dynamic',
  'int',
  'double',
  'num',
  'bool',
  'String',
  'Future',
  'Stream',
  'List',
  'Map',
  'Set',
  'Duration',
  'DateTime',
  'Object',
};

final _dartToken = RegExp(
  r'''(//.*$|/\*[\s\S]*?\*/)|(r?'(?:[^'\\\n]|\\.)*'|r?"(?:[^"\\\n]|\\.)*")|(@?\b[A-Za-z_$][\w$]*\b)|(\b\d[\d_]*(?:\.\d+)?\b)''',
  multiLine: true,
);
final _yamlToken = RegExp(
  r'''(#.*$)|('[^'\n]*'|"[^"\n]*")|(^\s*[\w.-]+(?=:))|(\b\d[\d.]*\b|\^[\d.]+)''',
  multiLine: true,
);
final _shellToken = RegExp(
  r'''(#.*$)|('[^'\n]*'|"[^"\n]*")|(^\s*(?:dart|flutter|git|curl|cd|bash|unzip|ffmpeg)\b)|(--?[\w-]+)''',
  multiLine: true,
);

String _highlight(String code, String lang) {
  final RegExp? re = switch (lang) {
    'dart' => _dartToken,
    'yaml' || 'yml' => _yamlToken,
    'bash' || 'sh' || 'shell' || 'console' => _shellToken,
    _ => null,
  };
  if (re == null) return _esc(code);
  final out = StringBuffer();
  var last = 0;
  for (final m in re.allMatches(code)) {
    out.write(_esc(code.substring(last, m.start)));
    final token = m[0]!;
    String cls;
    if (m[1] != null) {
      cls = 'tok-c';
    } else if (m[2] != null) {
      cls = 'tok-s';
    } else if (m[3] != null) {
      if (lang == 'dart') {
        final next = m.end < code.length ? code[m.end] : '';
        if (_dartKeywords.contains(token) || token.startsWith('@')) {
          cls = 'tok-k';
        } else if (next == '(') {
          cls = 'tok-f';
        } else if (RegExp(r'^[A-Z]').hasMatch(token)) {
          cls = 'tok-t';
        } else {
          cls = '';
        }
      } else {
        cls = 'tok-f';
      }
    } else {
      cls = 'tok-n';
    }
    out.write(
      cls.isEmpty ? _esc(token) : '<span class="$cls">${_esc(token)}</span>',
    );
    last = m.end;
  }
  out.write(_esc(code.substring(last)));
  return out.toString();
}

String _pre(String code, [String lang = 'dart']) =>
    '<pre><code class="lang-$lang">${_highlight(code.trim(), lang)}</code></pre>';

var _tabSeq = 0;

String _codeTabs(
  List<({String kind, String label, String lang, String code})> items,
) {
  final id = 't${++_tabSeq}';
  final tabs = [
    for (final (i, it) in items.indexed)
      '<button type="button" role="tab" id="$id-tab-$i" aria-controls="$id-panel-$i" aria-selected="${i == 0}" data-kind="${it.kind}">${_esc(it.label)}</button>',
  ].join();
  final panels = [
    for (final (i, it) in items.indexed)
      '<div role="tabpanel" id="$id-panel-$i" aria-labelledby="$id-tab-$i"${i == 0 ? '' : ' hidden'}>${_pre(it.code, it.lang)}</div>',
  ].join();
  return '<div class="code-tabs"><div role="tablist" aria-label="Code examples">$tabs</div>$panels</div>';
}

String _table(List<String> head, List<List<String>> rows) =>
    '<div class="table-wrap"><table><thead><tr>${head.map((h) => '<th scope="col">$h</th>').join()}</tr></thead><tbody>${rows.map((r) => '<tr>${r.map((c) => '<td>$c</td>').join()}</tr>').join()}</tbody></table></div>';

// --------------------------------------------------------------- Markdown

final class _Rendered {
  _Rendered(this.title, this.description, this.body, this.headings);

  final String title;
  final String description;
  final String body;
  final List<({int level, String id, String text})> headings;
}

_Rendered _render(DocPage page) {
  final source = File('${_root.path}/${page.source}').readAsStringSync();
  var html = md.markdownToHtml(source, extensionSet: md.ExtensionSet.gitHubWeb);

  // Links and images.
  html = html.replaceAllMapped(
    RegExp(r'(href|src)="([^"]*)"'),
    (m) => '${m[1]}="${_esc(_rewrite(_unesc(m[2]!), m[1]!, page))}"',
  );

  // Code blocks.
  html = html.replaceAllMapped(
    RegExp(
      r'<pre><code(?: class="language-([\w-]+)")?>([\s\S]*?)</code></pre>',
    ),
    (m) {
      final lang = m[1] ?? 'text';
      return '<pre><code class="lang-$lang">${_highlight(_unesc(m[2]!).trimRight(), lang)}</code></pre>';
    },
  );

  // Wide tables scroll inside a box.
  html = html
      .replaceAll('<table>', '<div class="table-wrap"><table>')
      .replaceAll('</table>', '</table></div>');

  final h1 = RegExp(r'<h1[^>]*>([\s\S]*?)</h1>').firstMatch(html);
  final title = page.title ?? (h1 == null ? page.url : _text(h1[1]!));
  final firstParagraph = RegExp(r'<p>([\s\S]*?)</p>').firstMatch(html);
  var description = firstParagraph == null
      ? Site.description
      : _text(firstParagraph[1]!);
  if (description.length > 200) {
    description = '${description.substring(0, 197).trimRight()}…';
  }
  final headings = [
    for (final m in RegExp(
      r'<h([23]) id="([^"]+)">([\s\S]*?)</h\1>',
    ).allMatches(html))
      (level: int.parse(m[1]!), id: m[2]!, text: _text(m[3]!)),
  ];
  return _Rendered(title, description, html, headings);
}

const _blob = '${Site.repo}/blob/main/';
const _tree = '${Site.repo}/tree/main/';
const _raw =
    'https://raw.githubusercontent.com/boffincoders/durable_queue/main/';

/// Points a link in [page]'s Markdown at the site when the target is one of
/// its pages, and at GitHub otherwise.
String _rewrite(String href, String attribute, DocPage page) {
  if (href.isEmpty || href.startsWith('#')) return href;
  String? repoPath;
  var fragment = '';
  final hash = href.indexOf('#');
  var target = hash < 0 ? href : href.substring(0, hash);
  if (hash >= 0) fragment = href.substring(hash);
  if (target.startsWith(_blob)) {
    repoPath = target.substring(_blob.length);
  } else if (target.startsWith(_tree)) {
    repoPath = target.substring(_tree.length);
  } else if (RegExp(r'^[a-z][a-z0-9+.-]*:').hasMatch(target)) {
    return href;
  } else {
    repoPath = _normalize(
      '${page.source.contains('/') ? page.source.substring(0, page.source.lastIndexOf('/')) : ''}/$target',
    );
  }
  repoPath = repoPath.replaceAll(RegExp(r'/+$'), '');
  if (attribute == 'src') return '$_raw$repoPath';

  final from = _rootFrom(page.url);
  if (repoPath.isEmpty || repoPath == 'README.md') return '$from$fragment';
  final direct = _bySource[repoPath] ?? _bySource['$repoPath/README.md'];
  if (direct != null) return '$from$direct$fragment';
  final onDisk = FileSystemEntity.typeSync('${_root.path}/$repoPath');
  final base = onDisk == FileSystemEntityType.directory ? _tree : _blob;
  return '$base$repoPath$fragment';
}

String _normalize(String path) {
  final parts = <String>[];
  for (final part in path.split('/')) {
    if (part.isEmpty || part == '.') continue;
    if (part == '..') {
      if (parts.isNotEmpty) parts.removeLast();
    } else {
      parts.add(part);
    }
  }
  return parts.join('/');
}

// ----------------------------------------------------------------- layout

const _icons = (
  search: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" aria-hidden="true"><circle cx="11" cy="11" r="7"/><path d="m20 20-3.5-3.5"/></svg>',
  // A moon in light mode and a sun in dark mode; style.css shows one.
  theme: '<svg class="icon-moon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" aria-hidden="true"><path d="M21 12.8A9 9 0 1 1 11.2 3a7 7 0 0 0 9.8 9.8z"/></svg><svg class="icon-sun" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" aria-hidden="true"><circle cx="12" cy="12" r="5"/><path d="M12 1v2M12 21v2M4.2 4.2l1.4 1.4M18.4 18.4l1.4 1.4M1 12h2M21 12h2M4.2 19.8l1.4-1.4M18.4 5.6l1.4-1.4"/></svg>',
  menu: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" aria-hidden="true"><path d="M4 6h16M4 12h16M4 18h16"/></svg>',
  github: '<svg viewBox="0 0 16 16" fill="currentColor" aria-hidden="true"><path d="M8 0C3.58 0 0 3.58 0 8c0 3.54 2.29 6.53 5.47 7.59.4.07.55-.17.55-.38 0-.19-.01-.82-.01-1.49-2.01.37-2.53-.49-2.69-.94-.09-.23-.48-.94-.82-1.13-.28-.15-.68-.52-.01-.53.63-.01 1.08.58 1.23.82.72 1.21 1.87.87 2.33.66.07-.52.28-.87.51-1.07-1.78-.2-3.64-.89-3.64-3.95 0-.87.31-1.59.82-2.15-.08-.2-.36-1.02.08-2.12 0 0 .67-.21 2.2.82.64-.18 1.32-.27 2-.27.68 0 1.36.09 2 .27 1.53-1.04 2.2-.82 2.2-.82.44 1.1.16 1.92.08 2.12.51.56.82 1.27.82 2.15 0 3.07-1.87 3.75-3.65 3.95.29.25.54.73.54 1.48 0 1.07-.01 1.93-.01 2.2 0 .21.15.46.55.38A8.013 8.013 0 0 0 16 8c0-4.42-3.58-8-8-8z"/></svg>',
);

/// The gold Boffin Coders logo, the same in light and dark themes.
String _logo(String r, {String cls = 'brand-mark', int size = 30}) =>
    '<img class="$cls" src="${r}brand/bc-gold.png" alt="Boffin Coders" width="$size" height="$size">';

const _org = {
  '@type': 'Organization',
  '@id': '${Org.url}/#organization',
  'name': Org.name,
  'url': Org.url,
  'description': Org.description,
  'logo': '${Site.url}brand/bc-gold.png',
  'sameAs': Org.sameAs,
};

/// JSON-LD for every page: the maintainer, this site and the package, plus
/// the page itself when it is not the home page.
String _structuredData(String file, String fullTitle, String description) {
  final orgRef = {'@id': _org['@id']};
  final graph = [
    _org,
    {
      '@type': 'WebSite',
      '@id': '${Site.url}#website',
      'name': Site.name,
      'url': Site.url,
      'description': Site.description,
      'publisher': orgRef,
    },
    {
      '@type': 'SoftwareSourceCode',
      '@id': '${Site.url}#software',
      'name': Site.name,
      'description': Site.description,
      'codeRepository': Site.repo,
      'url': Site.url,
      'programmingLanguage': 'Dart',
      'runtimePlatform': 'Dart, Flutter',
      'license': 'https://opensource.org/licenses/MIT',
      'version': _version('packages/durable_queue'),
      'author': orgRef,
      'maintainer': orgRef,
    },
    if (file != 'index.html')
      {
        '@type': 'WebPage',
        'name': fullTitle,
        'description': description,
        'url': '${Site.url}${file.replaceAll(RegExp(r'index\.html$'), '')}',
        'isPartOf': {'@id': '${Site.url}#website'},
        'publisher': orgRef,
      },
  ];
  return '<script type="application/ld+json">${jsonEncode({'@context': 'https://schema.org', '@graph': graph}).replaceAll('</', r'<\/')}</script>';
}

String _sidebar(String r, String current) {
  String link(String href, String label, {bool mono = false}) {
    final here = current == href;
    final toc = here && _rendered[href] != null
        ? _rendered[href]!.headings
              .where((h) => h.level == 2)
              .map((h) => '<a href="#${h.id}">${_esc(h.text)}</a>')
              .join()
        : '';
    return '<a href="$r$href"${here ? ' aria-current="page"' : ''}${mono ? ' class="mono"' : ''}>${_esc(label)}</a>'
        '${toc.isEmpty ? '' : '<div class="toc">$toc</div>'}';
  }

  String group(String section) => docPages
      .where((p) => p.section == section && p.section != 'Packages')
      .map((p) => link(p.url, p.title ?? p.url))
      .join();

  return '''<nav class="sidebar" aria-label="Documentation">
<h4>Start here</h4>${link('', 'Overview')}${group('Start here')}
<h4>Packages</h4>${link('packages/', 'All packages')}${packages.map((p) => link(p.url, p.name, mono: true)).join()}
<h4>Trust</h4>${group('Trust')}
<h4>Reference</h4>${group('Reference')}<a href="https://pub.dev/documentation/durable_queue/latest/">API reference</a><a href="${Site.repo}/blob/main/packages/durable_queue/CHANGELOG.md">Changelog</a>
</nav>''';
}

void _page({
  required String file,
  required String title,
  required String description,
  required String body,
  String current = '',
  bool docs = true,
  String extraHead = '',
  bool noindex = false,
  List<({int level, String id, String text})> headings = const [],
}) {
  final url = file.replaceAll(RegExp(r'index\.html$'), '');
  // The 404 page is served at any depth, so it links from its <base>.
  final r = file == '404.html' ? './' : _rootFrom(url);
  final canonical = '${Site.url}$url';
  final fullTitle = file == 'index.html'
      ? '${Site.name}: ${Site.tagline}'
      : '$title · ${Site.name}';
  final nav = topNav
      .map(
        (e) =>
            '<a href="$r${e.$1}"${current.isNotEmpty && (current == e.$1 || (e.$2 == 'Packages' && current.startsWith('packages/'))) ? ' aria-current="page"' : ''}>${e.$2}</a>',
      )
      .join();
  final html =
      '''<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>${_esc(fullTitle)}</title>
<meta name="description" content="${_esc(description)}">
<link rel="canonical" href="$canonical">
<meta property="og:type" content="website">
<meta property="og:title" content="${_esc(fullTitle)}">
<meta property="og:description" content="${_esc(description)}">
<meta property="og:url" content="$canonical">
<meta property="og:site_name" content="${Site.name} by Boffin Coders">
<meta property="og:image" content="${Site.url}og.png">
<meta property="og:image:width" content="1200">
<meta property="og:image:height" content="630">
<meta property="og:image:alt" content="${Site.name}: ${_esc(Site.tagline)}, by Boffin Coders">
<meta name="twitter:card" content="summary_large_image">
<meta name="twitter:image" content="${Site.url}og.png">
<meta name="author" content="Boffin Coders">
${noindex ? '<meta name="robots" content="noindex">' : _structuredData(file, fullTitle, description)}
<meta name="theme-color" content="#0d0d0d" media="(prefers-color-scheme: dark)">
<meta name="theme-color" content="#ffffff" media="(prefers-color-scheme: light)">
<meta name="dq-root" content="$r">
<link rel="icon" href="${r}favicon.png" type="image/png">
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link rel="stylesheet" href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&family=JetBrains+Mono:wght@400;500&family=Space+Grotesk:wght@500;600;700&display=swap">
<link rel="stylesheet" href="${r}style.css">
<script>try{var t=localStorage.getItem("dq-theme");if(t)document.documentElement.dataset.theme=t}catch(e){}</script>
$extraHead
</head>
<body>
<a class="skip" href="#main">Skip to content</a>
<header class="topbar">
<button class="icon-btn menu-btn" type="button" data-menu aria-label="Menu" aria-expanded="false">${_icons.menu}</button>
<a class="brand" href="$r" aria-label="${Site.name} home">${_logo(r)}<span class="brand-text"><span class="brand-name">${Site.name}</span><span class="brand-by">by Boffin Coders</span></span></a>
<nav class="topnav" aria-label="Main">$nav</nav>
<div class="top-actions">
<button class="search-btn" type="button" data-search aria-label="Search">${_icons.search}<span class="label">Search</span><kbd>⌘K</kbd></button>
<button class="icon-btn" type="button" data-theme-toggle aria-label="Toggle dark mode">${_icons.theme}</button>
<a class="icon-btn" href="${Site.repo}" aria-label="GitHub repository">${_icons.github}</a>
</div>
</header>
${docs ? '<div class="layout">${_sidebar(r, current)}<main id="main" class="content">$body</main></div>' : '<main id="main">$body</main>'}
<footer>
<a class="footer-brand" href="${Org.link('/', 'footer')}">${_logo(r, cls: 'footer-logo', size: 36)}<span>Built and maintained by Boffin Coders</span></a>
<p>${Site.name} ${_esc(_version('packages/durable_queue'))} · MIT licence · <a href="${Site.repo}">GitHub</a> · <a href="${Site.pub}">pub.dev</a></p>
<p>More open source by Boffin Coders: ${sisterSites.map((s) => '<a href="${s.url}">${s.name}</a>').join(' · ')} · <a href="$hubUrl">All Boffin Coders open source</a></p>
<p>Found a problem with these pages? <a href="${Site.repo}/issues">Open an issue</a>.</p>
</footer>
<dialog class="search" aria-label="Search the documentation"><input type="search" placeholder="Search pages and sections…" aria-label="Search"><ul></ul></dialog>
<script src="${r}search-index.js"></script>
<script src="${r}site.js"></script>
</body>
</html>
''';
  _pages.add((file: file, html: html));
  if (file != '404.html') {
    _search.add({
      'title': title.isEmpty ? Site.name : title,
      'text': description,
      'url': url,
    });
    for (final h in headings.where((h) => h.level == 2)) {
      _search.add({
        'title': '$title › ${h.text}',
        'text': title,
        'url': '$url#${h.id}',
      });
    }
  }
}

// ------------------------------------------------------------------- home

void _home() {
  final core = _version('packages/durable_queue');
  final quickStart = _codeTabs([
    (
      kind: 'dart',
      label: 'Dart',
      lang: 'dart',
      code: r'''
final class UploadPhotoTask extends DurableTask {
  UploadPhotoTask({required this.path});

  final String path;

  @override
  String get type => 'upload_photo';

  @override
  Map<String, dynamic> toJson() => {'path': path};

  static UploadPhotoTask fromJson(Map<String, dynamic> json) =>
      UploadPhotoTask(path: json['path'] as String);
}

final queue = DurableQueue(storage: SqliteQueueStorage.open(path));

queue.register<UploadPhotoTask>(
  type: 'upload_photo',
  decoder: UploadPhotoTask.fromJson,
  handler: (task, context) => photoApi.upload(task.path),
);

await queue.start(); // recovers tasks interrupted by the last shutdown

// Stored before enqueue returns, then retried until it succeeds or
// its attempts run out.
await queue.enqueue(
  UploadPhotoTask(path: '/storage/avatar.jpg'),
  retryPolicy: RetryPolicy.exponential(
    maxAttempts: 5,
    initialDelay: Duration(seconds: 1),
  ),
);''',
    ),
    (
      kind: 'flutter',
      label: 'Flutter',
      lang: 'dart',
      code: r'''
Future<void> main() async {
  WidgetsFlutterBinding.ensureInitialized();
  final queue = DurableQueue(storage: await openStorage());
  queue.register<UploadTask>(
    type: 'upload',
    decoder: UploadTask.fromJson,
    handler: upload,
  );

  // Starts the queue with the app and pauses it in the background.
  runApp(DurableQueueScope(queue: queue, child: const MyApp()));
}

// Anywhere below the scope:
final queue = DurableQueueScope.of(context);
await queue.enqueue(UploadTask(path));''',
    ),
    (
      kind: 'install',
      label: 'pubspec.yaml',
      lang: 'yaml',
      code:
          '''
dependencies:
  durable_queue: ^$core
  durable_queue_sqlite: ^${_version('packages/durable_queue_sqlite')}   # or durable_queue_hive
  durable_queue_flutter: ^${_version('packages/durable_queue_flutter')}  # Flutter apps''',
    ),
  ]);

  final packageCards = packages
      .map(
        (p) =>
            '<a class="card" href="./${p.url}"><h3 class="mono">${p.name}<span class="ver">${_esc(_version(p.dir))}</span></h3><p>${_esc(p.summary)}</p></a>',
      )
      .join();

  final diagram = RegExp(r'```text\n([\s\S]*?)```')
      .firstMatch(File('${_root.path}/README.md').readAsStringSync())![1]!;

  _page(
    file: 'index.html',
    title: '',
    description: Site.description,
    docs: false,
    body:
        '''<section class="hero">
<p class="eyebrow">Open source · MIT · Dart &amp; Flutter</p>
<h1>Work that survives the user <span class="accent">closing the app</span>.</h1>
<p class="lead">durable_queue stores every task before <code>enqueue</code> returns, retries it with backoff across crashes and restarts, waits while the device is offline, and can hand uploads to the operating system so they finish in the background.</p>
<div class="hero-actions"><a class="btn primary" href="./guide/">Get started</a><a class="btn" href="${Site.explainer}">Watch the 10-minute explainer</a><a class="btn" href="./guarantees/">What we guarantee</a></div>
<div class="install"><span>dart pub add durable_queue</span><button type="button" data-copy="dart pub add durable_queue">Copy</button></div>
<div class="badges"><span>Pure Dart core, no runtime dependencies</span><span>Android · iOS · web · desktop</span><span>Killed mid-upload in CI</span><span>MIT</span></div>
</section>

<section class="home-section">
<div class="demo">
<img src="${Site.demoGif}" alt="Demo on an Android phone: photos are queued, the app goes to the home screen, the uploads still reach the server, and the server is told about each one when the app is opened again" width="1000" height="588" loading="lazy">
<div>
<h2 style="text-align:left;margin-top:0">Close the app. The uploads still arrive.</h2>
<p>The demo app queues photos, goes to the home screen, and the server still receives every upload, including ones whose first try the server failed on purpose. When the app opens again, the server is told about each photo exactly once.</p>
<ul>
<li><a href="${Site.androidVideo}">Full video on an Android phone</a> (103 s)</li>
<li><a href="${Site.iphoneVideo}">Full video on an iPhone</a> (78 s)</li>
<li><a href="./demo/">Run the demo app yourself</a></li>
</ul>
</div>
</div>
</section>

<section class="home-section">
<h2>Quick start</h2>
<p class="lead">Describe work as data, register a handler, and enqueue. The queue owns retries, ordering and recovery.</p>
$quickStart
<p style="text-align:center"><a href="./guide/">Read the full guide →</a></p>
</section>

<section class="home-section">
<h2>The problem</h2>
<p class="lead">A <code>try</code>/<code>catch</code> handles one failure. Then you still have to decide:</p>
<div class="grid">
<div class="card"><h3>Where does the work live?</h3><p>Until it can be retried, and whether it survives a restart or a crash.</p></div>
<div class="card"><h3>When do you try again?</h3><p>How long to wait between attempts, and when to give up.</p></div>
<div class="card"><h3>What about double taps?</h3><p>How to stop the same photo from uploading twice.</p></div>
<div class="card"><h3>Which goes first?</h3><p>How many uploads may run at once, and in what order.</p></div>
<div class="card"><h3>What if step 2 fails?</h3><p>What happens to step 3 when the step it depends on does not succeed.</p></div>
<div class="card"><h3>What if the app closes?</h3><p>Whether an upload in progress can finish without the app.</p></div>
</div>
</section>

<section class="home-section">
<h2>Proof, not promises</h2>
<p class="lead">Every claim is tested, and the results are public.</p>
${_table(['Where', 'What the test does', 'Result'], [
          ['Android 14 emulator, in CI', 'Kills the app (<code>kill -9</code>) with 6 uploads in flight, relaunches it offline for 15 seconds, then online', 'Nothing sent while offline. All 6 uploads completed; the step after each ran exactly once.'],
          ['iOS 26.5 simulator, in CI', 'Terminates the app with 6 uploads in flight, then relaunches it', 'The uploads kept going while the app was terminated. All 6 completed; the step after each ran exactly once.'],
          ['Real iPhone and Android phone', 'Photos queued, app sent to the home screen', 'Every upload reached the server (<a href="${Site.androidVideo}">Android</a>, <a href="${Site.iphoneVideo}">iPhone</a>).'],
          ['GitHub runners, Ubuntu and macOS', 'Benchmarks for storing, running and recovering tasks', 'About 0.15 to 0.3 ms to store a task durably; 1,000 interrupted tasks recovered in 0.25 to 0.6 s. <a href="./benchmarks/">All numbers and the method →</a>'],
        ])}
<div class="callout"><p><strong>What we do not promise:</strong> exactly-once delivery. If the app is killed after a handler's side effect but before its result is stored, the task runs again. Send the task's idempotency key so your server can drop the repeat. <a href="./guarantees/">Read the guarantees →</a></p></div>
</section>

<section class="home-section">
<h2>Nine packages, one core</h2>
<p class="lead">Each integration is a separate package, so the core never forces a database, an HTTP client or Flutter on you.</p>
<div class="grid">$packageCards</div>
</section>

<section class="home-section diagram">
<h2>How the packages fit together</h2>
<p class="lead">The core decides <em>when</em> each task runs. A gate decides whether a task may start now, storage decides where tasks live, and a handler does the work.</p>
<div class="arch">${File('${_website.path}/architecture.svg').readAsStringSync().replaceAll('{{root}}', './')}</div>
<details class="text-version"><summary>Text version of the diagram</summary>${_pre(diagram, 'text')}</details>
</section>

<section class="home-section">
<h2>Everything you need</h2>
<div class="grid">
<div class="card"><h3>Retries that make sense</h3><p>Fixed or exponential backoff with jitter, a <code>retryIf</code> filter, and <code>Retry-After</code> with Dio.</p></div>
<div class="card"><h3>Dependencies and chains</h3><p>"Upload, then tell the server": a task starts only after the ones it depends on completed.</p></div>
<div class="card"><h3>Waits for the network</h3><p>Tasks wait while offline without using attempts, and start when the connection returns.</p></div>
<div class="card"><h3>Deduplication</h3><p>A deduplication key stops a double tap from queuing the same work twice.</p></div>
<div class="card"><h3>Background transfers</h3><p>Uploads and downloads handed to the operating system keep going after the app is closed.</p></div>
<div class="card"><h3>Encryption at rest</h3><p>Optional AES-256-GCM for stored payloads and error messages, with key rotation.</p></div>
<div class="card"><h3>Priorities and rate limits</h3><p>Higher priority first, limits per task type or per key such as an API host.</p></div>
<div class="card"><h3>Flutter widgets</h3><p>Rebuild your UI from stored task status; start and pause with the app's lifecycle.</p></div>
<div class="card"><h3>Testable</h3><p>An injectable clock tests retry logic without waiting.</p></div>
</div>
<p style="text-align:center;margin-top:28px"><a class="btn primary" href="./guide/">Get started</a> <a class="btn" href="./cookbook/">Browse the cookbook</a></p>
</section>

<section class="home-section">
<div class="built-by">
<img src="./brand/bc-gold.png" alt="Boffin Coders logo" width="56" height="56">
<div>
<h2>Built by Boffin Coders</h2>
<p>${_esc(Org.description)} We build and maintain durable_queue as open source. Need offline-first sync, reliable uploads or a whole Flutter app built for you?</p>
<p class="built-by-actions"><a class="btn primary" href="${Org.link('/hire/flutter-developers', 'home')}">Hire Flutter developers</a><a class="btn" href="${Org.link('/contact-us', 'home')}">Talk to us</a><a class="btn" href="${Org.link('/', 'home')}">boffincoders.com</a></p>
</div>
</div>
</section>''',
  );
}

// --------------------------------------------------------- packages index

void _packagesIndex() {
  _page(
    file: 'packages/index.html',
    current: 'packages/',
    title: 'Packages',
    description: 'The durable_queue packages: the pure Dart core, storage (SQLite, Hive), encryption, Dio, Flutter, connectivity, background transfers and Workmanager.',
    body:
        '''<p class="eyebrow">Packages</p>
<h1>All packages</h1>
<p class="lead">Start with the core and add only what you need. Each package's page is its README.</p>
${_table(['Package', 'Version', 'Needs', 'What it adds'], [
          for (final p in packages) ['<a href="../${p.url}"><code>${p.name}</code></a>', '<code>${_esc(_version(p.dir))}</code>', p.flutter ? 'Flutter' : 'Dart', _esc(p.summary)],
        ])}''',
  );
}

// -------------------------------------------------------------- doc pages

void _docPage(DocPage page, _Rendered rendered) {
  var body = rendered.body;
  final package = packages.where((p) => p.url == page.url).firstOrNull;
  if (package != null) {
    final meta =
        '<div class="pkg-meta"><span class="version">${_esc(_version(package.dir))}</span>'
        '<a href="https://pub.dev/packages/${package.name}">pub.dev</a>'
        '<a href="https://pub.dev/documentation/${package.name}/latest/">API reference</a>'
        '<a href="$_blob${package.dir}/CHANGELOG.md">Changelog</a>'
        '<a href="$_tree${package.dir}">Source</a></div>';
    body = body.replaceFirst('</h1>', '</h1>$meta');
  }
  _page(
    file: '${page.url}index.html',
    current: page.url,
    title: rendered.title,
    description: rendered.description,
    headings: rendered.headings,
    body:
        '<p class="eyebrow">${_esc(page.section)}</p>$body'
        '<p class="edit">This page is <a href="$_blob${page.source}"><code>${_esc(page.source)}</code></a> in the repository. <a href="${Site.repo}/edit/main/${page.source}">Suggest a change</a>.</p>',
  );
}

void _notFound() {
  _page(
    file: '404.html',
    title: 'Page not found',
    description: 'Page not found.',
    docs: false,
    noindex: true,
    // Served for any missing path, so links resolve from the site root.
    extraHead: '<base href="${Site.basePath}">',
    body: '<section class="hero"><h1>Page not found</h1><p class="lead">That page does not exist. Try the <a href="./">home page</a> or press <kbd>/</kbd> to search.</p></section>',
  );
}

// ------------------------------------------------------------------ write

void _write() {
  if (_out.existsSync()) _out.deleteSync(recursive: true);
  for (final page in _pages) {
    File('${_out.path}/${page.file}')
      ..createSync(recursive: true)
      ..writeAsStringSync(page.html);
  }
  final static = Directory('${_website.path}/static');
  for (final file in static.listSync(recursive: true).whereType<File>()) {
    final target = File(
      '${_out.path}${file.path.substring(static.path.length)}',
    );
    target.parent.createSync(recursive: true);
    file.copySync(target.path);
  }
  File('${_out.path}/search-index.js').writeAsStringSync(
    'window.DQ_SEARCH=${jsonEncode([
      ..._search,
      {'title': 'API reference', 'text': 'Every class and method of durable_queue, on pub.dev.', 'url': 'https://pub.dev/documentation/durable_queue/latest/'},
    ])};\n',
  );
  File('${_out.path}/.nojekyll').writeAsStringSync('');
  File('${_out.path}/llms.txt').writeAsStringSync('''
# ${Site.name}

> ${Site.description} Open source under the MIT licence.

Maintained by Boffin Coders (${Org.url}). ${Org.description}

## Docs

- [Overview](${Site.url}): what the queue does, quick start, how the packages fit together
- [Guide](${Site.url}guide/): tasks, handlers, retries and backoff, priorities, dependencies, rate limits, events, recovery
- [Cookbook](${Site.url}cookbook/): offline forms, ordered chat messages, upload then notify, rate-limited sync, background jobs, progress in the UI, testing
- [Guarantees](${Site.url}guarantees/): what is promised, what is not, and how it is tested
- [Benchmarks](${Site.url}benchmarks/): cost of storing, running and recovering tasks with each storage
- [Demo app](${Site.url}demo/): a photo outbox app and the device kill tests
- [Storage contract](${Site.url}storage-contract/): for authors of storage adapters
- [API reference](https://pub.dev/documentation/durable_queue/latest/)

## Packages

${packages.map((p) => '- [${p.name}](${Site.url}${p.url}): ${p.summary} (https://pub.dev/packages/${p.name})').join('\n')}

## Links

- Source: ${Site.repo}
- pub.dev: ${Site.pub}
- Maintainer: ${Org.url}

## More open source by Boffin Coders

${sisterSites.map((s) => '- [${s.name}](${s.url}): ${s.summary}').join('\n')}
- [All Boffin Coders open source]($hubUrl)
- https://github.com/boffincoders
''');
  File('${_out.path}/robots.txt').writeAsStringSync(
    'User-agent: *\nAllow: /\nSitemap: ${Site.url}sitemap.xml\n',
  );
  final urls = _pages
      .where((p) => p.file != '404.html')
      .map(
        (p) =>
            '  <url><loc>${Site.url}${p.file.replaceAll(RegExp(r'index\.html$'), '')}</loc></url>',
      )
      .join('\n');
  File('${_out.path}/sitemap.xml').writeAsStringSync(
    '<?xml version="1.0" encoding="UTF-8"?>\n<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">\n$urls\n</urlset>\n',
  );
}
