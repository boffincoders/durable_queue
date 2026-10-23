/// Where the demo server runs. The default reaches the computer running an
/// Android emulator. On the iOS simulator use `http://localhost:8080`; on a
/// phone, the computer's address on your network.
///
///   flutter run --dart-define=DEMO_SERVER=http://192.168.1.20:8080
const demoServer = String.fromEnvironment(
  'DEMO_SERVER',
  defaultValue: 'http://10.0.2.2:8080',
);

/// When true, the app takes instructions from the demo server and reports
/// its queue to it. Used by `tool/kill_test.dart`.
const demoAutopilot = bool.fromEnvironment('DEMO_AUTOPILOT');

/// The group of every photo upload and its follow-up request.
const photosGroup = 'photos';
