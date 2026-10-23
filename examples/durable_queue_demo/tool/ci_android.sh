#!/usr/bin/env bash
# Builds the demo, installs it on the running emulator, and runs the kill
# test. Run from examples/durable_queue_demo.
set -euxo pipefail
bash tool/prepare_platforms.sh
flutter build apk --debug \
  --dart-define=DEMO_SERVER=http://10.0.2.2:8080 \
  --dart-define=DEMO_AUTOPILOT=true
adb install -r build/app/outputs/flutter-apk/app-debug.apk
# Ask for the notification permission up front so nothing waits on a dialog.
adb shell pm grant dev.boffincoders.durable_queue_demo android.permission.POST_NOTIFICATIONS || true
dart tool/kill_test.dart --platform android --app-id dev.boffincoders.durable_queue_demo
