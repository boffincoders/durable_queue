#!/usr/bin/env bash
# Builds the demo for the iOS simulator, installs it on a booted iPhone
# simulator, and runs the kill test. Run from examples/durable_queue_demo.
set -euxo pipefail
bash tool/prepare_platforms.sh

udid=$(xcrun simctl list devices available -j | python3 -c '
import json, sys
devices = json.load(sys.stdin)["devices"]
runtimes = sorted((r for r in devices if "iOS" in r), reverse=True)
for runtime in runtimes:
    for device in devices[runtime]:
        if device["name"].startswith("iPhone"):
            print(device["udid"])
            sys.exit(0)
sys.exit("No iPhone simulator found")
')
xcrun simctl boot "$udid" || true
xcrun simctl bootstatus "$udid" -b

flutter build ios --simulator --debug \
  --dart-define=DEMO_SERVER=http://localhost:8080 \
  --dart-define=DEMO_AUTOPILOT=true
app=build/ios/iphonesimulator/Runner.app
xcrun simctl install booted "$app"
bundle=$(/usr/libexec/PlistBuddy -c 'Print :CFBundleIdentifier' "$app/Info.plist")
dart tool/kill_test.dart --platform ios --app-id "$bundle"
