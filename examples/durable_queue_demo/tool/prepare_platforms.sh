#!/usr/bin/env bash
# Creates the android/ and ios/ folders for the demo with `flutter create`
# and applies the settings the demo needs. Safe to run again.
#
#   bash tool/prepare_platforms.sh
#
# Commit the generated folders if you want to change them by hand.
set -euo pipefail
cd "$(dirname "$0")/.."

if [ ! -d android ] || [ ! -d ios ]; then
  flutter create --org dev.boffincoders --project-name durable_queue_demo \
    --platforms=android,ios --no-pub .
  # `flutter create` adds the counter app's test when it is missing.
  if [ -f test/widget_test.dart ] && grep -q "MyApp" test/widget_test.dart; then
    rm test/widget_test.dart
  fi
fi

# Android: the demo server is plain HTTP on your computer.
manifest=android/app/src/main/AndroidManifest.xml
if ! grep -q usesCleartextTraffic "$manifest"; then
  perl -0pi -e 's/<application/<application\n        android:usesCleartextTraffic="true"/' "$manifest"
fi
if ! grep -q 'android.permission.INTERNET' "$manifest"; then
  perl -0pi -e 's/(<manifest[^>]*>)/$1\n    <uses-permission android:name="android.permission.INTERNET"\/>/' "$manifest"
fi

# iOS: allow plain HTTP to local addresses, and iOS 14 or later
# (background_downloader needs it).
plist=ios/Runner/Info.plist
if ! grep -q NSAllowsLocalNetworking "$plist"; then
  perl -0pi -e 's|<dict>|<dict>\n\t<key>NSAppTransportSecurity</key>\n\t<dict>\n\t\t<key>NSAllowsLocalNetworking</key>\n\t\t<true/>\n\t</dict>|' "$plist"
fi
if ! grep -q NSLocalNetworkUsageDescription "$plist"; then
  perl -0pi -e 's|<dict>|<dict>\n\t<key>NSLocalNetworkUsageDescription</key>\n\t<string>The demo uploads photos to the demo server on your computer.</string>|' "$plist"
fi
perl -pi -e 's/IPHONEOS_DEPLOYMENT_TARGET = 1[0-3]\.0;/IPHONEOS_DEPLOYMENT_TARGET = 14.0;/g' \
  ios/Runner.xcodeproj/project.pbxproj
if [ -f ios/Podfile ]; then
  perl -pi -e "s/^#? ?platform :ios, '1[0-3]\.0'/platform :ios, '14.0'/" ios/Podfile
fi

echo "Platforms ready."
