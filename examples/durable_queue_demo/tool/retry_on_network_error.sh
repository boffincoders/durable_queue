#!/usr/bin/env bash
# Runs a build command, and runs it again when it failed because of the
# network. Sourced by ci_android.sh and ci_ios.sh.
#
#   retry_on_network_error flutter build ios --simulator ...
#
# Builds download prebuilt files, such as package:sqlite3's SQLite library
# from github.com. A DNS or connection blip on the CI runner then fails the
# whole device test (seen as "Failed host lookup: 'github.com'" during the
# Xcode build). Other failures are not retried, so a real build error still
# fails at once.

retry_on_network_error() {
  local attempts=3 attempt=1 log status
  log=$(mktemp)
  while true; do
    set +e
    "$@" 2>&1 | tee "$log"
    status=${PIPESTATUS[0]}
    set -e
    if [ "$status" -eq 0 ]; then
      rm -f "$log"
      return 0
    fi
    if [ "$attempt" -ge "$attempts" ] ||
      ! grep -qE "Failed host lookup|SocketException|Connection (reset|refused|closed)|Could not resolve|Network is unreachable|timed out" "$log"; then
      rm -f "$log"
      return "$status"
    fi
    echo "::warning::Build failed with a network error (attempt $attempt of $attempts); retrying in 30 seconds."
    attempt=$((attempt + 1))
    sleep 30
  done
}
