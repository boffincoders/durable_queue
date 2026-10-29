#!/bin/sh
# Renders the social preview card (website/static/og.png) and the light and
# dark architecture diagrams used by the README (docs/media/architecture-*.png)
# with headless Chrome.
# Usage: sh website/og/render.sh   (set CHROME to your Chrome binary if it is not the macOS default)
set -e
here="$(cd "$(dirname "$0")" && pwd)"
root="$here/../.."
chrome="${CHROME:-/Applications/Google Chrome.app/Contents/MacOS/Google Chrome}"
shot() { # width height scale output input
  "$chrome" --headless=new --disable-gpu --hide-scrollbars --force-device-scale-factor="$3" \
    --window-size="$1,$2" --virtual-time-budget=4000 --screenshot="$4" "file://$5" 2>/dev/null
}
shot 1200 630 1 "$root/website/static/og.png" "$here/card.html"
for theme in light dark; do
  frame="$here/.diagram-$theme.html"
  sed 's|{{root}}|../static/|' "$root/website/architecture.svg" > "$frame.svg"
  sed -e "s/THEME/$theme/" -e "/<!--SVG-->/r $frame.svg" -e "/<!--SVG-->/d" "$here/diagram.html" > "$frame"
  shot 960 712 2 "$root/docs/media/architecture-$theme.png" "$frame"
  rm "$frame" "$frame.svg"
done
