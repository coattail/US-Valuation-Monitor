#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)"
LAUNCH_AGENTS_DIR="${HOME}/Library/LaunchAgents"
LOG_DIR="${HOME}/Library/Logs/us-valuation-monitor"
AGENT_ID="com.sunny.us-valuation-monitor.refresh"
PLIST_PATH="${LAUNCH_AGENTS_DIR}/${AGENT_ID}.plist"
SCRIPT_PATH="${ROOT_DIR}/scripts/refresh-local-data.sh"
USER_ID="$(id -u)"

case "${ROOT_DIR}" in
  "${HOME}/Desktop/"* | "${HOME}/Documents/"* | "${HOME}/Downloads/"*)
    echo "Refusing to install launchd refresh from privacy-protected macOS folder:"
    echo "  ${ROOT_DIR}"
    echo "Move the project to a path like ~/Projects/us-valuation-monitor and retry."
    exit 1
    ;;
esac

mkdir -p "${LAUNCH_AGENTS_DIR}" "${LOG_DIR}"

cat >"${PLIST_PATH}" <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
  <dict>
    <key>Label</key>
    <string>${AGENT_ID}</string>
    <key>ProgramArguments</key>
    <array>
      <string>/bin/bash</string>
      <string>${SCRIPT_PATH}</string>
    </array>
    <key>WorkingDirectory</key>
    <string>${ROOT_DIR}</string>
    <key>RunAtLoad</key>
    <false/>
    <key>StartCalendarInterval</key>
    <array>
      <dict>
        <key>Weekday</key>
        <integer>2</integer>
        <key>Hour</key>
        <integer>6</integer>
        <key>Minute</key>
        <integer>15</integer>
      </dict>
      <dict>
        <key>Weekday</key>
        <integer>3</integer>
        <key>Hour</key>
        <integer>6</integer>
        <key>Minute</key>
        <integer>15</integer>
      </dict>
      <dict>
        <key>Weekday</key>
        <integer>4</integer>
        <key>Hour</key>
        <integer>6</integer>
        <key>Minute</key>
        <integer>15</integer>
      </dict>
      <dict>
        <key>Weekday</key>
        <integer>5</integer>
        <key>Hour</key>
        <integer>6</integer>
        <key>Minute</key>
        <integer>15</integer>
      </dict>
      <dict>
        <key>Weekday</key>
        <integer>6</integer>
        <key>Hour</key>
        <integer>6</integer>
        <key>Minute</key>
        <integer>15</integer>
      </dict>
    </array>
    <key>StandardOutPath</key>
    <string>${LOG_DIR}/launchd-refresh.log</string>
    <key>StandardErrorPath</key>
    <string>${LOG_DIR}/launchd-refresh.log</string>
  </dict>
</plist>
EOF

chmod 644 "${PLIST_PATH}"

launchctl bootout "gui/${USER_ID}" "${PLIST_PATH}" >/dev/null 2>&1 || true
launchctl bootstrap "gui/${USER_ID}" "${PLIST_PATH}"
launchctl enable "gui/${USER_ID}/${AGENT_ID}"

echo "Installed ${AGENT_ID}"
echo "Plist: ${PLIST_PATH}"
echo "Trading-day schedule: Tue-Sat 06:15 Asia/Shanghai"
echo "Manual run: launchctl kickstart -k gui/${USER_ID}/${AGENT_ID}"
