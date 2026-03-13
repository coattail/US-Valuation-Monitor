#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)"
APP_SUPPORT_DIR="${HOME}/Library/Application Support/us-valuation-monitor"
LOG_DIR="${HOME}/Library/Logs/us-valuation-monitor"
LOCK_DIR="${TMPDIR:-/tmp}/us-valuation-monitor-refresh.lock"
LOCK_PID_FILE="${LOCK_DIR}/pid"
NODE_VERSION="25.8.0"

mkdir -p "${APP_SUPPORT_DIR}" "${LOG_DIR}"
LOG_FILE="${LOG_DIR}/local-refresh.log"

exec >>"${LOG_FILE}" 2>&1

timestamp() {
  date "+%Y-%m-%d %H:%M:%S %Z"
}

cleanup_lock() {
  rm -rf "${LOCK_DIR}" >/dev/null 2>&1 || true
}

acquire_lock() {
  local lock_pid=""

  if mkdir "${LOCK_DIR}" >/dev/null 2>&1; then
    printf '%s\n' "$$" >"${LOCK_PID_FILE}"
    return 0
  fi

  if [[ -f "${LOCK_PID_FILE}" ]]; then
    lock_pid="$(cat "${LOCK_PID_FILE}" 2>/dev/null || true)"
    if [[ -n "${lock_pid}" ]] && kill -0 "${lock_pid}" >/dev/null 2>&1; then
      echo "[$(timestamp)] refresh already running (pid ${lock_pid}), skipping"
      return 1
    fi
  fi

  echo "[$(timestamp)] stale lock detected, rebuilding lock"
  rm -rf "${LOCK_DIR}" >/dev/null 2>&1 || true
  mkdir -p "${LOCK_DIR}"
  printf '%s\n' "$$" >"${LOCK_PID_FILE}"
  return 0
}

download_node() {
  local arch node_dist install_dir tmp_dir extract_dir archive_file url

  case "$(uname -m)" in
    arm64)
      node_dist="node-v${NODE_VERSION}-darwin-arm64"
      ;;
    x86_64)
      node_dist="node-v${NODE_VERSION}-darwin-x64"
      ;;
    *)
      echo "[$(timestamp)] unsupported architecture: $(uname -m)"
      return 1
      ;;
  esac

  install_dir="${APP_SUPPORT_DIR}/${node_dist}"
  if is_node_usable "${install_dir}/bin/node"; then
    printf '%s\n' "${install_dir}/bin/node"
    return 0
  fi

  rm -rf "${install_dir}" >/dev/null 2>&1 || true
  tmp_dir="$(mktemp -d)"
  extract_dir="${tmp_dir}/extract"
  archive_file="${tmp_dir}/node.tar.gz"
  mkdir -p "${extract_dir}"
  url="https://nodejs.org/dist/v${NODE_VERSION}/${node_dist}.tar.gz"

  echo "[$(timestamp)] downloading ${url}" >&2
  curl --silent --show-error --fail --location --retry 5 --retry-all-errors --retry-delay 2 \
    --connect-timeout 10 --max-time 1200 "${url}" -o "${archive_file}"
  tar -xzf "${archive_file}" -C "${extract_dir}"

  if ! is_node_usable "${extract_dir}/${node_dist}/bin/node"; then
    echo "[$(timestamp)] node install verification failed for ${node_dist}" >&2
    rm -rf "${tmp_dir}" >/dev/null 2>&1 || true
    return 1
  fi

  mv "${extract_dir}/${node_dist}" "${install_dir}"
  rm -rf "${tmp_dir}"

  printf '%s\n' "${install_dir}/bin/node"
}

is_node_usable() {
  local node_bin="$1"
  [[ -n "${node_bin}" ]] && [[ -x "${node_bin}" ]] && "${node_bin}" -v >/dev/null 2>&1
}

resolve_node() {
  local candidates candidate downloaded

  candidates=(
    "$(command -v node 2>/dev/null || true)"
    "/opt/homebrew/bin/node"
    "/usr/local/bin/node"
    "${APP_SUPPORT_DIR}/node/bin/node"
    "${APP_SUPPORT_DIR}/node-v${NODE_VERSION}-darwin-arm64/bin/node"
    "${APP_SUPPORT_DIR}/node-v${NODE_VERSION}-darwin-x64/bin/node"
    "/tmp/node-v${NODE_VERSION}-darwin-arm64/bin/node"
    "/tmp/node-v${NODE_VERSION}-darwin-x64/bin/node"
  )

  for candidate in "${candidates[@]}"; do
    if [[ -n "${candidate}" && -x "${candidate}" ]]; then
      if is_node_usable "${candidate}"; then
        printf '%s\n' "${candidate}"
        return 0
      fi
      if [[ "${candidate}" == "${APP_SUPPORT_DIR}/"* ]]; then
        echo "[$(timestamp)] removing invalid cached node: ${candidate}" >&2
        rm -rf "$(dirname "$(dirname "${candidate}")")" >/dev/null 2>&1 || true
      fi
    fi
  done

  downloaded="$(download_node)"
  mkdir -p "${APP_SUPPORT_DIR}"
  ln -sfn "$(dirname "$(dirname "${downloaded}")")" "${APP_SUPPORT_DIR}/node"
  printf '%s\n' "${downloaded}"
}

if ! acquire_lock; then
  exit 0
fi
trap cleanup_lock EXIT

echo "[$(timestamp)] refresh start"

NODE_BIN="$(resolve_node)"
echo "[$(timestamp)] using node: ${NODE_BIN}"

cd "${ROOT_DIR}"

"${NODE_BIN}" packages/data-pipeline/src/build-snapshot.ts
"${NODE_BIN}" packages/data-pipeline/src/split-index-dataset.ts
"${NODE_BIN}" packages/data-pipeline/src/build-company-snapshot.ts
"${NODE_BIN}" packages/data-pipeline/src/split-company-dataset.ts

python3 - <<'PY'
import json
from pathlib import Path

root = Path("data/standardized")
index_data = json.loads((root / "valuation-history.json").read_text())
company_data = json.loads((root / "company-valuation-history.json").read_text())

latest_index = max(
    (point["date"] for item in index_data["indices"] for point in item.get("points", [])),
    default="n/a",
)
latest_company = max(
    (point["date"] for item in company_data["indices"] for point in item.get("points", [])),
    default="n/a",
)

print(
    f"[summary] index_generated_at={index_data.get('generatedAt')} "
    f"latest_index_date={latest_index}"
)
print(
    f"[summary] company_generated_at={company_data.get('generatedAt')} "
    f"latest_company_date={latest_company}"
)
PY

echo "[$(timestamp)] refresh complete"
