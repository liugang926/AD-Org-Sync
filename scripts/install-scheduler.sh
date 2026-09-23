#!/usr/bin/env bash
set -Eeuo pipefail
umask 077

project="${COMPOSE_PROJECT_NAME:-ad_org_sync}"
[[ "${project}" =~ ^[a-z0-9][a-z0-9_-]*$ ]] || { echo "Invalid Compose project name" >&2; exit 1; }
docker_bin="$(command -v docker)" || { echo "Docker command is required for host scheduling" >&2; exit 1; }
logger_bin="$(command -v logger)" || { echo "Logger command is required for host scheduling" >&2; exit 1; }
command -v crontab >/dev/null || { echo "Crontab command is required for host scheduling" >&2; exit 1; }
for binary in "${docker_bin}" "${logger_bin}"; do
  [[ "${binary}" =~ ^/[a-zA-Z0-9_./-]+$ ]] || { echo "Scheduler command path is unsafe" >&2; exit 1; }
done

marker="# AD_ORG_SYNC_SCHEDULER"
entry="* * * * * ${docker_bin} exec ${project}-web-1 python -m sync_app.cli enqueue_sync --due 2>&1 | ${logger_bin} -t ad-org-sync-scheduler ${marker}"
current="$(mktemp)"
filtered="${current}.filtered"
error="${current}.error"
trap 'rm -f "${current}" "${filtered}" "${error}"' EXIT
if ! crontab -l > "${current}" 2> "${error}"; then
  if grep -qi 'no crontab' "${error}"; then
    : > "${current}"
  else
    echo "Cannot read current crontab; scheduler was not changed" >&2
    exit 1
  fi
fi
if [[ "$(grep -Fc "${marker}" "${current}" || true)" == "1" ]] && grep -Fxq "${entry}" "${current}"; then
  echo "Host scheduler already installed."
  exit 0
fi
awk -v marker="${marker}" 'index($0, marker) == 0' "${current}" > "${filtered}"
printf '%s\n' "${entry}" >> "${filtered}"
crontab "${filtered}"
echo "Host scheduler installed; application schedule remains controlled by Django settings."
