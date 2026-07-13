#!/usr/bin/env bash
set -Eeuo pipefail

readonly ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
readonly ENV_FILE="${PRODUCTION_ENV_FILE:-/opt/ad-org-sync/shared/.env}"
readonly STATE_DIR="${PRODUCTION_STATE_DIR:-/opt/ad-org-sync/shared}"
readonly LAST_SUCCESSFUL_FILE="${STATE_DIR}/last_successful_image_tag"
readonly COMPOSE_PROJECT_NAME="${COMPOSE_PROJECT_NAME:-ad_org_sync}"
readonly IMAGE_TAG="${AD_ORG_SYNC_IMAGE_TAG:-${GITHUB_SHA:-$(git -C "${ROOT_DIR}" rev-parse --short=12 HEAD)}}"

if [[ ! -r "${ENV_FILE}" ]]; then
  echo "Production environment file is missing or unreadable: ${ENV_FILE}" >&2
  exit 1
fi

mkdir -p "${STATE_DIR}"
cd "${ROOT_DIR}"

export AD_ORG_SYNC_IMAGE_TAG="${IMAGE_TAG}"
export COMPOSE_PROJECT_NAME

compose=(docker compose --project-name "${COMPOSE_PROJECT_NAME}" --env-file "${ENV_FILE}")
previous_tag=""
deployment_started=0

if [[ -r "${LAST_SUCCESSFUL_FILE}" ]]; then
  previous_tag="$(tr -d '\r\n' < "${LAST_SUCCESSFUL_FILE}")"
fi

wait_for_readiness() {
  local port
  port="$(awk -F= '/^[[:space:]]*AD_ORG_SYNC_HTTP_PORT=/{gsub(/[[:space:]\r]/, "", $2); value=$2} END{print value}' "${ENV_FILE}")"
  port="${port:-80}"

  for _ in $(seq 1 30); do
    if curl --fail --silent --show-error --max-time 5 "http://127.0.0.1:${port}/readyz" >/dev/null; then
      return 0
    fi
    sleep 2
  done
  return 1
}

rollback() {
  local exit_code=$?
  if [[ "${deployment_started}" == "1" && -n "${previous_tag}" && "${previous_tag}" != "${IMAGE_TAG}" ]]; then
    echo "Deployment failed; rolling back to image tag ${previous_tag}." >&2
    export AD_ORG_SYNC_IMAGE_TAG="${previous_tag}"
    "${compose[@]}" up -d --no-build --remove-orphans || true
    wait_for_readiness || true
  fi
  exit "${exit_code}"
}
trap rollback ERR

"${compose[@]}" config --quiet

if "${compose[@]}" ps --status running --services | grep -qx web; then
  "${compose[@]}" exec -T web \
    python -m sync_app.cli db-backup --db-path /data/app.db --label "pre-deploy-${IMAGE_TAG}" --json
fi

"${compose[@]}" build --pull web volume-permissions
deployment_started=1
"${compose[@]}" up -d --remove-orphans

wait_for_readiness
"${compose[@]}" exec -T web python -m sync_app.cli db-check --db-path /data/app.db --json

printf '%s\n' "${IMAGE_TAG}" > "${LAST_SUCCESSFUL_FILE}"
trap - ERR

echo "Production deployment succeeded with image tag ${IMAGE_TAG}."
"${compose[@]}" ps
