#!/usr/bin/env bash
set -Eeuo pipefail
umask 077
readonly ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
readonly ENV_FILE="${PRODUCTION_ENV_FILE:-/opt/ad-org-sync/shared/.env}"
readonly STATE_DIR="${PRODUCTION_STATE_DIR:-/opt/ad-org-sync/shared}"
readonly LAST_SUCCESSFUL_FILE="${STATE_DIR}/last_successful_image_tag"
readonly DJANGO_SUCCESSFUL_FILE="${STATE_DIR}/last_successful_django_image_tag"
readonly COMPOSE_PROJECT_NAME="${COMPOSE_PROJECT_NAME:-ad_org_sync}"
readonly IMAGE_TAG="${AD_ORG_SYNC_IMAGE_TAG:-${GITHUB_SHA:-}}"
[[ -r "${ENV_FILE}" ]] || { echo "Missing protected environment file" >&2; exit 1; }
[[ "$(stat -c '%a' "${ENV_FILE}")" == "600" ]] || { echo "Environment file must be mode 0600" >&2; exit 1; }
[[ "${IMAGE_TAG}" =~ ^[0-9a-f]{40}$ ]] || { echo "A verified full commit SHA is required" >&2; exit 1; }
mkdir -p "${STATE_DIR}"
cd "${ROOT_DIR}"
export AD_ORG_SYNC_IMAGE_TAG="${IMAGE_TAG}" COMPOSE_PROJECT_NAME
compose=(docker compose --project-name "${COMPOSE_PROJECT_NAME}" --env-file "${ENV_FILE}")
previous_tag=""
previous_django_tag=""
deployment_started=0
if [[ -r "${LAST_SUCCESSFUL_FILE}" ]]; then previous_tag="$(tr -d '\r\n' < "${LAST_SUCCESSFUL_FILE}")"; fi
if [[ -r "${DJANGO_SUCCESSFUL_FILE}" ]]; then previous_django_tag="$(tr -d '\r\n' < "${DJANGO_SUCCESSFUL_FILE}")"; fi

wait_for_readiness() {
  local port
  port="$(awk -F= '/^[[:space:]]*AD_ORG_SYNC_HTTP_PORT=/{gsub(/[[:space:]\r]/, "", $2); value=$2} END{print value}' "${ENV_FILE}")"
  port="${port:-80}"
  for _ in $(seq 1 45); do
    if curl --fail --silent --max-time 5 "http://127.0.0.1:${port}/readyz" >/dev/null; then return 0; fi
    sleep 2
  done
  return 1
}

rollback() {
  local code=$?
  trap - ERR
  if [[ "${deployment_started}" == "1" && "${previous_tag}" =~ ^[0-9a-f]{40}$ && "${previous_tag}" == "${previous_django_tag}" && "${previous_tag}" != "${IMAGE_TAG}" ]]; then
    echo "Deployment failed; restoring previous application revision." >&2
    export AD_ORG_SYNC_IMAGE_TAG="${previous_tag}"
    if [[ -r "${STATE_DIR}/last_successful_compose.yml" ]]; then
      if docker compose --project-directory "${ROOT_DIR}" --project-name "${COMPOSE_PROJECT_NAME}" --env-file "${ENV_FILE}" -f "${STATE_DIR}/last_successful_compose.yml" up -d --no-build --remove-orphans && wait_for_readiness; then
        echo "Previous application readiness recovered; database was not automatically restored." >&2
      else
        echo "ROLLBACK FAILED. Preserve database and backups for manual recovery." >&2
      fi
    else
      echo "No previous Compose contract saved; manual recovery required." >&2
    fi
  elif [[ "${deployment_started}" == "1" ]]; then
    echo "No verified Django rollback revision; retired legacy service will remain stopped." >&2
    "${compose[@]}" stop web worker nginx || true
  fi
  exit "${code}"
}
trap rollback ERR
"${compose[@]}" config --quiet
if "${compose[@]}" ps --status running --services | grep -qx web; then
  # Existing Django deployments use online SQLite backup before rollout.
  "${compose[@]}" exec -T web python -m sync_app.cli db_backup
fi
"${compose[@]}" build --pull web worker volume-permissions
deployment_started=1
# Stop the worker before changing the schema; do not replace data volumes.
"${compose[@]}" stop worker
"${compose[@]}" up -d --remove-orphans
wait_for_readiness
"${compose[@]}" exec -T web python -m sync_app.cli db_check
bash scripts/install-scheduler.sh
cp docker-compose.yml "${STATE_DIR}/last_successful_compose.yml.tmp"
mv "${STATE_DIR}/last_successful_compose.yml.tmp" "${STATE_DIR}/last_successful_compose.yml"
printf '%s\n' "${IMAGE_TAG}" > "${LAST_SUCCESSFUL_FILE}.tmp"
mv "${LAST_SUCCESSFUL_FILE}.tmp" "${LAST_SUCCESSFUL_FILE}"
printf '%s\n' "${IMAGE_TAG}" > "${DJANGO_SUCCESSFUL_FILE}.tmp"
mv "${DJANGO_SUCCESSFUL_FILE}.tmp" "${DJANGO_SUCCESSFUL_FILE}"
trap - ERR
echo "Deployment and database checks succeeded for ${IMAGE_TAG}."
"${compose[@]}" ps
