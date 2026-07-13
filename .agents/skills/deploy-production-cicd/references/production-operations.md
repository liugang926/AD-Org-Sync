# Production Operations Profile

Use this reference only for AD Org Sync production and CI/CD work. Do not store secret values here.

## Repository and release path

- GitHub repository: `liugang926/AD-Org-Sync`
- Protected deployment branch: `main`
- Workflow: `.github/workflows/ci.yml`
- Deployment entry point: `scripts/deploy-production.sh`
- Compose definition: `docker-compose.yml`
- Nginx definition: `deploy/nginx/default.conf`
- Production URL: `http://10.106.1.122`

Production currently uses private-network HTTP. Do not claim HTTPS protection until a domain and trusted certificate are configured and verified.

## Runner

- Runner name: `ad-org-sync-prod-10-106-1-122`
- Required labels: `self-hosted`, `Linux`, `X64`, `production`
- Install directory: `/opt/actions-runner`
- Service: `actions.runner.liugang926-AD-Org-Sync.ad-org-sync-prod-10-106-1-122.service`
- Service account: `nginx`

The runner must be a systemd service, enabled across reboots, and have Docker group access. Root password login and GitHub registration tokens are not part of routine deployments.

## Production layout

- Shared state: `/opt/ad-org-sync/shared`
- Protected environment: `/opt/ad-org-sync/shared/.env` (`0600`)
- Protected host password file: `/opt/ad-org-sync/shared/admin_password.txt` (`0600`)
- Last successful tag: `/opt/ad-org-sync/shared/last_successful_image_tag`
- Release snapshots: `/opt/ad-org-sync/releases`
- Compose project: `ad_org_sync`
- Web image: `ad-org-sync-web:<full-git-sha>`
- Expected containers: `ad_org_sync-web-1`, `ad_org_sync-nginx-1`

The running Docker image tag and `last_successful_image_tag` are authoritative for the deployed application revision. A legacy release symlink alone is not deployment evidence.

## Required environment behavior

- `AD_ORG_SYNC_PUBLIC_BASE_URL` must match the externally used URL.
- `AD_ORG_SYNC_HTTP_BIND` and `AD_ORG_SYNC_HTTP_PORT` define the Nginx listener.
- `AD_ORG_SYNC_ADMIN_USERNAME` must match the intended bootstrap administrator.
- `AD_ORG_SYNC_ADMIN_PASSWORD_FILE` must point to the protected host password file.
- Secure cookies may be disabled only while the private endpoint is HTTP; enable them when HTTPS is introduced.

Do not print the environment file because it may acquire secrets over time. Select only non-secret keys when diagnosing configuration.

## Verification commands

Use authenticated tooling without exposing credentials:

```bash
gh run view <run-id> --repo liugang926/AD-Org-Sync --json status,conclusion,jobs,url
gh api repos/liugang926/AD-Org-Sync/actions/runners
```

On production:

```bash
cat /opt/ad-org-sync/shared/last_successful_image_tag
docker ps --filter 'name=ad_org_sync-' --format '{{.Names}}|{{.Image}}|{{.Status}}'
systemctl is-active actions.runner.liugang926-AD-Org-Sync.ad-org-sync-prod-10-106-1-122.service
curl --fail --silent --show-error http://127.0.0.1/healthz
curl --fail --silent --show-error http://127.0.0.1/readyz
```

From a separate client, verify:

```text
GET http://10.106.1.122/healthz -> 200 and status=ok
GET http://10.106.1.122/readyz  -> 200, status=ready, all checks=true
GET http://10.106.1.122/login   -> 200
```

## Rollback evidence

The deploy script reads the previous tag before rollout and traps errors after deployment begins. A valid rollback restores that previous tag with Compose and waits for readiness. Verify the running image, health response, and database integrity after rollback; the existence of an old image is not enough.

Manual database restore, volume deletion, runner re-registration, firewall changes, and TLS changes require explicit authorization and separate verification.
