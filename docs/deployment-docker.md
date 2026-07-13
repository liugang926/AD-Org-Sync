# Docker Deployment

The repository Docker configuration is production-oriented. The application and Nginx reverse proxy both run in Docker. It does not contain a default administrator password and does not reset an existing administrator during restart.

## Prerequisites

1. Choose the external URL. The bundled Nginx container serves HTTP; terminate TLS at an upstream load balancer or extend the Nginx configuration with managed certificates before using a public network.
2. Create a password file that is readable only by the deployment operator.
3. Keep the password file outside source control. The repository ignores `secrets/` by default.

PowerShell example:

```powershell
New-Item -ItemType Directory -Force secrets | Out-Null
Set-Content -NoNewline -Encoding UTF8 secrets/admin_password.txt "<strong-password>"
$env:AD_ORG_SYNC_ADMIN_PASSWORD_FILE = (Resolve-Path secrets/admin_password.txt)
$env:AD_ORG_SYNC_PUBLIC_BASE_URL = "https://sync.example.com"
docker compose up -d --build
```

POSIX shell example:

```sh
install -d -m 700 secrets
printf '%s' '<strong-password>' > secrets/admin_password.txt
chmod 600 secrets/admin_password.txt
export AD_ORG_SYNC_ADMIN_PASSWORD_FILE="$PWD/secrets/admin_password.txt"
export AD_ORG_SYNC_PUBLIC_BASE_URL="https://sync.example.com"
docker compose up -d --build
```

The container performs three startup steps:

1. Initialize or upgrade the SQLite database.
2. Create the administrator only when the account is missing.
3. Start the Web control plane with secure cookies enabled by default.

An existing administrator password is never reset automatically. Rotate it explicitly with `bootstrap-admin --reset` during an approved maintenance operation.

## Network and health checks

The application container is available only to the Compose network. The Nginx container publishes `${AD_ORG_SYNC_HTTP_BIND:-0.0.0.0}:${AD_ORG_SYNC_HTTP_PORT:-80}` and proxies to the application. The application exposes:

- `GET /healthz` for liveness.
- `GET /readyz` for readiness.

Do not expose application port 8010 directly. Keep `AD_ORG_SYNC_SECURE_COOKIES=always` when the public URL uses HTTPS; use `never` only for an explicitly approved private HTTP deployment.

## Upgrade

```sh
docker compose build --pull
docker compose up -d
docker compose exec web python -m sync_app.cli db-check --db-path /data/app.db
```

The named `ad_org_sync_data` volume contains the database and its backups. Back up this volume before major upgrades and verify `/readyz` after every deployment.

## Continuous deployment

`scripts/deploy-production.sh` is intended for a GitHub Actions self-hosted runner labeled `production`. A push to `main` deploys only after the quality, Windows, container, supply-chain, and browser-regression jobs pass. The script:

1. validates the Compose configuration;
2. creates a pre-deployment SQLite backup when the service already exists;
3. builds a commit-addressed application image;
4. recreates the Compose services and waits for readiness;
5. runs a database integrity check and records the successful image tag;
6. restores the previous successful image when startup or validation fails.

Production configuration stays outside the checkout at `/opt/ad-org-sync/shared/.env`. The administrator password file referenced by that environment file must also stay outside source control.

## DingTalk SSPR production endpoint

For the current production environment, the externally visible base URL must be exactly:

```text
https://it-service.tianjizn.com:9443
```

Keep secure cookies enabled. Port `9443` is part of the public origin and must not be omitted from the public base URL, DingTalk homepage, callback, probes, or operator documentation. The DingTalk micro-app homepage is `https://it-service.tianjizn.com:9443/sspr?corpid=$CORPID$`; the safe fallback callback is `https://it-service.tianjizn.com:9443/sspr/callback/dingtalk`.

After an approved exact-SHA deployment, verify `/healthz`, `/readyz`, `/login`, `/sspr`, `/sspr/oauth/start`, and the callback from outside the production host. Do not perform a real employee password reset unless a specifically authorized test account has been supplied. Without such an account, stop after DingTalk verification, binding resolution, and the pre-reset account page.
