# Docker Deployment

The repository Docker configuration is production-oriented. It does not contain a default administrator password, does not reset an existing administrator during restart, and binds the published port to loopback by default.

## Prerequisites

1. Terminate TLS at a reverse proxy and choose the external HTTPS URL.
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

The default host publication is `127.0.0.1:8010`, intended for a reverse proxy on the same host. The container exposes:

- `GET /healthz` for liveness.
- `GET /readyz` for readiness.

Do not expose port 8010 directly to an untrusted network. Keep `AD_ORG_SYNC_SECURE_COOKIES=always` when the public URL uses HTTPS.

## Upgrade

```sh
docker compose build --pull
docker compose up -d
docker compose exec web python -m sync_app.cli db-check --db-path /data/app.db
```

The named `ad_org_sync_data` volume contains the database and its backups. Back up this volume before major upgrades and verify `/readyz` after every deployment.
