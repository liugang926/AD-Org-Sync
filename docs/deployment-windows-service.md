# Windows Service Deployment

This project now supports a Windows Service deployment path for the web control plane.

## Prerequisites

- Windows Server or Windows workstation
- Python 3.10+
- A writable deployment directory
- Administrative PowerShell session for service installation

## First-Time Install

```powershell
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements-deploy.txt
.\install_web_service.ps1 -AdminUsername admin -AdminPassword "simple88"
```

Default behavior:

- Database path: `.appdata\local_web.db`
- Config compatibility path: `config.ini`
- Service name: `ADOrgSyncWeb`
- Bind address: `http://127.0.0.1:8010`

## Common Operations

Show service state:

```powershell
.\manage_web_service.ps1 -Action status
```

Restart after source changes:

```powershell
.\upgrade_web_service.ps1
```

Stop the service:

```powershell
.\manage_web_service.ps1 -Action stop
```

Remove the service:

```powershell
.\uninstall_web_service.ps1
```

## Health Endpoints

- Liveness: `GET /healthz`
- Readiness: `GET /readyz`

`/readyz` returns `503` until the database is reachable, static assets are present, the default organization exists, and the local administrator has been bootstrapped.

## Notes

- `install_web_service.ps1` automatically installs the `web` and `deploy` dependency sets.
- The Windows service stores its runtime configuration in the service registry parameters instead of relying on the current working directory.
- Legacy `config.ini` remains a compatibility input only. Database-backed organization settings remain the source of truth.
