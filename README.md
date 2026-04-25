# AD Org Sync

`AD Org Sync` is a Windows-first identity synchronization platform for syncing enterprise source directories into Active Directory over LDAPS.

The current production-ready target is:

- Source side: provider-based source connector framework
- Implemented live adapters in this build: WeCom, DingTalk
- Planned source schemas / contexts already reserved: Feishu, HR master data
- Target side: provider-based target connector framework with Active Directory / LDAPS
- Control plane: FastAPI Web console + CLI
- Local state: SQLite
- Architecture style: modular monolith with explicit bounded-context entry points

The codebase has already been refactored away from a simple `WeCom -> AD` utility into a multi-organization, policy-driven sync platform. Source-provider and target-provider registries are in place so DingTalk, Feishu, HR systems, and future directory targets can be added without rebuilding the core governance flow.

## Current Capabilities

- Multi-organization management
- Source connector abstraction with provider-aware runtime
- AD connector routing and multi-domain support
- Department to OU synchronization
- User provisioning, update, reactivation, and disable workflows
- Identity binding and department override rules
- Exception rules and protected account/group policies
- Conflict queue with manual resolution and recommendations
- High-risk approval flow with dry-run to apply gating
- Disable circuit breaker / throttling protection
- Future onboarding, contractor expiry, offboarding grace period, and replay queue
- Attribute mapping and AD to source write-back policy
- Advanced group lifecycle management
- Audit logs, operation logs, retention cleanup, and backup rotation
- Web console, CLI, import/export bundle, and bilingual UI
- SSPR bounded-context service skeleton for future employee self-service flows

## Architecture

- `sync_app/web/`
  - FastAPI control plane, authentication, RBAC, UI rendering
- `sync_app/web/services/`
  - Web-facing application service facades for jobs, conflicts, config, and integrations
- `sync_app/cli/`
  - CLI parser, command handlers, and console entry point
- `sync_app/services/`
  - Synchronization runtime, orchestration, reporting, config bundle handling
- `sync_app/storage/`
  - SQLite repositories, migrations, retention, backup, audit persistence
- `sync_app/storage/schema/`
  - SQLite migrations, default settings, and protected-group defaults
- `sync_app/providers/source/`
  - Source directory provider abstraction, registry, and concrete provider implementations
- `sync_app/providers/target/`
  - Target directory provider abstraction, registry, and AD / LDAPS adapter
- `sync_app/modules/`
  - Bounded-context product modules that stay outside the sync runtime, starting with SSPR
- `sync_app/core/`
  - Domain models, sync policies, conflict recommendation logic, rule governance
- `sync_app/core/models/`
  - Split domain model package for config, directory, jobs, conflicts, integrations, lifecycle, and Web admin records
- `sync_app/clients/`
  - Source API clients and notification clients

Layering rules are enforced by tests:

- `core`, `services`, `storage`, `providers`, and `modules` must not import `sync_app.web`
- `core` must not import provider implementations
- new product capabilities enter through `sync_app/modules/<context>/`
- new providers are registered through provider registries instead of being wired into `core`

## Product Direction

This repository is no longer structured as a one-off single-source sync script pack.

The current platform direction is:

- `Organization`
  - tenant-level scope, policies, connector ownership
- `Source Connector`
  - source provider config, source routing, source validation
- `Target Connector`
  - AD / LDAPS connection and connector-level overrides
- `Sync Governance`
  - mapping rules, exceptions, lifecycle queues, conflicts, approvals, throttling
- `Bounded Context`
  - optional product modules such as SSPR that reuse provider ports and audit services without entering the sync runtime

This lets the product evolve toward:

- WeCom
- DingTalk
- Feishu
- HR or master-data source systems
- additional target directory providers
- employee self-service capabilities such as SSPR

without redesigning the synchronization control plane.

## Delivery Pipeline

- CI runs the Python suite on Windows and a dedicated browser-regression job with Playwright Chromium.
- Tagged releases (`v*`) build and publish a wheel, a Windows `exe`, and a deployable web `zip` through GitHub Actions.
- The packaged web build now includes both Jinja templates and `sync_app/web/static` assets, so CSS and JS are present in non-source deployments.

## Fast Deployment

For a new Windows environment, the fastest supported deployment path is:

1. Prepare a working directory with either the source tree or the release `zip`.
2. Copy in a legacy `config.ini` only if you want to import an existing connector configuration.
3. Create a virtual environment and install deployment dependencies.
4. Run `install_web_service.ps1` once to initialize the database, bootstrap the admin account, register the Windows service, and run health checks.
5. Open the web console and finish organization-level configuration from `/config`.
6. Validate source and LDAP connectivity before the first `dry-run`.

One-command example:

```powershell
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements-deploy.txt
.\install_web_service.ps1 -AdminUsername admin -AdminPassword "simple888"
```

After installation:

- Web console: `http://127.0.0.1:8010`
- Health probe: `http://127.0.0.1:8010/healthz`
- Readiness probe: `http://127.0.0.1:8010/readyz`
- Service management: `.\manage_web_service.ps1 -Action status`

If you do not pass `-AdminUsername` and `-AdminPassword`, the service still starts, but the first visit will redirect to `/setup` so you can create the initial administrator in the browser.

## Quick Start

### 1. Create a virtual environment

```powershell
python -m venv .venv
.\.venv\Scripts\activate
pip install -r requirements-web.txt
```

### 2. Initialize or reuse the local database

The application will initialize SQLite automatically on first use.

Typical local paths:

- `%APPDATA%\ADOrgSync\app.db`
- fallback: `.appdata\ADOrgSync\app.db`

For explicit testing or demo use, pass `--db-path`.

### 3. Validate configuration

Database-backed organization config is now the primary configuration source.

```powershell
.\.venv\Scripts\python.exe -m sync_app.cli init-web --db-path test_artifacts\demo_web.db --config config.ini
.\.venv\Scripts\python.exe -m sync_app.cli bootstrap-admin --db-path test_artifacts\demo_web.db --username admin --password simple88
.\.venv\Scripts\python.exe -m sync_app.cli validate-config --db-path test_artifacts\demo_web.db --org-id default
.\.venv\Scripts\python.exe -m sync_app.cli test-source --db-path test_artifacts\demo_web.db --org-id default
.\.venv\Scripts\python.exe -m sync_app.cli test-ldap --db-path test_artifacts\demo_web.db --org-id default
```

Legacy `--config config.ini` is still supported as an import / compatibility source.

### 4. Start the Web console

```powershell
.\.venv\Scripts\python.exe -m sync_app.cli web --db-path test_artifacts\demo_web.db --host 127.0.0.1 --port 8010
```

### 5. Run synchronization

```powershell
.\.venv\Scripts\python.exe -m sync_app.cli sync --mode dry-run --db-path test_artifacts\demo_web.db --org-id default
.\.venv\Scripts\python.exe -m sync_app.cli sync --mode apply --db-path test_artifacts\demo_web.db --org-id default
```

Recommended rollout path:

1. Complete source and LDAP connectivity checks
2. Run `dry-run`
3. Review jobs, conflicts, risky operations, and exception hits
4. Approve high-risk plans if required
5. Run `apply`

## Windows Service Deployment

For a production-style local deployment on Windows, prefer the service scripts instead of scheduled tasks.

```powershell
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements-deploy.txt
.\install_web_service.ps1 -AdminUsername admin -AdminPassword "simple88"
```

Daily operations:

```powershell
.\manage_web_service.ps1 -Action status
.\upgrade_web_service.ps1
.\uninstall_web_service.ps1
```

Health endpoints:

- `GET /healthz` for liveness
- `GET /readyz` for readiness

Detailed deployment notes are in [docs/deployment-windows-service.md](docs/deployment-windows-service.md).

## Operational Guides

Current `v1` operational docs:

- [Admin V1 Operations Guide](docs/guides/admin-v1-operations-guide.md)
- [External Integrations API v1](docs/api/external-integrations-v1.md)
- [V1 Go-Live Checklist](docs/runbooks/v1-go-live-checklist.md)
- [Full Feature Execution Roadmap](docs/plan/full-feature-execution-roadmap.md)

Architecture and extension docs:

- [Bounded Context Entry Points](docs/architecture/bounded-context-entrypoints.md)
- [Architecture Optimization Completion Plan](docs/plan/architecture-optimization-completion-plan.md)
- [SSPR Bounded Context Execution Plan](docs/plan/sspr-bounded-context-execution-plan.md)
- [Technical Optimization Roadmap](docs/plan/technical-optimization-roadmap.md)
- [Architecture Decision Records](docs/adr/)

## Browser Regression

The project now includes Playwright-backed browser regression coverage for login, dashboard header controls, config provider fields, and jobs empty-state actions.

```powershell
.\.venv\Scripts\python.exe -m pip install -r requirements-test.txt
.\.venv\Scripts\python.exe -m playwright install chromium
.\.venv\Scripts\python.exe -m unittest tests.test_web_browser_regression -v
```

## Web Console

Main pages:

- `Dashboard`
- `Getting Started`
- `Organizations`
- `Config`
- `Advanced Sync`
- `Mappings`
- `Exceptions`
- `Conflicts`
- `Jobs`
- `Database`
- `Audit`
- `Users`
- `Account`

Access model:

- `super_admin`
  - full administrative access
- `operator`
  - operational access without sensitive config management
- `auditor`
  - read-only oversight access

The UI supports:

- English by default
- automatic Simplified Chinese fallback for Chinese browsers
- manual language switching
- basic and advanced modes

## Key Safety Controls

- Protected built-in AD accounts are blocked by default
- Protected AD groups are excluded by default
- High-risk apply can be forced through dry-run approval
- Bulk disable circuit breaker can block suspicious mass disable plans
- Session security, CSRF, role-based access control, and password policy are enforced in the Web plane
- On Windows, database-backed connector secrets are encrypted at rest with DPAPI when available
- Audit logs, operation logs, conflict logs, review records, and retention cleanup are built in

## Configuration Model

Primary configuration source:

- SQLite organization configuration
- SQLite connector configuration
- SQLite advanced sync policy tables

Legacy compatibility:

- `config.ini`
- per-organization legacy import path
- per-connector legacy import path

Those file paths are now treated as import / compatibility inputs, not the primary runtime source of truth.

## Extending the Platform

Use these entry points for new work:

- Source provider: add an adapter under `sync_app/providers/source/<provider>/` or `sync_app/providers/source/<provider>.py`, then register it through `sync_app.providers.source.registry`.
- Target provider: add an adapter under `sync_app/providers/target/`, then register it through `sync_app.providers.target.registry`.
- Product module: add a bounded context under `sync_app/modules/<context>/`; Web routes and CLI handlers should call that module's service layer only.
- SSPR: continue from `sync_app/modules/sspr/`; employee authentication and Web adapters should stay separate from administrator Web sessions and the sync runtime.

Before adding a new feature, run the architecture guard tests to confirm the dependency direction remains clean.

## CLI Reference

```powershell
.\.venv\Scripts\python.exe -m sync_app.cli version
.\.venv\Scripts\python.exe -m sync_app.cli init-web --db-path app.db --config config.ini
.\.venv\Scripts\python.exe -m sync_app.cli bootstrap-admin --db-path app.db --username admin --password simple88
.\.venv\Scripts\python.exe -m sync_app.cli validate-config --db-path app.db --org-id default
.\.venv\Scripts\python.exe -m sync_app.cli test-source --db-path app.db --org-id default
.\.venv\Scripts\python.exe -m sync_app.cli test-ldap --db-path app.db --org-id default
.\.venv\Scripts\python.exe -m sync_app.cli sync --mode dry-run --db-path app.db --org-id default
.\.venv\Scripts\python.exe -m sync_app.cli sync --mode apply --db-path app.db --org-id default
.\.venv\Scripts\python.exe -m sync_app.cli approve-plan <job_id> --notes "reviewed"
.\.venv\Scripts\python.exe -m sync_app.cli conflicts list --status open --json
.\.venv\Scripts\python.exe -m sync_app.cli conflicts apply-recommendation <conflict_id> --reason "checked manually"
.\.venv\Scripts\python.exe -m sync_app.cli config-export --db-path app.db --org-id default
.\.venv\Scripts\python.exe -m sync_app.cli config-import --db-path app.db --org-id target --file bundle.json
.\.venv\Scripts\python.exe -m sync_app.cli db-check
.\.venv\Scripts\python.exe -m sync_app.cli db-backup --label manual
.\.venv\Scripts\python.exe -m sync_app.cli web --db-path app.db --host 127.0.0.1 --port 8000
```

## Testing

```powershell
.\.venv\Scripts\python.exe -m compileall sync_app tests
.\.venv\Scripts\python.exe -m pytest
.\.venv\Scripts\python.exe -m pytest tests/test_architecture_boundaries.py tests/test_structure_guards.py
```

## Security Notes

- Do not commit `config.ini`
- Do not commit runtime databases, logs, backups, or generated artifacts
- Keep LDAPS certificate validation enabled in production
- Use reverse proxy / TLS termination settings explicitly when deploying behind a gateway
- Review `SECURITY.md` before production rollout

## Repository Notes

This is the first GitHub delivery for the refactored platformized codebase.

The repository includes:

- current web control plane
- current SQLite-backed configuration and governance model
- provider abstraction groundwork for future source systems
- target provider abstraction and registry for future directory targets
- bounded-context entry point for future SSPR and employee self-service modules
- multi-organization runtime and UI support
- safety and audit controls required for enterprise rollout
