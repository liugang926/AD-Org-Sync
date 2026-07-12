# Reliability operations

## Service objectives

The authenticated `GET /api/observability/slo` endpoint evaluates the in-process metrics against these defaults:

- synchronization success rate: at least 99%;
- Outbox delivery rate: at least 99.5%;
- Plan phase maximum duration: 300 seconds;
- Apply phase maximum duration: 1,800 seconds.

`GET /metrics` exports authenticated Prometheus text. `X-Correlation-ID` is accepted and returned by the Web application; invalid header characters are removed. Logs include correlation, organization, and job fields and redact common credentials and direct identifiers after message interpolation.

Metrics are process-local. Production collectors must scrape them before restart; long-window SLO alerting belongs in the metrics backend.

## Database checks and recovery drills

Run integrity and migration verification:

```powershell
python -m sync_app.cli db-check --db-path .\data\app.db --json
```

Create a backup:

```powershell
python -m sync_app.cli db-backup --db-path .\data\app.db --label pre-change --json
```

Prove that a backup can be restored. Omitting `--backup-path` creates a fresh drill backup first:

```powershell
python -m sync_app.cli db-restore-check --db-path .\data\app.db --backup-path .\data\backups\app_pre-change_YYYYMMDD_HHMMSS.db --json
```

The drill restores to an isolated temporary database, runs SQLite integrity and migration-checksum validation, and compares logical row counts for every user table. Temporary restored databases are deleted; the backup and JSON report should be retained by the operator.

## Interrupted synchronization

Runtime phases are guarded by the sequence `replay → prepare → plan → apply → finalize` and persisted on the job record. When an execution lease expires:

- before Apply: enqueue a fresh run;
- after Apply starts: inspect operation logs and target state, then run a new dry run before any Apply retry.

Never mark an interrupted Apply job successful manually. Approval fingerprints are versioned and organization-scoped; a new dry run is required when the configuration, desired state, approval TTL, or plan fingerprint changes.
