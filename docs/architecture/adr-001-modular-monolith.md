# ADR-001: Keep the synchronization platform as a modular monolith

- Status: Accepted
- Date: 2026-07-10

## Context

AD Org Sync has multiple source providers, a target-directory boundary, a Web control plane, CLI and desktop entry points, scheduled execution, approval workflows, and an integration Outbox. These are distinct business capabilities, but they currently share one deployment, one SQLite database, one execution lease, and one operational owner.

Splitting them into network services now would add distributed transactions, remote authorization, schema compatibility, deployment ordering, and cross-service tracing before the workload requires those costs.

## Decision

The system remains a modular monolith. Boundaries are enforced in-process:

- `sync_app/application`: channel-neutral use cases with mandatory tenant/actor context.
- `sync_app/core`: models, fingerprints, state machines, observability, and SLO policy.
- `sync_app/providers`: registered source and target contracts; provider-specific APIs stay behind adapters.
- `sync_app/services`: orchestration and runtime phases.
- `sync_app/storage`: migrations, repositories, Unit of Work, leases, and Outbox.
- `sync_app/web`, `sync_app/cli`, `sync_app/ui`: delivery adapters; routes and handlers do not own synchronization rules.

SQLite is treated as a single-node persistence boundary. Cross-repository changes that must be atomic join a `DatabaseManager.transaction()` Unit of Work. External delivery uses the Outbox; no network call is allowed to define database commit success.

## Service-extraction triggers

Extraction is reconsidered only when evidence meets at least one trigger:

1. High availability requires two or more active application instances and the single-node database cannot meet the availability SLO.
2. SQLite lock retries exceed 1% of writes for seven days, or database write p95 exceeds two seconds after query/index remediation.
3. Outbox ready/dead-letter backlog exceeds 10,000 records or remains above the delivery SLO for 15 minutes despite worker scaling within one process.
4. A provider needs an independent release/security boundary or materially different runtime dependencies.
5. Tenant data residency or regulatory isolation requires separate storage and deployment ownership.

Before extraction, the target capability must have a stable application contract, Provider/Outbox contract tests, idempotency keys, correlation propagation, an independent data owner, and a migration/rollback plan. PostgreSQL or another multi-writer store is selected before horizontal active-active execution; SQLite files are never shared over a network filesystem.

## Consequences

This decision keeps transactions and local operations simple while making future extraction deliberate. Module boundaries, contract tests, metrics, and the Outbox are required now because they are also the seams a later service split would use.
