# Phase 3 Data Sources Migration Contract

## Product goal / 产品目标

Phase 3 separates connection configuration, source browsing, snapshot history,
data repair, binding reconciliation, and synchronization scope into task-based
pages. It reduces the daily Source Directory from the former 27-column identity
relationship table to eight business columns without removing capabilities.

第三阶段把连接配置、源目录浏览、快照历史、数据修复、绑定对账和同步范围拆分为按任务组织的页面。源目录日常列表从原有约 27 列身份证据表降为 8 个业务列，同时保留原有能力和兼容写入端点。

## Page changes / 页面变化

| Canonical route | Single responsibility | Primary CTA |
| --- | --- | --- |
| `/data-sources/connectors` | Organization-scoped source and target AD connection settings | Save Connection Settings |
| `/data-sources/source-directory` | Browse the latest successful employee snapshot in eight columns | Refresh Directory |
| `/data-sources/snapshots` | Compare immutable source refresh versions and counts | Open Latest Snapshot |
| `/data-sources/data-quality` | Persist quality scans, trends, and repair exports | Run Quality Scan |
| `/identity-governance/binding-reconciliation` | Candidate, binding, AD, Dry Run, and Apply evidence plus stale-binding safety flow | Scan Binding Differences |
| `/sync-policies/scope` | Scope selection, account naming, and account-creation Dry Run preparation | Save Sync Scope |

Advanced evidence is kept in collapsed sections. Binding Reconciliation renders
Candidate, Current Binding, Latest Dry Run, Latest Apply, and Current AD State
as a five-step identity timeline for every user.

技术证据进入折叠区。绑定对账为每个用户展示 Candidate、当前绑定、最近 Dry Run、最近 Apply、当前 AD 状态五步身份时间线。

## Permission and security impact / 权限与安全影响

- Connector reads require `config.read`; connection writes require
  `config.write`, POST, CSRF, organization scope, and an audit event.
- Blank submitted secrets preserve the current encrypted value. Audit payloads
  contain only change flags and non-secret connection metadata.
- A connector ID already owned by another organization is rejected; the new
  endpoint cannot move or overwrite it.
- Data-quality persistence now requires `config.write`; exports remain read-only.
- Binding cleanup keeps `mappings.write` and the five-step high-risk workflow.
  Unknown, unavailable, protected, ambiguous, or changed AD evidence never
  authorizes deletion.
- Account-creation preparation re-verifies candidates and only saves an exact
  Dry Run scope. This phase does not operate real AD accounts.
- All GET routes remain read-only.

## Compatibility / 兼容方案

| Existing entry | Compatibility behavior |
| --- | --- |
| `/config` | Retains the legacy combined configuration page. `/data-sources/connectors` is the dedicated connection page. |
| `/source-directory` | Serves the new source-only daily list through the same handler as `/data-sources/source-directory`. |
| `/data-quality` | Serves the same Data Quality handler as `/data-sources/data-quality`. |
| `/source-directory/test` | Legacy POST remains; the UI entry moves to Connectors and returns there. |
| `/source-directory/refresh` | Legacy POST remains and returns to Source Directory. |
| `/source-directory/reconcile-stale-bindings*` | Compatibility aliases only; the UI uses the independent Binding Reconciliation scan and execute POST routes. |
| `/source-directory/scope` | Legacy POST remains; completion returns to Sync Scope. |
| `/source-directory/create-selection` | Legacy POST remains; failures return to Sync Scope and success proceeds to Dry Run review. |
| `/advanced-sync/data-quality-snapshot` | Compatibility API remains, but the duplicate Advanced Sync UI entry is removed. |

No schema migration is required. Snapshot History reads existing
`source_directory_snapshots` rows with organization-scoped pagination and
filters.

## Test evidence / 测试证据

- Unit: snapshot filtering, pagination, and organization isolation.
- Integration: canonical and legacy routes, CSRF/RBAC, blank-secret retention,
  audit redaction, connector policy preservation, cross-organization connector
  rejection, eight-column lists, identity timeline, and fail-closed binding cleanup.
- Browser: desktop/narrow-screen navigation across Source Directory, Binding
  Reconciliation, Sync Scope, Snapshot History, and Connectors; keyboard-focusable
  table regions; selection and cleanup workflows; no secret leakage.
- Full repository CI-equivalent checks must pass before the Draft PR is handed
  off for merge confirmation.

## Rollback / 回滚说明

Revert the Phase 3 commit and redeploy the previous verified image. There is no
schema rollback and no data transformation to reverse. Existing source
snapshots, quality snapshots, bindings, scope selections, connector policies,
and audit logs remain valid. Legacy POST paths and `/config` allow the previous
UI to resume without data conversion.
