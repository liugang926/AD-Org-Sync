# V1 Admin Operations Guide

For WeCom, DingTalk, and Feishu source-user preview, employee-ID/custom-field account mapping, scoped Dry Run/Apply, provider permissions, snapshot migration, and troubleshooting, also follow the [Source Directory and AD Account Mapping Guide](source-directory-ad-account-mapping.md).

## Scope

This guide is the operator-facing manual for the current `v1` control plane.

It assumes the following capabilities are already available in the deployed build:

- Control Tower, Data Sources, Identity Governance, Sync Policies, and Execution Center
- Operations Center: lifecycle queue, automation, notifications, and audit log
- System Management: organizations, administrators, employee self-service, database, branding, and deployment settings
- Config Release Center and the organization-scoped external integration API

The recommended operating model is still:

1. `dry run`
2. review conflicts and high-risk changes
3. approve if needed
4. `apply`

## Daily Control Loop

Use this lightweight sequence at the start of each admin session:

1. Open `/overview/control-tower`
   - Confirm the selected organization.
   - Review preflight status, active jobs, and the latest warnings.
   - Switch to `Advanced` mode if you need routing, lifecycle, or governance views.
2. Open `/execution-center/dry-run`
   - Check whether the organization is `Ready`, `Needs Attention`, or `Blocked`.
   - Run the next `dry run` if configuration or source data changed.
3. Open the latest job detail
   - Review `Change Comparison`.
   - Check planned operations, conflicts, and failure diagnostics.
4. Open `/identity-governance/conflicts`
   - Clear open identity conflicts before `apply`.
5. Open `/operations-center/automation`
   - Confirm whether scheduled `apply` is still allowed.
6. If you are preparing a release, open `/config/releases`
   - Publish a fresh configuration snapshot before rollout.

## First Rollout

Use this sequence for a new organization or a newly enabled connector:

1. Complete `/data-sources/connectors` and the relevant `/sync-policies/*` pages
   - Save source connector settings.
   - Save LDAP settings.
   - Confirm sync scope, root OU, disabled-user OU, and naming defaults.
2. Run live checks
   - Source connectivity
   - LDAP connectivity
   - directory / scope preview where applicable
3. Open `/execution-center/dry-run`
   - Run `dry run`.
4. Open the resulting job detail
   - Review `Change Comparison`.
   - Review planned operations and `high-risk` counts.
5. Open `/identity-governance/conflicts`
   - Resolve `multiple_ad_candidates`, `shared_ad_account`, and other open conflicts.
6. If the plan is high-risk, approve the review
   - From the job detail, approve the dry-run plan.
7. Run `apply`
   - Only after conflicts and required approvals are clear.

## Safe Change Rollout

Use this flow for connector, mapping, exception, lifecycle, or naming changes:

1. Publish a configuration snapshot in `/config/releases`.
2. Save the new change in its owning connector, identity-governance, or sync-policy page.
3. Run `dry run` from `/execution-center/dry-run`.
4. Compare the new job against:
   - the previous successful `dry run`
   - the previous `apply`
5. Review:
   - new high-risk changes
   - new conflicts
   - changed users, groups, and OUs
6. Approve and run `apply` only after the diff is understood.

## Same-Account Decisions

Use this flow when a source user may need to bind to an existing AD account:

1. Open the relevant item in `/identity-governance/conflicts`.
2. Open the `Same-Account Decision Guide`.
3. Confirm the candidate AD account:
   - enabled state
   - OU
   - recent login / key attributes
   - current binding or sharing state
4. Review the projected outcome:
   - fields the next sync will update
   - whether a new account would otherwise be created
   - whether conflict risk remains if you do not bind
5. Apply one decision only:
   - bind to the existing AD account
   - keep the conflict unresolved until ownership is clarified
   - use an exception only if this is truly temporary

## Rule Governance

Use `/identity-governance/manual-overrides` and `/identity-governance/exception-rules` as governed policy stores, not as a scratchpad.

Every long-lived binding, override, or exception should have:

- `rule owner`
- `effective reason`
- `next review at`
- optional expiry when the rule is temporary

Review these regularly:

1. Expired rules
2. Rules nearing expiry
3. Rules overdue for review
4. Rules with unexpectedly high hit counts

Avoid carrying temporary conflict workarounds indefinitely. If a rule has stopped being useful, delete it instead of leaving it dormant.

## Lifecycle Operations

Use `/operations-center/lifecycle-queue` as the daily queue for time-based actions.

Work through these sections:

1. `Future Onboarding`
2. `Contractor Expiry`
3. `Offboarding Grace`
4. `Replay Queue`

Preferred actions:

- `approve` when the queued action is correct and should be executed now
- `defer` when the action is valid but the effective time must move
- `skip` when a one-off exception is needed
- `retry` when a replay request should be reissued

## Data Quality Operations

Use `/data-sources/data-quality` after source-side changes, before go-live, and during weekly hygiene reviews.

Watch these indicators:

- missing email
- missing employee ID
- duplicate email
- duplicate employee ID
- department anomalies
- naming-risk users

Recommended cadence:

1. Use `Refresh Source Data` after major source-side changes. This is the only Data Quality action that contacts the source connector.
2. Run the quality scan against the displayed immutable source snapshot. The request and stored result bind the exact source snapshot ID and fingerprint, so an existing snapshot remains scannable while the source platform is offline.
3. Review blocking issues before confirming the snapshot. A confirmed review may be reused only by a newer snapshot with the identical fingerprint.
4. Export repair items for HR or source-system owners.
5. Track whether the total backlog is shrinking over time. A failed refresh is reported separately and does not invalidate the last successful snapshot.

## Automation And Notifications

Use `/operations-center/automation` for schedule time, retry behavior, execution mode, and the unattended Apply safety gate. Use `/operations-center/notifications` for signal selection, channels, webhooks, and failed-delivery evidence.

Minimum recommended settings:

1. Keep `schedule_execution_mode` on `dry_run` until production rollout is stable.
2. In Notifications, enable dry-run failure reminders.
3. In Notifications, enable conflict backlog reminders.
4. Keep the scheduled-apply safety gate enabled.
5. Require:
   - a recent successful `dry run`
   - zero open conflicts
   - approved review when high-risk changes exist

Only move scheduled execution to `apply` after several clean dry-run cycles.

## Release And Rollback

Use `/config/releases` for controlled configuration rollout.

Recommended pattern:

1. Publish a snapshot before every material configuration change.
2. Run `dry run` after the change.
3. If behavior is wrong, roll back to the previous snapshot.
4. Re-run `dry run` immediately after rollback to confirm recovery.

Do not make large production changes without a fresh snapshot and a verified rollback target.

## External Integrations

Use the advanced channel section on `/operations-center/notifications` when an external workflow or dashboard needs access.

Current `v1` support:

- `Job Status API`
- `Conflict API`
- high-risk review approval callback
- outbound webhook subscriptions

Recommended practice:

1. Rotate an organization-scoped bearer token.
2. Store it in your ITSM or workflow secret store.
3. Register only the webhook events you actually consume.
4. Use a per-target shared secret if the receiver validates HMAC signatures.

See `docs/api/external-integrations-v1.md` for the wire contract.

## System Management

Use the owning page instead of editing global and organization settings from the legacy Config form:

- `/system-management/organizations`: tenant definitions and configuration bundles
- `/system-management/administrators`: local administrators and roles
- `/system-management/employee-self-service`: organization-scoped SSPR policy; saving does not reset a real account
- `/system-management/database`: integrity state and manual backup
- `/system-management/branding`: global product appearance
- `/system-management/deployment`: persisted web runtime settings and restart status

Deployment settings are configuration only. If the page reports `Restart required`, use the approved CI/CD deployment workflow. Confirm `/healthz` and `/readyz` after restart before treating the values as active.

All administration timestamps show the browser-local absolute time and a relative time. Hover or inspect the time element for its raw ISO value.

Legacy Phase 7 GET URLs permanently redirect to their canonical routes and preserve query parameters. Legacy POST paths remain available for compatibility, but new automation should use canonical paths.

Authentication handoffs also preserve safe HTTP semantics. Starting SSPR or administrator SSO requires a CSRF-checked POST. Provider callback GET pages only display and automatically submit the secure handoff; OAuth transaction consumption, login-state changes, and audit records occur on POST.

## Escalation Triggers

Pause `apply` and re-evaluate if any of these are true:

- the latest `dry run` added unexpected high-risk changes
- open conflicts increased instead of shrinking
- a temporary exception has expired or is overdue for review
- scheduled apply is blocked by stale dry-run age or pending review
- a release rollback was needed and the next dry run is still not green

## Recommended Weekly Review

Run this once per week per active organization:

1. Review the latest `Job Center` history.
2. Review `Conflict Queue` backlog age.
3. Review governance reminders in `/mappings` and `/exceptions`.
4. Review lifecycle backlog.
5. Review `Data Quality Center` trends.
6. Confirm automation policies and integration subscriptions are still valid.
