# V1 Admin Operations Guide

## Scope

This guide is the operator-facing manual for the current `v1` control plane.

It assumes the following capabilities are already available in the deployed build:

- `Dashboard`
- `Config`
- `Advanced Sync`
- `Job Center`
- `Conflict Queue`
- `Identity Overrides`
- `Exception Rules`
- `Config Release Center`
- `Notification And Automation Center`
- `Data Quality Center`
- `Lifecycle Workbench`
- `External Integration Center`

The recommended operating model is still:

1. `dry run`
2. review conflicts and high-risk changes
3. approve if needed
4. `apply`

## Daily Control Loop

Use this lightweight sequence at the start of each admin session:

1. Open `/dashboard`
   - Confirm the selected organization.
   - Review preflight status, active jobs, and the latest warnings.
   - Switch to `Advanced` mode if you need routing, lifecycle, or governance views.
2. Open `/jobs`
   - Check whether the organization is `Ready`, `Needs Attention`, or `Blocked`.
   - Run the next `dry run` if configuration or source data changed.
3. Open the latest job detail
   - Review `Change Comparison`.
   - Check planned operations, conflicts, and failure diagnostics.
4. Open `/conflicts`
   - Clear open identity conflicts before `apply`.
5. Open `/automation-center`
   - Confirm whether scheduled `apply` is still allowed.
6. If you are preparing a release, open `/config/releases`
   - Publish a fresh configuration snapshot before rollout.

## First Rollout

Use this sequence for a new organization or a newly enabled connector:

1. Complete `/config`
   - Save source connector settings.
   - Save LDAP settings.
   - Confirm sync scope, root OU, disabled-user OU, and naming defaults.
2. Run live checks
   - Source connectivity
   - LDAP connectivity
   - directory / scope preview where applicable
3. Open `/jobs`
   - Run `dry run`.
4. Open the resulting job detail
   - Review `Change Comparison`.
   - Review planned operations and `high-risk` counts.
5. Open `/conflicts`
   - Resolve `multiple_ad_candidates`, `shared_ad_account`, and other open conflicts.
6. If the plan is high-risk, approve the review
   - From the job detail, approve the dry-run plan.
7. Run `apply`
   - Only after conflicts and required approvals are clear.

## Safe Change Rollout

Use this flow for connector, mapping, exception, lifecycle, or naming changes:

1. Publish a configuration snapshot in `/config/releases`.
2. Save the new change in `/config`, `/advanced-sync`, `/mappings`, or `/exceptions`.
3. Run `dry run`.
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

1. Open the relevant item in `/conflicts`.
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

Use `/mappings` and `/exceptions` as governed policy stores, not as a scratchpad.

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

Use `/lifecycle` as the daily queue for time-based actions.

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

Use `/data-quality` after source-side changes, before go-live, and during weekly hygiene reviews.

Watch these indicators:

- missing email
- missing employee ID
- duplicate email
- duplicate employee ID
- department anomalies
- naming-risk users

Recommended cadence:

1. Run a snapshot after major connector or mapping changes.
2. Export repair items for HR or source-system owners.
3. Track whether the total backlog is shrinking over time.

## Notification And Automation

Use `/automation-center` to keep unattended execution safe.

Minimum recommended settings:

1. Keep `schedule_execution_mode` on `dry_run` until production rollout is stable.
2. Enable dry-run failure reminders.
3. Enable conflict backlog reminders.
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

Use `/integrations` when an external workflow or dashboard needs access.

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
