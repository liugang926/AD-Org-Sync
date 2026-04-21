# V1 Go-Live Checklist

## Purpose

Use this checklist before enabling a production organization for real `apply` execution.

It is designed for the current `v1` feature set, including:

- dry-run diff
- same-account decision guide
- governance metadata
- config release and rollback
- lifecycle workbench
- automation center
- data quality center
- external integrations

## 1. Environment Readiness

- Confirm the deployment method is stable.
  - Windows service, local process, or approved host pattern
- Confirm the correct database path is in use.
- Confirm backups are enabled and the backup directory is writable.
- Confirm at least one `super_admin` account can sign in.
- Confirm health endpoints respond:
  - `/healthz`
  - `/readyz`

## 2. Organization And Connector Setup

- Confirm the correct organization is selected.
- Confirm source connector credentials are saved.
- Confirm LDAP settings are saved.
- Confirm scope settings are intentional:
  - source root unit IDs
  - root OU
  - disabled-user OU
  - naming defaults
- If using advanced sync:
  - confirm connector routing
  - confirm department to OU mappings
  - confirm attribute mappings
  - confirm lifecycle policies

## 3. Security And Access

- Confirm admin roles are assigned intentionally.
  - `super_admin`
  - `operator`
  - `auditor`
- Confirm passwords meet the current minimum policy.
- Confirm CSRF/session settings are acceptable for the deployment model.
- Confirm protected AD accounts and protected AD groups are still in place.

## 4. Data Quality Gate

Open `/data-quality` and confirm:

- missing email backlog is understood
- missing employee ID backlog is understood
- duplicate email backlog is understood
- duplicate employee ID backlog is understood
- department anomaly backlog is understood
- naming-risk users are reviewed

Recommended acceptance:

- no critical naming-risk users remain unexplained
- the remaining repair list has a named owner

## 5. Governance Gate

Open `/mappings` and `/exceptions` and confirm:

- temporary rules have an expiry date
- long-lived rules have a `rule owner`
- long-lived rules have an `effective reason`
- review-due rules are either refreshed or removed
- stale workaround rules are removed instead of carried forward

## 6. Release Snapshot Gate

Before the final production rollout:

1. Open `/config/releases`
2. Publish a fresh snapshot
3. Record the snapshot ID that would be used for rollback

Do not go live without a rollback target that exists in the release center.

## 7. Dry-Run Acceptance Gate

Run a fresh `dry run` from `/jobs`.

Review the resulting job and confirm:

- no unexplained increase in planned operations
- no unexplained increase in high-risk operations
- no unresolved high-severity conflicts
- dry-run diff vs previous dry run is understood
- dry-run diff vs previous apply is understood

If the plan includes high-risk actions:

- approve the review intentionally
- capture who approved it and why

## 8. Same-Account Conflict Gate

For any user that may bind to an existing AD account:

1. Open the conflict item in `/conflicts`
2. Open the `Same-Account Decision Guide`
3. Confirm:
   - target AD account state
   - OU and key attributes
   - projected field updates
   - what happens if you do not bind

Do not approve `apply` while the ownership of a shared AD account is still unclear.

## 9. Automation Gate

Open `/automation-center` and confirm:

- dry-run failure notification is configured if operations expect alerts
- conflict backlog threshold is set
- review-pending reminders are enabled if high-risk plans require external coordination
- scheduled-apply safety gate is enabled
- schedule mode is still `dry_run` during validation

Recommended rollout pattern:

1. keep scheduled mode on `dry_run`
2. observe several green cycles
3. only then move scheduled mode to `apply`

## 10. External Integration Gate

If ITSM, workflow, or monitoring integrations are in scope:

- rotate an organization-scoped bearer token in `/integrations`
- store the token in a secret manager
- register only the webhook events that are actually consumed
- test one approval callback end-to-end
- test one webhook receiver end-to-end

Recommended `v1` events to validate:

- `job.completed`
- `job.failed`
- `job.review_required`
- `review.approved`

## 11. Lifecycle Gate

Open `/lifecycle` and confirm:

- no stale replay backlog
- no unexpected offboarding queue entries
- no contractor expiry items that would trigger immediately by mistake
- future onboarding timing rules are understood

## 12. Apply Window Checklist

Immediately before the first production `apply`:

- confirm no active sync job is already running
- confirm the latest successful dry run is recent
- confirm open conflict count is acceptable
- confirm required review approvals are present
- confirm the rollback snapshot ID is recorded
- confirm the change window owner is present

Then:

1. run `apply`
2. watch the job detail until completion
3. review errors, executed operations, and post-run status

## 13. Post-Apply Validation

After the first production `apply`, confirm:

- expected users were created or updated
- no unexpected disables occurred
- expected groups and OU placements landed correctly
- conflict backlog did not unexpectedly grow
- no new high-risk follow-up dry run appeared unexpectedly

## 14. Rollback Drill

If the first production rollout behaves unexpectedly:

1. open `/config/releases`
2. roll back to the pre-change snapshot
3. run a fresh `dry run`
4. confirm the rollback removed the problematic configuration delta

Record:

- target snapshot ID
- rollback snapshot ID
- safety snapshot ID created during rollback

## 15. Hypercare For The First Week

Run this review every day during the first production week:

1. review latest jobs
2. review open conflicts
3. review pending approvals
4. review governance reminders
5. review lifecycle backlog
6. review data quality trend movement
7. review webhook delivery results in `/integrations`

## 16. Recommended Evidence To Capture

Store these artifacts with the rollout record:

- config release snapshot ID before go-live
- latest successful dry-run job ID
- first production apply job ID
- approval record for any high-risk dry run
- exported repair list from data quality if there is open hygiene debt
- webhook or ITSM integration validation evidence

## 17. Minimal Smoke Test Sequence

If you need one short end-to-end pre-go-live drill, run this exact sequence:

1. publish config snapshot
2. run `dry run`
3. inspect diff and conflict queue
4. use the decision guide for any same-account conflict
5. approve the plan if high-risk review is required
6. run `apply`
7. confirm replay queue behavior if approval created replay work
8. perform one rollback from the release center
9. run one more `dry run` after rollback

If all nine steps succeed cleanly, the organization is ready for controlled production rollout.
