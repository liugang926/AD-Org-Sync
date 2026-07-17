# Phase 7 Operations Center And System Management Migration Contract

## Product objective

Phase 7 closes the administration information architecture by separating daily operations from global or organization-level system settings. Each canonical page owns one business task and one primary CTA.

## Canonical page ownership

| Area | Canonical route | Page responsibility | Primary CTA |
| --- | --- | --- | --- |
| Operations | `/operations-center/lifecycle-queue` | Review time-based onboarding, expiry, offboarding, and replay work | Review actionable queue |
| Operations | `/operations-center/automation` | Configure schedule timing, retries, execution mode, and unattended Apply gate | Save automation policy |
| Operations | `/operations-center/notifications` | Configure notification signals and inspect channels or delivery evidence | Save notification policy |
| Operations | `/operations-center/audit-log` | Search global and selected-organization audit evidence | Search |
| System | `/system-management/organizations` | Manage organization definitions and configuration bundles | Save organization |
| System | `/system-management/administrators` | Manage local administrators and roles | Create user |
| System | `/system-management/employee-self-service` | Configure organization-scoped SSPR policy | Save self-service settings |
| System | `/system-management/database` | Inspect database state and create an operator backup | Create backup |
| System | `/system-management/branding` | Configure global display name, mark, and attribution | Save appearance |
| System | `/system-management/deployment` | Configure global web runtime settings and restart readiness | Save deployment settings |

The personal account page remains available at `/system-management/account` from the top bar. It is not duplicated in formal system navigation.

## Responsibility boundaries

- Automation never edits notification signal selection.
- Notifications never edits schedule or unattended Apply gating.
- Employee self-service never operates a real AD account; it only persists portal policy.
- Deployment settings never restart or deploy the service. The page reports whether persisted and active settings differ and directs the operator to the approved CI/CD workflow.
- Branding and deployment are global. Every other page clearly marks organization or combined audit scope.
- Config preserves moved values in hidden compatibility fields and links to the new owner pages, preventing an old connector-draft save from resetting moved settings.

## URL compatibility

The following exact legacy GET paths return `308 Permanent Redirect`, including their query string:

| Legacy | Canonical |
| --- | --- |
| `/lifecycle` | `/operations-center/lifecycle-queue` |
| `/automation-center` | `/operations-center/automation` |
| `/integrations` | `/operations-center/notifications` |
| `/audit` | `/operations-center/audit-log` |
| `/organizations` | `/system-management/organizations` |
| `/users` | `/system-management/administrators` |
| `/database` | `/system-management/database` |
| `/account` | `/system-management/account` |

Legacy route registrations and legacy POST paths remain available for integrations, bookmarks, and rollback. Canonical forms submit to canonical POST paths. The external integration JSON API under `/api/integrations` does not change.

## Permissions and security

- Existing capabilities remain authoritative: `config.read/write`, `audit.read`, `organizations.manage`, `users.manage`, `database.read/manage`, and `account.manage`.
- Every new setting write is POST-only, validates CSRF, checks capability, and writes an audit record without secrets.
- Organization-scoped settings continue to use the selected organization. Branding and deployment remain global.
- Legacy GET redirects run before page handling and do not write data.
- Organization bundle export GET is read-only; it no longer creates an audit database row.
- The SSPR result GET reads its short-lived receipt and clears the HttpOnly capability cookie without updating the database.
- SSPR OAuth initialization is a CSRF-checked POST. Its legacy GET only returns to the read-only portal entry, and the DingTalk callback GET never creates or consumes an OAuth transaction.
- OIDC start is a CSRF-checked POST. The identity-provider callback GET renders a read-only handoff form; token exchange, session consumption, last-login updates, and audit writes occur only on the CSRF-checked callback POST.
- No new code invokes source providers, target providers, LDAP, or real AD operations.

## Time contract

`time[data-local-time]` elements keep the raw ISO value in `datetime` and `title`. JavaScript renders the user-local absolute time plus an `Intl.RelativeTimeFormat` value and exposes the combined value through `aria-label`. The lightweight SSPR page uses the same contract.

## Test evidence required

- Unit/integration: canonical GET and POST registration, RBAC, CSRF, organization/global scope, audit actions, cross-page setting preservation, query-preserving legacy redirects, and read-only GET behavior.
- Browser: canonical route rendering at 390 and 1440 px, one primary CTA, horizontal overflow, local plus relative time, raw time evidence, legacy redirect destination, keyboard focus, and Chinese copy.
- Repository gates: format/lint, type check, full unit suite, browser regression, packaging/migrations/SBOM, Windows compatibility, and container smoke tests.

## Rollback

This phase has no schema migration. Roll back the application revision to restore the old navigation and templates. Existing settings retain the same keys and scope, legacy handlers remain registered, and no data transformation is required.
