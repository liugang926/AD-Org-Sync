# Administration Information Architecture Browser Matrix

The final regression matrix covers the canonical administration routes after Phase 7.

## Viewports and interaction

| Coverage | Values |
| --- | --- |
| Desktop widths | 1024, 1366, 1440 px |
| Narrow widths | 390, 768 px |
| Languages | English and Simplified Chinese |
| Input | Pointer and keyboard focus |
| Accessibility | Visible focus, one page primary CTA, table keyboard region, WCAG AA design tokens, status live regions |
| Time | User-local absolute, relative value, raw ISO title |

## Route matrix

| Area | Routes | Required evidence |
| --- | --- | --- |
| Overview | `/overview/control-tower` | Current organization/environment, blocker, next action |
| Data sources | `/data-sources/connectors`, `/data-sources/source-directory`, `/data-sources/snapshots`, `/data-sources/data-quality` | Task-owned CTA, basic tables at eight columns or fewer, advanced evidence disclosure |
| Identity governance | `/identity-governance/identity-matching`, `/identity-governance/binding-reconciliation`, `/identity-governance/conflicts`, `/identity-governance/manual-overrides`, `/identity-governance/exception-rules` | Identity timeline/drawer, focus return, safe binding state |
| Sync policies | `/sync-policies/scope`, `/sync-policies/account-naming`, `/sync-policies/attribute-mappings`, `/sync-policies/department-ou-routing`, `/sync-policies/group-rules`, `/sync-policies/lifecycle`, `/sync-policies/security` | Policy status, one owner, one save CTA |
| Execution | `/execution-center/dry-run`, `/execution-center/plan-review`, `/execution-center/apply`, `/execution-center/jobs` | Dry Run to review to Apply gate, organization/environment/snapshot/impact evidence |
| Operations | `/operations-center/lifecycle-queue`, `/operations-center/automation`, `/operations-center/notifications`, `/operations-center/audit-log` | Queue focus, schedule boundary, notification boundary, audit search |
| System | `/system-management/organizations`, `/system-management/administrators`, `/system-management/employee-self-service`, `/system-management/database`, `/system-management/branding`, `/system-management/deployment` | Scope badges, one primary CTA, local time, restart status |
| Public | `/login`, `/auth/oidc/callback`, `/sspr`, `/sspr/callback/dingtalk` | Narrow layout, secure focus, localized copy, read-only GET handoff, no administration shell leak |

The automated browser suite writes full-page evidence under `test_artifacts/browser` and a responsive JSON record. Phase 7 adds `phase7-<page>-390.png` and `phase7-<page>-1440.png` evidence for all Operations and System pages.
