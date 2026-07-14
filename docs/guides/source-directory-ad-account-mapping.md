# Source Directory and AD Account Mapping Guide

This guide covers the end-to-end administrator flow for selecting users from WeCom, DingTalk, or Feishu and mapping a source field to the Active Directory `sAMAccountName`.

## Identity model

The durable source identity anchor and binding lookup boundary is always:

`organization + source_provider + connector + source_user_id`

Employee ID is not an identity primary key. Employee IDs can be empty, changed, duplicated, or reassigned. Employee ID, email local part, romanized name, and custom scalar fields are used only to generate or match an AD username. The resulting binding is persisted inside the complete four-part boundary, and a manual binding always takes precedence over an automatic naming rule. Identical source user IDs from DingTalk, WeCom, or Feishu, or from different organizations/connectors, are independent identities. An ambiguous provider, connector, or multiple exact binding is reported as a conflict instead of selecting the first row.

## Understand the four relationship states

The Web console deliberately avoids the old catch-all term “Mapping Preview.”

| State | Meaning | Evidence |
| --- | --- | --- |
| Field mapping candidate | The username produced by the saved referenced field, naming strategy/template, and normalization rules. It is not a binding. | Current source snapshot and mapping configuration |
| Before synchronization | The persisted binding that existed before the run and its verified or historical AD state. | `user_identity_bindings`, connector boundary, and bounded AD lookup |
| Planned after Dry Run | What the latest matching Dry Run intends to create, update, or bind. It is a proposal and can become stale. | Structured `resolve_identity_binding` evidence and planned operations |
| Applied actual state | The account actually written by an error-free completed Apply, with a successful user operation and a matching enabled persisted binding. | Completed Apply job, operation details, and confirmed binding |

A failed, canceled, or partially failed Apply never qualifies as an applied
actual state. Candidate generation, a planned operation, and a Dry Run alone do
not qualify either. The relationship page shows those distinctions side by side.
The successful AD operation proves the post-Apply account exists; enabled and
locked flags remain unknown unless a bounded AD verification actually reads
them. Use **Verify current-page AD status** for that batch check.

## Configure and verify a provider

1. Open **Config** and select WeCom, DingTalk, or Feishu.
2. Enter the application credentials. Secrets are stored through the existing encrypted secret store and are never returned by source-directory APIs.
3. Open **Source Directory**.
4. Select **Test Connection**. A successful test must read both organization structure and at least one user page; token acquisition alone is not considered success.
5. Select **Refresh Directory**. Refresh runs in the background. A failed refresh records a redacted error and retains the previous successful snapshot.

Credentials:

| Provider | Required values | Runtime token |
| --- | --- | --- |
| WeCom | CorpID, CorpSecret; AgentID when required by the application | WeCom access token |
| DingTalk | AppKey/Client ID, AppSecret/Client Secret; optional Agent ID | DingTalk access token with automatic refresh |
| Feishu | App ID, App Secret; optional tenant key field | `tenant_access_token` with automatic refresh |

The clients retry transient HTTP failures and rate limits. Administrator errors distinguish invalid credentials, missing contact permissions, insufficient data scope, rate limiting, network timeout, partial department failure, and an empty visible directory without including secrets or full sensitive responses.

## Minimum contact permissions

Grant read-only permissions and the smallest data scope that covers the synchronization responsibility area.

- WeCom: allow the self-built application to read departments, department members, and user details for every managed department. Include any extended attributes that contain the employee ID.
- DingTalk: grant organization/department and user read access, including user detail and job-number fields, for every managed department.
- Feishu: use a custom application with `contact.base:readonly`. Add field permissions required by the configured mapping and display columns: user basic information, organization structure, employee information/employee number (`contact:user.employee:readonly`), email, mobile, and user ID. Configure the application's contact data scope; reading children of root department `0` requires an all-member scope. See the official [contact API overview](https://open.feishu.cn/document/server-docs/contact-v3/resources?lang=zh-CN), [scope rules](https://open.feishu.cn/document/server-docs/contact-v3/scope/scope_authority), [field scopes](https://open.feishu.cn/document/server-docs/application-scope/scope-list), and [tenant token endpoint](https://open.feishu.cn/document/server-docs/authentication-management/access-token/tenant_access_token_internal?lang=zh-CN).

Do not grant write permissions unless a separately configured feature needs them.

## Normalized provider fields

Provider adapters normalize data before the sync core sees it.

| Unified field | WeCom | DingTalk | Feishu |
| --- | --- | --- | --- |
| `source_user_id` | `userid` | `userid`/`staffid` | `user_id`, then `open_id` fallback |
| `display_name` | `name` | `name` | `name` |
| `employee_id` | common top-level or `extattr.attrs` aliases | `job_number`, `staff_no`, and aliases | `employee_no` |
| `email` | `email` | `email`, `org_email` | `email`, `enterprise_email` |
| `mobile` | `mobile` | `mobile` | `mobile` |
| `position` | `position` | `title`/`position` | `job_title` |
| departments | `department` | `dept_id_list` and aliases | `department_ids` |
| status | active/status fields | active/status fields | `status.is_activated`, resigned/frozen/unjoined flags |

Employee-ID aliases recognized by every adapter include `employee_id`, `employeeid`, `employee_no`, `employee_number`, `job_number`, `jobnumber`, `staff_no`, `staffno`, `staff_id`, `workcode`, and `work_code`.

The snapshot keeps normalized display fields plus a bounded set of non-sensitive scalar raw fields. Mobile values are stored masked. Nested, secret-like, token-like, password-like, phone-like, oversized, and excess raw fields are not persisted.

## Snapshot behavior

Snapshots are isolated by organization, provider, and connector. The Web page never sends the complete directory to the browser; search, status, department, employee-ID presence, and pagination filters are evaluated by SQLite.

The page shows the latest successful refresh, current refresh status, expiry warning, user count, missing/duplicate employee ID counts, mapping coverage, and normalized username collisions. A new successful snapshot automatically re-fingerprints the saved selection, invalidating approval for an older snapshot. If a refresh fails, the last successful snapshot remains active.

Migration 31 adds:

- `source_directory_snapshots`
- `source_department_snapshots`
- `source_user_snapshots`
- `source_field_catalogs`
- `sync_scope_selections`
- `sync_job_source_scopes`
- provider-aware identity binding uniqueness

Migration 32 extends binding uniqueness to
`(org_id, source_provider, connector_id, source_user_id)`. The relationship
preview itself needs no new migration: it reuses source snapshots,
`user_identity_bindings`, `sync_job_source_scopes`, `planned_operations`, and
structured `sync_operation_logs`. Existing databases therefore retain their
normal migration checksum and backup/restore guarantees.

Run the normal application startup or `python -m sync_app.cli db-check`; do not edit old migrations. Existing databases are upgraded in place and migration checksums remain enforced.

## Choose the AD account source

Available strategies are platform user ID, employee ID, email local part, pinyin initials plus employee ID, full pinyin plus employee ID, surname/given-name romanization strategies, a custom template, or a discovered scalar source field.

The selector renders each option as `business label (actual source field name)`. Business labels for discovered fields come from the current snapshot's normalization metadata rather than a UI-side provider field-name table. A raw field is labeled as employee ID, platform user ID, email, display name, position, primary department, status, or provider only when its values feed that normalized field in the current snapshot. Provider-specific or tenant-defined fields retain their provider label (or their raw field name when no label is available). Refresh the source directory after changing provider fields so the catalog and coverage counts are rebuilt.

The referenced field is the saved source of the field mapping calculation. The
page displays its business label, actual field name, value (masked only for
email, mobile, and sensitive tenant fields), mapping
method, resolved template, normalized candidate, and whether illegal characters
were removed or the 20-character limit caused truncation. Email, mobile, and
sensitive tenant-defined fields remain masked. The preview applies AD account
rules before Dry Run:

- empty mapping fields are reported;
- illegal characters are removed;
- output is limited to 20 characters;
- truncation and normalized case-insensitive collisions are reported;
- duplicate employee IDs are reported;
- protected AD accounts and ambiguous existing AD matches are handled by the existing conflict/protection layer;
- a manual binding overrides all generated candidates.

The configured collision policy still controls safe fallback candidates. Existing AD accounts are never silently overwritten.

## Select a synchronization scope

- `full`: all active users in the snapshot. A successful full run may evaluate missing managed users for offboarding.
- `department`: users in the selected departments and descendants. Global missing-user cleanup is disabled.
- `selected_users`: only checked source users. Global missing-user cleanup is disabled.
- `source_user`: one source user replay. Global missing-user cleanup is disabled.

For every partial scope, the runtime records `partial_scope_offboarding_suppressed`, does not plan disable/move/delete actions for out-of-scope users, does not call global state cleanup, and does not update the full-directory-success marker.

## Dry Run, approval, and Apply

1. Save the scope and mapping.
2. Run **Dry Run** from **Jobs**.
3. Review create, update, reactivate, OU move, skip, conflict, missing-field, duplicate-identifier, existing-account-match, and high-risk operations.
4. Approve the generated review.
5. Run **Apply**.

Scoped plans always require a matching approved Dry Run. The plan fingerprint includes the selected scope, selected department/user IDs, source field, naming strategy/template, source snapshot fingerprint, and normalized planned operations. Approval lookup also includes the configuration fingerprint. Changing configuration, scope, selected users, mapping, or snapshot blocks Apply and requires a new Dry Run.

Dry Run identity resolution is read-only with respect to active bindings. An
automatic resolution is stored as a structured `propose_identity_binding`
planned operation. Apply collects successful per-user confirmations in memory
and commits them atomically only when the whole Apply finishes without errors.
Failed, canceled, and partially failed runs leave no newly confirmed automatic
binding. Manual and previously confirmed bindings remain readable before either
mode starts.

The relationship fingerprint includes organization, provider, connector,
source snapshot, saved selection, referenced field/template result, current
runtime configuration, exact binding signature, and verified AD state when
available. A new snapshot, mapping/template or naming-policy change, connector
change, manual binding change, AD state change, scope change, or organization
configuration change marks older Dry Run evidence stale. Run a fresh Dry Run
before approval or Apply.

## Manual binding

Use **Identity Overrides** (`/mappings`) to bind a real source user to an existing AD `sAMAccountName`. The page validates that the source user and AD user exist, blocks one AD user from being assigned to multiple source identities, records provider and connector, and writes audit events. Bulk import uses the same validation. Binding sources distinguish manual, employee-ID, user-ID, email-local-part, existing-AD-match, and generated decisions. The list retains orphaned bindings when a source user leaves the current snapshot and labels them “Not in current snapshot.” Filters cover source display name/ID, AD username, provider, connector, binding source, enabled state, and Apply status with server-side totals.

## Review the complete relationship

1. Open **Source Directory** and use the **Identity relationship** filter for
   bound, unbound, candidate-only, manual, automatic, disabled, AD-state,
   planned, Apply, or conflict views.
2. Read the grouped columns from left to right: source/mapping, before sync,
   planned after Dry Run, applied result, then difference/risk.
3. Use **Verify current-page AD status** when live evidence is necessary. Only
   server-computed candidates and bindings on the current page are queried.
4. Follow the safe row links to the binding editor, Dry Run, Apply, or conflict
   record. On mobile, the grouped table scrolls horizontally and remains
   keyboard focusable.
5. Open the corresponding job. **Identity Resolution Results** presents the
   structured per-user decision without requiring the raw JSON details panel.

AD verification uses one batch request per connector and deduplicates account
names. It never loads the full AD directory and never accepts an arbitrary AD
username from the browser. If AD is unavailable, source and binding evidence
still renders, the state says temporarily unavailable, the response remains
successful, and LDAP exception text, DN, SID, host details, and credentials are
not returned. The verification time identifies live or historical evidence.

## API and security boundaries

Authenticated organization-scoped endpoints include:

- `GET /source-directory`
- `POST /source-directory/test`
- `POST /source-directory/refresh`
- `GET /api/source-directory/status`
- `GET /api/source-directory/users`
- `GET /api/source-directory/relationships`
- `GET /api/source-directory/fields`
- `GET /api/source-directory/preview?source_user_id=...`
- `GET /api/jobs/{job_id}/identity-resolutions`
- `POST /source-directory/scope`

Relationship APIs return separate `source_user`, `mapping_input`,
`candidate_mapping`, `before_state`, `planned_after_state`,
`applied_after_state`, `difference`, `risks`, and `evidence` objects. They use the
current authenticated organization/provider and server-side connector routing;
client-supplied organization/provider/connector or arbitrary AD enumeration is
not accepted. Pagination and relationship filters are applied on the server,
and the browser never receives the complete source or AD directory.

Writes require the existing `config.write` capability and CSRF validation, and they create audit records without credential payloads. Reads require `config.read` (job evidence uses `jobs.read`) and apply the selected organization on every repository query. GET previews do not create, update, disable, or delete a binding and never write to AD.

## Troubleshooting

- **Token succeeds but the test fails:** the application cannot read a department and user page. Expand contact API permission/data scope, then republish or reauthorize the application if the platform requires it.
- **No employee IDs:** grant the employee/job-number field permission or configure the provider's extended employee-ID attribute. Use another account source only after reviewing identity policy.
- **Partial department errors:** the refresh completes with warnings only when it still has a safe visible directory. Inspect the warning summary and correct scope before Dry Run.
- **Snapshot stale:** refresh and save the scope again. A stale or changed fingerprint cannot reuse old approval.
- **Apply says review required:** approve the exact latest Dry Run. Any configuration, scope, or snapshot change intentionally invalidates the previous approval.
- **Duplicate account preview:** correct the source data, select a safer field/collision policy, or create a reviewed manual binding. Never bypass the conflict queue by overwriting an AD account.
- **DingTalk user has only a candidate:** this is normal before the first Dry Run/Apply. Confirm the referenced field and candidate, run Dry Run, review the proposed binding, then run the approved Apply.
- **Source user has no binding:** filter **Unbound** or **Candidate only**, check for an empty/duplicate referenced field or conflict, and run a new Dry Run after correction.
- **Bound AD account is missing:** verify the current page, confirm the correct connector, then repair or review the binding; do not assume a same-named account is the bound account.
- **Duplicate or ambiguous binding:** use the complete organization/provider/connector/source-user boundary. Disable or remove the incorrect reviewed rule; the service will not silently choose one.
- **Connector conflict:** correct department-root routing or move the binding to the connector selected by the existing runtime router, then run Dry Run again.
- **Dry Run is stale:** refresh/save the source scope and rerun Dry Run. Do not approve or Apply an older plan.
- **Check the latest Apply:** use the Apply link in Source Directory, the latest Apply status in Identity Overrides, or the job’s Identity Resolution Results table. Only `succeeded` is an applied actual state.
- **AD verification is unavailable:** keep using the source/binding evidence, fix target connectivity or certificate/authentication configuration, and retry current-page verification. “Unavailable” is not the same as “missing.”

Automated tests use fake providers and mock HTTP/LDAP clients. Production connectivity must still be verified with real tenant and AD credentials in the deployment environment.
