# Source Directory and AD Account Mapping Guide

This guide covers the end-to-end administrator flow for selecting users from WeCom, DingTalk, or Feishu and mapping a source field to the Active Directory `sAMAccountName`.

## Identity model

The durable source identity anchor is always:

`organization + source_provider + source_user_id`

Employee ID is not an identity primary key. Employee IDs can be empty, changed, duplicated, or reassigned. Employee ID, email local part, romanized name, and custom scalar fields are used only to generate or match an AD username. The resulting `source_user_id -> ad_username` binding is persisted, and a manual binding always takes precedence over an automatic naming rule.

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

Run the normal application startup or `python -m sync_app.cli db-check`; do not edit old migrations. Existing databases are upgraded in place and migration checksums remain enforced.

## Choose the AD account source

Available strategies are platform user ID, employee ID, email local part, pinyin initials plus employee ID, full pinyin plus employee ID, surname/given-name romanization strategies, a custom template, or a discovered scalar source field.

The selector renders each option as `business label (actual source field name)`. Business labels for discovered fields come from the current snapshot's normalization metadata rather than a UI-side provider field-name table. A raw field is labeled as employee ID, platform user ID, email, display name, position, primary department, status, or provider only when its values feed that normalized field in the current snapshot. Provider-specific or tenant-defined fields retain their provider label (or their raw field name when no label is available). Refresh the source directory after changing provider fields so the catalog and coverage counts are rebuilt.

The preview applies AD account rules before Dry Run:

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

## Manual binding

Use **Identity Overrides** (`/mappings`) to bind a real source user to an existing AD `sAMAccountName`. The page validates that the source user and AD user exist, blocks one AD user from being assigned to multiple source identities, records `source_provider`, and writes audit events. Bulk import uses the same validation. Binding sources distinguish manual, employee-ID, user-ID, email-local-part, and generated decisions.

## API and security boundaries

Authenticated organization-scoped endpoints include:

- `GET /source-directory`
- `POST /source-directory/test`
- `POST /source-directory/refresh`
- `GET /api/source-directory/status`
- `GET /api/source-directory/users`
- `GET /api/source-directory/fields`
- `GET /api/source-directory/preview?source_user_id=...`
- `POST /source-directory/scope`

Writes require the existing `config.write` capability and CSRF validation, and they create audit records without credential payloads. Reads require `config.read` and apply the selected organization on every repository query.

## Troubleshooting

- **Token succeeds but the test fails:** the application cannot read a department and user page. Expand contact API permission/data scope, then republish or reauthorize the application if the platform requires it.
- **No employee IDs:** grant the employee/job-number field permission or configure the provider's extended employee-ID attribute. Use another account source only after reviewing identity policy.
- **Partial department errors:** the refresh completes with warnings only when it still has a safe visible directory. Inspect the warning summary and correct scope before Dry Run.
- **Snapshot stale:** refresh and save the scope again. A stale or changed fingerprint cannot reuse old approval.
- **Apply says review required:** approve the exact latest Dry Run. Any configuration, scope, or snapshot change intentionally invalidates the previous approval.
- **Duplicate account preview:** correct the source data, select a safer field/collision policy, or create a reviewed manual binding. Never bypass the conflict queue by overwriting an AD account.

Automated tests use fake providers and mock HTTP/LDAP clients. Production connectivity must still be verified with real tenant and AD credentials in the deployment environment.
