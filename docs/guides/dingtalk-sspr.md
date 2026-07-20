# DingTalk SSPR Administration and Troubleshooting

This guide configures the passwordless DingTalk employee flow that resets a bound AD account without using an AD Org Sync administrator login.

## Security model

The browser obtains a short-lived DingTalk code through `requestAuthCode` and posts it to AD Org Sync. The server exchanges that code for the trusted DingTalk `userId`. Employees cannot submit a trusted `source_user_id`, AD username, connector, provider, or organization.

Authorization requires exactly one enabled binding matching all of:

```text
org_id + source_provider=dingtalk + connector_id + source_user_id
```

The employee session, administrator session, OAuth transaction, CSRF capability, and reset-result receipt are separate. SQLite stores only hashes of employee capabilities. OAuth state and successful reset sessions are one-time; successful password changes consume the session immediately. Account data and passwords are never placed in query strings, logs, audit payloads, or administrator sessions.

## DingTalk application configuration

Use an internal H5 micro-application belonging to the same DingTalk organization as the employees.

1. Set the micro-app homepage to:

   ```text
   https://it-service.tianjizn.com:9443/sspr?corpid=$CORPID$
   ```

2. Configure the safe domain/origin as `it-service.tianjizn.com:9443` wherever the DingTalk console accepts a host and port. Do not omit `:9443`; this environment does not expose the service on the default HTTPS port.
3. Record the fallback callback as:

   ```text
   https://it-service.tianjizn.com:9443/sspr/callback/dingtalk
   ```

4. Grant the basic employee identity permission used by the code exchange (`qyapi_base`). Directory synchronization still needs its separate member/contact read permissions and data scope.
5. Keep the identifiers distinct:

   - CorpId: DingTalk organization identifier, stored in the SSPR CorpId field.
   - ClientId/AppKey: internal application key, stored in the DingTalk source connector AppKey field.
   - AgentId: application agent identifier; it is not CorpId.
   - AppSecret: application secret, stored only through the existing encrypted secret configuration.

The current official references are the [requestAuthCode JSAPI explorer](https://open.dingtalk.com/tools/explorer/jsapi?id=11723) and [server-side userId exchange documentation](https://open.dingtalk.com/document/orgapp-server/obtain-the-userid-of-a-user-by-using-the-log-free).

`requestAuthCode` returns the code to JavaScript; it is not a redirect-based OAuth code flow. `/sspr/callback/dingtalk` is therefore a safe fallback entry page that starts the same JSAPI process. The code and state are posted to `/sspr/auth/dingtalk` and are not accepted in the callback query string.

## AD Org Sync configuration

Open the dedicated authoritative pages for the intended organization:

1. In **Connectors** (`/data-sources/connectors`), select DingTalk as the source provider and save its AppKey, AgentId, and AppSecret.
2. In **Employee Self-Service** (`/system-management/employee-self-service`), set DingTalk CorpId.
3. In **Deployment Settings** (`/system-management/deployment`), set the global public base URL to `https://it-service.tianjizn.com:9443`. Employee Self-Service shows the derived callback URL as a read-only summary and links back to this authoritative field.
4. In **Connectors**, configure and test the AD/LDAPS target. Keep certificate validation enabled.
5. In **Employee Self-Service**, set the SSPR minimum password length, verification TTL, and default unlock behavior.
6. Verify the identity binding page contains an enabled DingTalk-to-AD record with a non-empty AD username and the correct connector.
7. Enable SSPR only after the status panel reports Ready.

The status panel reports the public origin/port, homepage, auth and callback URLs, CorpId uniqueness, AppKey/AppSecret presence, AD target presence, enabled binding count, and latest verification/reset evidence. It never displays a secret.

If the same CorpId is enabled for more than one organization, authentication is rejected. If a verified user has zero or multiple eligible AD bindings across applicable connectors, the employee sees an unbound message and no AD account is disclosed.

## Employee flow

1. The employee opens the app from the DingTalk workbench.
2. `/sspr` redirects to `/sspr/oauth/start` and automatically calls `requestAuthCode`.
3. The server exchanges the one-time code and creates a short-lived employee session.
4. `/sspr/account` rechecks the exact binding, protected-account list, AD existence, enabled state, and lock state.
5. `/sspr/password/reset` validates CSRF, password confirmation, length, configured complexity, identity fragments, rate limits, and the one-time session before calling the AD target provider.
6. The employee sees a PRG success page. Password reset and optional unlock results are reported separately, and the new password is never echoed.

`force_change_at_next_login` is always false for employee SSPR.

## Audit and operations

Search the Audit page for:

- `sspr.oauth.started`
- `sspr.verify`
- `sspr.password_reset`
- `sspr.session.expired`
- `sspr.session.revoked`

Events may contain organization, provider, connector, source user, bound AD username, request IP, correlation ID, sanitized category, and unlock result. They must not contain passwords, AppSecret, access tokens, auth codes, OAuth state, cookies, or plaintext session capabilities.

Before enabling the app in production:

1. Verify `https://it-service.tianjizn.com:9443/healthz`, `/readyz`, and `/login` return 200.
2. Verify `/sspr` does not redirect to the administrator `/login`.
3. Verify `/sspr/oauth/start` and the callback are reachable and an unverified browser cannot open `/sspr/account`.
4. Use a DingTalk JSAPI stub in automated tests. Do not use a real employee account for an unauthorized password reset.
5. If no explicitly authorized test account is available, stop production acceptance at successful DingTalk verification, exact binding resolution, and the pre-reset account page.

## Troubleshooting

### `ERR_CONNECTION_REFUSED`

Confirm the URL includes `https://` and `:9443`. Test from a client outside the production host. Then check the public listener, reverse proxy, firewall/security group, container health, and the configured public base URL. Do not “fix” the symptom by changing the documented URL to port 443 or by exposing the application container port directly.

### Feature disabled or configuration incomplete

Review every missing item in the SSPR status panel. Common causes are an empty CorpId, a duplicate CorpId in another enabled organization, a non-DingTalk source provider, a missing AppSecret, an HTTP public base URL, or an incomplete AD target.

### DingTalk verification fails

- `invalid_credentials`: confirm AppKey and AppSecret belong to the same internal app.
- `invalid_auth_code` or `expired_auth_code`: restart from the DingTalk workbench; codes are short-lived and one-time.
- `permission_denied`: confirm `qyapi_base`, application authorization, and the relevant data scope.
- `organization_mismatch`: confirm the homepage CorpId and the organization mapping; reject duplicate mappings.
- `rate_limited`: wait for `Retry-After` and investigate repeated attempts.
- `network_error`: check DNS, proxy, TLS, and outbound access to DingTalk APIs.
- `invalid_response`: confirm the API is returning a non-empty `userId` and inspect the correlation ID without logging the raw response.

### Employee is unbound

Confirm the binding uses `source_provider=dingtalk`, the trusted DingTalk `userId`, the correct organization and connector, `is_enabled=true`, and a non-empty AD username. A WeCom binding with the same source user string is intentionally not eligible.

### AD rejects the password

The employee receives a sanitized complexity/policy/history message. Check the configured minimum and complexity policy, AD password history, minimum password age, and domain policy. Do not request or log the attempted password.

### Account unlock is incomplete

Password change success and unlock success are independent. If the password changed but unlock failed, do not repeat the password reset automatically. Check AD connectivity and have the IT service desk unlock the account if necessary.

## Rollback

Deploy only the exact approved merge SHA after every required CI job succeeds. If production validation fails, let `scripts/deploy-production.sh` restore the previous successful image tag, then verify the running image, `last_successful_image_tag`, database integrity, health, and readiness. Never delete the SQLite volume, backups, or production data as a rollback shortcut.
