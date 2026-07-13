# SSPR Bounded Context Execution Plan

## Goal

Use SSPR as the first real validation of the new bounded-context architecture.
The feature must stay outside sync runtime orchestration and enter through
`sync_app/modules/sspr/`.

## Phase 1: Service Contract

Completed in the first slice:

1. `sync_app/modules/sspr/domain.py` defines request/result models.
2. `sync_app/modules/sspr/service.py` resolves identity bindings, calls target
   provider reset/unlock capabilities, and writes audit logs.
3. `TargetDirectoryProvider` exposes optional `reset_user_password` and
   `unlock_user` capabilities.
4. AD/LDAPS adapter delegates those capabilities to `ADSyncLDAPS`.
5. Tests prove SSPR does not import Web and does not leak passwords into audit
   payloads.

## Phase 2: Employee Verification — Complete

1. Add employee-auth session model under `sync_app/modules/sspr/`.
2. Support source-provider verification through a module-local verifier adapter.
3. Keep employee auth separate from administrator Web sessions by using
   SQLite-backed SSPR verification sessions whose capabilities are stored only
   as hashes.
4. Rate-limit verification and reset attempts through shared, persistent
   organization/provider/user/IP buckets.
5. Allow password reset services to require a verified employee session before
   touching target providers.

6. Consume OAuth state and successful reset sessions atomically, with stale
   claim recovery and bounded cleanup.
7. Resolve identity only from DingTalk's server-side auth-code exchange; the
   browser does not provide a trusted source user ID.

## Phase 3: DingTalk Web Adapter — Complete

1. Add `/sspr`, `/sspr/oauth/start`, `/sspr/auth/dingtalk`, the safe callback,
   account, reset, result, and logout routes under Web as thin adapters.
2. Route handlers call `SSPRService`; they must not call target providers
   directly.
3. Render a bilingual, mobile-first employee-only flow with automatic DingTalk
   JSAPI verification, accessible password feedback, submission state, and PRG
   success handling.
4. Write all reset attempts to `WebAuditLogRepository`.
5. Wire the current DingTalk `requestAuthCode` flow and server-side
   `/topapi/v2/user/getuserinfo` exchange into `SourceProviderSSPRVerifier`.
6. Protect every POST with an independent SSPR CSRF capability and set
   `Secure`, `HttpOnly`, short-lived, path-scoped cookies plus `no-store` and a
   restrictive CSP.
7. Authorize only the server-derived
   `org + provider + connector + source user` binding and block missing,
   ambiguous, disabled, and protected accounts before AD mutation.

## Phase 4: Operations — Complete for Release Candidate

1. Organization settings cover enablement, separate DingTalk CorpId, minimum
   password policy, verification TTL, and default unlock behavior.
2. Audit events cover OAuth start, verification, reset, expiry, and revocation
   without passwords, auth codes, state, cookies, or plaintext session tokens.
3. The admin page reports readiness, unique CorpId mapping, HTTPS/port,
   callback/auth URLs, binding count, and latest verification/reset evidence.
4. The DingTalk SSPR guide and production profile document configuration,
   9443 connectivity, safe acceptance, CI gates, exact-SHA deployment, and
   rollback boundaries.

Release remains gated by a draft PR, all required CI jobs, explicit merge
approval, and exact merge-SHA production verification.
