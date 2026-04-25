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

## Phase 2: Employee Verification

Completed service-layer slice:

1. Add employee-auth session model under `sync_app/modules/sspr/`.
2. Support source-provider verification through a module-local verifier adapter.
3. Keep employee auth separate from administrator Web sessions by using SSPR
   verification sessions.
4. Rate-limit verification attempts by source user and IP.
5. Allow password reset services to require a verified employee session before
   touching target providers.

The first implementation is intentionally service-only. Web routes and concrete
WeCom QR/OAuth plumbing build on these contracts in Phase 3.

## Phase 3: Web Adapter

Completed Web-adapter slice:

1. Add public `/sspr` routes under Web as a thin adapter.
2. Route handlers call `SSPRVerificationService` and `SSPRService`; they do not
   call target providers directly.
3. Render employee-only verification/reset forms and success/failure states.
4. Write verification and reset attempts to `WebAuditLogRepository`.
5. Keep employee auth separate from administrator Web sessions by passing the
   SSPR verification session through the form flow.
6. Add `/sspr/callback/{provider_id}` as the provider OAuth callback endpoint.
7. Add WeCom and DingTalk source-provider verification adapters that turn an
   OAuth code into a source identity for `SourceProviderSSPRVerifier`.

Remaining provider-specific work:

1. Generate provider-specific QR/OAuth authorization URLs from SSPR settings.
2. Add deployment runbook steps for provider callback URLs and source app
   permissions.

## Phase 4: Operations

Completed operations-settings slice:

1. Add organization-scoped settings for enablement, minimum password policy,
   default unlock behavior, and verification session TTL.
2. Keep `/sspr` public but disabled by default until the organization enables
   the module from `/config`.
3. Enforce the configured minimum password length in `SSPRService` before the
   target provider is called.
4. Surface portal and provider callback URLs on the admin configuration page.

Remaining operations work:

1. Generate provider-specific QR/OAuth authorization URLs from SSPR settings.
2. Add audit search labels for `sspr.password_reset`.
3. Add deployment runbook steps for source app callback URLs and permissions.
