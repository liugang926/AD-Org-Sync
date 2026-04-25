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

Next implementation slice:

1. Add employee-auth session model under `sync_app/modules/sspr/`.
2. Support source-provider verification, initially WeCom QR/OAuth.
3. Keep employee auth separate from administrator Web sessions.
4. Rate-limit verification and reset attempts by source user and IP.

## Phase 3: Web Adapter

1. Add SSPR routes under Web as a thin adapter.
2. Route handlers call `SSPRService`; they must not call target providers
   directly.
3. Render employee-only forms and success/failure states.
4. Write all reset attempts to `WebAuditLogRepository`.

## Phase 4: Operations

1. Add settings for enablement, minimum password policy, and unlock behavior.
2. Add audit search labels for `sspr.password_reset`.
3. Add admin-visible SSPR status and runbook notes.
