# Bounded Context Entry Points

This project is a modular monolith. New product capabilities must enter through
an explicit bounded-context package or provider adapter, not through Web routes,
CLI handlers, or sync runtime internals.

## Rules

1. Web routes and CLI handlers call application services only.
2. A new feature area uses `sync_app/modules/<context>/`.
3. A new source system uses `sync_app/providers/source/<provider>/` and registers
   through `sync_app.providers.source.registry`.
4. A new target directory uses `sync_app/providers/target/<provider>.py` and
   registers through `sync_app.providers.target.registry`.
5. `sync_app/core` must not import provider implementations.
6. `sync_app/services` must not import `sync_app.web`.
7. `sync_app/storage` and `sync_app/providers` must not import `sync_app.web`.

## Planned Contexts

### SSPR

SSPR belongs under `sync_app/modules/sspr/`. It should expose a service API for
password reset and account unlock workflows, reuse target provider ports for
directory writes, and write Web audit events through application services. Web
adapters may load organization-scoped `SSPRSettings`, but module services must
remain independent of Web sessions and route state.

### HR Source System

HR master data belongs under `sync_app/providers/source/hr_master/` when
implemented. The adapter should emit canonical `DepartmentNode` and
`SourceDirectoryUser` payloads, then rely on existing attribute mapping and
lifecycle policies instead of writing AD directly.

### Additional Target Providers

Additional targets belong under `sync_app/providers/target/` and must be wired
by `sync_app.providers.target.registry`. Runtime code should depend on
`TargetDirectoryProvider` capabilities, not provider-specific branches.
