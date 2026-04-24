# AD Org Sync UI System

This document defines the first stable UI layer for the AD Org Sync control plane. It is intentionally lightweight: the product still uses server-rendered Jinja templates, so the system should make existing pages easier to compose without introducing a frontend build chain.

## Product Tone

AD Org Sync is an operational safety console, not a generic admin CRUD panel. The interface should help admins answer four questions quickly:

1. Can we safely apply changes now?
2. What changed since the last known-good run?
3. What needs a human decision?
4. What will happen if I approve this action?

## Layout Primitives

Use these primitives before adding page-specific CSS:

| Class | Purpose |
| :--- | :--- |
| `.page-stack` | Default vertical page rhythm. |
| `.page-stack.tight` | Denser page rhythm for detail pages. |
| `.grid`, `.grid.cols-2`, `.grid.cols-3` | Balanced responsive columns. |
| `.form-grid` | Responsive form layout with stable field spacing. |
| `.stack-sm`, `.stack-md`, `.stack-row` | Local grouping and horizontal decision rows. |
| `.list-stack` | Repeated cards or rows with predictable spacing. |

## Surface Primitives

| Class | Purpose |
| :--- | :--- |
| `.card` | Primary page surface. It should not animate by default. |
| `.card.is-interactive`, `.card.card--interactive` | Opt-in hover elevation for clickable cards only. |
| `.subcard` | Nested surface inside a card. |
| `.toolbar-card` | Filter/action container with lower visual weight. |
| `.section-header` | Section title plus supporting action or status. |
| `.hero-header` | Page-level title area when metadata/actions sit beside the heading. |

## Data Display

| Class | Purpose |
| :--- | :--- |
| `.metric-grid`, `.metric-card` | Compact KPI groups. |
| `.dense-meta` | Read-only key/value facts. |
| `.table-sm` | Dense operational tables. |
| `.table-empty` | Empty state row inside a table. |
| `.cell-strong`, `.cell-mono`, `.cell-truncate` | Reusable table cell emphasis patterns. |

## Form And Action Rules

Buttons should use semantic variants instead of page-local styling:

| Variant | Use |
| :--- | :--- |
| `.button.primary` | Main forward action. |
| `.button.secondary` | Safe alternative action. |
| `.button.success` | Explicit approval or ready-to-apply action. |
| `.button.danger` | Destructive or high-risk action. |
| `.button.ghost` | Low-emphasis navigation or dismiss action. |
| `.button.small`, `.button.sm` | Dense table and toolbar actions. |

Disabled actions must remain visible but muted. Prefer explaining the blocker near the button instead of hiding the action.

## Interaction Standards

1. Hover elevation is opt-in. Static evidence cards should not move.
2. Dangerous actions need visible context before confirmation.
3. Empty states should explain the next best action.
4. Tables should stay readable before they become feature-rich.
5. Mobile layout should collapse columns before reducing content meaning.

## Current Phase

Phase 0 established the missing CSS primitives used by existing templates. Phase 1 rebuilt the Dashboard into a Control Tower and Jobs into a Run Review workspace. Phase 2 extended the same system to human decision surfaces: Same-Account Wizard, Conflict Queue, Lifecycle Workbench, and a shared high-risk confirmation panel. Phase 3 applies the operating-system shell to configuration, data quality, release, and integration pages.
