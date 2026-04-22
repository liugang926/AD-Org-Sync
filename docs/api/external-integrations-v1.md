# External Integrations API v1

## Scope

`v1` external integrations expose four capabilities:

- `Job Status API`
- `Conflict API`
- high-risk review approval callback
- outbound webhook subscriptions

The management surface for token rotation and subscription maintenance is the web page:

- `/integrations`

## Authentication

All JSON API requests use an organization-scoped bearer token.

Get the token from:

1. open `/integrations`
2. choose `Rotate Token`
3. copy the token immediately from the success message

Request header:

```http
Authorization: Bearer <integration_api_token>
```

If the token is missing or invalid, the API returns:

```json
{
  "ok": false,
  "error": "Invalid or missing integration API token"
}
```

with HTTP `401`.

## Base Path

All endpoints are scoped to an organization:

```text
/api/integrations/orgs/{org_id}
```

Example:

```text
/api/integrations/orgs/default/jobs
```

## Endpoints

### List Jobs

```http
GET /api/integrations/orgs/{org_id}/jobs
```

Query parameters:

- `limit`
  - optional
  - default `20`
  - max `100`
- `status`
  - optional
  - filters by job status such as `COMPLETED` or `FAILED`

Example:

```bash
curl -H "Authorization: Bearer $TOKEN" \
  "http://127.0.0.1:8010/api/integrations/orgs/default/jobs?limit=10"
```

Response shape:

```json
{
  "ok": true,
  "org_id": "default",
  "count": 1,
  "items": [
    {
      "job_id": "job-integration-001",
      "org_id": "default",
      "trigger_type": "manual",
      "execution_mode": "dry_run",
      "status": "COMPLETED",
      "requested_by": "",
      "requested_config_path": "",
      "plan_source_job_id": "",
      "config_snapshot_hash": "",
      "started_at": "2026-04-21T00:00:00+00:00",
      "ended_at": "",
      "planned_operation_count": 0,
      "executed_operation_count": 0,
      "error_count": 0,
      "summary": {
        "planned_operation_count": 4,
        "conflict_count": 1,
        "high_risk_operation_count": 1,
        "review_required": true,
        "plan_fingerprint": "plan-integration-001"
      },
      "review_required": true,
      "review": {
        "job_id": "job-integration-001",
        "status": "pending",
        "high_risk_operation_count": 1,
        "plan_fingerprint": "plan-integration-001",
        "reviewer_username": "",
        "review_notes": "",
        "reviewed_at": "",
        "expires_at": "",
        "created_at": "2026-04-21T00:00:00+00:00"
      }
    }
  ]
}
```

### Get Job Detail

```http
GET /api/integrations/orgs/{org_id}/jobs/{job_id}
```

Example:

```bash
curl -H "Authorization: Bearer $TOKEN" \
  "http://127.0.0.1:8010/api/integrations/orgs/default/jobs/job-integration-001"
```

Response shape:

```json
{
  "ok": true,
  "org_id": "default",
  "item": {
    "...": "same job payload shape as list response"
  }
}
```

If the job does not belong to the requested organization, the API returns HTTP `404`.

### List Conflicts

```http
GET /api/integrations/orgs/{org_id}/conflicts
```

Query parameters:

- `limit`
  - optional
  - default `50`
  - max `200`
- `status`
  - optional
  - default `open`
- `job_id`
  - optional

Example:

```bash
curl -H "Authorization: Bearer $TOKEN" \
  "http://127.0.0.1:8010/api/integrations/orgs/default/conflicts?status=open"
```

Response shape:

```json
{
  "ok": true,
  "org_id": "default",
  "count": 1,
  "items": [
    {
      "id": 1,
      "job_id": "job-integration-001",
      "conflict_type": "multiple_ad_candidates",
      "severity": "warning",
      "status": "open",
      "source_id": "alice",
      "target_key": "identity_binding",
      "message": "needs identity decision",
      "resolution_hint": "",
      "details": {},
      "created_at": "2026-04-21T00:00:00+00:00",
      "resolved_at": ""
    }
  ]
}
```

### Approve High-Risk Review

```http
POST /api/integrations/orgs/{org_id}/reviews/{job_id}/approve
Content-Type: application/json
```

Request body:

```json
{
  "reviewer_username": "itsm-workflow",
  "review_notes": "Approved externally"
}
```

Both fields are optional. If omitted, the API uses:

- `reviewer_username = "integration_api"`
- `review_notes = ""`

Example:

```bash
curl -X POST \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"reviewer_username":"itsm-workflow","review_notes":"Approved externally"}' \
  "http://127.0.0.1:8010/api/integrations/orgs/default/reviews/job-integration-001/approve"
```

Response shape:

```json
{
  "ok": true,
  "org_id": "default",
  "job_id": "job-integration-001",
  "expires_at": "2026-04-21T04:00:00+00:00",
  "replay_request_id": 7,
  "fresh_approval": true,
  "review": {
    "job_id": "job-integration-001",
    "status": "approved",
    "high_risk_operation_count": 1,
    "plan_fingerprint": "plan-integration-001",
    "reviewer_username": "itsm-workflow",
    "review_notes": "Approved externally",
    "reviewed_at": "2026-04-21T00:10:00+00:00",
    "expires_at": "2026-04-21T04:00:00+00:00",
    "created_at": "2026-04-21T00:00:00+00:00"
  }
}
```

Behavior notes:

- If automatic replay is enabled for the organization, first-time approval may create a replay request.
- The callback is idempotent.
  - the first successful approval returns `fresh_approval: true`
  - repeated approvals return `fresh_approval: false`
  - repeated approvals do not create additional replay requests

## Webhook Subscriptions

Webhook subscriptions are managed from `/integrations`.

Each subscription is scoped by:

- `organization`
- `event_type`
- `target_url`

Saving the same `event_type + target_url` pair updates the existing record in place.

## Webhook Events

Current outbound event types:

- `job.completed`
- `job.failed`
- `job.review_required`
- `review.approved`

Each webhook sends a JSON envelope:

```json
{
  "event_type": "job.completed",
  "delivery_id": "f0e1d2c3b4a59687...",
  "occurred_at": "2026-04-21T00:10:00+00:00",
  "payload": {
    "organization": {
      "org_id": "default"
    },
    "job": {
      "...": "serialized job payload"
    }
  }
}
```

HTTP headers:

- `Content-Type: application/json`
- `X-AD-Org-Sync-Event: <event_type>`
- `X-AD-Org-Sync-Delivery: <delivery_id>`
- `X-AD-Org-Sync-Signature: sha256=<hex>`
  - only present when the subscription has a shared secret

## Signature Verification

When a subscription secret is configured, the sender computes:

```text
HMAC-SHA256(secret, raw_request_body)
```

The header value format is:

```text
sha256=<hex_digest>
```

Receivers should verify the signature against the exact raw request body before parsing JSON.

## Webhook Payload Examples

### job.completed

```json
{
  "event_type": "job.completed",
  "delivery_id": "b8e4d5f6c7a89123",
  "occurred_at": "2026-04-21T00:10:00+00:00",
  "payload": {
    "organization": {
      "org_id": "default"
    },
    "job": {
      "job_id": "job-integration-001",
      "execution_mode": "dry_run",
      "status": "COMPLETED",
      "summary": {
        "planned_operation_count": 4,
        "conflict_count": 1,
        "high_risk_operation_count": 1,
        "review_required": true
      },
      "review_required": true,
      "review": {
        "status": "pending"
      }
    }
  }
}
```

### review.approved

```json
{
  "event_type": "review.approved",
  "delivery_id": "bbf31e550b8a4d89",
  "occurred_at": "2026-04-21T00:15:00+00:00",
  "payload": {
    "organization": {
      "org_id": "default"
    },
    "job": {
      "job_id": "job-integration-001",
      "review_required": true
    },
    "review": {
      "job_id": "job-integration-001",
      "status": "approved",
      "reviewer_username": "itsm-workflow",
      "review_notes": "Approved externally"
    },
    "replay_request_id": 7,
    "approved_by": "itsm-workflow"
  }
}
```

## Error Semantics

Common HTTP responses:

- `200`
  - successful read or approval callback
- `400`
  - invalid subscription input or approval request that is structurally invalid
- `401`
  - bearer token missing or invalid
- `404`
  - organization or job not found in the requested scope

## Operational Notes

- The bearer token is organization-scoped and excluded from config bundle export.
- Approval callbacks write audit logs just like web UI approvals.
- Outbound webhook delivery status is visible in `/integrations`.
- `v1` does not yet expose webhook subscription CRUD as JSON APIs; subscription management is UI-driven.
