---
name: deploy-production-cicd
description: Publish AD Org Sync changes through GitHub pull requests, enforce the repository CI gates, merge only with explicit approval, deploy the verified revision to the Docker production environment through the self-hosted GitHub Actions runner, and verify health or rollback. Use for release preparation, production deployment, CI/CD changes, runner maintenance, deployment failures, rollback checks, or post-deployment validation in this repository.
---

# Deploy Production CI/CD

Apply this repository's production release contract. Treat CI success, deployment success, and independent production validation as three separate gates.

Read [references/production-operations.md](references/production-operations.md) before changing the workflow, runner, Docker topology, production configuration, credentials, or rollback state.

## Preserve Scope

1. Run `git status -sb` and inspect the diff before staging.
2. Preserve unrelated and concurrent user changes. Never use `git add -A` in a mixed worktree.
3. Never commit passwords, tokens, `.env` files, databases, backups, logs, runner credentials, or generated production state.
4. Confirm the intended branch, commit, repository, and production target before any external mutation.

## Validate Before Publishing

Use `.github/workflows/ci.yml` as the source of truth. Run checks proportional to the change locally, then require all GitHub jobs:

- `Quality / Python 3.10`
- `Quality / Python 3.12`
- `windows`
- `container`
- `Wheel / migrations / SBOM`
- `browser-regression`

Do not waive a real failure. If a local test is blocked by a locked workspace artifact, rerun it in an isolated temporary working directory and record why. A pull-request deployment job being skipped is expected because production deploys only on a push to `main`.

## Publish Through a Pull Request

1. Use an `agent/<description>` branch unless an appropriate feature branch already exists.
2. Stage only intended files, commit tersely, and push with tracking.
3. Open a draft pull request into `main` with the change, impact, risk, and validation evidence.
4. Wait for every required check to succeed.
5. Never mark ready or merge without explicit user approval.
6. After approval, mark the PR ready and merge it. Do not delete branches or rewrite history unless requested.

Never push directly to `main` to bypass the pull-request gates.

## Observe Automatic Production Deployment

After merging, locate the CI run for the exact merge SHA and monitor it until terminal state. Require all validation jobs to succeed before `Deploy / Production` starts.

The production job must:

1. Run only for a push to `refs/heads/main`.
2. Use `[self-hosted, linux, x64, production]`.
3. Use the `production` environment and non-cancelling production concurrency.
4. Check out the verified revision.
5. Set the image tag to the full `github.sha`.
6. Invoke `bash scripts/deploy-production.sh`.

Do not report success while the workflow is queued or running. On failure, inspect the failed step and production state; do not rerun blindly.

## Enforce the Deployment Contract

Keep `scripts/deploy-production.sh` responsible for this ordered sequence:

1. Validate the protected production environment file and Compose configuration.
2. Back up the live SQLite database when the web service is already running.
3. Build fresh Docker images with pull enabled.
4. Start the Compose project without cancelling another production deployment.
5. Poll `/readyz` with a bounded timeout.
6. Run `db-check` inside the deployed web container.
7. Write `last_successful_image_tag` only after all checks pass.
8. On failure after rollout begins, restore the previous successful image tag and recheck readiness.

Preserve persistent data, logs, backups, and secrets in named volumes. Do not replace database volumes during an application deployment.

## Protect Credentials

- Never print a GitHub registration token, server password, application password, password hash, session cookie, or secret file content.
- Keep the host administrator password file at mode `0600`.
- Copy it into the Docker secrets volume as UID/GID `10001:10001`, mode `0400`; mount that volume read-only in the web container.
- Keep the configured administrator username, protected host password file, Docker secret, and database credential aligned.
- Back up the database before resetting a production administrator. Verify the password hash indirectly and perform a real CSRF-aware browser or HTTP login without logging the password.
- Rotate credentials disclosed in chat or logs after the immediate task.

## Independently Verify Production

After Actions reports success, verify production directly:

1. Confirm `origin/main` equals the workflow and merge SHA.
2. Confirm the running web image tag equals that full SHA.
3. Confirm the web and Nginx containers are healthy.
4. Require HTTP 200 from `/healthz`, `/readyz`, and `/login`.
5. Require every `/readyz` check to be true.
6. Confirm `last_successful_image_tag` equals the deployed SHA.
7. Confirm the self-hosted runner returns to `online` and not busy.
8. For credential changes, require an authenticated redirect to `/dashboard` and a successful authentication audit entry.

Report the PR, merge SHA, Actions run, image tag, health evidence, runner state, and any residual risk such as HTTP without TLS. Never call the release complete from GitHub status alone.

## Handle Failure Safely

- If CI fails, stop before production and fix through a new commit.
- If deployment fails, first verify whether automatic image rollback completed and whether `/readyz` recovered.
- If database integrity fails, preserve artifacts and backups; do not restore or edit live data without explicit authorization.
- If the runner is offline, keep the existing production service running while repairing the runner service.
- If health checks fail but containers are running, inspect application and Nginx logs before restarting.
- Never use `git reset --hard`, delete production volumes, or remove backups as a recovery shortcut.
