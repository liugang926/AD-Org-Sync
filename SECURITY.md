# Security Policy

## Supported Scope

Security fixes are only guaranteed for the latest version on the default branch.

## Reporting a Vulnerability

- Do not open a public issue for credential leaks, privilege escalation, LDAP injection, or remote execution problems.
- Report privately to the maintainers first.
- Include affected version, deployment mode, reproduction steps, and redacted logs when possible.

## Sensitive Data Handling

- Never commit `config.ini`, SQLite runtime databases, logs, or generated reports.
- Treat WeCom secrets, webhook URLs, LDAP passwords, and generated default passwords as credentials.
- Rotate credentials immediately if they were ever committed or shared in logs.

## Hardening Expectations

- Prefer LDAPS with certificate validation enabled in production.
- Run first with `dry-run` before any production apply.
- Restrict the AD service account to the smallest workable scope.
