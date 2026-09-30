# Contributing

## Ground Rules

- Keep changes small and reviewable.
- Prefer bug fixes, tests, and documentation updates over broad rewrites.
- Do not commit secrets, runtime logs, local databases, or packaged executables.

## Development Setup

```powershell
python -m venv .venv
.\.venv\Scripts\activate
$env:PIP_CONSTRAINT = (Resolve-Path ./constraints.txt).Path
$env:PIP_BUILD_CONSTRAINT = $env:PIP_CONSTRAINT
python -m pip install --upgrade pip
python -m pip install -e ".[test]"
python -m pip check
```

Run the commands below in that environment so the same constraints also apply to
isolated wheel builds. In a new terminal, set both absolute constraint paths
again. See [dependency updates](README.md#依赖版本与安装) before changing versions.

## Useful Commands

```powershell
python -m sync_app.cli --version
python -m sync_app.cli --help
python -m compileall sync_app
python manage.py check
python manage.py makemigrations --check --dry-run
python -m pytest -q --ignore=tests/test_browser.py
python -m build --wheel
```

## Pull Requests

- Describe the operational impact, not just the code change.
- Include validation steps.
- For sync-rule changes, explain rollback behavior.
- For UI changes, attach screenshots when possible.

## Design Expectations

- Keep `sync_app/` as the only source of active implementation.
- Do not add legacy framework compatibility wrappers or multi-tenant abstractions.
- New sync rules should be explicit, testable, and documented.
