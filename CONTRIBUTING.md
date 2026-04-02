# Contributing

## Ground Rules

- Keep changes small and reviewable.
- Prefer bug fixes, tests, and documentation updates over broad rewrites.
- Do not commit secrets, runtime logs, local databases, or packaged executables.

## Development Setup

```powershell
python -m venv venv
venv\Scripts\activate
pip install -r requirements.txt
```

## Useful Commands

```powershell
python -m sync_app.cli version
python -m sync_app.cli --help
python -m compileall sync_app
venv\Scripts\python.exe -m PyInstaller --noconfirm --clean WeCom-AD-Sync.spec
```

## Pull Requests

- Describe the operational impact, not just the code change.
- Include validation steps.
- For sync-rule changes, explain rollback behavior.
- For UI changes, attach screenshots when possible.

## Design Expectations

- Keep `sync_app/` as the only source of active implementation.
- Root-level modules are compatibility wrappers.
- New sync rules should be explicit, testable, and documented.
