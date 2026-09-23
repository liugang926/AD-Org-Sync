"""Run the real deployment script against isolated command substitutes."""
import os
import shutil
import subprocess
from pathlib import Path

import pytest


@pytest.mark.parametrize("scenario", ["success", "legacy_failure", "django_failure", "build_failure"])
def test_deployment_preserves_retirement_and_verified_state(tmp_path, scenario):
    bash = "C:/Program Files/Git/bin/bash.exe" if os.name == "nt" else shutil.which("bash")
    if not bash or not Path(bash).exists():
        pytest.skip("Bash is required to exercise the Linux deployment contract")
    root = Path(__file__).resolve().parents[1]
    (tmp_path / "scripts").mkdir()
    (tmp_path / "bin").mkdir()
    (tmp_path / "state").mkdir()
    (tmp_path / "scripts/deploy-production.sh").write_text((root / "scripts/deploy-production.sh").read_text(), encoding="utf-8", newline="\n")
    (tmp_path / "docker-compose.yml").write_text("services: {}\n")
    (tmp_path / "env").write_text("AD_ORG_SYNC_HTTP_PORT=80\n")
    (tmp_path / "env").chmod(0o600)
    old, new = "a" * 40, "b" * 40
    state = tmp_path / "state"
    (state / "last_successful_image_tag").write_text(old)
    (state / "last_successful_compose.yml").write_text("services: {}\n")
    if scenario == "django_failure":
        (state / "last_successful_django_image_tag").write_text(old)
    commands = {
        "docker": '''#!/usr/bin/env bash
printf '%s|%s\n' "$AD_ORG_SYNC_IMAGE_TAG" "$*" >> "$PWD/commands.log"
if [[ "$*" == *"ps --status running --services"* ]]; then echo web; fi
if [[ "$SCENARIO" == build_failure && "$*" == *"build --pull"* ]]; then exit 1; fi
if [[ "$SCENARIO" == *_failure && "$AD_ORG_SYNC_IMAGE_TAG" == b* && "$*" == *"up -d"* ]]; then exit 1; fi
exit 0
''',
        "curl": "#!/usr/bin/env bash\nexit 0\n",
    }
    # Git Bash does not model POSIX permission bits on NTFS. Linux uses real stat.
    if os.name == "nt":
        commands["stat"] = "#!/usr/bin/env bash\nprintf '600\\n'\n"
    for name, content in commands.items():
        path = tmp_path / "bin" / name
        path.write_text(content, encoding="utf-8", newline="\n")
        path.chmod(0o755)
    env = os.environ.copy()
    env.update(SCENARIO=scenario, AD_ORG_SYNC_IMAGE_TAG=new)
    result = subprocess.run([bash, "-c", 'export PATH="$PWD/bin:$PATH" PRODUCTION_ENV_FILE="$PWD/env" PRODUCTION_STATE_DIR="$PWD/state"; bash scripts/deploy-production.sh'], cwd=tmp_path, env=env, capture_output=True, text=True, timeout=30)
    calls = (tmp_path / "commands.log").read_text()
    assert calls.index("db_backup") < calls.index("build --pull")
    if scenario == "success":
        assert result.returncode == 0, result.stderr
        assert (state / "last_successful_image_tag").read_text().strip() == new
        assert (state / "last_successful_django_image_tag").read_text().strip() == new
        assert "db_check" in calls
    else:
        assert result.returncode != 0
        assert (state / "last_successful_image_tag").read_text() == old
        if scenario == "legacy_failure":
            assert "stop web worker nginx" in calls
            assert "--no-build" not in calls
            assert old + "|" not in calls
        elif scenario == "django_failure":
            assert old + "|" in calls
            assert "up -d --no-build" in calls
        else:
            assert "up -d" not in calls
            assert "stop worker" not in calls
