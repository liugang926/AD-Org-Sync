"""Exercise optional robot secrets through the existing Compose initializer."""
import json
import os
import shutil
import stat
import subprocess
import sys
from pathlib import Path

import pytest
import yaml


ROOT = Path(__file__).resolve().parents[1]
NAMES = ("password_robot_webhook", "password_robot_sign_secret")


def test_robot_settings_use_optional_file_paths_without_reading_secrets(tmp_path):
    env = os.environ.copy()
    env["AD_ORG_SYNC_DATA_DIR"] = str(tmp_path / "data")
    keys = ("DINGTALK_PASSWORD_ROBOT_WEBHOOK_FILE", "DINGTALK_PASSWORD_ROBOT_SIGN_SECRET_FILE")
    for key in keys:
        env.pop(key, None)
    code = (
        "import json; from sync_app import settings; "
        f"print(json.dumps([getattr(settings, key) for key in {keys!r}]))"
    )
    result = subprocess.run([sys.executable, "-c", code], cwd=ROOT, env=env, capture_output=True, text=True, timeout=15)
    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout) == ["", ""]
    paths = [str(tmp_path / "missing-webhook"), str(tmp_path / "missing-signing-secret")]
    env.update(zip(keys, paths))
    result = subprocess.run([sys.executable, "-c", code], cwd=ROOT, env=env, capture_output=True, text=True, timeout=15)
    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout) == paths
    assert not any(Path(path).exists() for path in paths)


def test_robot_secrets_keep_shared_readonly_volume_and_optional_host_bindings():
    compose = yaml.safe_load((ROOT / "docker-compose.yml").read_text(encoding="utf-8"))
    for service in ("web", "worker"):
        app = compose["services"][service]
        assert app["read_only"] is True
        assert "ad_org_sync_secrets:/run/secrets:ro" in app["volumes"]
        assert app["environment"]["DINGTALK_PASSWORD_ROBOT_WEBHOOK_FILE"] == "${DINGTALK_PASSWORD_ROBOT_WEBHOOK_HOST_FILE:+/run/secrets/password_robot_webhook}"
        assert app["environment"]["DINGTALK_PASSWORD_ROBOT_SIGN_SECRET_FILE"] == "${DINGTALK_PASSWORD_ROBOT_SIGN_SECRET_HOST_FILE:+/run/secrets/password_robot_sign_secret}"
        assert "DINGTALK_PASSWORD_ROBOT_WEBHOOK" not in app["environment"]
        assert "DINGTALK_PASSWORD_ROBOT_SIGN_SECRET" not in app["environment"]
    bindings = {
        item["target"]: item for item in compose["services"]["volume-permissions"]["volumes"] if isinstance(item, dict)
    }
    for name, key in zip(NAMES, ("DINGTALK_PASSWORD_ROBOT_WEBHOOK_HOST_FILE", "DINGTALK_PASSWORD_ROBOT_SIGN_SECRET_HOST_FILE")):
        binding = bindings[f"/source/{name}"]
        assert binding["source"] == "${" + key + ":-/dev/null}"
        assert binding["read_only"] is True
        assert binding["bind"]["create_host_path"] is False


@pytest.mark.parametrize("webhook_host_file,sign_host_file", [
    (None, None), ("", ""), ("/tmp/isolated-webhook-file", None),
    (None, "/tmp/isolated-signing-file"), ("/tmp/isolated-webhook-file", "/tmp/isolated-signing-file"),
])
def test_compose_robot_paths_are_empty_unless_host_files_selected(tmp_path, webhook_host_file, sign_host_file):
    docker = shutil.which("docker")
    if not docker:
        pytest.skip("Docker Compose is required to evaluate its interpolation contract")
    available = subprocess.run([docker, "compose", "version"], capture_output=True, text=True, timeout=15)
    if available.returncode != 0:
        pytest.skip("Docker Compose plugin is not installed")
    env_file = tmp_path / "isolated-compose.env"
    env_file.write_text(
        "AD_ORG_SYNC_PUBLIC_BASE_URL=https://example.invalid\n"
        "AD_ORG_SYNC_ADMIN_PASSWORD_FILE=/tmp/isolated-admin-file\n"
        + ("" if webhook_host_file is None else f"DINGTALK_PASSWORD_ROBOT_WEBHOOK_HOST_FILE={webhook_host_file}\n")
        + ("" if sign_host_file is None else f"DINGTALK_PASSWORD_ROBOT_SIGN_SECRET_HOST_FILE={sign_host_file}\n"),
        encoding="utf-8",
    )
    env = os.environ.copy()
    env.pop("DINGTALK_PASSWORD_ROBOT_WEBHOOK_HOST_FILE", None)
    env.pop("DINGTALK_PASSWORD_ROBOT_SIGN_SECRET_HOST_FILE", None)
    result = subprocess.run(
        [docker, "compose", "--env-file", str(env_file), "config", "--format", "json"],
        cwd=ROOT, env=env, capture_output=True, text=True, timeout=15,
    )
    assert result.returncode == 0, result.stderr
    config = json.loads(result.stdout)
    for service in ("web", "worker"):
        assert config["services"][service]["environment"]["DINGTALK_PASSWORD_ROBOT_WEBHOOK_FILE"] == (
            "/run/secrets/password_robot_webhook" if webhook_host_file else ""
        )
        assert config["services"][service]["environment"]["DINGTALK_PASSWORD_ROBOT_SIGN_SECRET_FILE"] == (
            "/run/secrets/password_robot_sign_secret" if sign_host_file else ""
        )


@pytest.mark.parametrize("scenario", ["missing", "both", "webhook_only", "empty", "invalid_webhook", "invalid_sign", "directory"])
def test_optional_robot_secret_initializer_copies_or_removes_only_named_files(tmp_path, scenario):
    bash = "C:/Program Files/Git/bin/bash.exe" if os.name == "nt" else shutil.which("bash")
    if not bash or not Path(bash).exists():
        pytest.skip("Bash is required to exercise the Linux secret initializer")
    for directory in ("source", "secrets", "data", "logs", "bin"):
        (tmp_path / directory).mkdir()
    source, secrets = tmp_path / "source", tmp_path / "secrets"
    (source / "admin_password").write_text("isolated-admin-fixture", encoding="utf-8")
    old = {name: f"old-{name}".encode() for name in (*NAMES, "admin_password", "other_secret")}
    for name, content in old.items():
        (secrets / name).write_bytes(content)
    contents = {name: f"isolated-{name}".encode() for name in NAMES}
    if scenario in {"both", "invalid_webhook", "invalid_sign", "directory"}:
        selected = NAMES
    elif scenario in {"webhook_only", "empty"}:
        selected = NAMES[:1]
    else:
        selected = ()
    for name in selected:
        if scenario == "directory" and name == NAMES[0]:
            (source / name).mkdir()
        else:
            (source / name).write_bytes(b"" if scenario == "empty" else contents[name])
            (source / name).chmod(0o600 if name == NAMES[0] else 0o400)
    if scenario == "invalid_webhook":
        (source / NAMES[0]).chmod(0o644)
    if scenario == "invalid_sign":
        (source / NAMES[1]).chmod(0o640)

    commands = {
        "chown": '#!/usr/bin/env bash\nprintf "chown\\n" >> "$PWD/mutations.log"\n',
        # CI runs without root; keep the real install/copy/mode behavior while
        # recording and removing only the requested UID/GID arguments.
        "install": '''#!/usr/bin/env bash
printf '%s\n' "$*" >> "$PWD/install.log"
args=()
while (( $# )); do
  case "$1" in
    -o|-g) shift 2 ;;
    *) args+=("$1"); shift ;;
  esac
done
exec "$REAL_INSTALL" "${args[@]}"
''',
    }
    if os.name == "nt":
        # NTFS cannot represent these POSIX bits; Linux uses actual stat/modes.
        commands["stat"] = '''#!/usr/bin/env bash
case "$*" in
  *password_robot_webhook*) printf '%s\n' "$WEBHOOK_MODE" ;;
  *password_robot_sign_secret*) printf '%s\n' "$SIGN_MODE" ;;
  *) exit 1 ;;
esac
'''
    for name, content in commands.items():
        path = tmp_path / "bin" / name
        path.write_text(content, encoding="utf-8", newline="\n")
        path.chmod(0o755)
    compose = yaml.safe_load((ROOT / "docker-compose.yml").read_text(encoding="utf-8"))
    shell = compose["services"]["volume-permissions"]["command"]
    assert shell[:2] == ["sh", "-ec"]
    # Compose converts $$ to literal shell $, and the fixed container mount paths
    # become equivalent isolated directories; no application code is substituted.
    command = shell[2].replace("$$", "$")
    for container_path, isolated_path in (("/source", "./source"), ("/run/secrets", "./secrets"), ("/app/logs", "./logs"), ("/data", "./data")):
        command = command.replace(container_path, isolated_path)
    env = os.environ.copy()
    env.update(WEBHOOK_MODE="644" if scenario == "invalid_webhook" else "600", SIGN_MODE="640" if scenario == "invalid_sign" else "400")
    result = subprocess.run(
        [bash, "-ec", 'export REAL_INSTALL="$(command -v install)"; export PATH="$PWD/bin:$PATH"; ' + command],
        cwd=tmp_path, env=env, capture_output=True, text=True, timeout=15,
    )
    if scenario in {"invalid_webhook", "invalid_sign", "directory"}:
        assert result.returncode != 0
        assert "Password robot secret source must" in result.stderr
        assert not (tmp_path / "mutations.log").exists()
        assert not (tmp_path / "install.log").exists()
        assert {name: (secrets / name).read_bytes() for name in old} == old
    else:
        assert result.returncode == 0, result.stderr
        for name in NAMES:
            if name in selected and scenario != "empty":
                assert (secrets / name).read_bytes() == contents[name]
                if os.name != "nt":
                    assert stat.S_IMODE((secrets / name).stat().st_mode) == 0o400
                assert f"-m 0400 -o 10001 -g 10001 ./source/{name} ./secrets/{name}" in (tmp_path / "install.log").read_text(encoding="utf-8")
            else:
                assert not (secrets / name).exists()
        assert (secrets / "admin_password").read_text(encoding="utf-8") == "isolated-admin-fixture"
        assert (secrets / "other_secret").read_bytes() == old["other_secret"]
