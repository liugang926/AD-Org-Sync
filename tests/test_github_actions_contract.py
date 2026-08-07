from __future__ import annotations

import re
from pathlib import Path


REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
WORKFLOW_ROOT = REPOSITORY_ROOT / ".github" / "workflows"
ACTION_REFERENCE = re.compile(r"^\s*uses:\s*([^\s@]+)@([^\s#]+)", re.MULTILINE)
NODE_24_MINIMUM_MAJOR = {
    "actions/checkout": 7,
    "actions/setup-python": 7,
    "actions/upload-artifact": 7,
    "softprops/action-gh-release": 3,
}
FORBIDDEN_RUNTIME_OVERRIDES = {
    "ACTIONS_ALLOW_USE_UNSECURE_NODE_VERSION",
    "FORCE_JAVASCRIPT_ACTIONS_TO_NODE24",
}


def _workflow_sources() -> dict[Path, str]:
    paths = sorted((*WORKFLOW_ROOT.glob("*.yml"), *WORKFLOW_ROOT.glob("*.yaml")))
    assert paths, "no GitHub Actions workflows were found"
    return {path: path.read_text(encoding="utf-8") for path in paths}


def test_javascript_actions_use_node_24_compatible_majors() -> None:
    observed: set[str] = set()
    violations: list[str] = []

    for path, source in _workflow_sources().items():
        for action, reference in ACTION_REFERENCE.findall(source):
            minimum_major = NODE_24_MINIMUM_MAJOR.get(action)
            if minimum_major is None:
                continue
            observed.add(action)
            match = re.fullmatch(r"v(\d+)(?:\.\d+){0,2}", reference)
            if match is None or int(match.group(1)) < minimum_major:
                violations.append(
                    f"{path.relative_to(REPOSITORY_ROOT)}: {action}@{reference} "
                    f"must use v{minimum_major} or newer"
                )

    assert observed == set(NODE_24_MINIMUM_MAJOR)
    assert not violations, "\n".join(violations)


def test_workflows_do_not_mask_javascript_runtime_deprecations() -> None:
    workflow_text = "\n".join(_workflow_sources().values())
    present = sorted(FORBIDDEN_RUNTIME_OVERRIDES.intersection(workflow_text))
    assert not present, f"remove JavaScript runtime overrides: {', '.join(present)}"
