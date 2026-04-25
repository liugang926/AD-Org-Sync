import ast
from pathlib import Path


def _find_imports(package_root: Path, forbidden_prefixes: tuple[str, ...]) -> list[str]:
    offenders: list[str] = []
    for module_path in package_root.rglob("*.py"):
        module = ast.parse(module_path.read_text(encoding="utf-8"))
        for node in ast.walk(module):
            if isinstance(node, ast.ImportFrom):
                imported_module = node.module or ""
                if imported_module in forbidden_prefixes or any(
                    imported_module.startswith(f"{prefix}.") for prefix in forbidden_prefixes
                ):
                    offenders.append(f"{module_path}:{node.lineno} imports {imported_module}")
            elif isinstance(node, ast.Import):
                for alias in node.names:
                    imported_module = alias.name
                    if imported_module in forbidden_prefixes or any(
                        imported_module.startswith(f"{prefix}.") for prefix in forbidden_prefixes
                    ):
                        offenders.append(f"{module_path}:{node.lineno} imports {imported_module}")
    return offenders


def test_core_services_storage_providers_keep_layer_direction():
    layer_rules = {
        Path("sync_app/core"): ("sync_app.providers", "sync_app.web"),
        Path("sync_app/services"): ("sync_app.web",),
        Path("sync_app/storage"): ("sync_app.web",),
        Path("sync_app/providers"): ("sync_app.web",),
        Path("sync_app/modules"): ("sync_app.web",),
    }

    offenders: list[str] = []
    for package_root, forbidden_prefixes in layer_rules.items():
        offenders.extend(_find_imports(package_root, forbidden_prefixes))

    assert offenders == []


def test_target_provider_factory_lives_in_registry():
    registry_module = ast.parse(Path("sync_app/providers/target/registry.py").read_text(encoding="utf-8"))
    adapter_module = ast.parse(Path("sync_app/providers/target/ad_ldaps.py").read_text(encoding="utf-8"))

    registry_functions = {
        node.name for node in registry_module.body if isinstance(node, ast.FunctionDef)
    }
    adapter_functions = {
        node.name for node in adapter_module.body if isinstance(node, ast.FunctionDef)
    }

    assert "build_target_provider" in registry_functions
    assert "build_target_provider" not in adapter_functions


def test_new_feature_entrypoint_conventions_are_documented():
    doc_path = Path("docs/architecture/bounded-context-entrypoints.md")
    doc = doc_path.read_text(encoding="utf-8")

    assert Path("sync_app/modules/sspr").is_dir()
    for required_term in (
        "sync_app/modules/<context>/",
        "sync_app/providers/source/<provider>/",
        "sync_app.providers.source.registry",
        "sync_app.providers.target.registry",
        "SSPR",
        "HR",
        "TargetDirectoryProvider",
    ):
        assert required_term in doc
