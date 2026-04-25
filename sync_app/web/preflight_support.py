from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Optional

from fastapi import FastAPI, Request

from sync_app.core.config import load_sync_config
from sync_app.core.models import AppConfig, OrganizationRecord
from sync_app.providers.source import get_source_provider_schema
from sync_app.services.config_validation import (
    run_config_security_self_check,
    test_ldap_connection,
    test_source_connection,
    validate_config,
)
from sync_app.web.app_state import get_web_repositories, get_web_runtime_state, get_web_services
from sync_app.web.dashboard_state import (
    build_getting_started_data as build_getting_started_view_state,
    count_check_statuses,
    merge_saved_preflight_snapshot as merge_saved_preflight_snapshot_data,
    summarize_check_status,
)
from sync_app.web.request_support import RequestSupport
from sync_app.web.runtime import resolve_web_runtime_settings, web_runtime_requires_restart


class DashboardSupport:
    def __init__(
        self,
        *,
        app: FastAPI,
        config_path: str,
        request_support: RequestSupport,
        test_source_connection: Any = test_source_connection,
        test_ldap_connection: Any = test_ldap_connection,
    ) -> None:
        self.app = app
        self.config_path = config_path
        self.request_support = request_support
        self.test_source_connection = test_source_connection
        self.test_ldap_connection = test_ldap_connection

    def load_config_summary(
        self,
        organization: Optional[OrganizationRecord] = None,
        *,
        config_path_override: Optional[str] = None,
    ) -> tuple[Optional[AppConfig], list[str], list[str]]:
        try:
            if organization is not None:
                repositories = get_web_repositories(self.app)
                config = repositories.org_config_repo.get_app_config(
                    organization.org_id,
                    config_path=config_path_override or organization.config_path or self.config_path,
                )
            else:
                config = load_sync_config(config_path_override or self.config_path)
        except Exception as exc:
            return None, [f"Failed to load configuration: {exc}"], []
        is_valid, errors = validate_config(config)
        warnings = run_config_security_self_check(config)
        return config, ([] if is_valid else errors), warnings

    def _build_preflight_snapshot_from_loaded_data(
        self,
        request: Request,
        *,
        current_org: OrganizationRecord,
        config: Optional[AppConfig],
        validation_errors: list[str],
        security_warnings: list[str],
        include_live: bool = False,
    ) -> dict[str, Any]:
        repositories = get_web_repositories(request)
        recent_jobs = repositories.job_repo.list_recent_job_records(limit=100, org_id=current_org.org_id)
        connector_count = repositories.connector_repo.count_connectors(org_id=current_org.org_id)
        open_conflicts_total = repositories.conflict_repo.list_conflict_records_page(
            limit=1,
            offset=0,
            status="open",
            org_id=current_org.org_id,
        )[1]
        dry_run_completed = any(
            str(job.execution_mode).lower() == "dry_run" and str(job.status).lower() == "success"
            for job in recent_jobs
        )
        apply_completed = any(
            str(job.execution_mode).lower() == "apply" and str(job.status).lower() == "success"
            for job in recent_jobs
        )
        checks: list[dict[str, Any]] = []
        source_provider_name = self.request_support.source_provider_label(config.source_provider if config else "wecom")

        if config and not validation_errors:
            checks.append(
                {
                    "key": "config",
                    "label": "Organization configuration",
                    "status": "success",
                    "detail": "Required {provider} and LDAP settings are complete.",
                    "detail_params": {"provider": source_provider_name},
                    "action_url": "/config",
                }
            )
        else:
            checks.append(
                {
                    "key": "config",
                    "label": "Organization configuration",
                    "status": "error",
                    "detail": validation_errors[0] if validation_errors else "Organization configuration is incomplete.",
                    "action_url": "/config",
                }
            )

        connector_detail = (
            "Organization has {count} dedicated connector(s)."
            if connector_count
            else "No dedicated connectors are configured. The organization will use its primary directory settings."
        )
        checks.append(
            {
                "key": "connectors",
                "label": "Connector routing",
                "status": "success",
                "detail": connector_detail,
                "detail_params": {"count": connector_count} if connector_count else {},
                "action_url": "/advanced-sync",
            }
        )

        breaker_enabled = repositories.settings_repo.get_bool(
            "disable_circuit_breaker_enabled",
            False,
            org_id=current_org.org_id,
        )
        checks.append(
            {
                "key": "circuit_breaker",
                "label": "Safety breaker",
                "status": "success" if breaker_enabled else "warning",
                "detail": (
                    "Disable-user circuit breaker is enabled."
                    if breaker_enabled
                    else "Disable-user circuit breaker is still off. Enable it before unattended production runs."
                ),
                "action_url": "/advanced-sync",
            }
        )

        checks.append(
            {
                "key": "dry_run",
                "label": "First dry run",
                "status": "success" if dry_run_completed else "warning",
                "detail": (
                    "At least one successful dry run has been recorded."
                    if dry_run_completed
                    else "No successful dry run has been recorded yet."
                ),
                "action_url": "/jobs",
            }
        )
        checks.append(
            {
                "key": "conflicts",
                "label": "Open conflict queue",
                "status": "success" if open_conflicts_total == 0 else "warning",
                "detail": (
                    "No unresolved identity conflicts are waiting."
                    if open_conflicts_total == 0
                    else "There are {count} unresolved conflict(s) that still need review."
                ),
                "detail_params": {"count": open_conflicts_total} if open_conflicts_total else {},
                "action_url": "/conflicts",
            }
        )
        checks.append(
            {
                "key": "apply",
                "label": "First apply",
                "status": "success" if apply_completed else "warning",
                "detail": (
                    "At least one successful apply run has been recorded."
                    if apply_completed
                    else "No successful apply run has been recorded yet."
                ),
                "action_url": "/jobs",
            }
        )

        for warning in security_warnings[:2]:
            checks.append(
                {
                    "key": f"security_{len(checks)}",
                    "label": "Security recommendation",
                    "status": "warning",
                    "detail": warning,
                    "action_url": "/config",
                }
            )

        if include_live:
            if (
                config
                and not validation_errors
                and config.source_connector.corpid
                and config.source_connector.corpsecret
            ):
                source_ok, source_message = self.test_source_connection(
                    config.source_connector.corpid,
                    config.source_connector.corpsecret,
                    config.source_connector.agentid,
                    source_provider=config.source_provider,
                )
                checks.append(
                    {
                        "key": "live_source",
                        "label": "Live {provider} connection",
                        "label_params": {"provider": source_provider_name},
                        "status": "success" if source_ok else "error",
                        "detail": source_message,
                        "action_url": "/config",
                    }
                )
            else:
                if config and not get_source_provider_schema(config.source_provider).implemented:
                    live_source_detail = "Skipped because {provider} is not implemented in this build."
                    live_source_detail_params = {"provider": source_provider_name}
                else:
                    live_source_detail = "Skipped because {provider} credentials are incomplete or still invalid."
                    live_source_detail_params = {"provider": source_provider_name}
                checks.append(
                    {
                        "key": "live_source",
                        "label": "Live {provider} connection",
                        "label_params": {"provider": source_provider_name},
                        "status": "warning",
                        "detail": live_source_detail,
                        "detail_params": live_source_detail_params,
                        "action_url": "/config",
                    }
                )
            if config and not validation_errors and config.ldap.server and config.ldap.domain and config.ldap.username and config.ldap.password:
                ldap_ok, ldap_message = self.test_ldap_connection(
                    config.ldap.server,
                    config.ldap.domain,
                    config.ldap.username,
                    config.ldap.password,
                    use_ssl=config.ldap.use_ssl,
                    port=config.ldap.port,
                    validate_cert=config.ldap.validate_cert,
                    ca_cert_path=config.ldap.ca_cert_path,
                )
                checks.append(
                    {
                        "key": "live_ldap",
                        "label": "Live LDAP connection",
                        "status": "success" if ldap_ok else "error",
                        "detail": ldap_message,
                        "action_url": "/config",
                    }
                )
            else:
                checks.append(
                    {
                        "key": "live_ldap",
                        "label": "Live LDAP connection",
                        "status": "warning",
                        "detail": "Skipped because LDAP credentials are incomplete or still invalid.",
                        "action_url": "/config",
                    }
                )

        overall_status = summarize_check_status(checks)
        if str(checks[0].get("status")) == "error":
            next_action_url = "/config"
            next_action_label = "Open Organization Config"
        elif include_live and any(
            str(item.get("key") or "") in {"live_source", "live_wecom", "live_ldap"}
            and str(item.get("status") or "") == "error"
            for item in checks
        ):
            next_action_url = "/config"
            next_action_label = "Fix Connectivity"
        elif not dry_run_completed:
            next_action_url = "/jobs"
            next_action_label = "Run First Dry Run"
        elif open_conflicts_total > 0:
            next_action_url = "/conflicts"
            next_action_label = "Review Conflict Queue"
        elif not apply_completed:
            next_action_url = "/jobs"
            next_action_label = "Run First Apply"
        else:
            next_action_url = "/dashboard"
            next_action_label = "Environment Ready"
        return {
            "org_id": current_org.org_id,
            "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "checks": checks,
            "overall_status": overall_status,
            "status_counts": count_check_statuses(checks),
            "has_live_checks": include_live,
            "next_action_url": next_action_url,
            "next_action_label": next_action_label,
            "dry_run_completed": dry_run_completed,
            "apply_completed": apply_completed,
            "open_conflict_count": open_conflicts_total,
        }

    def build_preflight_snapshot(
        self,
        request: Request,
        *,
        include_live: bool = False,
        current_org: Optional[OrganizationRecord] = None,
        config: Optional[AppConfig] = None,
        validation_errors: Optional[list[str]] = None,
        security_warnings: Optional[list[str]] = None,
    ) -> dict[str, Any]:
        resolved_org = current_org or self.request_support.get_current_org(request)
        resolved_config = config
        resolved_validation_errors = list(validation_errors or [])
        resolved_security_warnings = list(security_warnings or [])
        if resolved_config is None and not validation_errors and not security_warnings:
            resolved_config, resolved_validation_errors, resolved_security_warnings = self.load_config_summary(resolved_org)
        return self._build_preflight_snapshot_from_loaded_data(
            request,
            current_org=resolved_org,
            config=resolved_config,
            validation_errors=resolved_validation_errors,
            security_warnings=resolved_security_warnings,
            include_live=include_live,
        )

    @staticmethod
    def _build_control_tower_blockers(
        *,
        preflight_snapshot: dict[str, Any],
        job_center_summary: dict[str, Any],
        sync_runner_error: str | None,
    ) -> list[dict[str, Any]]:
        blockers: list[dict[str, Any]] = []
        if sync_runner_error:
            blockers.append(
                {
                    "level": "error",
                    "title": "Background runner error",
                    "detail": "Last background execution error: {error}",
                    "detail_params": {"error": sync_runner_error},
                    "action_url": "/jobs",
                    "action_label": "Open Job Center",
                }
            )

        for reason in list(job_center_summary.get("blocked_reasons") or [])[:4]:
            next_url = str(job_center_summary.get("next_action_url") or "/jobs")
            blockers.append(
                {
                    "level": str(job_center_summary.get("overall_status") or "warning"),
                    "title": "Apply gate blocker",
                    "detail": str(reason),
                    "action_url": next_url,
                    "action_label": str(job_center_summary.get("next_action_label") or "Review"),
                }
            )

        existing_details = {str(item.get("detail") or "") for item in blockers}
        for check in list(preflight_snapshot.get("checks") or []):
            status = str(check.get("status") or "")
            if status not in {"error", "warning"}:
                continue
            detail = str(check.get("detail") or "")
            if detail in existing_details:
                continue
            blockers.append(
                {
                    "level": status,
                    "title": str(check.get("label") or "Preflight check"),
                    "title_params": dict(check.get("label_params") or {}),
                    "detail": detail,
                    "detail_params": dict(check.get("detail_params") or {}),
                    "action_url": str(check.get("action_url") or "/dashboard"),
                    "action_label": "Review",
                }
            )
            existing_details.add(detail)
            if len(blockers) >= 6:
                break
        return blockers

    @staticmethod
    def _build_control_tower_timeline(
        *,
        recent_jobs: list[Any],
        active_job: Any | None,
        preflight_snapshot: dict[str, Any],
    ) -> list[dict[str, Any]]:
        timeline: list[dict[str, Any]] = []
        if active_job is not None:
            timeline.append(
                {
                    "level": "info",
                    "title": "Active synchronization job",
                    "detail": str(getattr(active_job, "job_id", "") or "-"),
                    "meta": str(getattr(active_job, "status", "") or ""),
                    "href": f"/jobs/{getattr(active_job, 'job_id', '')}",
                }
            )

        if preflight_snapshot.get("live_ran_at"):
            timeline.append(
                {
                    "level": str(preflight_snapshot.get("overall_status") or "info"),
                    "title": "Live preflight completed",
                    "detail": str(preflight_snapshot.get("live_ran_at") or ""),
                    "meta": str(preflight_snapshot.get("overall_status") or ""),
                    "href": "/dashboard#preflight",
                }
            )

        for job in recent_jobs[:5]:
            mode = str(getattr(job, "execution_mode", "") or "")
            status = str(getattr(job, "status", "") or "")
            timeline.append(
                {
                    "level": (
                        "success"
                        if status.upper() == "COMPLETED"
                        else ("error" if status.upper() == "FAILED" else "warning")
                    ),
                    "title": "Dry run completed" if mode == "dry_run" else "Apply run completed",
                    "detail": str(getattr(job, "job_id", "") or "-"),
                    "meta": status,
                    "href": f"/jobs/{getattr(job, 'job_id', '')}",
                }
            )
            if len(timeline) >= 6:
                break
        return timeline

    def build_dashboard_data(self, request: Request) -> dict[str, Any]:
        current_org = self.request_support.get_current_org(request)
        repositories = get_web_repositories(request)
        runtime_state = get_web_runtime_state(request)
        config, validation_errors, security_warnings = self.load_config_summary(current_org)
        persisted_web_runtime_settings = resolve_web_runtime_settings(repositories.settings_repo)
        web_runtime_settings = dict(runtime_state.web_runtime_settings)
        web_runtime_warnings = list(web_runtime_settings.get("warnings", []))
        if web_runtime_requires_restart(
            runtime_state.startup_persisted_web_runtime_settings,
            persisted_web_runtime_settings,
        ):
            web_runtime_warnings.append(
                "Web deployment settings changed in storage. Restart the web process to apply proxy and cookie updates."
            )
        recent_jobs = repositories.job_repo.list_recent_job_records(limit=10, org_id=current_org.org_id)
        active_job = repositories.job_repo.get_active_job_record(org_id=current_org.org_id)
        db_info = repositories.db_manager.runtime_info()
        enabled_rules = repositories.exclusion_repo.list_enabled_rule_records(org_id=current_org.org_id)
        bindings = repositories.user_binding_repo.list_enabled_binding_records(org_id=current_org.org_id)
        overrides = repositories.department_override_repo.list_override_records(org_id=current_org.org_id)
        exception_rules = repositories.exception_rule_repo.list_enabled_rule_records(org_id=current_org.org_id)
        preflight_snapshot = merge_saved_preflight_snapshot_data(
            request.session.get("_preflight_snapshot"),
            self.build_preflight_snapshot(
                request,
                include_live=False,
                current_org=current_org,
                config=config,
                validation_errors=validation_errors,
                security_warnings=security_warnings,
            ),
        )
        services = get_web_services(request)
        job_center_summary = services.jobs.build_job_center_summary(
            org_id=current_org.org_id,
            preflight_summary=preflight_snapshot,
        )
        open_conflicts_count = int(preflight_snapshot.get("open_conflict_count") or 0)
        control_tower_blockers = self._build_control_tower_blockers(
            preflight_snapshot=preflight_snapshot,
            job_center_summary=job_center_summary,
            sync_runner_error=runtime_state.sync_runner.last_error,
        )
        return {
            "active_job": active_job,
            "recent_jobs": recent_jobs,
            "job_center_summary": job_center_summary,
            "control_tower_blockers": control_tower_blockers,
            "control_tower_timeline": self._build_control_tower_timeline(
                recent_jobs=recent_jobs,
                active_job=active_job,
                preflight_snapshot=preflight_snapshot,
            ),
            "current_org": current_org,
            "current_org_connector_count": repositories.connector_repo.count_connectors(org_id=current_org.org_id),
            "current_org_job_count": repositories.job_repo.count_jobs(org_id=current_org.org_id),
            "enabled_organization_count": len(repositories.organization_repo.list_organization_records(enabled_only=True)),
            "config_public": config.to_public_dict() if config else None,
            "config_validation_errors": validation_errors,
            "config_security_warnings": security_warnings,
            "db_info": db_info,
            "enabled_rule_count": len(enabled_rules),
            "exception_rule_count": len(exception_rules),
            "open_conflicts_count": open_conflicts_count,
            "user_count": repositories.user_repo.count_users(),
            "binding_count": len(bindings),
            "override_count": len(overrides),
            "preflight_summary": preflight_snapshot,
            "getting_started": build_getting_started_view_state(
                current_org_name=current_org.name,
                preflight_snapshot=preflight_snapshot,
                source_provider_name=self.request_support.source_provider_label(config.source_provider if config else "wecom"),
                ui_mode=self.request_support.get_ui_mode(request),
            ),
            "placement_strategy": repositories.settings_repo.get_value(
                "user_ou_placement_strategy",
                "source_primary_department",
                org_id=current_org.org_id,
            ),
            "web_runtime": web_runtime_settings,
            "web_runtime_warnings": web_runtime_warnings,
            "sync_runner_error": runtime_state.sync_runner.last_error,
        }
