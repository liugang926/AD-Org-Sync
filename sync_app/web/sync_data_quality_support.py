from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Optional

from fastapi import Request

from sync_app.core.models import SourceDirectoryUser, UserDepartmentBundle
from sync_app.core.sync_policies import build_template_context
from sync_app.services.runtime_connectors import is_department_in_connector_scope
from sync_app.services.runtime_identity import resolve_target_department


class SyncDataQualitySupportMixin:
    def build_source_data_quality_snapshot(self, request: Request) -> dict[str, Any]:
        config, source_provider = self._get_source_provider(request)
        try:
            department_tree = self._build_department_tree(source_provider.list_departments())
            users_by_id: dict[str, SourceDirectoryUser] = {}
            for department_id in sorted(department_tree):
                for user in source_provider.list_department_users(int(department_id)) or []:
                    self._merge_source_directory_user(users_by_id, user)

            for user in users_by_id.values():
                template_context = build_template_context(user)
                if str(user.name or "").strip() and str(user.email or "").strip() and str(
                    template_context.get("employee_id") or ""
                ).strip():
                    continue
                detail_payload = source_provider.get_user_detail(str(user.source_user_id or "").strip()) or {}
                if detail_payload:
                    user.merge_payload(detail_payload)
        finally:
            self._close_directory_resource(source_provider)

        runtime_context = self._build_runtime_connector_context(
            request,
            config=config,
            department_tree=department_tree,
        )
        connector_specs_by_id = runtime_context["connector_specs_by_id"]
        department_connector_map = runtime_context["department_connector_map"]
        department_scope_root_map = runtime_context["department_scope_root_map"]
        excluded_department_names = runtime_context["excluded_department_names"]
        placement_blocked_department_ids = runtime_context["placement_blocked_department_ids"]
        placement_strategy = runtime_context["user_ou_placement_strategy"]

        def is_department_excluded(dept_info: Optional[Any]) -> bool:
            return (
                not dept_info
                or dept_info.name in excluded_department_names
                or not is_department_in_connector_scope(
                    dept_info,
                    connector_specs_by_id=connector_specs_by_id,
                    department_connector_map=department_connector_map,
                    department_scope_root_map=department_scope_root_map,
                )
            )

        def is_department_blocked_for_placement(dept_info: Optional[Any]) -> bool:
            return is_department_excluded(dept_info) or (
                bool(dept_info) and dept_info.department_id in placement_blocked_department_ids
            )

        missing_email_repair_items: list[dict[str, Any]] = []
        missing_employee_id_repair_items: list[dict[str, Any]] = []
        missing_department_repair_items: list[dict[str, Any]] = []
        placement_gap_repair_items: list[dict[str, Any]] = []
        routing_ambiguity_repair_items: list[dict[str, Any]] = []
        naming_gap_repair_items: list[dict[str, Any]] = []
        duplicate_emails: dict[str, list[SourceDirectoryUser]] = {}
        duplicate_employee_ids: dict[str, list[SourceDirectoryUser]] = {}
        managed_username_groups: dict[tuple[str, str], list[dict[str, str]]] = {}
        connector_counts: dict[tuple[str, str], int] = {}
        users_with_multiple_departments = 0

        for source_user_id in sorted(users_by_id):
            user = users_by_id[source_user_id]
            template_context = build_template_context(user)
            normalized_email = str(user.email or "").strip()
            normalized_employee_id = str(template_context.get("employee_id") or "").strip()
            user_title = self._format_quality_user_title(user)

            if normalized_email:
                duplicate_emails.setdefault(normalized_email.lower(), []).append(user)
            else:
                missing_email_repair_items.append(
                    self._build_quality_repair_item(
                        key="missing_email",
                        label="Users missing work email",
                        severity="warning",
                        title=user_title,
                        detail="No work email was found on the source directory record.",
                        action="Backfill email on the source directory record where it is supposed to exist.",
                        source_user_id=user.source_user_id,
                        display_name=user.name,
                    )
                )

            if normalized_employee_id:
                duplicate_employee_ids.setdefault(normalized_employee_id.lower(), []).append(user)
            else:
                missing_employee_id_repair_items.append(
                    self._build_quality_repair_item(
                        key="missing_employee_id",
                        label="Users missing employee ID",
                        severity="warning",
                        title=user_title,
                        detail="No employee ID was found on the source directory record.",
                        action="Backfill employee ID or switch the naming strategy away from employee-ID-driven rules.",
                        source_user_id=user.source_user_id,
                        display_name=user.name,
                    )
                )

            if len(user.departments) > 1:
                users_with_multiple_departments += 1

            valid_departments = [
                department_tree.get(int(department_id))
                for department_id in user.departments
                if str(department_id).strip().lstrip("-").isdigit()
            ]
            valid_departments = [item for item in valid_departments if item is not None]
            if not valid_departments:
                missing_department_repair_items.append(
                    self._build_quality_repair_item(
                        key="missing_departments",
                        label="Users without valid departments",
                        severity="error",
                        title=user_title,
                        detail="No valid source department membership was found for routing and OU placement.",
                        action="Fix the source department assignment first, then rerun dry run.",
                        source_user_id=user.source_user_id,
                        display_name=user.name,
                    )
                )
                connector_counts[("__unrouted__", "No valid department routing")] = (
                    connector_counts.get(("__unrouted__", "No valid department routing"), 0) + 1
                )
                continue

            connector_candidates: dict[str, str] = {}
            for department in valid_departments:
                connector_id = department_connector_map.get(int(department.department_id), "default")
                connector_spec = connector_specs_by_id.get(connector_id, connector_specs_by_id["default"])
                connector_candidates[connector_id] = str(
                    connector_spec.get("name") or "Default Connector"
                )

            if len(connector_candidates) > 1:
                connector_counts[("__multiple__", "Multiple connector matches")] = (
                    connector_counts.get(("__multiple__", "Multiple connector matches"), 0) + 1
                )
                routing_ambiguity_repair_items.append(
                    self._build_quality_repair_item(
                        key="routing_ambiguity",
                        label="Users matching multiple connectors",
                        severity="error",
                        title=user_title,
                        detail="Matches multiple connector scopes: "
                        + ", ".join(
                            f"{name} [{connector_id}]"
                            for connector_id, name in sorted(connector_candidates.items())
                        ),
                        action="Adjust connector root units or source department ownership so each user resolves to one connector.",
                        source_user_id=user.source_user_id,
                        display_name=user.name,
                        connector_name=", ".join(
                            f"{name} [{connector_id}]"
                            for connector_id, name in sorted(connector_candidates.items())
                        ),
                    )
                )
                continue

            selected_connector_id = next(iter(connector_candidates.keys()), "default")
            selected_connector_spec = connector_specs_by_id.get(
                selected_connector_id,
                connector_specs_by_id["default"],
            )
            selected_connector_name = str(
                selected_connector_spec.get("name") or "Default Connector"
            )
            connector_counts[(selected_connector_id, selected_connector_name)] = (
                connector_counts.get((selected_connector_id, selected_connector_name), 0) + 1
            )

            bundle = UserDepartmentBundle(user=user)
            for department in valid_departments:
                bundle.add_department(department)
            target_department, _placement_reason = resolve_target_department(
                bundle,
                placement_strategy=placement_strategy,
                is_department_excluded=is_department_blocked_for_placement,
                override_department_id=None,
            )
            if target_department is None:
                placement_gap_repair_items.append(
                    self._build_quality_repair_item(
                        key="placement_unresolved",
                        label="Users blocked by placement rules",
                        severity="warning",
                        title=user_title,
                        detail="Current placement rules excluded every source department for this user."
                        f" Effective connector: {selected_connector_name} [{selected_connector_id}].",
                        action="Review placement strategy, exclusion rules, and connector root-unit scope.",
                        source_user_id=user.source_user_id,
                        display_name=user.name,
                        connector_id=selected_connector_id,
                        connector_name=selected_connector_name,
                    )
                )

            preview = self._build_username_preview_from_user(
                user,
                connector_spec=selected_connector_spec,
            )
            naming_gap_detail = self._describe_naming_prerequisite_gap(preview, user)
            if naming_gap_detail:
                naming_gap_repair_items.append(
                    self._build_quality_repair_item(
                        key="naming_prerequisite_gap",
                        label="Users missing naming prerequisites",
                        severity="warning",
                        title=user_title,
                        detail=naming_gap_detail
                        + f" Effective connector: {selected_connector_name} [{selected_connector_id}].",
                        action="Backfill the required fields or switch to a naming rule that fits the available source data.",
                        source_user_id=user.source_user_id,
                        display_name=user.name,
                        connector_id=selected_connector_id,
                        connector_name=selected_connector_name,
                    )
                )

            primary_managed_candidate = next(
                (
                    candidate
                    for candidate in list(preview.get("candidates") or [])
                    if candidate.get("managed")
                ),
                None,
            )
            if primary_managed_candidate:
                normalized_username = str(primary_managed_candidate.get("username") or "").strip().lower()
                if normalized_username:
                    managed_username_groups.setdefault(
                        (selected_connector_id, normalized_username),
                        [],
                    ).append(
                        {
                            "title": user_title,
                            "source_user_id": user.source_user_id,
                            "display_name": user.name,
                            "connector_id": selected_connector_id,
                            "connector_name": selected_connector_name,
                        }
                    )

        duplicate_email_repair_items = [
            self._build_quality_repair_item(
                key="duplicate_email",
                label="Duplicate work emails",
                severity="warning",
                title=email_value,
                detail="Shared by "
                + ", ".join(item.source_user_id for item in matched_users[:5]),
                action="Confirm whether the duplicates are legitimate shared accounts or source-data defects.",
                source_user_ids=[item.source_user_id for item in matched_users],
            )
            for email_value, matched_users in sorted(duplicate_emails.items())
            if len({str(item.source_user_id or "").strip().lower() for item in matched_users}) > 1
        ]
        duplicate_employee_id_repair_items = [
            self._build_quality_repair_item(
                key="duplicate_employee_id",
                label="Duplicate employee IDs",
                severity="warning",
                title=employee_id,
                detail="Shared by "
                + ", ".join(item.source_user_id for item in matched_users[:5]),
                action="Fix the source HR identifiers before relying on employee-ID naming or collision fallbacks.",
                source_user_ids=[item.source_user_id for item in matched_users],
            )
            for employee_id, matched_users in sorted(duplicate_employee_ids.items())
            if len({str(item.source_user_id or "").strip().lower() for item in matched_users}) > 1
        ]
        managed_username_collision_repair_items = [
            self._build_quality_repair_item(
                key="managed_username_collision",
                label="Predicted managed username collisions",
                severity="error",
                title=f"{username} [{matched_users[0]['connector_name']}]",
                detail="Would be generated for "
                + ", ".join(item["title"] for item in matched_users[:5]),
                action="Tune the username strategy or collision policy before running apply.",
                source_user_ids=[str(item.get("source_user_id") or "").strip() for item in matched_users],
                connector_id=connector_id,
                connector_name=str(matched_users[0]["connector_name"] or ""),
            )
            for (connector_id, username), matched_users in sorted(managed_username_groups.items())
            if len({str(item.get('title') or '').strip().lower() for item in matched_users}) > 1
        ]

        missing_department_samples = self._build_quality_samples(missing_department_repair_items)
        routing_ambiguity_samples = self._build_quality_samples(routing_ambiguity_repair_items)
        managed_username_collision_samples = self._build_quality_samples(managed_username_collision_repair_items)
        placement_gap_samples = self._build_quality_samples(placement_gap_repair_items)
        naming_gap_samples = self._build_quality_samples(naming_gap_repair_items)
        missing_email_samples = self._build_quality_samples(missing_email_repair_items)
        missing_employee_id_samples = self._build_quality_samples(missing_employee_id_repair_items)
        duplicate_email_samples = self._build_quality_samples(duplicate_email_repair_items)
        duplicate_employee_id_samples = self._build_quality_samples(duplicate_employee_id_repair_items)

        issues = [
            self._build_quality_issue(
                key="missing_departments",
                label="Users without valid departments",
                severity="error",
                description="These users cannot be routed into the managed OU tree because the source directory does not expose a valid department membership.",
                action="Fix the source department assignment first, then rerun dry run.",
                samples=missing_department_samples,
                count=len(missing_department_repair_items),
            ),
            self._build_quality_issue(
                key="routing_ambiguity",
                label="Users matching multiple connectors",
                severity="error",
                description="These users span more than one connector scope, so runtime cannot choose a single provisioning target.",
                action="Adjust connector root units or source department ownership so each user resolves to one connector.",
                samples=routing_ambiguity_samples,
                count=len(routing_ambiguity_repair_items),
            ),
            self._build_quality_issue(
                key="managed_username_collision",
                label="Predicted managed username collisions",
                severity="error",
                description="Different users would generate the same primary managed AD username inside the same connector.",
                action="Tune the username strategy or collision policy before running apply.",
                samples=managed_username_collision_samples,
                count=len(managed_username_collision_repair_items),
            ),
            self._build_quality_issue(
                key="placement_unresolved",
                label="Users blocked by placement rules",
                severity="warning",
                description="These users have departments, but the current placement policy excludes every candidate branch.",
                action="Review placement strategy, exclusion rules, and connector root-unit scope.",
                samples=placement_gap_samples,
                count=len(placement_gap_repair_items),
            ),
            self._build_quality_issue(
                key="naming_prerequisite_gap",
                label="Users missing naming prerequisites",
                severity="warning",
                description="These source records are missing fields required by the currently selected naming strategy.",
                action="Backfill the required fields or switch to a naming rule that fits the available source data.",
                samples=naming_gap_samples,
                count=len(naming_gap_repair_items),
            ),
            self._build_quality_issue(
                key="missing_email",
                label="Users missing work email",
                severity="warning",
                description="Blank work email makes email-based naming, write-back, and notification workflows harder to operate safely.",
                action="Backfill email on the source directory record where it is supposed to exist.",
                samples=missing_email_samples,
                count=len(missing_email_repair_items),
            ),
            self._build_quality_issue(
                key="missing_employee_id",
                label="Users missing employee ID",
                severity="warning",
                description="Blank employee ID reduces the quality of employee-ID-based naming and same-name collision handling.",
                action="Backfill employee ID or switch the naming strategy away from employee-ID-driven rules.",
                samples=missing_employee_id_samples,
                count=len(missing_employee_id_repair_items),
            ),
            self._build_quality_issue(
                key="duplicate_email",
                label="Duplicate work emails",
                severity="warning",
                description="Multiple source users share the same work email.",
                action="Confirm whether the duplicates are legitimate shared accounts or source-data defects.",
                samples=duplicate_email_samples,
                count=len(duplicate_email_repair_items),
            ),
            self._build_quality_issue(
                key="duplicate_employee_id",
                label="Duplicate employee IDs",
                severity="warning",
                description="Multiple source users share the same employee ID.",
                action="Fix the source HR identifiers before relying on employee-ID naming or collision fallbacks.",
                samples=duplicate_employee_id_samples,
                count=len(duplicate_employee_id_repair_items),
            ),
        ]
        normalized_issues = [issue for issue in issues if issue]
        severity_rank = {"error": 0, "warning": 1, "info": 2, "success": 3}
        normalized_issues.sort(
            key=lambda item: (
                severity_rank.get(str(item.get("severity") or "warning"), 9),
                -int(item.get("count") or 0),
                str(item.get("label") or ""),
            )
        )
        error_issue_count = sum(1 for item in normalized_issues if item["severity"] == "error")
        warning_issue_count = sum(1 for item in normalized_issues if item["severity"] == "warning")
        repair_items = [
            *missing_department_repair_items,
            *routing_ambiguity_repair_items,
            *managed_username_collision_repair_items,
            *placement_gap_repair_items,
            *naming_gap_repair_items,
            *missing_email_repair_items,
            *missing_employee_id_repair_items,
            *duplicate_email_repair_items,
            *duplicate_employee_id_repair_items,
        ]
        high_risk_items = [
            item
            for item in repair_items
            if str(item.get("severity") or "").strip().lower() == "error"
            or str(item.get("key") or "").strip().lower() in {"placement_unresolved", "naming_prerequisite_gap"}
        ]

        return {
            "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "analysis_notes": [
                "Counts reflect unique source users merged across all returned department memberships.",
                "Manual bindings and per-user department overrides are not expanded in this snapshot.",
            ],
            "summary": {
                "total_users": len(users_by_id),
                "department_count": len(department_tree),
                "users_with_multiple_departments": users_with_multiple_departments,
                "users_missing_email": len(missing_email_repair_items),
                "users_missing_employee_id": len(missing_employee_id_repair_items),
                "users_without_departments": len(missing_department_repair_items),
                "placement_unresolved_count": len(placement_gap_repair_items),
                "routing_ambiguity_count": len(routing_ambiguity_repair_items),
                "naming_prerequisite_gap_count": len(naming_gap_repair_items),
                "duplicate_email_count": len(duplicate_email_repair_items),
                "duplicate_employee_id_count": len(duplicate_employee_id_repair_items),
                "managed_username_collision_count": len(managed_username_collision_repair_items),
                "department_anomaly_count": (
                    len(missing_department_repair_items)
                    + len(placement_gap_repair_items)
                    + len(routing_ambiguity_repair_items)
                ),
                "naming_risk_count": (
                    len(naming_gap_repair_items)
                    + len(managed_username_collision_repair_items)
                ),
                "error_issue_count": error_issue_count,
                "warning_issue_count": warning_issue_count,
            },
            "connector_breakdown": [
                {
                    "connector_id": connector_id,
                    "name": connector_name,
                    "user_count": user_count,
                }
                for (connector_id, connector_name), user_count in sorted(
                    connector_counts.items(),
                    key=lambda item: (-item[1], item[0][1], item[0][0]),
                )
            ],
            "issues": normalized_issues,
            "repair_items": repair_items,
            "high_risk_items": high_risk_items,
        }
