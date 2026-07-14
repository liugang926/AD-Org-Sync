from __future__ import annotations

from typing import Any, Callable, Optional

from sync_app.core.models import UserDepartmentBundle
from sync_app.services.runtime_context import SyncContext
from sync_app.services.identity_relationships import (
    build_identity_preview_fingerprint,
    build_runtime_identity_evidence,
)
from sync_app.services.runtime_identity import build_identity_candidates


def _select_available_managed_candidate(
    candidates: list[dict[str, Any]],
    *,
    connector_existing_users: dict[str, Any],
    reserved_usernames: set[str],
    connector_id: str,
    is_protected_ad_account: Callable[[str, str], bool],
) -> Optional[dict[str, Any]]:
    existing_usernames = {
        str(key).strip().lower() for key in connector_existing_users.keys()
    }
    for candidate in candidates:
        if not candidate.get("managed"):
            continue
        username = str(candidate.get("username") or "").strip()
        if not username:
            continue
        lowered = username.lower()
        if lowered in existing_usernames:
            continue
        if lowered in reserved_usernames:
            continue
        if is_protected_ad_account(username, connector_id):
            continue
        return candidate
    return None


def collect_source_user_departments(
    ctx: SyncContext,
) -> dict[str, UserDepartmentBundle]:
    user_departments = ctx.identity.user_departments
    user_departments.clear()
    ctx.working.source_user_ids.clear()

    source_scope = ctx.environment.source_scope
    if source_scope:
        selected_user_ids = set(source_scope.get('selected_source_user_ids') or [])
        selected_department_ids = {str(value) for value in source_scope.get('selected_department_ids') or []}
        allowed_department_ids: set[str] = set()
        if source_scope.get('scope_type') == 'department':
            for department in ctx.repositories.source_directory_repo.list_departments(
                int(source_scope['snapshot_id']), org_id=ctx.organization.org_id
            ):
                path_ids = {str(value) for value in department.get('path_ids') or []}
                if path_ids & selected_department_ids:
                    allowed_department_ids.add(str(department['source_department_id']))
        offset = 0
        selected_rows: list[dict[str, Any]] = []
        while True:
            page = ctx.repositories.source_directory_repo.list_users(
                int(source_scope['snapshot_id']),
                org_id=ctx.organization.org_id,
                provider_id=source_scope['provider_id'],
                source_user_ids=selected_user_ids if source_scope.get('scope_type') in {'selected_users', 'source_user'} else None,
                status='active',
                limit=200,
                offset=offset,
            )
            selected_rows.extend(page['items'])
            offset += len(page['items'])
            if offset >= int(page['total']) or not page['items']:
                break
        for row in selected_rows:
            row_department_ids = {str(value) for value in row.get('department_ids') or []}
            if source_scope.get('scope_type') == 'department' and not (row_department_ids & allowed_department_ids):
                continue
            payload = dict(row.get('raw_payload') or {})
            payload.update(
                {
                    'userid': row['source_user_id'], 'name': row['display_name'],
                    'employee_id': row['employee_id'], 'email': row['email'],
                    'position': row['position'], 'department': [int(value) for value in row_department_ids],
                    'department_names': row.get('department_names') or [],
                    'primary_department_id': row.get('primary_department_id') or None,
                    'account_status': row.get('account_status') or 'active', 'is_active': bool(row.get('is_active')),
                    'provider_id': source_scope['provider_id'],
                }
            )
            user = ctx.environment.source_provider.normalize_user(payload)
            userid = user.userid
            ctx.working.source_user_ids.add(userid)
            bundle = UserDepartmentBundle(user=user)
            for department_id in user.departments:
                department = ctx.environment.dept_tree.get(int(department_id))
                if department:
                    bundle.add_department(department)
                    department.users.append(user)
            user_departments[userid] = bundle
        ctx.sync_stats['total_users'] = len(ctx.working.source_user_ids)
        if ctx.hooks.stats_callback:
            ctx.hooks.stats_callback('total_users', len(ctx.working.source_user_ids))
        return user_departments

    for dept_id, dept_info in ctx.environment.dept_tree.items():
        if ctx.hooks.is_cancelled():
            raise InterruptedError("sync cancelled by user")
        try:
            users = ctx.environment.source_provider.list_department_users(dept_id)
            dept_info.users = users
            for user in users:
                userid = user.userid
                ctx.working.source_user_ids.add(userid)
                if userid not in user_departments:
                    user_departments[userid] = UserDepartmentBundle(user=user)
                else:
                    user_departments[userid].user.merge_payload(user.to_state_payload())
                user_departments[userid].add_department(dept_info)
        except Exception as fetch_error:
            ctx.logger.error(
                f"failed to load users from department {dept_info.name}: {fetch_error}"
            )

    ctx.sync_stats["total_users"] = len(ctx.working.source_user_ids)
    if ctx.hooks.stats_callback:
        ctx.hooks.stats_callback("total_users", len(ctx.working.source_user_ids))
    return user_departments


def resolve_identity_bindings_phase(
    ctx: SyncContext,
    *,
    get_connector_id_for_department: Callable[[Any], str],
    get_connector_spec: Callable[[str], dict[str, Any]],
    get_ad_sync: Callable[[str], Any],
    is_protected_ad_account: Callable[[str, str], bool],
    record_exception_skip: Callable[..., None],
    record_protected_account_skip: Callable[..., None],
) -> None:
    user_departments = ctx.identity.user_departments
    active_user_bindings = ctx.identity.active_user_bindings
    binding_resolution_details = ctx.identity.binding_resolution_details
    binding_records_by_source_user_id = ctx.identity.binding_records_by_source_user_id
    binding_record_candidates_by_source_user_id = (
        ctx.identity.binding_record_candidates_by_source_user_id
    )
    user_connector_id_by_userid = ctx.identity.user_connector_id_by_userid
    disabled_bound_userids = ctx.identity.disabled_bound_userids
    exception_skipped_userids = ctx.identity.exception_skipped_userids
    source_user_detail_cache = ctx.identity.source_user_detail_cache
    existing_users_map_by_connector = ctx.identity.existing_users_map_by_connector
    reserved_managed_usernames_by_connector = (
        ctx.identity.reserved_managed_usernames_by_connector
    )
    current_source_ad_usernames_by_connector = (
        ctx.working.current_source_ad_usernames_by_connector
    )
    enabled_ad_users_by_connector = ctx.working.enabled_ad_users_by_connector
    enabled_ad_users = ctx.working.enabled_ad_users_flat

    active_user_bindings.clear()
    binding_records_by_source_user_id.clear()
    binding_record_candidates_by_source_user_id.clear()
    binding_resolution_details.clear()
    user_connector_id_by_userid.clear()
    disabled_bound_userids.clear()
    exception_skipped_userids.clear()
    source_user_detail_cache.clear()
    existing_users_map_by_connector.clear()
    reserved_managed_usernames_by_connector.clear()
    current_source_ad_usernames_by_connector.clear()
    enabled_ad_users_by_connector.clear()
    enabled_ad_users.clear()

    def get_source_user_detail_cached(
        userid: str, user: Optional[Any] = None
    ) -> dict[str, Any]:
        if userid not in source_user_detail_cache and ctx.environment.source_scope:
            source_user_detail_cache[userid] = user.to_state_payload() if user else {}
        if userid not in source_user_detail_cache:
            try:
                source_user_detail_cache[userid] = (
                    ctx.environment.source_provider.get_user_detail(userid) or {}
                )
            except Exception as detail_error:
                ctx.logger.warning(
                    "failed to load %s user detail for %s: %s",
                    ctx.environment.source_provider_name,
                    userid,
                    detail_error,
                )
                source_user_detail_cache[userid] = {}
        detail_payload = source_user_detail_cache[userid]
        if user and detail_payload:
            user.merge_payload(detail_payload)
        return detail_payload

    source_provider_id = str(
        getattr(ctx.config, "source_provider", "wecom") or "wecom"
    ).strip().lower()
    preloaded_binding_records = (
        ctx.repositories.user_binding_repo.list_binding_records_for_source_identities(
        user_departments.keys(),
        org_id=ctx.organization.org_id,
        source_provider=source_provider_id,
        )
    )
    for record in preloaded_binding_records:
        binding_record_candidates_by_source_user_id.setdefault(
            record.source_user_id, []
        ).append(record)

    identity_candidates_by_userid: dict[str, list[dict[str, str]]] = {}
    identity_candidate_usernames_by_connector: dict[str, set[str]] = {}
    managed_primary_username_counts_by_connector: dict[str, dict[str, int]] = {}
    for userid, bundle in user_departments.items():
        get_source_user_detail_cached(userid, bundle.user)
        connector_candidates = {
            get_connector_id_for_department(department)
            for department in bundle.departments
            if department and department.department_id
        }
        connector_candidates.discard("")
        if not connector_candidates:
            connector_candidates = {"default"}
        if len(connector_candidates) > 1:
            ctx.hooks.record_conflict(
                conflict_type="multiple_connector_candidates",
                source_id=userid,
                target_key="connector_assignment",
                message=(
                    f"Source user {userid} spans multiple connector roots: "
                    + ", ".join(sorted(connector_candidates))
                ),
                resolution_hint="Narrow the department connector roots or move the user into a single managed root",
                details={
                    "userid": userid,
                    "connector_ids": sorted(connector_candidates),
                },
            )
            continue
        connector_id = next(iter(connector_candidates))
        user_connector_id_by_userid[userid] = connector_id
        connector_spec = get_connector_spec(connector_id)
        candidates = build_identity_candidates(
            bundle.user,
            username_strategy=connector_spec.get("username_strategy")
            or "custom_template",
            username_collision_policy=connector_spec.get("username_collision_policy")
            or "append_employee_id",
            username_collision_template=connector_spec.get(
                "username_collision_template"
            )
            or "",
            username_template=connector_spec.get("username_template") or "",
        )
        identity_candidates_by_userid[userid] = candidates
        for candidate in candidates:
            identity_candidate_usernames_by_connector.setdefault(
                connector_id, set()
            ).add(candidate["username"])
        primary_managed_candidate = next(
            (candidate for candidate in candidates if candidate.get("managed")),
            None,
        )
        if primary_managed_candidate:
            managed_primary_username_counts_by_connector.setdefault(connector_id, {})
            primary_username = (
                str(primary_managed_candidate.get("username") or "").strip().lower()
            )
            if primary_username:
                managed_primary_username_counts_by_connector[connector_id][
                    primary_username
                ] = (
                    managed_primary_username_counts_by_connector[connector_id].get(
                        primary_username, 0
                    )
                    + 1
                )

    for record in preloaded_binding_records:
        identity_candidate_usernames_by_connector.setdefault(
            str(record.connector_id or "default"), set()
        ).add(str(record.ad_username or ""))

    for connector_id, usernames in identity_candidate_usernames_by_connector.items():
        existing_users_map_by_connector[connector_id] = get_ad_sync(
            connector_id
        ).get_users_batch(sorted(usernames))
    pending_auto_bindings: dict[str, dict[str, Any]] = {}

    for userid in sorted(user_departments.keys()):
        if ctx.hooks.has_exception_rule("skip_user_sync", userid):
            exception_skipped_userids.add(userid)
            record_exception_skip(
                stage_name="plan",
                object_type="user",
                operation_type="user_sync",
                exception_rule_type="skip_user_sync",
                match_value=userid,
                reason=f"skip user {userid}: matched exception rule skip_user_sync",
                source_id=userid,
                details={"userid": userid},
            )
            continue

        connector_id = user_connector_id_by_userid.get(userid, "default")
        binding_candidates = binding_record_candidates_by_source_user_id.get(userid, [])
        exact_binding_candidates = [
            item for item in binding_candidates if item.connector_id == connector_id
        ]
        conflict_identity_evidence: dict[str, Any] = {}
        if len(exact_binding_candidates) > 1 or (
            not exact_binding_candidates and binding_candidates
        ):
            conflict_identity_evidence = build_runtime_identity_evidence(
                user=user_departments[userid].user,
                org_id=ctx.organization.org_id,
                source_provider=source_provider_id,
                connector_id=connector_id,
                connector_spec=get_connector_spec(connector_id),
                source_scope=ctx.environment.source_scope,
                config_fingerprint=ctx.config_hash,
                binding_records=binding_candidates,
                before_ad_state={
                    "status": "not_checked",
                    "exists": None,
                    "enabled": None,
                    "locked": None,
                    "protected": False,
                },
            )
        if len(exact_binding_candidates) > 1:
            conflict_message = (
                f"Source user {userid} has multiple persisted identity bindings in the current connector boundary"
            )
            ctx.hooks.record_conflict(
                conflict_type="multiple_identity_bindings",
                source_id=userid,
                target_key="identity_binding",
                message=conflict_message,
                resolution_hint="Keep exactly one provider and connector binding for this source identity",
                details={
                    "source_provider": source_provider_id,
                    "connector_id": connector_id,
                    "binding_connectors": sorted(
                        {str(item.connector_id or "default") for item in exact_binding_candidates}
                    ),
                },
            )
            ctx.hooks.record_operation(
                stage_name="plan",
                object_type="user_binding",
                operation_type="resolve_identity_binding",
                status="conflict",
                message=conflict_message,
                source_id=userid,
                rule_source="persisted_binding_lookup",
                reason_code="multiple_identity_bindings",
                details={
                    "source_provider": source_provider_id,
                    "connector_id": connector_id,
                    "binding_count": len(exact_binding_candidates),
                    "binding_connectors": sorted(
                        {str(item.connector_id or "default") for item in exact_binding_candidates}
                    ),
                    **conflict_identity_evidence,
                },
            )
            continue
        if not exact_binding_candidates and binding_candidates:
            conflict_message = (
                f"Source user {userid} has a persisted identity binding under a different connector"
            )
            binding_connectors = sorted(
                {str(item.connector_id or "default") for item in binding_candidates}
            )
            ctx.hooks.record_conflict(
                conflict_type="connector_migration_required",
                source_id=userid,
                target_key="identity_binding",
                message=conflict_message,
                resolution_hint="Review and migrate the binding to the resolved connector before synchronization",
                details={
                    "source_provider": source_provider_id,
                    "resolved_connector_id": connector_id,
                    "binding_connectors": binding_connectors,
                },
            )
            ctx.hooks.record_operation(
                stage_name="plan",
                object_type="user_binding",
                operation_type="resolve_identity_binding",
                status="conflict",
                message=conflict_message,
                source_id=userid,
                rule_source="persisted_binding_lookup",
                reason_code="connector_migration_required",
                details={
                    "source_provider": source_provider_id,
                    "connector_id": connector_id,
                    "binding_connectors": binding_connectors,
                    **conflict_identity_evidence,
                },
            )
            continue
        binding_record = (
            exact_binding_candidates[0]
            if len(exact_binding_candidates) == 1
            else None
        )
        if binding_record:
            binding_records_by_source_user_id[userid] = binding_record
        connector_existing_users = existing_users_map_by_connector.get(connector_id, {})
        before_username = str(binding_record.ad_username if binding_record else "")
        before_ad_state = {
            "status": (
                "exists"
                if before_username and before_username in connector_existing_users
                else ("missing" if before_username else "not_checked")
            ),
            "exists": (
                before_username in connector_existing_users if before_username else None
            ),
            "enabled": None,
            "locked": None,
            "protected": bool(
                before_username
                and is_protected_ad_account(before_username, connector_id)
            ),
        }
        identity_evidence = build_runtime_identity_evidence(
            user=user_departments[userid].user,
            org_id=ctx.organization.org_id,
            source_provider=source_provider_id,
            connector_id=connector_id,
            connector_spec=get_connector_spec(connector_id),
            source_scope=ctx.environment.source_scope,
            config_fingerprint=ctx.config_hash,
            binding_records=binding_candidates,
            before_ad_state=before_ad_state,
        )
        if binding_record:
            binding_connector_id = binding_record.connector_id or connector_id
            if is_protected_ad_account(
                binding_record.ad_username, binding_connector_id
            ):
                record_protected_account_skip(
                    stage_name="plan",
                    object_type="user_binding",
                    operation_type="resolve_identity_binding",
                    connector_id=binding_connector_id,
                    ad_username=binding_record.ad_username,
                    source_id=userid,
                    details={
                        "userid": userid,
                        "binding_source": binding_record.source,
                    },
                )
                continue
            if not binding_record.is_enabled:
                disabled_bound_userids.add(userid)
                ctx.hooks.record_event(
                    "INFO",
                    "user_binding_disabled",
                    f"skip user {userid}: user identity binding is disabled",
                    stage_name="plan",
                )
                ctx.hooks.record_operation(
                    stage_name="plan",
                    object_type="user_binding",
                    operation_type="resolve_identity_binding",
                    status="skipped",
                    message=f"skip user {userid}: user identity binding is disabled",
                    source_id=userid,
                    target_id=binding_record.ad_username,
                    rule_source="disabled_binding",
                    reason_code="binding_disabled",
                    details={
                        "userid": userid,
                        "ad_username": binding_record.ad_username,
                        **identity_evidence,
                    },
                )
                continue

            if (
                ctx.policy_settings.connector_routing_enabled
                and binding_record.connector_id
                and binding_record.connector_id != connector_id
            ):
                conflict_message = (
                    f"Source user {userid} moved from connector {binding_record.connector_id} "
                    f"to {connector_id} and requires migration review"
                )
                ctx.hooks.record_conflict(
                    conflict_type="connector_migration_required",
                    source_id=userid,
                    target_key=f"{binding_record.connector_id}->{connector_id}",
                    message=conflict_message,
                    resolution_hint="Review cross-domain migration, then update the manual binding connector or rebind the user",
                    details={
                        "userid": userid,
                        "existing_connector_id": binding_record.connector_id,
                        "target_connector_id": connector_id,
                        "ad_username": binding_record.ad_username,
                    },
                )
                ctx.hooks.record_operation(
                    stage_name="plan",
                    object_type="user_binding",
                    operation_type="resolve_identity_binding",
                    status="conflict",
                    message=conflict_message,
                    source_id=userid,
                    target_id=binding_record.ad_username,
                    rule_source="connector_routing",
                    reason_code="connector_migration_required",
                    details={
                        "existing_connector_id": binding_record.connector_id,
                        "target_connector_id": connector_id,
                        "ad_username": binding_record.ad_username,
                    },
                )
                continue

            binding_source = (
                "manual_binding"
                if binding_record.source == "manual"
                else "existing_binding"
            )
            active_user_bindings[userid] = binding_record.ad_username
            binding_resolution_details[userid] = {
                "source": binding_source,
                "ad_username": binding_record.ad_username,
                "connector_id": binding_connector_id,
                "rule_hits": [binding_source],
                "explanation": "Using the persisted identity binding",
                "binding_record_source": binding_record.source,
                "is_manual": binding_record.source == "manual",
                "binding_was_persisted": True,
                "before_state": {
                    "bound_ad_username": binding_record.ad_username,
                    "binding_source": binding_record.source,
                    "binding_enabled": binding_record.is_enabled,
                    "connector_id": binding_connector_id,
                    "ad_account_state": before_ad_state,
                },
                **identity_evidence,
            }
            current_source_ad_usernames_by_connector.setdefault(
                binding_connector_id,
                set(),
            ).add(binding_record.ad_username)
            reserved_managed_usernames_by_connector.setdefault(
                binding_connector_id,
                set(),
            ).add(str(binding_record.ad_username).strip().lower())
            ctx.hooks.record_operation(
                stage_name="plan",
                object_type="user_binding",
                operation_type="resolve_identity_binding",
                status="selected",
                message=f"resolved {userid} -> {binding_record.ad_username}",
                source_id=userid,
                target_id=binding_record.ad_username,
                rule_source=binding_source,
                reason_code="persisted_binding",
                details=binding_resolution_details[userid],
            )
            continue

        candidates = identity_candidates_by_userid.get(
            userid
        ) or build_identity_candidates(
            user_departments[userid].user,
            username_strategy=get_connector_spec(connector_id).get("username_strategy")
            or "custom_template",
            username_collision_policy=get_connector_spec(connector_id).get(
                "username_collision_policy"
            )
            or "append_employee_id",
            username_collision_template=get_connector_spec(connector_id).get(
                "username_collision_template"
            )
            or "",
            username_template=get_connector_spec(connector_id).get("username_template")
            or "",
        )
        connector_existing_users = existing_users_map_by_connector.get(connector_id, {})
        existing_candidates = [
            candidate
            for candidate in candidates
            if candidate.get("allow_existing_match")
            and candidate["username"] in connector_existing_users
        ]
        protected_existing_candidates = [
            candidate
            for candidate in existing_candidates
            if is_protected_ad_account(candidate["username"], connector_id)
        ]
        for candidate in protected_existing_candidates:
            record_protected_account_skip(
                stage_name="plan",
                object_type="user_binding",
                operation_type="resolve_identity_binding",
                connector_id=connector_id,
                ad_username=candidate["username"],
                source_id=userid,
                details={
                    "userid": userid,
                    "candidate_rule": candidate["rule"],
                },
            )
        existing_candidates = [
            candidate
            for candidate in existing_candidates
            if not is_protected_ad_account(candidate["username"], connector_id)
        ]
        unique_existing_usernames = {
            candidate["username"].lower(): candidate
            for candidate in existing_candidates
        }
        if len(unique_existing_usernames) > 1:
            conflict_message = (
                f"Source user {userid} matched multiple AD candidates: "
                + " / ".join(
                    sorted(
                        candidate["username"]
                        for candidate in unique_existing_usernames.values()
                    )
                )
            )
            ctx.hooks.record_conflict(
                conflict_type="multiple_ad_candidates",
                source_id=userid,
                target_key="identity_binding",
                message=conflict_message,
                resolution_hint="Create a manual identity binding before rerunning synchronization",
                details={
                    "userid": userid,
                    "candidates": list(unique_existing_usernames.values()),
                    "message_code": "conflicts.message.multiple_ad_candidates",
                    "message_params": {
                        "source_id": userid,
                        "candidates": ": "
                        + " / ".join(
                            sorted(
                                candidate["username"]
                                for candidate in unique_existing_usernames.values()
                            )
                        ),
                    },
                    "resolution_code": "conflicts.resolution.multiple_ad_candidates",
                },
            )
            ctx.hooks.record_event(
                "WARNING",
                "user_binding_conflict",
                conflict_message,
                stage_name="plan",
                payload={
                    "userid": userid,
                    "candidates": list(unique_existing_usernames.values()),
                },
            )
            ctx.hooks.record_operation(
                stage_name="plan",
                object_type="user_binding",
                operation_type="resolve_identity_binding",
                status="conflict",
                message=conflict_message,
                source_id=userid,
                rule_source="auto_candidate_resolution",
                reason_code="multiple_ad_candidates",
                details={
                    "userid": userid,
                    "candidates": list(unique_existing_usernames.values()),
                },
            )
            continue

        if existing_candidates:
            selected_candidate = next(iter(unique_existing_usernames.values()))
            claim_mode = (
                str(
                    getattr(
                        ctx.policy_settings,
                        "first_sync_identity_claim_mode",
                        "auto_safe",
                    )
                    or "auto_safe"
                )
                .strip()
                .lower()
            )
            if claim_mode == "review":
                conflict_message = (
                    f"Source user {userid} matched existing AD account "
                    f"{selected_candidate['username']} and requires identity claim review"
                )
                conflict_details = {
                    "userid": userid,
                    "connector_id": connector_id,
                    "candidate": selected_candidate,
                    "claim_policy": claim_mode,
                    "message_code": "conflicts.message.existing_ad_identity_claim_review",
                    "message_params": {
                        "source_id": userid,
                        "target_key": selected_candidate["username"],
                    },
                    "resolution_code": "conflicts.resolution.existing_ad_identity_claim_review",
                }
                ctx.hooks.record_conflict(
                    conflict_type="existing_ad_identity_claim_review",
                    source_id=userid,
                    target_key=selected_candidate["username"],
                    message=conflict_message,
                    resolution_hint=(
                        "Approve a manual identity binding, or switch the first-sync identity claim policy "
                        "to auto-claim safe existing AD matches"
                    ),
                    details=conflict_details,
                )
                ctx.hooks.record_event(
                    "WARNING",
                    "user_binding_claim_review",
                    conflict_message,
                    stage_name="plan",
                    payload=conflict_details,
                )
                ctx.hooks.record_operation(
                    stage_name="plan",
                    object_type="user_binding",
                    operation_type="resolve_identity_binding",
                    status="conflict",
                    message=conflict_message,
                    source_id=userid,
                    target_id=selected_candidate["username"],
                    rule_source="first_sync_identity_claim_policy",
                    reason_code="identity_claim_review_required",
                    details=conflict_details,
                )
                continue
            resolution = {
                "source": selected_candidate["rule"],
                "ad_username": selected_candidate["username"],
                "connector_id": connector_id,
                "rule_hits": [selected_candidate["rule"]],
                "explanation": selected_candidate["explanation"],
                "binding_record_source": selected_candidate["rule"],
                "is_manual": False,
                "claim_policy": claim_mode,
                "binding_was_persisted": False,
                **identity_evidence,
            }
        else:
            primary_managed_username = next(
                (
                    str(candidate.get("username") or "").strip().lower()
                    for candidate in candidates
                    if candidate.get("rule") == "managed_username_primary"
                ),
                "",
            )
            managed_candidates = [
                candidate
                for candidate in candidates
                if not (
                    candidate.get("rule") == "managed_username_primary"
                    and primary_managed_username
                    and managed_primary_username_counts_by_connector.get(
                        connector_id, {}
                    ).get(primary_managed_username, 0)
                    > 1
                )
            ]
            default_candidate = _select_available_managed_candidate(
                managed_candidates,
                connector_existing_users=connector_existing_users,
                reserved_usernames=reserved_managed_usernames_by_connector.setdefault(
                    connector_id, set()
                ),
                connector_id=connector_id,
                is_protected_ad_account=is_protected_ad_account,
            )
            if default_candidate is None:
                protected_managed_candidates = [
                    candidate
                    for candidate in managed_candidates
                    if candidate.get("managed")
                    and is_protected_ad_account(
                        str(candidate.get("username") or ""), connector_id
                    )
                ]
                if protected_managed_candidates:
                    protected_candidate = protected_managed_candidates[0]
                    record_protected_account_skip(
                        stage_name="plan",
                        object_type="user_binding",
                        operation_type="resolve_identity_binding",
                        connector_id=connector_id,
                        ad_username=str(protected_candidate.get("username") or ""),
                        source_id=userid,
                        details={
                            "userid": userid,
                            "candidate_rule": protected_candidate.get("rule"),
                        },
                    )
                    continue
                conflict_message = f"Source user {userid} does not have a unique managed AD username candidate under connector {connector_id}"
                ctx.hooks.record_conflict(
                    conflict_type="managed_username_collision",
                    source_id=userid,
                    target_key="managed_username",
                    message=conflict_message,
                    resolution_hint="Adjust the connector username strategy, collision policy, or add a manual identity binding",
                    details={
                        "userid": userid,
                        "connector_id": connector_id,
                        "candidates": managed_candidates,
                    },
                )
                ctx.hooks.record_operation(
                    stage_name="plan",
                    object_type="user_binding",
                    operation_type="resolve_identity_binding",
                    status="conflict",
                    message=conflict_message,
                    source_id=userid,
                    rule_source="managed_username_generation",
                    reason_code="managed_username_collision",
                    details={
                        "connector_id": connector_id,
                        "candidates": managed_candidates,
                    },
                )
                continue
            resolution = {
                "source": default_candidate["rule"],
                "ad_username": default_candidate["username"],
                "connector_id": connector_id,
                "rule_hits": [default_candidate["rule"]],
                "explanation": default_candidate["explanation"],
                "binding_record_source": "managed_generated",
                "is_manual": False,
                "managed_username_base": primary_managed_username,
                "binding_was_persisted": False,
                **identity_evidence,
            }

        if is_protected_ad_account(resolution["ad_username"], connector_id):
            record_protected_account_skip(
                stage_name="plan",
                object_type="user_binding",
                operation_type="resolve_identity_binding",
                connector_id=connector_id,
                ad_username=resolution["ad_username"],
                source_id=userid,
                details={
                    "userid": userid,
                    "binding_source": resolution["source"],
                },
            )
            continue
        selected_exists = (
            resolution["ad_username"]
            in existing_users_map_by_connector.get(connector_id, {})
        )
        resolution["before_state"] = {
            "bound_ad_username": "",
            "binding_source": "",
            "binding_enabled": False,
            "connector_id": connector_id,
            "ad_account_state": {
                "status": "exists" if selected_exists else "missing",
                "exists": selected_exists,
                "enabled": None,
                "locked": None,
                "protected": False,
            },
        }
        resolution["preview_fingerprint"] = build_identity_preview_fingerprint(
            org_id=ctx.organization.org_id,
            source_provider=source_provider_id,
            connector_id=connector_id,
            source_user_id=userid,
            source_snapshot_fingerprint=str(
                (ctx.environment.source_scope or {}).get(
                    "source_snapshot_fingerprint"
                )
                or ""
            ),
            selection_fingerprint=str(
                (ctx.environment.source_scope or {}).get("selection_fingerprint")
                or ""
            ),
            config_fingerprint=ctx.config_hash,
            mapping_input=dict(resolution.get("mapping_input") or {}),
            candidate_mapping=dict(resolution.get("candidate_mapping") or {}),
            binding_signature=[],
            ad_state=resolution["before_state"]["ad_account_state"],
        )
        pending_auto_bindings[userid] = resolution
        reserved_managed_usernames_by_connector.setdefault(connector_id, set()).add(
            str(resolution["ad_username"]).strip().lower()
        )

    username_to_userids: dict[str, list[str]] = {}
    for userid, resolution in {
        **binding_resolution_details,
        **pending_auto_bindings,
    }.items():
        ad_username = str(resolution.get("ad_username") or "").strip().lower()
        if not ad_username:
            continue
        username_to_userids.setdefault(ad_username, []).append(userid)

    conflicted_userids = set()
    for ad_username, userids in username_to_userids.items():
        if len(userids) <= 1:
            continue

        authoritative_userids = [
            userid
            for userid in userids
            if binding_resolution_details.get(userid, {}).get("source")
            in {"manual_binding", "existing_binding"}
        ]
        if len(authoritative_userids) == 1:
            losing_userids = [
                userid for userid in userids if userid != authoritative_userids[0]
            ]
        else:
            losing_userids = list(userids)

        for userid in losing_userids:
            conflicted_userids.add(userid)
            conflict_message = f"AD account {ad_username} matched multiple source users: {', '.join(sorted(userids))}"
            ctx.hooks.record_conflict(
                conflict_type="shared_ad_account",
                source_id=userid,
                target_key=ad_username,
                message=conflict_message,
                resolution_hint="Create unique manual identity bindings for the affected users before rerunning synchronization",
                details={
                    "ad_username": ad_username,
                    "source_user_ids": sorted(userids),
                    "message_code": "conflicts.message.shared_ad_account",
                    "message_params": {
                        "target_key": ad_username,
                        "source_users": ": " + ", ".join(sorted(userids)),
                    },
                    "resolution_code": "conflicts.resolution.shared_ad_account",
                },
            )
            ctx.hooks.record_operation(
                stage_name="plan",
                object_type="user_binding",
                operation_type="resolve_identity_binding",
                status="conflict",
                message=conflict_message,
                source_id=userid,
                target_id=ad_username,
                rule_source="duplicate_binding_detection",
                reason_code="shared_ad_account",
                details={
                    "ad_username": ad_username,
                    "source_user_ids": sorted(userids),
                },
            )

    for userid, resolution in pending_auto_bindings.items():
        if userid in conflicted_userids:
            continue
        resolved_username = resolution["ad_username"]
        resolved_connector_id = resolution.get(
            "connector_id"
        ) or user_connector_id_by_userid.get(userid, "default")
        active_user_bindings[userid] = resolved_username
        binding_resolution_details[userid] = resolution
        current_source_ad_usernames_by_connector.setdefault(
            resolved_connector_id, set()
        ).add(resolved_username)
        ctx.hooks.record_operation(
            stage_name="plan",
            object_type="user_binding",
            operation_type="resolve_identity_binding",
            status="selected",
            message=f"resolved {userid} -> {resolved_username}",
            source_id=userid,
            target_id=resolved_username,
            rule_source=resolution["source"],
            reason_code="auto_resolution",
            details=resolution,
        )
        ctx.hooks.add_planned_operation(
            object_type="user_binding",
            operation_type="propose_identity_binding",
            source_id=userid,
            risk_level="normal",
            desired_state={
                "source_provider": source_provider_id,
                "connector_id": resolved_connector_id,
                "ad_username": resolved_username,
                "binding_source": resolution["binding_record_source"],
                "preview_fingerprint": resolution.get("preview_fingerprint") or "",
                "planned_account_state": (
                    "existing"
                    if resolved_username
                    in existing_users_map_by_connector.get(resolved_connector_id, {})
                    else "create"
                ),
            },
        )

    for connector_id in ctx.environment.connector_specs_by_id.keys():
        connector_enabled_users = get_ad_sync(connector_id).get_all_enabled_users()
        enabled_ad_users_by_connector[connector_id] = connector_enabled_users
        enabled_ad_users.extend(
            [f"{connector_id}:{username}" for username in connector_enabled_users]
        )
