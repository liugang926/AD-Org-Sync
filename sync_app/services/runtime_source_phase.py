from __future__ import annotations

from typing import Any, Callable, Optional

from sync_app.core.models import UserDepartmentBundle
from sync_app.services.runtime_context import SyncContext
from sync_app.services.runtime_identity import build_identity_candidates


def collect_source_user_departments(ctx: SyncContext) -> dict[str, UserDepartmentBundle]:
    user_departments = ctx.identity.user_departments
    user_departments.clear()
    ctx.working.source_user_ids.clear()

    for dept_id, dept_info in ctx.environment.dept_tree.items():
        if ctx.hooks.is_cancelled():
            raise InterruptedError('sync cancelled by user')
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
            ctx.logger.error(f"failed to load users from department {dept_info.name}: {fetch_error}")

    ctx.sync_stats['total_users'] = len(ctx.working.source_user_ids)
    if ctx.hooks.stats_callback:
        ctx.hooks.stats_callback('total_users', len(ctx.working.source_user_ids))
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
    user_connector_id_by_userid = ctx.identity.user_connector_id_by_userid
    disabled_bound_userids = ctx.identity.disabled_bound_userids
    exception_skipped_userids = ctx.identity.exception_skipped_userids
    source_user_detail_cache = ctx.identity.source_user_detail_cache
    existing_users_map_by_connector = ctx.identity.existing_users_map_by_connector
    current_source_ad_usernames_by_connector = ctx.working.current_source_ad_usernames_by_connector
    enabled_ad_users_by_connector = ctx.working.enabled_ad_users_by_connector
    enabled_ad_users = ctx.working.enabled_ad_users_flat

    active_user_bindings.clear()
    binding_resolution_details.clear()
    user_connector_id_by_userid.clear()
    disabled_bound_userids.clear()
    exception_skipped_userids.clear()
    source_user_detail_cache.clear()
    existing_users_map_by_connector.clear()
    current_source_ad_usernames_by_connector.clear()
    enabled_ad_users_by_connector.clear()
    enabled_ad_users.clear()

    def get_source_user_detail_cached(userid: str, user: Optional[Any] = None) -> dict[str, Any]:
        if userid not in source_user_detail_cache:
            try:
                source_user_detail_cache[userid] = ctx.environment.source_provider.get_user_detail(userid) or {}
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

    identity_candidates_by_userid: dict[str, list[dict[str, str]]] = {}
    identity_candidate_usernames_by_connector: dict[str, set[str]] = {}
    for userid, bundle in user_departments.items():
        get_source_user_detail_cached(userid, bundle.user)
        connector_candidates = {
            get_connector_id_for_department(department)
            for department in bundle.departments
            if department and department.department_id
        }
        connector_candidates.discard('')
        if not connector_candidates:
            connector_candidates = {'default'}
        if len(connector_candidates) > 1:
            ctx.hooks.record_conflict(
                conflict_type='multiple_connector_candidates',
                source_id=userid,
                target_key='connector_assignment',
                message=(
                    f"Source user {userid} spans multiple connector roots: "
                    + ", ".join(sorted(connector_candidates))
                ),
                resolution_hint='Narrow the department connector roots or move the user into a single managed root',
                details={'userid': userid, 'connector_ids': sorted(connector_candidates)},
            )
            continue
        connector_id = next(iter(connector_candidates))
        user_connector_id_by_userid[userid] = connector_id
        connector_spec = get_connector_spec(connector_id)
        candidates = build_identity_candidates(
            bundle.user,
            username_template=connector_spec.get('username_template') or '',
        )
        identity_candidates_by_userid[userid] = candidates
        for candidate in candidates:
            identity_candidate_usernames_by_connector.setdefault(connector_id, set()).add(candidate['username'])

    for connector_id, usernames in identity_candidate_usernames_by_connector.items():
        existing_users_map_by_connector[connector_id] = get_ad_sync(connector_id).get_users_batch(sorted(usernames))
    pending_auto_bindings: dict[str, dict[str, Any]] = {}

    for userid in sorted(user_departments.keys()):
        if ctx.hooks.has_exception_rule('skip_user_sync', userid):
            exception_skipped_userids.add(userid)
            record_exception_skip(
                stage_name='plan',
                object_type='user',
                operation_type='user_sync',
                exception_rule_type='skip_user_sync',
                match_value=userid,
                reason=f"skip user {userid}: matched exception rule skip_user_sync",
                source_id=userid,
                details={'userid': userid},
            )
            continue

        connector_id = user_connector_id_by_userid.get(userid, 'default')
        binding_record = ctx.repositories.user_binding_repo.get_binding_record_by_source_user_id(userid)
        if binding_record:
            binding_connector_id = binding_record.connector_id or connector_id
            if is_protected_ad_account(binding_record.ad_username, binding_connector_id):
                record_protected_account_skip(
                    stage_name='plan',
                    object_type='user_binding',
                    operation_type='resolve_identity_binding',
                    connector_id=binding_connector_id,
                    ad_username=binding_record.ad_username,
                    source_id=userid,
                    details={
                        'userid': userid,
                        'binding_source': binding_record.source,
                    },
                )
                continue
            if not binding_record.is_enabled:
                disabled_bound_userids.add(userid)
                ctx.hooks.record_event(
                    'INFO',
                    'user_binding_disabled',
                    f"skip user {userid}: user identity binding is disabled",
                    stage_name='plan',
                )
                ctx.hooks.record_operation(
                    stage_name='plan',
                    object_type='user_binding',
                    operation_type='resolve_identity_binding',
                    status='skipped',
                    message=f"skip user {userid}: user identity binding is disabled",
                    source_id=userid,
                    target_id=binding_record.ad_username,
                    rule_source='disabled_binding',
                    reason_code='binding_disabled',
                    details={'userid': userid, 'ad_username': binding_record.ad_username},
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
                    conflict_type='connector_migration_required',
                    source_id=userid,
                    target_key=f"{binding_record.connector_id}->{connector_id}",
                    message=conflict_message,
                    resolution_hint='Review cross-domain migration, then update the manual binding connector or rebind the user',
                    details={
                        'userid': userid,
                        'existing_connector_id': binding_record.connector_id,
                        'target_connector_id': connector_id,
                        'ad_username': binding_record.ad_username,
                    },
                )
                ctx.hooks.record_operation(
                    stage_name='plan',
                    object_type='user_binding',
                    operation_type='resolve_identity_binding',
                    status='conflict',
                    message=conflict_message,
                    source_id=userid,
                    target_id=binding_record.ad_username,
                    rule_source='connector_routing',
                    reason_code='connector_migration_required',
                    details={
                        'existing_connector_id': binding_record.connector_id,
                        'target_connector_id': connector_id,
                        'ad_username': binding_record.ad_username,
                    },
                )
                continue

            binding_source = 'manual_binding' if binding_record.source == 'manual' else 'existing_binding'
            active_user_bindings[userid] = binding_record.ad_username
            binding_resolution_details[userid] = {
                'source': binding_source,
                'ad_username': binding_record.ad_username,
                'connector_id': binding_connector_id,
                'rule_hits': [binding_source],
                'explanation': 'Using the persisted identity binding',
                'binding_record_source': binding_record.source,
                'is_manual': binding_record.source == 'manual',
            }
            current_source_ad_usernames_by_connector.setdefault(
                binding_connector_id,
                set(),
            ).add(binding_record.ad_username)
            ctx.hooks.record_operation(
                stage_name='plan',
                object_type='user_binding',
                operation_type='resolve_identity_binding',
                status='selected',
                message=f"resolved {userid} -> {binding_record.ad_username}",
                source_id=userid,
                target_id=binding_record.ad_username,
                rule_source=binding_source,
                reason_code='persisted_binding',
                details=binding_resolution_details[userid],
            )
            continue

        candidates = identity_candidates_by_userid.get(userid) or build_identity_candidates(
            user_departments[userid].user,
            username_template=get_connector_spec(connector_id).get('username_template') or '',
        )
        connector_existing_users = existing_users_map_by_connector.get(connector_id, {})
        existing_candidates = [
            candidate
            for candidate in candidates
            if candidate['rule'] != 'derived_default_userid'
            and candidate['username'] in connector_existing_users
        ]
        protected_existing_candidates = [
            candidate
            for candidate in existing_candidates
            if is_protected_ad_account(candidate['username'], connector_id)
        ]
        for candidate in protected_existing_candidates:
            record_protected_account_skip(
                stage_name='plan',
                object_type='user_binding',
                operation_type='resolve_identity_binding',
                connector_id=connector_id,
                ad_username=candidate['username'],
                source_id=userid,
                details={
                    'userid': userid,
                    'candidate_rule': candidate['rule'],
                },
            )
        existing_candidates = [
            candidate
            for candidate in existing_candidates
            if not is_protected_ad_account(candidate['username'], connector_id)
        ]
        unique_existing_usernames = {candidate['username'].lower(): candidate for candidate in existing_candidates}
        if len(unique_existing_usernames) > 1:
            conflict_message = (
                f"Source user {userid} matched multiple AD candidates: "
                + " / ".join(sorted(candidate['username'] for candidate in unique_existing_usernames.values()))
            )
            ctx.hooks.record_conflict(
                conflict_type='multiple_ad_candidates',
                source_id=userid,
                target_key='identity_binding',
                message=conflict_message,
                resolution_hint='Create a manual identity binding before rerunning synchronization',
                details={
                    'userid': userid,
                    'candidates': list(unique_existing_usernames.values()),
                },
            )
            ctx.hooks.record_event(
                'WARNING',
                'user_binding_conflict',
                conflict_message,
                stage_name='plan',
                payload={'userid': userid, 'candidates': list(unique_existing_usernames.values())},
            )
            ctx.hooks.record_operation(
                stage_name='plan',
                object_type='user_binding',
                operation_type='resolve_identity_binding',
                status='conflict',
                message=conflict_message,
                source_id=userid,
                rule_source='auto_candidate_resolution',
                reason_code='multiple_ad_candidates',
                details={
                    'userid': userid,
                    'candidates': list(unique_existing_usernames.values()),
                },
            )
            continue

        if existing_candidates:
            selected_candidate = next(iter(unique_existing_usernames.values()))
            resolution = {
                'source': selected_candidate['rule'],
                'ad_username': selected_candidate['username'],
                'connector_id': connector_id,
                'rule_hits': [selected_candidate['rule']],
                'explanation': selected_candidate['explanation'],
                'binding_record_source': selected_candidate['rule'],
                'is_manual': False,
            }
        else:
            default_candidate = next(
                (candidate for candidate in candidates if candidate['rule'] == 'derived_default_userid'),
                candidates[0]
                if candidates
                else {
                    'rule': 'derived_default_userid',
                    'username': userid,
                    'explanation': 'Defaulting to userid because no existing AD user matched',
                },
            )
            resolution = {
                'source': 'derived_default_userid',
                'ad_username': default_candidate['username'],
                'connector_id': connector_id,
                'rule_hits': [default_candidate['rule']],
                'explanation': default_candidate['explanation'],
                'binding_record_source': 'derived_default',
                'is_manual': False,
            }

        if is_protected_ad_account(resolution['ad_username'], connector_id):
            record_protected_account_skip(
                stage_name='plan',
                object_type='user_binding',
                operation_type='resolve_identity_binding',
                connector_id=connector_id,
                ad_username=resolution['ad_username'],
                source_id=userid,
                details={
                    'userid': userid,
                    'binding_source': resolution['source'],
                },
            )
            continue
        pending_auto_bindings[userid] = resolution

    username_to_userids: dict[str, list[str]] = {}
    for userid, resolution in {**binding_resolution_details, **pending_auto_bindings}.items():
        ad_username = str(resolution.get('ad_username') or '').strip().lower()
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
            if binding_resolution_details.get(userid, {}).get('source') in {'manual_binding', 'existing_binding'}
        ]
        if len(authoritative_userids) == 1:
            losing_userids = [userid for userid in userids if userid != authoritative_userids[0]]
        else:
            losing_userids = list(userids)

        for userid in losing_userids:
            conflicted_userids.add(userid)
            conflict_message = (
                f"AD account {ad_username} matched multiple source users: {', '.join(sorted(userids))}"
            )
            ctx.hooks.record_conflict(
                conflict_type='shared_ad_account',
                source_id=userid,
                target_key=ad_username,
                message=conflict_message,
                resolution_hint='Create unique manual identity bindings for the affected users before rerunning synchronization',
                details={'ad_username': ad_username, 'source_user_ids': sorted(userids)},
            )
            ctx.hooks.record_operation(
                stage_name='plan',
                object_type='user_binding',
                operation_type='resolve_identity_binding',
                status='conflict',
                message=conflict_message,
                source_id=userid,
                target_id=ad_username,
                rule_source='duplicate_binding_detection',
                reason_code='shared_ad_account',
                details={'ad_username': ad_username, 'source_user_ids': sorted(userids)},
            )

    for userid, resolution in pending_auto_bindings.items():
        if userid in conflicted_userids:
            continue
        resolved_username = resolution['ad_username']
        resolved_connector_id = resolution.get('connector_id') or user_connector_id_by_userid.get(userid, 'default')
        ctx.repositories.user_binding_repo.upsert_binding_for_source_user(
            userid,
            resolved_username,
            connector_id=resolved_connector_id,
            source=resolution['binding_record_source'],
            notes=resolution['explanation'],
            preserve_manual=True,
        )
        active_user_bindings[userid] = resolved_username
        binding_resolution_details[userid] = resolution
        current_source_ad_usernames_by_connector.setdefault(resolved_connector_id, set()).add(resolved_username)
        ctx.hooks.record_operation(
            stage_name='plan',
            object_type='user_binding',
            operation_type='resolve_identity_binding',
            status='selected',
            message=f"resolved {userid} -> {resolved_username}",
            source_id=userid,
            target_id=resolved_username,
            rule_source=resolution['source'],
            reason_code='auto_resolution',
            details=resolution,
        )

    for connector_id, connector_usernames in current_source_ad_usernames_by_connector.items():
        existing_users_map_by_connector[connector_id] = get_ad_sync(connector_id).get_users_batch(
            sorted(connector_usernames)
        )
    for connector_id in ctx.environment.connector_specs_by_id.keys():
        connector_enabled_users = get_ad_sync(connector_id).get_all_enabled_users()
        enabled_ad_users_by_connector[connector_id] = connector_enabled_users
        enabled_ad_users.extend([f"{connector_id}:{username}" for username in connector_enabled_users])
