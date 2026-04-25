from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Optional
from urllib.parse import urlencode

from fastapi import FastAPI, Request

from sync_app.services.conflict_decision import build_binding_decision_summary
from sync_app.web.app_state import get_web_repositories


class SyncConflictSupportMixin:
    def _build_conflict_candidate_options(
        self,
        conflict: Any,
        recommendation: Optional[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        details = getattr(conflict, "details", None) or {}
        if not isinstance(details, dict):
            details = {}

        candidates_by_username: dict[str, dict[str, Any]] = {}

        def add_candidate(
            username: str,
            *,
            rule: str = "",
            explanation: str = "",
            is_recommended: bool = False,
        ) -> None:
            normalized_username = str(username or "").strip()
            if not normalized_username:
                return
            key = normalized_username.lower()
            existing = candidates_by_username.get(key)
            if existing is None:
                candidates_by_username[key] = {
                    "username": normalized_username,
                    "rule": str(rule or ""),
                    "explanation": str(explanation or ""),
                    "is_recommended": bool(is_recommended),
                }
                return
            if rule and not existing["rule"]:
                existing["rule"] = str(rule)
            if explanation and not existing["explanation"]:
                existing["explanation"] = str(explanation)
            if is_recommended:
                existing["is_recommended"] = True

        if recommendation and recommendation.get("ad_username"):
            add_candidate(
                str(recommendation.get("ad_username") or ""),
                rule="recommended_action",
                explanation=str(recommendation.get("reason") or ""),
                is_recommended=True,
            )

        for candidate in list(details.get("candidates") or []):
            if not isinstance(candidate, dict):
                continue
            add_candidate(
                str(candidate.get("username") or ""),
                rule=str(candidate.get("rule") or ""),
                explanation=str(candidate.get("explanation") or ""),
            )

        conflict_type = str(getattr(conflict, "conflict_type", "") or "").strip().lower()
        if conflict_type == "existing_ad_identity_claim_review":
            claim_candidate = details.get("candidate") if isinstance(details.get("candidate"), dict) else {}
            add_candidate(
                str(claim_candidate.get("username") or getattr(conflict, "target_key", "") or ""),
                rule=str(claim_candidate.get("rule") or "existing_ad_identity_claim_review"),
                explanation=str(
                    claim_candidate.get("explanation")
                    or "This existing AD account matched the first-sync identity claim policy and is waiting for review."
                ),
            )
        if conflict_type == "shared_ad_account":
            add_candidate(
                str(getattr(conflict, "target_key", "") or details.get("ad_username") or ""),
                rule="shared_ad_account",
                explanation="This AD account is currently shared by multiple source users.",
            )

        return sorted(
            candidates_by_username.values(),
            key=lambda item: (
                0 if item["is_recommended"] else 1,
                str(item["username"] or "").lower(),
            ),
        )

    def _load_target_account_summary(self, request: Request, ad_username: str) -> dict[str, Any]:
        normalized_ad_username = str(ad_username or "").strip()
        if not normalized_ad_username:
            return {
                "username": "",
                "exists": False,
                "enabled": None,
                "display_name": "",
                "mail": "",
                "title": "",
                "description": "",
                "telephone_number": "",
                "last_logon": "",
                "distinguished_name": "",
                "ou_path": "",
            }

        user_details: dict[str, Any] = {}
        batch_record = None
        enabled: bool | None = None
        try:
            _config, target_provider = self._get_target_provider(request)
            try:
                if hasattr(target_provider, "get_users_batch"):
                    batch_records = dict(target_provider.get_users_batch([normalized_ad_username]) or {})
                    batch_record = next(
                        (
                            item
                            for key, item in batch_records.items()
                            if str(key or "").strip().lower() == normalized_ad_username.lower()
                        ),
                        None,
                    )
                if hasattr(target_provider, "get_user_details"):
                    user_details = dict(target_provider.get_user_details(normalized_ad_username) or {})
                is_user_active = getattr(target_provider, "is_user_active", None)
                if callable(is_user_active):
                    enabled = bool(is_user_active(normalized_ad_username))
            finally:
                self._close_directory_resource(target_provider)
        except Exception as exc:
            self.logger.warning("failed to load target account summary for %s: %s", normalized_ad_username, exc)

        exists = bool(user_details) or batch_record is not None
        distinguished_name = str(
            user_details.get("DistinguishedName")
            or getattr(batch_record, "dn", "")
            or ""
        )
        return {
            "username": normalized_ad_username,
            "exists": exists,
            "enabled": enabled if exists else None,
            "display_name": str(
                user_details.get("DisplayName")
                or getattr(batch_record, "display_name", "")
                or ""
            ),
            "mail": str(
                user_details.get("Mail")
                or getattr(batch_record, "email", "")
                or ""
            ),
            "title": str(user_details.get("Title") or ""),
            "description": str(user_details.get("Description") or ""),
            "telephone_number": str(user_details.get("TelephoneNumber") or ""),
            "last_logon": str(user_details.get("LastLogonDate") or ""),
            "distinguished_name": distinguished_name,
            "ou_path": self._normalize_ou_path(distinguished_name),
        }

    def _build_conflict_field_updates(
        self,
        request: Request,
        *,
        connector_id: str,
    ) -> list[dict[str, str]]:
        current_org = self.request_support.get_current_org(request)
        repositories = get_web_repositories(request)
        items: list[dict[str, str]] = []
        seen_fields: set[str] = set()

        def add_item(name: str, *, source: str) -> None:
            normalized_name = str(name or "").strip()
            if not normalized_name:
                return
            key = normalized_name.lower()
            if key in seen_fields:
                return
            seen_fields.add(key)
            items.append(
                {
                    "name": normalized_name,
                    "source": str(source or ""),
                }
            )

        add_item("displayName", source="Core user sync")
        add_item("mail", source="Core user sync")
        add_item("target OU", source="OU placement")

        for rule in repositories.attribute_mapping_repo.list_rule_records(
            direction="source_to_ad",
            connector_id=str(connector_id or "").strip() or "default",
            enabled_only=True,
            org_id=current_org.org_id,
        ):
            add_item(
                str(getattr(rule, "target_field", "") or ""),
                source=str(getattr(rule, "source_field", "") or "Attribute mapping"),
            )
        return items

    def build_conflict_decision_guide(
        self,
        request: Request,
        conflict: Any,
        *,
        ad_username: str = "",
    ) -> dict[str, Any]:
        current_org = self.request_support.get_current_org(request)
        current_org_id = current_org.org_id
        details = getattr(conflict, "details", None) or {}
        if not isinstance(details, dict):
            details = {}

        recommendation = self.recommend_conflict_resolution(conflict)
        explanation = None
        explanation_error = ""
        if str(getattr(conflict, "source_id", "") or "").strip():
            try:
                explanation = self.explain_identity_routing(request, str(conflict.source_id))
            except Exception as exc:
                explanation_error = str(exc)

        candidate_options = self._build_conflict_candidate_options(conflict, recommendation)
        selected_target_username = str(ad_username or "").strip()
        if not selected_target_username:
            selected_target_username = next(
                (
                    str(item.get("username") or "").strip()
                    for item in candidate_options
                    if item.get("is_recommended")
                ),
                "",
            )
        if not selected_target_username:
            selected_target_username = next(
                (str(item.get("username") or "").strip() for item in candidate_options),
                "",
            )
        if not selected_target_username:
            selected_target_username = str(getattr(conflict, "target_key", "") or "").strip()
        for item in candidate_options:
            item["is_selected"] = (
                str(item.get("username") or "").strip().lower()
                == selected_target_username.lower()
            )

        selected_connector = dict((explanation or {}).get("selected_connector") or {})
        current_binding = dict((explanation or {}).get("binding") or {})
        connector_id = str(
            selected_connector.get("connector_id")
            or current_binding.get("connector_id")
            or "default"
        ).strip() or "default"

        target_account = self._load_target_account_summary(request, selected_target_username)
        repositories = get_web_repositories(request)
        existing_binding_owner = (
            repositories.user_binding_repo.get_binding_record_by_ad_username(
                selected_target_username,
                org_id=current_org_id,
            )
            if selected_target_username
            else None
        )
        field_updates = self._build_conflict_field_updates(
            request,
            connector_id=connector_id,
        )
        config = self._get_org_app_config(request)
        shared_source_user_ids = [
            str(item or "").strip()
            for item in list(details.get("source_user_ids") or details.get("wecom_userids") or [])
            if str(item or "").strip()
        ]
        decision = build_binding_decision_summary(
            conflict_type=str(getattr(conflict, "conflict_type", "") or ""),
            source_user_id=str(getattr(conflict, "source_id", "") or ""),
            selected_target_username=selected_target_username,
            target_exists=bool(target_account.get("exists")),
            target_enabled=target_account.get("enabled"),
            current_binding_owner=(
                str(getattr(existing_binding_owner, "source_user_id", "") or "")
                if existing_binding_owner
                else ""
            ),
            is_protected_account=(
                self.is_protected_ad_account_name(selected_target_username, config.exclude_accounts)
                if selected_target_username
                else False
            ),
            shared_source_user_ids=shared_source_user_ids,
            rehire_restore_enabled=repositories.settings_repo.get_bool(
                "rehire_restore_enabled",
                False,
                org_id=current_org_id,
            ),
        )
        return {
            "source_user": dict((explanation or {}).get("user") or {})
            or {
                "userid": str(getattr(conflict, "source_id", "") or ""),
                "name": str(getattr(conflict, "source_id", "") or ""),
                "email": "",
            },
            "routing": explanation or {},
            "routing_error": explanation_error,
            "recommendation": recommendation or {},
            "candidate_options": candidate_options,
            "selected_target_username": selected_target_username,
            "selected_connector": selected_connector,
            "target_account": target_account,
            "existing_binding_owner": (
                {
                    "source_user_id": str(getattr(existing_binding_owner, "source_user_id", "") or ""),
                    "connector_id": str(getattr(existing_binding_owner, "connector_id", "") or ""),
                    "source": str(getattr(existing_binding_owner, "source", "") or ""),
                    "notes": str(getattr(existing_binding_owner, "notes", "") or ""),
                }
                if existing_binding_owner
                else None
            ),
            "field_updates": field_updates,
            "shared_source_user_ids": shared_source_user_ids,
            "decision": decision,
        }

    def build_conflicts_return_url(self, query: str, status: str, job_id: str) -> str:
        query_parts: dict[str, str] = {}
        if query:
            query_parts["q"] = query
        if status:
            query_parts["status"] = status
        if job_id:
            query_parts["job_id"] = job_id
        if not query_parts:
            return "/conflicts"
        return "/conflicts?" + urlencode(query_parts)

    def resolve_conflict_records_for_source(
        self,
        *,
        app: FastAPI,
        job_id: str,
        source_id: str,
        resolution_payload: dict[str, Any],
        actor_username: str,
    ) -> int:
        resolved_at = datetime.now(timezone.utc).isoformat(timespec="seconds")
        return get_web_repositories(app).conflict_repo.resolve_open_conflicts_for_source(
            job_id=job_id,
            source_id=source_id,
            resolution_payload={
                **resolution_payload,
                "actor_username": actor_username,
            },
            resolved_at=resolved_at,
        )

    def apply_conflict_manual_binding(
        self,
        *,
        app: FastAPI,
        conflict: Any,
        ad_username: str,
        actor_username: str,
        org_id: str,
        notes: str = "",
    ) -> tuple[bool, str, int]:
        normalized_ad_username = str(ad_username or "").strip()
        if not conflict.source_id or not normalized_ad_username:
            return False, "Conflict does not support manual binding", 0

        conflict_message = None
        repositories = get_web_repositories(app)
        config = repositories.org_config_repo.get_app_config(org_id, config_path="")
        if self.is_protected_ad_account_name(normalized_ad_username, config.exclude_accounts):
            conflict_message = (
                f"AD account {normalized_ad_username} is system-protected and cannot be managed by sync."
            )
        else:
            existing_by_ad = repositories.user_binding_repo.get_binding_record_by_ad_username(
                normalized_ad_username,
                org_id=org_id,
            )
            if existing_by_ad and existing_by_ad.source_user_id != conflict.source_id:
                conflict_message = (
                    f"AD account {normalized_ad_username} is already bound to source user "
                    f"{existing_by_ad.source_user_id}. Resolve the existing binding first."
                )
        if conflict_message:
            return False, conflict_message, 0

        binding_notes = str(notes or "").strip() or f"resolved from conflict {conflict.id}"
        repositories.user_binding_repo.upsert_binding_for_source_user(
            conflict.source_id,
            normalized_ad_username,
            org_id=org_id,
            source="manual",
            notes=binding_notes,
            preserve_manual=False,
        )
        resolved_count = self.resolve_conflict_records_for_source(
            app=app,
            job_id=conflict.job_id,
            source_id=conflict.source_id,
            resolution_payload={
                "action": "manual_binding",
                "ad_username": normalized_ad_username,
                "notes": binding_notes,
                "source_conflict_id": conflict.id,
            },
            actor_username=actor_username,
        )
        self.enqueue_replay_request(
            app=app,
            request_type="conflict_resolution",
            requested_by=actor_username,
            org_id=org_id,
            target_scope="source_user",
            target_id=conflict.source_id,
            trigger_reason="manual_binding_resolved",
            payload={
                "conflict_id": conflict.id,
                "job_id": conflict.job_id,
                "action": "manual_binding",
                "ad_username": normalized_ad_username,
            },
        )
        return True, normalized_ad_username, resolved_count

    def apply_conflict_skip_user_sync(
        self,
        *,
        app: FastAPI,
        conflict: Any,
        actor_username: str,
        org_id: str,
        notes: str = "",
    ) -> tuple[bool, str, int]:
        if not conflict.source_id:
            return False, "Conflict does not have a source user to whitelist", 0

        rule_notes = str(notes or "").strip() or f"resolved from conflict {conflict.id}"
        get_web_repositories(app).exception_rule_repo.upsert_rule(
            rule_type="skip_user_sync",
            match_value=conflict.source_id,
            org_id=org_id,
            notes=rule_notes,
            is_enabled=True,
        )
        resolved_count = self.resolve_conflict_records_for_source(
            app=app,
            job_id=conflict.job_id,
            source_id=conflict.source_id,
            resolution_payload={
                "action": "skip_user_sync",
                "notes": rule_notes,
                "source_conflict_id": conflict.id,
            },
            actor_username=actor_username,
        )
        self.enqueue_replay_request(
            app=app,
            request_type="conflict_resolution",
            requested_by=actor_username,
            org_id=org_id,
            target_scope="source_user",
            target_id=conflict.source_id,
            trigger_reason="skip_user_sync_added",
            payload={
                "conflict_id": conflict.id,
                "job_id": conflict.job_id,
                "action": "skip_user_sync",
            },
        )
        return True, rule_notes, resolved_count

    def apply_conflict_recommendation(
        self,
        *,
        app: FastAPI,
        conflict: Any,
        actor_username: str,
        org_id: str,
        confirmation_reason: str = "",
    ) -> tuple[bool, str, int, Optional[dict[str, Any]]]:
        recommendation = self.recommend_conflict_resolution(conflict)
        if not recommendation:
            return False, "No recommendation is available for this conflict", 0, None

        action = str(recommendation.get("action") or "").strip().lower()
        reason = str(recommendation.get("reason") or "").strip()
        normalized_confirmation_reason = str(confirmation_reason or "").strip()
        if self.recommendation_requires_confirmation(recommendation) and not normalized_confirmation_reason:
            return (
                False,
                "This recommendation requires a confirmation reason before it can be applied",
                0,
                recommendation,
            )

        notes = normalized_confirmation_reason or reason or f"recommended resolution from conflict {conflict.id}"
        if action == "manual_binding":
            ok, detail, resolved_count = self.apply_conflict_manual_binding(
                app=app,
                conflict=conflict,
                ad_username=str(recommendation.get("ad_username") or ""),
                actor_username=actor_username,
                org_id=org_id,
                notes=notes,
            )
            return ok, detail, resolved_count, recommendation
        if action == "skip_user_sync":
            ok, detail, resolved_count = self.apply_conflict_skip_user_sync(
                app=app,
                conflict=conflict,
                actor_username=actor_username,
                org_id=org_id,
                notes=notes,
            )
            return ok, detail, resolved_count, recommendation
        return False, f"Unsupported recommendation action: {action or '-'}", 0, recommendation
