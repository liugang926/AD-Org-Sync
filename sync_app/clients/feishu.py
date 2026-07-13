from __future__ import annotations

import hashlib
import logging
import time
from collections import deque
from typing import Any
from urllib.parse import quote

from sync_app.infra.requests_compat import ensure_requests_available, requests


class FeishuAPIError(RuntimeError):
    """A redacted Feishu API failure safe for logs and administrator responses."""


class FeishuAPI:
    """Feishu contact-v3 client using a custom app tenant_access_token.

    Endpoints follow the official Feishu Open Platform contact-v3 documentation:
    auth/v3/tenant_access_token/internal, departments/:id/children,
    users/find_by_department, and users/:id.
    """

    BASE_URL = "https://open.feishu.cn/open-apis"
    ROOT_DEPARTMENT_ID = "0"
    TOKEN_ERROR_CODES = {99991661, 99991663, 99991664, 99991668}

    def __init__(
        self,
        app_id: str,
        app_secret: str,
        tenant_key: str | None = None,
        *,
        logger=None,
    ) -> None:
        ensure_requests_available()
        self.app_id = str(app_id or "").strip()
        self.app_secret = str(app_secret or "").strip()
        self.tenant_key = str(tenant_key or "").strip()
        self.logger = logger or logging.getLogger(__name__)
        self.session = requests.Session()
        self.timeout = 30
        self.max_retries = 3
        self.retry_delay = 1.0
        self.access_token = ""
        self.token_expires_at = 0.0
        self._department_external_to_internal: dict[str, int] = {}
        self._department_internal_to_external: dict[int, str] = {}
        retry = requests.adapters.Retry(
            total=self.max_retries,
            backoff_factor=0.5,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET", "POST"],
        )
        self.session.mount("https://", requests.adapters.HTTPAdapter(max_retries=retry))
        self._refresh_access_token()

    def _redact(self, value: Any) -> str:
        text = str(value or "")
        for secret in (self.app_secret, self.access_token):
            if secret:
                text = text.replace(secret, "***")
        return text[:500]

    def _refresh_access_token(self) -> None:
        url = f"{self.BASE_URL}/auth/v3/tenant_access_token/internal"
        try:
            response = self.session.post(
                url,
                json={"app_id": self.app_id, "app_secret": self.app_secret},
                timeout=self.timeout,
            )
            response.raise_for_status()
            payload = response.json()
        except requests.RequestException as exc:
            raise FeishuAPIError(f"Feishu authentication request failed: {self._redact(exc)}") from exc
        code = int(payload.get("code") or 0)
        token = str(payload.get("tenant_access_token") or "").strip()
        if code != 0 or not token:
            raise FeishuAPIError(
                f"Feishu authentication failed: code={code}, message={self._redact(payload.get('msg') or 'unknown error')}"
            )
        self.access_token = token
        self.token_expires_at = time.time() + max(int(payload.get("expire") or 7200) - 200, 60)
        self.logger.info("Feishu tenant access token refreshed")

    def _ensure_token(self) -> None:
        if not self.access_token or time.time() >= self.token_expires_at:
            self._refresh_access_token()

    def _request(self, method: str, path: str, **kwargs: Any) -> dict[str, Any]:
        self._ensure_token()
        url = f"{self.BASE_URL}{path}"
        kwargs.setdefault("timeout", self.timeout)
        for attempt in range(1, self.max_retries + 1):
            headers = dict(kwargs.pop("headers", {}) or {})
            headers["Authorization"] = f"Bearer {self.access_token}"
            headers.setdefault("Content-Type", "application/json; charset=utf-8")
            try:
                response = self.session.request(method.upper(), url, headers=headers, **kwargs)
                if response.status_code in {401, 403}:
                    if response.status_code == 401 and attempt < self.max_retries:
                        self._refresh_access_token()
                        continue
                    raise FeishuAPIError(
                        "Feishu denied contact access. Check application permissions and contact data scope."
                    )
                if response.status_code == 429:
                    if attempt < self.max_retries:
                        retry_after = float(response.headers.get("Retry-After") or self.retry_delay * attempt)
                        time.sleep(min(max(retry_after, 0.0), 10.0))
                        continue
                    raise FeishuAPIError("Feishu API rate limit exceeded; retry later")
                response.raise_for_status()
                payload = response.json()
            except FeishuAPIError:
                raise
            except requests.RequestException as exc:
                if attempt < self.max_retries:
                    time.sleep(self.retry_delay * attempt)
                    continue
                raise FeishuAPIError(f"Feishu API request failed: {self._redact(exc)}") from exc
            code = int(payload.get("code") or 0)
            if code in self.TOKEN_ERROR_CODES and attempt < self.max_retries:
                self._refresh_access_token()
                continue
            if code != 0:
                message = self._redact(payload.get("msg") or "unknown error")
                if code in {41050, 99991672, 99991679}:
                    raise FeishuAPIError(
                        f"Feishu contact permission or data scope is insufficient: code={code}, message={message}"
                    )
                raise FeishuAPIError(f"Feishu API failed: code={code}, message={message}")
            data = payload.get("data")
            return dict(data or {}) if isinstance(data, dict) else {}
        raise FeishuAPIError("Feishu API request failed after retries")

    @staticmethod
    def _stable_department_id(external_id: str) -> int:
        if str(external_id).isdigit():
            return int(external_id)
        digest = hashlib.sha256(str(external_id).encode("utf-8")).digest()
        return -int.from_bytes(digest[:7], "big")

    def _remember_department(self, external_id: str) -> int:
        normalized = str(external_id or self.ROOT_DEPARTMENT_ID)
        internal = self._stable_department_id(normalized)
        self._department_external_to_internal[normalized] = internal
        self._department_internal_to_external[internal] = normalized
        return internal

    def _normalize_department(self, payload: dict[str, Any]) -> dict[str, Any]:
        external_id = str(payload.get("open_department_id") or payload.get("department_id") or "")
        parent_external_id = str(
            payload.get("parent_department_id") or payload.get("parent_open_department_id") or self.ROOT_DEPARTMENT_ID
        )
        return {
            "id": self._remember_department(external_id),
            "name": str(payload.get("name") or payload.get("i18n_name", {}).get("zh_cn") or ""),
            "parentid": self._remember_department(parent_external_id),
            "source_department_id": external_id,
            "source_parent_department_id": parent_external_id,
        }

    def _normalize_user(self, payload: dict[str, Any]) -> dict[str, Any]:
        department_external_ids = [str(value) for value in payload.get("department_ids") or [] if str(value)]
        departments = [self._remember_department(value) for value in department_external_ids]
        status = payload.get("status") if isinstance(payload.get("status"), dict) else {}
        is_active = not bool(status.get("is_resigned") or status.get("is_frozen") or status.get("is_unjoin"))
        if "is_activated" in status:
            is_active = is_active and bool(status.get("is_activated"))
        normalized = dict(payload)
        normalized.update(
            {
                "userid": str(payload.get("user_id") or payload.get("open_id") or payload.get("union_id") or ""),
                "name": str(payload.get("name") or payload.get("en_name") or ""),
                "email": str(payload.get("email") or payload.get("enterprise_email") or ""),
                "mobile": str(payload.get("mobile") or ""),
                "position": str(payload.get("job_title") or payload.get("position") or ""),
                "employee_id": str(payload.get("employee_no") or payload.get("employee_id") or ""),
                "department": departments,
                "source_department_ids": department_external_ids,
                "primary_department_id": departments[0] if departments else None,
                "is_active": is_active,
                "account_status": "active" if is_active else "inactive",
                "provider_id": "feishu",
            }
        )
        return normalized

    def _paged_get(self, path: str, *, params: dict[str, Any]) -> list[dict[str, Any]]:
        items: list[dict[str, Any]] = []
        page_token = ""
        while True:
            page_params = dict(params)
            if page_token:
                page_params["page_token"] = page_token
            data = self._request("GET", path, params=page_params)
            page_items = data.get("items") or []
            items.extend(dict(item) for item in page_items if isinstance(item, dict))
            if not data.get("has_more"):
                break
            page_token = str(data.get("page_token") or "")
            if not page_token:
                break
        return items

    def list_sub_departments(self, external_parent_id: str) -> list[dict[str, Any]]:
        path = f"/contact/v3/departments/{quote(str(external_parent_id), safe='')}/children"
        rows = self._paged_get(
            path,
            params={
                "department_id_type": "open_department_id",
                "user_id_type": "user_id",
                "fetch_child": "false",
                "page_size": 50,
            },
        )
        return [self._normalize_department(row) for row in rows]

    def get_department_list(self) -> list[dict[str, Any]]:
        departments: list[dict[str, Any]] = []
        queue: deque[str] = deque([self.ROOT_DEPARTMENT_ID])
        seen = {self.ROOT_DEPARTMENT_ID}
        self._remember_department(self.ROOT_DEPARTMENT_ID)
        while queue:
            parent_id = queue.popleft()
            for department in self.list_sub_departments(parent_id):
                external_id = str(department.get("source_department_id") or "")
                if not external_id or external_id in seen:
                    continue
                seen.add(external_id)
                departments.append(department)
                queue.append(external_id)
        return departments

    def get_department_users(self, department_id: int | str) -> list[dict[str, Any]]:
        try:
            internal_id = int(department_id)
        except (TypeError, ValueError):
            internal_id = 0
        external_id = self._department_internal_to_external.get(internal_id, str(department_id))
        rows = self._paged_get(
            "/contact/v3/users/find_by_department",
            params={
                "department_id": external_id,
                "department_id_type": "open_department_id",
                "user_id_type": "user_id",
                "page_size": 50,
            },
        )
        return [self._normalize_user(row) for row in rows]

    def get_user_detail(self, user_id: str) -> dict[str, Any]:
        data = self._request(
            "GET",
            f"/contact/v3/users/{quote(str(user_id), safe='')}",
            params={"user_id_type": "user_id", "department_id_type": "open_department_id"},
        )
        user = data.get("user")
        return self._normalize_user(dict(user or {})) if isinstance(user, dict) else {}

    def close(self) -> None:
        if getattr(self, "session", None):
            self.session.close()
            self.session = None


__all__ = ["FeishuAPI", "FeishuAPIError"]
