import logging
import json
import re
import time
from collections import deque
from typing import Any, Dict, List

from sync_app.infra.requests_compat import ensure_requests_available, requests


class DingTalkAPI:
    """DingTalk API client for internal application directory access."""

    AUTH_URL = "https://api.dingtalk.com/v1.0/oauth2/accessToken"
    OAPI_BASE_URL = "https://oapi.dingtalk.com"
    ROOT_DEPARTMENT_ID = 1

    def __init__(self, app_key: str, app_secret: str, agentid: str = None, logger=None):
        ensure_requests_available()

        self.app_key = app_key
        self.app_secret = app_secret
        self.agentid = agentid
        self.access_token = None
        self.token_expires_at = 0
        self.logger = logger or logging.getLogger(__name__)

        self.session = requests.Session()
        self.max_retries = 3
        self.retry_delay = 2
        self.timeout = 30

        retry_strategy = requests.adapters.Retry(
            total=self.max_retries,
            backoff_factor=0.5,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET", "POST"],
        )
        adapter = requests.adapters.HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("https://", adapter)

        self._refresh_access_token()

    def _refresh_access_token(self) -> None:
        payload = {
            "appKey": self.app_key,
            "appSecret": self.app_secret,
        }
        try:
            response = self.session.post(self.AUTH_URL, json=payload, timeout=self.timeout)
            response.raise_for_status()
            result = response.json()
        except requests.RequestException as exc:
            self.logger.error("failed to get DingTalk access token: %s", exc)
            raise

        access_token = str(result.get("accessToken") or result.get("access_token") or "").strip()
        if not access_token:
            error_msg = (
                "failed to get DingTalk access token: "
                f"code={result.get('code')}, message={result.get('message') or result.get('errmsg')}"
            )
            self.logger.error(error_msg)
            raise Exception(error_msg)

        expires_in = int(result.get("expireIn") or result.get("expires_in") or 7200)
        self.access_token = access_token
        self.token_expires_at = time.time() + expires_in - 200
        self.logger.info("DingTalk token refreshed")

    def _ensure_token_valid(self) -> None:
        if time.time() >= self.token_expires_at:
            self.logger.info("DingTalk token expired, refreshing")
            self._refresh_access_token()

    def _should_refresh_token(self, result: Dict[str, Any]) -> bool:
        error_code = str(result.get("errcode") or result.get("code") or "").strip().lower()
        return error_code in {"40014", "42001", "401", "invalidtoken", "invalid_token"}

    def _request(self, method: str, url: str, **kwargs) -> Dict[str, Any]:
        self._ensure_token_valid()
        kwargs.setdefault("timeout", self.timeout)

        for attempt in range(self.max_retries):
            try:
                response = (
                    self.session.get(url, **kwargs)
                    if method.upper() == "GET"
                    else self.session.post(url, **kwargs)
                )
                if response.status_code in {401, 403}:
                    self.logger.info("DingTalk token rejected, refreshing")
                    self._refresh_access_token()
                    continue
                response.raise_for_status()
                result = response.json()
                if self._should_refresh_token(result):
                    self.logger.info("DingTalk token invalid, refreshing")
                    self._refresh_access_token()
                    continue
                return result
            except requests.RequestException as exc:
                self.logger.warning(
                    "DingTalk request failed (%s/%s): %s",
                    attempt + 1,
                    self.max_retries,
                    exc,
                )
                if attempt < self.max_retries - 1:
                    time.sleep(self.retry_delay)
                    continue
                raise

        raise Exception("DingTalk request failed after retries")

    def _extract_result(self, result: Dict[str, Any], *, operation: str) -> Any:
        error_code = result.get("errcode")
        if error_code is None:
            error_code = result.get("code")
        if error_code not in (None, "", 0, "0"):
            raise Exception(
                f"{operation} failed: code={error_code}, "
                f"message={result.get('errmsg') or result.get('message') or 'unknown error'}"
            )
        return result.get("result", result)

    def _post_oapi(self, path: str, payload: Dict[str, Any]) -> Any:
        url = f"{self.OAPI_BASE_URL}{path}?access_token={self.access_token}"
        result = self._request("POST", url, json=payload)
        return self._extract_result(result, operation=path)

    def _coerce_bool(self, value: Any) -> bool:
        if isinstance(value, bool):
            return value
        if value in (None, ""):
            return False
        if isinstance(value, (int, float)):
            return value != 0
        text = str(value).strip().lower()
        if text in {"true", "1", "yes", "y"}:
            return True
        if text in {"false", "0", "no", "n", ""}:
            return False
        return bool(value)

    def _coerce_int_list(self, value: Any) -> List[int]:
        normalized: List[int] = []

        def append_unique(raw_value: Any) -> None:
            try:
                parsed = int(raw_value)
            except (TypeError, ValueError):
                return
            if parsed not in normalized:
                normalized.append(parsed)

        if value in (None, ""):
            return normalized

        if isinstance(value, dict):
            for key in ("dept_id", "deptId", "department_id", "departmentId"):
                if key in value:
                    for item in self._coerce_int_list(value.get(key)):
                        append_unique(item)
            if normalized:
                return normalized

            numeric_keys = []
            for key in value.keys():
                key_text = str(key).strip()
                if key_text.lstrip("-").isdigit():
                    numeric_keys.append(key_text)
            if numeric_keys:
                for key_text in numeric_keys:
                    append_unique(key_text)
                return normalized

            for nested_value in value.values():
                for item in self._coerce_int_list(nested_value):
                    append_unique(item)
            return normalized

        if isinstance(value, (list, tuple, set)):
            for item in value:
                for nested_item in self._coerce_int_list(item):
                    append_unique(nested_item)
            return normalized

        if isinstance(value, str):
            text = value.strip()
            if not text:
                return normalized
            if text.startswith("[") and text.endswith("]"):
                try:
                    return self._coerce_int_list(json.loads(text))
                except json.JSONDecodeError:
                    pass
            for token in re.findall(r"-?\d+", text):
                append_unique(token)
            return normalized

        append_unique(value)
        return normalized

    def _normalize_department(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "id": int(payload.get("dept_id") or payload.get("deptId") or payload.get("id") or 0),
            "name": str(payload.get("name") or payload.get("dept_name") or payload.get("deptName") or ""),
            "parentid": int(payload.get("parent_id") or payload.get("parentId") or payload.get("parentid") or 0),
        }

    def _normalize_user(self, payload: Dict[str, Any], *, department_id: int | None = None) -> Dict[str, Any]:
        normalized_departments = self._coerce_int_list(
            payload.get("dept_id_list")
            or payload.get("deptIdList")
            or payload.get("department")
            or payload.get("departments")
            or payload.get("dept_id")
            or payload.get("deptId")
            or payload.get("dept_order_list")
            or []
        )
        if department_id is not None and not normalized_departments:
            normalized_departments = [int(department_id)]

        primary_department = payload.get("main_department") or payload.get("mainDepartment")
        if primary_department in (None, "") and normalized_departments:
            primary_department = normalized_departments[0]

        normalized = dict(payload)
        normalized.update(
            {
                "userid": str(
                    payload.get("userid")
                    or payload.get("userId")
                    or payload.get("staffid")
                    or payload.get("staffId")
                    or payload.get("unionid")
                    or payload.get("unionId")
                    or ""
                ),
                "name": str(payload.get("name") or payload.get("nick") or payload.get("displayName") or ""),
                "email": str(
                    payload.get("email")
                    or payload.get("org_email")
                    or payload.get("orgEmail")
                    or payload.get("work_email")
                    or payload.get("workEmail")
                    or ""
                ),
                "department": normalized_departments,
            }
        )
        if primary_department not in (None, ""):
            primary_candidates = self._coerce_int_list(primary_department)
            if primary_candidates:
                normalized["main_department"] = primary_candidates[0]
        return normalized

    def list_sub_departments(self, parent_department_id: int) -> List[Dict[str, Any]]:
        result = self._post_oapi(
            "/topapi/v2/department/listsub",
            {"dept_id": int(parent_department_id)},
        )
        if isinstance(result, list):
            items = result
        else:
            items = (
                result.get("list")
                or result.get("departments")
                or result.get("dept_list")
                or result.get("items")
                or []
            )
        if isinstance(items, dict):
            items = [items]
        return [self._normalize_department(dict(item or {})) for item in items if isinstance(item, dict)]

    def get_department_detail(self, department_id: int) -> Dict[str, Any]:
        result = self._post_oapi(
            "/topapi/v2/department/get",
            {"dept_id": int(department_id)},
        )
        if not isinstance(result, dict):
            return {}
        return self._normalize_department(dict(result))

    def get_department_list(self) -> List[Dict[str, Any]]:
        departments: List[Dict[str, Any]] = []
        seen_department_ids = set()
        queue = deque()

        root_department = self.get_department_detail(self.ROOT_DEPARTMENT_ID)
        if root_department:
            root_department_id = int(root_department.get("id") or self.ROOT_DEPARTMENT_ID)
            seen_department_ids.add(root_department_id)
            departments.append(root_department)
            queue.append(root_department_id)
        else:
            queue.append(self.ROOT_DEPARTMENT_ID)

        while queue:
            parent_department_id = int(queue.popleft())
            for department in self.list_sub_departments(parent_department_id):
                department_id = int(department.get("id") or 0)
                if not department_id or department_id in seen_department_ids:
                    continue
                seen_department_ids.add(department_id)
                departments.append(department)
                queue.append(department_id)

        return departments

    def get_department_users(self, department_id: int) -> List[Dict[str, Any]]:
        users: List[Dict[str, Any]] = []
        cursor = 0

        while True:
            result = self._post_oapi(
                "/topapi/v2/user/list",
                {
                    "dept_id": int(department_id),
                    "cursor": int(cursor),
                    "size": 100,
                },
            )
            if isinstance(result, list):
                items = result
                has_more = False
                next_cursor = None
            else:
                items = result.get("list") or result.get("user_list") or result.get("users") or []
                has_more = self._coerce_bool(result.get("has_more"))
                next_cursor = result.get("next_cursor")
            if isinstance(items, dict):
                items = [items]
            users.extend(
                self._normalize_user(dict(item or {}), department_id=department_id)
                for item in items
                if isinstance(item, dict)
            )
            if not has_more:
                break
            cursor = int(next_cursor or (int(cursor) + len(items)))

        return users

    def get_user_detail(self, userid: str) -> Dict[str, Any]:
        result = self._post_oapi("/topapi/v2/user/get", {"userid": userid})
        if not isinstance(result, dict):
            return {}
        return self._normalize_user(dict(result), department_id=None)

    def update_user(self, userid: str, updates: Dict[str, Any]) -> bool:
        payload = {"userid": userid}
        payload.update({key: value for key, value in dict(updates or {}).items() if value not in (None, "")})
        self._post_oapi("/topapi/v2/user/update", payload)
        return True

    def close(self):
        if getattr(self, "session", None):
            self.session.close()
            self.session = None

    def __del__(self):
        try:
            self.close()
        except Exception:
            pass
