import logging
import time
from typing import Dict, List

from sync_app.infra.requests_compat import ensure_requests_available, requests

class WeComAPI:
    """WeCom API client."""

    def __init__(self, corpid: str, corpsecret: str, agentid: str = None, logger=None):
        ensure_requests_available()

        self.corpid = corpid
        self.corpsecret = corpsecret
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
        url = (
            "https://qyapi.weixin.qq.com/cgi-bin/gettoken"
            f"?corpid={self.corpid}&corpsecret={self.corpsecret}"
        )
        try:
            response = self.session.get(url, timeout=self.timeout)
            response.raise_for_status()
            result = response.json()
        except requests.RequestException as exc:
            self.logger.error(f"failed to get access token: {exc}")
            raise

        if result.get("errcode") != 0:
            error_msg = (
                "failed to get access token: "
                f"errcode={result.get('errcode')}, errmsg={result.get('errmsg')}"
            )
            self.logger.error(error_msg)
            raise Exception(error_msg)

        self.access_token = result["access_token"]
        self.token_expires_at = time.time() + result.get("expires_in", 7200) - 200
        auth_type = "self-built app" if self.agentid else "generic"
        self.logger.info(f"WeCom token refreshed via {auth_type} mode")

    def _ensure_token_valid(self) -> None:
        if time.time() >= self.token_expires_at:
            self.logger.info("WeCom token expired, refreshing")
            self._refresh_access_token()

    def _request(self, method: str, url: str, **kwargs) -> Dict:
        self._ensure_token_valid()
        kwargs.setdefault("timeout", self.timeout)

        for attempt in range(self.max_retries):
            try:
                response = (
                    self.session.get(url, **kwargs)
                    if method.upper() == "GET"
                    else self.session.post(url, **kwargs)
                )
                response.raise_for_status()
                result = response.json()
                if result.get("errcode") == 42001:
                    self.logger.info("WeCom token invalid, refreshing")
                    self._refresh_access_token()
                    continue
                return result
            except requests.RequestException as exc:
                self.logger.warning(
                    f"WeCom request failed ({attempt + 1}/{self.max_retries}): {exc}"
                )
                if attempt < self.max_retries - 1:
                    time.sleep(self.retry_delay)
                    continue
                raise

        raise Exception("WeCom request failed after retries")

    def get_department_list(self) -> List[Dict]:
        url = (
            "https://qyapi.weixin.qq.com/cgi-bin/department/list"
            f"?access_token={self.access_token}"
        )
        result = self._request("GET", url)
        if result.get("errcode") != 0:
            raise Exception(
                "failed to get department list: "
                f"errcode={result.get('errcode')}, errmsg={result.get('errmsg')}"
            )
        return result.get("department", [])

    def get_department_users(self, department_id: int) -> List[Dict]:
        url = (
            "https://qyapi.weixin.qq.com/cgi-bin/user/list"
            f"?access_token={self.access_token}&department_id={department_id}&fetch_child=0"
        )
        result = self._request("GET", url)
        if result.get("errcode") != 0:
            raise Exception(
                f"failed to get users for department {department_id}: "
                f"errcode={result.get('errcode')}, errmsg={result.get('errmsg')}"
            )
        return result.get("userlist", [])

    def get_user_detail(self, userid: str) -> Dict:
        url = (
            "https://qyapi.weixin.qq.com/cgi-bin/user/get"
            f"?access_token={self.access_token}&userid={userid}"
        )
        result = self._request("GET", url)
        if result.get("errcode") != 0:
            self.logger.error(
                f"failed to get WeCom user detail for {userid}: {result.get('errmsg')}"
            )
            return {}
        return result

    def get_oauth_user_info(self, code: str) -> Dict:
        normalized_code = str(code or "").strip()
        if not normalized_code:
            return {}
        url = (
            "https://qyapi.weixin.qq.com/cgi-bin/auth/getuserinfo"
            f"?access_token={self.access_token}&code={normalized_code}"
        )
        result = self._request("GET", url)
        if result.get("errcode") != 0:
            self.logger.error(
                "failed to get WeCom OAuth user info: errcode=%s, errmsg=%s",
                result.get("errcode"),
                result.get("errmsg"),
            )
            return {}
        return result

    def update_user(self, userid: str, updates: Dict) -> bool:
        payload = {"userid": userid}
        payload.update({key: value for key, value in dict(updates or {}).items() if value not in (None, "")})
        url = (
            "https://qyapi.weixin.qq.com/cgi-bin/user/update"
            f"?access_token={self.access_token}"
        )
        result = self._request("POST", url, json=payload)
        if result.get("errcode") != 0:
            raise Exception(
                f"failed to update user {userid}: "
                f"errcode={result.get('errcode')}, errmsg={result.get('errmsg')}"
            )
        return True

    def get_tag_list(self) -> List[Dict]:
        url = (
            "https://qyapi.weixin.qq.com/cgi-bin/tag/list"
            f"?access_token={self.access_token}"
        )
        result = self._request("GET", url)
        if result.get("errcode") != 0:
            raise Exception(
                "failed to get tag list: "
                f"errcode={result.get('errcode')}, errmsg={result.get('errmsg')}"
            )
        return result.get("taglist", [])

    def get_tag_users(self, tag_id: str | int) -> Dict:
        url = (
            "https://qyapi.weixin.qq.com/cgi-bin/tag/get"
            f"?access_token={self.access_token}&tagid={tag_id}"
        )
        result = self._request("GET", url)
        if result.get("errcode") != 0:
            raise Exception(
                f"failed to get users for tag {tag_id}: "
                f"errcode={result.get('errcode')}, errmsg={result.get('errmsg')}"
            )
        return result

    def list_external_group_chats(self, *, status_filter: int = 0, limit: int = 100) -> List[Dict]:
        items: List[Dict] = []
        cursor = None
        while True:
            url = (
                "https://qyapi.weixin.qq.com/cgi-bin/externalcontact/groupchat/list"
                f"?access_token={self.access_token}"
            )
            payload = {
                "status_filter": int(status_filter),
                "limit": max(int(limit), 1),
            }
            if cursor:
                payload["cursor"] = cursor
            result = self._request("POST", url, json=payload)
            if result.get("errcode") != 0:
                raise Exception(
                    "failed to list external group chats: "
                    f"errcode={result.get('errcode')}, errmsg={result.get('errmsg')}"
                )
            items.extend(result.get("group_chat_list", []))
            cursor = result.get("next_cursor")
            if not cursor:
                break
        return items

    def get_external_group_chat(self, chat_id: str) -> Dict:
        url = (
            "https://qyapi.weixin.qq.com/cgi-bin/externalcontact/groupchat/get"
            f"?access_token={self.access_token}"
        )
        result = self._request("POST", url, json={"chat_id": chat_id, "need_name": 1})
        if result.get("errcode") != 0:
            raise Exception(
                f"failed to get external group chat {chat_id}: "
                f"errcode={result.get('errcode')}, errmsg={result.get('errmsg')}"
            )
        return result.get("group_chat", {})

    def get_all_users(self) -> List[Dict]:
        all_users = []
        try:
            for dept in self.get_department_list():
                all_users.extend(self.get_department_users(dept["id"]))
        except Exception as exc:
            self.logger.error(f"failed to get all WeCom users: {exc}")
            return []

        unique_users = []
        seen_userids = set()
        for user in all_users:
            userid = user.get("userid")
            if not userid or userid in seen_userids:
                continue
            seen_userids.add(userid)
            unique_users.append(user)
        return unique_users

    def close(self):
        if getattr(self, "session", None):
            self.session.close()
            self.session = None

    def __del__(self):
        try:
            self.close()
        except Exception:
            pass
