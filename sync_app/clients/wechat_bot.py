import logging

from sync_app.infra.requests_compat import ensure_requests_available, requests

def mask_webhook_url(webhook_url: str) -> str:
    if not webhook_url:
        return ""
    if len(webhook_url) <= 16:
        return "***"
    return f"{webhook_url[:12]}***{webhook_url[-8:]}"


class WeChatBot:
    """WeCom bot webhook client."""

    def __init__(self, webhook_url: str):
        ensure_requests_available()

        self.webhook_url = webhook_url
        self.logger = logging.getLogger(__name__)
        self.session = requests.Session()
        self.max_retries = 3

        retry_strategy = requests.adapters.Retry(
            total=self.max_retries,
            backoff_factor=0.5,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["POST"],
        )
        adapter = requests.adapters.HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("https://", adapter)
        self.logger.info(f"WeCom bot initialized: {mask_webhook_url(webhook_url)}")

    def send_message(self, content: str) -> bool:
        try:
            response = self.session.post(
                self.webhook_url,
                json={"msgtype": "markdown", "markdown": {"content": content}},
                timeout=10,
            )
            response.raise_for_status()
            result = response.json()
            if result.get("errcode") == 0:
                self.logger.info("WeCom bot message sent")
                return True

            self.logger.error(f"WeCom bot send failed: {result}")
            return False
        except requests.RequestException as exc:
            self.logger.error(f"WeCom bot request failed: {exc}")
            return False
        except Exception as exc:
            self.logger.error(f"WeCom bot send failed: {exc}")
            return False

    def close(self):
        if getattr(self, "session", None):
            self.session.close()
            self.session = None

    def __del__(self):
        try:
            self.close()
        except Exception:
            pass
