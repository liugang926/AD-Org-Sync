import hashlib
import json
from datetime import datetime
from uuid import uuid4

APP_VERSION = "2.0.0"


def generate_job_id() -> str:
    return f"sync_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{uuid4().hex[:8]}"


def hash_user_state(user_data: dict) -> str:
    key_fields = {
        "userid": user_data.get("userid", ""),
        "name": user_data.get("name", ""),
        "email": user_data.get("email", ""),
        "department": sorted(user_data.get("department", []) or []),
        "status": user_data.get("status", ""),
        "enable": user_data.get("enable", ""),
    }
    return hashlib.sha256(json.dumps(key_fields, sort_keys=True, ensure_ascii=False).encode("utf-8")).hexdigest()


def hash_department_state(department_data: dict) -> str:
    key_fields = {
        "id": department_data.get("id", ""),
        "name": department_data.get("name", ""),
        "parentid": department_data.get("parentid", ""),
        "order": department_data.get("order", ""),
    }
    return hashlib.sha256(json.dumps(key_fields, sort_keys=True, ensure_ascii=False).encode("utf-8")).hexdigest()


def format_time_duration(seconds: float) -> str:
    minutes, seconds = divmod(int(seconds), 60)
    hours, minutes = divmod(minutes, 60)
    if hours > 0:
        return f"{hours}h {minutes}m {seconds}s"
    if minutes > 0:
        return f"{minutes}m {seconds}s"
    return f"{seconds}s"
