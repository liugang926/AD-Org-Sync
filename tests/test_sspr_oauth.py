from urllib.parse import parse_qs, urlparse

from sync_app.web.routes_sspr import _decode_sspr_callback_state
from sync_app.web.sspr_oauth import build_sspr_oauth_launch


def test_build_wecom_sspr_oauth_launch_url():
    launch = build_sspr_oauth_launch(
        provider_id="wecom",
        client_id="corp-001",
        callback_url="https://sync.example.com/sspr/callback/wecom",
        org_id="default",
        source_user_id="alice",
    )

    parsed = urlparse(launch.authorization_url)
    query = parse_qs(parsed.query)
    assert launch.available
    assert parsed.netloc == "open.weixin.qq.com"
    assert parsed.path == "/connect/oauth2/authorize"
    assert parsed.fragment == "wechat_redirect"
    assert query["appid"] == ["corp-001"]
    assert query["scope"] == ["snsapi_base"]
    assert query["redirect_uri"] == ["https://sync.example.com/sspr/callback/wecom"]
    assert _decode_sspr_callback_state(query["state"][0]) == {
        "org_id": "default",
        "provider_id": "wecom",
        "source_user_id": "alice",
    }


def test_build_dingtalk_sspr_oauth_launch_url():
    launch = build_sspr_oauth_launch(
        provider_id="dingtalk",
        client_id="ding-app-key",
        callback_url="https://sync.example.com/sspr/callback/dingtalk",
        org_id="default",
        source_user_id="alice",
    )

    parsed = urlparse(launch.authorization_url)
    query = parse_qs(parsed.query)
    assert launch.available
    assert parsed.netloc == "login.dingtalk.com"
    assert parsed.path == "/oauth2/auth"
    assert query["client_id"] == ["ding-app-key"]
    assert query["scope"] == ["openid"]
    assert query["redirect_uri"] == ["https://sync.example.com/sspr/callback/dingtalk"]
    assert _decode_sspr_callback_state(query["state"][0])["provider_id"] == "dingtalk"


def test_build_sspr_oauth_launch_reports_missing_client_id():
    launch = build_sspr_oauth_launch(
        provider_id="wecom",
        client_id="",
        callback_url="https://sync.example.com/sspr/callback/wecom",
        org_id="default",
        source_user_id="alice",
    )

    assert not launch.available
    assert launch.error == "Source provider client ID is not configured."
