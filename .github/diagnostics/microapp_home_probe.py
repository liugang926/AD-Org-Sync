"""Read only the configured homepage for the dedicated DingTalk microapp."""
import os
from urllib.parse import urlsplit

import requests


app_key = os.environ["DINGTALK_APP_KEY"]
app_secret = os.environ["DINGTALK_APP_SECRET"]
agent_id = "4762931603"
token_response = requests.post(
    "https://api.dingtalk.com/v1.0/oauth2/accessToken",
    json={"appKey": app_key, "appSecret": app_secret}, timeout=20,
)
token_response.raise_for_status()
token = token_response.json().get("accessToken")
assert token, "application token unavailable"
response = requests.post(
    "https://oapi.dingtalk.com/microapp/list",
    params={"access_token": token}, json={}, timeout=20,
)
response.raise_for_status()
data = response.json()
assert data.get("errcode") == 0, "microapp list rejected with code " + str(data.get("errcode"))
apps = [app for app in data.get("appList", []) if str(app.get("agentId")) == agent_id]
assert len(apps) == 1, "dedicated microapp not uniquely listed"
app = apps[0]
home = urlsplit(str(app.get("homepageLink") or ""))
pc_home = urlsplit(str(app.get("pcHomepageLink") or ""))
expected = ("https", "it-service.tianjizn.com", 9443, "/sspr")
print("microapp_found=true")
print("mobile_homepage_configured=" + str(bool(home.scheme and home.netloc)).lower())
print("mobile_homepage_matches_sspr=" + str((home.scheme, home.hostname, home.port, home.path) == expected).lower())
print("mobile_homepage_https=" + str(home.scheme == "https").lower())
print("mobile_homepage_host_matches=" + str(home.hostname == expected[1]).lower())
print("mobile_homepage_port_matches=" + str(home.port == expected[2]).lower())
print("mobile_homepage_path_matches=" + str(home.path == expected[3]).lower())
print("mobile_homepage_has_corpid_placeholder=" + str("$CORPID$" in home.query).lower())
print("pc_homepage_configured=" + str(bool(pc_home.scheme and pc_home.netloc)).lower())
print("pc_homepage_matches_sspr=" + str((pc_home.scheme, pc_home.hostname, pc_home.port, pc_home.path) == expected).lower())
print("pc_homepage_https=" + str(pc_home.scheme == "https").lower())
print("pc_homepage_host_matches=" + str(pc_home.hostname == expected[1]).lower())
print("pc_homepage_port_matches=" + str(pc_home.port == expected[2]).lower())
print("pc_homepage_path_matches=" + str(pc_home.path == expected[3]).lower())
