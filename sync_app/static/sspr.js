"use strict";
const form = document.getElementById("verify");
if (form) {
  const status = document.getElementById("status");
  const button = form.querySelector("button");
  let verifying = false;

  const fail = message => {
    status.textContent = message;
    verifying = false;
    button.disabled = false;
    button.hidden = false;
  };

  const verify = () => {
    if (verifying) return;
    if (!window.dd || typeof window.dd.requestAuthCode !== "function") {
      fail("请从钉钉工作台打开此应用。");
      return;
    }
    verifying = true;
    button.hidden = true;
    button.disabled = true;
    status.textContent = "正在确认身份和 AD 账号…";
    try {
      window.dd.requestAuthCode({
        corpId: form.dataset.corp, clientId: form.dataset.client,
        success: async result => {
          try {
            const body = new URLSearchParams({
              code: result.code || result.authCode || "",
              csrfmiddlewaretoken: form.querySelector("[name=csrfmiddlewaretoken]").value
            });
            const response = await fetch("/sspr/auth/dingtalk", {method: "POST", credentials: "same-origin", body});
            const data = await response.json();
            if (!response.ok) return fail(data.error || "验证失败，请重试。");
            window.location.replace(data.next);
          } catch (_) { fail("网络请求失败，请重试。"); }
        },
        fail: () => fail("钉钉验证失败，请重新验证。")
      });
    } catch (_) { fail("钉钉验证失败，请重新验证。"); }
  };

  form.addEventListener("submit", event => {
    event.preventDefault();
    verify();
  });
  verify();
}
