"use strict";
const form = document.getElementById("verify");
if (form) form.addEventListener("submit", event => {
  event.preventDefault();
  const status = document.getElementById("status");
  const button = form.querySelector("button");
  const fail = message => { status.textContent = message; button.disabled = false; };
  if (!window.dd || typeof window.dd.requestAuthCode !== "function") return fail("请从钉钉工作台打开此应用。");
  button.disabled = true;
  status.textContent = "正在验证身份并匹配 AD 账号…";
  window.dd.requestAuthCode({
    corpId: form.dataset.corp, clientId: form.dataset.client,
    onSuccess: async result => {
      try {
        const body = new URLSearchParams({code: result.code || result.authCode || "", csrfmiddlewaretoken: form.querySelector("[name=csrfmiddlewaretoken]").value});
        const response = await fetch("/sspr/auth/dingtalk", {method: "POST", credentials: "same-origin", body});
        const data = await response.json();
        if (!response.ok) return fail(data.error || "验证失败，请重试。");
        window.location.replace(data.next);
      } catch (_) { fail("网络请求失败，请重试。"); }
    },
    onFail: () => fail("钉钉验证失败，请重新打开应用。")
  });
});
