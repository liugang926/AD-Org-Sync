"use strict";
const form = document.getElementById("verify");
if (form) {
  const status = document.getElementById("status");
  const button = form.querySelector("button");
  const sdkUrl = "https://g.alicdn.com/dingding/dingtalk-jsapi/3.2.9/dingtalk.open.js";
  let busy = false;
  let attempt = 0;
  let timer;
  let sdkScript;
  let controller;

  const fail = (message, current) => {
    if (current !== attempt) return;
    attempt += 1;
    clearTimeout(timer);
    if (sdkScript) sdkScript.remove();
    if (controller) controller.abort();
    sdkScript = undefined;
    controller = undefined;
    busy = false;
    status.textContent = message;
    button.disabled = false;
    button.hidden = false;
  };

  const submitCode = async (result, current) => {
    if (current !== attempt) return;
    const code = result && (result.code || result.authCode);
    if (!code) return fail("钉钉未返回授权码，请重新验证。", current);
    status.textContent = "正在确认身份和 AD 账号…";
    controller = new AbortController();
    timer = setTimeout(() => fail("身份核验超时，请检查网络后重试。", current), 20000);
    try {
      const body = new URLSearchParams({
        code,
        csrfmiddlewaretoken: form.querySelector("[name=csrfmiddlewaretoken]").value
      });
      const response = await fetch("/sspr/auth/dingtalk", {
        method: "POST", credentials: "same-origin", body, signal: controller.signal
      });
      const data = await response.json();
      if (current !== attempt) return;
      clearTimeout(timer);
      controller = undefined;
      if (!response.ok) return fail(data.error || "验证失败，请重试。", current);
      window.location.replace(data.next);
    } catch (_) {
      fail("身份核验请求失败，请检查网络后重试。", current);
    }
  };

  const requestCode = current => {
    if (current !== attempt) return;
    status.textContent = "正在获取钉钉授权…";
    timer = setTimeout(() => fail("钉钉授权超时，请重新验证。", current), 12000);
    try {
      window.dd.requestAuthCode({
        corpId: form.dataset.corp, clientId: form.dataset.client,
        success: result => {
          if (current !== attempt) return;
          clearTimeout(timer);
          submitCode(result, current);
        },
        fail: () => fail("钉钉验证失败，请重新验证。", current)
      });
    } catch (_) {
      fail("钉钉验证失败，请重新验证。", current);
    }
  };

  const verify = () => {
    if (busy) return;
    busy = true;
    const current = ++attempt;
    button.hidden = true;
    button.disabled = true;
    if (window.dd && typeof window.dd.requestAuthCode === "function") {
      requestCode(current);
      return;
    }
    status.textContent = "正在连接钉钉…";
    const script = document.createElement("script");
    sdkScript = script;
    script.async = true;
    script.src = sdkUrl;
    script.onload = () => {
      if (current !== attempt) return;
      clearTimeout(timer);
      sdkScript = undefined;
      if (window.dd && typeof window.dd.requestAuthCode === "function") {
        requestCode(current);
      } else {
        fail("请从钉钉工作台打开此应用。", current);
      }
    };
    script.onerror = () => fail("钉钉组件加载失败，请从钉钉工作台打开并检查网络后重试。", current);
    timer = setTimeout(() => fail("连接钉钉超时，请从钉钉工作台打开并检查网络后重试。", current), 8000);
    document.head.appendChild(script);
  };

  form.addEventListener("submit", event => {
    event.preventDefault();
    verify();
  });
  verify();
}
