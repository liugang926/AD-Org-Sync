(function () {
  "use strict";

  function announce(message) {
    var live = document.getElementById("sspr-live");
    if (live) live.textContent = message || "";
  }

  function localizeTimes() {
    var locale = document.documentElement.lang || navigator.language || "en";
    var absolute = new Intl.DateTimeFormat(locale, {
      year: "numeric", month: "short", day: "numeric", hour: "2-digit", minute: "2-digit", timeZoneName: "short"
    });
    var relative = new Intl.RelativeTimeFormat(locale, { numeric: "auto" });
    var units = [["year", 31536000], ["month", 2592000], ["week", 604800], ["day", 86400], ["hour", 3600], ["minute", 60], ["second", 1]];
    document.querySelectorAll("time[data-local-time]").forEach(function (element) {
      var raw = element.getAttribute("datetime") || "";
      var parsed = new Date(raw);
      if (!raw || Number.isNaN(parsed.getTime())) return;
      var delta = (parsed.getTime() - Date.now()) / 1000;
      var selected = units.find(function (item) { return Math.abs(delta) >= item[1]; }) || units[units.length - 1];
      var display = absolute.format(parsed) + " · " + relative.format(Math.round(delta / selected[1]), selected[0]);
      element.textContent = display;
      element.title = raw;
      element.setAttribute("aria-label", display + "; " + raw);
    });
  }

  function startDingTalkAuthentication(root) {
    var retry = root.querySelector("[data-sspr-retry]");
    var title = root.querySelector("h2");
    var fail = function (message) {
      if (title) title.textContent = root.dataset.errorTitle || "Verification failed";
      var paragraph = root.querySelector("p");
      if (paragraph) paragraph.textContent = message || root.dataset.errorMessage || "Try again.";
      var spinner = root.querySelector(".sspr-spinner");
      if (spinner) spinner.hidden = true;
      if (retry) retry.hidden = false;
      announce(message || root.dataset.errorMessage);
    };
    var exchange = function (authCode) {
      announce(root.dataset.submittingMessage);
      fetch(root.dataset.authUrl, {
        method: "POST",
        credentials: "same-origin",
        headers: { "Content-Type": "application/json", "X-Requested-With": "XMLHttpRequest" },
        body: JSON.stringify({ authCode: authCode, state: root.dataset.state })
      }).then(function (response) {
        return response.json().then(function (payload) {
          if (!response.ok) throw new Error(payload.message || root.dataset.errorMessage);
          return payload;
        });
      }).then(function (payload) {
        window.location.replace(payload.nextUrl || "/sspr/account");
      }).catch(function (error) {
        fail(error && error.message ? error.message : root.dataset.errorMessage);
      });
    };
    var invoke = function () {
      if (!window.dd || typeof window.dd.requestAuthCode !== "function") {
        fail(root.dataset.errorMessage);
        return;
      }
      window.dd.requestAuthCode({
        corpId: root.dataset.corpId,
        clientId: root.dataset.clientId,
        onSuccess: function (result) {
          var code = result && (result.code || result.authCode);
          if (!code) return fail(root.dataset.errorMessage);
          exchange(code);
        },
        onFail: function () { fail(root.dataset.errorMessage); }
      });
    };
    if (window.dd && typeof window.dd.ready === "function") {
      window.dd.ready(invoke);
      if (typeof window.dd.error === "function") window.dd.error(function () { fail(root.dataset.errorMessage); });
    } else {
      invoke();
    }
  }

  function setupPasswordForm(form) {
    var password = form.querySelector("#new_password");
    var confirmation = form.querySelector("#confirm_password");
    var minimum = parseInt(form.dataset.minLength || "12", 10);
    var identity = (form.dataset.identityValues || "").toLowerCase().split(/[^0-9a-z\u4e00-\u9fff]+/).filter(function (item) { return item.length >= 3; });
    var rules = {
      length: form.querySelector('[data-password-rule="length"]'),
      complexity: form.querySelector('[data-password-rule="complexity"]'),
      identity: form.querySelector('[data-password-rule="identity"]'),
      match: form.querySelector('[data-password-rule="match"]')
    };
    var setRule = function (element, valid, touched) {
      if (!element) return;
      element.classList.toggle("is-valid", valid);
      element.classList.toggle("is-invalid", touched && !valid);
      var icon = element.querySelector("span");
      if (icon) icon.textContent = valid ? "✓" : touched ? "×" : "○";
    };
    var validate = function () {
      var value = password.value || "";
      var folded = value.toLowerCase();
      var classCount = [/[A-Z]/.test(value), /[a-z]/.test(value), /[0-9]/.test(value), /[^A-Za-z0-9]/.test(value)].filter(Boolean).length;
      setRule(rules.length, value.length >= minimum, value.length > 0);
      setRule(rules.complexity, classCount === 4, value.length > 0);
      setRule(rules.identity, !identity.some(function (item) { return folded.indexOf(item) !== -1; }), value.length > 0);
      setRule(rules.match, value.length > 0 && value === confirmation.value, confirmation.value.length > 0);
    };
    password.addEventListener("input", validate);
    confirmation.addEventListener("input", validate);
    form.querySelectorAll("[data-password-toggle]").forEach(function (button) {
      button.addEventListener("click", function () {
        var input = document.getElementById(button.dataset.passwordToggle);
        if (!input) return;
        var showing = input.type === "text";
        input.type = showing ? "password" : "text";
        button.textContent = showing ? button.dataset.showLabel : button.dataset.hideLabel;
        button.setAttribute("aria-pressed", showing ? "false" : "true");
      });
    });
    form.addEventListener("submit", function () {
      var submit = form.querySelector("[data-sspr-submit]");
      if (submit) {
        submit.disabled = true;
        submit.textContent = form.dataset.submittingMessage || submit.textContent;
      }
      announce(form.dataset.submittingMessage);
    });
  }

  document.addEventListener("DOMContentLoaded", function () {
    localizeTimes();
    var start = document.querySelector("[data-sspr-start]");
    if (start) {
      announce(start.dataset.submittingMessage);
      if (typeof start.requestSubmit === "function") start.requestSubmit();
      else start.submit();
    }
    var auth = document.querySelector("[data-sspr-auth]");
    if (auth) startDingTalkAuthentication(auth);
    var form = document.querySelector("[data-sspr-reset-form]");
    if (form) setupPasswordForm(form);
  });
}());
