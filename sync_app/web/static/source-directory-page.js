(() => {
  "use strict";

  const refreshPoll = document.querySelector("[data-source-refresh-poll]");
  if (!(refreshPoll instanceof HTMLElement)) return;

  const statusUrl = refreshPoll.dataset.statusUrl || "/api/source-directory/status";
  const refreshMessage = refreshPoll.querySelector("[data-source-refresh-message]");
  let pollCount = 0;

  const setRefreshMessage = (value) => {
    if (refreshMessage instanceof HTMLElement && value) {
      refreshMessage.textContent = value;
    }
  };

  const pollRefreshStatus = async () => {
    pollCount += 1;
    try {
      const response = await fetch(statusUrl, {
        credentials: "same-origin",
        cache: "no-store",
        headers: { Accept: "application/json" },
      });
      if (!response.ok) throw new Error(`status ${response.status}`);
      const payload = await response.json();
      const refreshStatus = String(payload?.latest_refresh?.status || "").toLowerCase();
      if (refreshStatus === "succeeded") {
        setRefreshMessage(refreshPoll.dataset.succeededText);
        window.setTimeout(() => window.location.reload(), 250);
        return;
      }
      if (refreshStatus === "failed") {
        setRefreshMessage(refreshPoll.dataset.failedText);
        window.setTimeout(() => window.location.reload(), 250);
        return;
      }
      if (pollCount >= 20) {
        setRefreshMessage(refreshPoll.dataset.longRunningText);
      } else {
        setRefreshMessage(refreshPoll.dataset.refreshingText);
      }
    } catch (_error) {
      if (pollCount >= 20) setRefreshMessage(refreshPoll.dataset.longRunningText);
    }
    window.setTimeout(pollRefreshStatus, 3000);
  };

  window.setTimeout(pollRefreshStatus, 1000);
})();
