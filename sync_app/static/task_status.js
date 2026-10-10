(() => {
  "use strict";
  const container = document.querySelector("[data-job-poll]");
  if (!container) return;
  const tracked = new Map(Array.from(container.querySelectorAll('[data-job-id][data-active="true"]'))
    .map(node => [node.dataset.jobId, node]));
  const notice = container.querySelector("[data-poll-status]");
  if (!tracked.size || !notice) return;

  const updateText = (node, selector, value) => {
    const target = node.querySelector(selector);
    if (target) target.textContent = value;
  };
  async function poll() {
    if (document.hidden) {
      window.setTimeout(poll, 5000);
      return;
    }
    const controller = new AbortController();
    const timeout = window.setTimeout(() => controller.abort(), 10000);
    let retry = true;
    try {
      const url = new URL(container.dataset.jobPoll, window.location.origin);
      for (const id of tracked.keys()) url.searchParams.append("id", id);
      const response = await fetch(url, {credentials: "same-origin", cache: "no-store", signal: controller.signal});
      if (response.redirected || response.status === 401 || response.status === 403) {
        notice.textContent = "登录已失效或无权查看任务，请刷新页面后重新登录。";
        retry = false;
        return;
      }
      if (!response.ok) throw new Error("Status unavailable");
      const data = await response.json();
      if (!Array.isArray(data.jobs) || data.jobs.length !== tracked.size) throw new Error("Incomplete status response");
      for (const job of data.jobs) {
        const node = tracked.get(job.id);
        if (!node || typeof job.active !== "boolean") throw new Error("Invalid status response");
        updateText(node, "[data-job-status]", job.label);
        node.querySelector("[data-job-status]").className = "status-pill tone-" + job.tone;
        updateText(node, "[data-job-message]", job.message);
        updateText(node, "[data-job-elapsed]", job.elapsed);
        if (!job.active) {
          node.dataset.active = "false";
          tracked.delete(job.id);
          if (container.dataset.reloadOnFinish === "true") {
            retry = false;
            window.location.reload();
            return;
          }
        }
      }
      notice.textContent = tracked.size ? "任务状态每 5 秒自动更新，无需重复提交。" : "任务状态已更新，可查看详情。";
    } catch (_) {
      notice.textContent = "暂时无法更新任务状态，将自动重试；这不代表后台任务失败。也可手动刷新页面。";
    } finally {
      window.clearTimeout(timeout);
      if (retry && tracked.size) window.setTimeout(poll, 5000);
    }
  }
  window.setTimeout(poll, 5000);
})();
