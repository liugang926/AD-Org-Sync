(() => {
  "use strict";

  const checkboxes = Array.from(document.querySelectorAll("[data-source-user-checkbox]"));
  const pageCheckbox = document.querySelector("[data-select-page-checkbox]");
  const modeInput = document.querySelector("[data-selection-mode]");
  const status = document.querySelector("[data-source-selection-status]");
  if (!modeInput || !status) return;

  const updateStatus = () => {
    if (modeInput.value === "all_filtered") {
      status.textContent = status.dataset.allFilteredText || "";
      return;
    }
    const count = checkboxes.filter((item) => item.checked).length;
    const template = count === 1
      ? status.dataset.oneSelectedTemplate
      : status.dataset.manySelectedTemplate;
    status.textContent = (template || "").replace("{count}", String(count));
    if (pageCheckbox) {
      pageCheckbox.checked = Boolean(checkboxes.length) && count === checkboxes.length;
      pageCheckbox.indeterminate = count > 0 && count < checkboxes.length;
    }
  };

  const selectCurrentPage = () => {
    modeInput.value = "explicit";
    checkboxes.forEach((item) => { item.checked = true; });
    updateStatus();
  };

  document.querySelector("[data-select-current-page]")?.addEventListener("click", selectCurrentPage);
  pageCheckbox?.addEventListener("change", () => {
    modeInput.value = "explicit";
    checkboxes.forEach((item) => { item.checked = pageCheckbox.checked; });
    updateStatus();
  });
  document.querySelector("[data-select-all-filtered]")?.addEventListener("click", () => {
    modeInput.value = "all_filtered";
    checkboxes.forEach((item) => { item.checked = false; });
    updateStatus();
  });
  document.querySelector("[data-clear-source-selection]")?.addEventListener("click", () => {
    modeInput.value = "explicit";
    checkboxes.forEach((item) => { item.checked = false; });
    updateStatus();
  });
  checkboxes.forEach((item) => item.addEventListener("change", () => {
    modeInput.value = "explicit";
    updateStatus();
  }));
  updateStatus();
})();
