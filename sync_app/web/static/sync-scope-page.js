(() => {
  "use strict";

  const checkboxes = Array.from(document.querySelectorAll("[data-source-user-checkbox]"));
  const pageCheckbox = document.querySelector("[data-select-page-checkbox]");
  const modeInput = document.querySelector("[data-selection-mode]");
  const status = document.querySelector("[data-source-selection-status]");
  const scopeSelect = document.querySelector(
    '#sync-scope-selection-form select[name="scope_type"]',
  );
  const createCandidateButtons = Array.from(
    document.querySelectorAll("[data-select-create-candidate]"),
  );
  const prepareCreationsButton = document.querySelector(
    "[data-prepare-account-creations]",
  );
  const eligibleSourceUserIds = new Set(
    createCandidateButtons.map((button) => button.dataset.selectCreateCandidate || ""),
  );
  const checkboxBySourceUserId = new Map(
    checkboxes.map((checkbox) => [checkbox.value, checkbox]),
  );
  if (!modeInput || !status) return;

  const updateCreationControls = () => {
    const selected = checkboxes.filter((item) => item.checked);
    const canPrepare = modeInput.value === "explicit"
      && selected.length > 0
      && selected.every((item) => eligibleSourceUserIds.has(item.value));
    if (prepareCreationsButton instanceof HTMLButtonElement) {
      prepareCreationsButton.disabled = !canPrepare;
      prepareCreationsButton.setAttribute("aria-disabled", canPrepare ? "false" : "true");
    }
    createCandidateButtons.forEach((button) => {
      if (!(button instanceof HTMLButtonElement)) return;
      const sourceUserId = button.dataset.selectCreateCandidate || "";
      const checkbox = checkboxBySourceUserId.get(sourceUserId);
      const label = button.querySelector("span");
      if (!button.dataset.defaultText && label) {
        button.dataset.defaultText = label.textContent || "";
      }
      const selectedForCreation = Boolean(checkbox?.checked);
      button.disabled = selectedForCreation;
      button.setAttribute("aria-pressed", selectedForCreation ? "true" : "false");
      if (label) {
        label.textContent = selectedForCreation
          ? button.dataset.selectedText || button.dataset.defaultText || ""
          : button.dataset.defaultText || "";
      }
    });
  };

  const updateStatus = () => {
    if (modeInput.value === "all_filtered") {
      status.textContent = status.dataset.allFilteredText || "";
      updateCreationControls();
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
    updateCreationControls();
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
  document.querySelector("[data-select-create-candidates]")?.addEventListener("click", () => {
    modeInput.value = "explicit";
    checkboxes.forEach((item) => {
      item.checked = eligibleSourceUserIds.has(item.value);
    });
    if (scopeSelect instanceof HTMLSelectElement) scopeSelect.value = "selected_users";
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
  createCandidateButtons.forEach((button) => button.addEventListener("click", () => {
    const checkbox = checkboxBySourceUserId.get(
      button.dataset.selectCreateCandidate || "",
    );
    if (!checkbox) return;
    modeInput.value = "explicit";
    checkbox.checked = true;
    if (scopeSelect instanceof HTMLSelectElement) scopeSelect.value = "selected_users";
    updateStatus();
  }));

  updateStatus();
})();
