document.addEventListener("DOMContentLoaded", () => {
  const defaultConfirmMessage =
    document.body?.dataset.confirmMessage || "Are you sure you want to perform this action?";

  function escapeHtml(value) {
    return String(value ?? "")
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;")
      .replace(/'/g, "&#39;");
  }

  // Initialize Lucide Icons
  if (window.lucide) {
    window.lucide.createIcons();
  }

  // --- 1. Auto-submit Forms ---
  document.querySelectorAll("[data-auto-submit]").forEach((el) => {
    el.addEventListener("change", () => {
      el.closest("form")?.submit();
    });
  });

  // --- 1.5. Dynamic Source Provider UI ---
  const sourceProviderSelect = document.getElementById("source_provider");
  const sourceProviderCatalogNode = document.getElementById("source-provider-ui-catalog");
  if (sourceProviderSelect && sourceProviderCatalogNode) {
    let sourceProviderCatalog = {};
    try {
      sourceProviderCatalog = JSON.parse(sourceProviderCatalogNode.textContent || "{}");
    } catch (_error) {
      sourceProviderCatalog = {};
    }

    const setTextContent = (selector, value) => {
      document.querySelectorAll(selector).forEach((node) => {
        node.textContent = value || "";
      });
    };

    const updateProviderBadge = (label) => {
      document.querySelectorAll("[data-config-provider-badge]").forEach((node) => {
        node.innerHTML = `<span class="badge badge-info">${escapeHtml(label || "")}</span>`;
      });
    };

    const getFieldHelpNode = (group) => {
      const children = Array.from(group.children).filter((child) => child.nodeType === Node.ELEMENT_NODE);
      return children.length > 2 ? children[children.length - 1] : null;
    };

    const updateProviderField = (fieldName, fieldMeta) => {
      if (!fieldMeta) {
        return;
      }
      const group = document.getElementById(`group-${fieldName}`);
      const input = document.getElementById(fieldName);
      if (!group || !input) {
        return;
      }

      const label = group.querySelector("label");
      if (label) {
        const requiredMarkup = fieldMeta.required
          ? '<span style="color: var(--danger); margin-left: 4px;">*</span>'
          : "";
        label.innerHTML = `${escapeHtml(fieldMeta.label || "")}${requiredMarkup}`;
      }

      const configuredProvider = (input.dataset.providerSecretConfiguredProvider || "").trim().toLowerCase();
      const activeProvider = String(sourceProviderSelect?.value || "").trim().toLowerCase();
      const isConfiguredSecret =
        input.dataset.providerSecretConfigured === "true" &&
        configuredProvider &&
        configuredProvider === activeProvider;
      if (!(fieldMeta.secret && isConfiguredSecret)) {
        input.setAttribute("placeholder", fieldMeta.placeholder || "");
      }
      if (fieldMeta.required && !(fieldMeta.secret && isConfiguredSecret)) {
        input.setAttribute("required", "required");
      } else {
        input.removeAttribute("required");
      }

      const helpNode = getFieldHelpNode(group);
      if (helpNode) {
        const helpText = fieldMeta.helpText || "";
        helpNode.textContent = helpText;
        helpNode.style.display = helpText ? "" : "none";
      }
    };

    const updateProviderSelectHelp = (description) => {
      const providerGroup = document.getElementById("group-source_provider");
      if (!providerGroup) {
        return;
      }
      const helpNode = getFieldHelpNode(providerGroup);
      if (helpNode) {
        const helpText = description || "";
        helpNode.textContent = helpText;
        helpNode.style.display = helpText ? "" : "none";
      }
    };

    const applySourceProviderUi = () => {
      const schema = sourceProviderCatalog[sourceProviderSelect.value];
      if (!schema) {
        return;
      }

      setTextContent("[data-config-provider-page-title]", schema.pageTitle || "");
      setTextContent("[data-config-provider-page-summary]", schema.pageSummary || "");
      setTextContent("[data-config-provider-current]", schema.displayName || "");
      setTextContent("[data-config-provider-guidance]", schema.sourceGuidance || "");
      setTextContent("[data-config-provider-card-title]", schema.connectorTitle || "");
      setTextContent("[data-config-provider-card-description]", schema.connectorDescription || "");
      updateProviderBadge(schema.displayName || "");
      updateProviderSelectHelp(schema.description || "");

      Object.entries(schema.fields || {}).forEach(([fieldName, fieldMeta]) => {
        updateProviderField(fieldName, fieldMeta);
      });
    };

    sourceProviderSelect.addEventListener("change", applySourceProviderUi);
    applySourceProviderUi();
  }

  // --- 1.6. Config Catalog Browsers ---
  const configForm =
    document.querySelector("form[data-config-form]") ||
    document.querySelector('form[action="/config"]') ||
    document.querySelector('form[action="/config/preview"]');
  if (configForm) {
    const sourceCatalogUrl = configForm.dataset.sourceCatalogUrl || "";
    const targetCatalogUrl = configForm.dataset.targetOuCatalogUrl || "";
    const labels = {
      loading: configForm.dataset.labelLoading || "Loading...",
      closePicker: configForm.dataset.labelClosePicker || "Close Picker",
      sourceId: configForm.dataset.labelSourceId || "Source ID",
      adDn: configForm.dataset.labelAdDn || "AD DN",
      adGuid: configForm.dataset.labelAdGuid || "AD Object GUID",
      domainRoot: configForm.dataset.labelDomainRoot || "Domain Root",
      sourceSelected: configForm.dataset.labelSourceSelected || "Selected __COUNT__ source units",
      targetSelected: configForm.dataset.labelTargetSelected || "OU selected",
      results: configForm.dataset.labelResults || "__COUNT__ results",
      noResults: configForm.dataset.labelNoResults || "No matching results",
      rowSelected: configForm.dataset.labelRowSelected || "Selected",
      rowSelect: configForm.dataset.labelRowSelect || "Click to select",
      sourceHint: configForm.dataset.messageSourceHint || "",
      targetHint: configForm.dataset.messageTargetHint || "",
      sourceLoaded: configForm.dataset.messageSourceLoaded || "",
      targetLoaded: configForm.dataset.messageTargetLoaded || "",
    };
    const sourceCatalogState = { key: "", items: [] };
    const targetCatalogState = { key: "", items: [] };
    const sourceCatalogFields = ["source_provider", "corpid", "agentid", "corpsecret"];
    const targetCatalogFields = [
      "ldap_server",
      "ldap_domain",
      "ldap_username",
      "ldap_password",
      "ldap_port",
      "ldap_use_ssl",
      "ldap_validate_cert",
      "ldap_ca_cert_path",
    ];

    const sourcePickerField = configForm.querySelector("#group-source_root_unit_ids");
    const sourceRootInput = document.getElementById("source_root_unit_ids");
    const sourceRootDisplayInput = document.getElementById("source_root_unit_display_text");
    const replaceCountLabel = (template, count) => String(template || "").replace("__COUNT__", String(count));
    const normalizeSearchValue = (value) => String(value || "").trim().toLowerCase();

    const setStatus = (target, message, isError = false) => {
      if (!target) {
        return;
      }
      target.textContent = message || "";
      target.classList.toggle("config-browser__status--error", Boolean(isError));
    };

    const setPickerMeta = (fieldName, text = "") => {
      const metaNode = document.querySelector(`[data-picker-meta-for="${fieldName}"]`);
      if (!metaNode) {
        return;
      }
      const normalized = String(text || "").trim();
      metaNode.textContent = normalized;
      metaNode.hidden = !normalized;
    };

    const updatePickerToggleLabel = (fieldRoot, isOpen) => {
      if (!fieldRoot) {
        return;
      }
      fieldRoot.querySelectorAll("[data-open-label]").forEach((button) => {
        const span = button.querySelector("span");
        const openLabel = button.dataset.openLabel || span?.dataset.defaultLabel || span?.textContent || "";
        if (span && !span.dataset.defaultLabel) {
          span.dataset.defaultLabel = openLabel;
        }
        if (span) {
          span.textContent = isOpen ? (button.dataset.closeLabel || labels.closePicker) : openLabel;
        }
        button.setAttribute("aria-expanded", String(Boolean(isOpen)));
      });
    };

    const setPickerOpenState = (fieldRoot, isOpen) => {
      if (!fieldRoot) {
        return;
      }
      fieldRoot.classList.toggle("is-open", Boolean(isOpen));
      updatePickerToggleLabel(fieldRoot, Boolean(isOpen));
    };

    const setPickerClearState = (fieldName, hasValue) => {
      document.querySelectorAll(`[data-picker-clear-field="${fieldName}"]`).forEach((button) => {
        button.hidden = !hasValue;
      });
    };

    const buildConfigFormData = (fieldNames) => {
      const formData = new FormData();
      fieldNames.forEach((fieldName) => {
        const input = configForm.querySelector(`[name="${fieldName}"]`);
        if (!input) {
          return;
        }
        if ((input.type === "checkbox" || input.type === "radio") && !input.checked) {
          return;
        }
        formData.append(fieldName, input.value);
      });
      return formData;
    };

    const setPickerSummary = (fieldName, value, summaryText = "", titleText = "") => {
      const input = document.getElementById(fieldName);
      const summaryNode = document.querySelector(`[data-picker-summary-for="${fieldName}"]`);
      if (!input || !summaryNode) {
        return;
      }
      const placeholder = summaryNode.dataset.pickerPlaceholder || "";
      input.value = value || "";
      const text = summaryText || value || placeholder;
      summaryNode.textContent = text;
      summaryNode.classList.toggle("is-placeholder", !value);
      summaryNode.setAttribute("title", titleText || text || placeholder);
      const surface = document.querySelector(`[data-picker-surface-for="${fieldName}"]`);
      surface?.classList.toggle("is-selected", Boolean(value));
      setPickerClearState(fieldName, Boolean(value));
    };

    const formatSourceSummary = (items) => {
      if (!items.length) {
        return "";
      }
      if (items.length <= 3) {
        return items.map((item) => `${item.name} [${item.department_id}]`).join(", ");
      }
      return `${items.slice(0, 2).map((item) => `${item.name} [${item.department_id}]`).join(", ")} +${items.length - 2}`;
    };

    const setSourceDisplayText = (value = "") => {
      if (sourceRootDisplayInput) {
        sourceRootDisplayInput.value = String(value || "").trim();
      }
    };

    const buildCatalogSignature = (fieldNames) =>
      fieldNames
        .map((fieldName) => {
          const input = configForm.querySelector(`[name="${fieldName}"]`);
          if (!input) {
            return `${fieldName}=`;
          }
          if (input.type === "checkbox" || input.type === "radio") {
            return `${fieldName}=${input.checked ? input.value : ""}`;
          }
          return `${fieldName}=${input.value || ""}`;
        })
        .join("|");

    const setResultsSummary = (target, { visibleCount = 0, selectedCount = 0, totalCount = 0 } = {}) => {
      if (!target) {
        return;
      }
      if (!totalCount) {
        target.textContent = "";
        target.hidden = true;
        return;
      }
      if (!visibleCount) {
        target.textContent = labels.noResults;
        target.hidden = false;
        return;
      }
      const parts = [replaceCountLabel(labels.results, visibleCount)];
      if (selectedCount) {
        parts.push(replaceCountLabel(labels.sourceSelected, selectedCount));
      }
      target.textContent = parts.join(" · ");
      target.hidden = false;
    };

    const applySourceFilter = () => {
      if (!sourcePickerField) {
        return;
      }
      const panel = sourcePickerField.querySelector("[data-config-source-browser]");
      const query = normalizeSearchValue(panel?.querySelector("[data-config-source-filter]")?.value);
      const rows = Array.from(sourcePickerField.querySelectorAll("[data-config-source-list] .config-tree-row"));
      let visibleCount = 0;
      rows.forEach((row) => {
        const matches = !query || normalizeSearchValue(row.dataset.searchText).includes(query);
        row.classList.toggle("is-filter-hidden", !matches);
        if (matches) {
          visibleCount += 1;
        }
      });
      setResultsSummary(sourcePickerField.querySelector("[data-config-source-results]"), {
        visibleCount,
        selectedCount: sourcePickerField.querySelectorAll("[data-source-unit-checkbox]:checked").length,
        totalCount: rows.length,
      });
    };

    const applyTargetFilter = (panel) => {
      if (!panel) {
        return;
      }
      const query = normalizeSearchValue(panel.querySelector("[data-config-target-filter]")?.value);
      const rows = Array.from(panel.querySelectorAll("[data-config-target-list] .config-tree-row"));
      let visibleCount = 0;
      rows.forEach((row) => {
        const matches = !query || normalizeSearchValue(row.dataset.searchText).includes(query);
        row.classList.toggle("is-filter-hidden", !matches);
        if (matches) {
          visibleCount += 1;
        }
      });
      setResultsSummary(panel.querySelector("[data-config-target-results]"), {
        visibleCount,
        totalCount: rows.length,
      });
    };

    const invalidateSourceCatalog = () => {
      sourceCatalogState.key = "";
      sourceCatalogState.items = [];
      if (sourcePickerField) {
        const sourceList = sourcePickerField.querySelector("[data-config-source-list]");
        if (sourceList) {
          sourceList.innerHTML = `<div class="config-browser__empty">${escapeHtml(labels.sourceHint)}</div>`;
        }
        const filterInput = sourcePickerField.querySelector("[data-config-source-filter]");
        if (filterInput) {
          filterInput.value = "";
        }
        setResultsSummary(sourcePickerField.querySelector("[data-config-source-results]"));
        setStatus(sourcePickerField.querySelector("[data-config-source-status]"), labels.sourceHint, false);
      }
    };

    const invalidateTargetCatalog = () => {
      targetCatalogState.key = "";
      targetCatalogState.items = [];
      configForm.querySelectorAll("[data-config-target-browser]").forEach((panel) => {
        const targetList = panel.querySelector("[data-config-target-list]");
        if (targetList) {
          targetList.innerHTML = `<div class="config-browser__empty">${escapeHtml(labels.targetHint)}</div>`;
        }
        const filterInput = panel.querySelector("[data-config-target-filter]");
        if (filterInput) {
          filterInput.value = "";
        }
        setResultsSummary(panel.querySelector("[data-config-target-results]"));
        setStatus(panel.querySelector("[data-config-target-status]"), labels.targetHint, false);
      });
    };

    const hideAllInlinePickers = () => {
      configForm.querySelectorAll("[data-config-source-browser], [data-config-target-browser]").forEach((panel) => {
        panel.hidden = true;
        setPickerOpenState(panel.closest(".picker-field"), false);
      });
    };

    const updateSourceSelectionStyles = () => {
      sourcePickerField?.querySelectorAll(".config-tree-row").forEach((row) => {
        const checkbox = row.querySelector("[data-source-unit-checkbox]");
        row.classList.toggle("is-selected", Boolean(checkbox?.checked));
      });
    };

    const syncSourceSelection = () => {
      if (!sourcePickerField) {
        return;
      }
      const selectedIds = Array.from(sourcePickerField.querySelectorAll("[data-source-unit-checkbox]:checked"))
        .map((node) => node.value)
        .filter(Boolean);
      const selectedItems = sourceCatalogState.items.filter((item) => selectedIds.includes(String(item.department_id)));
      const summaryText = formatSourceSummary(selectedItems);
      setPickerSummary(
        "source_root_unit_ids",
        selectedIds.join(", "),
        summaryText,
        selectedItems.map((item) => item.path_display || item.name).join("\n"),
      );
      setSourceDisplayText(summaryText);
      setPickerMeta(
        "source_root_unit_ids",
        selectedItems.length ? replaceCountLabel(labels.sourceSelected, selectedItems.length) : "",
      );
      updateSourceSelectionStyles();
      applySourceFilter();
      setStatus(sourcePickerField.querySelector("[data-config-source-status]"), labels.sourceLoaded, false);
    };

    const chooseTargetOu = (panel, row) => {
      const fieldName = panel?.dataset.targetField || "";
      if (!fieldName || !row) {
        return;
      }
      const targetValue = row.dataset.targetOuValue || "";
      const summaryText = row.dataset.targetOuSummary || targetValue;
      const dnText = row.dataset.targetOuDn || "";
      setPickerSummary(fieldName, targetValue, summaryText, dnText || summaryText);
      setPickerMeta(fieldName, targetValue ? labels.targetSelected : "");
      setStatus(panel.querySelector("[data-config-target-status]"), labels.targetLoaded, false);
      panel.hidden = true;
      setPickerOpenState(panel.closest(".picker-field"), false);
    };

    const renderSourceUnits = (fieldRoot, payload, signature = "") => {
      const sourceList = fieldRoot?.querySelector("[data-config-source-list]");
      if (!sourceList) {
        return;
      }
      const items = Array.isArray(payload.items) ? payload.items : [];
      const selectedIds = new Set(
        String(sourceRootInput?.value || "")
          .split(",")
          .map((item) => item.trim())
          .filter(Boolean),
      );
      sourceCatalogState.key = signature;
      sourceCatalogState.items = items;
      if (!items.length) {
        sourceList.innerHTML = `<div class="config-browser__empty">${escapeHtml(labels.sourceHint)}</div>`;
        setPickerMeta("source_root_unit_ids", selectedIds.size ? replaceCountLabel(labels.sourceSelected, selectedIds.size) : "");
        setResultsSummary(fieldRoot.querySelector("[data-config-source-results]"));
        return;
      }
      sourceList.innerHTML = items
        .map((item) => {
          const pathDisplay = item.path_display || item.name || item.department_id;
          const isSelected = selectedIds.has(String(item.department_id));
          const searchText = [item.name, item.department_id, pathDisplay].filter(Boolean).join(" ");
          return `
            <label class="config-tree-row ${isSelected ? "is-selected" : ""}" style="--tree-level:${Number(item.level || 0)}" data-search-text="${escapeHtml(searchText)}">
              <div class="config-tree-row__main">
                <input type="checkbox" data-source-unit-checkbox value="${escapeHtml(item.department_id)}" ${
                  isSelected ? "checked" : ""
                }>
                <div class="config-tree-row__copy">
                  <div class="config-tree-row__title">${escapeHtml(item.name || item.department_id)}</div>
                  <div class="config-tree-row__detail">${escapeHtml(pathDisplay)}</div>
                </div>
              </div>
              <div class="config-tree-row__trailing">
                <span class="badge badge-info">${escapeHtml(labels.sourceId)} ${escapeHtml(item.department_id)}</span>
              </div>
            </label>
          `;
        })
        .join("");
      sourceList.querySelectorAll("[data-source-unit-checkbox]").forEach((checkbox) => {
        checkbox.addEventListener("change", syncSourceSelection);
      });
      syncSourceSelection();
    };

    const renderTargetOus = (fieldRoot, payload, signature = "") => {
      const targetList = fieldRoot?.querySelector("[data-config-target-list]");
      const fieldName = fieldRoot?.dataset.targetField || "";
      if (!targetList || !fieldName) {
        return;
      }
      const items = Array.isArray(payload.items) ? payload.items : [];
      const currentValue = String(document.getElementById(fieldName)?.value || "").trim();
      targetCatalogState.key = signature;
      targetCatalogState.items = items;
      if (!items.length) {
        targetList.innerHTML = `<div class="config-browser__empty">${escapeHtml(labels.targetHint)}</div>`;
        setResultsSummary(fieldRoot.querySelector("[data-config-target-results]"));
        return;
      }
      targetList.innerHTML = items
        .map((item) => {
          const value = item.path_value || "";
          const pathDisplay = item.path_display || labels.domainRoot;
          const isSelected = Boolean(currentValue && currentValue === value);
          const guidMarkup = item.guid
            ? `<div class="config-tree-row__meta">${escapeHtml(labels.adGuid)}: ${escapeHtml(item.guid)}</div>`
            : "";
          const searchText = [item.name, pathDisplay, item.dn, item.guid].filter(Boolean).join(" ");
          return `
            <div
              class="config-tree-row config-tree-row--target config-tree-row--interactive ${isSelected ? "is-selected" : ""}"
              style="--tree-level:${Number(item.level || 0)}"
              tabindex="0"
              role="button"
              data-target-ou-row
              data-target-ou-value="${escapeHtml(value)}"
              data-target-ou-summary="${escapeHtml(pathDisplay)}"
              data-target-ou-dn="${escapeHtml(item.dn || "")}"
              data-search-text="${escapeHtml(searchText)}"
            >
              <div class="config-tree-row__copy">
                <div class="config-tree-row__title">${escapeHtml(item.name || labels.domainRoot)}</div>
                <div class="config-tree-row__detail">${escapeHtml(pathDisplay)}</div>
                <div class="config-tree-row__meta">${escapeHtml(labels.adDn)}: ${escapeHtml(item.dn || "")}</div>
                ${guidMarkup}
              </div>
              <div class="config-tree-row__trailing">
                <span class="config-tree-row__status ${isSelected ? "is-selected" : ""}">${escapeHtml(
                  isSelected ? labels.rowSelected : labels.rowSelect,
                )}</span>
              </div>
            </div>
          `;
        })
        .join("");
      targetList.querySelectorAll("[data-target-ou-row]").forEach((row) => {
        row.addEventListener("click", () => {
          chooseTargetOu(fieldRoot, row);
        });
        row.addEventListener("keydown", (event) => {
          if (event.key === "Enter" || event.key === " ") {
            event.preventDefault();
            chooseTargetOu(fieldRoot, row);
          }
        });
      });
      applyTargetFilter(fieldRoot);
    };

    const postCatalogRequest = async (url, formData) => {
      const response = await fetch(url, {
        method: "POST",
        body: formData,
        credentials: "same-origin",
      });
      const payload = await response.json().catch(() => ({ ok: false, error: "Request failed" }));
      if (!response.ok || payload.ok === false) {
        throw new Error(payload.error || "Request failed");
      }
      return payload;
    };

    const loadSourceCatalog = async () => {
      const status = sourcePickerField?.querySelector("[data-config-source-status]");
      const loadButton = sourcePickerField?.querySelector("[data-config-load-source-units]");
      if (!sourceCatalogUrl || !loadButton || !sourcePickerField) {
        return;
      }
      const signature = buildCatalogSignature(sourceCatalogFields);
      setStatus(status, labels.loading, false);
      loadButton.disabled = true;
      try {
        const payload = await postCatalogRequest(
          sourceCatalogUrl,
          buildConfigFormData(["csrf_token", ...sourceCatalogFields]),
        );
        renderSourceUnits(sourcePickerField, payload, signature);
        setStatus(status, labels.sourceLoaded, false);
      } catch (error) {
        renderSourceUnits(sourcePickerField, { items: [] }, "");
        setStatus(status, error.message || labels.sourceHint, true);
      } finally {
        loadButton.disabled = false;
      }
    };

    const loadTargetCatalog = async (panel) => {
      const status = panel?.querySelector("[data-config-target-status]");
      const loadButton = panel?.querySelector("[data-config-load-target-ous]");
      if (!targetCatalogUrl || !loadButton || !panel) {
        return;
      }
      const signature = buildCatalogSignature(targetCatalogFields);
      setStatus(status, labels.loading, false);
      loadButton.disabled = true;
      try {
        const payload = await postCatalogRequest(
          targetCatalogUrl,
          buildConfigFormData(["csrf_token", ...targetCatalogFields]),
        );
        renderTargetOus(panel, payload, signature);
        setStatus(status, labels.targetLoaded, false);
      } catch (error) {
        renderTargetOus(panel, { items: [] }, "");
        setStatus(status, error.message || labels.targetHint, true);
      } finally {
        loadButton.disabled = false;
      }
    };

    const clearPickerValue = (fieldName) => {
      setPickerSummary(fieldName, "", "", "");
      setPickerMeta(fieldName, "");
      if (fieldName === "source_root_unit_ids" && sourcePickerField) {
        setSourceDisplayText("");
        sourcePickerField.querySelectorAll("[data-source-unit-checkbox]").forEach((checkbox) => {
          checkbox.checked = false;
        });
        syncSourceSelection();
        return;
      }
      const targetPanel = Array.from(configForm.querySelectorAll("[data-config-target-browser]")).find(
        (panel) => panel.dataset.targetField === fieldName,
      );
      if (targetPanel && targetCatalogState.items.length) {
        renderTargetOus(targetPanel, { items: targetCatalogState.items }, targetCatalogState.key);
      }
    };

    const initializePickerMeta = () => {
      const selectedSourceIds = String(sourceRootInput?.value || "")
        .split(",")
        .map((item) => item.trim())
        .filter(Boolean);
      setPickerMeta(
        "source_root_unit_ids",
        selectedSourceIds.length ? replaceCountLabel(labels.sourceSelected, selectedSourceIds.length) : "",
      );
      setPickerClearState("source_root_unit_ids", selectedSourceIds.length > 0);
      ["directory_root_ou_path", "disabled_users_ou_path", "custom_group_ou_path"].forEach((fieldName) => {
        const hasValue = Boolean(document.getElementById(fieldName)?.value);
        setPickerMeta(fieldName, hasValue ? labels.targetSelected : "");
        setPickerClearState(fieldName, hasValue);
        setPickerOpenState(document.getElementById(`group-${fieldName}`), false);
      });
      setPickerOpenState(sourcePickerField, false);
    };

    const ensureSourceSummaryDisplay = async () => {
      if (!sourcePickerField || !sourceRootInput || !sourceRootInput.value.trim()) {
        return;
      }
      const currentSummary = String(sourceRootDisplayInput?.value || "").trim();
      const rawIds = String(sourceRootInput.value || "").trim();
      if (currentSummary && currentSummary !== rawIds) {
        return;
      }
      try {
        await loadSourceCatalog();
      } catch (_error) {
        // Keep the persisted ID-based summary if background resolution fails.
      }
    };

    sourceCatalogFields.forEach((fieldName) => {
      configForm.querySelectorAll(`[name="${fieldName}"]`).forEach((input) => {
        input.addEventListener("change", invalidateSourceCatalog);
        input.addEventListener("input", invalidateSourceCatalog);
      });
    });
    targetCatalogFields.forEach((fieldName) => {
      configForm.querySelectorAll(`[name="${fieldName}"]`).forEach((input) => {
        input.addEventListener("change", invalidateTargetCatalog);
        input.addEventListener("input", invalidateTargetCatalog);
      });
    });

    sourcePickerField?.querySelector("[data-config-source-filter]")?.addEventListener("input", applySourceFilter);

    sourcePickerField?.querySelector("[data-config-open-source-browser]")?.addEventListener("click", async () => {
      const panel = sourcePickerField.querySelector("[data-config-source-browser]");
      if (!panel) {
        return;
      }
      if (!panel.hidden) {
        panel.hidden = true;
        setPickerOpenState(sourcePickerField, false);
        return;
      }
      hideAllInlinePickers();
      panel.hidden = false;
      setPickerOpenState(sourcePickerField, true);
      panel.scrollIntoView({ block: "nearest", behavior: "smooth" });
      panel.querySelector("[data-config-source-filter]")?.focus();
      const signature = buildCatalogSignature(sourceCatalogFields);
      if (sourceCatalogState.items.length && sourceCatalogState.key === signature) {
        renderSourceUnits(sourcePickerField, { items: sourceCatalogState.items }, signature);
        setStatus(sourcePickerField.querySelector("[data-config-source-status]"), labels.sourceLoaded, false);
        return;
      }
      await loadSourceCatalog();
    });

    sourcePickerField?.querySelector("[data-config-load-source-units]")?.addEventListener("click", async () => {
      if (!sourcePickerField) {
        return;
      }
      sourceCatalogState.key = "";
      sourceCatalogState.items = [];
      await loadSourceCatalog();
    });

    sourcePickerField?.querySelector("[data-config-close-source-browser]")?.addEventListener("click", () => {
      const panel = sourcePickerField.querySelector("[data-config-source-browser]");
      if (panel) {
        panel.hidden = true;
        setPickerOpenState(sourcePickerField, false);
      }
    });

    configForm.querySelectorAll("[data-config-target-browser]").forEach((panel) => {
      const fieldRoot = panel.closest(".picker-field");
      const fieldName = panel.dataset.targetField || "";
      if (!fieldRoot || !fieldName) {
        return;
      }
      panel.querySelector("[data-config-target-filter]")?.addEventListener("input", () => applyTargetFilter(panel));
      fieldRoot.querySelector("[data-config-open-target-browser]")?.addEventListener("click", async () => {
        if (!panel.hidden) {
          panel.hidden = true;
          setPickerOpenState(fieldRoot, false);
          return;
        }
        hideAllInlinePickers();
        panel.hidden = false;
        setPickerOpenState(fieldRoot, true);
        panel.scrollIntoView({ block: "nearest", behavior: "smooth" });
        panel.querySelector("[data-config-target-filter]")?.focus();
        const signature = buildCatalogSignature(targetCatalogFields);
        if (targetCatalogState.items.length && targetCatalogState.key === signature) {
          renderTargetOus(panel, { items: targetCatalogState.items }, signature);
          setStatus(panel.querySelector("[data-config-target-status]"), labels.targetLoaded, false);
          return;
        }
        await loadTargetCatalog(panel);
      });

      panel.querySelector("[data-config-load-target-ous]")?.addEventListener("click", async () => {
        targetCatalogState.key = "";
        targetCatalogState.items = [];
        await loadTargetCatalog(panel);
      });

      panel.querySelector("[data-config-close-target-browser]")?.addEventListener("click", () => {
        panel.hidden = true;
        setPickerOpenState(fieldRoot, false);
      });
    });

    configForm.querySelectorAll("[data-picker-clear]").forEach((button) => {
      button.addEventListener("click", () => {
        const fieldName = button.dataset.pickerClearField || "";
        if (!fieldName) {
          return;
        }
        clearPickerValue(fieldName);
      });
    });

    initializePickerMeta();
    void ensureSourceSummaryDisplay();
  }

  // --- 2. Confirmation & Loading States ---
  document.querySelectorAll("button[data-confirm], a[data-confirm]").forEach((el) => {
    el.addEventListener("click", (e) => {
      const message = el.getAttribute("data-confirm") || defaultConfirmMessage;
      if (!confirm(message)) {
        e.preventDefault();
        e.stopImmediatePropagation();
        return;
      }
      
      if (el.tagName === "BUTTON" && el.type === "submit") {
        setLoading(el);
      }
    });
  });

  // --- 3. Global Form Loading Feedback ---
  document.querySelectorAll("form").forEach(form => {
    form.addEventListener("submit", (e) => {
      const submitBtn = e.submitter instanceof HTMLElement
        ? e.submitter
        : form.querySelector('button[type="submit"]');
      if (submitBtn && !submitBtn.hasAttribute('data-confirm')) {
        // Slight delay to allow native validation
        setTimeout(() => {
          if (!e.defaultPrevented) {
            setLoading(submitBtn);
          }
        }, 10);
      }
    });
  });

  function setLoading(btn) {
    btn.classList.add("btn-loading");
    // Preserve width to prevent layout shift
    const width = btn.offsetWidth;
    btn.style.width = width + 'px';
  }

  // --- 4. Toast (Flash) Notifications ---
  const flashMessages = document.querySelectorAll(".flash");
  flashMessages.forEach((flash) => {
    // Auto-dismiss success messages
    if (flash.classList.contains("success")) {
      setTimeout(() => {
        dismissFlash(flash);
      }, 6000);
    }
  });

  document.querySelectorAll("[data-dismiss-closest]").forEach((el) => {
    el.addEventListener("click", () => {
      const selector = el.getAttribute("data-dismiss-closest");
      const target = selector ? el.closest(selector) : null;
      if (target) {
        dismissFlash(target);
      }
    });
  });

  window.dismissFlash = function(el) {
    el.style.transform = "translateX(120%)";
    el.style.opacity = "0";
    el.style.transition = "all 0.5s cubic-bezier(0.4, 0, 0.2, 1)";
    setTimeout(() => el.remove(), 500);
  };

  // --- 5. Navigation Active State ---
  const currentPath = window.location.pathname;
  document.querySelectorAll("nav a").forEach(link => {
    const href = link.getAttribute("href");
    if (href === currentPath || (href !== "/" && currentPath.startsWith(href))) {
      link.classList.add("active");
    }
  });

  // --- 6. Table Row Hover Enhancements ---
  document.querySelectorAll("tr").forEach(tr => {
    tr.addEventListener("mouseenter", () => {
      tr.style.transition = "background-color 0.2s ease";
    });
  });
  // --- 7. TomSelect Dynamic Field Selectors ---
  if (typeof TomSelect !== "undefined") {
    const bindTomSelectRemote = (selector, url) => {
      document.querySelectorAll(selector).forEach(el => {
        // Only bind if not already bound
        if (el.tomselect) return;

        new TomSelect(el, {
          create: true, // allow manual entry if fallback needed
          valueField: 'id',
          labelField: 'name',
          searchField: ['name', 'id'],
          preload: true,
          plugins: ['remove_button', 'clear_button'],
          load: function(query, callback) {
            fetch(url)
              .then(response => response.json())
              .then(json => {
                if (json.ok && Array.isArray(json.options)) {
                  callback(json.options);
                } else {
                  callback();
                }
              }).catch(() => {
                callback();
              });
          }
        });
      });
    };

    // Initialize the remote options
    bindTomSelectRemote("input[name='root_department_ids']", "/api/metadata/departments");
    bindTomSelectRemote("input[name='managed_tag_ids']", "/api/metadata/tags");
    bindTomSelectRemote("input[name='managed_external_chat_ids']", "/api/metadata/external-chats");

    // Initialize local tag inputs without remote search
    document.querySelectorAll("textarea[name='soft_excluded_groups']").forEach(el => {
      if (el.tomselect) return;
      new TomSelect(el, {
        create: true,
        plugins: ['remove_button', 'clear_button'],
        persist: false,
        createOnBlur: true
      });
    });
  }
});
