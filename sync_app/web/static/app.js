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

      const isConfiguredSecret = input.dataset.providerSecretConfigured === "true";
      if (!(fieldMeta.secret && isConfiguredSecret)) {
        input.setAttribute("placeholder", fieldMeta.placeholder || "");
      }
      if (fieldMeta.required) {
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
  const configForm = document.querySelector('form[action="/config/preview"]');
  if (configForm) {
    const sourceCatalogUrl = configForm.dataset.sourceCatalogUrl || "";
    const targetCatalogUrl = configForm.dataset.targetOuCatalogUrl || "";
    const labels = {
      loading: configForm.dataset.labelLoading || "Loading...",
      loadSource: configForm.dataset.labelLoadSource || "Load Source Unit Tree",
      closePicker: configForm.dataset.labelClosePicker || "Close Picker",
      loadTarget: configForm.dataset.labelLoadTarget || "Load AD OU Tree",
      selectOu: configForm.dataset.labelSelectOu || "Select This OU",
      sourceId: configForm.dataset.labelSourceId || "Source ID",
      adDn: configForm.dataset.labelAdDn || "AD DN",
      adGuid: configForm.dataset.labelAdGuid || "AD Object GUID",
      domainRoot: configForm.dataset.labelDomainRoot || "Domain Root",
      sourceHint: configForm.dataset.messageSourceHint || "",
      targetHint: configForm.dataset.messageTargetHint || "",
      sourceLoaded: configForm.dataset.messageSourceLoaded || "",
      targetLoaded: configForm.dataset.messageTargetLoaded || "",
    };
    let sourceCatalogCache = [];
    let targetCatalogCache = [];

    const sourcePickerField = configForm.querySelector("#group-source_root_unit_ids");
    const sourceRootInput = document.getElementById("source_root_unit_ids");

    const setStatus = (target, message, isError = false) => {
      if (!target) {
        return;
      }
      target.textContent = message || "";
      target.classList.toggle("config-browser__status--error", Boolean(isError));
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

    const hideAllInlinePickers = () => {
      configForm.querySelectorAll("[data-config-source-browser], [data-config-target-browser]").forEach((panel) => {
        panel.hidden = true;
      });
    };

    const syncSourceSelection = () => {
      if (!sourcePickerField) {
        return;
      }
      const selectedIds = Array.from(sourcePickerField.querySelectorAll("[data-source-unit-checkbox]:checked"))
        .map((node) => node.value)
        .filter(Boolean);
      const selectedItems = sourceCatalogCache.filter((item) => selectedIds.includes(String(item.department_id)));
      setPickerSummary(
        "source_root_unit_ids",
        selectedIds.join(", "),
        formatSourceSummary(selectedItems),
        selectedItems.map((item) => item.path_display || item.name).join("\n"),
      );
      setStatus(sourcePickerField.querySelector("[data-config-source-status]"), labels.sourceLoaded, false);
    };

    const renderSourceUnits = (fieldRoot, payload) => {
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
      sourceCatalogCache = items;
      if (!items.length) {
        sourceList.innerHTML = `<div class="config-browser__empty">${escapeHtml(labels.sourceHint)}</div>`;
        return;
      }
      sourceList.innerHTML = items
        .map((item) => {
          const pathDisplay = item.path_display || item.name || item.department_id;
          return `
            <label class="config-tree-row" style="--tree-level:${Number(item.level || 0)}">
              <div class="config-tree-row__main">
                <input type="checkbox" data-source-unit-checkbox value="${escapeHtml(item.department_id)}" ${
                  selectedIds.has(String(item.department_id)) ? "checked" : ""
                }>
                <div class="config-tree-row__copy">
                  <div class="config-tree-row__title">${escapeHtml(item.name || item.department_id)}</div>
                  <div class="config-tree-row__detail">${escapeHtml(pathDisplay)}</div>
                </div>
              </div>
              <span class="badge badge-info">${escapeHtml(labels.sourceId)} ${escapeHtml(item.department_id)}</span>
            </label>
          `;
        })
        .join("");
      sourceList.querySelectorAll("[data-source-unit-checkbox]").forEach((checkbox) => {
        checkbox.addEventListener("change", syncSourceSelection);
      });
      syncSourceSelection();
    };

    const renderTargetOus = (fieldRoot, payload) => {
      const targetList = fieldRoot?.querySelector("[data-config-target-list]");
      const fieldName = fieldRoot?.dataset.targetField || "";
      if (!targetList || !fieldName) {
        return;
      }
      const items = Array.isArray(payload.items) ? payload.items : [];
      targetCatalogCache = items;
      if (!items.length) {
        targetList.innerHTML = `<div class="config-browser__empty">${escapeHtml(labels.targetHint)}</div>`;
        return;
      }
      targetList.innerHTML = items
        .map((item) => {
          const value = item.path_value || "";
          const pathDisplay = item.path_display || labels.domainRoot;
          const guidMarkup = item.guid
            ? `<div class="config-tree-row__meta">${escapeHtml(labels.adGuid)}: ${escapeHtml(item.guid)}</div>`
            : "";
          return `
            <div class="config-tree-row config-tree-row--target" style="--tree-level:${Number(item.level || 0)}">
              <div class="config-tree-row__copy">
                <div class="config-tree-row__title">${escapeHtml(item.name || labels.domainRoot)}</div>
                <div class="config-tree-row__detail">${escapeHtml(pathDisplay)}</div>
                <div class="config-tree-row__meta">${escapeHtml(labels.adDn)}: ${escapeHtml(item.dn || "")}</div>
                ${guidMarkup}
              </div>
              <button
                type="button"
                class="button ghost sm"
                data-target-ou-value="${escapeHtml(value)}"
                data-target-ou-summary="${escapeHtml(pathDisplay)}"
                data-target-ou-dn="${escapeHtml(item.dn || "")}"
              >${escapeHtml(labels.selectOu)}</button>
            </div>
          `;
        })
        .join("");
      targetList.querySelectorAll("[data-target-ou-value]").forEach((button) => {
        button.addEventListener("click", () => {
          const targetValue = button.dataset.targetOuValue || "";
          const summaryText = button.dataset.targetOuSummary || targetValue;
          const dnText = button.dataset.targetOuDn || "";
          setPickerSummary(fieldName, targetValue, summaryText, dnText || summaryText);
          setStatus(fieldRoot.querySelector("[data-config-target-status]"), labels.targetLoaded, false);
          fieldRoot.hidden = true;
        });
      });
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

    sourcePickerField?.querySelector("[data-config-open-source-browser]")?.addEventListener("click", async () => {
      const panel = sourcePickerField.querySelector("[data-config-source-browser]");
      const status = sourcePickerField.querySelector("[data-config-source-status]");
      const loadButton = sourcePickerField.querySelector("[data-config-load-source-units]");
      hideAllInlinePickers();
      if (!panel) {
        return;
      }
      panel.hidden = false;
      panel.scrollIntoView({ block: "nearest", behavior: "smooth" });
      if (sourceCatalogCache.length) {
        setStatus(status, labels.sourceLoaded, false);
        return;
      }
      if (!sourceCatalogUrl || !loadButton) {
        return;
      }
      setStatus(status, labels.loading, false);
      loadButton.disabled = true;
      try {
        const payload = await postCatalogRequest(
          sourceCatalogUrl,
          buildConfigFormData(["csrf_token", "source_provider", "corpid", "agentid", "corpsecret"]),
        );
        renderSourceUnits(sourcePickerField, payload);
        setStatus(status, labels.sourceLoaded, false);
      } catch (error) {
        renderSourceUnits(sourcePickerField, { items: [] });
        setStatus(status, error.message || labels.sourceHint, true);
      } finally {
        loadButton.disabled = false;
      }
    });

    sourcePickerField?.querySelector("[data-config-load-source-units]")?.addEventListener("click", async () => {
      const status = sourcePickerField.querySelector("[data-config-source-status]");
      const loadButton = sourcePickerField.querySelector("[data-config-load-source-units]");
      if (!sourceCatalogUrl || !loadButton) {
        return;
      }
      setStatus(status, labels.loading, false);
      loadButton.disabled = true;
      try {
        const payload = await postCatalogRequest(
          sourceCatalogUrl,
          buildConfigFormData(["csrf_token", "source_provider", "corpid", "agentid", "corpsecret"]),
        );
        renderSourceUnits(sourcePickerField, payload);
        setStatus(status, labels.sourceLoaded, false);
      } catch (error) {
        renderSourceUnits(sourcePickerField, { items: [] });
        setStatus(status, error.message || labels.sourceHint, true);
      } finally {
        loadButton.disabled = false;
      }
    });

    sourcePickerField?.querySelector("[data-config-close-source-browser]")?.addEventListener("click", () => {
      const panel = sourcePickerField.querySelector("[data-config-source-browser]");
      if (panel) {
        panel.hidden = true;
      }
    });

    configForm.querySelectorAll("[data-config-target-browser]").forEach((panel) => {
      const fieldRoot = panel.closest(".picker-field");
      const fieldName = panel.dataset.targetField || "";
      if (!fieldRoot || !fieldName) {
        return;
      }
      fieldRoot.querySelector("[data-config-open-target-browser]")?.addEventListener("click", async () => {
        const status = panel.querySelector("[data-config-target-status]");
        hideAllInlinePickers();
        panel.hidden = false;
        panel.scrollIntoView({ block: "nearest", behavior: "smooth" });
        if (targetCatalogCache.length) {
          renderTargetOus(panel, { items: targetCatalogCache });
          setStatus(status, labels.targetLoaded, false);
          return;
        }
        const loadButton = panel.querySelector("[data-config-load-target-ous]");
        if (!targetCatalogUrl || !loadButton) {
          return;
        }
        setStatus(status, labels.loading, false);
        loadButton.disabled = true;
        try {
          const payload = await postCatalogRequest(
            targetCatalogUrl,
            buildConfigFormData([
              "csrf_token",
              "ldap_server",
              "ldap_domain",
              "ldap_username",
              "ldap_password",
              "ldap_port",
              "ldap_use_ssl",
              "ldap_validate_cert",
              "ldap_ca_cert_path",
            ]),
          );
          renderTargetOus(panel, payload);
          setStatus(status, labels.targetLoaded, false);
        } catch (error) {
          renderTargetOus(panel, { items: [] });
          setStatus(status, error.message || labels.targetHint, true);
        } finally {
          loadButton.disabled = false;
        }
      });

      panel.querySelector("[data-config-load-target-ous]")?.addEventListener("click", async () => {
        const status = panel.querySelector("[data-config-target-status]");
        const loadButton = panel.querySelector("[data-config-load-target-ous]");
        if (!targetCatalogUrl || !loadButton) {
          return;
        }
        setStatus(status, labels.loading, false);
        loadButton.disabled = true;
        try {
          const payload = await postCatalogRequest(
            targetCatalogUrl,
            buildConfigFormData([
              "csrf_token",
              "ldap_server",
              "ldap_domain",
              "ldap_username",
              "ldap_password",
              "ldap_port",
              "ldap_use_ssl",
              "ldap_validate_cert",
              "ldap_ca_cert_path",
            ]),
          );
          renderTargetOus(panel, payload);
          setStatus(status, labels.targetLoaded, false);
        } catch (error) {
          renderTargetOus(panel, { items: [] });
          setStatus(status, error.message || labels.targetHint, true);
        } finally {
          loadButton.disabled = false;
        }
      });

      panel.querySelector("[data-config-close-target-browser]")?.addEventListener("click", () => {
        panel.hidden = true;
      });
    });
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
      const submitBtn = form.querySelector('button[type="submit"]:not(.secondary):not(.ghost)');
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
