(() => {
  const ADOrgSync = (window.ADOrgSync = window.ADOrgSync || {});
  const escapeHtml =
    ADOrgSync.escapeHtml ||
    ((value) =>
      String(value ?? "")
        .replace(/&/g, "&amp;")
        .replace(/</g, "&lt;")
        .replace(/>/g, "&gt;")
        .replace(/"/g, "&quot;")
        .replace(/'/g, "&#39;"));
  const buildIndentSteps = (level, className) =>
    Array.from({ length: Math.max(0, Number(level || 0)) }, () => `<span class="${className}" aria-hidden="true"></span>`).join("");

  ADOrgSync.initMappingsPage = () => {
    if (typeof window.TomSelect === "undefined") {
      return;
    }

    const mappingsPage = document.querySelector("[data-mappings-page]");
    if (!mappingsPage) {
      return;
    }

    const sourceUserSearchUrl = mappingsPage.dataset.sourceUserSearchUrl || "";
    const targetUserSearchUrl = mappingsPage.dataset.targetUserSearchUrl || "";
    const sourceUserDepartmentsUrl = mappingsPage.dataset.sourceUserDepartmentsUrl || "";
    const loadingLabel = mappingsPage.dataset.mappingLoadingLabel || "Loading options...";
    const noResultsLabel = mappingsPage.dataset.mappingNoResultsLabel || "No matching records found";
    const searchSourcePlaceholder =
      mappingsPage.dataset.mappingSearchSourcePlaceholder || "Search source user name or ID";
    const searchTargetPlaceholder =
      mappingsPage.dataset.mappingSearchTargetPlaceholder || "Search AD username, display name, or mail";
    const departmentPlaceholder =
      mappingsPage.dataset.mappingDepartmentPlaceholder || "Select a primary department from this user";
    const departmentEmptyLabel =
      mappingsPage.dataset.mappingDepartmentEmptyLabel || "Select a source user first";
    const typeToSearchLabel =
      mappingsPage.dataset.mappingTypeToSearchLabel || "Type at least 2 characters to search";

    const renderMappingOptionShell = ({ title = "", detail = "", meta = "", level = 0 }) => `
      <div class="mapping-select-option">
        <div class="mapping-select-option__indent" aria-hidden="true">${buildIndentSteps(
          level,
          "mapping-select-option__indent-step",
        )}</div>
        <div class="mapping-select-option__content">
          <div class="mapping-select-option__title">${escapeHtml(title)}</div>
          ${detail ? `<div class="mapping-select-option__detail">${escapeHtml(detail)}</div>` : ""}
          ${meta ? `<div class="mapping-select-option__meta">${escapeHtml(meta)}</div>` : ""}
        </div>
      </div>
    `;

    const renderSelectedValue = (text) =>
      `<div class="mapping-select-item">${escapeHtml(text || "")}</div>`;

    const buildRemoteTomSelect = (element, options = {}) => {
      if (!element || element.tomselect) {
        return element?.tomselect || null;
      }
      const {
        url,
        placeholder = "",
        searchField = ["name", "id"],
        buildOptionText = (item) => item.name || item.id || "",
        renderOption,
        renderItem,
      } = options;
      return new TomSelect(element, {
        maxItems: 1,
        create: false,
        persist: false,
        allowEmptyOption: true,
        closeAfterSelect: true,
        loadThrottle: 200,
        valueField: "id",
        labelField: "name",
        searchField,
        placeholder,
        plugins: ["clear_button"],
        shouldLoad(query) {
          return String(query || "").trim().length >= 2;
        },
        render: {
          option(data) {
            return renderOption ? renderOption(data) : renderMappingOptionShell({ title: buildOptionText(data) });
          },
          item(data) {
            return renderItem ? renderItem(data) : renderSelectedValue(buildOptionText(data));
          },
          no_results() {
            return `<div class="mapping-select-empty">${escapeHtml(noResultsLabel)}</div>`;
          },
          loading() {
            return `<div class="mapping-select-empty">${escapeHtml(loadingLabel)}</div>`;
          },
          not_loading() {
            return `<div class="mapping-select-empty">${escapeHtml(typeToSearchLabel)}</div>`;
          },
        },
        load(query, callback) {
          const normalized = String(query || "").trim();
          if (normalized.length < 2 || !url) {
            callback();
            return;
          }
          fetch(`${url}?q=${encodeURIComponent(normalized)}`, { credentials: "same-origin" })
            .then((response) => response.json())
            .then((json) => {
              if (json.ok && Array.isArray(json.options)) {
                callback(json.options);
                return;
              }
              callback();
            })
            .catch(() => callback());
        },
      });
    };

    const resetDepartmentSelector = (departmentSelect, placeholderText = departmentEmptyLabel) => {
      if (!departmentSelect) {
        return;
      }
      const control = departmentSelect.tomselect;
      if (!control) {
        departmentSelect.innerHTML = `<option value="">${placeholderText}</option>`;
        departmentSelect.disabled = true;
        return;
      }
      control.clear(true);
      control.clearOptions();
      control.addOption({ id: "", name: placeholderText });
      control.refreshOptions(false);
      control.settings.placeholder = placeholderText;
      control.inputState();
      control.disable();
      departmentSelect.disabled = true;
    };

    const loadDepartmentsForSourceUser = (departmentSelect, sourceUserId) => {
      if (!departmentSelect || !departmentSelect.tomselect) {
        return;
      }
      const control = departmentSelect.tomselect;
      const normalizedSourceUserId = String(sourceUserId || "").trim();
      if (!normalizedSourceUserId || !sourceUserDepartmentsUrl) {
        resetDepartmentSelector(departmentSelect);
        return;
      }
      control.clear(true);
      control.clearOptions();
      control.settings.placeholder = loadingLabel;
      control.inputState();
      control.disable();
      fetch(`${sourceUserDepartmentsUrl}?user_id=${encodeURIComponent(normalizedSourceUserId)}`, {
        credentials: "same-origin",
      })
        .then((response) => response.json())
        .then((json) => {
          const options = json.ok && Array.isArray(json.options) ? json.options : [];
          control.clearOptions();
          if (!options.length) {
            control.settings.placeholder = noResultsLabel;
            control.inputState();
            departmentSelect.disabled = false;
            control.enable();
            return;
          }
          control.addOptions(options);
          control.settings.placeholder = departmentPlaceholder;
          control.inputState();
          departmentSelect.disabled = false;
          control.enable();
          control.refreshOptions(false);
        })
        .catch(() => {
          control.clearOptions();
          control.settings.placeholder = noResultsLabel;
          control.inputState();
          departmentSelect.disabled = false;
          control.enable();
        });
    };

    document.querySelectorAll("[data-mapping-source-user-select]").forEach((element) => {
      const control = buildRemoteTomSelect(element, {
        url: sourceUserSearchUrl,
        placeholder: element.dataset.placeholder || searchSourcePlaceholder,
        searchField: ["name", "id", "email"],
        buildOptionText: (item) => `${item.name || item.id || ""} [${item.id || ""}]`,
        renderOption: (item) => {
          const departments =
            Array.isArray(item.departments) && item.departments.length
              ? `${item.departments.length} departments`
              : "";
          const email = item.email ? ` · ${item.email}` : "";
          return renderMappingOptionShell({
            title: `${item.name || item.id || ""} [${item.id || ""}]`,
            detail: `${item.id || ""}${email}`,
            meta: departments,
          });
        },
        renderItem: (item) => renderSelectedValue(`${item.name || item.id || ""} [${item.id || ""}]`),
      });
      const departmentTargetId = element.dataset.mappingDepartmentTarget || "";
      if (control && departmentTargetId) {
        control.on("change", (value) => {
          const departmentSelect = document.getElementById(departmentTargetId);
          loadDepartmentsForSourceUser(departmentSelect, value);
        });
        if (!element.value) {
          const departmentSelect = document.getElementById(departmentTargetId);
          resetDepartmentSelector(departmentSelect);
        }
      }
    });

    document.querySelectorAll("[data-mapping-target-user-select]").forEach((element) => {
      buildRemoteTomSelect(element, {
        url: targetUserSearchUrl,
        placeholder: element.dataset.placeholder || searchTargetPlaceholder,
        searchField: ["name", "id", "mail", "upn", "dn"],
        buildOptionText: (item) => `${item.name || item.id || ""} [${item.id || ""}]`,
        renderOption: (item) => {
          const contact = item.mail || item.upn || "";
          return renderMappingOptionShell({
            title: `${item.name || item.id || ""} [${item.id || ""}]`,
            detail: item.id || "",
            meta: contact || item.dn || "",
          });
        },
        renderItem: (item) => renderSelectedValue(`${item.name || item.id || ""} [${item.id || ""}]`),
      });
    });

    document.querySelectorAll("[data-mapping-source-department-select]").forEach((element) => {
      if (element.tomselect) {
        return;
      }
      const control = new TomSelect(element, {
        maxItems: 1,
        create: false,
        persist: false,
        allowEmptyOption: true,
        closeAfterSelect: true,
        valueField: "id",
        labelField: "name",
        searchField: ["name", "id", "path_display"],
        placeholder: departmentEmptyLabel,
        plugins: ["clear_button"],
        render: {
          option(data) {
            return renderMappingOptionShell({
              title: `${data.name || data.id || ""} [${data.id || ""}]`,
              detail: data.path_display || "",
              level: Number(data.level || 0),
            });
          },
          item(data) {
            return renderSelectedValue(`${data.name || data.id || ""} [${data.id || ""}]`);
          },
          no_results() {
            return `<div class="mapping-select-empty">${escapeHtml(noResultsLabel)}</div>`;
          },
        },
      });
      const sourceUserSelectId = element.dataset.sourceUserSelect || "";
      if (sourceUserSelectId) {
        const sourceUserElement = document.getElementById(sourceUserSelectId);
        const initialValue = sourceUserElement?.value || "";
        if (initialValue) {
          loadDepartmentsForSourceUser(element, initialValue);
        } else {
          resetDepartmentSelector(element);
        }
      } else {
        control.enable();
        element.disabled = false;
      }
    });
  };
})();
