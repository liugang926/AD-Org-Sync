(() => {
  const ADOrgSync = (window.ADOrgSync = window.ADOrgSync || {});

  const defaultConfirmMessage = () =>
    document.body?.dataset.confirmMessage || "Are you sure you want to perform this action?";

  const escapeHtml =
    ADOrgSync.escapeHtml ||
    ((value) =>
      String(value ?? "")
        .replace(/&/g, "&amp;")
        .replace(/</g, "&lt;")
        .replace(/>/g, "&gt;")
        .replace(/"/g, "&quot;")
        .replace(/'/g, "&#39;"));

  function announce(message) {
    const status = document.querySelector("[data-app-status]");
    if (!(status instanceof HTMLElement) || !message) {
      return;
    }
    status.textContent = "";
    window.requestAnimationFrame(() => {
      status.textContent = String(message);
    });
  }

  function setLoading(button) {
    if (!(button instanceof HTMLElement) || button.classList.contains("btn-loading")) {
      return;
    }
    button.classList.add("btn-loading");
    button.setAttribute("aria-busy", "true");
    const width = button.offsetWidth;
    button.style.width = `${width}px`;
    announce(document.body?.dataset.loadingLabel || "Loading...");
  }

  function dismissFlash(element) {
    if (!(element instanceof HTMLElement)) {
      return;
    }
    element.style.transform = "translateX(120%)";
    element.style.opacity = "0";
    element.style.transition = "all 0.5s cubic-bezier(0.4, 0, 0.2, 1)";
    window.setTimeout(() => element.remove(), 500);
  }

  function initIcons() {
    if (window.lucide) {
      window.lucide.createIcons();
    }
  }

  function initAutoSubmit() {
    document.querySelectorAll("[data-auto-submit]").forEach((element) => {
      element.addEventListener("change", () => {
        element.closest("form")?.submit();
      });
    });
  }

  function initModeSwitchContext() {
    document.querySelectorAll("form[action='/ui-mode']").forEach((form) => {
      form.addEventListener("submit", () => {
        const returnUrl = form.querySelector("input[name='return_url']");
        if (!(returnUrl instanceof HTMLInputElement) || !window.location.hash) {
          return;
        }
        returnUrl.value = `${returnUrl.value.split("#", 1)[0]}${window.location.hash}`;
      });
    });
  }

  function initCopyButtons() {
    const status = document.querySelector("[data-copy-status]");
    document.querySelectorAll("[data-copy-value]").forEach((button) => {
      button.addEventListener("click", async () => {
        const value = button.getAttribute("data-copy-value") || "";
        if (!value) {
          return;
        }
        try {
          await navigator.clipboard.writeText(value);
        } catch (_error) {
          const fallback = document.createElement("textarea");
          fallback.value = value;
          fallback.setAttribute("readonly", "");
          fallback.className = "sr-only";
          document.body.appendChild(fallback);
          fallback.select();
          document.execCommand("copy");
          fallback.remove();
        }
        if (status instanceof HTMLElement) {
          status.textContent = "";
          window.requestAnimationFrame(() => {
            status.textContent = button.getAttribute("data-copied-label") || "Copied";
          });
        }
      });
    });
  }

  function initConfirmationPrompts() {
    const dialog = document.querySelector("[data-confirm-dialog]");
    const panel = dialog?.querySelector(".confirm-dialog__panel");
    const titleTarget = dialog?.querySelector("[data-confirm-title-target]");
    const messageTarget = dialog?.querySelector("[data-confirm-message-target]");
    const detailsTarget = dialog?.querySelector("[data-confirm-details]");
    const verification = dialog?.querySelector("[data-confirm-verification]");
    const inputHelp = dialog?.querySelector("[data-confirm-input-help]");
    const confirmInput = dialog?.querySelector("[data-confirm-input]");
    const approveButton = dialog?.querySelector("[data-confirm-approve]");
    const nextButton = dialog?.querySelector("[data-confirm-next]");
    const backButton = dialog?.querySelector("[data-confirm-back]");
    const wizardSteps = dialog
      ? Array.from(dialog.querySelectorAll("[data-confirm-wizard-step]"))
      : [];
    const approveLabel = approveButton?.querySelector("span");
    const defaultApproveLabel = approveLabel?.textContent || "Continue";
    const cancelButtons = dialog ? Array.from(dialog.querySelectorAll("[data-confirm-cancel]")) : [];
    let pendingElement = null;
    let pendingRequiredText = "";
    let restoreFocusTo = null;

    const setConfirmationStep = (step) => {
      if (!(dialog instanceof HTMLElement)) {
        return;
      }
      const usesWizard = dialog.dataset.confirmFlow === "wizard";
      const resolvedStep = usesWizard ? step : "confirm";
      dialog.dataset.confirmStep = resolvedStep;
      if (nextButton instanceof HTMLElement) {
        nextButton.hidden = !usesWizard || resolvedStep !== "review";
      }
      if (backButton instanceof HTMLElement) {
        backButton.hidden = !usesWizard || resolvedStep !== "confirm";
      }
      if (approveButton instanceof HTMLElement) {
        approveButton.hidden = usesWizard && resolvedStep === "review";
      }
      if (verification instanceof HTMLElement) {
        verification.hidden = resolvedStep !== "confirm" || !pendingRequiredText;
      }
      wizardSteps.forEach((item) => {
        if (!(item instanceof HTMLElement)) return;
        if (item.dataset.confirmWizardStep === resolvedStep) {
          item.setAttribute("aria-current", "step");
        } else {
          item.removeAttribute("aria-current");
        }
      });
    };

    const policyFormDiff = (element) => {
      const form = element.closest("form[data-policy-change-form]");
      if (!(form instanceof HTMLFormElement)) {
        return { oldValues: "", newValues: "" };
      }
      const ignoredNames = new Set([
        "csrf_token",
        "submission_kind",
        "selection_mode",
        "selection_search",
        "selection_department_id",
        "selection_status",
        "selection_employee_id_state",
      ]);
      const currentByName = new Map();
      const initialByName = new Map();
      const choiceNames = new Set();
      const addValue = (target, name, value) => {
        const values = target.get(name) || [];
        values.push(String(value || "").trim() || "—");
        target.set(name, values);
      };
      Array.from(form.elements).forEach((control) => {
        if (
          !(
            control instanceof HTMLInputElement
            || control instanceof HTMLSelectElement
            || control instanceof HTMLTextAreaElement
          )
          || !control.name
          || control.disabled
          || ignoredNames.has(control.name)
          || (control instanceof HTMLInputElement && ["hidden", "submit", "button"].includes(control.type))
        ) {
          return;
        }
        if (control instanceof HTMLInputElement && ["checkbox", "radio"].includes(control.type)) {
          choiceNames.add(control.name);
          if (control.checked) addValue(currentByName, control.name, control.value || "On");
          if (control.defaultChecked) addValue(initialByName, control.name, control.value || "On");
          return;
        }
        if (control instanceof HTMLSelectElement) {
          Array.from(control.options).forEach((option) => {
            if (option.selected) addValue(currentByName, control.name, option.textContent || option.value);
            if (option.defaultSelected) addValue(initialByName, control.name, option.textContent || option.value);
          });
          return;
        }
        addValue(currentByName, control.name, control.value);
        addValue(initialByName, control.name, control.defaultValue);
      });
      choiceNames.forEach((name) => {
        if (!currentByName.has(name)) currentByName.set(name, ["Off"]);
        if (!initialByName.has(name)) initialByName.set(name, ["Off"]);
      });
      const label = (name) =>
        name.replaceAll("_", " ").replace(/\b\w/g, (character) => character.toUpperCase());
      const changedNames = Array.from(
        new Set([...currentByName.keys(), ...initialByName.keys()]),
      ).filter(
        (name) =>
          JSON.stringify(currentByName.get(name) || []) !==
          JSON.stringify(initialByName.get(name) || []),
      );
      if (!changedNames.length) {
        return {
          oldValues: "No value changes detected",
          newValues: "No value changes detected",
        };
      }
      const summarize = (source) => {
        const rows = changedNames.slice(0, 8).map(
          (name) => `${label(name)}: ${(source.get(name) || ["—"]).join(", ")}`,
        );
        if (changedNames.length > 8) rows.push(`+${changedNames.length - 8} more`);
        return rows.join(" · ");
      };
      return {
        oldValues: summarize(initialByName),
        newValues: summarize(currentByName),
      };
    };

    const resolveConfirmationValue = (template, element) => {
      const selectionScope = element.closest("[data-selection-scope]") || element.closest("form");
      const form = element.closest("form");
      const selectedCount = selectionScope?.querySelectorAll("input[type='checkbox']:checked").length || 0;
      const actionSelect = form?.querySelector("select[name='action'], select[name='bulk_action']");
      const selectedAction =
        actionSelect instanceof HTMLSelectElement
          ? actionSelect.selectedOptions[0]?.textContent?.trim() || actionSelect.value
          : "";
      const policyDiff = policyFormDiff(element);
      return String(template)
        .replaceAll("{selected_count}", String(selectedCount))
        .replaceAll("{selected_action}", selectedAction)
        .replaceAll("{policy_old_values}", policyDiff.oldValues)
        .replaceAll("{policy_new_values}", policyDiff.newValues)
        .replaceAll(
          "{policy_impact}",
          form?.dataset.policyImpact || element.dataset.policyImpact || "0",
        )
        .replaceAll(
          "{policy_scope}",
          form?.dataset.policyScope || element.dataset.policyScope || "",
        );
    };

    const closeDialog = ({ restoreFocus = true } = {}) => {
      if (!(dialog instanceof HTMLElement)) {
        return;
      }
      dialog.hidden = true;
      document.body?.classList.remove("confirm-dialog-open");
      pendingElement = null;
      pendingRequiredText = "";
      if (confirmInput instanceof HTMLInputElement) {
        confirmInput.value = "";
      }
      if (approveButton instanceof HTMLButtonElement) {
        approveButton.disabled = false;
        approveButton.removeAttribute("aria-describedby");
      }
      if (approveLabel instanceof HTMLElement) {
        approveLabel.textContent = defaultApproveLabel;
      }
      if (restoreFocus && restoreFocusTo instanceof HTMLElement) {
        restoreFocusTo.focus();
      }
      restoreFocusTo = null;
    };

    const runConfirmedAction = () => {
      const element = pendingElement;
      closeDialog({ restoreFocus: false });
      if (!(element instanceof HTMLElement)) {
        return;
      }
      if (element.tagName === "A") {
        const href = element.getAttribute("href");
        if (href) {
          window.location.href = href;
        }
        return;
      }
      if (element.tagName === "BUTTON" && element.getAttribute("type") === "submit") {
        setLoading(element);
        element.closest("form")?.requestSubmit(element);
        return;
      }
      element.click();
    };

    if (approveButton instanceof HTMLElement) {
      approveButton.addEventListener("click", runConfirmedAction);
    }
    if (nextButton instanceof HTMLElement) {
      nextButton.addEventListener("click", () => {
        setConfirmationStep("confirm");
        if (pendingRequiredText && confirmInput instanceof HTMLInputElement) {
          confirmInput.focus();
        } else if (approveButton instanceof HTMLElement) {
          approveButton.focus();
        }
      });
    }
    if (backButton instanceof HTMLElement) {
      backButton.addEventListener("click", () => {
        setConfirmationStep("review");
        if (nextButton instanceof HTMLElement) {
          nextButton.focus();
        }
      });
    }
    cancelButtons.forEach((button) => {
      button.addEventListener("click", closeDialog);
    });
    document.addEventListener("keydown", (event) => {
      if (!(dialog instanceof HTMLElement) || dialog.hidden) {
        return;
      }
      if (event.key === "Escape") {
        event.preventDefault();
        closeDialog();
        return;
      }
      if (event.key === "Tab") {
        const focusable = Array.from(
          dialog.querySelectorAll(
            'button:not([disabled]), input:not([disabled]), [href], [tabindex]:not([tabindex="-1"])'
          )
        ).filter((element) => element instanceof HTMLElement && !element.hidden);
        if (focusable.length === 0) {
          event.preventDefault();
          panel?.focus();
          return;
        }
        const first = focusable[0];
        const last = focusable[focusable.length - 1];
        if (event.shiftKey && document.activeElement === first) {
          event.preventDefault();
          last.focus();
        } else if (!event.shiftKey && document.activeElement === last) {
          event.preventDefault();
          first.focus();
        }
      }
    });

    document
      .querySelectorAll("button[data-confirm], a[data-confirm], form[data-confirm] button[type='submit']")
      .forEach((element) => {
      element.addEventListener("click", (event) => {
        const source = element.hasAttribute("data-confirm") ? element : element.closest("form[data-confirm]");
        const message = source?.getAttribute("data-confirm") || defaultConfirmMessage();
        const requiredText = source?.getAttribute("data-confirm-require") || "";
        if (dialog instanceof HTMLElement && messageTarget instanceof HTMLElement) {
          event.preventDefault();
          event.stopImmediatePropagation();
          pendingElement = element;
          pendingRequiredText = requiredText;
          restoreFocusTo = element;
          messageTarget.textContent = message;
          if (titleTarget instanceof HTMLElement) {
            titleTarget.textContent =
              source?.getAttribute("data-confirm-title") || titleTarget.dataset.defaultTitle || titleTarget.textContent;
          }
          if (detailsTarget instanceof HTMLElement) {
            detailsTarget.replaceChildren();
            for (let index = 1; index <= 16; index += 1) {
              const label = source?.getAttribute(`data-confirm-detail-${index}-label`) || "";
              const valueTemplate = source?.getAttribute(`data-confirm-detail-${index}-value`) || "";
              const value = resolveConfirmationValue(valueTemplate, element);
              if (!label || !value) {
                continue;
              }
              const term = document.createElement("dt");
              const description = document.createElement("dd");
              term.textContent = label;
              description.textContent = value;
              detailsTarget.append(term, description);
            }
            detailsTarget.hidden = detailsTarget.childElementCount === 0;
          }
          if (verification instanceof HTMLElement && confirmInput instanceof HTMLInputElement) {
            confirmInput.value = "";
            if (inputHelp instanceof HTMLElement) {
              const template = source?.getAttribute("data-confirm-input-help") || "Type {value} to confirm.";
              inputHelp.textContent = template.replace("{value}", requiredText);
            }
            if (approveButton instanceof HTMLButtonElement) {
              approveButton.disabled = Boolean(requiredText);
              if (requiredText) {
                approveButton.setAttribute("aria-describedby", "confirm-dialog-input-help");
              } else {
                approveButton.removeAttribute("aria-describedby");
              }
            }
            confirmInput.oninput = () => {
              if (approveButton instanceof HTMLButtonElement) {
                approveButton.disabled = confirmInput.value.trim() !== requiredText;
              }
            };
          }
          if (approveLabel instanceof HTMLElement) {
            approveLabel.textContent = element.textContent?.trim() || defaultApproveLabel;
          }
          dialog.hidden = false;
          setConfirmationStep("review");
          document.body?.classList.add("confirm-dialog-open");
          if (window.lucide) {
            window.lucide.createIcons();
          }
          if (dialog.dataset.confirmFlow === "wizard" && nextButton instanceof HTMLElement) {
            nextButton.focus();
          } else if (requiredText && confirmInput instanceof HTMLInputElement) {
            confirmInput.focus();
          } else if (cancelButtons[0] instanceof HTMLElement) {
            cancelButtons[0].focus();
          } else if (approveButton instanceof HTMLElement) {
            approveButton.focus();
          }
          return;
        }
        if (!window.confirm(message)) {
          event.preventDefault();
          event.stopImmediatePropagation();
          return;
        }
        if (element.tagName === "BUTTON" && element.getAttribute("type") === "submit") {
          setLoading(element);
        }
      });
    });
  }

  function initIdentityDrawers() {
    const drawers = Array.from(
      document.querySelectorAll("[data-detail-drawer], [data-identity-drawer]")
    );
    if (!drawers.length) {
      return;
    }
    let activeDrawer = null;
    let restoreFocusTo = null;

    const focusableElements = (drawer) =>
      Array.from(
        drawer.querySelectorAll(
          'button:not([disabled]), a[href], input:not([disabled]), select:not([disabled]), textarea:not([disabled]), details > summary, [tabindex]:not([tabindex="-1"])'
        )
      ).filter(
        (element) =>
          element instanceof HTMLElement &&
          !element.hidden &&
          element.getClientRects().length > 0 &&
          element.getAttribute("aria-hidden") !== "true"
      );

    const closeDrawer = ({ restoreFocus = true } = {}) => {
      if (!(activeDrawer instanceof HTMLElement)) {
        return;
      }
      activeDrawer.hidden = true;
      document.body?.classList.remove("identity-drawer-open", "detail-drawer-open");
      activeDrawer = null;
      if (restoreFocus && restoreFocusTo instanceof HTMLElement) {
        restoreFocusTo.focus();
      }
      restoreFocusTo = null;
    };

    const openDrawer = (opener) => {
      const drawerId = opener.getAttribute("data-identity-drawer-open") || "";
      const drawer = drawerId ? document.getElementById(drawerId) : null;
      if (!(drawer instanceof HTMLElement)) {
        return;
      }
      if (activeDrawer instanceof HTMLElement && activeDrawer !== drawer) {
        activeDrawer.hidden = true;
      }
      activeDrawer = drawer;
      restoreFocusTo = opener;
      drawer.hidden = false;
      document.body?.classList.add("identity-drawer-open", "detail-drawer-open");
      const preferred = drawer.querySelector(
        ".identity-drawer__header [data-identity-drawer-close]"
      );
      const panel = drawer.querySelector(
        ".detail-drawer__panel, .identity-drawer__panel"
      );
      if (preferred instanceof HTMLElement) {
        preferred.focus();
      } else if (panel instanceof HTMLElement) {
        panel.focus();
      }
    };

    document.querySelectorAll("[data-identity-drawer-open]").forEach((opener) => {
      opener.addEventListener("click", () => openDrawer(opener));
    });
    drawers.forEach((drawer) => {
      drawer.querySelectorAll("[data-identity-drawer-close]").forEach((closer) => {
        closer.addEventListener("click", () => closeDrawer());
      });
    });
    document.addEventListener("keydown", (event) => {
      if (!(activeDrawer instanceof HTMLElement) || activeDrawer.hidden) {
        return;
      }
      if (event.key === "Escape") {
        event.preventDefault();
        closeDrawer();
        return;
      }
      if (event.key !== "Tab") {
        return;
      }
      const focusable = focusableElements(activeDrawer);
      const panel = activeDrawer.querySelector(
        ".detail-drawer__panel, .identity-drawer__panel"
      );
      if (!focusable.length) {
        event.preventDefault();
        panel?.focus();
        return;
      }
      const first = focusable[0];
      const last = focusable[focusable.length - 1];
      if (event.shiftKey && document.activeElement === first) {
        event.preventDefault();
        last.focus();
      } else if (!event.shiftKey && document.activeElement === last) {
        event.preventDefault();
        first.focus();
      }
    });
  }

  function initSelectionSummaries() {
    document.querySelectorAll("[data-selection-scope]").forEach((scope) => {
      let previousCheckedCount = null;
      const update = () => {
        const checkedCount = scope.querySelectorAll("input[type='checkbox']:checked").length;
        scope.querySelectorAll("[data-selection-count]").forEach((target) => {
          target.textContent = String(checkedCount);
        });
        scope.querySelectorAll("[data-selection-has-items]").forEach((target) => {
          target.classList.toggle("is-active", checkedCount > 0);
        });
        scope.querySelectorAll("[data-selection-requires-items]").forEach((target) => {
          if (target instanceof HTMLButtonElement) {
            target.disabled = checkedCount === 0;
            target.setAttribute("aria-disabled", String(checkedCount === 0));
          }
        });
        if (previousCheckedCount !== null && previousCheckedCount !== checkedCount) {
          announce(`${checkedCount} selected`);
        }
        previousCheckedCount = checkedCount;
      };
      scope.querySelectorAll("input[type='checkbox']").forEach((checkbox) => {
        checkbox.addEventListener("change", update);
      });
      update();
    });
  }

  function initIdentityWorkbenchBatch() {
    document.querySelectorAll("[data-identity-batch-form]").forEach((form) => {
      const modeSelect = form.querySelector("[data-identity-batch-mode]");
      const batchBar = form.querySelector("[data-identity-batch-bar]");
      const purposeTarget = form.querySelector("[data-identity-selection-purpose]");
      const submitButton = form.querySelector("[data-identity-batch-submit]");
      const submitLabel = submitButton?.querySelector("span");
      const checkboxes = Array.from(form.querySelectorAll("[data-identity-select]"));
      if (!(modeSelect instanceof HTMLSelectElement) || !(batchBar instanceof HTMLElement)) {
        return;
      }

      const selectedOption = () => modeSelect.selectedOptions[0];
      const update = () => {
        const checkedCount = checkboxes.filter((checkbox) => checkbox.checked).length;
        form.querySelectorAll("[data-selection-count]").forEach((target) => {
          target.textContent = String(checkedCount);
        });
        batchBar.classList.toggle("is-active", checkedCount > 0);
        if (submitButton instanceof HTMLButtonElement) {
          submitButton.disabled = checkedCount === 0;
          submitButton.setAttribute("aria-disabled", String(checkedCount === 0));
        }
      };

      const applyMode = ({ clear = true } = {}) => {
        const option = selectedOption();
        const mode = modeSelect.value;
        if (clear) {
          checkboxes.forEach((checkbox) => {
            checkbox.checked = false;
          });
        }
        checkboxes.forEach((checkbox) => {
          const supportedModes = String(checkbox.getAttribute("data-supported-modes") || "")
            .split(",")
            .map((value) => value.trim())
            .filter(Boolean);
          checkbox.disabled = !supportedModes.includes(mode);
          if (checkbox.disabled) {
            checkbox.checked = false;
          }
        });
        const action = option?.getAttribute("data-action") || "";
        if (action) {
          form.setAttribute("action", action);
        }
        if (purposeTarget instanceof HTMLElement) {
          purposeTarget.textContent = option?.getAttribute("data-purpose") || "";
        }
        if (submitLabel instanceof HTMLElement) {
          submitLabel.textContent =
            option?.getAttribute("data-submit-label") || submitLabel.textContent;
        }
        update();
      };

      modeSelect.addEventListener("change", () => applyMode({ clear: true }));
      checkboxes.forEach((checkbox) => checkbox.addEventListener("change", update));
      form.querySelectorAll("[data-identity-row-select]").forEach((button) => {
        button.addEventListener("click", () => {
          const requestedMode = button.getAttribute("data-mode") || "";
          if (
            requestedMode &&
            Array.from(modeSelect.options).some((option) => option.value === requestedMode)
          ) {
            const changedMode = modeSelect.value !== requestedMode;
            modeSelect.value = requestedMode;
            applyMode({ clear: changedMode });
          }
          const row = button.closest("[data-identity-row]");
          const checkbox = row?.querySelector("[data-identity-select]");
          if (checkbox instanceof HTMLInputElement && !checkbox.disabled) {
            checkbox.checked = true;
          }
          button.closest("details")?.removeAttribute("open");
          update();
        });
      });
      form.querySelector("[data-identity-clear-selection]")?.addEventListener("click", () => {
        checkboxes.forEach((checkbox) => {
          checkbox.checked = false;
        });
        update();
      });
      applyMode({ clear: false });
    });
  }

  function initFormLoading() {
    document.querySelectorAll("form").forEach((form) => {
      form.addEventListener("submit", (event) => {
        const submitter =
          event.submitter instanceof HTMLElement
            ? event.submitter
            : form.querySelector('button[type="submit"]');
        if (!submitter || submitter.hasAttribute("data-confirm")) {
          return;
        }
        window.setTimeout(() => {
          if (!event.defaultPrevented) {
            setLoading(submitter);
          }
        }, 10);
      });
    });
  }

  function initFlashMessages() {
    document.querySelectorAll(".flash").forEach((flash) => {
      if (flash.classList.contains("success")) {
        window.setTimeout(() => dismissFlash(flash), 6000);
      }
    });

    document.querySelectorAll("[data-dismiss-closest]").forEach((element) => {
      element.addEventListener("click", () => {
        const selector = element.getAttribute("data-dismiss-closest");
        const target = selector ? element.closest(selector) : null;
        if (target) {
          dismissFlash(target);
        }
      });
    });
  }

  function initSidebarActiveState() {
    const currentPath = window.location.pathname;
    const nav = document.querySelector("[data-sidebar-nav]");
    if (!(nav instanceof HTMLElement)) {
      return;
    }
    const links = Array.from(nav.querySelectorAll("a:not([data-sidebar-recent-link])"));
    const routePaths = (link) => {
      const href = link.getAttribute("href");
      const aliases = (link.getAttribute("data-route-aliases") || "")
        .split(/\s+/)
        .filter(Boolean);
      return [href, ...aliases].filter(Boolean);
    };
    const pathMatches = (routePath) =>
      routePath === currentPath ||
      (routePath !== "/" && currentPath.startsWith(`${routePath}/`));
    const serverActiveLink = links.find(
      (link) => link.getAttribute("aria-current") === "page"
    );
    const matchedActiveLink = links
      .filter((link) => routePaths(link).some(pathMatches))
      .sort((left, right) => {
        const leftLength = Math.max(...routePaths(left).filter(pathMatches).map((path) => path.length));
        const rightLength = Math.max(...routePaths(right).filter(pathMatches).map((path) => path.length));
        return rightLength - leftLength;
      })[0];
    const activeLink = serverActiveLink || matchedActiveLink;

    links.forEach((link) => {
      const isActive = link === activeLink;
      link.classList.toggle("active", isActive);
      if (isActive) {
        link.setAttribute("aria-current", "page");
      } else {
        link.removeAttribute("aria-current");
      }
    });
    if (activeLink instanceof HTMLAnchorElement) {
      const group = activeLink.closest("details");
      if (group instanceof HTMLDetailsElement) {
        group.open = true;
      }
    }

    const storageKey = "ad-org-sync.recent-navigation";
    const recent = nav.querySelector("[data-sidebar-recent]");
    const recentLinks = nav.querySelector("[data-sidebar-recent-links]");
    let recentPaths = [];
    try {
      const stored = JSON.parse(window.localStorage.getItem(storageKey) || "[]");
      recentPaths = Array.isArray(stored) ? stored.filter((item) => typeof item === "string") : [];
    } catch (_error) {
      recentPaths = [];
    }

    const canonicalPath = (path) => {
      const source = links.find((link) => routePaths(link).includes(path));
      return source instanceof HTMLAnchorElement ? source.getAttribute("href") : null;
    };
    recentPaths = recentPaths
      .map(canonicalPath)
      .filter((path, index, items) => path && items.indexOf(path) === index);

    const renderRecent = () => {
      if (!(recent instanceof HTMLElement) || !(recentLinks instanceof HTMLElement)) {
        return;
      }
      recentLinks.replaceChildren();
      const activePath = activeLink instanceof HTMLAnchorElement ? activeLink.getAttribute("href") : null;
      recentPaths.filter((path) => path !== activePath).slice(0, 3).forEach((path) => {
        const source = links.find((link) => link.getAttribute("href") === path);
        if (!(source instanceof HTMLAnchorElement)) {
          return;
        }
        const clone = source.cloneNode(true);
        clone.setAttribute("data-sidebar-recent-link", "true");
        clone.classList.remove("active");
        clone.removeAttribute("aria-current");
        recentLinks.appendChild(clone);
      });
      recent.hidden = recentLinks.childElementCount === 0;
    };

    links.forEach((link) => {
      link.addEventListener("click", () => {
        const href = link.getAttribute("href");
        if (!href) {
          return;
        }
        recentPaths = [href, ...recentPaths.filter((path) => path !== href)].slice(0, 3);
        try {
          window.localStorage.setItem(storageKey, JSON.stringify(recentPaths));
        } catch (_error) {
          // Navigation still works when storage is unavailable.
        }
      });
    });
    renderRecent();
  }

  function initTableHover() {
    document.querySelectorAll("tr").forEach((row) => {
      row.addEventListener("mouseenter", () => {
        row.style.transition = "background-color 0.2s ease";
      });
      });
  }

  function initTableScrollContainers() {
    const containers = Array.from(
      new Set(
        Array.from(
          document.querySelectorAll(".table-shell, .table-scroll, [data-table-region]")
        )
      )
    );
    if (!containers.length) {
      return;
    }
    const label =
      document.body?.dataset.tableScrollLabel ||
      "Scrollable data table. Use arrow keys to review hidden columns.";
    const keyboardHelp =
      document.body?.dataset.tableKeyboardHelp ||
      "Use Left and Right to scroll columns, Up and Down to move rows, and Enter to open the first row action.";
    const update = () => {
      containers.forEach((container) => {
        const scrollable = container.scrollWidth > container.clientWidth + 1;
        container.dataset.scrollable = String(scrollable);
        container.setAttribute("role", "region");
        if (!container.hasAttribute("aria-label")) {
          container.setAttribute("aria-label", label);
        }
        if (!container.hasAttribute("tabindex")) {
          container.setAttribute("tabindex", "0");
        }
      });
    };
    containers.forEach((container, containerIndex) => {
      const helpId = `table-keyboard-help-${containerIndex + 1}`;
      const help = document.createElement("span");
      help.id = helpId;
      help.className = "sr-only";
      help.textContent = keyboardHelp;
      container.prepend(help);
      const describedBy = (container.getAttribute("aria-describedby") || "")
        .split(/\s+/)
        .filter(Boolean);
      if (!describedBy.includes(helpId)) {
        describedBy.push(helpId);
        container.setAttribute("aria-describedby", describedBy.join(" "));
      }

      const rows = Array.from(container.querySelectorAll("tbody tr")).filter(
        (row) => row.querySelector("td:not(.table-empty)")
      );
      rows.forEach((row) => row.setAttribute("tabindex", "-1"));
      let activeRowIndex = -1;
      const focusRow = (index) => {
        if (!rows.length) {
          return;
        }
        activeRowIndex = Math.max(0, Math.min(rows.length - 1, index));
        rows.forEach((row, rowIndex) => {
          if (rowIndex === activeRowIndex) {
            row.setAttribute("data-keyboard-row", "true");
          } else {
            row.removeAttribute("data-keyboard-row");
          }
        });
        rows[activeRowIndex].focus({ preventScroll: true });
        rows[activeRowIndex].scrollIntoView({
          block: "nearest",
          inline: "nearest",
        });
      };

      container.addEventListener("focusin", () => {
        container.dataset.keyboardActive = "true";
      });
      container.addEventListener("focusout", (event) => {
        if (!container.contains(event.relatedTarget)) {
          delete container.dataset.keyboardActive;
        }
      });
      container.addEventListener("keydown", (event) => {
        const targetIsContainer = event.target === container;
        const targetIsRow = event.target instanceof HTMLTableRowElement;
        if (!targetIsContainer && !targetIsRow) {
          if (event.key === "Escape") {
            container.focus();
          }
          return;
        }
        if (event.key === "ArrowLeft" || event.key === "ArrowRight") {
          event.preventDefault();
          container.scrollBy({
            left: event.key === "ArrowLeft" ? -120 : 120,
            behavior: "smooth",
          });
          return;
        }
        if (event.key === "Home" || event.key === "End") {
          event.preventDefault();
          container.scrollTo({
            left: event.key === "Home" ? 0 : container.scrollWidth,
            behavior: "smooth",
          });
          return;
        }
        if (event.key === "ArrowDown" || event.key === "ArrowUp") {
          event.preventDefault();
          const direction = event.key === "ArrowDown" ? 1 : -1;
          const initialIndex = direction > 0 ? 0 : rows.length - 1;
          focusRow(
            activeRowIndex < 0 ? initialIndex : activeRowIndex + direction
          );
          return;
        }
        if (event.key === "Enter" && targetIsRow) {
          const action = event.target.querySelector(
            "a[href], button:not([disabled]), summary, input:not([disabled]), select:not([disabled])"
          );
          if (action instanceof HTMLElement) {
            event.preventDefault();
            action.focus();
          }
        }
      });
    });
    update();
    window.addEventListener("resize", update);
  }

  function initLocalizedTimes() {
    const locale = document.documentElement.lang || navigator.language || "en";
    const relativeFormatter = new Intl.RelativeTimeFormat(locale, { numeric: "auto" });
    const relativeUnits = [
      ["year", 365 * 24 * 60 * 60],
      ["month", 30 * 24 * 60 * 60],
      ["week", 7 * 24 * 60 * 60],
      ["day", 24 * 60 * 60],
      ["hour", 60 * 60],
      ["minute", 60],
      ["second", 1],
    ];
    document.querySelectorAll("time[data-local-time]").forEach((element) => {
      const rawValue = element.getAttribute("datetime") || "";
      const parsed = new Date(rawValue);
      if (!rawValue || Number.isNaN(parsed.getTime())) {
        return;
      }
      const pad = (value) => String(value).padStart(2, "0");
      const formatted = [
        parsed.getFullYear(),
        pad(parsed.getMonth() + 1),
        pad(parsed.getDate()),
      ].join("-") + ` ${pad(parsed.getHours())}:${pad(parsed.getMinutes())}`;
      const deltaSeconds = (parsed.getTime() - Date.now()) / 1000;
      const [relativeUnit, divisor] =
        relativeUnits.find(([, seconds]) => Math.abs(deltaSeconds) >= seconds) || relativeUnits.at(-1);
      const relative = relativeFormatter.format(Math.round(deltaSeconds / divisor), relativeUnit);
      const display = `${formatted}${
        String(locale).toLowerCase().startsWith("zh") ? " · " : ", "
      }${relative}`;
      element.textContent = display;
      element.setAttribute("title", rawValue);
      element.setAttribute("aria-label", `${display}; ${rawValue}`);
      const utcTarget = element.parentElement?.querySelector("[data-raw-utc]");
      if (utcTarget instanceof HTMLElement) {
        utcTarget.textContent = `UTC: ${parsed.toISOString()}`;
      }
    });
  }

  function initMobileNav() {
    const body = document.body;
    const sidebar = document.querySelector("[data-app-sidebar]");
    const toggle = document.querySelector("[data-mobile-nav-toggle]");
    if (!body || !sidebar || !(toggle instanceof HTMLElement)) {
      return;
    }

    const mobileBreakpoint = 768;
    const main = document.querySelector("main");
    const focusableElements = () =>
      Array.from(
        sidebar.querySelectorAll(
          'a[href], button:not([disabled]), summary, select:not([disabled]), [tabindex]:not([tabindex="-1"])'
        )
      ).filter((element) => element instanceof HTMLElement && !element.hidden);

    const setOpen = (isOpen, { restoreFocus = false } = {}) => {
      const normalized = Boolean(isOpen) && window.innerWidth <= mobileBreakpoint;
      body.classList.toggle("mobile-nav-open", normalized);
      toggle.setAttribute("aria-expanded", String(normalized));
      if (normalized) {
        sidebar.removeAttribute("aria-hidden");
        sidebar.removeAttribute("inert");
        main?.setAttribute("inert", "");
        const preferred = sidebar.querySelector("[data-mobile-nav-close]");
        const focusable = focusableElements();
        if (preferred instanceof HTMLElement) {
          preferred.focus();
        } else if (focusable[0] instanceof HTMLElement) {
          focusable[0].focus();
        }
        return;
      }
      main?.removeAttribute("inert");
      if (window.innerWidth <= mobileBreakpoint) {
        sidebar.setAttribute("aria-hidden", "true");
        sidebar.setAttribute("inert", "");
      } else {
        sidebar.removeAttribute("aria-hidden");
        sidebar.removeAttribute("inert");
      }
      if (restoreFocus) {
        toggle.focus();
      }
    };

    toggle.addEventListener("click", () => {
      setOpen(!body.classList.contains("mobile-nav-open"));
    });

    document.querySelectorAll("[data-mobile-nav-close]").forEach((element) => {
      element.addEventListener("click", () => setOpen(false, { restoreFocus: true }));
    });

    sidebar.querySelectorAll("a").forEach((link) => {
      link.addEventListener("click", () => setOpen(false));
    });

    window.addEventListener("resize", () => {
      if (window.innerWidth > mobileBreakpoint) {
        setOpen(false);
      }
    });

    document.addEventListener("keydown", (event) => {
      if (!body.classList.contains("mobile-nav-open")) {
        return;
      }
      if (event.key === "Escape") {
        event.preventDefault();
        setOpen(false, { restoreFocus: true });
        return;
      }
      if (event.key === "Tab") {
        const focusable = focusableElements();
        if (!focusable.length) {
          event.preventDefault();
          return;
        }
        const first = focusable[0];
        const last = focusable[focusable.length - 1];
        if (event.shiftKey && document.activeElement === first) {
          event.preventDefault();
          last.focus();
        } else if (!event.shiftKey && document.activeElement === last) {
          event.preventDefault();
          first.focus();
        }
      }
    });

    setOpen(false);
  }

  function bindTomSelectRemote(selector, url) {
    document.querySelectorAll(selector).forEach((element) => {
      if (element.tomselect) {
        return;
      }
      const tomSelectLabels = document.body?.dataset || {};

      new TomSelect(element, {
        create: true,
        valueField: "id",
        labelField: "name",
        searchField: ["name", "id"],
        preload: true,
        plugins: {
          remove_button: { title: tomSelectLabels.removeItemLabel || "Remove item" },
          clear_button: { title: tomSelectLabels.clearAllLabel || "Clear all" },
        },
        load(_query, callback) {
          fetch(url, { credentials: "same-origin" })
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
    });
  }

  function initSharedTomSelectFields() {
    if (typeof window.TomSelect === "undefined") {
      return;
    }

    bindTomSelectRemote("input[name='root_department_ids']", "/api/metadata/departments");
    bindTomSelectRemote("input[name='managed_tag_ids']", "/api/metadata/tags");
    bindTomSelectRemote("input[name='managed_external_chat_ids']", "/api/metadata/external-chats");

    document.querySelectorAll("textarea[name='soft_excluded_groups']").forEach((element) => {
      if (element.tomselect) {
        return;
      }
      const tomSelectLabels = document.body?.dataset || {};
      new TomSelect(element, {
        create: true,
        plugins: {
          remove_button: { title: tomSelectLabels.removeItemLabel || "Remove item" },
          clear_button: { title: tomSelectLabels.clearAllLabel || "Clear all" },
        },
        persist: false,
        createOnBlur: true,
      });
    });
  }

  function initDepartmentTrees() {
    document.querySelectorAll("[data-department-tree]").forEach((tree) => {
      const search = tree.querySelector("[data-department-tree-search]");
      const nodes = Array.from(tree.querySelectorAll("[data-department-tree-node]"));
      if (search instanceof HTMLInputElement) {
        search.addEventListener("input", () => {
          const query = search.value.trim().toLocaleLowerCase();
          nodes.forEach((node) => {
            const searchText = node.getAttribute("data-department-search-text") || "";
            node.hidden = Boolean(query) && !searchText.includes(query);
          });
        });
      }
      nodes.forEach((node) => {
        const selector = node.querySelector("[data-department-name]");
        if (!(selector instanceof HTMLInputElement)) {
          return;
        }
        selector.addEventListener("change", () => {
          const form = selector.closest("form");
          const target = form?.querySelector("[data-selected-department-name]");
          if (selector.checked && target instanceof HTMLInputElement) {
            target.value = selector.getAttribute("data-department-name") || "";
          }
        });
      });
    });
  }

  function initScheduledApplyConfirmation() {
    const mode = document.querySelector("[data-scheduled-mode]");
    const confirmation = document.querySelector("[data-scheduled-apply-confirmation]");
    if (!(mode instanceof HTMLSelectElement) || !(confirmation instanceof HTMLElement)) {
      return;
    }
    const checkbox = confirmation.querySelector('input[name="confirm_scheduled_apply"]');
    const update = () => {
      const isApply = mode.value === "apply";
      confirmation.hidden = !isApply;
      if (checkbox instanceof HTMLInputElement) {
        checkbox.required = isApply;
        if (!isApply) checkbox.checked = false;
      }
    };
    mode.addEventListener("change", update);
    update();
  }

  function initOrganizationImportPreview() {
    const form = document.querySelector("[data-organization-import-form]");
    if (!(form instanceof HTMLFormElement)) return;
    const fileInput = form.querySelector("[data-organization-bundle-file]");
    const jsonInput = form.querySelector("[data-organization-bundle-json]");
    const targetInput = form.querySelector('input[name="target_org_id"]');
    const preview = form.querySelector("[data-organization-import-preview]");
    const replace = form.querySelector("[data-import-replace]");
    const confirmation = form.querySelector("[data-import-replace-confirmation]");
    const confirmCheckbox = confirmation?.querySelector('input[name="confirm_replace"]');
    let parsedBundle = null;

    const setText = (selector, value) => {
      const element = form.querySelector(selector);
      if (element) element.textContent = String(value);
    };
    const renderPreview = () => {
      if (!(preview instanceof HTMLElement) || !(jsonInput instanceof HTMLTextAreaElement)) return;
      try {
        parsedBundle = JSON.parse(jsonInput.value || "");
        const organization = parsedBundle?.organization || {};
        const target = String(targetInput?.value || "").trim() || organization.org_id || "-";
        setText("[data-import-preview-title]", preview.dataset.validTitle || "Import Preview");
        setText("[data-import-preview-org]", target);
        setText("[data-import-preview-settings]", Object.keys(parsedBundle?.org_settings || {}).length);
        setText("[data-import-preview-connectors]", (parsedBundle?.connectors || []).length);
        setText("[data-import-preview-rules]", (parsedBundle?.attribute_mappings || []).length + (parsedBundle?.department_ou_mappings || []).length + (parsedBundle?.group_exclusion_rules || []).length);
        preview.classList.remove("error");
        preview.hidden = false;
      } catch (_error) {
        parsedBundle = null;
        if (!jsonInput.value.trim()) {
          preview.hidden = true;
          return;
        }
        setText("[data-import-preview-title]", preview.dataset.invalidTitle || "Invalid bundle JSON");
        preview.classList.add("error");
        preview.hidden = false;
      }
    };
    fileInput?.addEventListener("change", async () => {
      const file = fileInput.files?.[0];
      if (!file || !(jsonInput instanceof HTMLTextAreaElement)) return;
      jsonInput.value = await file.text();
      renderPreview();
    });
    jsonInput?.addEventListener("input", renderPreview);
    targetInput?.addEventListener("input", () => { if (parsedBundle) renderPreview(); });
    const updateReplacement = () => {
      const enabled = replace instanceof HTMLInputElement && replace.checked;
      if (confirmation instanceof HTMLElement) confirmation.hidden = !enabled;
      if (confirmCheckbox instanceof HTMLInputElement) {
        confirmCheckbox.required = enabled;
        if (!enabled) confirmCheckbox.checked = false;
      }
    };
    replace?.addEventListener("change", updateReplacement);
    updateReplacement();
    renderPreview();
  }

  function boot() {
    initIcons();
    initAutoSubmit();
    initModeSwitchContext();
    initCopyButtons();
    initConfirmationPrompts();
    initIdentityDrawers();
    initSelectionSummaries();
    initIdentityWorkbenchBatch();
    initFormLoading();
    initFlashMessages();
    initSidebarActiveState();
    initTableHover();
    initTableScrollContainers();
    initLocalizedTimes();
    initMobileNav();
    initSharedTomSelectFields();
    initDepartmentTrees();
    initScheduledApplyConfirmation();
    initOrganizationImportPreview();
    ADOrgSync.initAdvancedSyncPage?.();
    ADOrgSync.initConfigPage?.();
    ADOrgSync.initMappingsPage?.();
  }

  ADOrgSync.escapeHtml = escapeHtml;
  ADOrgSync.setLoading = setLoading;
  ADOrgSync.announce = announce;
  ADOrgSync.dismissFlash = dismissFlash;
  window.dismissFlash = dismissFlash;

  if (document.readyState !== "complete") {
    document.addEventListener("DOMContentLoaded", boot, { once: true });
  } else {
    boot();
  }
})();
