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

  function setLoading(button) {
    if (!(button instanceof HTMLElement) || button.classList.contains("btn-loading")) {
      return;
    }
    button.classList.add("btn-loading");
    const width = button.offsetWidth;
    button.style.width = `${width}px`;
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
    const cancelButtons = dialog ? Array.from(dialog.querySelectorAll("[data-confirm-cancel]")) : [];
    let pendingElement = null;
    let restoreFocusTo = null;

    const resolveConfirmationValue = (template, element) => {
      const selectionScope = element.closest("[data-selection-scope]") || element.closest("form");
      const form = element.closest("form");
      const selectedCount = selectionScope?.querySelectorAll("input[type='checkbox']:checked").length || 0;
      const actionSelect = form?.querySelector("select[name='action'], select[name='bulk_action']");
      const selectedAction =
        actionSelect instanceof HTMLSelectElement
          ? actionSelect.selectedOptions[0]?.textContent?.trim() || actionSelect.value
          : "";
      return String(template)
        .replaceAll("{selected_count}", String(selectedCount))
        .replaceAll("{selected_action}", selectedAction);
    };

    const closeDialog = ({ restoreFocus = true } = {}) => {
      if (!(dialog instanceof HTMLElement)) {
        return;
      }
      dialog.hidden = true;
      document.body?.classList.remove("confirm-dialog-open");
      pendingElement = null;
      if (confirmInput instanceof HTMLInputElement) {
        confirmInput.value = "";
      }
      if (approveButton instanceof HTMLButtonElement) {
        approveButton.disabled = false;
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
          restoreFocusTo = element;
          messageTarget.textContent = message;
          if (titleTarget instanceof HTMLElement) {
            titleTarget.textContent =
              source?.getAttribute("data-confirm-title") || titleTarget.dataset.defaultTitle || titleTarget.textContent;
          }
          if (detailsTarget instanceof HTMLElement) {
            detailsTarget.replaceChildren();
            for (let index = 1; index <= 8; index += 1) {
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
            verification.hidden = !requiredText;
            confirmInput.value = "";
            if (inputHelp instanceof HTMLElement) {
              const template = source?.getAttribute("data-confirm-input-help") || "Type {value} to confirm.";
              inputHelp.textContent = template.replace("{value}", requiredText);
            }
            if (approveButton instanceof HTMLButtonElement) {
              approveButton.disabled = Boolean(requiredText);
            }
            confirmInput.oninput = () => {
              if (approveButton instanceof HTMLButtonElement) {
                approveButton.disabled = confirmInput.value.trim() !== requiredText;
              }
            };
          }
          dialog.hidden = false;
          document.body?.classList.add("confirm-dialog-open");
          if (window.lucide) {
            window.lucide.createIcons();
          }
          if (requiredText && confirmInput instanceof HTMLInputElement) {
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

  function initSelectionSummaries() {
    document.querySelectorAll("[data-selection-scope]").forEach((scope) => {
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
      };
      scope.querySelectorAll("input[type='checkbox']").forEach((checkbox) => {
        checkbox.addEventListener("change", update);
      });
      update();
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
    links.forEach((link) => {
      const href = link.getAttribute("href");
      if (href === currentPath || (href !== "/" && currentPath.startsWith(href || ""))) {
        link.classList.add("active");
        const group = link.closest("details");
        if (group instanceof HTMLDetailsElement) {
          group.open = true;
        }
      }
    });

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

    const renderRecent = () => {
      if (!(recent instanceof HTMLElement) || !(recentLinks instanceof HTMLElement)) {
        return;
      }
      recentLinks.replaceChildren();
      recentPaths.slice(0, 3).forEach((path) => {
        const source = links.find((link) => link.getAttribute("href") === path);
        if (!(source instanceof HTMLAnchorElement)) {
          return;
        }
        if (requiredText && window.prompt(`Type ${requiredText} to confirm.`) !== requiredText) {
          event.preventDefault();
          event.stopImmediatePropagation();
          return;
        }
        const clone = source.cloneNode(true);
        clone.setAttribute("data-sidebar-recent-link", "true");
        clone.classList.toggle("active", path === currentPath);
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

  function initMobileNav() {
    const body = document.body;
    const sidebar = document.querySelector("[data-app-sidebar]");
    const toggle = document.querySelector("[data-mobile-nav-toggle]");
    if (!body || !sidebar || !(toggle instanceof HTMLElement)) {
      return;
    }

    const mobileBreakpoint = 768;

    const setOpen = (isOpen) => {
      const normalized = Boolean(isOpen) && window.innerWidth <= mobileBreakpoint;
      body.classList.toggle("mobile-nav-open", normalized);
      toggle.setAttribute("aria-expanded", String(normalized));
    };

    toggle.addEventListener("click", () => {
      setOpen(!body.classList.contains("mobile-nav-open"));
    });

    document.querySelectorAll("[data-mobile-nav-close]").forEach((element) => {
      element.addEventListener("click", () => setOpen(false));
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
      if (event.key === "Escape") {
        setOpen(false);
      }
    });
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

  function boot() {
    initIcons();
    initAutoSubmit();
    initConfirmationPrompts();
    initSelectionSummaries();
    initFormLoading();
    initFlashMessages();
    initSidebarActiveState();
    initTableHover();
    initMobileNav();
    initSharedTomSelectFields();
    ADOrgSync.initAdvancedSyncPage?.();
    ADOrgSync.initConfigPage?.();
    ADOrgSync.initMappingsPage?.();
  }

  ADOrgSync.escapeHtml = escapeHtml;
  ADOrgSync.setLoading = setLoading;
  ADOrgSync.dismissFlash = dismissFlash;
  window.dismissFlash = dismissFlash;

  if (document.readyState !== "complete") {
    document.addEventListener("DOMContentLoaded", boot, { once: true });
  } else {
    boot();
  }
})();
