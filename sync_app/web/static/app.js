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
    const approveLabel = approveButton?.querySelector("span");
    const defaultApproveLabel = approveLabel?.textContent || "Continue";
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
            verification.hidden = !requiredText;
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

  function initIdentityDrawers() {
    const drawers = Array.from(document.querySelectorAll("[data-identity-drawer]"));
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
      document.body?.classList.remove("identity-drawer-open");
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
      document.body?.classList.add("identity-drawer-open");
      const preferred = drawer.querySelector(
        ".identity-drawer__header [data-identity-drawer-close]"
      );
      const panel = drawer.querySelector(".identity-drawer__panel");
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
      const panel = activeDrawer.querySelector(".identity-drawer__panel");
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
    const activeLink = links
      .filter((link) => routePaths(link).some(pathMatches))
      .sort((left, right) => {
        const leftLength = Math.max(...routePaths(left).filter(pathMatches).map((path) => path.length));
        const rightLength = Math.max(...routePaths(right).filter(pathMatches).map((path) => path.length));
        return rightLength - leftLength;
      })[0];

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
    const containers = Array.from(document.querySelectorAll(".table-shell"));
    if (!containers.length) {
      return;
    }
    const label =
      document.body?.dataset.tableScrollLabel ||
      "Scrollable data table. Use arrow keys to review hidden columns.";
    const update = () => {
      containers.forEach((container) => {
        const scrollable = container.scrollWidth > container.clientWidth + 1;
        container.dataset.scrollable = String(scrollable);
        container.setAttribute("role", "region");
        container.setAttribute("aria-label", label);
      });
    };
    update();
    window.addEventListener("resize", update);
  }

  function initLocalizedTimes() {
    const locale = document.documentElement.lang || navigator.language || "en";
    const formatter = new Intl.DateTimeFormat(locale, {
      year: "numeric",
      month: "short",
      day: "numeric",
      hour: "2-digit",
      minute: "2-digit",
      timeZoneName: "short",
    });
    document.querySelectorAll("time[data-local-time]").forEach((element) => {
      const rawValue = element.getAttribute("datetime") || "";
      const parsed = new Date(rawValue);
      if (!rawValue || Number.isNaN(parsed.getTime())) {
        return;
      }
      const formatted = formatter.format(parsed);
      element.textContent = formatted;
      element.setAttribute("aria-label", formatted);
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

  function boot() {
    initIcons();
    initAutoSubmit();
    initCopyButtons();
    initConfirmationPrompts();
    initIdentityDrawers();
    initSelectionSummaries();
    initFormLoading();
    initFlashMessages();
    initSidebarActiveState();
    initTableHover();
    initTableScrollContainers();
    initLocalizedTimes();
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
