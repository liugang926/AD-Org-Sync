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
    document.querySelectorAll("button[data-confirm], a[data-confirm]").forEach((element) => {
      element.addEventListener("click", (event) => {
        const message = element.getAttribute("data-confirm") || defaultConfirmMessage();
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
    document.querySelectorAll("[data-sidebar-nav] a").forEach((link) => {
      const href = link.getAttribute("href");
      if (href === currentPath || (href !== "/" && currentPath.startsWith(href || ""))) {
        link.classList.add("active");
      }
    });
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

      new TomSelect(element, {
        create: true,
        valueField: "id",
        labelField: "name",
        searchField: ["name", "id"],
        preload: true,
        plugins: ["remove_button", "clear_button"],
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
      new TomSelect(element, {
        create: true,
        plugins: ["remove_button", "clear_button"],
        persist: false,
        createOnBlur: true,
      });
    });
  }

  function boot() {
    initIcons();
    initAutoSubmit();
    initConfirmationPrompts();
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
