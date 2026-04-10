document.addEventListener("DOMContentLoaded", () => {
  const defaultConfirmMessage =
    document.body?.dataset.confirmMessage || "Are you sure you want to perform this action?";

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
});
