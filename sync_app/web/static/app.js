document.addEventListener("DOMContentLoaded", () => {
  const defaultConfirmMessage =
    document.body?.dataset.confirmMessage || "Are you sure you want to perform this action?";

  document.querySelectorAll("[data-auto-submit]").forEach((element) => {
    element.addEventListener("change", () => {
      const form = element.form;
      if (!form) {
        return;
      }
      if (typeof form.requestSubmit === "function") {
        form.requestSubmit();
        return;
      }
      form.submit();
    });
  });

  document.querySelectorAll("[data-dismiss-target]").forEach((element) => {
    const dismiss = () => {
      const target = document.querySelector(element.dataset.dismissTarget || "");
      if (target) {
        target.remove();
      }
    };
    element.addEventListener("click", dismiss);
    element.addEventListener("keydown", (event) => {
      if (event.key === "Enter" || event.key === " ") {
        event.preventDefault();
        dismiss();
      }
    });
  });

  document.querySelectorAll("form").forEach((form) => {
    form.addEventListener("submit", (event) => {
      const submitter =
        event.submitter ||
        form.querySelector('button[type="submit"], input[type="submit"]');

      if (!submitter) {
        return;
      }

      const confirmMessage =
        submitter.dataset.confirm ||
        form.dataset.confirm ||
        (submitter.classList.contains("confirm-action") || form.classList.contains("confirm-action")
          ? defaultConfirmMessage
          : "");

      if (confirmMessage && !window.confirm(confirmMessage)) {
        event.preventDefault();
        return;
      }

      const skipLoader =
        submitter.classList.contains("no-loader") ||
        submitter.hasAttribute("data-no-loader") ||
        form.hasAttribute("data-no-loader");
      if (skipLoader) {
        return;
      }

      window.requestAnimationFrame(() => {
        submitter.classList.add("btn-loading");
        submitter.disabled = true;
      });
    });
  });

  const flash = document.getElementById("global-flash");
  if (flash && flash.classList.contains("success")) {
    window.setTimeout(() => {
      flash.style.opacity = "0";
      flash.style.transition = "opacity 0.5s ease";
      window.setTimeout(() => flash.remove(), 500);
    }, 5000);
  }
});
