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

  const badge = (text, level = "info") =>
    `<span class="badge badge-${level}">${escapeHtml(text || "")}</span>`;

  const detailItem = (label, value) => `
    <div class="detail-item">
      <div class="label">${escapeHtml(label || "")}</div>
      <div class="value">${value || "-"}</div>
    </div>
  `;

  const setResultState = (target, html) => {
    if (!(target instanceof HTMLElement)) {
      return;
    }
    target.innerHTML = html;
    if (window.lucide) {
      window.lucide.createIcons();
    }
  };

  const clearLoading = (button) => {
    if (!(button instanceof HTMLElement)) {
      return;
    }
    button.classList.remove("btn-loading");
    button.style.width = "";
  };

  const renderUsernamePreviewSections = (preview, { heading = true } = {}) => {
    const primaryCandidate = preview?.primary_candidate;
    const candidates = Array.isArray(preview?.candidates) ? preview.candidates : [];
    const templateContextEntries = Object.entries(preview?.template_context || {}).filter(
      ([, value]) => String(value || "").trim().length > 0,
    );

    const header = heading
      ? `
        <div class="surface-inline">
          ${badge(preview?.connector?.name || "Connector", "info")}
          ${badge(preview?.strategy || "custom_template", "success")}
          ${badge(preview?.collision_policy || "append_employee_id", "warning")}
        </div>
      `
      : "";

    const primaryBlock = primaryCandidate
      ? `
        <div class="check-item">
          <div class="check-item__header">
            <h3 class="check-item__title"><code>${escapeHtml(primaryCandidate.username || "")}</code></h3>
            ${badge(primaryCandidate.managed ? "Managed Candidate" : "Existing Match", primaryCandidate.managed ? "success" : "info")}
          </div>
          <p class="check-item__detail">${escapeHtml(primaryCandidate.explanation || "")}</p>
          <div class="muted">Rule: ${escapeHtml(primaryCandidate.rule || "-")}</div>
        </div>
      `
      : `
        <div class="panel-note warning">
          No username candidate could be generated from the current sample payload.
        </div>
      `;

    const candidateList = candidates.length
      ? `
        <div class="check-list">
          ${candidates
            .map(
              (candidate) => `
                <div class="check-item">
                  <div class="check-item__header">
                    <h3 class="check-item__title"><code>${escapeHtml(candidate.username || "")}</code></h3>
                    ${badge(candidate.managed ? "Managed" : "Existing Match", candidate.managed ? "success" : "info")}
                  </div>
                  <p class="check-item__detail">${escapeHtml(candidate.explanation || "")}</p>
                  <div class="muted">Rule: ${escapeHtml(candidate.rule || "-")}</div>
                </div>
              `,
            )
            .join("")}
        </div>
      `
      : "";

    const contextGrid = templateContextEntries.length
      ? `
        <div class="detail-grid">
          ${templateContextEntries
            .map(([key, value]) => detailItem(key, `<code>${escapeHtml(value)}</code>`))
            .join("")}
        </div>
      `
      : `
        <div class="panel-note info">
          This preview did not need any additional placeholder values beyond the fields you entered.
        </div>
      `;

    return `
      <div class="stack-tight">
        ${header}
        <div class="detail-grid">
          ${detailItem("Resolved Template", `<code>${escapeHtml(preview?.resolved_template || "-")}</code>`)}
          ${detailItem("Custom Template", `<code>${escapeHtml(preview?.username_template || "-")}</code>`)}
          ${detailItem("Collision Template", `<code>${escapeHtml(preview?.collision_template || "-")}</code>`)}
          ${detailItem("Candidate Count", escapeHtml(String(candidates.length || 0)))}
        </div>
        ${primaryBlock}
        <div>
          <div class="muted">Template Context</div>
          ${contextGrid}
        </div>
        <div>
          <div class="muted">Candidate Order</div>
          ${candidateList}
        </div>
      </div>
    `;
  };

  const renderIdentityExplanation = (explanation) => {
    const departments = Array.isArray(explanation?.departments) ? explanation.departments : [];
    const connectorCandidates = Array.isArray(explanation?.connector_candidates)
      ? explanation.connector_candidates
      : [];

    const routingBlock =
      explanation?.routing_status === "resolved"
        ? `
          <div class="panel-note info">
            Runtime will use connector <strong>${escapeHtml(
              explanation?.selected_connector?.name || explanation?.selected_connector?.connector_id || "default",
            )}</strong> for this identity.
          </div>
        `
        : `
          <div class="panel-note warning">
            This user currently spans multiple connector roots. Runtime would raise a connector-assignment conflict until the scope is simplified.
          </div>
        `;

    const bindingBlock = explanation?.binding
      ? `
        <div class="panel-note info">
          Existing binding: <strong>${escapeHtml(explanation.binding.ad_username || "-")}</strong>
          (${escapeHtml(explanation.binding.source || "manual")})
        </div>
      `
      : "";

    const overrideBlock = explanation?.department_override
      ? `
        <div class="panel-note warning">
          Department override forces primary department ${escapeHtml(
            String(explanation.department_override.primary_department_id || ""),
          )}.
        </div>
      `
      : "";

    const departmentList = departments.length
      ? `
        <div class="check-list">
          ${departments
            .map(
              (department) => `
                <div class="check-item">
                  <div class="check-item__header">
                    <h3 class="check-item__title">${escapeHtml(department.name || "")} [${escapeHtml(
                      String(department.department_id || ""),
                    )}]</h3>
                    ${badge(department.connector_name || department.connector_id || "default", "info")}
                  </div>
                  <p class="check-item__detail">${escapeHtml(department.path_display || "")}</p>
                  <div class="muted">Scoped Path: ${escapeHtml(department.scoped_path_display || "-")}</div>
                  <div class="muted">Scope Root: ${escapeHtml(String(department.scope_root_id || "-"))}</div>
                </div>
              `,
            )
            .join("")}
        </div>
      `
      : `
        <div class="panel-note warning">
          No source department membership was returned for this user.
        </div>
      `;

    const connectorText = connectorCandidates.length
      ? connectorCandidates.map((item) => `${item.name || item.connector_id} [${item.connector_id}]`).join(", ")
      : "-";
    const claimPolicy = explanation?.identity_claim_policy || {};
    const claimCandidates = Array.isArray(claimPolicy?.claim_candidates) ? claimPolicy.claim_candidates : [];
    const claimCandidateList = claimCandidates.length
      ? `
        <div class="check-list">
          ${claimCandidates
            .map(
              (candidate) => `
                <div class="check-item">
                  <div class="check-item__header">
                    <h3 class="check-item__title"><code>${escapeHtml(candidate.username || "")}</code></h3>
                    ${badge("Existing AD Claim Candidate", "info")}
                  </div>
                  <p class="check-item__detail">${escapeHtml(candidate.explanation || "")}</p>
                  <div class="muted">Rule: ${escapeHtml(candidate.rule || "-")}</div>
                </div>
              `,
            )
            .join("")}
        </div>
      `
      : `
        <div class="panel-note info">
          No existing-AD claim candidates were generated from the current source identity.
        </div>
      `;

    return `
      <div class="stack-tight">
        ${routingBlock}
        ${bindingBlock}
        ${overrideBlock}
        <div class="detail-grid">
          ${detailItem("Source User", `<code>${escapeHtml(explanation?.user?.userid || "")}</code>`)}
          ${detailItem("Display Name", escapeHtml(explanation?.user?.name || "-"))}
          ${detailItem("Email", escapeHtml(explanation?.user?.email || "-"))}
          ${detailItem("Connector Candidates", escapeHtml(connectorText))}
          ${detailItem(
            "Selected Connector",
            explanation?.selected_connector
              ? escapeHtml(`${explanation.selected_connector.name || explanation.selected_connector.connector_id} [${explanation.selected_connector.connector_id}]`)
              : "-",
          )}
          ${detailItem("Placement Strategy", escapeHtml(explanation?.placement_strategy || "-"))}
          ${detailItem(
            "Target Department",
            explanation?.target_department
              ? escapeHtml(`${explanation.target_department.name || ""} [${explanation.target_department.department_id || ""}]`)
              : "-",
          )}
          ${detailItem("Target OU Path", `<code>${escapeHtml(explanation?.target_ou_path || "-")}</code>`)}
        </div>
        <div>
          <div class="muted">First Sync Identity Claim</div>
          <div class="panel-note ${claimPolicy?.mode === "review" ? "warning" : "info"}">
            ${escapeHtml(claimPolicy?.label || "Auto-claim safe existing AD matches")}
          </div>
          <div class="detail-grid">
            ${detailItem("Existing Match Behavior", `<code>${escapeHtml(claimPolicy?.existing_match_behavior || "-")}</code>`)}
            ${detailItem("Claim Candidate Count", escapeHtml(String(claimPolicy?.claim_candidate_count ?? claimCandidates.length)))}
          </div>
          ${claimCandidateList}
        </div>
        <div>
          <div class="muted">Department Routing</div>
          ${departmentList}
        </div>
        ${
          explanation?.username_preview
            ? `
              <div>
                <div class="muted">Effective Naming Preview</div>
                ${renderUsernamePreviewSections(explanation.username_preview, { heading: false })}
              </div>
            `
            : ""
        }
      </div>
    `;
  };

  const renderDataQualitySnapshot = (snapshot) => {
    const summary = snapshot?.summary || {};
    const issues = Array.isArray(snapshot?.issues) ? snapshot.issues : [];
    const connectorBreakdown = Array.isArray(snapshot?.connector_breakdown)
      ? snapshot.connector_breakdown
      : [];
    const analysisNotes = Array.isArray(snapshot?.analysis_notes) ? snapshot.analysis_notes : [];

    const issueBlocks = issues.length
      ? `
        <div class="check-list">
          ${issues
            .map((issue) => {
              const samples = Array.isArray(issue?.samples) ? issue.samples : [];
              const sampleBlock = samples.length
                ? `
                  <div class="stack-tight">
                    ${samples
                      .map(
                        (sample) => `
                          <div class="panel-note info">
                            <strong>${escapeHtml(sample?.title || "-")}</strong>
                            <div class="muted">${escapeHtml(sample?.detail || "-")}</div>
                          </div>
                        `,
                      )
                      .join("")}
                  </div>
                `
                : "";
              return `
                <div class="check-item">
                  <div class="check-item__header">
                    <h3 class="check-item__title">${escapeHtml(issue?.label || "Issue")}</h3>
                    ${badge(issue?.severity || "warning", issue?.severity || "warning")}
                  </div>
                  <p class="check-item__detail">${escapeHtml(issue?.description || "-")}</p>
                  <div class="muted">Affected records: ${escapeHtml(String(issue?.count || 0))}</div>
                  <div class="muted">Recommended action: ${escapeHtml(issue?.action || "-")}</div>
                  ${sampleBlock}
                </div>
              `;
            })
            .join("")}
        </div>
      `
      : `
        <div class="panel-note success">
          No obvious source-data blockers were detected in this snapshot.
        </div>
      `;

    const connectorBlock = connectorBreakdown.length
      ? `
        <div class="surface-inline">
          ${connectorBreakdown
            .map((item) =>
              badge(
                `${item?.name || item?.connector_id || "Connector"}: ${String(item?.user_count || 0)}`,
                item?.connector_id === "__multiple__" || item?.connector_id === "__unrouted__" ? "warning" : "info",
              ),
            )
            .join("")}
        </div>
      `
      : "";

    const notesBlock = analysisNotes.length
      ? `
        <div class="stack-tight">
          ${analysisNotes
            .map((note) => `<div class="muted">${escapeHtml(note || "")}</div>`)
            .join("")}
        </div>
      `
      : "";

    return `
      <div class="stack-tight">
        <div class="surface-inline">
          ${badge(
            summary?.error_issue_count ? "Needs Attention" : summary?.warning_issue_count ? "Review Recommended" : "Healthy",
            summary?.error_issue_count ? "error" : summary?.warning_issue_count ? "warning" : "success",
          )}
          <span class="muted">Generated at ${escapeHtml(snapshot?.generated_at || "-")}</span>
        </div>
        <div class="detail-grid">
          ${detailItem("Users", escapeHtml(String(summary?.total_users || 0)))}
          ${detailItem("Departments", escapeHtml(String(summary?.department_count || 0)))}
          ${detailItem("Missing Email", escapeHtml(String(summary?.users_missing_email || 0)))}
          ${detailItem("Missing Employee ID", escapeHtml(String(summary?.users_missing_employee_id || 0)))}
          ${detailItem("Placement Gaps", escapeHtml(String(summary?.placement_unresolved_count || 0)))}
          ${detailItem("Connector Ambiguity", escapeHtml(String(summary?.routing_ambiguity_count || 0)))}
          ${detailItem("Naming Gaps", escapeHtml(String(summary?.naming_prerequisite_gap_count || 0)))}
          ${detailItem("Username Collisions", escapeHtml(String(summary?.managed_username_collision_count || 0)))}
          ${detailItem("Duplicate Emails", escapeHtml(String(summary?.duplicate_email_count || 0)))}
          ${detailItem("Duplicate Employee IDs", escapeHtml(String(summary?.duplicate_employee_id_count || 0)))}
        </div>
        ${connectorBlock}
        ${notesBlock}
        <div>
          <div class="muted">Detected Issues</div>
          ${issueBlocks}
        </div>
      </div>
    `;
  };

  ADOrgSync.initAdvancedSyncPage = () => {
    const page = document.querySelector("[data-advanced-sync-page]");
    if (!(page instanceof HTMLElement)) {
      return;
    }

    const dataQualitySnapshotUrl =
      page.dataset.dataQualitySnapshotUrl || "/advanced-sync/data-quality-snapshot";
    const usernamePreviewUrl = page.dataset.usernamePreviewUrl || "/advanced-sync/username-preview";
    const identityExplainUrl = page.dataset.identityExplainUrl || "/advanced-sync/identity-explain";

    const dataQualityButton = page.querySelector("[data-data-quality-run]");
    const dataQualityResults = page.querySelector("[data-data-quality-results]");
    if (dataQualityButton instanceof HTMLElement && dataQualityResults instanceof HTMLElement) {
      dataQualityButton.addEventListener("click", () => {
        ADOrgSync.setLoading?.(dataQualityButton);
        setResultState(
          dataQualityResults,
          '<div class="panel-note info">Scanning source users, departments, and naming outcomes. This can take a little while on larger directories...</div>',
        );
        fetch(dataQualitySnapshotUrl, {
          credentials: "same-origin",
        })
          .then((response) => response.json())
          .then((json) => {
            if (!json?.ok) {
              throw new Error(json?.error || "Data quality snapshot failed.");
            }
            setResultState(dataQualityResults, renderDataQualitySnapshot(json.snapshot || {}));
          })
          .catch((error) => {
            setResultState(
              dataQualityResults,
              `<div class="panel-note error">${escapeHtml(error?.message || "Data quality snapshot failed.")}</div>`,
            );
          })
          .finally(() => clearLoading(dataQualityButton));
      });
    }

    const previewForm = page.querySelector("[data-username-preview-form]");
    const previewResults = page.querySelector("[data-username-preview-results]");
    if (previewForm instanceof HTMLFormElement && previewResults instanceof HTMLElement) {
      previewForm.addEventListener("submit", (event) => {
        event.preventDefault();
        const submitter =
          event.submitter instanceof HTMLElement
            ? event.submitter
            : previewForm.querySelector('button[type="submit"]');
        ADOrgSync.setLoading?.(submitter);
        setResultState(
          previewResults,
          '<div class="panel-note info">Building username candidates from the sample payload...</div>',
        );
        fetch(usernamePreviewUrl, {
          method: "POST",
          credentials: "same-origin",
          body: new FormData(previewForm),
        })
          .then((response) => response.json())
          .then((json) => {
            if (!json?.ok) {
              throw new Error(json?.error || "Preview failed.");
            }
            setResultState(previewResults, renderUsernamePreviewSections(json.preview || {}));
          })
          .catch((error) => {
            setResultState(
              previewResults,
              `<div class="panel-note error">${escapeHtml(error?.message || "Preview failed.")}</div>`,
            );
          })
          .finally(() => clearLoading(submitter));
      });
    }

    const explainForm = page.querySelector("[data-identity-explain-form]");
    const explainResults = page.querySelector("[data-identity-explain-results]");
    if (explainForm instanceof HTMLFormElement && explainResults instanceof HTMLElement) {
      explainForm.addEventListener("submit", (event) => {
        event.preventDefault();
        const submitter =
          event.submitter instanceof HTMLElement
            ? event.submitter
            : explainForm.querySelector('button[type="submit"]');
        ADOrgSync.setLoading?.(submitter);
        const sourceUserId = String(
          new FormData(explainForm).get("source_user_id") || "",
        ).trim();
        if (!sourceUserId) {
          setResultState(
            explainResults,
            '<div class="panel-note warning">Enter a source user ID before running the explainer.</div>',
          );
          clearLoading(submitter);
          return;
        }
        setResultState(
          explainResults,
          '<div class="panel-note info">Resolving connector scope, placement, and naming rules...</div>',
        );
        fetch(`${identityExplainUrl}?user_id=${encodeURIComponent(sourceUserId)}`, {
          credentials: "same-origin",
        })
          .then((response) => response.json())
          .then((json) => {
            if (!json?.ok) {
              throw new Error(json?.error || "Explanation failed.");
            }
            setResultState(explainResults, renderIdentityExplanation(json.explanation || {}));
          })
          .catch((error) => {
            setResultState(
              explainResults,
              `<div class="panel-note error">${escapeHtml(error?.message || "Explanation failed.")}</div>`,
            );
          })
          .finally(() => clearLoading(submitter));
      });
    }
  };
})();
