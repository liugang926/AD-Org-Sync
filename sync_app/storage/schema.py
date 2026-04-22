from __future__ import annotations

DEFAULT_APP_SETTINGS = {
    "group_display_separator": ("-", "string"),
    "group_display_mode": ("compact_path", "string"),
    "group_display_apply_scope": ("new_only", "string"),
    "group_recursive_enabled": ("true", "bool"),
    "schedule_execution_mode": ("apply", "string"),
    "managed_relation_cleanup_enabled": ("false", "bool"),
    "user_ou_placement_strategy": ("source_primary_department", "string"),
    "source_root_unit_ids": ("", "string"),
    "source_root_unit_display_text": ("", "string"),
    "directory_root_ou_path": ("", "string"),
    "disabled_users_ou_path": ("Disabled Users", "string"),
    "job_history_retention_days": ("30", "int"),
    "event_history_retention_days": ("30", "int"),
    "audit_log_retention_days": ("90", "int"),
    "web_bind_host": ("127.0.0.1", "string"),
    "web_bind_port": ("8000", "int"),
    "web_public_base_url": ("", "string"),
    "web_session_cookie_secure_mode": ("auto", "string"),
    "web_trust_proxy_headers": ("false", "bool"),
    "web_forwarded_allow_ips": ("127.0.0.1", "string"),
    "brand_display_name": ("AD Org Sync", "string"),
    "brand_mark_text": ("AD", "string"),
    "brand_attribution": ("微信公众号：大刘讲IT", "string"),
    "web_session_idle_minutes": ("30", "int"),
    "web_login_max_attempts": ("5", "int"),
    "web_login_window_seconds": ("300", "int"),
    "web_login_lockout_seconds": ("300", "int"),
    "web_admin_password_min_length": ("8", "int"),
    "wecom_department_cache_ttl_seconds": ("300", "int"),
    "backup_retention_days": ("30", "int"),
    "backup_retention_max_files": ("30", "int"),
    "high_risk_apply_requires_review": ("true", "bool"),
    "high_risk_review_ttl_minutes": ("240", "int"),
    "offboarding_grace_days": ("0", "int"),
    "offboarding_notify_managers": ("false", "bool"),
    "disable_circuit_breaker_enabled": ("false", "bool"),
    "disable_circuit_breaker_percent": ("5", "float"),
    "disable_circuit_breaker_min_count": ("10", "int"),
    "disable_circuit_breaker_requires_approval": ("true", "bool"),
    "managed_group_type": ("security", "string"),
    "managed_group_mail_domain": ("", "string"),
    "custom_group_ou_path": ("Managed Groups", "string"),
    "advanced_connector_routing_enabled": ("false", "bool"),
    "attribute_mapping_enabled": ("false", "bool"),
    "write_back_enabled": ("false", "bool"),
    "custom_group_sync_enabled": ("false", "bool"),
    "offboarding_lifecycle_enabled": ("false", "bool"),
    "field_conflict_queue_enabled": ("false", "bool"),
    "rehire_restore_enabled": ("false", "bool"),
    "custom_group_archive_enabled": ("false", "bool"),
    "scheduled_review_execution_enabled": ("false", "bool"),
    "automatic_replay_enabled": ("false", "bool"),
    "ops_notify_dry_run_failure_enabled": ("false", "bool"),
    "ops_notify_conflict_backlog_enabled": ("false", "bool"),
    "ops_notify_conflict_backlog_threshold": ("5", "int"),
    "ops_notify_review_pending_enabled": ("false", "bool"),
    "ops_notify_rule_governance_enabled": ("false", "bool"),
    "ops_scheduled_apply_gate_enabled": ("true", "bool"),
    "ops_scheduled_apply_max_dry_run_age_hours": ("24", "int"),
    "ops_scheduled_apply_requires_zero_conflicts": ("true", "bool"),
    "ops_scheduled_apply_requires_review_approval": ("true", "bool"),
    "integration_api_token": ("", "string"),
    "future_onboarding_enabled": ("false", "bool"),
    "future_onboarding_start_field": ("hire_date", "string"),
    "contractor_lifecycle_enabled": ("false", "bool"),
    "lifecycle_employment_type_field": ("employment_type", "string"),
    "contractor_end_field": ("contract_end_date", "string"),
    "lifecycle_sponsor_field": ("sponsor_userid", "string"),
    "contractor_type_values": ("contractor,intern,vendor,temp", "string"),
}


ORG_SCOPED_APP_SETTINGS = {
    "group_display_separator",
    "group_display_mode",
    "group_display_apply_scope",
    "group_recursive_enabled",
    "group_recursive_enabled_user_override",
    "schedule_execution_mode",
    "managed_relation_cleanup_enabled",
    "user_ou_placement_strategy",
    "source_root_unit_ids",
    "source_root_unit_display_text",
    "directory_root_ou_path",
    "disabled_users_ou_path",
    "offboarding_grace_days",
    "offboarding_notify_managers",
    "disable_circuit_breaker_enabled",
    "disable_circuit_breaker_percent",
    "disable_circuit_breaker_min_count",
    "disable_circuit_breaker_requires_approval",
    "managed_group_type",
    "managed_group_mail_domain",
    "custom_group_ou_path",
    "advanced_connector_routing_enabled",
    "attribute_mapping_enabled",
    "write_back_enabled",
    "custom_group_sync_enabled",
    "offboarding_lifecycle_enabled",
    "field_conflict_queue_enabled",
    "rehire_restore_enabled",
    "custom_group_archive_enabled",
    "scheduled_review_execution_enabled",
    "automatic_replay_enabled",
    "ops_notify_dry_run_failure_enabled",
    "ops_notify_conflict_backlog_enabled",
    "ops_notify_conflict_backlog_threshold",
    "ops_notify_review_pending_enabled",
    "ops_notify_rule_governance_enabled",
    "ops_scheduled_apply_gate_enabled",
    "ops_scheduled_apply_max_dry_run_age_hours",
    "ops_scheduled_apply_requires_zero_conflicts",
    "ops_scheduled_apply_requires_review_approval",
    "integration_api_token",
    "future_onboarding_enabled",
    "future_onboarding_start_field",
    "contractor_lifecycle_enabled",
    "lifecycle_employment_type_field",
    "contractor_end_field",
    "lifecycle_sponsor_field",
    "contractor_type_values",
    "last_sync_time",
    "last_sync_success",
}


DEFAULT_HARD_PROTECTED_GROUPS = [
    "Domain Admins",
    "Schema Admins",
    "Enterprise Admins",
    "Administrators",
    "Account Operators",
    "Server Operators",
    "Backup Operators",
    "Print Operators",
    "Domain Controllers",
    "Read-only Domain Controllers",
    "Protected Users",
    "Key Admins",
    "Enterprise Key Admins",
]


DEFAULT_SOFT_EXCLUDED_GROUPS = [
    "Domain Users",
    "Domain Guests",
    "Domain Computers",
    "Users",
    "Guests",
    "Replicator",
    "Group Policy Creator Owners",
]


MIGRATIONS = [
    (
        1,
        "create core local storage tables",
        """
        CREATE TABLE IF NOT EXISTS schema_migrations (
          version INTEGER PRIMARY KEY,
          description TEXT NOT NULL,
          applied_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS app_settings (
          key TEXT PRIMARY KEY,
          value TEXT NOT NULL,
          value_type TEXT NOT NULL DEFAULT 'string',
          updated_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS group_exclusion_rules (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          rule_type TEXT NOT NULL,
          protection_level TEXT NOT NULL,
          match_type TEXT NOT NULL,
          match_value TEXT NOT NULL,
          display_name TEXT,
          is_enabled INTEGER NOT NULL DEFAULT 1,
          source TEXT NOT NULL,
          created_at TEXT NOT NULL,
          updated_at TEXT NOT NULL
        );

        CREATE UNIQUE INDEX IF NOT EXISTS idx_group_exclusion_rules_unique
        ON group_exclusion_rules (rule_type, protection_level, match_type, match_value);

        CREATE TABLE IF NOT EXISTS sync_jobs (
          job_id TEXT PRIMARY KEY,
          trigger_type TEXT NOT NULL,
          execution_mode TEXT NOT NULL,
          status TEXT NOT NULL,
          plan_source_job_id TEXT,
          app_version TEXT,
          config_snapshot_hash TEXT,
          started_at TEXT NOT NULL,
          ended_at TEXT,
          planned_operation_count INTEGER NOT NULL DEFAULT 0,
          executed_operation_count INTEGER NOT NULL DEFAULT 0,
          error_count INTEGER NOT NULL DEFAULT 0,
          summary_json TEXT
        );

        CREATE INDEX IF NOT EXISTS idx_sync_jobs_started_at ON sync_jobs (started_at DESC);
        CREATE INDEX IF NOT EXISTS idx_sync_jobs_status ON sync_jobs (status);

        CREATE TABLE IF NOT EXISTS sync_events (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          job_id TEXT NOT NULL,
          stage_name TEXT,
          level TEXT NOT NULL,
          event_type TEXT NOT NULL,
          message TEXT NOT NULL,
          payload_json TEXT,
          created_at TEXT NOT NULL,
          FOREIGN KEY(job_id) REFERENCES sync_jobs(job_id)
        );

        CREATE INDEX IF NOT EXISTS idx_sync_events_job_id ON sync_events (job_id, created_at);
        CREATE INDEX IF NOT EXISTS idx_sync_events_event_type ON sync_events (event_type);

        CREATE TABLE IF NOT EXISTS planned_operations (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          job_id TEXT NOT NULL,
          object_type TEXT NOT NULL,
          source_id TEXT,
          department_id TEXT,
          target_dn TEXT,
          operation_type TEXT NOT NULL,
          desired_state_json TEXT,
          risk_level TEXT NOT NULL DEFAULT 'normal',
          status TEXT NOT NULL DEFAULT 'planned',
          created_at TEXT NOT NULL,
          FOREIGN KEY(job_id) REFERENCES sync_jobs(job_id)
        );

        CREATE INDEX IF NOT EXISTS idx_planned_operations_job_id
        ON planned_operations (job_id, object_type, operation_type);

        CREATE TABLE IF NOT EXISTS object_sync_state (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          source_type TEXT NOT NULL,
          object_type TEXT NOT NULL,
          source_id TEXT NOT NULL,
          source_hash TEXT NOT NULL,
          display_name TEXT,
          target_dn TEXT,
          last_seen_at TEXT NOT NULL,
          last_job_id TEXT,
          last_action TEXT,
          last_status TEXT,
          extra_json TEXT,
          UNIQUE(source_type, object_type, source_id)
        );

        CREATE INDEX IF NOT EXISTS idx_object_sync_state_object
        ON object_sync_state (object_type, source_id);

        CREATE TABLE IF NOT EXISTS managed_group_bindings (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          department_id TEXT NOT NULL UNIQUE,
          parent_department_id TEXT,
          group_sam TEXT NOT NULL UNIQUE,
          group_dn TEXT,
          group_cn TEXT,
          display_name TEXT,
          path_text TEXT,
          status TEXT NOT NULL DEFAULT 'active',
          updated_at TEXT NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_managed_group_bindings_parent
        ON managed_group_bindings (parent_department_id);
        """,
    ),
    (
        2,
        "backfill recursive group default and track user override",
        """
        UPDATE app_settings
        SET value = 'true',
            value_type = 'bool',
            updated_at = CURRENT_TIMESTAMP
        WHERE key = 'group_recursive_enabled'
          AND LOWER(value) IN ('0', 'false', 'no', 'off')
          AND NOT EXISTS (
              SELECT 1
              FROM app_settings
              WHERE key = 'group_recursive_enabled_user_override'
                AND LOWER(value) IN ('1', 'true', 'yes', 'on')
          );

        INSERT OR IGNORE INTO app_settings (key, value, value_type, updated_at)
        VALUES ('group_recursive_enabled_user_override', 'false', 'bool', CURRENT_TIMESTAMP);
        """,
    ),
    (
        3,
        "create web admin and audit tables",
        """
        CREATE TABLE IF NOT EXISTS web_admin_users (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          username TEXT NOT NULL UNIQUE,
          password_hash TEXT NOT NULL,
          role TEXT NOT NULL DEFAULT 'super_admin',
          is_enabled INTEGER NOT NULL DEFAULT 1,
          must_change_password INTEGER NOT NULL DEFAULT 0,
          created_at TEXT NOT NULL,
          updated_at TEXT NOT NULL,
          last_login_at TEXT
        );

        CREATE INDEX IF NOT EXISTS idx_web_admin_users_username
        ON web_admin_users (username);

        CREATE TABLE IF NOT EXISTS web_audit_logs (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          actor_username TEXT,
          action_type TEXT NOT NULL,
          target_type TEXT,
          target_id TEXT,
          result TEXT NOT NULL,
          message TEXT NOT NULL,
          payload_json TEXT,
          created_at TEXT NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_web_audit_logs_created_at
        ON web_audit_logs (created_at DESC);

        CREATE INDEX IF NOT EXISTS idx_web_audit_logs_action
        ON web_audit_logs (action_type, created_at DESC);
        """,
    ),
    (
        4,
        "create user identity binding and department override tables",
        """
        CREATE TABLE IF NOT EXISTS user_identity_bindings (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          wecom_userid TEXT NOT NULL UNIQUE,
          ad_username TEXT NOT NULL UNIQUE,
          source TEXT NOT NULL DEFAULT 'derived_default',
          notes TEXT,
          is_enabled INTEGER NOT NULL DEFAULT 1,
          updated_at TEXT NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_user_identity_bindings_ad_username
        ON user_identity_bindings (ad_username);

        CREATE TABLE IF NOT EXISTS user_department_overrides (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          wecom_userid TEXT NOT NULL UNIQUE,
          primary_department_id TEXT NOT NULL,
          notes TEXT,
          updated_at TEXT NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_user_department_overrides_department
        ON user_department_overrides (primary_department_id);
        """,
    ),
    (
        5,
        "create sync operation, conflict, and review tables",
        """
        CREATE TABLE IF NOT EXISTS sync_operation_logs (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          job_id TEXT NOT NULL,
          stage_name TEXT NOT NULL,
          object_type TEXT NOT NULL,
          operation_type TEXT NOT NULL,
          source_id TEXT,
          department_id TEXT,
          target_id TEXT,
          target_dn TEXT,
          risk_level TEXT NOT NULL DEFAULT 'normal',
          status TEXT NOT NULL,
          message TEXT NOT NULL,
          rule_source TEXT,
          reason_code TEXT,
          details_json TEXT,
          created_at TEXT NOT NULL,
          FOREIGN KEY(job_id) REFERENCES sync_jobs(job_id)
        );

        CREATE INDEX IF NOT EXISTS idx_sync_operation_logs_job
        ON sync_operation_logs (job_id, created_at DESC, id DESC);

        CREATE TABLE IF NOT EXISTS sync_conflicts (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          job_id TEXT NOT NULL,
          conflict_type TEXT NOT NULL,
          severity TEXT NOT NULL DEFAULT 'warning',
          status TEXT NOT NULL DEFAULT 'open',
          source_id TEXT NOT NULL,
          target_key TEXT,
          message TEXT NOT NULL,
          resolution_hint TEXT,
          details_json TEXT,
          created_at TEXT NOT NULL,
          FOREIGN KEY(job_id) REFERENCES sync_jobs(job_id)
        );

        CREATE INDEX IF NOT EXISTS idx_sync_conflicts_job
        ON sync_conflicts (job_id, created_at DESC, id DESC);

        CREATE TABLE IF NOT EXISTS sync_plan_reviews (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          job_id TEXT NOT NULL UNIQUE,
          plan_fingerprint TEXT NOT NULL,
          config_snapshot_hash TEXT NOT NULL,
          high_risk_operation_count INTEGER NOT NULL DEFAULT 0,
          status TEXT NOT NULL DEFAULT 'pending',
          reviewer_username TEXT,
          review_notes TEXT,
          created_at TEXT NOT NULL,
          reviewed_at TEXT,
          expires_at TEXT,
          FOREIGN KEY(job_id) REFERENCES sync_jobs(job_id)
        );

        CREATE INDEX IF NOT EXISTS idx_sync_plan_reviews_match
        ON sync_plan_reviews (plan_fingerprint, config_snapshot_hash, status, expires_at);
        """,
    ),
    (
        6,
        "create sync exception rules table",
        """
        CREATE TABLE IF NOT EXISTS sync_exception_rules (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          rule_type TEXT NOT NULL,
          match_type TEXT NOT NULL,
          match_value TEXT NOT NULL,
          notes TEXT,
          is_enabled INTEGER NOT NULL DEFAULT 1,
          created_at TEXT NOT NULL,
          updated_at TEXT NOT NULL
        );

        CREATE UNIQUE INDEX IF NOT EXISTS idx_sync_exception_rules_unique
        ON sync_exception_rules (rule_type, match_type, match_value);

        CREATE INDEX IF NOT EXISTS idx_sync_exception_rules_enabled
        ON sync_exception_rules (is_enabled, rule_type, updated_at DESC);
        """,
    ),
    (
        7,
        "extend sync conflicts with resolution metadata",
        """
        ALTER TABLE sync_conflicts ADD COLUMN resolution_payload_json TEXT;
        ALTER TABLE sync_conflicts ADD COLUMN resolved_at TEXT;
        """,
    ),
    (
        8,
        "add pagination and retention indexes",
        """
        CREATE INDEX IF NOT EXISTS idx_sync_events_job_created_id
        ON sync_events (job_id, created_at DESC, id DESC);

        CREATE INDEX IF NOT EXISTS idx_planned_operations_job_created_id
        ON planned_operations (job_id, created_at ASC, id ASC);

        CREATE INDEX IF NOT EXISTS idx_sync_operation_logs_job_created_id
        ON sync_operation_logs (job_id, created_at ASC, id ASC);

        CREATE INDEX IF NOT EXISTS idx_sync_conflicts_status_created_id
        ON sync_conflicts (status, created_at DESC, id DESC);

        CREATE INDEX IF NOT EXISTS idx_sync_conflicts_job_status_created_id
        ON sync_conflicts (job_id, status, created_at DESC, id DESC);

        CREATE INDEX IF NOT EXISTS idx_web_audit_logs_created_id
        ON web_audit_logs (created_at DESC, id DESC);
        """,
    ),
    (
        9,
        "add enterprise sync policy tables and connector-aware bindings",
        """
        ALTER TABLE user_identity_bindings RENAME TO user_identity_bindings_v1;

        CREATE TABLE IF NOT EXISTS user_identity_bindings (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          wecom_userid TEXT NOT NULL UNIQUE,
          connector_id TEXT NOT NULL DEFAULT 'default',
          ad_username TEXT NOT NULL,
          source TEXT NOT NULL DEFAULT 'derived_default',
          notes TEXT,
          is_enabled INTEGER NOT NULL DEFAULT 1,
          updated_at TEXT NOT NULL
        );

        INSERT INTO user_identity_bindings (
          id, wecom_userid, connector_id, ad_username, source, notes, is_enabled, updated_at
        )
        SELECT id, wecom_userid, 'default', ad_username, source, notes, is_enabled, updated_at
        FROM user_identity_bindings_v1;

        DROP TABLE user_identity_bindings_v1;

        CREATE UNIQUE INDEX IF NOT EXISTS idx_user_identity_bindings_connector_username
        ON user_identity_bindings (connector_id, ad_username);

        CREATE INDEX IF NOT EXISTS idx_user_identity_bindings_ad_username
        ON user_identity_bindings (connector_id, ad_username);

        CREATE TABLE IF NOT EXISTS attribute_mapping_rules (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          connector_id TEXT NOT NULL DEFAULT '',
          direction TEXT NOT NULL,
          source_field TEXT NOT NULL,
          target_field TEXT NOT NULL,
          transform_template TEXT,
          sync_mode TEXT NOT NULL DEFAULT 'replace',
          is_enabled INTEGER NOT NULL DEFAULT 1,
          notes TEXT,
          created_at TEXT NOT NULL,
          updated_at TEXT NOT NULL
        );

        CREATE UNIQUE INDEX IF NOT EXISTS idx_attribute_mapping_rules_unique
        ON attribute_mapping_rules (connector_id, direction, source_field, target_field);

        CREATE INDEX IF NOT EXISTS idx_attribute_mapping_rules_direction
        ON attribute_mapping_rules (direction, connector_id, is_enabled, updated_at DESC);

        CREATE TABLE IF NOT EXISTS sync_connectors (
          connector_id TEXT PRIMARY KEY,
          name TEXT NOT NULL,
          config_path TEXT NOT NULL,
          root_department_ids_json TEXT,
          username_template TEXT,
          disabled_users_ou TEXT,
          group_type TEXT NOT NULL DEFAULT 'security',
          group_mail_domain TEXT,
          custom_group_ou_path TEXT,
          managed_tag_ids_json TEXT,
          managed_external_chat_ids_json TEXT,
          is_enabled INTEGER NOT NULL DEFAULT 1,
          created_at TEXT NOT NULL,
          updated_at TEXT NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_sync_connectors_enabled
        ON sync_connectors (is_enabled, updated_at DESC);

        CREATE TABLE IF NOT EXISTS offboarding_queue (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          connector_id TEXT NOT NULL DEFAULT 'default',
          wecom_userid TEXT,
          ad_username TEXT NOT NULL,
          status TEXT NOT NULL DEFAULT 'pending',
          reason TEXT,
          manager_userids_json TEXT,
          first_missing_at TEXT NOT NULL,
          due_at TEXT NOT NULL,
          notified_at TEXT,
          last_job_id TEXT,
          updated_at TEXT NOT NULL
        );

        CREATE UNIQUE INDEX IF NOT EXISTS idx_offboarding_queue_unique
        ON offboarding_queue (connector_id, ad_username);

        CREATE INDEX IF NOT EXISTS idx_offboarding_queue_status_due
        ON offboarding_queue (status, due_at, connector_id);

        CREATE TABLE IF NOT EXISTS custom_managed_group_bindings (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          connector_id TEXT NOT NULL DEFAULT 'default',
          source_type TEXT NOT NULL,
          source_key TEXT NOT NULL,
          group_sam TEXT NOT NULL,
          group_dn TEXT,
          group_cn TEXT,
          display_name TEXT,
          status TEXT NOT NULL DEFAULT 'active',
          updated_at TEXT NOT NULL
        );

        CREATE UNIQUE INDEX IF NOT EXISTS idx_custom_managed_group_bindings_source
        ON custom_managed_group_bindings (connector_id, source_type, source_key);

        CREATE UNIQUE INDEX IF NOT EXISTS idx_custom_managed_group_bindings_group
        ON custom_managed_group_bindings (connector_id, group_sam);
        """,
    ),
    (
        10,
        "add business loop defaults, expiring exception rules, replay queue, and custom group lifecycle fields",
        """
        ALTER TABLE sync_exception_rules ADD COLUMN expires_at TEXT;
        ALTER TABLE sync_exception_rules ADD COLUMN is_once INTEGER NOT NULL DEFAULT 0;
        ALTER TABLE sync_exception_rules ADD COLUMN last_matched_at TEXT;

        CREATE INDEX IF NOT EXISTS idx_sync_exception_rules_expires
        ON sync_exception_rules (is_enabled, expires_at, rule_type, updated_at DESC);

        ALTER TABLE custom_managed_group_bindings ADD COLUMN last_seen_at TEXT;
        ALTER TABLE custom_managed_group_bindings ADD COLUMN archived_at TEXT;

        CREATE INDEX IF NOT EXISTS idx_custom_managed_group_bindings_status
        ON custom_managed_group_bindings (status, connector_id, updated_at DESC);

        CREATE TABLE IF NOT EXISTS sync_replay_requests (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          request_type TEXT NOT NULL,
          execution_mode TEXT NOT NULL,
          status TEXT NOT NULL DEFAULT 'pending',
          requested_by TEXT,
          target_scope TEXT NOT NULL DEFAULT 'full',
          target_id TEXT,
          trigger_reason TEXT,
          payload_json TEXT,
          created_at TEXT NOT NULL,
          started_at TEXT,
          finished_at TEXT,
          last_job_id TEXT,
          result_summary_json TEXT
        );

        CREATE INDEX IF NOT EXISTS idx_sync_replay_requests_status
        ON sync_replay_requests (status, created_at ASC, id ASC);
        """,
    ),
    (
        11,
        "add scheduled user lifecycle queue",
        """
        CREATE TABLE IF NOT EXISTS user_lifecycle_queue (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          lifecycle_type TEXT NOT NULL,
          connector_id TEXT NOT NULL DEFAULT 'default',
          wecom_userid TEXT NOT NULL,
          ad_username TEXT,
          status TEXT NOT NULL DEFAULT 'pending',
          reason TEXT,
          employment_type TEXT,
          sponsor_userid TEXT,
          manager_userids_json TEXT,
          effective_at TEXT NOT NULL,
          notified_at TEXT,
          completed_at TEXT,
          last_job_id TEXT,
          payload_json TEXT,
          updated_at TEXT NOT NULL
        );

        CREATE UNIQUE INDEX IF NOT EXISTS idx_user_lifecycle_queue_unique
        ON user_lifecycle_queue (lifecycle_type, connector_id, wecom_userid);

        CREATE INDEX IF NOT EXISTS idx_user_lifecycle_queue_pending
        ON user_lifecycle_queue (status, lifecycle_type, effective_at ASC, connector_id, id ASC);
        """,
    ),
    (
        12,
        "add organization scope for jobs and connectors",
        """
        CREATE TABLE IF NOT EXISTS organizations (
          org_id TEXT PRIMARY KEY,
          name TEXT NOT NULL,
          config_path TEXT NOT NULL,
          description TEXT,
          is_enabled INTEGER NOT NULL DEFAULT 1,
          is_default INTEGER NOT NULL DEFAULT 0,
          created_at TEXT NOT NULL,
          updated_at TEXT NOT NULL
        );

        CREATE UNIQUE INDEX IF NOT EXISTS idx_organizations_default
        ON organizations (is_default)
        WHERE is_default = 1;

        CREATE INDEX IF NOT EXISTS idx_organizations_enabled
        ON organizations (is_enabled, name ASC, org_id ASC);

        ALTER TABLE sync_jobs ADD COLUMN org_id TEXT NOT NULL DEFAULT 'default';
        CREATE INDEX IF NOT EXISTS idx_sync_jobs_org_started_at
        ON sync_jobs (org_id, started_at DESC);

        ALTER TABLE sync_connectors ADD COLUMN org_id TEXT NOT NULL DEFAULT 'default';
        CREATE INDEX IF NOT EXISTS idx_sync_connectors_org_enabled
        ON sync_connectors (org_id, is_enabled, updated_at DESC);

        INSERT OR IGNORE INTO organizations (
          org_id, name, config_path, description, is_enabled, is_default, created_at, updated_at
        ) VALUES (
          'default', 'Default Organization', 'config.ini', '', 1, 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
        );
        """,
    ),
    (
        13,
        "scope bindings, state, exceptions, lifecycle, and replay tables by organization",
        """
        ALTER TABLE object_sync_state RENAME TO object_sync_state_v12;
        CREATE TABLE object_sync_state (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          org_id TEXT NOT NULL DEFAULT 'default',
          source_type TEXT NOT NULL,
          object_type TEXT NOT NULL,
          source_id TEXT NOT NULL,
          source_hash TEXT NOT NULL,
          display_name TEXT,
          target_dn TEXT,
          last_seen_at TEXT NOT NULL,
          last_job_id TEXT,
          last_action TEXT,
          last_status TEXT,
          extra_json TEXT
        );
        INSERT INTO object_sync_state (
          id, org_id, source_type, object_type, source_id, source_hash, display_name,
          target_dn, last_seen_at, last_job_id, last_action, last_status, extra_json
        )
        SELECT
          id, 'default', source_type, object_type, source_id, source_hash, display_name,
          target_dn, last_seen_at, last_job_id, last_action, last_status, extra_json
        FROM object_sync_state_v12;
        DROP TABLE object_sync_state_v12;
        CREATE UNIQUE INDEX idx_object_sync_state_unique
        ON object_sync_state (org_id, source_type, object_type, source_id);
        CREATE INDEX idx_object_sync_state_object
        ON object_sync_state (org_id, object_type, source_id);

        ALTER TABLE managed_group_bindings RENAME TO managed_group_bindings_v12;
        CREATE TABLE managed_group_bindings (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          org_id TEXT NOT NULL DEFAULT 'default',
          department_id TEXT NOT NULL,
          parent_department_id TEXT,
          group_sam TEXT NOT NULL,
          group_dn TEXT,
          group_cn TEXT,
          display_name TEXT,
          path_text TEXT,
          status TEXT NOT NULL DEFAULT 'active',
          updated_at TEXT NOT NULL
        );
        INSERT INTO managed_group_bindings (
          id, org_id, department_id, parent_department_id, group_sam, group_dn, group_cn,
          display_name, path_text, status, updated_at
        )
        SELECT
          id, 'default', department_id, parent_department_id, group_sam, group_dn, group_cn,
          display_name, path_text, status, updated_at
        FROM managed_group_bindings_v12;
        DROP TABLE managed_group_bindings_v12;
        CREATE UNIQUE INDEX idx_managed_group_bindings_department
        ON managed_group_bindings (org_id, department_id);
        CREATE UNIQUE INDEX idx_managed_group_bindings_group_sam
        ON managed_group_bindings (org_id, group_sam);
        CREATE INDEX idx_managed_group_bindings_parent
        ON managed_group_bindings (org_id, parent_department_id);

        ALTER TABLE user_identity_bindings RENAME TO user_identity_bindings_v12;
        CREATE TABLE user_identity_bindings (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          org_id TEXT NOT NULL DEFAULT 'default',
          wecom_userid TEXT NOT NULL,
          connector_id TEXT NOT NULL DEFAULT 'default',
          ad_username TEXT NOT NULL,
          source TEXT NOT NULL DEFAULT 'derived_default',
          notes TEXT,
          is_enabled INTEGER NOT NULL DEFAULT 1,
          updated_at TEXT NOT NULL
        );
        INSERT INTO user_identity_bindings (
          id, org_id, wecom_userid, connector_id, ad_username, source, notes, is_enabled, updated_at
        )
        SELECT
          id, 'default', wecom_userid, connector_id, ad_username, source, notes, is_enabled, updated_at
        FROM user_identity_bindings_v12;
        DROP TABLE user_identity_bindings_v12;
        CREATE UNIQUE INDEX idx_user_identity_bindings_userid
        ON user_identity_bindings (org_id, wecom_userid);
        CREATE UNIQUE INDEX idx_user_identity_bindings_connector_username
        ON user_identity_bindings (org_id, connector_id, ad_username);
        CREATE INDEX idx_user_identity_bindings_ad_username
        ON user_identity_bindings (org_id, connector_id, ad_username);

        ALTER TABLE user_department_overrides RENAME TO user_department_overrides_v12;
        CREATE TABLE user_department_overrides (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          org_id TEXT NOT NULL DEFAULT 'default',
          wecom_userid TEXT NOT NULL,
          primary_department_id TEXT NOT NULL,
          notes TEXT,
          updated_at TEXT NOT NULL
        );
        INSERT INTO user_department_overrides (
          id, org_id, wecom_userid, primary_department_id, notes, updated_at
        )
        SELECT
          id, 'default', wecom_userid, primary_department_id, notes, updated_at
        FROM user_department_overrides_v12;
        DROP TABLE user_department_overrides_v12;
        CREATE UNIQUE INDEX idx_user_department_overrides_userid
        ON user_department_overrides (org_id, wecom_userid);
        CREATE INDEX idx_user_department_overrides_department
        ON user_department_overrides (org_id, primary_department_id);

        ALTER TABLE sync_exception_rules RENAME TO sync_exception_rules_v12;
        CREATE TABLE sync_exception_rules (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          org_id TEXT NOT NULL DEFAULT 'default',
          rule_type TEXT NOT NULL,
          match_type TEXT NOT NULL,
          match_value TEXT NOT NULL,
          notes TEXT,
          is_enabled INTEGER NOT NULL DEFAULT 1,
          created_at TEXT NOT NULL,
          updated_at TEXT NOT NULL,
          expires_at TEXT,
          is_once INTEGER NOT NULL DEFAULT 0,
          last_matched_at TEXT
        );
        INSERT INTO sync_exception_rules (
          id, org_id, rule_type, match_type, match_value, notes, is_enabled, created_at,
          updated_at, expires_at, is_once, last_matched_at
        )
        SELECT
          id, 'default', rule_type, match_type, match_value, notes, is_enabled, created_at,
          updated_at, expires_at, is_once, last_matched_at
        FROM sync_exception_rules_v12;
        DROP TABLE sync_exception_rules_v12;
        CREATE UNIQUE INDEX idx_sync_exception_rules_unique
        ON sync_exception_rules (org_id, rule_type, match_type, match_value);
        CREATE INDEX idx_sync_exception_rules_enabled
        ON sync_exception_rules (org_id, is_enabled, rule_type, updated_at DESC);
        CREATE INDEX idx_sync_exception_rules_expires
        ON sync_exception_rules (org_id, is_enabled, expires_at, rule_type, updated_at DESC);

        ALTER TABLE offboarding_queue RENAME TO offboarding_queue_v12;
        CREATE TABLE offboarding_queue (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          org_id TEXT NOT NULL DEFAULT 'default',
          connector_id TEXT NOT NULL DEFAULT 'default',
          wecom_userid TEXT,
          ad_username TEXT NOT NULL,
          status TEXT NOT NULL DEFAULT 'pending',
          reason TEXT,
          manager_userids_json TEXT,
          first_missing_at TEXT NOT NULL,
          due_at TEXT NOT NULL,
          notified_at TEXT,
          last_job_id TEXT,
          updated_at TEXT NOT NULL
        );
        INSERT INTO offboarding_queue (
          id, org_id, connector_id, wecom_userid, ad_username, status, reason, manager_userids_json,
          first_missing_at, due_at, notified_at, last_job_id, updated_at
        )
        SELECT
          id, 'default', connector_id, wecom_userid, ad_username, status, reason, manager_userids_json,
          first_missing_at, due_at, notified_at, last_job_id, updated_at
        FROM offboarding_queue_v12;
        DROP TABLE offboarding_queue_v12;
        CREATE UNIQUE INDEX idx_offboarding_queue_unique
        ON offboarding_queue (org_id, connector_id, ad_username);
        CREATE INDEX idx_offboarding_queue_status_due
        ON offboarding_queue (org_id, status, due_at, connector_id);

        ALTER TABLE user_lifecycle_queue RENAME TO user_lifecycle_queue_v12;
        CREATE TABLE user_lifecycle_queue (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          org_id TEXT NOT NULL DEFAULT 'default',
          lifecycle_type TEXT NOT NULL,
          connector_id TEXT NOT NULL DEFAULT 'default',
          wecom_userid TEXT NOT NULL,
          ad_username TEXT,
          status TEXT NOT NULL DEFAULT 'pending',
          reason TEXT,
          employment_type TEXT,
          sponsor_userid TEXT,
          manager_userids_json TEXT,
          effective_at TEXT NOT NULL,
          notified_at TEXT,
          completed_at TEXT,
          last_job_id TEXT,
          payload_json TEXT,
          updated_at TEXT NOT NULL
        );
        INSERT INTO user_lifecycle_queue (
          id, org_id, lifecycle_type, connector_id, wecom_userid, ad_username, status, reason,
          employment_type, sponsor_userid, manager_userids_json, effective_at, notified_at,
          completed_at, last_job_id, payload_json, updated_at
        )
        SELECT
          id, 'default', lifecycle_type, connector_id, wecom_userid, ad_username, status, reason,
          employment_type, sponsor_userid, manager_userids_json, effective_at, notified_at,
          completed_at, last_job_id, payload_json, updated_at
        FROM user_lifecycle_queue_v12;
        DROP TABLE user_lifecycle_queue_v12;
        CREATE UNIQUE INDEX idx_user_lifecycle_queue_unique
        ON user_lifecycle_queue (org_id, lifecycle_type, connector_id, wecom_userid);
        CREATE INDEX idx_user_lifecycle_queue_pending
        ON user_lifecycle_queue (org_id, status, lifecycle_type, effective_at ASC, connector_id, id ASC);

        ALTER TABLE custom_managed_group_bindings RENAME TO custom_managed_group_bindings_v12;
        CREATE TABLE custom_managed_group_bindings (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          org_id TEXT NOT NULL DEFAULT 'default',
          connector_id TEXT NOT NULL DEFAULT 'default',
          source_type TEXT NOT NULL,
          source_key TEXT NOT NULL,
          group_sam TEXT NOT NULL,
          group_dn TEXT,
          group_cn TEXT,
          display_name TEXT,
          status TEXT NOT NULL DEFAULT 'active',
          last_seen_at TEXT,
          archived_at TEXT,
          updated_at TEXT NOT NULL
        );
        INSERT INTO custom_managed_group_bindings (
          id, org_id, connector_id, source_type, source_key, group_sam, group_dn, group_cn,
          display_name, status, last_seen_at, archived_at, updated_at
        )
        SELECT
          id, 'default', connector_id, source_type, source_key, group_sam, group_dn, group_cn,
          display_name, status, last_seen_at, archived_at, updated_at
        FROM custom_managed_group_bindings_v12;
        DROP TABLE custom_managed_group_bindings_v12;
        CREATE UNIQUE INDEX idx_custom_managed_group_bindings_source
        ON custom_managed_group_bindings (org_id, connector_id, source_type, source_key);
        CREATE UNIQUE INDEX idx_custom_managed_group_bindings_group
        ON custom_managed_group_bindings (org_id, connector_id, group_sam);
        CREATE INDEX idx_custom_managed_group_bindings_status
        ON custom_managed_group_bindings (org_id, status, connector_id, updated_at DESC);

        ALTER TABLE sync_replay_requests RENAME TO sync_replay_requests_v12;
        CREATE TABLE sync_replay_requests (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          org_id TEXT NOT NULL DEFAULT 'default',
          request_type TEXT NOT NULL,
          execution_mode TEXT NOT NULL,
          status TEXT NOT NULL DEFAULT 'pending',
          requested_by TEXT,
          target_scope TEXT NOT NULL DEFAULT 'full',
          target_id TEXT,
          trigger_reason TEXT,
          payload_json TEXT,
          created_at TEXT NOT NULL,
          started_at TEXT,
          finished_at TEXT,
          last_job_id TEXT,
          result_summary_json TEXT
        );
        INSERT INTO sync_replay_requests (
          id, org_id, request_type, execution_mode, status, requested_by, target_scope, target_id,
          trigger_reason, payload_json, created_at, started_at, finished_at, last_job_id, result_summary_json
        )
        SELECT
          id, 'default', request_type, execution_mode, status, requested_by, target_scope, target_id,
          trigger_reason, payload_json, created_at, started_at, finished_at, last_job_id, result_summary_json
        FROM sync_replay_requests_v12;
        DROP TABLE sync_replay_requests_v12;
        CREATE INDEX idx_sync_replay_requests_status
        ON sync_replay_requests (org_id, status, created_at ASC, id ASC);
        """,
    ),
    (
        14,
        "scope attribute mapping rules by organization",
        """
        ALTER TABLE attribute_mapping_rules RENAME TO attribute_mapping_rules_v13;
        CREATE TABLE attribute_mapping_rules (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          org_id TEXT NOT NULL DEFAULT 'default',
          connector_id TEXT NOT NULL DEFAULT '',
          direction TEXT NOT NULL,
          source_field TEXT NOT NULL,
          target_field TEXT NOT NULL,
          transform_template TEXT,
          sync_mode TEXT NOT NULL DEFAULT 'replace',
          is_enabled INTEGER NOT NULL DEFAULT 1,
          notes TEXT,
          created_at TEXT NOT NULL,
          updated_at TEXT NOT NULL
        );
        INSERT INTO attribute_mapping_rules (
          id, org_id, connector_id, direction, source_field, target_field,
          transform_template, sync_mode, is_enabled, notes, created_at, updated_at
        )
        SELECT
          id, 'default', connector_id, direction, source_field, target_field,
          transform_template, sync_mode, is_enabled, notes, created_at, updated_at
        FROM attribute_mapping_rules_v13;
        DROP TABLE attribute_mapping_rules_v13;
        CREATE UNIQUE INDEX idx_attribute_mapping_rules_unique
        ON attribute_mapping_rules (org_id, connector_id, direction, source_field, target_field);
        CREATE INDEX idx_attribute_mapping_rules_direction
        ON attribute_mapping_rules (org_id, direction, connector_id, is_enabled, updated_at DESC);
        """,
    ),
    (
        15,
        "scope group exclusion rules by organization",
        """
        ALTER TABLE group_exclusion_rules RENAME TO group_exclusion_rules_v14;
        CREATE TABLE group_exclusion_rules (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          org_id TEXT NOT NULL DEFAULT 'default',
          rule_type TEXT NOT NULL,
          protection_level TEXT NOT NULL,
          match_type TEXT NOT NULL,
          match_value TEXT NOT NULL,
          display_name TEXT,
          is_enabled INTEGER NOT NULL DEFAULT 1,
          source TEXT NOT NULL,
          created_at TEXT NOT NULL,
          updated_at TEXT NOT NULL
        );
        INSERT INTO group_exclusion_rules (
          id, org_id, rule_type, protection_level, match_type, match_value,
          display_name, is_enabled, source, created_at, updated_at
        )
        SELECT
          id, 'default', rule_type, protection_level, match_type, match_value,
          display_name, is_enabled, source, created_at, updated_at
        FROM group_exclusion_rules_v14;
        DROP TABLE group_exclusion_rules_v14;
        CREATE UNIQUE INDEX idx_group_exclusion_rules_unique
        ON group_exclusion_rules (org_id, rule_type, protection_level, match_type, match_value);
        CREATE INDEX idx_group_exclusion_rules_enabled
        ON group_exclusion_rules (org_id, is_enabled, protection_level, source, display_name);
        """,
    ),
    (
        16,
        "scope web audit logs by organization",
        """
        ALTER TABLE web_audit_logs RENAME TO web_audit_logs_v15;

        CREATE TABLE web_audit_logs (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          org_id TEXT NOT NULL DEFAULT '',
          actor_username TEXT,
          action_type TEXT NOT NULL,
          target_type TEXT,
          target_id TEXT,
          result TEXT NOT NULL,
          message TEXT NOT NULL,
          payload_json TEXT,
          created_at TEXT NOT NULL
        );

        INSERT INTO web_audit_logs (
          id, org_id, actor_username, action_type, target_type, target_id,
          result, message, payload_json, created_at
        )
        SELECT
          id, '', actor_username, action_type, target_type, target_id,
          result, message, payload_json, created_at
        FROM web_audit_logs_v15;

        DROP TABLE web_audit_logs_v15;

        CREATE INDEX idx_web_audit_logs_created_at
        ON web_audit_logs (created_at DESC);

        CREATE INDEX idx_web_audit_logs_action
        ON web_audit_logs (action_type, created_at DESC);

        CREATE INDEX idx_web_audit_logs_created_id
        ON web_audit_logs (created_at DESC, id DESC);

        CREATE INDEX idx_web_audit_logs_org_created_id
        ON web_audit_logs (org_id, created_at DESC, id DESC);
        """,
    ),
    (
        17,
        "store connector LDAP and account overrides in database",
        """
        ALTER TABLE sync_connectors ADD COLUMN ldap_server TEXT;
        ALTER TABLE sync_connectors ADD COLUMN ldap_domain TEXT;
        ALTER TABLE sync_connectors ADD COLUMN ldap_username TEXT;
        ALTER TABLE sync_connectors ADD COLUMN ldap_password TEXT;
        ALTER TABLE sync_connectors ADD COLUMN ldap_use_ssl INTEGER;
        ALTER TABLE sync_connectors ADD COLUMN ldap_port INTEGER;
        ALTER TABLE sync_connectors ADD COLUMN ldap_validate_cert INTEGER;
        ALTER TABLE sync_connectors ADD COLUMN ldap_ca_cert_path TEXT;
        ALTER TABLE sync_connectors ADD COLUMN default_password TEXT;
        ALTER TABLE sync_connectors ADD COLUMN force_change_password INTEGER;
        ALTER TABLE sync_connectors ADD COLUMN password_complexity TEXT;
        """,
    ),
    (
        18,
        "normalize attribute mapping direction values",
        """
        DELETE FROM attribute_mapping_rules
        WHERE direction = 'wecom_to_ad'
          AND EXISTS (
            SELECT 1
            FROM attribute_mapping_rules AS newer
            WHERE newer.org_id = attribute_mapping_rules.org_id
              AND newer.connector_id = attribute_mapping_rules.connector_id
              AND newer.direction = 'source_to_ad'
              AND newer.source_field = attribute_mapping_rules.source_field
              AND newer.target_field = attribute_mapping_rules.target_field
          );

        DELETE FROM attribute_mapping_rules
        WHERE direction = 'ad_to_wecom'
          AND EXISTS (
            SELECT 1
            FROM attribute_mapping_rules AS newer
            WHERE newer.org_id = attribute_mapping_rules.org_id
              AND newer.connector_id = attribute_mapping_rules.connector_id
              AND newer.direction = 'ad_to_source'
              AND newer.source_field = attribute_mapping_rules.source_field
              AND newer.target_field = attribute_mapping_rules.target_field
          );

        UPDATE attribute_mapping_rules
        SET direction = 'source_to_ad'
        WHERE direction = 'wecom_to_ad';

        UPDATE attribute_mapping_rules
        SET direction = 'ad_to_source'
        WHERE direction = 'ad_to_wecom';
        """,
    ),
    (
        19,
        "rename managed source user columns to source_user_id",
        """
        ALTER TABLE user_identity_bindings RENAME TO user_identity_bindings_v18_source;
        CREATE TABLE user_identity_bindings (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          org_id TEXT NOT NULL DEFAULT 'default',
          source_user_id TEXT NOT NULL,
          connector_id TEXT NOT NULL DEFAULT 'default',
          ad_username TEXT NOT NULL,
          source TEXT NOT NULL DEFAULT 'derived_default',
          notes TEXT,
          is_enabled INTEGER NOT NULL DEFAULT 1,
          updated_at TEXT NOT NULL
        );
        INSERT INTO user_identity_bindings (
          id, org_id, source_user_id, connector_id, ad_username, source, notes, is_enabled, updated_at
        )
        SELECT
          id, org_id, wecom_userid, connector_id, ad_username, source, notes, is_enabled, updated_at
        FROM user_identity_bindings_v18_source;
        DROP TABLE user_identity_bindings_v18_source;
        CREATE UNIQUE INDEX idx_user_identity_bindings_userid
        ON user_identity_bindings (org_id, source_user_id);
        CREATE UNIQUE INDEX idx_user_identity_bindings_connector_username
        ON user_identity_bindings (org_id, connector_id, ad_username);
        CREATE INDEX idx_user_identity_bindings_ad_username
        ON user_identity_bindings (org_id, connector_id, ad_username);

        ALTER TABLE user_department_overrides RENAME TO user_department_overrides_v18_source;
        CREATE TABLE user_department_overrides (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          org_id TEXT NOT NULL DEFAULT 'default',
          source_user_id TEXT NOT NULL,
          primary_department_id TEXT NOT NULL,
          notes TEXT,
          updated_at TEXT NOT NULL
        );
        INSERT INTO user_department_overrides (
          id, org_id, source_user_id, primary_department_id, notes, updated_at
        )
        SELECT
          id, org_id, wecom_userid, primary_department_id, notes, updated_at
        FROM user_department_overrides_v18_source;
        DROP TABLE user_department_overrides_v18_source;
        CREATE UNIQUE INDEX idx_user_department_overrides_userid
        ON user_department_overrides (org_id, source_user_id);
        CREATE INDEX idx_user_department_overrides_department
        ON user_department_overrides (org_id, primary_department_id);

        ALTER TABLE offboarding_queue RENAME TO offboarding_queue_v18_source;
        CREATE TABLE offboarding_queue (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          org_id TEXT NOT NULL DEFAULT 'default',
          connector_id TEXT NOT NULL DEFAULT 'default',
          source_user_id TEXT,
          ad_username TEXT NOT NULL,
          status TEXT NOT NULL DEFAULT 'pending',
          reason TEXT,
          manager_userids_json TEXT,
          first_missing_at TEXT NOT NULL,
          due_at TEXT NOT NULL,
          notified_at TEXT,
          last_job_id TEXT,
          updated_at TEXT NOT NULL
        );
        INSERT INTO offboarding_queue (
          id, org_id, connector_id, source_user_id, ad_username, status, reason, manager_userids_json,
          first_missing_at, due_at, notified_at, last_job_id, updated_at
        )
        SELECT
          id, org_id, connector_id, wecom_userid, ad_username, status, reason, manager_userids_json,
          first_missing_at, due_at, notified_at, last_job_id, updated_at
        FROM offboarding_queue_v18_source;
        DROP TABLE offboarding_queue_v18_source;
        CREATE UNIQUE INDEX idx_offboarding_queue_unique
        ON offboarding_queue (org_id, connector_id, ad_username);
        CREATE INDEX idx_offboarding_queue_status_due
        ON offboarding_queue (org_id, status, due_at, connector_id);

        ALTER TABLE user_lifecycle_queue RENAME TO user_lifecycle_queue_v18_source;
        CREATE TABLE user_lifecycle_queue (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          org_id TEXT NOT NULL DEFAULT 'default',
          lifecycle_type TEXT NOT NULL,
          connector_id TEXT NOT NULL DEFAULT 'default',
          source_user_id TEXT NOT NULL,
          ad_username TEXT,
          status TEXT NOT NULL DEFAULT 'pending',
          reason TEXT,
          employment_type TEXT,
          sponsor_userid TEXT,
          manager_userids_json TEXT,
          effective_at TEXT NOT NULL,
          notified_at TEXT,
          completed_at TEXT,
          last_job_id TEXT,
          payload_json TEXT,
          updated_at TEXT NOT NULL
        );
        INSERT INTO user_lifecycle_queue (
          id, org_id, lifecycle_type, connector_id, source_user_id, ad_username, status, reason,
          employment_type, sponsor_userid, manager_userids_json, effective_at, notified_at,
          completed_at, last_job_id, payload_json, updated_at
        )
        SELECT
          id, org_id, lifecycle_type, connector_id, wecom_userid, ad_username, status, reason,
          employment_type, sponsor_userid, manager_userids_json, effective_at, notified_at,
          completed_at, last_job_id, payload_json, updated_at
        FROM user_lifecycle_queue_v18_source;
        DROP TABLE user_lifecycle_queue_v18_source;
        CREATE UNIQUE INDEX idx_user_lifecycle_queue_unique
        ON user_lifecycle_queue (org_id, lifecycle_type, connector_id, source_user_id);
        CREATE INDEX idx_user_lifecycle_queue_pending
        ON user_lifecycle_queue (org_id, status, lifecycle_type, effective_at ASC, connector_id, id ASC);
        """,
    ),
    (
        20,
        "add username strategy, collision template, binding anchors, and department ou mapping table",
        """
        ALTER TABLE sync_connectors ADD COLUMN username_strategy TEXT NOT NULL DEFAULT 'custom_template';
        ALTER TABLE sync_connectors ADD COLUMN username_collision_policy TEXT NOT NULL DEFAULT 'append_employee_id';
        ALTER TABLE sync_connectors ADD COLUMN username_collision_template TEXT NOT NULL DEFAULT '';

        CREATE TABLE IF NOT EXISTS department_ou_mappings (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          org_id TEXT NOT NULL DEFAULT 'default',
          connector_id TEXT NOT NULL DEFAULT '',
          source_department_id TEXT NOT NULL,
          source_department_name TEXT,
          target_ou_path TEXT NOT NULL,
          apply_mode TEXT NOT NULL DEFAULT 'subtree',
          notes TEXT,
          is_enabled INTEGER NOT NULL DEFAULT 1,
          created_at TEXT NOT NULL,
          updated_at TEXT NOT NULL
        );

        CREATE UNIQUE INDEX IF NOT EXISTS idx_department_ou_mappings_unique
        ON department_ou_mappings (org_id, connector_id, source_department_id);

        CREATE INDEX IF NOT EXISTS idx_department_ou_mappings_lookup
        ON department_ou_mappings (org_id, connector_id, is_enabled, source_department_id);

        ALTER TABLE user_identity_bindings ADD COLUMN source_display_name TEXT NOT NULL DEFAULT '';
        ALTER TABLE user_identity_bindings ADD COLUMN target_object_guid TEXT NOT NULL DEFAULT '';
        ALTER TABLE user_identity_bindings ADD COLUMN target_object_dn TEXT NOT NULL DEFAULT '';
        ALTER TABLE user_identity_bindings ADD COLUMN managed_username_base TEXT NOT NULL DEFAULT '';

        CREATE INDEX IF NOT EXISTS idx_user_identity_bindings_target_guid
        ON user_identity_bindings (org_id, connector_id, target_object_guid);
        """,
    ),
    (
        21,
        "reserved compatibility slot after migration 20 squash",
        """
        SELECT 1;
        """,
    ),
    (
        22,
        "add persisted dispatch metadata for queued and leased sync jobs",
        """
        ALTER TABLE sync_jobs ADD COLUMN requested_by TEXT NOT NULL DEFAULT '';
        ALTER TABLE sync_jobs ADD COLUMN requested_config_path TEXT NOT NULL DEFAULT '';
        ALTER TABLE sync_jobs ADD COLUMN lease_owner TEXT NOT NULL DEFAULT '';
        ALTER TABLE sync_jobs ADD COLUMN lease_expires_at TEXT NOT NULL DEFAULT '';

        CREATE INDEX IF NOT EXISTS idx_sync_jobs_org_status_started_at
        ON sync_jobs (org_id, status, started_at DESC);

        CREATE INDEX IF NOT EXISTS idx_sync_jobs_lease_expires_at
        ON sync_jobs (lease_expires_at);
        """,
    ),
    (
        23,
        "add governance lifecycle metadata for bindings, overrides, and exception rules",
        """
        ALTER TABLE user_identity_bindings ADD COLUMN rule_owner TEXT NOT NULL DEFAULT '';
        ALTER TABLE user_identity_bindings ADD COLUMN effective_reason TEXT NOT NULL DEFAULT '';
        ALTER TABLE user_identity_bindings ADD COLUMN next_review_at TEXT NOT NULL DEFAULT '';
        ALTER TABLE user_identity_bindings ADD COLUMN last_reviewed_at TEXT NOT NULL DEFAULT '';
        ALTER TABLE user_identity_bindings ADD COLUMN hit_count INTEGER NOT NULL DEFAULT 0;
        ALTER TABLE user_identity_bindings ADD COLUMN last_hit_at TEXT NOT NULL DEFAULT '';

        ALTER TABLE user_department_overrides ADD COLUMN rule_owner TEXT NOT NULL DEFAULT '';
        ALTER TABLE user_department_overrides ADD COLUMN effective_reason TEXT NOT NULL DEFAULT '';
        ALTER TABLE user_department_overrides ADD COLUMN next_review_at TEXT NOT NULL DEFAULT '';
        ALTER TABLE user_department_overrides ADD COLUMN last_reviewed_at TEXT NOT NULL DEFAULT '';
        ALTER TABLE user_department_overrides ADD COLUMN hit_count INTEGER NOT NULL DEFAULT 0;
        ALTER TABLE user_department_overrides ADD COLUMN last_hit_at TEXT NOT NULL DEFAULT '';

        ALTER TABLE sync_exception_rules ADD COLUMN rule_owner TEXT NOT NULL DEFAULT '';
        ALTER TABLE sync_exception_rules ADD COLUMN effective_reason TEXT NOT NULL DEFAULT '';
        ALTER TABLE sync_exception_rules ADD COLUMN next_review_at TEXT NOT NULL DEFAULT '';
        ALTER TABLE sync_exception_rules ADD COLUMN last_reviewed_at TEXT NOT NULL DEFAULT '';
        ALTER TABLE sync_exception_rules ADD COLUMN hit_count INTEGER NOT NULL DEFAULT 0;
        ALTER TABLE sync_exception_rules ADD COLUMN last_hit_at TEXT NOT NULL DEFAULT '';

        CREATE INDEX IF NOT EXISTS idx_user_identity_bindings_review
        ON user_identity_bindings (org_id, next_review_at, last_reviewed_at);

        CREATE INDEX IF NOT EXISTS idx_user_department_overrides_review
        ON user_department_overrides (org_id, next_review_at, last_reviewed_at);

        CREATE INDEX IF NOT EXISTS idx_sync_exception_rules_review
        ON sync_exception_rules (org_id, is_enabled, next_review_at, last_reviewed_at);
        """,
    ),
    (
        24,
        "add configuration release snapshot history",
        """
        CREATE TABLE IF NOT EXISTS config_release_snapshots (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          org_id TEXT NOT NULL DEFAULT 'default',
          snapshot_name TEXT NOT NULL DEFAULT '',
          trigger_action TEXT NOT NULL DEFAULT 'manual_release',
          created_by TEXT NOT NULL DEFAULT '',
          source_snapshot_id INTEGER,
          bundle_hash TEXT NOT NULL DEFAULT '',
          bundle_json TEXT NOT NULL,
          summary_json TEXT,
          created_at TEXT NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_config_release_snapshots_org_created
        ON config_release_snapshots (org_id, created_at DESC, id DESC);

        CREATE INDEX IF NOT EXISTS idx_config_release_snapshots_org_hash
        ON config_release_snapshots (org_id, bundle_hash);
        """,
    ),
    (
        25,
        "add data quality snapshot history",
        """
        CREATE TABLE IF NOT EXISTS data_quality_snapshots (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          org_id TEXT NOT NULL DEFAULT 'default',
          trigger_action TEXT NOT NULL DEFAULT 'manual_scan',
          created_by TEXT NOT NULL DEFAULT '',
          summary_json TEXT,
          snapshot_json TEXT NOT NULL,
          created_at TEXT NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_data_quality_snapshots_org_created
        ON data_quality_snapshots (org_id, created_at DESC, id DESC);
        """,
    ),
    (
        26,
        "add external integration webhook subscriptions",
        """
        CREATE TABLE IF NOT EXISTS integration_webhook_subscriptions (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          org_id TEXT NOT NULL DEFAULT 'default',
          event_type TEXT NOT NULL,
          target_url TEXT NOT NULL,
          secret TEXT NOT NULL DEFAULT '',
          description TEXT NOT NULL DEFAULT '',
          is_enabled INTEGER NOT NULL DEFAULT 1,
          last_attempt_at TEXT NOT NULL DEFAULT '',
          last_status TEXT NOT NULL DEFAULT '',
          last_error TEXT NOT NULL DEFAULT '',
          created_at TEXT NOT NULL,
          updated_at TEXT NOT NULL
        );

        CREATE UNIQUE INDEX IF NOT EXISTS idx_integration_webhook_subscriptions_unique
        ON integration_webhook_subscriptions (org_id, event_type, target_url);

        CREATE INDEX IF NOT EXISTS idx_integration_webhook_subscriptions_lookup
        ON integration_webhook_subscriptions (org_id, event_type, is_enabled, updated_at DESC, id DESC);
        """,
    ),
]
