from __future__ import annotations

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
    (
        27,
        "add integration webhook outbox",
        """
        CREATE TABLE IF NOT EXISTS integration_webhook_outbox (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          org_id TEXT NOT NULL DEFAULT 'default',
          subscription_id INTEGER,
          event_type TEXT NOT NULL,
          delivery_id TEXT NOT NULL,
          target_url TEXT NOT NULL,
          secret TEXT NOT NULL DEFAULT '',
          payload_json TEXT NOT NULL,
          status TEXT NOT NULL DEFAULT 'pending',
          attempt_count INTEGER NOT NULL DEFAULT 0,
          max_attempts INTEGER NOT NULL DEFAULT 5,
          next_attempt_at TEXT NOT NULL DEFAULT '',
          last_attempt_at TEXT NOT NULL DEFAULT '',
          last_status TEXT NOT NULL DEFAULT '',
          last_error TEXT NOT NULL DEFAULT '',
          locked_at TEXT NOT NULL DEFAULT '',
          lease_expires_at TEXT NOT NULL DEFAULT '',
          created_at TEXT NOT NULL,
          updated_at TEXT NOT NULL,
          FOREIGN KEY(subscription_id) REFERENCES integration_webhook_subscriptions(id)
        );

        CREATE INDEX IF NOT EXISTS idx_integration_webhook_outbox_ready
        ON integration_webhook_outbox (
          org_id, status, next_attempt_at, lease_expires_at, created_at ASC, id ASC
        );

        CREATE INDEX IF NOT EXISTS idx_integration_webhook_outbox_subscription
        ON integration_webhook_outbox (subscription_id, created_at DESC, id DESC);
        """,
    ),
    (
        28,
        "persist sync runtime phase and recovery metadata",
        """
        ALTER TABLE sync_jobs ADD COLUMN current_phase TEXT NOT NULL DEFAULT '';
        ALTER TABLE sync_jobs ADD COLUMN last_completed_phase TEXT NOT NULL DEFAULT '';
        ALTER TABLE sync_jobs ADD COLUMN phase_started_at TEXT NOT NULL DEFAULT '';
        ALTER TABLE sync_jobs ADD COLUMN phase_updated_at TEXT NOT NULL DEFAULT '';
        ALTER TABLE sync_jobs ADD COLUMN recovery_hint TEXT NOT NULL DEFAULT '';

        CREATE INDEX IF NOT EXISTS idx_sync_jobs_recovery_phase
        ON sync_jobs (status, current_phase, phase_updated_at);
        """,
    ),
    (
        29,
        "harden integration outbox idempotency fencing and dead-letter metadata",
        """
        ALTER TABLE integration_webhook_outbox ADD COLUMN idempotency_key TEXT NOT NULL DEFAULT '';
        ALTER TABLE integration_webhook_outbox ADD COLUMN lease_token TEXT NOT NULL DEFAULT '';
        ALTER TABLE integration_webhook_outbox ADD COLUMN dead_lettered_at TEXT NOT NULL DEFAULT '';
        ALTER TABLE integration_webhook_outbox ADD COLUMN replay_count INTEGER NOT NULL DEFAULT 0;
        ALTER TABLE integration_webhook_outbox ADD COLUMN last_replayed_at TEXT NOT NULL DEFAULT '';
        ALTER TABLE integration_webhook_outbox ADD COLUMN last_replayed_by TEXT NOT NULL DEFAULT '';

        CREATE UNIQUE INDEX IF NOT EXISTS idx_integration_webhook_outbox_idempotency
        ON integration_webhook_outbox (idempotency_key)
        WHERE idempotency_key <> '';

        CREATE INDEX IF NOT EXISTS idx_integration_webhook_outbox_dead_letter
        ON integration_webhook_outbox (org_id, dead_lettered_at, updated_at DESC, id DESC);
        """,
    ),
    (
        30,
        "record immutable migration checksums",
        """
        ALTER TABLE schema_migrations ADD COLUMN checksum TEXT NOT NULL DEFAULT '';
        """,
    ),
    (
        31,
        "add source directory snapshots, field catalogs, sync scopes, and provider-aware identities",
        """
        ALTER TABLE user_identity_bindings ADD COLUMN source_provider TEXT NOT NULL DEFAULT 'wecom';
        DROP INDEX IF EXISTS idx_user_identity_bindings_userid;
        CREATE UNIQUE INDEX idx_user_identity_bindings_source_identity
        ON user_identity_bindings (org_id, source_provider, source_user_id);

        CREATE TABLE source_directory_snapshots (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          org_id TEXT NOT NULL,
          provider_id TEXT NOT NULL,
          connector_id TEXT NOT NULL DEFAULT 'default',
          status TEXT NOT NULL DEFAULT 'refreshing',
          started_at TEXT NOT NULL,
          completed_at TEXT NOT NULL DEFAULT '',
          last_success_at TEXT NOT NULL DEFAULT '',
          expires_at TEXT NOT NULL DEFAULT '',
          error_summary TEXT NOT NULL DEFAULT '',
          warning_summary TEXT NOT NULL DEFAULT '',
          department_count INTEGER NOT NULL DEFAULT 0,
          user_count INTEGER NOT NULL DEFAULT 0,
          field_count INTEGER NOT NULL DEFAULT 0,
          missing_employee_id_count INTEGER NOT NULL DEFAULT 0,
          duplicate_employee_id_count INTEGER NOT NULL DEFAULT 0,
          snapshot_fingerprint TEXT NOT NULL DEFAULT '',
          created_by TEXT NOT NULL DEFAULT '',
          metadata_json TEXT
        );
        CREATE INDEX idx_source_directory_snapshots_latest
        ON source_directory_snapshots (org_id, provider_id, connector_id, status, completed_at DESC, id DESC);

        CREATE TABLE source_department_snapshots (
          snapshot_id INTEGER NOT NULL,
          org_id TEXT NOT NULL,
          provider_id TEXT NOT NULL,
          source_department_id TEXT NOT NULL,
          name TEXT NOT NULL DEFAULT '',
          parent_department_id TEXT NOT NULL DEFAULT '',
          path_names_json TEXT NOT NULL DEFAULT '[]',
          path_ids_json TEXT NOT NULL DEFAULT '[]',
          PRIMARY KEY (snapshot_id, source_department_id),
          FOREIGN KEY(snapshot_id) REFERENCES source_directory_snapshots(id) ON DELETE CASCADE
        );
        CREATE INDEX idx_source_department_snapshots_lookup
        ON source_department_snapshots (org_id, provider_id, snapshot_id, parent_department_id);

        CREATE TABLE source_user_snapshots (
          snapshot_id INTEGER NOT NULL,
          org_id TEXT NOT NULL,
          provider_id TEXT NOT NULL,
          source_user_id TEXT NOT NULL,
          display_name TEXT NOT NULL DEFAULT '',
          employee_id TEXT NOT NULL DEFAULT '',
          email TEXT NOT NULL DEFAULT '',
          mobile_masked TEXT NOT NULL DEFAULT '',
          position TEXT NOT NULL DEFAULT '',
          department_ids_json TEXT NOT NULL DEFAULT '[]',
          department_names_json TEXT NOT NULL DEFAULT '[]',
          primary_department_id TEXT NOT NULL DEFAULT '',
          account_status TEXT NOT NULL DEFAULT 'active',
          is_active INTEGER NOT NULL DEFAULT 1,
          raw_payload_json TEXT NOT NULL DEFAULT '{}',
          search_text TEXT NOT NULL DEFAULT '',
          PRIMARY KEY (snapshot_id, source_user_id),
          FOREIGN KEY(snapshot_id) REFERENCES source_directory_snapshots(id) ON DELETE CASCADE
        );
        CREATE INDEX idx_source_user_snapshots_lookup
        ON source_user_snapshots (org_id, provider_id, snapshot_id, is_active, employee_id, source_user_id);
        CREATE INDEX idx_source_user_snapshots_department
        ON source_user_snapshots (snapshot_id, primary_department_id, is_active);

        CREATE TABLE source_field_catalogs (
          snapshot_id INTEGER NOT NULL,
          org_id TEXT NOT NULL,
          provider_id TEXT NOT NULL,
          field_name TEXT NOT NULL,
          field_label TEXT NOT NULL DEFAULT '',
          data_type TEXT NOT NULL DEFAULT 'string',
          coverage_count INTEGER NOT NULL DEFAULT 0,
          sample_values_json TEXT NOT NULL DEFAULT '[]',
          PRIMARY KEY (snapshot_id, field_name),
          FOREIGN KEY(snapshot_id) REFERENCES source_directory_snapshots(id) ON DELETE CASCADE
        );

        CREATE TABLE sync_scope_selections (
          org_id TEXT NOT NULL,
          provider_id TEXT NOT NULL,
          connector_id TEXT NOT NULL DEFAULT 'default',
          scope_type TEXT NOT NULL DEFAULT 'full',
          selected_department_ids_json TEXT NOT NULL DEFAULT '[]',
          selected_source_user_ids_json TEXT NOT NULL DEFAULT '[]',
          username_strategy TEXT NOT NULL DEFAULT 'userid',
          username_template TEXT NOT NULL DEFAULT '',
          source_field TEXT NOT NULL DEFAULT 'source_user_id',
          snapshot_id INTEGER,
          source_snapshot_fingerprint TEXT NOT NULL DEFAULT '',
          selection_fingerprint TEXT NOT NULL DEFAULT '',
          requested_by TEXT NOT NULL DEFAULT '',
          updated_at TEXT NOT NULL,
          PRIMARY KEY (org_id, provider_id, connector_id),
          FOREIGN KEY(snapshot_id) REFERENCES source_directory_snapshots(id)
        );

        CREATE TABLE sync_job_source_scopes (
          job_id TEXT PRIMARY KEY,
          org_id TEXT NOT NULL,
          provider_id TEXT NOT NULL,
          connector_id TEXT NOT NULL DEFAULT 'default',
          execution_mode TEXT NOT NULL,
          scope_type TEXT NOT NULL,
          selected_department_ids_json TEXT NOT NULL DEFAULT '[]',
          selected_source_user_ids_json TEXT NOT NULL DEFAULT '[]',
          requested_by TEXT NOT NULL DEFAULT '',
          config_fingerprint TEXT NOT NULL DEFAULT '',
          source_snapshot_fingerprint TEXT NOT NULL DEFAULT '',
          selection_fingerprint TEXT NOT NULL DEFAULT '',
          snapshot_id INTEGER,
          created_at TEXT NOT NULL,
          FOREIGN KEY(job_id) REFERENCES sync_jobs(job_id) ON DELETE CASCADE,
          FOREIGN KEY(snapshot_id) REFERENCES source_directory_snapshots(id)
        );
        CREATE INDEX idx_sync_job_source_scopes_match
        ON sync_job_source_scopes (org_id, provider_id, connector_id, execution_mode, selection_fingerprint, created_at DESC);
        """,
    ),
    (
        32,
        "persist bounded SSPR authentication sessions and connector-scoped identities",
        """
        DROP INDEX IF EXISTS idx_user_identity_bindings_source_identity;
        CREATE UNIQUE INDEX idx_user_identity_bindings_source_identity
        ON user_identity_bindings (org_id, source_provider, connector_id, source_user_id);

        CREATE TABLE sspr_verification_sessions (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          token_hash TEXT NOT NULL UNIQUE,
          csrf_token_hash TEXT NOT NULL,
          org_id TEXT NOT NULL,
          provider_id TEXT NOT NULL,
          connector_id TEXT NOT NULL DEFAULT '',
          source_user_id TEXT NOT NULL,
          display_name TEXT NOT NULL DEFAULT '',
          issued_at TEXT NOT NULL,
          expires_at TEXT NOT NULL,
          consumed_at TEXT,
          revoked_at TEXT,
          claimed_at TEXT,
          claim_token_hash TEXT,
          request_ip TEXT NOT NULL DEFAULT '',
          user_agent_hash TEXT NOT NULL DEFAULT '',
          created_at TEXT NOT NULL,
          updated_at TEXT NOT NULL
        );
        CREATE INDEX idx_sspr_sessions_identity
        ON sspr_verification_sessions (
          org_id, provider_id, connector_id, source_user_id, expires_at
        );
        CREATE INDEX idx_sspr_sessions_cleanup
        ON sspr_verification_sessions (expires_at, consumed_at, revoked_at);

        CREATE TABLE sspr_oauth_transactions (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          state_hash TEXT NOT NULL UNIQUE,
          org_id TEXT NOT NULL,
          provider_id TEXT NOT NULL,
          connector_id TEXT NOT NULL DEFAULT '',
          corp_id TEXT NOT NULL,
          return_path TEXT NOT NULL DEFAULT '/sspr/account',
          issued_at TEXT NOT NULL,
          expires_at TEXT NOT NULL,
          consumed_at TEXT,
          request_ip TEXT NOT NULL DEFAULT '',
          user_agent_hash TEXT NOT NULL DEFAULT '',
          created_at TEXT NOT NULL,
          updated_at TEXT NOT NULL
        );
        CREATE INDEX idx_sspr_oauth_context
        ON sspr_oauth_transactions (org_id, provider_id, corp_id, expires_at);
        CREATE INDEX idx_sspr_oauth_cleanup
        ON sspr_oauth_transactions (expires_at, consumed_at);

        CREATE TABLE sspr_reset_receipts (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          token_hash TEXT NOT NULL UNIQUE,
          org_id TEXT NOT NULL,
          ad_username TEXT NOT NULL,
          completed_at TEXT NOT NULL,
          expires_at TEXT NOT NULL,
          unlock_requested INTEGER NOT NULL DEFAULT 0,
          unlock_succeeded INTEGER NOT NULL DEFAULT 0,
          consumed_at TEXT,
          created_at TEXT NOT NULL,
          updated_at TEXT NOT NULL
        );
        CREATE INDEX idx_sspr_reset_receipts_cleanup
        ON sspr_reset_receipts (expires_at, consumed_at);

        CREATE TABLE sspr_rate_limit_buckets (
          bucket_hash TEXT PRIMARY KEY,
          attempts INTEGER NOT NULL DEFAULT 0,
          window_started_at TEXT NOT NULL,
          locked_until TEXT,
          updated_at TEXT NOT NULL
        );
        CREATE INDEX idx_sspr_rate_limit_cleanup
        ON sspr_rate_limit_buckets (updated_at, locked_until);
        """,
    ),
    (
        33,
        "add optimistic concurrency revisions to identity bindings",
        """
        ALTER TABLE user_identity_bindings
        ADD COLUMN binding_revision INTEGER NOT NULL DEFAULT 1;
        """,
    ),
    (
        34,
        "add enterprise identity graph, AD snapshots, matching evidence, and takeover staging",
        """
        CREATE TABLE enterprise_identities (
          identity_id TEXT PRIMARY KEY,
          org_id TEXT NOT NULL,
          display_name TEXT NOT NULL DEFAULT '',
          canonical_employee_id TEXT NOT NULL DEFAULT '',
          employment_status TEXT NOT NULL DEFAULT 'active',
          employment_type TEXT NOT NULL DEFAULT 'employee',
          primary_department_id TEXT NOT NULL DEFAULT '',
          canonical_fields_json TEXT NOT NULL DEFAULT '{}',
          field_sources_json TEXT NOT NULL DEFAULT '{}',
          status TEXT NOT NULL DEFAULT 'active',
          identity_revision INTEGER NOT NULL DEFAULT 1,
          created_by TEXT NOT NULL DEFAULT '',
          created_at TEXT NOT NULL,
          updated_at TEXT NOT NULL
        );
        CREATE INDEX idx_enterprise_identities_employee
        ON enterprise_identities (org_id, canonical_employee_id, status, identity_id);

        CREATE TABLE source_connectors (
          org_id TEXT NOT NULL,
          connector_id TEXT NOT NULL,
          provider_id TEXT NOT NULL,
          name TEXT NOT NULL DEFAULT '',
          corpid TEXT NOT NULL DEFAULT '',
          agentid TEXT NOT NULL DEFAULT '',
          corpsecret TEXT NOT NULL DEFAULT '',
          is_enabled INTEGER NOT NULL DEFAULT 1,
          credentials_expires_at TEXT NOT NULL DEFAULT '',
          granted_permissions_json TEXT NOT NULL DEFAULT '[]',
          required_permissions_json TEXT NOT NULL DEFAULT '[]',
          authorization_scope_json TEXT NOT NULL DEFAULT '{}',
          connection_status TEXT NOT NULL DEFAULT 'not_tested',
          last_tested_at TEXT NOT NULL DEFAULT '',
          last_sync_at TEXT NOT NULL DEFAULT '',
          department_count INTEGER NOT NULL DEFAULT 0,
          account_count INTEGER NOT NULL DEFAULT 0,
          quality_issue_count INTEGER NOT NULL DEFAULT 0,
          last_error TEXT NOT NULL DEFAULT '',
          created_at TEXT NOT NULL,
          updated_at TEXT NOT NULL,
          PRIMARY KEY (org_id, connector_id)
        );
        CREATE INDEX idx_source_connectors_provider
        ON source_connectors (org_id, provider_id, is_enabled, connector_id);
        CREATE UNIQUE INDEX idx_source_connectors_tenant
        ON source_connectors (org_id, provider_id, corpid)
        WHERE corpid <> '';

        CREATE TABLE platform_accounts (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          org_id TEXT NOT NULL,
          provider_id TEXT NOT NULL,
          connector_id TEXT NOT NULL DEFAULT 'default',
          platform_account_id TEXT NOT NULL,
          display_name TEXT NOT NULL DEFAULT '',
          employee_id TEXT NOT NULL DEFAULT '',
          email TEXT NOT NULL DEFAULT '',
          mobile TEXT NOT NULL DEFAULT '',
          account_status TEXT NOT NULL DEFAULT 'active',
          account_type TEXT NOT NULL DEFAULT 'person',
          primary_department_id TEXT NOT NULL DEFAULT '',
          department_ids_json TEXT NOT NULL DEFAULT '[]',
          manager_account_id TEXT NOT NULL DEFAULT '',
          custom_fields_json TEXT NOT NULL DEFAULT '{}',
          source_snapshot_id INTEGER,
          raw_payload_json TEXT NOT NULL DEFAULT '{}',
          is_excluded INTEGER NOT NULL DEFAULT 0,
          first_seen_at TEXT NOT NULL,
          last_seen_at TEXT NOT NULL,
          created_at TEXT NOT NULL,
          updated_at TEXT NOT NULL,
          UNIQUE (org_id, provider_id, connector_id, platform_account_id),
          FOREIGN KEY(source_snapshot_id) REFERENCES source_directory_snapshots(id)
        );
        CREATE INDEX idx_platform_accounts_identity_fields
        ON platform_accounts (org_id, employee_id, email, mobile, account_status);
        CREATE INDEX idx_platform_accounts_snapshot
        ON platform_accounts (org_id, provider_id, connector_id, source_snapshot_id);

        CREATE TABLE ad_directory_snapshots (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          org_id TEXT NOT NULL,
          connector_id TEXT NOT NULL DEFAULT 'default',
          status TEXT NOT NULL DEFAULT 'refreshing',
          started_at TEXT NOT NULL,
          completed_at TEXT NOT NULL DEFAULT '',
          expires_at TEXT NOT NULL DEFAULT '',
          user_count INTEGER NOT NULL DEFAULT 0,
          ou_count INTEGER NOT NULL DEFAULT 0,
          duplicate_employee_id_count INTEGER NOT NULL DEFAULT 0,
          duplicate_employee_number_count INTEGER NOT NULL DEFAULT 0,
          snapshot_fingerprint TEXT NOT NULL DEFAULT '',
          capability_report_json TEXT NOT NULL DEFAULT '{}',
          error_summary TEXT NOT NULL DEFAULT '',
          created_by TEXT NOT NULL DEFAULT '',
          metadata_json TEXT NOT NULL DEFAULT '{}'
        );
        CREATE INDEX idx_ad_directory_snapshots_latest
        ON ad_directory_snapshots (org_id, connector_id, status, completed_at DESC, id DESC);

        CREATE TABLE ad_accounts (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          org_id TEXT NOT NULL,
          connector_id TEXT NOT NULL DEFAULT 'default',
          object_guid TEXT NOT NULL DEFAULT '',
          object_sid TEXT NOT NULL DEFAULT '',
          distinguished_name TEXT NOT NULL DEFAULT '',
          sam_account_name TEXT NOT NULL DEFAULT '',
          user_principal_name TEXT NOT NULL DEFAULT '',
          employee_id TEXT NOT NULL DEFAULT '',
          employee_number TEXT NOT NULL DEFAULT '',
          mail TEXT NOT NULL DEFAULT '',
          telephone_number TEXT NOT NULL DEFAULT '',
          mobile TEXT NOT NULL DEFAULT '',
          display_name TEXT NOT NULL DEFAULT '',
          account_enabled INTEGER,
          manager_dn TEXT NOT NULL DEFAULT '',
          group_membership_json TEXT NOT NULL DEFAULT '[]',
          ou_path TEXT NOT NULL DEFAULT '',
          extension_attributes_json TEXT NOT NULL DEFAULT '{}',
          when_created TEXT NOT NULL DEFAULT '',
          when_changed TEXT NOT NULL DEFAULT '',
          account_type TEXT NOT NULL DEFAULT 'person',
          is_protected INTEGER NOT NULL DEFAULT 0,
          latest_snapshot_id INTEGER,
          first_seen_at TEXT NOT NULL,
          last_seen_at TEXT NOT NULL,
          created_at TEXT NOT NULL,
          updated_at TEXT NOT NULL,
          FOREIGN KEY(latest_snapshot_id) REFERENCES ad_directory_snapshots(id)
        );
        CREATE UNIQUE INDEX idx_ad_accounts_object_guid
        ON ad_accounts (org_id, connector_id, object_guid)
        WHERE object_guid <> '';
        CREATE UNIQUE INDEX idx_ad_accounts_sam
        ON ad_accounts (org_id, connector_id, sam_account_name)
        WHERE sam_account_name <> '';
        CREATE INDEX idx_ad_accounts_match_fields
        ON ad_accounts (org_id, connector_id, employee_id, employee_number, mail, mobile);

        CREATE TABLE ad_ou_snapshots (
          snapshot_id INTEGER NOT NULL,
          org_id TEXT NOT NULL,
          connector_id TEXT NOT NULL DEFAULT 'default',
          object_guid TEXT NOT NULL DEFAULT '',
          distinguished_name TEXT NOT NULL,
          name TEXT NOT NULL DEFAULT '',
          parent_distinguished_name TEXT NOT NULL DEFAULT '',
          path TEXT NOT NULL DEFAULT '',
          when_created TEXT NOT NULL DEFAULT '',
          when_changed TEXT NOT NULL DEFAULT '',
          PRIMARY KEY (snapshot_id, distinguished_name),
          FOREIGN KEY(snapshot_id) REFERENCES ad_directory_snapshots(id) ON DELETE CASCADE
        );
        CREATE INDEX idx_ad_ou_snapshots_tree
        ON ad_ou_snapshots (org_id, connector_id, snapshot_id, parent_distinguished_name);

        CREATE TABLE identity_account_links (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          org_id TEXT NOT NULL,
          identity_id TEXT NOT NULL,
          account_kind TEXT NOT NULL,
          platform_account_id INTEGER,
          ad_account_id INTEGER,
          account_role TEXT NOT NULL DEFAULT 'source',
          account_purpose TEXT NOT NULL DEFAULT '',
          association_type TEXT NOT NULL DEFAULT 'automatic',
          status TEXT NOT NULL DEFAULT 'active',
          source TEXT NOT NULL DEFAULT '',
          evidence_json TEXT NOT NULL DEFAULT '{}',
          confidence INTEGER NOT NULL DEFAULT 0,
          created_by TEXT NOT NULL DEFAULT '',
          valid_until TEXT NOT NULL DEFAULT '',
          link_revision INTEGER NOT NULL DEFAULT 1,
          created_at TEXT NOT NULL,
          updated_at TEXT NOT NULL,
          FOREIGN KEY(identity_id) REFERENCES enterprise_identities(identity_id),
          FOREIGN KEY(platform_account_id) REFERENCES platform_accounts(id),
          FOREIGN KEY(ad_account_id) REFERENCES ad_accounts(id),
          CHECK (
            (account_kind = 'platform' AND platform_account_id IS NOT NULL AND ad_account_id IS NULL)
            OR (account_kind = 'ad' AND ad_account_id IS NOT NULL AND platform_account_id IS NULL)
          )
        );
        CREATE UNIQUE INDEX idx_identity_links_platform_active
        ON identity_account_links (org_id, platform_account_id)
        WHERE account_kind = 'platform' AND status = 'active';
        CREATE UNIQUE INDEX idx_identity_links_ad_active
        ON identity_account_links (org_id, ad_account_id)
        WHERE account_kind = 'ad' AND status = 'active';
        CREATE UNIQUE INDEX idx_identity_links_primary_ad
        ON identity_account_links (org_id, identity_id)
        WHERE account_kind = 'ad' AND account_role = 'primary_ad' AND status = 'active';
        CREATE INDEX idx_identity_links_identity
        ON identity_account_links (org_id, identity_id, status, account_kind, account_role);

        CREATE TABLE identity_match_rules (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          org_id TEXT NOT NULL,
          rule_order INTEGER NOT NULL DEFAULT 100,
          rule_name TEXT NOT NULL,
          source_provider TEXT NOT NULL DEFAULT '*',
          source_field TEXT NOT NULL,
          ad_field TEXT NOT NULL,
          is_required INTEGER NOT NULL DEFAULT 0,
          case_sensitive INTEGER NOT NULL DEFAULT 0,
          trim_whitespace INTEGER NOT NULL DEFAULT 1,
          strip_phone_country_code INTEGER NOT NULL DEFAULT 0,
          lowercase_email INTEGER NOT NULL DEFAULT 0,
          allow_fallback INTEGER NOT NULL DEFAULT 1,
          allow_auto_link INTEGER NOT NULL DEFAULT 0,
          confidence_level TEXT NOT NULL DEFAULT 'medium',
          confidence_score INTEGER NOT NULL DEFAULT 50,
          stop_on_conflict INTEGER NOT NULL DEFAULT 1,
          is_enabled INTEGER NOT NULL DEFAULT 1,
          rule_revision INTEGER NOT NULL DEFAULT 1,
          created_by TEXT NOT NULL DEFAULT '',
          created_at TEXT NOT NULL,
          updated_at TEXT NOT NULL,
          UNIQUE (org_id, rule_name)
        );
        CREATE INDEX idx_identity_match_rules_order
        ON identity_match_rules (org_id, is_enabled, rule_order, id);

        CREATE TABLE identity_match_runs (
          run_id TEXT PRIMARY KEY,
          org_id TEXT NOT NULL,
          source_snapshot_ids_json TEXT NOT NULL DEFAULT '[]',
          ad_snapshot_id INTEGER,
          rules_fingerprint TEXT NOT NULL DEFAULT '',
          config_fingerprint TEXT NOT NULL DEFAULT '',
          status TEXT NOT NULL DEFAULT 'running',
          summary_json TEXT NOT NULL DEFAULT '{}',
          created_by TEXT NOT NULL DEFAULT '',
          started_at TEXT NOT NULL,
          completed_at TEXT NOT NULL DEFAULT '',
          FOREIGN KEY(ad_snapshot_id) REFERENCES ad_directory_snapshots(id)
        );
        CREATE INDEX idx_identity_match_runs_latest
        ON identity_match_runs (org_id, status, completed_at DESC, started_at DESC);

        CREATE TABLE identity_match_candidates (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          run_id TEXT NOT NULL,
          org_id TEXT NOT NULL,
          platform_account_id INTEGER NOT NULL,
          ad_account_id INTEGER,
          proposed_identity_id TEXT,
          result_level TEXT NOT NULL,
          confidence INTEGER NOT NULL DEFAULT 0,
          matched_rule_ids_json TEXT NOT NULL DEFAULT '[]',
          matched_fields_json TEXT NOT NULL DEFAULT '{}',
          unmatched_fields_json TEXT NOT NULL DEFAULT '{}',
          conflict_fields_json TEXT NOT NULL DEFAULT '{}',
          evidence_json TEXT NOT NULL DEFAULT '{}',
          recommended_action TEXT NOT NULL DEFAULT '',
          status TEXT NOT NULL DEFAULT 'pending',
          candidate_fingerprint TEXT NOT NULL DEFAULT '',
          created_at TEXT NOT NULL,
          updated_at TEXT NOT NULL,
          FOREIGN KEY(run_id) REFERENCES identity_match_runs(run_id) ON DELETE CASCADE,
          FOREIGN KEY(platform_account_id) REFERENCES platform_accounts(id),
          FOREIGN KEY(ad_account_id) REFERENCES ad_accounts(id),
          FOREIGN KEY(proposed_identity_id) REFERENCES enterprise_identities(identity_id)
        );
        CREATE INDEX idx_identity_match_candidates_queue
        ON identity_match_candidates (org_id, run_id, result_level, status, platform_account_id);

        CREATE TABLE identity_match_decisions (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          org_id TEXT NOT NULL,
          candidate_id INTEGER NOT NULL,
          decision TEXT NOT NULL,
          reason TEXT NOT NULL DEFAULT '',
          candidate_fingerprint TEXT NOT NULL,
          request_id TEXT NOT NULL DEFAULT '',
          decision_payload_json TEXT NOT NULL DEFAULT '{}',
          resulting_identity_id TEXT,
          resulting_link_ids_json TEXT NOT NULL DEFAULT '[]',
          decided_by TEXT NOT NULL,
          created_at TEXT NOT NULL,
          FOREIGN KEY(candidate_id) REFERENCES identity_match_candidates(id),
          FOREIGN KEY(resulting_identity_id) REFERENCES enterprise_identities(identity_id)
        );
        CREATE INDEX idx_identity_match_decisions_candidate
        ON identity_match_decisions (org_id, candidate_id, created_at DESC);
        CREATE UNIQUE INDEX uq_identity_match_decisions_request
        ON identity_match_decisions (org_id, request_id)
        WHERE request_id <> '';

        CREATE TABLE field_authority_rules (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          org_id TEXT NOT NULL,
          field_name TEXT NOT NULL,
          source_provider TEXT NOT NULL DEFAULT '*',
          source_priority INTEGER NOT NULL DEFAULT 100,
          sync_direction TEXT NOT NULL DEFAULT 'source_to_ad',
          sync_mode TEXT NOT NULL DEFAULT 'replace',
          prevent_loop INTEGER NOT NULL DEFAULT 1,
          is_enabled INTEGER NOT NULL DEFAULT 1,
          rule_revision INTEGER NOT NULL DEFAULT 1,
          notes TEXT NOT NULL DEFAULT '',
          created_by TEXT NOT NULL DEFAULT '',
          created_at TEXT NOT NULL,
          updated_at TEXT NOT NULL,
          UNIQUE (org_id, field_name, source_provider)
        );
        CREATE INDEX idx_field_authority_rules_priority
        ON field_authority_rules (org_id, field_name, is_enabled, source_priority, id);

        CREATE TABLE account_takeover_batches (
          batch_id TEXT PRIMARY KEY,
          org_id TEXT NOT NULL,
          status TEXT NOT NULL DEFAULT 'validating',
          original_filename TEXT NOT NULL DEFAULT '',
          file_fingerprint TEXT NOT NULL DEFAULT '',
          row_count INTEGER NOT NULL DEFAULT 0,
          valid_count INTEGER NOT NULL DEFAULT 0,
          conflict_count INTEGER NOT NULL DEFAULT 0,
          overwrite_count INTEGER NOT NULL DEFAULT 0,
          preview_fingerprint TEXT NOT NULL DEFAULT '',
          approved_by TEXT NOT NULL DEFAULT '',
          approved_at TEXT NOT NULL DEFAULT '',
          applied_by TEXT NOT NULL DEFAULT '',
          applied_at TEXT NOT NULL DEFAULT '',
          created_by TEXT NOT NULL DEFAULT '',
          created_at TEXT NOT NULL,
          updated_at TEXT NOT NULL
        );

        CREATE TABLE account_takeover_rows (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          batch_id TEXT NOT NULL,
          org_id TEXT NOT NULL,
          row_number INTEGER NOT NULL,
          provider_id TEXT NOT NULL DEFAULT '',
          connector_id TEXT NOT NULL DEFAULT 'default',
          platform_account_id TEXT NOT NULL DEFAULT '',
          ad_account_key TEXT NOT NULL DEFAULT '',
          validation_status TEXT NOT NULL DEFAULT 'pending',
          proposed_action TEXT NOT NULL DEFAULT '',
          existing_identity_id TEXT NOT NULL DEFAULT '',
          conflict_codes_json TEXT NOT NULL DEFAULT '[]',
          normalized_payload_json TEXT NOT NULL DEFAULT '{}',
          result_json TEXT NOT NULL DEFAULT '{}',
          created_at TEXT NOT NULL,
          updated_at TEXT NOT NULL,
          UNIQUE (batch_id, row_number),
          FOREIGN KEY(batch_id) REFERENCES account_takeover_batches(batch_id) ON DELETE CASCADE
        );
        CREATE INDEX idx_account_takeover_rows_status
        ON account_takeover_rows (org_id, batch_id, validation_status, row_number);

        INSERT OR IGNORE INTO enterprise_identities (
          identity_id, org_id, display_name, canonical_employee_id,
          employment_status, employment_type, primary_department_id,
          canonical_fields_json, field_sources_json, status,
          identity_revision, created_by, created_at, updated_at
        )
        SELECT
          'legacy-eid-' || id,
          org_id,
          source_display_name,
          '',
          'active',
          'employee',
          '',
          '{}',
          '{"migration":"user_identity_bindings_v34"}',
          CASE WHEN is_enabled = 1 THEN 'active' ELSE 'inactive' END,
          1,
          'migration_v34',
          COALESCE(NULLIF(updated_at, ''), CURRENT_TIMESTAMP),
          COALESCE(NULLIF(updated_at, ''), CURRENT_TIMESTAMP)
        FROM user_identity_bindings;

        INSERT OR IGNORE INTO platform_accounts (
          org_id, provider_id, connector_id, platform_account_id,
          display_name, employee_id, email, mobile, account_status,
          account_type, primary_department_id, department_ids_json,
          manager_account_id, custom_fields_json, source_snapshot_id,
          raw_payload_json, is_excluded, first_seen_at, last_seen_at,
          created_at, updated_at
        )
        SELECT
          org_id,
          source_provider,
          connector_id,
          source_user_id,
          source_display_name,
          '', '', '',
          CASE WHEN is_enabled = 1 THEN 'active' ELSE 'inactive' END,
          'person', '', '[]', '', '{}', NULL,
          '{"migration":"user_identity_bindings_v34"}',
          0,
          COALESCE(NULLIF(updated_at, ''), CURRENT_TIMESTAMP),
          COALESCE(NULLIF(updated_at, ''), CURRENT_TIMESTAMP),
          COALESCE(NULLIF(updated_at, ''), CURRENT_TIMESTAMP),
          COALESCE(NULLIF(updated_at, ''), CURRENT_TIMESTAMP)
        FROM user_identity_bindings;

        INSERT OR IGNORE INTO ad_accounts (
          org_id, connector_id, object_guid, object_sid,
          distinguished_name, sam_account_name, user_principal_name,
          employee_id, employee_number, mail, telephone_number, mobile,
          display_name, account_enabled, manager_dn, group_membership_json,
          ou_path, extension_attributes_json, when_created, when_changed,
          account_type, is_protected, latest_snapshot_id, first_seen_at,
          last_seen_at, created_at, updated_at
        )
        SELECT
          org_id,
          connector_id,
          target_object_guid,
          '',
          target_object_dn,
          ad_username,
          '', '', '', '', '', '',
          source_display_name,
          CASE WHEN is_enabled = 1 THEN 1 ELSE NULL END,
          '', '[]', '', '{}', '', '', 'person', 0, NULL,
          COALESCE(NULLIF(updated_at, ''), CURRENT_TIMESTAMP),
          COALESCE(NULLIF(updated_at, ''), CURRENT_TIMESTAMP),
          COALESCE(NULLIF(updated_at, ''), CURRENT_TIMESTAMP),
          COALESCE(NULLIF(updated_at, ''), CURRENT_TIMESTAMP)
        FROM user_identity_bindings
        WHERE ad_username <> '';

        INSERT OR IGNORE INTO identity_account_links (
          org_id, identity_id, account_kind, platform_account_id,
          ad_account_id, account_role, account_purpose, association_type,
          status, source, evidence_json, confidence, created_by,
          valid_until, link_revision, created_at, updated_at
        )
        SELECT
          binding.org_id,
          'legacy-eid-' || binding.id,
          'platform',
          account.id,
          NULL,
          'source',
          '',
          CASE
            WHEN LOWER(binding.source) LIKE '%import%' THEN 'imported'
            WHEN LOWER(binding.source) LIKE '%manual%' THEN 'manual'
            ELSE 'automatic'
          END,
          CASE WHEN binding.is_enabled = 1 THEN 'active' ELSE 'inactive' END,
          binding.source,
          '{"migration":"user_identity_bindings_v34"}',
          CASE WHEN LOWER(binding.source) LIKE '%manual%' THEN 100 ELSE 90 END,
          'migration_v34',
          '', 1,
          COALESCE(NULLIF(binding.updated_at, ''), CURRENT_TIMESTAMP),
          COALESCE(NULLIF(binding.updated_at, ''), CURRENT_TIMESTAMP)
        FROM user_identity_bindings AS binding
        JOIN platform_accounts AS account
          ON account.org_id = binding.org_id
         AND account.provider_id = binding.source_provider
         AND account.connector_id = binding.connector_id
         AND account.platform_account_id = binding.source_user_id;

        INSERT OR IGNORE INTO identity_account_links (
          org_id, identity_id, account_kind, platform_account_id,
          ad_account_id, account_role, account_purpose, association_type,
          status, source, evidence_json, confidence, created_by,
          valid_until, link_revision, created_at, updated_at
        )
        SELECT
          binding.org_id,
          'legacy-eid-' || binding.id,
          'ad',
          NULL,
          account.id,
          'primary_ad',
          'primary_directory_account',
          CASE
            WHEN LOWER(binding.source) LIKE '%import%' THEN 'imported'
            WHEN LOWER(binding.source) LIKE '%manual%' THEN 'manual'
            ELSE 'automatic'
          END,
          CASE WHEN binding.is_enabled = 1 THEN 'active' ELSE 'inactive' END,
          binding.source,
          '{"migration":"user_identity_bindings_v34"}',
          CASE WHEN LOWER(binding.source) LIKE '%manual%' THEN 100 ELSE 90 END,
          'migration_v34',
          '', 1,
          COALESCE(NULLIF(binding.updated_at, ''), CURRENT_TIMESTAMP),
          COALESCE(NULLIF(binding.updated_at, ''), CURRENT_TIMESTAMP)
        FROM user_identity_bindings AS binding
        JOIN ad_accounts AS account
          ON account.org_id = binding.org_id
         AND account.connector_id = binding.connector_id
         AND LOWER(account.sam_account_name) = LOWER(binding.ad_username)
        WHERE binding.ad_username <> '';
        """,
    ),
    (
        35,
        "bind rollout reviews and execution plans to current identity evidence",
        """
        CREATE TABLE rollout_data_quality_reviews (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          org_id TEXT NOT NULL,
          source_snapshot_id INTEGER NOT NULL,
          source_snapshot_fingerprint TEXT NOT NULL,
          status TEXT NOT NULL DEFAULT 'confirmed',
          reviewer_username TEXT NOT NULL,
          review_notes TEXT NOT NULL DEFAULT '',
          reviewed_at TEXT NOT NULL,
          UNIQUE (org_id, source_snapshot_id),
          FOREIGN KEY(source_snapshot_id) REFERENCES source_directory_snapshots(id)
        );
        CREATE INDEX idx_rollout_data_quality_reviews_current
        ON rollout_data_quality_reviews (
          org_id, source_snapshot_id, source_snapshot_fingerprint, status, reviewed_at DESC
        );

        ALTER TABLE sync_job_source_scopes
        ADD COLUMN ad_snapshot_id INTEGER;
        ALTER TABLE sync_job_source_scopes
        ADD COLUMN ad_snapshot_fingerprint TEXT NOT NULL DEFAULT '';
        ALTER TABLE sync_job_source_scopes
        ADD COLUMN identity_match_run_id TEXT NOT NULL DEFAULT '';
        ALTER TABLE sync_job_source_scopes
        ADD COLUMN identity_match_rules_fingerprint TEXT NOT NULL DEFAULT '';
        ALTER TABLE sync_job_source_scopes
        ADD COLUMN policy_release_id INTEGER;
        ALTER TABLE sync_job_source_scopes
        ADD COLUMN policy_release_hash TEXT NOT NULL DEFAULT '';

        CREATE INDEX idx_sync_job_rollout_evidence
        ON sync_job_source_scopes (
          org_id, execution_mode, snapshot_id, ad_snapshot_id,
          identity_match_run_id, policy_release_id, created_at DESC
        );
        """,
    ),
    (
        36,
        "add durable source canonical and AD target field registries",
        """
        CREATE TABLE source_field_registry (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          org_id TEXT NOT NULL,
          provider_id TEXT NOT NULL,
          source_connector_id TEXT NOT NULL DEFAULT 'default',
          raw_field_path TEXT NOT NULL,
          raw_field_name TEXT NOT NULL,
          canonical_field_key TEXT NOT NULL DEFAULT '',
          display_label TEXT NOT NULL DEFAULT '',
          category TEXT NOT NULL DEFAULT 'custom',
          data_type TEXT NOT NULL DEFAULT 'string',
          is_multi_value INTEGER NOT NULL DEFAULT 0,
          is_sensitive INTEGER NOT NULL DEFAULT 0,
          is_identifier_candidate INTEGER NOT NULL DEFAULT 0,
          is_custom INTEGER NOT NULL DEFAULT 0,
          is_derived INTEGER NOT NULL DEFAULT 0,
          availability_status TEXT NOT NULL DEFAULT 'unknown',
          permission_status TEXT NOT NULL DEFAULT 'unknown',
          coverage_count INTEGER NOT NULL DEFAULT 0,
          coverage_rate REAL NOT NULL DEFAULT 0,
          masked_sample_values_json TEXT NOT NULL DEFAULT '[]',
          first_detected_at TEXT NOT NULL,
          last_detected_at TEXT NOT NULL,
          schema_version INTEGER NOT NULL DEFAULT 1,
          latest_snapshot_id INTEGER,
          UNIQUE (org_id, provider_id, source_connector_id, raw_field_path)
        );
        CREATE INDEX idx_source_field_registry_catalog
        ON source_field_registry (
          org_id, provider_id, source_connector_id, availability_status,
          category, raw_field_path
        );
        CREATE INDEX idx_source_field_registry_snapshot
        ON source_field_registry (org_id, latest_snapshot_id, schema_version);

        CREATE TABLE canonical_field_registry (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          org_id TEXT NOT NULL DEFAULT '*',
          canonical_field_key TEXT NOT NULL,
          display_label TEXT NOT NULL,
          category TEXT NOT NULL,
          data_type TEXT NOT NULL DEFAULT 'string',
          is_multi_value INTEGER NOT NULL DEFAULT 0,
          is_sensitive INTEGER NOT NULL DEFAULT 0,
          is_identifier INTEGER NOT NULL DEFAULT 0,
          is_custom INTEGER NOT NULL DEFAULT 0,
          is_derived INTEGER NOT NULL DEFAULT 0,
          allowed_mapping_roles_json TEXT NOT NULL DEFAULT '[]',
          description TEXT NOT NULL DEFAULT '',
          schema_version INTEGER NOT NULL DEFAULT 1,
          is_active INTEGER NOT NULL DEFAULT 1,
          created_at TEXT NOT NULL,
          updated_at TEXT NOT NULL,
          UNIQUE (org_id, canonical_field_key)
        );
        CREATE INDEX idx_canonical_field_registry_catalog
        ON canonical_field_registry (org_id, is_active, category, canonical_field_key);

        CREATE TABLE ad_target_attribute_registry (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          org_id TEXT NOT NULL,
          ad_connector_id TEXT NOT NULL DEFAULT 'default',
          ldap_attribute TEXT NOT NULL,
          display_label TEXT NOT NULL,
          category TEXT NOT NULL,
          data_type TEXT NOT NULL DEFAULT 'string',
          is_multi_value INTEGER NOT NULL DEFAULT 0,
          is_writable INTEGER NOT NULL DEFAULT 0,
          is_read_only INTEGER NOT NULL DEFAULT 1,
          requires_special_handler INTEGER NOT NULL DEFAULT 0,
          special_handler_type TEXT NOT NULL DEFAULT '',
          required_permissions_json TEXT NOT NULL DEFAULT '[]',
          supported_object_classes_json TEXT NOT NULL DEFAULT '["user"]',
          schema_detected INTEGER NOT NULL DEFAULT 0,
          capability_status TEXT NOT NULL DEFAULT 'unknown',
          validation_rules_json TEXT NOT NULL DEFAULT '{}',
          schema_version INTEGER NOT NULL DEFAULT 1,
          latest_snapshot_id INTEGER,
          last_checked_at TEXT NOT NULL,
          UNIQUE (org_id, ad_connector_id, ldap_attribute)
        );
        CREATE INDEX idx_ad_target_attribute_registry_catalog
        ON ad_target_attribute_registry (
          org_id, ad_connector_id, schema_detected, is_writable,
          category, ldap_attribute
        );
        CREATE INDEX idx_ad_target_attribute_registry_snapshot
        ON ad_target_attribute_registry (org_id, latest_snapshot_id, schema_version);
        """,
    ),
    (
        37,
        "version attribute mappings and field authority policies",
        """
        ALTER TABLE attribute_mapping_rules
        ADD COLUMN provider_scope TEXT NOT NULL DEFAULT '*';
        ALTER TABLE attribute_mapping_rules
        ADD COLUMN source_connector_id TEXT NOT NULL DEFAULT 'default';
        ALTER TABLE attribute_mapping_rules
        ADD COLUMN canonical_source_field TEXT NOT NULL DEFAULT '';
        ALTER TABLE attribute_mapping_rules
        ADD COLUMN raw_source_field_path TEXT NOT NULL DEFAULT '';
        ALTER TABLE attribute_mapping_rules
        ADD COLUMN ad_connector_id TEXT NOT NULL DEFAULT 'default';
        ALTER TABLE attribute_mapping_rules
        ADD COLUMN mapping_role TEXT NOT NULL DEFAULT 'ATTRIBUTE_SYNC';
        ALTER TABLE attribute_mapping_rules
        ADD COLUMN authority_mode TEXT NOT NULL DEFAULT 'PROVIDER_PRIORITY';
        ALTER TABLE attribute_mapping_rules
        ADD COLUMN transform_pipeline_json TEXT NOT NULL DEFAULT '[]';
        ALTER TABLE attribute_mapping_rules
        ADD COLUMN null_policy TEXT NOT NULL DEFAULT 'PRESERVE_TARGET';
        ALTER TABLE attribute_mapping_rules
        ADD COLUMN conflict_policy TEXT NOT NULL DEFAULT 'REJECT_ON_CONFLICT';
        ALTER TABLE attribute_mapping_rules
        ADD COLUMN write_policy TEXT NOT NULL DEFAULT 'REPLACE';
        ALTER TABLE attribute_mapping_rules
        ADD COLUMN version INTEGER NOT NULL DEFAULT 1;
        ALTER TABLE attribute_mapping_rules
        ADD COLUMN created_by TEXT NOT NULL DEFAULT '';

        UPDATE attribute_mapping_rules
        SET canonical_source_field = source_field,
            raw_source_field_path = source_field,
            source_connector_id = CASE
              WHEN connector_id = '' THEN 'default' ELSE connector_id END,
            ad_connector_id = CASE
              WHEN connector_id = '' THEN 'default' ELSE connector_id END,
            write_policy = CASE
              WHEN sync_mode = 'fill_if_empty' THEN 'FILL_IF_EMPTY'
              WHEN sync_mode = 'preserve' THEN 'PRESERVE_TARGET'
              ELSE 'REPLACE' END,
            null_policy = 'PRESERVE_TARGET'
        WHERE canonical_source_field = '';

        CREATE INDEX idx_attribute_mapping_rules_governed
        ON attribute_mapping_rules (
          org_id, provider_scope, ad_connector_id, mapping_role,
          is_enabled, version, updated_at DESC
        );

        ALTER TABLE field_authority_rules
        ADD COLUMN authority_mode TEXT NOT NULL DEFAULT 'PROVIDER_PRIORITY';
        ALTER TABLE field_authority_rules
        ADD COLUMN authoritative_connector_id TEXT NOT NULL DEFAULT '';
        ALTER TABLE field_authority_rules
        ADD COLUMN provider_priority_json TEXT NOT NULL DEFAULT '[]';
        ALTER TABLE field_authority_rules
        ADD COLUMN conflict_policy TEXT NOT NULL DEFAULT 'PROVIDER_PRIORITY';
        ALTER TABLE field_authority_rules
        ADD COLUMN null_policy TEXT NOT NULL DEFAULT 'PRESERVE_TARGET';
        ALTER TABLE field_authority_rules
        ADD COLUMN manual_override_policy TEXT NOT NULL DEFAULT 'REQUIRE_REVIEW';
        ALTER TABLE field_authority_rules
        ADD COLUMN effective_version INTEGER NOT NULL DEFAULT 1;

        UPDATE field_authority_rules
        SET provider_priority_json = json_array(source_provider),
            conflict_policy = 'PROVIDER_PRIORITY',
            effective_version = rule_revision;

        CREATE INDEX idx_field_authority_rules_governed
        ON field_authority_rules (
          org_id, field_name, authority_mode, is_enabled,
          effective_version, updated_at DESC
        );
        """,
    ),
    (
        38,
        "bind policy releases to directory and identity evidence versions",
        """
        ALTER TABLE config_release_snapshots
        ADD COLUMN source_snapshot_fingerprint TEXT NOT NULL DEFAULT '';
        ALTER TABLE config_release_snapshots
        ADD COLUMN ad_snapshot_id INTEGER;
        ALTER TABLE config_release_snapshots
        ADD COLUMN ad_snapshot_fingerprint TEXT NOT NULL DEFAULT '';
        ALTER TABLE config_release_snapshots
        ADD COLUMN source_field_catalog_fingerprint TEXT NOT NULL DEFAULT '';
        ALTER TABLE config_release_snapshots
        ADD COLUMN ad_capability_catalog_fingerprint TEXT NOT NULL DEFAULT '';
        ALTER TABLE config_release_snapshots
        ADD COLUMN identity_match_run_id TEXT NOT NULL DEFAULT '';
        ALTER TABLE config_release_snapshots
        ADD COLUMN identity_match_rules_fingerprint TEXT NOT NULL DEFAULT '';
        ALTER TABLE config_release_snapshots
        ADD COLUMN evidence_fingerprint TEXT NOT NULL DEFAULT '';

        CREATE INDEX idx_config_release_snapshots_evidence
        ON config_release_snapshots (
          org_id, source_snapshot_id, ad_snapshot_id, identity_match_run_id,
          evidence_fingerprint, created_at DESC
        );
        """,
    ),
    (
        39,
        "bind data quality scans to immutable source snapshot evidence",
        """
        ALTER TABLE data_quality_snapshots
        ADD COLUMN source_snapshot_id INTEGER;
        ALTER TABLE data_quality_snapshots
        ADD COLUMN source_snapshot_fingerprint TEXT NOT NULL DEFAULT '';
        ALTER TABLE data_quality_snapshots
        ADD COLUMN scan_status TEXT NOT NULL DEFAULT 'qualified';

        UPDATE data_quality_snapshots
        SET source_snapshot_id = CAST(
              json_extract(snapshot_json, '$.source_snapshot_id') AS INTEGER
            ),
            source_snapshot_fingerprint = COALESCE(
              json_extract(snapshot_json, '$.source_snapshot_fingerprint'),
              ''
            ),
            scan_status = CASE
              WHEN COALESCE(
                CAST(json_extract(summary_json, '$.error_issue_count') AS INTEGER),
                0
              ) > 0 THEN 'unqualified'
              ELSE 'qualified'
            END;

        CREATE INDEX idx_data_quality_snapshots_source_evidence
        ON data_quality_snapshots (
          org_id, source_snapshot_fingerprint, source_snapshot_id,
          created_at DESC, id DESC
        );
        """,
    ),
]
