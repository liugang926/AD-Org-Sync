# Notting AD Sync SQLite 表结构设计与实施任务清单

## 1. 目标

为工程化改造提供一套可落地的本地 SQLite 设计，用于承载：

1. 本地应用配置
2. 排除与保护策略
3. 同步作业与阶段状态
4. 计划变更与执行结果
5. 对象级增量同步状态

本设计不承载：

1. 企业微信或 AD 的业务真相
2. 密码、Secret、Webhook、token 等敏感凭据

## 2. 文件位置建议

SQLite 文件不应放在源码根目录。

建议默认位置：

- `%APPDATA%/NottingADSync/app.db`

打包版与源码版都应统一走应用数据目录。

## 3. SQLite 基础要求

建议初始化参数：

1. `PRAGMA journal_mode = WAL`
2. `PRAGMA foreign_keys = ON`
3. `PRAGMA synchronous = NORMAL`
4. `PRAGMA temp_store = MEMORY`

原因：

1. 提升可靠性
2. 避免 JSON 文件式脆弱写入
3. 保证基本查询性能

## 4. 表结构概览

建议最小表集如下：

1. `schema_migrations`
2. `app_settings`
3. `group_exclusion_rules`
4. `sync_jobs`
5. `sync_job_stages`
6. `sync_events`
7. `planned_operations`
8. `executed_operations`
9. `object_sync_state`
10. `managed_group_bindings`

## 5. 详细表设计

### 5.1 schema_migrations

```sql
CREATE TABLE schema_migrations (
  version INTEGER PRIMARY KEY,
  description TEXT NOT NULL,
  applied_at TEXT NOT NULL
);
```

用途：

1. 记录数据库 schema 版本
2. 支撑后续升级迁移

### 5.2 app_settings

```sql
CREATE TABLE app_settings (
  key TEXT PRIMARY KEY,
  value TEXT NOT NULL,
  value_type TEXT NOT NULL DEFAULT 'string',
  updated_at TEXT NOT NULL
);
```

建议初始键：

1. `group_display_separator`
2. `group_display_mode`
3. `group_display_apply_scope`
4. `group_recursive_enabled`
5. `schedule_execution_mode`
6. `managed_relation_cleanup_enabled`
7. `job_history_retention_days`
8. `event_history_retention_days`

### 5.3 group_exclusion_rules

```sql
CREATE TABLE group_exclusion_rules (
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
```

建议索引：

```sql
CREATE INDEX idx_group_exclusion_rules_match
ON group_exclusion_rules (match_type, match_value, is_enabled);
```

### 5.4 sync_jobs

```sql
CREATE TABLE sync_jobs (
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
```

建议索引：

```sql
CREATE INDEX idx_sync_jobs_started_at ON sync_jobs (started_at DESC);
CREATE INDEX idx_sync_jobs_status ON sync_jobs (status);
```

### 5.5 sync_job_stages

```sql
CREATE TABLE sync_job_stages (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  job_id TEXT NOT NULL,
  stage_name TEXT NOT NULL,
  status TEXT NOT NULL,
  object_count INTEGER NOT NULL DEFAULT 0,
  error_count INTEGER NOT NULL DEFAULT 0,
  started_at TEXT NOT NULL,
  ended_at TEXT,
  FOREIGN KEY(job_id) REFERENCES sync_jobs(job_id)
);
```

建议索引：

```sql
CREATE INDEX idx_sync_job_stages_job_id ON sync_job_stages (job_id, stage_name);
```

### 5.6 sync_events

```sql
CREATE TABLE sync_events (
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
```

建议索引：

```sql
CREATE INDEX idx_sync_events_job_id ON sync_events (job_id, created_at);
CREATE INDEX idx_sync_events_event_type ON sync_events (event_type);
```

### 5.7 planned_operations

```sql
CREATE TABLE planned_operations (
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
```

用途：

1. 保存 `dry_run` 和 `apply` 的统一计划快照
2. 作为执行前预览依据

建议索引：

```sql
CREATE INDEX idx_planned_operations_job_id
ON planned_operations (job_id, object_type, operation_type);
```

### 5.8 executed_operations

```sql
CREATE TABLE executed_operations (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  job_id TEXT NOT NULL,
  planned_operation_id INTEGER,
  object_type TEXT NOT NULL,
  source_id TEXT,
  department_id TEXT,
  target_dn TEXT,
  operation_type TEXT NOT NULL,
  result TEXT NOT NULL,
  error_code TEXT,
  error_message TEXT,
  payload_json TEXT,
  created_at TEXT NOT NULL,
  FOREIGN KEY(job_id) REFERENCES sync_jobs(job_id),
  FOREIGN KEY(planned_operation_id) REFERENCES planned_operations(id)
);
```

建议索引：

```sql
CREATE INDEX idx_executed_operations_job_id
ON executed_operations (job_id, object_type, operation_type, result);
```

### 5.9 object_sync_state

```sql
CREATE TABLE object_sync_state (
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
```

用途：

1. 支撑真正的增量同步
2. 记录对象最后一次已知状态摘要

建议索引：

```sql
CREATE INDEX idx_object_sync_state_object
ON object_sync_state (object_type, source_id);
```

### 5.10 managed_group_bindings

```sql
CREATE TABLE managed_group_bindings (
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
```

用途：

1. 记录系统管理的部门到组映射
2. 支撑按 `department_id` 精确找组
3. 支撑递归组层级与关系收敛

建议索引：

```sql
CREATE INDEX idx_managed_group_bindings_parent
ON managed_group_bindings (parent_department_id);
```

## 6. 初始迁移建议

建议迁移版本：

1. `v1` 创建基础表
2. `v2` 导入默认 `app_settings`
3. `v3` 导入默认保护组与排除组规则

## 7. Repository 分层建议

建议实现以下仓储接口：

1. `SettingsRepository`
2. `GroupExclusionRuleRepository`
3. `SyncJobRepository`
4. `SyncEventRepository`
5. `PlannedOperationRepository`
6. `ExecutedOperationRepository`
7. `ObjectStateRepository`
8. `ManagedGroupBindingRepository`

要求：

1. 业务逻辑不得直接拼写 SQL
2. 所有 SQL 通过仓储层集中维护
3. 写操作应显式事务化

## 8. 与现有文件的迁移建议

### `sync_state.json`

迁移目标：

- 迁移至 `object_sync_state`

### `config.ini`

保留用途：

1. 基础连接配置
2. 非敏感启动配置

迁移到 SQLite 的内容：

1. UI 偏好
2. 命名策略
3. 排除组策略
4. 调度执行模式等本地应用设置

### 日志文件

保留用途：

1. 人类可读日志
2. 故障导出

SQLite 补充用途：

1. 结构化检索
2. 作业级审计

## 9. 任务清单

### Phase 1: 基础设施

1. 新增 `DatabaseManager`
2. 实现 migration runner
3. 确定应用数据目录
4. 增加 SQLite 初始化与健康检查

### Phase 2: 配置层

1. 实现 `app_settings`
2. 接入命名策略配置
3. 接入排除/保护组规则
4. 提供默认值回填

### Phase 3: 作业层

1. 实现 `sync_jobs`
2. 实现 `sync_job_stages`
3. 实现 `sync_events`
4. 统一 `job_id`

### Phase 4: 计划与执行层

1. 实现 `planned_operations`
2. 实现 `executed_operations`
3. 接入 `dry_run`
4. 接入 `apply`

### Phase 5: 增量同步层

1. 实现 `object_sync_state`
2. 替换 `sync_state.json`
3. 定义哈希字段集
4. 增加状态修复和重建能力

### Phase 6: 组模型层

1. 实现 `managed_group_bindings`
2. 接入部门到组唯一映射
3. 接入递归组嵌套
4. 接入受管关系收敛

## 10. 测试清单

### 单元测试

1. migration 执行正确
2. 默认值回填正确
3. 配置读写正确
4. 同步状态 upsert 正确
5. 计划与执行记录关联正确

### 集成测试

1. 首次启动建库成功
2. 升级迁移成功
3. `dry_run` 会写计划但不写执行结果
4. `apply` 会写计划与执行结果
5. 删除本地库后可自动重建

### 冒烟测试

1. GUI 启动时能初始化数据库
2. 配置修改后重启仍生效
3. 同步执行后能查询 `job_id`

## 11. 风险与注意事项

1. 不要把凭据写入 SQLite
2. 不要把 SQLite 当作外部系统真相源
3. 不要在 UI 线程中直接执行重查询
4. 不要在无事务保护下批量写入作业记录

## 12. 最小可落地切入点

若希望先快速落地，建议最小范围按以下顺序实施：

1. `schema_migrations`
2. `app_settings`
3. `group_exclusion_rules`
4. `sync_jobs`
5. `sync_events`
6. `managed_group_bindings`

这样可以先支撑：

1. 本地配置持久化
2. 保护组/排除组规则
3. 作业追踪
4. 部门到组唯一映射

随后再接入：

1. `planned_operations`
2. `executed_operations`
3. `object_sync_state`

## 13. 建议的下一步输出

在本设计基础上，建议继续产出：

1. SQLite migration 脚本草案
2. Repository 接口定义
3. `job_id` 与事件模型字段规范
4. `planned_operations` / `executed_operations` 枚举字典
