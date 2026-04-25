# 架构优化完成计划

## 1. 目标

本计划承接现有 `技术优化路线图`，聚焦 P0-P3 的架构治理收口。

目标不是重写系统，而是在现有模块化单体基础上完成四件事：

1. 清理依赖方向，让 `core / services` 不反向依赖 `web`。
2. 拆分热点文件，让新增功能进入清晰领域模块。
3. 增加结构护栏，防止后续改动把边界重新打散。
4. 规定新功能入口，尤其是 `SSPR`、`HR 源系统`、更多 target provider。

完成后，项目应保持以下形态：

```text
presentation: web routes / CLI / desktop
application: use cases / service facades / sync orchestration
domain: models / policies / conflict decisions / rule governance
ports: SourceDirectoryProvider / TargetDirectoryProvider / NotificationPort
adapters: WeCom / DingTalk / AD LDAPS / SQLite / webhook
composition: app_state / runtime bootstrap / dependency wiring
```

## 2. 执行原则

1. 每一轮只改一个清晰边界，避免跨多个领域的大手术。
2. 先移动纯逻辑，再拆外部副作用代码。
3. 保留现有产品入口、URL、CLI 命令和数据库语义。
4. 每批必须有结构测试或定向测试防回退。
5. 新模块先薄后厚，先建立边界，再逐步迁移实现。

## 3. P0 依赖方向清理

### P0.1 移动 rule governance

现状：

- `sync_app/services/runtime.py` 引用 `sync_app.web.rule_governance`
- `sync_app/services/notification_automation_center.py` 也引用 Web 层治理汇总
- 这会让 services 依赖 presentation 层

目标：

- 将 `sync_app/web/rule_governance.py` 移到更低层
- 推荐目标路径：`sync_app/core/rule_governance.py`
- Web 层、service 层统一从新位置导入

交付物：

1. 新增或移动 `sync_app/core/rule_governance.py`
2. 更新以下导入：
   - `sync_app/services/runtime.py`
   - `sync_app/services/notification_automation_center.py`
   - `sync_app/web/routes_mappings.py`
   - `sync_app/web/routes_exceptions.py`
3. 保留兼容壳可选：
   - `sync_app/web/rule_governance.py` 仅 re-export，短期降低风险
   - 后续一轮再删除兼容壳

验收：

1. `sync_app/services` 中不存在 `from sync_app.web` 或 `import sync_app.web`
2. 规则治理相关测试继续通过
3. `tests/test_structure_guards.py` 增加 services 禁止依赖 web 的结构测试

建议回归：

```powershell
python -m pytest tests/test_structure_guards.py tests/test_rule_governance_metadata.py tests/test_runtime_dry_run.py -q
```

### P0.2 剥离 provider registry

现状：

- `sync_app/core/config.py` 导入 `sync_app.providers.source`
- `core` 既承担配置解析，又知道 provider schema 和 provider 构建
- 这让 core 难以保持纯领域层

目标：

- `core/config.py` 只负责配置对象解析、基础字段校验、TLS helper
- provider schema、provider 选择、连接测试迁到 application 或 provider registry

推荐拆分：

```text
sync_app/providers/source/registry.py
  - list_source_provider_schemas
  - get_source_provider_schema
  - get_source_provider_display_name
  - build_source_provider

sync_app/services/config_validation.py
  - validate_config
  - run_config_security_self_check
  - test_source_connection
  - test_ldap_connection wrapper if needed
```

分步策略：

1. 先新增 `providers/source/registry.py`，从现有 `base.py / wecom.py` 迁移 registry 和 factory。
2. 再新增 `services/config_validation.py`，迁移依赖 provider 的校验和连接测试。
3. 保留 `core/config.py` 的兼容导出一轮，避免一次性改动 CLI、Web、Runtime。
4. 最后一轮让调用方改为导入 `services.config_validation`。

验收：

1. `sync_app/core` 不导入 `sync_app.providers`
2. provider schema 增删不需要改 `core`
3. `tests/test_source_providers.py` 和配置相关测试继续通过
4. 新增结构测试：禁止 `sync_app/core` 依赖 provider 实现

建议回归：

```powershell
python -m pytest tests/test_structure_guards.py tests/test_source_providers.py tests/test_config_store.py tests/test_cli_config.py -q
```

## 4. P1 热点文件拆分

### P1.1 拆 core/models.py

现状：

- `sync_app/core/models.py` 超过 1800 行
- 同步模型、Web 用户模型、配置模型、生命周期模型、集成模型混在一起

目标结构：

```text
sync_app/core/models/
  __init__.py
  config.py
  directory.py
  sync_job.py
  conflicts.py
  lifecycle.py
  integrations.py
  web_admin.py
```

迁移顺序：

1. 先创建 package，`__init__.py` re-export 现有公开类名。
2. 按低耦合模型先迁移：`web_admin`、`integrations`、`lifecycle`。
3. 再迁移 `sync_job / conflicts`。
4. 最后迁移 `config / directory`。

验收：

1. 原有 `from sync_app.core.models import X` 不破坏
2. 单个模型文件控制在 500 行以内
3. 模型拆分不改变序列化字段

建议回归：

```powershell
python -m pytest tests/test_runtime_dry_run.py tests/test_web_storage.py tests/test_external_integrations.py -q
```

### P1.2 拆 storage/schema.py

现状：

- `sync_app/storage/schema.py` 包含默认设置、迁移 SQL、保护组清单
- 文件超过 1400 行，后续迁移扩展会越来越难审阅

目标结构：

```text
sync_app/storage/schema/
  __init__.py
  defaults.py
  protected_groups.py
  migrations.py
```

迁移顺序：

1. `DEFAULT_APP_SETTINGS / ORG_SCOPED_APP_SETTINGS` 移到 `defaults.py`
2. `DEFAULT_HARD_PROTECTED_GROUPS / DEFAULT_SOFT_EXCLUDED_GROUPS` 移到 `protected_groups.py`
3. `MIGRATIONS` 移到 `migrations.py`
4. `schema/__init__.py` re-export 兼容原导入

验收：

1. `DatabaseManager.initialize()` 行为不变
2. 新库、旧库迁移测试通过
3. schema 模块无循环导入

建议回归：

```powershell
python -m pytest tests/test_web_storage.py tests/test_config_store.py tests/test_v1_smoke_path.py -q
```

### P1.3 拆 web/service_facades.py

现状：

- 单文件承载 jobs、conflicts、config、integrations 等 facade
- 已经起到收口作用，但继续增长会变成新的中心化热点

目标结构：

```text
sync_app/web/services/
  __init__.py
  state.py
  jobs.py
  conflicts.py
  config.py
  integrations.py
```

迁移顺序：

1. 先迁移 `WebJobService`
2. 再迁移 `WebConflictService`
3. 然后迁移 `WebConfigService`
4. 最后迁移 `WebIntegrationService`
5. 原 `service_facades.py` 保留一轮 re-export

验收：

1. `get_web_services()` 对外对象结构不变
2. 路由层不重新拿回业务编排逻辑
3. 结构测试继续禁止路由直接处理审计和领域 workflow

建议回归：

```powershell
python -m pytest tests/test_structure_guards.py tests/test_web_authz.py tests/test_external_integrations.py -q
```

### P1.4 拆 cli.py

现状：

- `sync_app/cli.py` 超过 1200 行
- parser、handler、格式化输出、数据库操作混在同一文件

目标结构：

```text
sync_app/cli/
  __init__.py
  parser.py
  main.py
  handlers/
    config.py
    web.py
    sync.py
    conflicts.py
    database.py
```

迁移顺序：

1. 先抽 `build_parser` 到 `cli/parser.py`
2. 再按命令族迁移 handler
3. 保留 `sync_app.cli:main` 入口兼容
4. 最后将 CLI 输出格式化 helpers 下沉到对应 handler 或共享 helpers

验收：

1. `ad-org-sync = sync_app.cli:main` 不变
2. 旧命令 alias 继续可用
3. CLI 测试全部通过

建议回归：

```powershell
python -m pytest tests/test_cli_config.py tests/test_cli_conflicts.py tests/test_cli_web.py tests/test_cli_deploy.py -q
```

## 5. P2 结构护栏

新增结构测试分三类。

### P2.1 层级依赖护栏

规则：

1. `sync_app/services` 禁止导入 `sync_app.web`
2. `sync_app/core` 禁止导入 `sync_app.web`
3. `sync_app/core` 禁止导入 provider 实现模块
4. `sync_app/storage` 禁止导入 `sync_app.web`
5. `sync_app/providers` 禁止导入 `sync_app.web`

允许例外：

- 兼容壳 re-export 期间可用白名单，但必须注明删除批次

### P2.2 热点文件体量护栏

建议阈值：

1. 新增普通模块不超过 700 行
2. 新增 route 模块不超过 600 行
3. 新增 service facade 模块不超过 700 行
4. 迁移期允许旧文件超限，但不得继续增长

### P2.3 新功能入口护栏

规则：

1. 新 bounded context 必须有独立 module/package
2. Web route 只能调用 application service
3. CLI handler 只能调用 application service
4. 外部系统访问必须通过 provider/client/port，不允许散落在 route 中

建议新增测试文件：

```text
tests/test_architecture_boundaries.py
```

## 6. P3 新功能模块入口

### P3.1 SSPR

推荐路径：

```text
sync_app/modules/sspr/
  __init__.py
  domain.py
  service.py
  repositories.py
  routes.py
  source_auth.py
```

边界规则：

1. SSPR 不进入同步 runtime 内部。
2. 复用 AD target port 执行改密、解锁。
3. 复用 identity binding 查找 source user 与 AD username。
4. 所有操作写 Web audit log。
5. OAuth、扫码认证、员工会话与管理员 Web 会话分开。

### P3.2 HR 源系统

推荐路径：

```text
sync_app/providers/source/hr_master/
  __init__.py
  client.py
  provider.py
  schema.py
```

边界规则：

1. HR 作为 source provider，不直接写 AD。
2. 字段映射通过现有 attribute mapping 策略进入 runtime。
3. HR 特有生命周期字段先映射到 canonical source user payload。

### P3.3 更多 target provider

推荐路径：

```text
sync_app/providers/target/
  base.py
  ad_ldaps.py
  azure_ad.py
  openldap.py
  registry.py
```

边界规则：

1. Runtime 依赖 `TargetDirectoryProvider` port。
2. target provider 的能力差异通过 capability 描述，而不是在 runtime 中到处写 provider 判断。
3. 对不支持的能力返回明确错误或 planned operation warning。

## 7. 里程碑

### M0：计划入库

交付：

1. 本计划文档
2. 与现有技术优化路线图互相引用

验收：

1. 文档能指导 P0-P3 实施
2. 没有产品行为改动

### M1：P0 完成

交付：

1. services 不依赖 web
2. core 不依赖 provider 实现
3. provider registry 和配置校验职责分开
4. 新结构测试入库

验收：

1. 结构测试通过
2. source provider、config、runtime 定向测试通过

### M2：P1 首轮完成

交付：

1. `web/service_facades.py` 拆分
2. `cli.py` 拆分
3. `storage/schema.py` 拆分

验收：

1. Web 权限、CLI、SQLite 存储测试通过
2. 旧导入入口保留兼容

### M3：P1 二轮完成

交付：

1. `core/models.py` 拆分
2. 模型 re-export 兼容
3. 重点序列化测试通过

验收：

1. Runtime dry-run / apply 核心测试通过
2. Web storage 与 external integrations 测试通过

### M4：P2/P3 完成

交付：

1. 架构边界测试完整覆盖
2. 新 bounded context 模板明确
3. SSPR / HR / target provider 入口规则写入开发约定

验收：

1. 新功能不能绕过 application service 直接进 route
2. 新 provider 不需要改 core
3. 新 target 不需要大改 runtime 主流程

## 8. 推荐执行批次

### Batch 1

范围：

1. 移动 rule governance
2. 新增 services 禁止依赖 web 的结构测试

风险：低

### Batch 2

范围：

1. 新增 source provider registry
2. 拆出 config validation service
3. 新增 core/provider 依赖护栏

风险：中

### Batch 3

范围：

1. 拆 `web/service_facades.py`
2. 更新结构测试

风险：中

### Batch 4

范围：

1. 拆 `storage/schema.py`
2. 保留 re-export 兼容

风险：中

### Batch 5

范围：

1. 拆 `cli.py`
2. 保持 console script 入口不变

风险：中高

### Batch 6

范围：

1. 拆 `core/models.py`
2. 扩充模型序列化和导入兼容测试

风险：高

### Batch 7

范围：

1. 建立 `sync_app/modules/` 新功能入口约定
2. 为 SSPR / HR / target provider 写骨架约束或开发约定

风险：低

## 9. 完成定义

全部优化完成时，应满足：

1. `services -> web` 依赖为 0。
2. `core -> providers 实现` 依赖为 0。
3. 四个热点文件完成拆分或进入只读兼容壳状态。
4. 架构边界测试覆盖依赖方向、route/service 边界、新功能入口。
5. CLI、Web、Runtime、Storage 核心回归通过。
6. 后续新增 SSPR、HR、target provider 有明确落点，不再污染同步主链路。

## 10. 当前执行状态

### 2026-04-25

已完成 `Batch 1`：

1. 已将 `rule_governance` 真实实现下沉到 `sync_app/core/rule_governance.py`。
2. 已将 service 层和相关 Web route 的治理汇总导入切到 `sync_app.core.rule_governance`。
3. 已保留 `sync_app/web/rule_governance.py` 兼容 re-export 壳，降低短期导入兼容风险。
4. 已新增结构测试，禁止 `sync_app/services` 再导入 `sync_app.web`。
5. 已通过定向回归：`tests/test_structure_guards.py`、`tests/test_rule_governance_metadata.py`。

已完成 `Batch 2`：

1. 已新增 `sync_app/providers/source/registry.py`，由 registry 统一负责 source provider factory。
2. 已将 `sync_app/providers/source/wecom.py` 收回为 WeCom adapter 实现，不再承载多 provider factory。
3. 已新增 `sync_app/services/config_validation.py`，将依赖 provider schema / factory 的配置校验与 source 连接测试移出 `core`。
4. 已更新 CLI、Web、runtime、preflight 的导入点，改用 `sync_app.services.config_validation`。
5. 已新增结构测试，禁止 `sync_app/core` 再导入 `sync_app.providers`。
6. 已通过定向回归：`tests/test_structure_guards.py`、`tests/test_source_providers.py`、`tests/test_config_store.py`、`tests/test_cli_config.py`、`tests/test_runtime_dry_run.py`、`tests/test_cli_web.py`、`tests/test_web_readiness.py`。

已完成 `Batch 3`：

1. 已新增 `sync_app/web/services/` 包，按 `jobs / conflicts / config / integrations / state` 拆分 Web service facade。
2. 已将 `sync_app/web/service_facades.py` 收敛为兼容 re-export 壳。
3. 已将 `app_state` 的 service 组合入口切到 `sync_app.web.services`。
4. 已新增结构测试，要求 Web facade 类必须位于专用模块，兼容壳不得重新定义类。
5. 已通过定向回归：`tests/test_structure_guards.py`、`tests/test_web_authz.py`、`tests/test_external_integrations.py`、`tests/test_config_release.py`。

已完成 `Batch 4`：

1. 已将 `sync_app/storage/schema.py` 拆成 `sync_app/storage/schema/` 包。
2. 已按 `defaults / protected_groups / migrations` 拆分默认设置、默认保护组和 SQLite migration。
3. 已通过 `sync_app/storage/schema/__init__.py` 保留原有 `from sync_app.storage.schema import ...` 入口兼容。
4. 已新增结构测试，防止 schema 回退为单文件热点。
5. 已通过定向回归：`tests/test_structure_guards.py`、`tests/test_web_storage.py`、`tests/test_config_release.py`、`tests/test_cli_config.py`、`tests/test_typed_settings.py`。

已完成 `Batch 5`：

1. 已将 `sync_app/cli.py` 拆成 `sync_app/cli/` 包。
2. 已按 `parser / main / handlers(config, conflicts, database, sync, web)` 拆分 CLI parser 与命令处理逻辑。
3. 已保留 `sync_app.cli:main`、`python -m sync_app.cli`、`build_parser`、旧 `test-wecom` alias 兼容。
4. 已新增结构测试，防止 CLI 回退为单文件热点。
5. 已通过定向回归：`tests/test_structure_guards.py`、`tests/test_cli_config.py`、`tests/test_cli_conflicts.py`、`tests/test_cli_web.py`、`tests/test_cli_deploy.py`。

已完成 `Batch 6`：

1. 已将 `sync_app/core/models.py` 拆成 `sync_app/core/models/` 包。
2. 已按 `config / directory / sync_job / actions / conflicts / config_records / integrations / lifecycle / web_admin` 拆分模型。
3. 已通过 `sync_app/core/models/__init__.py` 保留原有 `from sync_app.core.models import ...` 入口兼容。
4. 已新增结构测试，固定核心模型类的专用模块落点。
5. 已通过定向回归：`tests/test_structure_guards.py`、`tests/test_runtime_dry_run.py`、`tests/test_runtime_apply_phase.py`、`tests/test_web_storage.py`、`tests/test_external_integrations.py`、`tests/test_lifecycle_workbench.py`、`tests/test_config_release.py`、`tests/test_source_providers.py`、`tests/test_sync_dispatch.py`、`tests/test_runtime_connectors.py`、`tests/test_job_diff.py`、`tests/test_data_quality_center.py`、`tests/test_typed_settings.py`、`tests/test_v1_smoke_path.py`、CLI 专项测试。

已完成 `Batch 7`：

1. 已新增 `sync_app/modules/` bounded context 入口，并预留 `sync_app/modules/sspr/`。
2. 已新增 `docs/architecture/bounded-context-entrypoints.md`，明确 SSPR、HR source provider、target provider 的接入边界。
3. 已将 target provider factory 下沉到 `sync_app/providers/target/registry.py`，让 target adapter 不再承载 registry 职责。
4. 已新增 `tests/test_architecture_boundaries.py`，覆盖层级依赖、target provider registry、新功能入口约定。
5. 已通过定向回归：`tests/test_architecture_boundaries.py`、`tests/test_target_providers.py`、`tests/test_structure_guards.py`。
