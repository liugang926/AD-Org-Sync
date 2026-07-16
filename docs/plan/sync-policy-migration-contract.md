# 第五阶段迁移契约：同步策略 / Phase 5 Migration Contract: Sync Policies

## 产品目标 / Product goal

将同步范围、账号命名、属性映射、部门与 OU 路由、组规则、生命周期和安全门禁拆成七个按管理员任务定义的页面。配置页只产生组织级或连接器级策略，不展示运营队列，不启动 Dry Run 或 Apply，也不直接写入 AD。

Split scope, account naming, attribute mappings, department and OU routing, group rules, lifecycle, and safety gates into seven administrator-task pages. Configuration pages create organization- or connector-scoped policy only. They do not show operational queues, start Dry Run or Apply, or write directly to AD.

## 页面职责 / Page responsibilities

| 页面 / Page | 唯一职责 / Single responsibility | 主要 CTA / Primary CTA |
| --- | --- | --- |
| `/sync-policies/scope` | 固定组织、连接器根部门、部门与源身份边界 / Fix the organization, connector roots, departments, and source identities eligible for planning | 保存同步范围 / Save Sync Scope |
| `/sync-policies/account-naming` | 定义源级或连接器级命名、动态源字段、冲突规则与只读预览 / Define source- or connector-level naming, dynamic source fields, collision rules, and read-only preview | 保存账号命名 / Save Account Naming |
| `/sync-policies/attribute-mappings` | 定义字段方向、转换、替换和受控回写 / Define field direction, transformation, replacement, and controlled write-back | 保存属性映射 / Save Attribute Mapping |
| `/sync-policies/department-ou-routing` | 将精确部门或子树路由到连接器 OU / Route an exact department or subtree to a connector OU | 保存路由规则 / Save Routing Rule |
| `/sync-policies/group-rules` | 定义受管组类型、邮件域、OU 和源标签/群聊纳入规则 / Define managed group type, mail domain, OU, and source tag/chat inclusion | 保存组规则 / Save Group Rules |
| `/sync-policies/lifecycle` | 定义入职、合同到期、离职宽限、返聘和重放策略 / Define joiner, contract expiry, offboarding grace, rehire, and replay policy | 保存生命周期策略 / Save Lifecycle Policy |
| `/sync-policies/security` | 定义多连接器路由、停用熔断、审批和身份认领门禁 / Define multi-connector routing, disable circuit breaker, approvals, and identity-claim gates | 保存安全策略 / Save Security Policy |

每页持续显示当前发布状态、阻塞原因和下一步。日常表格最多八列；连接密文、转换证据和运行时绑定进入连接器页、折叠证据或发布差异。账号创建准备保留在身份匹配页，验证后只形成精确 Dry Run 范围，不创建 AD 账号。

Every page continuously shows publication status, blocking reason, and next step. Daily tables use at most eight columns. Connector secrets, transformation evidence, and runtime bindings stay in Connectors, collapsed evidence, or release diffs. Account-creation preparation remains available from Identity Matching and only creates an exact Dry Run scope after verification; it does not create an AD account.

## 数据与服务归属 / Data and service ownership

| 策略对象 / Policy object | 持久化边界 / Persistence boundary | 更新规则 / Update rule |
| --- | --- | --- |
| 组织级属性、组、生命周期、安全开关 | `AdvancedSyncPolicySettings` + organization-scoped settings repository | 仅合并当前页面允许的字段；其他策略段保持原值 / Merge only fields owned by the current page; preserve every other section |
| 连接器范围、命名、组、停用 OU、非密文密码策略 | `SyncConnectorRecord` + connector repository | 完整保留连接地址、用户名、密文、其他策略和启用状态 / Preserve connection endpoints, usernames, secrets, unrelated policy, and enabled state |
| 属性映射 | organization-scoped attribute mapping repository | 保留现有方向、转换模式和 Connector 边界 / Keep existing direction, transformation mode, and Connector boundary |
| 部门与 OU 路由 | organization-scoped department OU mapping repository | 精确按组织和连接器验证 / Validate exact organization and Connector ownership |
| 配置发布 | 既有 config release service 与 snapshot repository | canonical 与旧入口共用发布、差异、下载和回滚服务 / Canonical and legacy routes share publish, diff, download, and rollback services |

本阶段不修改数据库 schema，不迁移或回填生产数据。分区更新服务拒绝跨页面字段，避免保存一个策略时重置连接密码、同步范围或其他策略。

This phase changes no database schema and migrates or backfills no production data. The sectional update service rejects cross-page fields so saving one policy cannot reset connector passwords, scope, or another policy.

## 权限与安全影响 / Permission and security impact

- 七个页面读取继续要求 `config.read`，所有持久化继续要求 `config.write`、POST 和有效 CSRF；GET 与命名预览不修改数据库。
- 每个 canonical 策略写入记录组织、操作者、策略段、目标类型、目标标识和非密文变更摘要。Provider、Connector、Organization、RBAC 与审计边界保持不变。
- 配置发布与回滚复用现有组织隔离。回滚继续使用统一高风险确认，显示组织、环境、快照版本与影响数；未标记环境拒绝执行。
- 生命周期页只定义计划策略，运营事项仍进入生命周期队列。安全页不能绕过环境标记、快照新鲜度、计划审批或 Apply 确认。
- AD 不可用、验证未知或连接失败不删除绑定。本阶段不调用真实 Apply，不操作真实 AD 账号或真实生产绑定。

- The seven pages keep `config.read` for reads. Every persistence action keeps `config.write`, POST, and valid CSRF. GET and naming preview do not mutate the database.
- Every canonical policy write audits organization, actor, section, target type, target ID, and a non-secret change summary. Provider, Connector, Organization, RBAC, and audit boundaries remain unchanged.
- Configuration publish and rollback reuse existing tenant isolation. Rollback keeps the unified high-risk confirmation with organization, environment, snapshot version, and impact count, and is rejected in an unlabeled environment.
- Lifecycle defines planning policy only; operational work remains in Lifecycle Queue. Security policy cannot bypass environment labels, snapshot freshness, plan approval, or Apply confirmation.
- Unavailable AD, unknown verification, or connection failure never deletes a binding. This phase does not invoke real Apply or modify real AD accounts or production bindings.

## 兼容方案 / Compatibility plan

| 旧入口 / Legacy entry | canonical 入口 / Canonical entry | 兼容行为 / Compatibility behavior |
| --- | --- | --- |
| `/advanced-sync` | 七个 `/sync-policies/*` 页面 | 旧综合页面与全部旧 POST 保留；`/sync-policies` 只读 307 到范围页 / The legacy monolith and all old POST routes remain; `/sync-policies` is a read-only 307 to Scope |
| `/source-directory/scope` | `/sync-policies/scope` | 共用处理器、组织范围、CSRF 和审计；省略旧命名字段时保留现值 / Shared handler, tenant scope, CSRF, and audit; omitted legacy naming fields preserve current values |
| `/advanced-sync/username-preview` | `/sync-policies/account-naming/preview` | 共用只读预览服务；不预留名称、不写 AD / Shared read-only preview; no name reservation or AD write |
| `/advanced-sync/mappings*` | `/sync-policies/attribute-mappings*` | 旧增删入口保留；canonical 返回 canonical 页面 / Legacy create/delete routes remain; canonical requests return to canonical pages |
| `/advanced-sync/department-ou-mappings*` | `/sync-policies/department-ou-routing*` | 旧增删入口保留；共用组织校验和审计 / Legacy create/delete routes remain with shared organization validation and audit |
| `/config/releases*` | `/sync-policies/releases*` | 共用发布、回滚和下载服务；canonical 表单保持 canonical action / Shared publish, rollback, and download service; canonical forms keep canonical actions |
| 原同步范围中的账号创建准备 / Creation preparation formerly shown in Scope | `/identity-governance/identity-matching` | 已验证缺失候选仍可形成精确 Dry Run 范围；同步范围不再展示 AD 验证或创建资格 / Verified missing candidates can still form an exact Dry Run scope; Scope no longer displays AD verification or creation eligibility |

## 测试证据要求 / Required test evidence

- 单元：策略段白名单、跨段拒绝、连接器密文和无关策略保留、动态源字段命名。
- 集成：七页单一主要 CTA、最多八列、状态/阻塞/下一步、权限、CSRF、组织隔离、审计、旧 URL、配置发布与回滚。
- 浏览器：390、768、1024、1366、1440 宽度，策略标签键盘焦点、窄屏横向滚动、账号命名只读预览、发布中心 canonical action。
- 全量：格式、类型、单元/集成、浏览器、迁移、wheel、容器、SBOM、Windows 与仓库 CI 等价检查。

- Unit: section allowlists, cross-section rejection, connector secret and unrelated-policy preservation, and dynamic-source-field naming.
- Integration: one primary CTA and at most eight columns across all seven pages, status/blocker/next-step guidance, permissions, CSRF, tenant isolation, audit, legacy URLs, and configuration publish/rollback.
- Browser: widths 390, 768, 1024, 1366, and 1440; keyboard focus on policy tabs, narrow-screen horizontal scrolling, read-only naming preview, and canonical release actions.
- Full: repository-equivalent formatting, typing, unit/integration, browser, migrations, wheel, container, SBOM, and Windows checks.

## 回滚说明 / Rollback

1. 停止第五阶段分支或撤销该 PR 的 merge commit。
2. 保留数据库、配置快照和连接器记录；本阶段没有 schema 或数据迁移。
3. 用户继续使用 `/advanced-sync`、`/source-directory/scope` 和 `/config/releases`；这些旧入口未删除。
4. 验证旧综合页读取、旧 POST CSRF、组织隔离、发布历史、回滚高风险门禁、审计读取和健康检查。

1. Stop the Phase 5 branch or revert this PR's merge commit.
2. Keep the database, configuration snapshots, and connector records; this phase has no schema or data migration.
3. Users can continue with `/advanced-sync`, `/source-directory/scope`, and `/config/releases`; these legacy entries are not removed.
4. Verify legacy page reads, legacy POST CSRF, tenant isolation, release history, rollback high-risk gates, audit reads, and health endpoints.
