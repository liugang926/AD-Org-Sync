# 第四阶段迁移契约：身份治理 / Phase 4 Migration Contract: Identity Governance

## 产品目标 / Product goal

将身份候选、绑定对账、冲突决策、人工覆盖和例外规则拆成五个按用户任务定义的页面。日常列表最多八个主要列；Candidate、当前绑定、最近 Dry Run、最近 Apply 与当前 AD 状态统一进入身份时间线。

Split candidate review, binding reconciliation, conflict decisions, manual overrides, and exception rules into five task-oriented pages. Daily lists use no more than eight primary columns. Candidate, current binding, latest Dry Run, latest Apply, and current AD state share one identity timeline.

## 页面变化 / Page changes

| 页面 / Page | 唯一职责 / Single responsibility | 主要 CTA / Primary CTA |
| --- | --- | --- |
| `/identity-governance/identity-matching` | 只读解释候选、置信度和证据；不写绑定、不更改 AD / Read-only candidate, confidence, and evidence review; never writes bindings or AD | 复核下一个候选 / Review Next Candidate |
| `/identity-governance/binding-reconciliation` | 比较持久化绑定与实时或缓存 AD 状态；清理仍走固定高风险流程 / Compare persisted bindings with live or cached AD state; cleanup keeps the fixed high-risk workflow | 扫描绑定差异 / Scan Binding Differences |
| `/identity-governance/conflicts` | 只处理无法自动决定的冲突 / Process only identities that cannot be decided automatically | 处理下一个冲突 / Process Next Conflict |
| `/identity-governance/manual-overrides` | 管理人工身份绑定与按用户部门放置覆盖 / Manage manual identity bindings and per-user department placement overrides | 新建人工覆盖 / New Manual Override |
| `/identity-governance/exception-rules` | 管理真正的用户、部门和组例外 / Manage true user, department, and group exceptions | 新建例外规则 / New Exception Rule |

详情抽屉支持键盘打开、Tab 焦点圈闭、Escape 关闭和焦点恢复；窄屏下占满可用宽度。技术字段、指纹、规则原因和任务标识不占用日常列表列数。

The detail drawer supports keyboard opening, Tab focus containment, Escape close, and focus restoration; it uses the available width on narrow screens. Technical fields, fingerprints, rule reasons, and job identifiers do not consume daily-list columns.

## 权限与安全影响 / Permission and security impact

- 身份匹配和绑定对账继续要求 `mappings.read`；冲突读取继续要求 `jobs.read`；人工决策继续要求 `mappings.write`；例外规则继续要求 `exceptions.read` / `exceptions.write`。
- 所有数据库修改仍是 POST，并继续经过既有 CSRF、组织上下文、RBAC 和审计服务。GET 页面只读取数据。
- Provider、Connector、组织隔离和审计载荷保持不变。canonical 写入口调用与旧入口相同的处理函数。
- AD 未验证、不可用或未知只会显示阻塞/警告，不会授权删除绑定。此阶段不调用真实 Apply，也不操作真实 AD 账号或生产绑定数据。

- Identity Matching and Binding Reconciliation keep `mappings.read`; conflict reads keep `jobs.read`; manual decisions keep `mappings.write`; exception rules keep `exceptions.read` / `exceptions.write`.
- Every database mutation remains POST-only and continues through existing CSRF, organization context, RBAC, and audit services. GET pages are read-only.
- Provider, Connector, tenant isolation, and audit payload boundaries remain unchanged. Canonical write routes call the same handlers as legacy routes.
- Unverified, unavailable, or unknown AD state only produces a blocker or warning and never authorizes binding deletion. This phase does not invoke a real Apply or modify real AD accounts or production bindings.

## 兼容方案 / Compatibility plan

| 旧入口 / Legacy entry | canonical 入口 / Canonical entry | 兼容行为 / Compatibility behavior |
| --- | --- | --- |
| `/mappings` 及其导入、导出、绑定、启停、部门覆盖 POST | `/identity-governance/manual-overrides` 及对应子路径 | 旧页面继续显示全部历史绑定；新页面只显示人工绑定和部门覆盖；共用写服务、CSRF、权限与审计 / Legacy page keeps all historical bindings; canonical page shows manual bindings and department overrides only; both share write services, CSRF, permissions, and audit |
| `/conflicts`、decision guide 与所有决策 POST | `/identity-governance/conflicts` | 查询参数和返回筛选保留在进入时使用的 URL 空间 / Query parameters and return filters stay in the URL space used to enter the flow |
| `/exceptions` 及导入、导出、启停、删除 POST | `/identity-governance/exception-rules` | 旧 URL 保留；canonical 治理摘要只统计例外规则 / Legacy URLs remain; canonical governance summary counts exception rules only |

回滚时可单独撤销第四阶段提交：旧路由与旧模板仍可工作，数据库 schema 无变化，不需要数据迁移或回填。

Rollback can revert the Phase 4 commit independently: legacy routes and templates remain usable, the database schema is unchanged, and no data migration or backfill is required.

## 测试证据要求 / Required test evidence

- 单元：置信度判定、人工覆盖优先级、冲突和缺失候选的失败关闭行为。
- 集成：五个 canonical 页面权限、组织隔离、八列/七列表格、canonical 与 legacy 查询兼容、POST CSRF、审计和 canonical 返回地址。
- 浏览器：详情抽屉五阶段时间线、焦点进入、Tab 圈闭、Escape 关闭、焦点恢复、桌面和窄屏回归。
- 全量：格式、类型、单元/集成、浏览器、迁移、wheel、容器、SBOM 和 Windows 等仓库 CI 等价检查。

- Unit: confidence assessment, manual-override precedence, and fail-closed conflict/missing-candidate behavior.
- Integration: permissions and tenant isolation for all five canonical pages, eight/seven-column tables, canonical/legacy query compatibility, POST CSRF, audit, and canonical return locations.
- Browser: five-stage timeline in the detail drawer, focus entry, Tab containment, Escape close, focus restoration, desktop and narrow-screen regression.
- Full suite: repository-equivalent formatting, typing, unit/integration, browser, migration, wheel, container, SBOM, and Windows checks.

## 回滚说明 / Rollback

1. 停止第四阶段分支或撤销该 PR 的 merge commit。
2. 保留数据库和快照；本阶段没有 schema 或生产数据写入。
3. 用户可继续使用 `/mappings`、`/conflicts` 和 `/exceptions`。
4. 验证登录、旧列表读取、CSRF 拒绝、审计读取和健康检查。

1. Stop the Phase 4 branch or revert this PR's merge commit.
2. Keep the database and snapshots; this phase has no schema or production-data write.
3. Users can continue with `/mappings`, `/conflicts`, and `/exceptions`.
4. Verify login, legacy list reads, CSRF rejection, audit reads, and health endpoints.
