# AD Org Sync 管理后台信息架构重构路线图

> 状态：前四阶段已合并、CI 通过并部署；第五阶段（同步策略拆分）正在功能分支实施
> 约束：不操作真实 AD 账号，不修改真实生产绑定数据，不在 GET 请求中写入数据。

## 1. 当前信息架构清单

当前侧边栏按技术能力和历史演进分为四组，而不是按管理员任务分组。

| 当前分组 | 页面 / 旧 URL | 当前职责 | 主要 capability |
| --- | --- | --- | --- |
| Run Monitoring | Dashboard `/dashboard` | 控制塔、阻塞项、Dry Run 新鲜度、Apply 门禁、预检、最近任务 | `dashboard.read` |
| Run Monitoring | Jobs `/jobs` | Dry Run、Apply、计划审核入口、运行状态、任务历史 | `jobs.read` / `jobs.run` / `jobs.review` |
| Run Monitoring | Automation Center `/automation-center` | 通知策略、计划执行门禁、失败提醒、最近高风险任务 | `config.read` / `config.write` |
| Run Monitoring | Audit `/audit` | 全局及组织审计查询 | `audit.read` |
| Identity Governance | Conflicts `/conflicts` | 冲突列表、推荐、手工绑定、跳过、驳回、批量处置 | `jobs.read` / `mappings.write` |
| Identity Governance | Mappings `/mappings` | 身份绑定、部门覆盖、导入导出、规则治理 | `mappings.read` / `mappings.write` |
| Identity Governance | Data Quality `/data-quality` | 质量快照、问题趋势、修复清单 | `config.read` / `config.write` |
| Identity Governance | Lifecycle `/lifecycle` | 入职、合同到期、离职、重放队列 | `config.read` / `config.write` |
| Identity Governance | Exceptions `/exceptions` | 例外规则、导入导出、启停和删除 | `exceptions.read` / `exceptions.write` |
| Rule Configuration | Config `/config` | 源连接、AD 连接、安全、范围、运行参数、Web/品牌配置、发布入口 | `config.read` / `config.write` |
| Rule Configuration | Source Directory `/source-directory` | 连接测试、刷新、筛选、AD 验证、过期绑定清理、同步范围、账号创建准备、身份关系证据 | `config.read` / `config.write` / `mappings.write` |
| Rule Configuration | Advanced Sync `/advanced-sync` | 功能开关、账号命名、连接器、属性映射、OU 路由、生命周期与组队列 | `config.read` / `config.write` |
| Platform Management | Integrations `/integrations` | API token、Webhook 订阅、投递重试、外部任务/冲突信号 | `config.read` / `config.write` |
| Platform Management | Organizations `/organizations` | 组织管理、选择、配置包导入导出 | `organizations.manage` |
| Platform Management | Database `/database` | 完整性检查、备份 | `database.read` / `database.manage` |
| Platform Management | Account `/account` | 当前管理员密码 | `account.manage` |
| 未进入正式导航 | Users `/users` | 管理员与角色 | `users.manage` |
| 未进入正式导航 | Config Releases `/config/releases` | 配置快照、比较、发布、回滚 | `config.read` / `config.write` |
| 未进入正式导航 | Getting Started `/getting-started` | 上手步骤和实时预检 | 登录用户 |
| 公共轻量入口 | SSPR `/sspr` | 员工自助密码重置 | 独立 SSPR 安全边界 |

### 已确认的结构性问题

- Source Directory 的身份关系表由 1 个选择列、7 个来源/候选列、5 个同步前列、5 个 Dry Run 列、6 个 Apply 结果列和 3 个差异/风险列组成，共 27 个业务列（另含选择列）。
- Dashboard、Jobs 和 Automation Center 同时解释 Dry Run、Apply 门禁和运行状态；Config 也直接提供首次 Dry Run。
- Config 与 Advanced Sync 同时承载连接器、策略和系统设置；Advanced Sync 还展示生命周期、重放和组绑定队列。
- Source Directory、Mappings、Conflicts 和 Data Quality 都以不同形式展示身份问题，但没有统一的“一个用户、一条时间线”。
- 基础模式主要通过隐藏高级导航降低入口数量，页面本体仍保持高级复杂度。
- `Recently Used` 克隆正式导航链接，克隆时会继承 `active` 与 `aria-current`，可能产生双高亮和两个“当前页面”。

## 2. 功能重复矩阵

| 能力 | 当前重复位置 | 产生的问题 | 目标唯一归属 |
| --- | --- | --- | --- |
| 全局状态与下一步 | Dashboard、Jobs、Getting Started | 多处给出不同下一步 | 控制塔只汇总状态并跳向唯一下一步 |
| Dry Run 启动 | Jobs、Config、Getting Started | 配置页与执行页边界不清 | 执行中心 / Dry Run |
| Apply 门禁与启动 | Dashboard、Jobs、Automation Center | 状态、策略、执行混在一起 | 执行中心 / Apply；控制塔只展示摘要 |
| 计划审核 | Jobs、Automation Center、外部集成回调 | 审核记录和调度门禁混合 | 执行中心 / 计划审核 |
| 任务状态与历史 | Dashboard、Jobs、Automation Center、Integrations | 相同任务被多次列表化 | 执行中心 / 任务历史；其他页只显示摘要链接 |
| 源连接配置 | Config、Source Directory、Advanced Sync | 配置、测试与目录浏览混合 | 数据源 / 连接器 |
| 源目录刷新与快照 | Source Directory、Data Quality | 当前目录与历史质量快照割裂 | 数据源 / 源目录、快照历史 |
| 数据质量 | Source Directory、Advanced Sync、Data Quality | 即时分析、快照和身份风险重复 | 数据源 / 数据质量 |
| 身份候选与现状 | Source Directory、Mappings、Conflicts | 候选、绑定、冲突分散 | 身份治理 / 身份匹配与绑定对账 |
| 手工身份决策 | Mappings、Conflicts、Source Directory | 创建、调整和清理入口分散 | 身份治理 / 人工覆盖；冲突队列只处理决策 |
| 同步范围 | Config、Source Directory、Advanced Sync connector roots | 组织范围、连接器范围和临时选择混用 | 同步策略 / 同步范围 |
| 账号命名与创建准备 | Source Directory、Advanced Sync | 预览和保存策略不在同一上下文 | 同步策略 / 账号命名；执行前影响进入 Dry Run |
| 属性与 OU 路由 | Config、Advanced Sync、Mappings | 默认策略、规则和个人覆盖混合 | 同步策略 / 属性映射、部门与 OU 路由；个人覆盖归身份治理 |
| 生命周期 | Advanced Sync、Lifecycle、Jobs | 策略、待办和执行结果混合 | 同步策略 / 生命周期策略；运营中心 / 生命周期队列 |
| 通知与 Webhook | Config、Automation Center、Integrations | 通知配置、调度和外部投递混合 | 运营中心 / 通知；自动化与计划任务单独负责调度 |
| 系统设置 | Config、Organizations、Database、Account/Users | 组织级与部署级设置混合 | 系统管理各业务对象页面 |

## 3. 目标路由与页面职责

所有页面遵循：一个主要 CTA；始终展示当前状态、阻塞原因和下一步；高级证据进入详情抽屉、折叠区或高级模式。

| 分区 | 目标路由 | 页面唯一职责 | 主要 CTA |
| --- | --- | --- | --- |
| 概览 | `/overview/control-tower` | 汇总当前组织、环境、快照、执行门禁和首要阻塞；不直接执行 Apply | 执行“下一步” |
| 数据源 | `/data-sources/connectors` | 管理 Provider/Connector 连接参数和连接测试 | 保存连接器 |
| 数据源 | `/data-sources/source-directory` | 浏览当前源快照中的部门和用户；基础列表不超过 8 列 | 刷新目录 |
| 数据源 | `/data-sources/snapshots` | 比较源快照、查看刷新来源和保留状态 | 查看最新快照 |
| 数据源 | `/data-sources/data-quality` | 扫描源数据质量并分派修复清单 | 运行质量扫描 |
| 身份治理 | `/identity-governance/identity-matching` | 查看自动匹配候选、证据和置信度 | 审核待匹配身份 |
| 身份治理 | `/identity-governance/binding-reconciliation` | 对比当前绑定与 AD 实际状态；清理必须走高风险五步流 | 扫描绑定差异 |
| 身份治理 | `/identity-governance/conflicts` | 处理无法自动决策的身份冲突 | 处理下一个冲突 |
| 身份治理 | `/identity-governance/manual-overrides` | 管理人工身份/部门覆盖及复核期限 | 新建人工覆盖 |
| 身份治理 | `/identity-governance/exception-rules` | 管理真正的例外规则 | 新建例外规则 |
| 同步策略 | `/sync-policies/scope` | 定义组织、连接器、部门和用户同步范围 | 保存同步范围 |
| 同步策略 | `/sync-policies/account-naming` | 定义账号命名、冲突和创建规则 | 保存命名策略 |
| 同步策略 | `/sync-policies/attribute-mappings` | 定义字段方向、变换和写回策略 | 保存属性映射 |
| 同步策略 | `/sync-policies/department-ou-routing` | 定义部门到 OU 路由 | 保存路由规则 |
| 同步策略 | `/sync-policies/group-rules` | 定义组同步、保护和排除策略 | 保存组规则 |
| 同步策略 | `/sync-policies/lifecycle` | 定义入转离、宽限期和恢复策略 | 保存生命周期策略 |
| 同步策略 | `/sync-policies/security` | 定义断路器、审批和受保护对象策略 | 保存安全策略 |
| 执行中心 | `/execution-center/dry-run` | 基于固定快照生成无写入计划 | 运行 Dry Run |
| 执行中心 | `/execution-center/plan-review` | 审核计划、风险和影响范围 | 批准计划 |
| 执行中心 | `/execution-center/apply` | 显示组织、环境、快照和影响数并执行已批准计划 | Apply 已批准计划 |
| 执行中心 | `/execution-center/jobs` | 查询任务历史、阶段、日志和结果 | 查看最新任务 |
| 运营中心 | `/operations-center/lifecycle` | 处理生命周期运营队列 | 处理选中项 |
| 运营中心 | `/operations-center/automation` | 管理自动化和计划任务 | 保存计划任务 |
| 运营中心 | `/operations-center/notifications` | 管理通知渠道、Webhook 与失败投递 | 保存通知配置 |
| 运营中心 | `/operations-center/audit` | 查询不可变审计证据 | 查询审计日志 |
| 系统管理 | `/system-management/organizations` | 管理组织及配置包 | 新建组织 |
| 系统管理 | `/system-management/administrators` | 管理管理员、角色与权限 | 新建管理员 |
| 系统管理 | `/system-management/employee-self-service` | 管理 SSPR 可用性、身份源与安全策略；不承载员工实际重置 | 保存自助服务设置 |
| 系统管理 | `/system-management/database` | 数据库检查、备份和恢复准备 | 创建备份 |
| 系统管理 | `/system-management/branding` | 管理名称、标记和外观 | 保存品牌设置 |
| 系统管理 | `/system-management/deployment` | 管理环境标记、公开地址和部署级设置 | 保存部署设置 |

### 身份时间线

身份详情使用同一时间线表达：

1. Candidate：来源字段、规则、置信度与候选 AD 账号；
2. Current binding：当前持久化绑定、来源、启用状态与最后复核；
3. Dry Run：计划身份、操作、任务、快照与新鲜度；
4. Apply：实际身份、结果、任务和完成时间；
5. Current AD state：实时验证结果、验证时间和不可用/未知原因。

基础列表只保留身份、来源、当前绑定、匹配状态、最近计划、当前 AD 状态、风险、下一步八列；其余证据进入身份详情。

## 4. 分阶段实施计划

每个阶段对应一个小型 Draft PR。上一个 PR 的 GitHub CI 全部成功并由维护者回复“确认合并”后，才进入下一阶段；Codex 不自行合并。

### PR 1：信息架构地基（本阶段）

- 建立目标分区导航与过渡规范 URL；保留全部旧 GET URL。
- 基础模式只显示 Control Tower、Source Directory、Conflict Queue、Run Review（审计员额外显示 Audit Logs），不超过 5 个日常入口。
- 修复 Recently Used 与正式导航双高亮及 `aria-current` 重复。
- 不改数据库、同步服务、写接口或 AD 行为。
- 测试：路由注册、RBAC 可见性、双语键、桌面/窄屏导航与最近使用回归。

### PR 2：高风险操作安全地基

实现与验收契约见 [`high-risk-operation-safety-contract.md`](high-risk-operation-safety-contract.md)。

- 建立统一“扫描—预览—确认—执行—审计”组件和服务契约。
- 所有高风险确认显示组织、环境、快照版本和影响数量。
- 未标记环境阻止 Apply、绑定清理和其他破坏性动作。
- 明确 AD 不可用、验证未知、连接失败均不得删除绑定。
- 测试：权限、CSRF、组织隔离、未知状态不删除、审计完整性。

### PR 3：数据源拆分

- 拆出 Connectors、Source Directory、Snapshot History、Data Quality。
- Source Directory 基础表降至最多 8 列；连接测试、清理、范围和账号创建移出。
- 保留 `/config`、`/source-directory`、`/data-quality` 兼容入口。
- 测试：Provider/Connector、多组织快照、筛选、窄屏和键盘回归。

### PR 4：身份治理与身份时间线

- 拆出 Identity Matching、Binding Reconciliation、Conflicts、Manual Overrides、Exception Rules。
- 为一个用户统一 Candidate、Binding、Dry Run、Apply、AD state 时间线。
- 保留 `/mappings`、`/conflicts`、`/exceptions` 查询参数兼容。
- 测试：候选证据、人工覆盖优先级、无误删、详情抽屉焦点管理。

### PR 5：同步策略拆分

实现与验收契约见 [`sync-policy-migration-contract.md`](sync-policy-migration-contract.md)。

- 将 Config/Advanced Sync 拆为范围、命名、属性、OU、组、生命周期、安全策略。
- 配置草稿、预览、发布和回滚仍使用现有组织隔离与审计边界。
- 测试：配置 round-trip、发布快照、回滚、Provider/Connector 边界。

### PR 6：执行中心拆分

- 拆出 Dry Run、Plan Review、Apply、Job History，消除 Dashboard/Jobs/Automation 重复。
- Apply 只接受同组织、同环境、同快照、未过期且已批准的计划。
- 测试：状态机、计划指纹、过期计划、重复提交、浏览器完整流程。

### PR 7：运营中心与系统管理收口

- 拆出生命周期、自动化、通知、审计，以及组织、管理员、自助服务、数据库、品牌、部署设置。
- 所有时间按用户本地时区显示，同时提供相对时间和原始时间。
- 完成旧 URL 重定向策略、运维手册和最终浏览器矩阵。

## 5. 风险与兼容方案

### 第一阶段兼容映射

第一阶段使用同一只读页面处理器同时服务新旧 GET URL，不改变 POST action、查询参数、RBAC、组织上下文或审计行为。

| 旧 URL | 第一阶段规范 URL | 说明 |
| --- | --- | --- |
| `/dashboard` | `/overview/control-tower` | 一对一兼容 |
| `/config` | `/data-sources/connectors` | 过渡入口；页面拆分在 PR 3/5 |
| `/source-directory` | `/data-sources/source-directory` | 一对一兼容 |
| `/data-quality` | `/data-sources/data-quality` | 一对一兼容 |
| `/conflicts` | `/identity-governance/conflicts` | 一对一兼容 |
| `/mappings` | `/identity-governance/manual-overrides` | 过渡入口；绑定对账在 PR 4 拆分 |
| `/exceptions` | `/identity-governance/exception-rules` | 一对一兼容 |
| `/advanced-sync` | `/sync-policies` | 过渡入口；策略子页在 PR 5 拆分 |
| `/jobs` | `/execution-center/run-review` | 过渡入口；四个执行页面在 PR 6 拆分 |
| `/lifecycle` | `/operations-center/lifecycle-queue` | 一对一兼容 |
| `/automation-center` | `/operations-center/automation` | 一对一兼容 |
| `/integrations` | `/operations-center/notifications` | 过渡入口；API 与通知在 PR 7 收口 |
| `/audit` | `/operations-center/audit-log` | 一对一兼容 |
| `/organizations` | `/system-management/organizations` | 一对一兼容 |
| `/users` | `/system-management/administrators` | 一对一兼容 |
| `/database` | `/system-management/database` | 一对一兼容 |
| `/account` | `/system-management/account` | 兼容个人账户入口 |

### 主要风险

| 风险 | 控制措施 | 回滚方式 |
| --- | --- | --- |
| 收藏夹、内部链接或外部文档仍使用旧 URL | 旧 GET URL 保持可用；JS 将旧 Recently Used 路径归一到规范 URL | 恢复旧导航模板，路由别名可无害保留 |
| 新导航改变可见入口 | 服务端继续按原 capability 过滤；基础模式只收敛入口，不撤销直接访问权限 | 恢复静态导航分组 |
| 路由别名绕过 RBAC | 新旧 URL 绑定同一处理函数，权限检查只执行一次且完全相同 | 删除规范 GET 装饰器 |
| 页面暂时仍含混合职责 | 规范 URL 标记为“过渡入口”，按后续 PR 拆分，不在本阶段复制服务逻辑 | 无数据迁移，可直接回滚 UI |
| 高风险门禁现状未完全满足目标 | PR 2 优先完成环境标记和五步流；在此之前不扩大或新增高风险入口 | 保持现有 POST/CSRF/RBAC/审计路径 |
| 中英文遗漏 | 导航词条进入双语 catalog，并以 key 集合测试约束 | 回滚词条与导航数据 |

### 不变的安全边界

- Organization、Provider、Connector、RBAC 和审计 repository 边界保持不变。
- 所有已有数据库写入仍使用 POST、CSRF 和 capability 检查；本阶段没有新增写接口。
- 规范 URL 仅增加 GET 入口，GET 不触发目录刷新、绑定清理、配置保存、任务启动或数据库写入。
- 不因 AD 不可用、验证未知或连接失败删除任何绑定。
- 不连接或操作真实 AD 账号，不修改真实生产绑定数据。
