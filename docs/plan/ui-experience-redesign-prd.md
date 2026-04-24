# AD Org Sync UI 体验重构 PRD

## 1. 文档信息

| 项目 | 内容 |
| :--- | :--- |
| 项目名称 | AD Org Sync |
| 文档类型 | UI/UX 体验重构 PRD |
| 当前版本 | v0.1 |
| 文档日期 | 2026-04-24 |
| 适用范围 | FastAPI Web 控制台、Jinja 模板、全局 CSS、页面级 JS、运维关键流程 |
| 当前状态 | 需求评审稿 |

## 2. 背景

AD Org Sync 已经从单一同步工具演进为多组织、策略驱动、带审批和治理能力的身份同步控制平面。功能密度快速增长后，当前 Web UI 的主要矛盾已经从“有没有功能”变成“管理员能不能安全、快速、有信心地做决策”。

当前 UI 可以完成基础操作，但整体体验仍像“功能页面集合”，没有形成围绕上线安全、每日运维、冲突决策、配置发布的清晰操作系统。用户对现状不满意是合理的，因为产品价值已经升级，但界面表达还停留在基础管理后台阶段。

## 3. 现状分析

### 3.1 视觉效果问题

| 问题 | 现象 | 影响 |
| :--- | :--- | :--- |
| 视觉语言普通 | 深色侧边栏 + 白色卡片 + Indigo 主按钮，缺少产品独特性 | 看起来像通用后台模板，无法承载“高风险同步控制台”的专业感 |
| 卡片滥用 | Dashboard、Jobs、Config 等页面大量白卡堆叠 | 所有信息权重接近，用户难以判断先看哪里 |
| 状态表达单薄 | 主要依赖 badge 和文本说明 | 高风险、阻断、可执行、待审批等状态不够醒目 |
| 内容留白失衡 | Config 页首屏左侧文字列过窄，纵向页面过长 | 信息阅读效率低，用户被迫滚动寻找关键动作 |
| hover 动效不合适 | 所有 `.card:hover` 都上浮 | 运维系统的静态信息卡不应像营销卡片一样飘动，容易造成噪音 |
| 字体气质偏默认 | 使用 Segoe UI / 系统字体，层级靠粗细堆出来 | 缺少现代控制台的精密、可信和清晰质感 |

### 3.2 信息架构问题

| 问题 | 现象 | 影响 |
| :--- | :--- | :--- |
| 导航按功能追加 | Jobs、Conflicts、Config、Advanced Sync、Automation、Data Quality、Integrations、Lifecycle 等并列 | 管理员不知道当前阶段应该进哪个页面 |
| Basic / Advanced 不是真正降噪 | Basic 下仍保留多个任务入口，Advanced 只是显示更多页面 | 不能帮助新管理员完成上线路径，也不能帮助老管理员聚焦每日任务 |
| 任务流被拆散 | 配置、Dry Run、冲突、审批、Apply、回滚分散在不同页面 | 关键上线流程需要跨页面跳转，认知成本高 |
| 页面标题解释过多 | 多数页面都用长段描述解释“这个页面做什么” | 用户已经进入页面后仍要阅读说明，说明 IA 没有自解释 |
| 新功能缺少归属 | Integration、Lifecycle、Data Quality、Automation 都叫 Center/Workbench | 命名接近，但任务边界不清楚 |

### 3.3 交互形态问题

| 问题 | 现象 | 影响 |
| :--- | :--- | :--- |
| 以表单提交为主 | 多数操作是同步 POST + flash | 没有足够的就地反馈、预览、撤销、二次确认上下文 |
| 高风险确认弱 | 主要使用浏览器 confirm | 对 Apply、绑定 AD 账号、回滚配置等高风险操作不够专业 |
| 批量操作弱 | 多数表格有 checkbox 和按钮，但缺少选中数量、批量影响预览、固定操作栏 | 容易误操作，也不利于处理大量队列 |
| 表格阅读负担大 | 字段多、行内信息拥挤、缺少列优先级和详情抽屉 | 管理员需要横向扫很多列才能做判断 |
| 缺少状态时间线 | Job、配置发布、生命周期队列没有统一 timeline | 用户不知道“刚发生什么、下一步是什么、风险在哪一步” |
| 空状态偏文案化 | 空状态多为文字和按钮 | 没有把“下一步推荐动作”做成强引导 |

### 3.4 组件系统问题

| 问题 | 证据 | 影响 |
| :--- | :--- | :--- |
| 模板类名未落地 | 模板大量使用 `grid`、`cols-2`、`form-grid`、`form-group`、`subcard`、`section-header`、`hero-header`、`table-sm`、`table-empty`，但 `app.css` 未定义这些类 | 多个页面结构在代码里存在，但视觉样式没有真正生效 |
| CSS 有重复定义 | `nav a`、`header` 等样式重复定义 | 长期维护容易互相覆盖，改一处影响不可预测 |
| 变体体系不完整 | 按钮宏支持 `success`、`small` 等语义，但 CSS 只定义了部分变体，如 `.sm` 而不是 `.small` | 组件 API 和样式 API 不一致 |
| 全局选择器过重 | `button:not(...)`、`input, select, textarea` 等全局规则覆盖范围大 | 新组件很难做局部风格，容易靠 `!important` 修补 |
| 缺少设计 token 分层 | 颜色、阴影、半径有变量，但没有语义 token，如 danger-surface、interactive-border、panel-elevated | 难以形成一致的产品级视觉体系 |

### 3.5 页面级诊断

| 页面 | 当前问题 | 改造方向 |
| :--- | :--- | :--- |
| Dashboard | 状态卡过多，Start Here 和 Preflight 平铺，下一步动作不够强 | 改成 Control Tower：顶部只回答“是否可上线/下一步做什么/阻断原因” |
| Config | 页面过长，说明多，字段密度高，首屏不聚焦 | 改成配置向导 + 分区进度 + 右侧实时发布影响预览 |
| Jobs | Job Center 有 readiness，但执行历史和对比洞察不够突出 | 改成 Run Review：Dry Run / Apply / Diff / Approval Gate 四段式 |
| Conflicts | 冲突处理信息分散，推荐动作、绑定决策、结果预期没有成为主线 | 改成 Decision Wizard：候选账号对比、绑定后影响、不绑定后果 |
| Advanced Sync | 单页接近 1000 行模板，配置域过多 | 拆成策略页群或分段 stepper，按“账号、路由、生命周期、组、质量”组织 |
| Lifecycle | 队列表格可用，但没有每日任务工作台的节奏 | 改成四列工作台：未来入职、合同到期、离职缓冲、待重放 |
| Data Quality | 有趋势和导出能力，但缺少“修复优先级” | 改成运营看板：趋势、阻断项、HR 修复清单、源系统责任人 |
| Config Release | 功能方向正确，但发布流视觉不明显 | 改成发布流水线：Live Config -> Snapshot -> Promote -> Rollback |
| Integration | API 信息和 webhook 管理并列 | 改成集成门户：凭证、API Explorer、Webhook Delivery Monitor |

## 4. 产品定位

### 4.1 新定位

将 Web UI 从“AD 同步管理后台”升级为“身份同步安全上线控制台”。

### 4.2 设计关键词

| 关键词 | 设计含义 |
| :--- | :--- |
| 控制塔 | 一眼看到上线状态、阻断项、下一步动作 |
| 审计感 | 高风险动作有上下文、有确认、有记录 |
| 决策辅助 | 冲突、绑定、审批、回滚必须展示后果 |
| 运维节奏 | 每日打开能直接处理待办，而不是找页面 |
| 专业克制 | 不追求炫酷，追求清晰、稳定、可信 |

## 5. 用户角色与核心场景

| 角色 | 目标 | 关键问题 |
| :--- | :--- | :--- |
| 初次部署管理员 | 完成配置并安全跑通第一次 Apply | 不知道下一步做什么，不知道是否安全 |
| 日常运维管理员 | 每天检查 dry run、冲突、生命周期队列 | 需要快速定位阻断项和高风险变化 |
| 审批人 | 判断高风险同步计划是否可批准 | 需要看到本次和上次相比变化了什么 |
| AD 管理员 | 判断绑定现有 AD 账号是否安全 | 需要看到账号状态、OU、最近登录、将更新字段 |
| 审计员 | 追踪规则、审批、发布、回滚记录 | 需要时间线、责任人、原因、影响范围 |

## 6. 目标体验

### 6.1 北极星体验

管理员进入系统后，10 秒内应知道：

1. 当前组织是否可以安全执行 Apply。
2. 如果不能，最重要的阻断原因是什么。
3. 下一步应该点击哪里。
4. 高风险变化是否比上次变多。
5. 是否存在必须人工决策的账号、规则或生命周期项。

### 6.2 体验原则

| 原则 | 要求 |
| :--- | :--- |
| 先结论，后细节 | 页面顶部必须给状态结论和下一步动作 |
| 风险可视化 | 高风险、阻断、待审批、可回滚要有清晰视觉差异 |
| 渐进披露 | 不把所有字段一次性展开，默认显示决策所需信息 |
| 操作前预览 | 高风险操作必须展示影响摘要和确认上下文 |
| 批量有反馈 | 批量操作必须显示选中数量、影响范围、结果状态 |
| 中英文等价 | 中文不是 fallback，而是完整可读的管理员语言 |

## 7. 信息架构重构

### 7.1 推荐导航

| 一级导航 | 子页面 | 说明 |
| :--- | :--- | :--- |
| Overview | Control Tower、Getting Started | 当前状态、上线路径、下一步动作 |
| Run & Review | Runs、Run Detail、Dry Run Diff、Approvals | 运行、差异、审批、Apply Gate |
| Decisions | Conflicts、Same-Account Wizard、Lifecycle Workbench | 所有需要人工判断的事项 |
| Configuration | Setup、Advanced Policies、Release Center | 配置、策略、快照、发布、回滚 |
| Data Quality | Quality Center、Repair Exports | 源数据质量、趋势、修复清单 |
| Integrations | API Tokens、Webhooks、Delivery Logs | 外部 API、Webhook、回调 |
| Administration | Organizations、Users、Audit、Database | 平台管理和审计 |

### 7.2 Basic / Advanced 新定义

| 模式 | 展示策略 |
| :--- | :--- |
| Basic | 只展示 Control Tower、Setup、Runs、Conflicts、Lifecycle、Audit |
| Advanced | 展示策略治理、发布回滚、数据质量、集成、数据库等完整能力 |

Basic 不应只是隐藏侧边栏项，而应改变页面内容密度和推荐动作。

## 8. 核心页面需求

### 8.1 Control Tower

目标：替代当前 Dashboard，成为所有管理员的默认入口。

首屏模块：

| 模块 | 内容 |
| :--- | :--- |
| Readiness Banner | 当前组织状态：Ready / Needs Review / Blocked |
| Next Best Action | 一个主按钮，例如 Run First Dry Run、Resolve Conflicts、Approve Plan |
| Risk Delta | 本次 dry run 相比上次成功 dry run / apply 的新增风险 |
| Blocking Queue | 配置错误、开放冲突、待审批、数据质量阻断 |
| Timeline | 最近 dry run、审批、apply、回滚、通知失败 |

验收标准：

1. 首屏最多 1 个主 CTA。
2. 页面顶部不得出现 5 个以上同等权重统计卡。
3. 阻断原因必须按严重程度排序。
4. 每个阻断项必须有明确跳转目标。

### 8.2 Setup / Config Wizard

目标：把 `/config` 从长表单改成可完成的配置流程。

建议结构：

| 步骤 | 内容 |
| :--- | :--- |
| Source | 源系统、凭据、源范围 |
| Target AD | LDAP、根 OU、禁用 OU、组 OU |
| Account Policy | 用户名策略、密码策略、保护账号 |
| Safety | dry run gate、熔断、审批 |
| Review | 配置摘要、变更预览、保存后下一步 |

交互要求：

1. 左侧为步骤导航和完成度。
2. 中间为当前步骤表单。
3. 右侧为实时摘要和风险提示。
4. 保存前可预览变更。
5. 保存后提示下一步 dry run，而不是只显示“保存成功”。

### 8.3 Runs / Job Center

目标：把运行中心从“任务列表”升级为“执行决策页”。

核心模块：

| 模块 | 内容 |
| :--- | :--- |
| Apply Gate | 是否允许 Apply，为什么 |
| Run Actions | Run Dry Run、Run Apply、Schedule Mode |
| Diff Summary | 本次 vs 上次 dry run / apply |
| Risk Review | 新增高风险、审批状态、冲突变化 |
| Execution History | 任务列表和筛选 |

交互要求：

1. Apply 按钮在 Gate 不满足时必须 disabled，并显示原因。
2. Dry Run 完成后自动突出差异摘要。
3. Job Detail 默认显示“摘要 + 风险 + 差异”，原始日志折叠到后面。
4. 高风险审批必须用专用确认面板，不使用浏览器 confirm。

### 8.4 Same-Account Decision Wizard

目标：把同账户绑定做成真正的决策向导。

步骤：

| 步骤 | 内容 |
| :--- | :--- |
| 1. Source Identity | 源用户、部门、邮箱、工号、路由解释 |
| 2. Candidate AD Accounts | 候选 AD 账号对比、推荐理由、冲突原因 |
| 3. Target Account State | 启用状态、OU、最近登录、关键属性、已有绑定 |
| 4. Sync Impact | 如果绑定，下次同步更新哪些字段 |
| 5. Decision | 绑定 / 暂不绑定 / 跳过 / 驳回，并记录原因 |

验收标准：

1. 默认选中推荐账号，但必须显示推荐置信度和原因。
2. 如果目标账号已被其他源用户绑定，主按钮必须变为警告态或禁用。
3. 绑定前必须展示“是否创建新账号、冲突是否继续、将更新字段”。
4. 完成后回到冲突队列，并突出已处理项。

### 8.5 Lifecycle Workbench

目标：成为管理员每日打开的队列页。

推荐布局：

| 列 | 内容 |
| :--- | :--- |
| Future Onboarding | 即将入职、已到期入职、缺资料 |
| Contractor Expiry | 合同即将到期、已到期、待 sponsor 确认 |
| Offboarding Grace | 离职缓冲、到期禁用、经理通知 |
| Replay Queue | 待重放、失败重试、已跳过 |

交互要求：

1. 顶部显示今日待办数量和最紧急项。
2. 每列支持筛选：Due Now、Deferred、Manual Hold。
3. 批量操作有 sticky action bar。
4. 执行动作前显示影响范围和可撤销路径。

### 8.6 Data Quality Center

目标：从“扫描结果”升级为“长期运营看板”。

模块：

| 模块 | 内容 |
| :--- | :--- |
| Quality Trend | 缺邮箱、缺工号、重复项、部门异常趋势 |
| Blockers | 会阻断 dry run/apply 的问题 |
| Repair List | 可导出给 HR / 源系统管理员的清单 |
| Ownership | 问题归属：HR、源系统、IT |
| Snapshot History | 历史扫描和差异 |

验收标准：

1. 趋势图必须显示最近 6-12 次扫描。
2. 修复清单支持按问题类型导出。
3. 高风险命名策略问题必须单独置顶。

### 8.7 Release Center

目标：把配置快照、对比、回滚做成发布流。

推荐视图：

| 阶段 | 内容 |
| :--- | :--- |
| Live Config | 当前未发布变更 |
| Snapshot | 已发布版本 |
| Compare | 字段级差异 |
| Promote | 测试组织到生产组织 |
| Rollback | 回滚目标、影响摘要、安全备份 |

交互要求：

1. 回滚前必须显示当前配置安全备份会被创建。
2. 差异按“敏感、高风险、普通”分组。
3. 发布快照后展示版本号、操作者、原因。

## 9. 设计系统需求

### 9.1 Token

| 类型 | 要求 |
| :--- | :--- |
| Color | 建立 semantic tokens：surface, surface-raised, text-primary, risk-high, gate-blocked, action-primary |
| Typography | 建立 display、title、body、mono、caption 五级字体规范 |
| Spacing | 使用 4px/8px 基准，明确 page、section、card、field 间距 |
| Radius | 区分 control、card、panel、pill |
| Elevation | 管理后台减少 hover lift，只在弹层和浮动操作区使用阴影 |

### 9.2 必须补齐的组件

| 组件 | 说明 |
| :--- | :--- |
| AppShell | 侧边栏、顶栏、组织切换、模式切换 |
| PageHero | 页面结论、主动作、关键状态 |
| StatusBanner | Ready / Blocked / Needs Review |
| RiskCard | 高风险变化、审批、冲突 |
| Stepper | 配置向导和决策向导 |
| DataTable | 表格、筛选、批量选择、空状态 |
| DetailDrawer | 表格行详情 |
| ConfirmPanel | 高风险操作确认 |
| Timeline | Job、发布、审批、回滚事件 |
| EmptyState | 带下一步动作的空状态 |

### 9.3 需要立即修复的样式债

| 优先级 | 项目 |
| :--- | :--- |
| P0 | 为模板已使用但未定义的类补齐 CSS：`grid`、`cols-2`、`form-grid`、`form-group`、`subcard`、`section-header`、`hero-header`、`table-sm`、`table-empty`、`cell-*` |
| P0 | 统一按钮 size API，修复 `size="small"` 与 `.sm` 不一致 |
| P1 | 移除全局 `.card:hover` 上浮，改成 opt-in |
| P1 | 清理重复 `nav a` 和 `header` 定义 |
| P1 | 减少 `!important`，用组件层级解决颜色覆盖 |
| P2 | 增加移动端表格策略：关键列卡片化或详情抽屉 |

## 10. 视觉方向建议

### 10.1 推荐方向：Operational Clarity

一种偏“企业控制塔”的轻色系统：

| 维度 | 建议 |
| :--- | :--- |
| 主色 | 从高饱和 Indigo 改为深蓝灰 + 安全绿 + 风险橙红 |
| 背景 | 使用非常浅的冷灰、分层 surface，不使用强装饰 |
| 字体 | 英文使用清晰现代 sans，中文优先苹方/微软雅黑，数字和 job id 使用 mono |
| 信息密度 | Dashboard 低密度，表格页中高密度，决策页中密度 |
| 图形语言 | 使用状态轨道、时间线、风险刻度、差异条，而不是普通图标堆叠 |

### 10.2 不建议的方向

1. 不建议做纯暗黑科技风，会降低长时间运维阅读舒适度。
2. 不建议只换配色，因为当前问题主要在 IA 和组件系统。
3. 不建议引入复杂前端框架作为第一步，当前 Jinja + CSS 仍可支撑一次高质量重构。
4. 不建议继续堆页面，必须先统一任务流和组件系统。

## 11. 分阶段计划

### Phase 0: UI 基建止血

周期：1 周

范围：

1. 补齐缺失 CSS utility 和组件类。
2. 清理重复样式和危险全局选择器。
3. 统一按钮、表单、卡片、表格、badge API。
4. 建立 `docs/design/ui-system.md` 设计系统文档。

验收：

1. 模板中核心类名 95% 以上有 CSS 定义。
2. Dashboard、Jobs、Config、Conflicts、Advanced Sync 页面在 1440px 和 390px 下无明显布局破裂。
3. 所有按钮 variant 和 size 都能正确生效。

### Phase 1: Control Tower 与 Run Review

周期：2 周

范围：

1. 重构 Dashboard 为 Control Tower。
2. 重构 Jobs 为 Run Review。
3. Job Detail 优先展示风险、差异、审批，而不是日志表。
4. Apply Gate 做成强状态组件。

验收：

1. 新管理员进入首页 10 秒内能找到下一步动作。
2. Apply 不可执行时主按钮禁用并显示原因。
3. Dry Run Diff 在 Job Detail 首屏可见。

### Phase 2: Decision Wizard 与 Lifecycle Workbench

周期：2 周

范围：

1. 重构 Same-Account Decision Guide 为 Wizard。
2. 重构 Conflict Queue 的列表和批量操作。
3. 重构 Lifecycle 为四列工作台。
4. 增加高风险确认面板。

验收：

1. 绑定 AD 账号前能清楚看到绑定后和不绑定后果。
2. 批量生命周期操作显示选中数量和影响摘要。
3. 冲突处理完成后队列状态即时可理解。

### Phase 3: Config Wizard、Data Quality、Release Center

周期：3 周

范围：

1. 将 Config 改成分步配置向导。
2. 将 Data Quality 改成趋势和修复清单看板。
3. 将 Release Center 改成发布流水线。
4. Integration Center 增加 API Explorer 和投递监控视图。

验收：

1. 配置页首屏不再是长表单开头，而是配置完成度和当前步骤。
2. 配置发布/回滚有明确版本线和影响摘要。
3. 数据质量中心能导出按责任方分组的修复清单。

## 12. 验收指标

| 指标 | 目标 |
| :--- | :--- |
| 首屏下一步识别时间 | 新管理员 <= 10 秒 |
| 首次 dry run 路径点击数 | <= 3 次 |
| 从冲突进入绑定决策并完成 | <= 5 步 |
| Config 页面默认展开高度 | 首屏只显示当前步骤，避免整页长表单 |
| 高风险操作确认 | 100% 使用 ConfirmPanel，不使用浏览器 confirm |
| 移动端可用性 | 390px 宽度下核心页面无横向溢出 |
| 样式覆盖债 | 去除大部分 `!important` 和重复选择器 |
| 浏览器回归 | 新增关键页面截图回归和交互回归 |

## 13. 非目标

1. 本 PRD 不要求立刻引入 React/Vue/前端构建链。
2. 本 PRD 不改变同步核心业务逻辑。
3. 本 PRD 不要求一次性重写所有页面。
4. 本 PRD 不以炫酷视觉为目标，重点是运维决策效率和上线安全。

## 14. 风险与依赖

| 风险 | 说明 | 缓解 |
| :--- | :--- | :--- |
| 页面数量多 | 一次重构所有页面成本高 | 先重构 Control Tower、Jobs、Conflicts 三条主链路 |
| Jinja 模板复用不足 | 组件宏能力有限 | 先补齐宏和 CSS，不急于换框架 |
| 双语文案复杂 | 中英文都需要高质量表达 | 建立关键任务文案表，先覆盖主流程 |
| 浏览器截图不稳定 | 固定侧边栏 full-page 截图可能产生拼接观感 | 增加 viewport screenshot 和移动端截图，不只依赖 full-page |

## 15. 下一步建议

建议立即执行 Phase 0 和 Phase 1：

1. 先补齐缺失 CSS 类和组件变体，让现有页面恢复设计一致性。
2. 同步重构 Dashboard 为 Control Tower，因为这是所有体验问题的入口。
3. 接着重构 Jobs / Job Detail，把 Dry Run Diff 和 Apply Gate 做成核心决策流。
4. 再推进 Same-Account Decision Wizard，让最复杂的人工判断变得可解释、可确认、可审计。

