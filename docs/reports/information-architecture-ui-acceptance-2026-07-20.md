# 信息架构与 UI 重构验收报告

- 验收日期：2026-07-20
- 验收基线：`origin/main` / `7ae4c18440b9308fe90f829d97a489d3084c268c`
- 验收分支：`codex/ia-ui-acceptance`
- 范围：信息架构、页面职责、基础/高级模式、RBAC、移动端、双语、执行门禁和高风险操作；不新增业务功能，不连接真实 AD，不修改生产绑定

## 验收结论

本轮验收确认重构已经解决主要的功能拆分混乱问题：

1. 导航、页面内容密度和权限分别由路由、UI 模式和 capability 控制，基础/高级模式不再被当作安全边界；同一路由始终只有一个当前导航入口。
2. 数据源只负责连接、源快照浏览、刷新状态、快照历史和数据质量；源目录没有身份绑定或同步策略编辑器。
3. 身份匹配、绑定对账、冲突、人工覆盖和例外规则分开，用户证据进入统一详情时间线，基础列表不超过 8 列。
4. 同步范围、账号命名、属性、OU 路由、组、生命周期和安全策略均有唯一编辑入口；策略变更使旧 Dry Run 失效，并提示重新运行和审核。
5. `/jobs` 是 Dry Run、审核、Apply 和任务历史的唯一执行中心；其他页面只显示摘要和跳转，不提供 Apply 提交入口。
6. 高风险操作使用扫描、预览、确认、执行、审计五步流程，并在服务端核对组织、环境、快照、影响数和预览标识；未知、不可用、歧义和过期状态失败关闭。

验收中发现并修复一个纯视觉缺陷：冲突决策结果卡的状态徽标在宽屏双列布局中可能被压缩为逐字换行。修复仅约束该徽标不换行，并新增浏览器布局断言；另清理两处测试内未使用的局部变量，使扩大到 `tests/` 的 Ruff 检查通过。

## 页面职责对照表

| 领域 | 规范入口 | 唯一职责 | 明确不负责 |
| --- | --- | --- | --- |
| 概览 | `/overview/control-tower` | 汇总组织状态、阻断项和唯一下一步 | 不直接执行 Dry Run 或 Apply |
| 数据源 | `/data-sources/connectors` | 源连接器和目标 AD 连接配置、连接测试 | 不编辑同步规则或身份绑定 |
| 数据源 | `/data-sources/source-directory` | 浏览最新成功源快照中的用户和部门、发起刷新 | 不编辑绑定、命名、范围或 Apply |
| 数据源 | `/data-sources/snapshots` | 浏览和比较不可变源快照及刷新结果 | 不修改当前目录或策略 |
| 数据源 | `/data-sources/data-quality` | 质量扫描、趋势和修复清单 | 不处理身份决策 |
| 身份治理 | `/identity-governance/identity-matching` | 解释候选、置信度、证据和可创建账号结论 | 不直接写 AD |
| 身份治理 | `/identity-governance/binding-reconciliation` | 扫描绑定与 AD 状态差异，并走高风险清理流程 | 不编辑同步策略 |
| 身份治理 | `/identity-governance/conflicts` | 处理无法自动决定的身份冲突 | 不承载普通绑定维护 |
| 身份治理 | `/identity-governance/manual-overrides` | 管理人工身份绑定和部门覆盖 | 不定义通用例外或全局策略 |
| 身份治理 | `/identity-governance/exception-rules` | 管理用户、部门和组例外 | 不承载批量通用策略 |
| 同步策略 | `/sync-policies/scope` | 组织、连接器根、部门和身份范围 | 不做账号创建验证或执行 |
| 同步策略 | `/sync-policies/account-naming` | 账号命名、冲突规则和只读预览 | 不配置连接密文 |
| 同步策略 | `/sync-policies/attribute-mappings` | 字段方向、转换和受控回写 | 不配置 OU 路由 |
| 同步策略 | `/sync-policies/department-ou-routing` | 部门到 OU 的精确或子树路由 | 不管理个人覆盖 |
| 同步策略 | `/sync-policies/group-rules` | 组同步、保护和排除规则 | 不处理运行队列 |
| 同步策略 | `/sync-policies/lifecycle` | 入转离、宽限期、返聘、重放和变更熔断策略 | 不处理每日生命周期待办 |
| 同步策略 | `/sync-policies/security` | 多连接器路由、审核和身份认领门禁 | 不绕过服务端 Apply 门禁 |
| 同步策略 | `/sync-policies/releases` | 策略快照、差异、发布和高风险回滚 | 不执行 AD 变更 |
| 执行中心 | `/jobs` | Dry Run、计划审核、Apply、任务历史 | 唯一 Apply 提交表面 |
| 执行中心 | `/execution-center/jobs/{job_id}` | 任务阶段、风险、差异、身份时间线和技术证据 | 不编辑策略 |
| 运营中心 | `/operations-center/lifecycle-queue` | 生命周期每日待办和重放队列 | 不定义生命周期策略 |
| 运营中心 | `/operations-center/automation` | 自动化和计划任务 | 不批准计划或展示完整任务历史 |
| 运营中心 | `/operations-center/notifications` | 通知条件和投递配置 | 不配置执行策略 |
| 运营中心 | `/operations-center/audit-log` | 只读审计查询 | 不触发业务写入 |
| 系统管理 | `/system-management/organizations` | 组织和配置包管理 | 不编辑组织内同步策略 |
| 系统管理 | `/system-management/administrators` | 管理员、角色和权限 | 不编辑员工身份 |
| 系统管理 | `/system-management/employee-self-service` | 员工自助服务可用性和安全设置 | 不执行员工实际重置 |
| 系统管理 | `/system-management/database` | 完整性检查、备份和恢复准备 | 不删除业务数据 |
| 系统管理 | `/system-management/branding` | 全局品牌和外观 | 不配置组织同步 |
| 系统管理 | `/system-management/deployment` | 环境标记、公共地址和部署设置 | 不执行部署 |
| 系统管理 | `/system-management/account` | 当前管理员账户 | 不管理其他管理员 |

## 旧入口与新入口映射

| 旧入口 | 规范入口 | 兼容行为 |
| --- | --- | --- |
| `/dashboard` | `/overview/control-tower` | 同一只读控制塔处理器 |
| `/config` | `/data-sources/connectors` | GET 重定向；旧 POST 按字段权威继续兼容 |
| `/source-directory` | `/data-sources/source-directory` | 复用源目录只读处理器 |
| `/data-quality` | `/data-sources/data-quality` | 复用数据质量处理器 |
| `/mappings` | `/identity-governance/manual-overrides` | 旧综合绑定历史继续兼容；新页只管理人工覆盖 |
| `/conflicts` | `/identity-governance/conflicts` | 保留查询参数和决策写服务 |
| `/exceptions` | `/identity-governance/exception-rules` | 保留旧读写别名 |
| `/advanced-sync` | `/sync-policies/scope` | GET 重定向；旧分区 POST 继续兼容 |
| `/source-directory/scope` | `/sync-policies/scope` | 复用范围保存、CSRF、组织隔离和审计 |
| `/config/releases` | `/sync-policies/releases` | 复用发布、差异、下载和回滚服务 |
| `/jobs`、`/execution-center/dry-run`、`/execution-center/plan-review`、`/execution-center/apply`、`/execution-center/jobs` | `/jobs` | 五个业务阶段集中到同一执行中心并保持单一 active 导航 |
| `/jobs/{job_id}` | `/execution-center/jobs/{job_id}` | 复用任务详情和组织隔离 |
| `/lifecycle` | `/operations-center/lifecycle-queue` | GET 重定向，旧写路径兼容 |
| `/automation-center` | `/operations-center/automation` | GET 重定向 |
| `/integrations` | `/operations-center/notifications` | GET 重定向 |
| `/audit` | `/operations-center/audit-log` | GET 重定向并保留只读查询 |
| `/organizations` | `/system-management/organizations` | GET 重定向，旧写路径兼容 |
| `/users` | `/system-management/administrators` | GET 重定向，旧写路径兼容 |
| `/database` | `/system-management/database` | GET 重定向，旧写路径兼容 |
| `/account` | `/system-management/account` | GET 重定向，旧写路径兼容 |

## 本地测试结果

| 门禁 | 结果 | 证据 |
| --- | --- | --- |
| Python 编译 | 通过 | `python -m compileall -q sync_app` |
| Ruff（应用与测试） | 通过 | `ruff check sync_app tests --select E9,F821,F841,B007,F541` |
| mypy | 通过 | CI 定义的 9 个增量类型边界 |
| locale JSON | 通过 | 中英文 2 个 catalog、3607 个共享键、无空值、键集合完全一致 |
| JavaScript 语法 | 通过 | 8 个本地脚本 `node --check` |
| 单元与集成测试 | 通过 | 565 passed，另有 410 subtests passed；独立 `--basetemp` 重跑耗时 906.42s，日志：`test_artifacts/acceptance-20260720/pytest-non-browser-rerun.out.log` |
| 浏览器回归 | 通过 | 32/32 passed；视觉修复后全量复测耗时 514.996s，日志：`test_artifacts/acceptance-20260720/pytest-browser-rerun.err.log` |
| 迁移与备份恢复 | 通过 | 专项测试通过；仍由最终全量与 CI 再验证 |
| Wheel | 通过 | 全新临时 worktree 构建，256 个包条目；隔离安装、CLI、模板/静态/locale 包数据通过 |
| SBOM | 通过 | CycloneDX 1.6，26 个组件，校验通过 |
| Docker | 通过 | `ad-org-sync:acceptance`，228 MB |
| GitHub CI | 待 Draft PR | 必须等待 Quality 3.10/3.12、Windows、Container、Wheel/migrations/SBOM、Browser 全部成功 |

## 浏览器验收场景与截图

所有场景使用隔离 SQLite、测试管理员、模拟执行器或受控 Provider，不连接真实 AD，不修改生产绑定。

| 场景 | 结果 | 主要截图 |
| --- | --- | --- |
| 首次配置到首次 Dry Run | 通过 | `test_artifacts/browser/connectors-page.png`、`test_artifacts/browser/jobs-page.png` |
| 刷新源目录 | 通过 | `test_artifacts/browser/source-directory-refreshing-auto-update-en.png` |
| 查看缺失工号用户 | 通过 | `test_artifacts/browser/evidence-source-directory-1440.png` |
| 发现可创建账号 | 通过 | `test_artifacts/browser/identity-relationship-create-selection-desktop-en.png` |
| 扫描并清理过期绑定 | 通过 | `test_artifacts/browser/binding-reconciliation-start-desktop-en.png`、`test_artifacts/browser/binding-reconciliation-cleanup-desktop-en.png` |
| 处理冲突 | 通过 | `test_artifacts/browser/conflict-queue-page.png`、`test_artifacts/browser/conflict-decision-page.png` |
| 修改同步策略 | 通过 | `test_artifacts/browser/sync-policy-account-naming-desktop-en.png` |
| 重新 Dry Run | 通过 | `test_artifacts/browser/execution-center-desktop-en.png` |
| 审核并 Apply | 通过 | `test_artifacts/browser/apply-confirm-390.png`、`test_artifacts/browser/execution-center-desktop-en.png` |
| 查看任务和审计记录 | 通过 | `test_artifacts/browser/evidence-jobs-1440.png`、`test_artifacts/browser/evidence-audit-1440.png` |
| 基础/高级模式人工核查 | 通过 | `test_artifacts/acceptance-20260720/browser-manual/dashboard-basic-1440.png`、`dashboard-advanced-1440.png` |
| 移动端导航人工核查 | 通过 | `test_artifacts/acceptance-20260720/browser-manual/jobs-mobile-nav-open-390.png` |

浏览器矩阵还生成了 390、768、1024、1366、1440 宽度的核心页面证据，以及 1920×1080、1440×900、1280×800、768×900、390×844 的身份工作台视觉矩阵；完整目录为 `test_artifacts/browser/`。

## 已知限制

1. 本地浏览器验收使用隔离测试数据和模拟 Apply 启动器，验证的是 UI、路由、门禁、请求上下文和审计链，不是对真实 AD 的破坏性演练。
2. 本机 Python 为 3.14；Python 3.10 和 3.12 由 GitHub CI 的干净环境覆盖。
3. Windows PyInstaller 包、Linux 干净容器和供应链作业以 Draft PR 的 GitHub CI 为最终依据；本地已完成 Wheel、SBOM 和 Docker 镜像构建。
4. 浏览器测试中未配置或故意无效的外部连接会记录预期的连接失败日志，但页面会失败关闭且不显示密文。
5. 本验收不部署生产、不合并 PR；生产状态不在本轮变更范围内。

## 回滚方式

### 回滚本验收 PR

本验收 PR 只包含冲突徽标布局修复、回归断言、Ruff 测试清理和本报告，不含 schema 或数据迁移。若需回滚，撤销该 PR 的 merge commit 并重新部署上一已验证镜像即可；数据库、快照、绑定和审计记录无需转换。

### 回滚 PR #39 的 UI 统一

1. 停止新任务调度，保留现有数据库、快照、绑定和审计数据。
2. 部署 PR #39 前的已验证提交 `595743d3b7a4a0bbd6590178e11db987d4a4cc4e`，或执行 `git revert -m 1 7ae4c18440b9308fe90f829d97a489d3084c268c` 后走新的 PR 和完整 CI。
3. 不重写迁移历史；尤其保留已部署数据库使用的 `binding_revision` 迁移。
4. 验证 `/healthz`、`/readyz`、登录、旧 GET 入口、CSRF 拒绝、组织隔离、Jobs Apply 门禁和审计读取。

若要回滚 PR #28 至 #39 的完整信息架构系列，应按合并逆序逐个 revert 并逐步验证，而不是直接重置分支或回退数据库；该操作需要单独的变更窗口和明确批准。

## Draft PR 与 CI

- Draft PR：待本地最终重跑通过后创建
- CI：待 Draft PR 创建后持续监控；全部成功后停止，等待用户回复“确认合并”
