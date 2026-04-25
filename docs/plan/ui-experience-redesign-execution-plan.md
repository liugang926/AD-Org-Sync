# AD Org Sync UI 体验重构执行计划

## 1. 执行原则

本计划承接 `docs/plan/ui-experience-redesign-prd.md`，采用“先稳住设计系统，再改关键任务流”的路径推进。

执行优先级：

1. 修复现有页面的样式基建缺口，避免继续在不稳定组件上叠功能。
2. 优先改造管理员每天都会打开、且直接影响上线安全的页面。
3. 每个阶段都必须有浏览器回归和移动端布局检查。
4. 不在第一轮引入 React/Vue/构建链，先发挥 Jinja + CSS 的维护效率。

## 2. 总体阶段

| 阶段 | 目标 | 交付物 | 验收重点 |
| :--- | :--- | :--- | :--- |
| Phase 0 | UI 基建止血 | CSS utility、组件变体、基础表格/表单/卡片系统 | 现有模板结构正常落地，页面不再像裸结构堆叠 |
| Phase 1 | 上线控制塔 | Control Tower、Run Review、Job Detail 首屏重排 | 管理员 10 秒内知道是否可上线和下一步 |
| Phase 2 | 人工决策闭环 | Same-Account Wizard、Conflict Queue、Lifecycle Workbench | 决策前看到后果，批量操作看到影响 |
| Phase 3 | 配置运营体系 | Config Wizard、Data Quality、Release Center、Integration Portal | 配置、质量、发布、集成形成长期运营体验 |

## 3. Phase 0 任务拆解

| 编号 | 任务 | 文件范围 | 状态 |
| :--- | :--- | :--- | :--- |
| P0-1 | 补齐 `grid`、`cols-2`、`form-grid`、`form-group` | `sync_app/web/static/app.css` | 已完成 |
| P0-2 | 补齐 `subcard`、`section-header`、`hero-header`、`back-link` | `sync_app/web/static/app.css` | 已完成 |
| P0-3 | 补齐 `table-sm`、`table-empty`、`cell-*`、`metric-grid` | `sync_app/web/static/app.css` | 已完成 |
| P0-4 | 补齐 `stack-row`、`list-stack`、`dense-meta`、`inline-checkbox` | `sync_app/web/static/app.css` | 已完成 |
| P0-5 | 统一按钮 `success`、`small`、`sm`、disabled 样式 | `sync_app/web/static/app.css` | 已完成 |
| P0-6 | 移除全局 `.card:hover` 上浮，改为 opt-in | `sync_app/web/static/app.css` | 已完成 |
| P0-7 | 增加设计系统说明文档 | `docs/design/ui-system.md` | 已完成 |
| P0-8 | 运行单元测试和浏览器回归 | `tests/` | 已完成 |

## 4. Phase 1 任务拆解

| 编号 | 任务 | 文件范围 | 状态 | 说明 |
| :--- | :--- | :--- | :--- | :--- |
| P1-1 | Dashboard 改造为 Control Tower | `dashboard.html`、`preflight_support.py` | 已完成 | 首屏状态、下一步、阻断项、近期时间线 |
| P1-2 | Jobs 改造为 Run Review | `jobs.html`、`service_facades.py` | 已完成 | Apply Gate、Run Actions、Impact Summary |
| P1-3 | Job Detail 首屏重排 | `job_detail.html` | 已完成 | 风险、冲突、错误、差异优先，日志表后置 |
| P1-4 | Apply Gate 组件化 | `app.css`、`dashboard.html`、`jobs.html` | 已完成 | 统一 Ready / Blocked / Needs Review 表达 |
| P1-5 | 浏览器回归扩展 | `tests/test_web_browser_regression.py` | 已完成 | 增加 Control Tower、Run Review、Job Detail 断言和截图 |

## 5. Phase 2 任务拆解

| 编号 | 任务 | 文件范围 | 说明 |
| :--- | :--- | :--- | :--- |
| P2-1 | Same-Account Decision Wizard | `conflict_decision_guide.html`、`sync_conflict_support.py` | 已完成：五步决策，绑定/不绑定后果明确 |
| P2-2 | Conflict Queue 卡片化和批量栏 | `conflicts.html`、`routes_conflicts.py` | 已完成：sticky action bar、选择数量、推荐动作 |
| P2-3 | Lifecycle 四列工作台 | `lifecycle_workbench.html`、`lifecycle_workbench.py` | 已完成：四列队列、Due Now、Manual Hold、Replay 视图 |
| P2-4 | 高风险确认面板 | `base.html`、`app.js`、`app.css` | 已完成：替代浏览器 confirm 的统一确认面板 |

## 6. Phase 3 任务拆解

| 编号 | 任务 | 文件范围 | 说明 |
| :--- | :--- | :--- | :--- |
| P3-1 | Config Wizard | `config.html`、`config-page.js`、`config_support.py` | 已完成：配置向导外壳、步骤状态、上线前路径 |
| P3-2 | Data Quality 运营看板 | `data_quality_center.html`、`data_quality_center.py` | 已完成：质量运营首屏、修复 backlog、趋势入口 |
| P3-3 | Release Pipeline | `config_release_center.html`、`config_release.py` | 已完成：Live -> Snapshot -> Compare -> Rollback 流水线 |
| P3-4 | Integration Portal | `integration_center.html`、`routes_integrations.py` | 已完成：集成门户首屏、API/Webhook/交付状态入口 |

## 7. 当前执行切入点

本轮立即执行 Phase 0。原因：

1. 当前多个模板类名没有 CSS 定义，直接影响新页面视觉完成度。
2. 按钮和卡片变体不完整，会拖累后续 Control Tower 和 Wizard 实现。
3. Phase 0 风险低、收益高，不改变后端业务逻辑。

完成 Phase 0 后，再进入 Phase 1 的 Control Tower 和 Run Review 重构。
