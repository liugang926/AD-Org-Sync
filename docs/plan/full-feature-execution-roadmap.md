# 全量功能执行路线图

## 1. 目标

本计划覆盖当前提出的 8 个功能方向，并按“安全上线能力优先、管理员决策效率优先、长期运营能力随后补齐”的原则推进。

核心目标：

1. 让管理员能够在 `dry run -> review -> apply` 链路中更快、更安全地做决定。
2. 让冲突、规则、生命周期、配置发布形成闭环，而不是散落在多个页面的点状能力。
3. 为通知自动化、外部 API、跨组织发布和长期数据质量运营提供稳定底座。

## 2. 分期

### P0：上线安全与决策效率

1. `同账户决策向导`
2. `Dry Run 差异对比`
3. `规则治理闭环 v2`

### P1：运维闭环与发布能力

4. `配置发布与回滚中心`
5. `生命周期工作台`
6. `通知与自动化策略中心`

### P2：长期运营与集成

7. `数据质量中心`
8. `外部集成接口`

## 3. 执行波次

### Wave 0：规划与底座对齐

- 输出本路线图与里程碑
- 统一各功能的对象模型和依赖关系
- 补第一批复用底座：
  - `job diff` 对比服务
  - 规则治理元数据扩展
  - 生命周期与通知触发信号标准化

### Wave 1：P0 首批交付

- 交付 `Dry Run 差异对比 v1`
- 交付 `同账户决策向导 v1`
- 交付 `规则治理闭环 v2` 数据层与页面层

### Wave 2：P1 闭环交付

- 配置快照、差异、回滚、推广
- 生命周期独立工作台与批量动作
- 条件化通知与自动化策略

### Wave 3：P2 能力补齐

- 数据质量趋势看板
- 修复清单导出
- Job / Conflict / Approval / Webhook API

## 4. 功能拆解

### 4.1 同账户决策向导

现有基础：

- `Conflict Queue`
- 手工绑定与推荐处理
- `Identity Route Explainer`
- AD 用户搜索接口

本期补齐：

- 目标 AD 账号当前状态卡片
- 最近登录、OU、启用状态、关键属性
- 绑定后下次同步预计更新字段
- 不绑定时是否会新建账号、是否继续冲突
- 从 `Conflict Queue` 与 `Identity Overrides` 双入口进入

### 4.2 Dry Run 差异对比

现有基础：

- `sync_jobs`
- `planned_operations`
- `sync_conflicts`
- `sync_plan_reviews`
- `plan_fingerprint`

本期补齐：

- 当前 job 与“上次成功 dry run”的差异摘要
- 当前 job 与“上次 apply”的差异摘要
- 新增高风险、新增冲突、对象状态变化汇总
- 用户 / 组 / OU 维度变化分布
- Job Detail 首版对比面板

### 4.3 规则治理闭环 v2

现有基础：

- `Rule Governance Snapshot`
- exception `expires_at`
- exception `last_matched_at`

本期补齐：

- rule owner
- effective reason
- review due / reviewed at
- hit count / last hit
- 即将到期、长期未复核、长期未命中提醒

### 4.4 数据质量中心

现有基础：

- `Source Data Quality Snapshot`

本期补齐：

- 趋势快照持久化
- 按类型统计趋势
- 高风险用户清单
- 导出 HR / 源系统修复列表

### 4.5 配置发布与回滚中心

现有基础：

- organization bundle export/import

本期补齐：

- 配置快照版本化
- 快照差异展示
- 一键回滚
- 测试组织到生产组织推广

### 4.6 通知与自动化策略中心

依赖：

- job diff
- governance 生命周期字段
- lifecycle / replay / review 状态

本期补齐：

- dry run 失败通知
- 冲突积压提醒
- 高风险审批待处理提醒
- 规则即将过期提醒
- “最近 dry run 绿色且无冲突”条件化 apply

### 4.7 生命周期工作台

现有基础：

- `user_lifecycle_queue`
- `offboarding_queue`
- replay request 列表

本期补齐：

- 独立页面与视图过滤
- future onboarding / contractor / offboarding / replay 合并视角
- 批量批准 / 延后 / 跳过 / 重试

### 4.8 外部集成接口

依赖：

- 内部对象模型稳定
- 对比与治理状态稳定

本期补齐：

- Job 状态 API
- Conflict API
- Approval callback
- Webhook subscription

## 5. 里程碑定义

### M1

- `Dry Run 差异对比 v1` 可在 Job Detail 使用
- 已有路线图文档

### M2

- `同账户决策向导 v1` 可在冲突页直接进入
- `规则治理闭环 v2` 已有 owner / reason / review / hit 元数据

### M3

- 配置发布与回滚中心可用
- 生命周期工作台可日常运营

### M4

- 通知自动化策略可配置
- 数据质量中心有趋势视图
- 外部接口首版可联调

## 6. 验收口径

每一批功能都必须满足：

1. 有明确入口页或入口动作。
2. 有数据库结构或持久化模型支撑，而不是纯内存逻辑。
3. 有最少一层自动化测试覆盖核心判断逻辑。
4. 不破坏当前 `dry run / apply / conflict / review` 主链路。

## 7. 当前回合启动项

本回合开始执行：

1. 新增本执行路线图文档
2. 落地 `Dry Run 差异对比 v1` 的后端对比服务
3. 在 `Job Detail` 接入第一版差异对比面板

## 8. 当前状态（2026-04-22）

当前 8 个目标都已经进入 `v1 已实现` 状态：

1. `同账户决策向导`
2. `Dry Run 差异对比`
3. `规则治理闭环 v2`
4. `配置发布与回滚中心`
5. `生命周期工作台`
6. `通知与自动化策略中心`
7. `数据质量中心`
8. `外部集成接口`

当前下一步不再是继续扩新能力，而是进入 `v1 收口与交付`：

1. 管理员操作手册
2. 外部接口与 webhook payload 文档
3. 上线检查清单与试运行 runbook
