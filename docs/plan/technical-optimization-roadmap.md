# 技术优化路线图

## 1. 当前判断

`v1` 功能已经齐备，当前阶段的主要矛盾已经从“能力缺失”转为：

1. 热点文件体量持续增大，后续修改越来越容易牵一发动全身。
2. 测试覆盖充足，但入口过度集中，定位失败成本偏高。
3. 外部通知与集成已经可用，但运行链路里仍有同步投递的稳定性风险。

本路线图的目标不是继续扩功能，而是让现有 `v1` 更稳定、更易维护、更适合试运行和后续迭代。

后续 P0-P3 架构治理、热点文件拆分、结构护栏和新功能模块入口的完整完成计划，见
[架构优化完成计划](architecture-optimization-completion-plan.md)。

## 2. 优先级

### P0：稳定性与复杂度收口

1. 拆分 `sync_support` 的聚合职责
2. 提取共享测试夹具并削减超大测试文件样板
3. 形成试运行与缺陷收口节奏

### P1：运行链路可靠性

4. 为外部 webhook / 通知引入 outbox 与重试模型
5. 收敛 `app.state` 直接暴露的 repo 依赖
6. 为配置与策略补 typed settings 访问层

### P2：持续演进能力

7. 继续拆分 runtime 热点模块
8. 按领域拆分 Web 端测试与权限测试
9. 为集成接口补版本化与幂等语义

## 3. 第一批执行项

本轮直接开始的优化：

1. 将 `sync_support` 中的数据质量快照职责移到独立模块
2. 将 runtime 假实现测试夹具提取到共享 helper
3. 将 Web 权限测试的环境搭建与通用 helper 提取到共享 base case

这三项都满足：

- 不改变已有产品入口
- 不改数据库模型
- 低风险
- 能立即降低单文件复杂度

当前状态：

1. 已完成 `sync_support -> sync_data_quality_support` 拆分
2. 已完成 `tests/helpers/runtime_fakes.py` 与 `tests/helpers/web_authz_case.py`
3. 已完成全量回归确认

## 4. 模块级建议

### 4.1 Web 支撑层

目标：

- `sync_support` 只保留与身份决策、冲突处理、目录查询直接相关的能力
- 数据质量、批量规则解析、冲突动作等进一步拆成按领域聚合的支持模块

建议拆分方向：

- `sync_data_quality_support`
- `sync_conflict_actions`
- `directory_lookup_support`

### 4.2 测试层

目标：

- 减少大测试文件中的环境搭建重复
- 让失败定位更快指向具体领域

建议拆分方向：

- `tests/helpers/runtime_fakes.py`
- `tests/helpers/web_authz_case.py`
- 后续再按页面域拆 `test_web_authz.py`

### 4.3 集成投递链路

目标：

- 外部 webhook 失败不阻塞主执行链路
- 支持重试、死信、投递审计

当前已完成：

1. 增加 webhook outbox 表
2. 业务链路只写 outbox
3. 支持 inline flush 与异步 flush 两种投递方式
4. 支持失败重试与投递状态持久化
5. 已接入独立的定时 outbox worker，并挂到 Web startup / shutdown 生命周期
6. 运维通知 webhook 已复用同一套 outbox 与 worker
7. 已完成 `sync_support -> sync_directory_support / sync_conflict_support` 二次拆分
8. 已完成 `web_app_state` 统一访问器首批接入，基础支撑模块不再直接依赖散落的 `request.app.state.*repo`
9. 已完成 `routes_jobs / routes_conflicts / routes_organizations` 第一批热点路由收口，并为其补结构护栏

下一步建议：

1. 评估为投递链路补死信归档与更细粒度的回放审计
2. 继续拆分 runtime 热点模块，并把 `mappings / exceptions / advanced_sync / integrations` 等剩余 route/service 逐步切到统一访问层
3. 逐步将高级同步和配置页面剩余散落键名继续收敛到 typed settings

## 5. 验收标准

每一轮优化至少满足：

1. 现有功能行为不回退
2. 自动化测试继续通过
3. 至少一个热点文件体量明显下降或职责边界更清晰
4. 文档能说明为什么要拆、拆到了哪里

## 6. 当前回合输出

当前回合完成后，应至少看到：

1. 技术优化路线图文档入库
2. `sync_support` 体量下降
3. runtime / web 测试共享夹具开始成型
4. 相关定向测试通过

当前实际进展：

1. 第一批复杂度收口已经完成
2. 第二批已完成集成 webhook outbox + 定时 worker + 运维通知复用
3. 已补上 failed outbox 视图、手动重放入口与批量重放动作
4. 已完成 `Web runtime / Web security / Automation policy` 的 typed settings 首批收敛
5. 已完成 `Advanced sync policy / Directory UI / Branding / Desktop local strategy` 的 typed settings 第二批收敛
6. 已完成 `sync_support` 的目录查询 / 身份路由 / 冲突动作二次拆分，当前 `SyncSupport` 本体收敛到聚合壳
7. 已完成 `app_state` 统一访问器，并接入 `request_support / preflight_support / config_preview / config_support / app middleware` 第一批基础模块
8. 已完成 `jobs / conflicts / organizations` 三个热点路由的 repo 访问收口，并补上结构测试防回退
9. 已完成 `mappings / exceptions / advanced_sync` 三个剩余热点路由的 repo 访问收口，并接入统一 `web_app_state` 访问层
10. 已完成 `public / auth / admin` 基础路由的 repo/runtime 访问收口，并补上结构测试防回退
11. 已完成 `config_submission / config_persistence / routes_config` 配置提交流程的 repo/runtime 访问收口，并补上结构测试防回退
12. 已完成 `integrations / automation_center / lifecycle / data_quality` 运营路由的 repo/runtime 访问收口，并补上结构测试防回退
13. 已完成 `sync_directory_support / sync_conflict_support` support 层热点模块的 repo/runtime 访问收口，并补上结构测试防回退
14. 已完成 `request_support / config_support` 共享基础模块的 repo/runtime 访问收口，并补上结构测试防回退
15. 已完成 `sync_support` 聚合壳最后一处 replay 直连收口，当前 `sync_app/web` 业务代码中的 `app.state` 直连仅剩初始化绑定
16. 已重新确认全量回归通过，当前 `sync_app/web` 中仅剩初始化绑定这一个刻意保留的 `app.state` 用法
17. 已开始引入 `Web service facade` 薄层，先收 `jobs / conflicts` 两个高频领域的路由编排逻辑
18. 已将 `jobs / conflicts` 路由剩余的审批、冲突解决和 bulk 写操作编排继续下沉到 facade，路由层进一步收薄
19. 已将 `integrations` 的 token、订阅、delivery retry、外部 API 查询和审批回调编排下沉到 facade
20. 已将配置发布中心的发布、回滚、下载和保存后运行时提示编排下沉到 facade，`routes_config` 不再直接处理发布审计或 snapshot repo 查询
