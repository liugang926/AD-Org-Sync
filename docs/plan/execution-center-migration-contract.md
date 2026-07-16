# 执行中心迁移与安全契约

## 产品目标

第六阶段将原 `/jobs` 混合页面拆为四个按用户任务组织的规范页面：Dry Run 生成计划、Plan Review 审核计划、Apply 消费已批准计划、Job History 查询运行与审计证据。Control Tower 只汇总状态与唯一下一步，Automation Center 只保留计划任务门禁和提醒，不再承载批准决策或完整任务列表。

本阶段不操作真实 AD 账号或生产绑定数据，不引入新的前端框架，也不删除旧能力。

## 页面与路由职责

| 规范路由 | 唯一职责 | 主要 CTA | 基础列表 |
| --- | --- | --- | --- |
| `/execution-center/dry-run` | 基于当前已保存范围生成不可变更计划，不写入 AD | Run Dry Run | 6 列 |
| `/execution-center/plan-review` | 选择一个 Dry Run，复核证据并记录批准 | Approve selected plan | 7 列 |
| `/execution-center/apply` | 重新校验并执行一个已批准计划 | Apply changes | 无日常宽表 |
| `/execution-center/jobs` | 监控任务状态并进入详细审计证据 | Run Dry Run | 8 列 |
| `/execution-center/jobs/{job_id}` | 查看阶段、身份时间线、操作、冲突和技术证据 | 按当前状态进入 Plan Review | 技术表保留在详情页 |

旧 `/jobs`、`/execution-center/run-review`、`/jobs/run`、`/jobs/{job_id}` 和 `/jobs/{job_id}/approve` 在迁移期保持兼容，但复用相同 RBAC、CSRF、组织隔离、计划校验和审计服务。新导航和内部业务链接只指向规范路由。

## 计划资格与状态机

Apply 只能消费一个明确选择的、已完成的 Dry Run。资格校验按以下顺序返回首要阻塞原因和下一步：

1. 当前运行环境必须有明确标记；
2. 任务必须属于当前组织，类型为 Dry Run 且状态为 `COMPLETED`；
3. 计划指纹、配置指纹、环境证据和生成时间必须完整；
4. 计划不得超过组织级 `execution_plan_max_age_hours`，默认 24 小时；
5. 任务绑定的 Provider、Connector、源快照、快照指纹、范围指纹和配置指纹必须与计划一致；
6. 当前运行配置指纹必须与 Dry Run 的配置指纹一致，配置无法安全解析时同样阻止；
7. 源快照必须仍为成功状态且未过期；
8. 当前保存的同步范围不得在 Dry Run 后变化；
9. 审核记录必须匹配同一任务、计划指纹和配置指纹，状态为已批准且批准未过期；
10. 同一计划不得已有 Apply 任务。

页面展示、批准、排队以及任务实际构造 AD 客户端前都会解析当前配置指纹并执行同一资格校验。这样即使 Dry Run 后或排队期间配置、环境、快照、范围或批准发生变化，也不会进入真实写入阶段。数据库用事务原子地建立 `plan_source_job_id` 绑定，防止重复提交产生第二个 Apply。

## 权限、安全与审计影响

- `jobs.read`：查看四个页面和任务详情；
- `jobs.run`：POST 启动 Dry Run 或 Apply；
- `jobs.review`：POST 批准计划；
- 所有写操作继续使用 POST、CSRF、capability 检查和组织隔离；GET 不写数据库；
- Apply 确认固定显示组织、环境、快照版本、影响数量和 Dry Run ID；
- Apply 阻止事件写入 `high_risk.apply.blocked`，请求事件写入 `high_risk.apply.requested`，排队写入 `job.enqueue`；
- 批准前重新校验组织、环境、当前配置指纹、快照、范围、计划时效和指纹；
- 保留 Provider、Connector、RBAC 和现有审计 repository 边界；
- AD 不可用、验证未知或连接失败不会触发绑定删除；本阶段无绑定删除实现变更。

## 交互与可访问性

- 四个页面共享可横向滚动的键盘可访问步骤导航；
- 每页页首只有一个主要 CTA，始终显示状态、首要阻塞原因和下一步；
- 组织、环境、快照版本和影响数量在执行上下文中持续可见；
- 技术指纹进入折叠证据或任务详情；
- 表格容器可聚焦，窄屏下上下文卡片改为单列；
- 时间使用现有 `data-local-time` 组件，在浏览器按用户本地时区显示相对时间并保留原始时间；
- 英文和简体中文使用同一 catalog key 集合。

## 测试证据要求

- 单元：组织隔离、环境变化、当前配置变化/不可解析、计划时效、快照状态/时效、范围指纹、审核状态/时效、已消费计划；
- repository/dispatch：同一计划原子创建一次 Apply，计划 ID 写入 `plan_source_job_id`；
- Web 集成：四个规范 GET 不写数据、单主 CTA、8 列上限、CSRF、RBAC、确认上下文、批准与 Apply 审计；
- 运行时：排队后再次校验，且门禁发生在 AD 客户端构造之前；
- 浏览器：中英文、桌面、窄屏、键盘焦点、Dry Run—审核—Apply—历史完整路径；
- 全量执行仓库 CI 等价检查，Draft PR 的 GitHub Actions 必须全部成功。

## 回滚方案

本阶段不新增数据库表或迁移。回滚应用提交即可恢复旧页面入口；已有 `plan_source_job_id` 字段和审核记录保持向后兼容，不需数据回滚。即使只回滚 UI，服务端严格门禁仍可独立保留。若必须整体回滚，先停止新任务调度，再部署上一已验证镜像；不删除任务、审核、审计或绑定数据。
