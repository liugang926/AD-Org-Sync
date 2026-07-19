# High-Risk Operation Safety Contract / 高风险操作安全契约

## Product objective / 产品目标

Every destructive or externally effective operation must follow one visible and auditable sequence: **Scan → Preview → Confirm → Execute → Audit**. The interface and server must keep the reviewed organization, environment, snapshot version, impact count, and preview ID consistent through confirmation.

所有破坏性操作或对外生效操作必须遵循统一且可审计的流程：**扫描 → 预览 → 确认 → 执行 → 审计**。界面与服务端必须确保确认前后的组织、环境、快照版本、影响数量和预览 ID 完全一致。

## Server-side contract / 服务端契约

- `HighRiskOperationContext` is the canonical context shared by the UI, route, dispatch layer, runtime, and audit record.
- Confirmation fields are untrusted input. The route rebuilds the current context and requires an exact match before execution.
- `AD_ORG_SYNC_ENVIRONMENT_LABEL` marks a deployed environment. Loopback-only development is labeled `Local environment`; any non-loopback runtime without an explicit label is `Unlabeled environment` and fails closed.
- Apply is checked when the web request is received, when a job is enqueued, and again before runtime constructs an AD client.
- Binding reconciliation stores only an opaque scan ID in the signed session. The full 15-minute scan report stays in bounded process memory, outside the application database, and execution performs a second live AD verification immediately before an exact revision-checked database deletion.
- Binding scans require `mappings.read`; cleanup requires `mappings.write`, POST, and CSRF. An unlabeled environment blocks cleanup, and Production requires an additional typed confirmation.
- An unavailable, unknown, failed, protected, stale, or changed AD result is never interpreted as permission to delete a binding.

- `HighRiskOperationContext` 是界面、路由、调度层、运行时和审计记录共享的唯一上下文。
- 确认字段属于不可信输入；路由会重建当前上下文，仅在逐项完全一致时执行。
- `AD_ORG_SYNC_ENVIRONMENT_LABEL` 用于标记部署环境。仅绑定回环地址的开发实例标记为 `Local environment`；非回环实例如未显式设置标签，则为 `Unlabeled environment` 并默认阻断。
- Apply 在 Web 请求、任务入队和运行时创建 AD 客户端之前分别校验。
- 绑定清理只在签名会话中保存预览元数据，15 分钟后过期，并在精确匹配删除前再次实时验证 AD。
- AD 不可用、未知、失败、受保护、状态过期或目标变化均不得解释为删除绑定的授权。

## Protected operations / 已保护操作

| Operation / 操作 | Preview evidence / 预览证据 | Server enforcement / 服务端门禁 |
|---|---|---|
| Apply | Latest successful Dry Run, source snapshot, planned impact / 最近成功 Dry Run、源快照、计划影响 | Route + dispatch + runtime / 路由、调度、运行时 |
| Binding reconciliation cleanup / 绑定对账清理 | Read-only live scan, eight result classes, exact targets, binding revisions, 15-minute preview | CSRF, `mappings.write`, organization/provider/connector/source identity match, labeled environment, Production second confirmation, second live verification, atomic compare-and-delete |
| Connector deletion / 连接器删除 | Connector identity and impact count / 连接器身份和影响数量 | Exact confirmation context and environment label / 精确确认上下文和环境标签 |
| Organization deletion / 组织删除 | Organization identity and impact count / 组织身份和影响数量 | Exact confirmation context, environment label, existing job-history guard / 精确确认上下文、环境标签、现有任务历史门禁 |
| Configuration rollback / 配置回滚 | Release snapshot ID and impact count / 发布快照 ID 和影响数量 | Exact confirmation context, organization scope, environment label / 精确确认上下文、组织隔离、环境标签 |

## Deployment requirement / 部署要求

Set a stable, human-readable label in every non-local deployment, for example `Staging`, `UAT`, or `Production`:

所有非本地部署都必须设置稳定且易读的环境标签，例如 `Staging`、`UAT` 或 `Production`：

```text
AD_ORG_SYNC_ENVIRONMENT_LABEL=Production
```

If the label is absent, read-only pages, scans, previews, and Dry Run remain available, while Apply and destructive actions stay blocked. This safe failure mode does not make the application unhealthy and does not affect health checks.

如果未设置标签，只读页面、扫描、预览和 Dry Run 仍然可用；Apply 与破坏性操作保持阻断。该安全失败模式不会使应用变为不健康，也不会影响健康检查。

## Rollback / 回滚

Rollback must retain migration 33 because deployed databases contain the
`binding_revision` column. Reverting application behavior may stop using that
column, but must not rewrite migration history. In-memory scan reports and their
opaque signed-session IDs can be discarded safely.

回退本 PR 将恢复原有单步绑定清理并移除新增环境门禁。本阶段没有数据库结构迁移，因此回滚无需修改数据库；会话级预览元数据可安全丢弃。
