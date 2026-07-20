# 配置权威入口与旧页面迁移

`Config` 与 `Advanced Sync` 不再作为可见编辑页。配置字段不改名、不改变持久化含义，只调整管理后台中的权威编辑入口。

| 配置域 | 权威入口 | 范围 | 说明 |
| --- | --- | --- | --- |
| 源系统凭据、基础 AD、附加 AD 连接 | `/data-sources/connectors` | 组织级 | 连接状态与连接测试也在此页 |
| 同步范围 | `/sync-policies/scope` | 组织级 | 包括组织、部门、显式用户与连接器根范围 |
| 账号命名与账号创建 | `/sync-policies/account-naming` | 组织级 | 包括默认建号密码、首次登录改密和复杂度；密码不回显 |
| 属性映射 | `/sync-policies/attribute-mappings` | 组织级 | 映射开关、方向、转换和空值行为 |
| 部门与 OU 路由 | `/sync-policies/department-ou-routing` | 组织级 | 目录根、禁用 OU 和连接器路由 |
| 组规则 | `/sync-policies/group-rules` | 组织级 | 组同步、嵌套、清理、排除与保护 |
| 生命周期策略 | `/sync-policies/lifecycle` | 组织级 | 只定义策略；不显示运营队列 |
| SSPR、钉钉验证和密码策略 | `/system-management/employee-self-service` | 组织级 | 回调 URL 为摘要，公开基础 URL 仍由部署设置唯一维护 |
| 调度、Dry Run/Apply 模式、无人值守门禁 | `/operations-center/automation` | 组织级 | 通知字段不在此重复编辑 |
| 通知 | `/operations-center/notifications` | 组织级 | 通知 Webhook 的唯一编辑入口 |
| 待入职、合同到期、离职、重放请求 | `/operations-center/lifecycle-queue` | 组织级 | 运营工作台，不属于 Advanced Sync |
| Web 部署 | `/system-management/deployment` | 全局级 | 需要 `system.manage` 权限和全局确认 |
| 品牌 | `/system-management/branding` | 全局级 | 需要 `system.manage` 权限和全局确认 |
| 数据库 | `/system-management/database` | 全局级 | 写操作需要 `database.manage` |
| 平台账号 | `/system-management/administrators` | 全局级 | 写操作需要 `users.manage` |

## 兼容策略

- `GET /config` 使用 `308` 重定向到 `/data-sources/connectors`，保留查询字符串。
- `GET /advanced-sync` 使用 `308` 重定向到 `/sync-policies/scope`，保留查询字符串。
- 旧 POST 地址暂时保留，供旧客户端和滚动升级使用；新页面不会生成这些旧提交地址。
- 旧 Config POST 与预览处理暂时保留给滚动升级中的旧客户端；正式导航和所有普通 GET 请求都会先由兼容中间件重定向到权威入口。
- 账号命名使用范例与只读预览位于帮助抽屉；完整操作说明保留在 `docs/guides/`。

代码中的字段归属契约见 `sync_app/web/configuration_ownership.py`。新增或迁移字段时必须先更新该注册表，并确保一个字段只映射到一个权威入口。
