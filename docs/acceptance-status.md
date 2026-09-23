# Django 重构验收状态

本文件记录证据边界，不作为自动宣布验收通过的依据。2026-09-23：旧平台已退役，Django 版本 `e9d9afb90d50be84a62d0ed3e8f1461825c359d9` 已经原 production runner 部署。对外 HTTPS 的 `/healthz`、`/readyz`、`/login`、`/sspr` 均返回 200；独立只读核验确认运行镜像 SHA、Web/Nginx 健康、数据库/表结构/worker 就绪，生产受限文件权限为 0600，宿主 cron 只有一条应用调度任务。管理员密码此前已通过公开 HTTPS 登录并产生成功审计。真实企业验收仍未完成。[生产部署](https://github.com/liugang926/AD-Org-Sync/actions/runs/35838192737)、[独立核验](https://github.com/liugang926/AD-Org-Sync/actions/runs/35841115771)

## 真实开发测试环境的只读证据

- 测试 AD 域控重启后，生产容器在 `LDAP_VERIFY_CERT=false` 下完成 TLS 1.3 握手，成功绑定并读取测试 OU 及其 427 个账号；自签证书未导入信任库。[连接验证任务](https://github.com/liugang926/AD-Org-Sync/actions/runs/35823677780)
- 同一任务完整读取钉钉 488 名人员、52 个部门。原项目保存的测试连接凭据迁入生产宿主受限 `.env`，未写入仓库；临时传递用的 GitHub Environment secrets 已删除。
- 为测试 OU 配置同步根范围并运行一次全量**预览**，耗时约 86 秒。计划包含 358 个关联、111 个新建、19 个冲突及 52 个部门 OU 项。冲突使任务停在 `blocked`，没有 AD 写入、有效绑定或写入操作记录。[预览任务](https://github.com/liugang926/AD-Org-Sync/actions/runs/35823938289)、[聚合核验](https://github.com/liugang926/AD-Org-Sync/actions/runs/35824229431)
- 19 个冲突中，11 个主部门不明确、7 个账号命名为空/重复/占用/受保护、1 个匹配字段缺失或重复。同步期间员工入口和就绪接口仍返回 200；这只验证了预览阶段的页面可访问性，不等于实际 AD 写入期间的性能验收。
- 生产入口的只读诊断确认 HTTPS 网关以 `10.106.1.119` 连接应用 Nginx。部署后探针发现运行中的 Nginx 未加载 `set_real_ip_from` 和 `real_ip_header`，因为旧容器保留了原挂载文件；正常与伪造转发头的请求都记录为网关地址，伪造头没有被采纳。当前修复在部署时重建 Nginx 并保存旧配置用于失败回退。修复发布后仍须验证网关是否实际传递真实客户端地址，才能确认不同员工的 IP 限流额度已隔离。[网关诊断](https://github.com/liugang926/AD-Org-Sync/actions/runs/35841115771)
- 定时同步和员工密码重置仍关闭；需先指定专用测试员工与账号，完成实际写入和本人密码重置验收。

## 已有自动化证据

| PRD 范围 | 证据位置与覆盖内容 |
| --- | --- |
| FR-01 来源完整性与范围 | `tests/test_directory.py`、`tests/test_sync.py`：分页错误、空目录、局部范围；独立刷新不依赖 AD |
| FR-02～05 匹配、命名、绑定 | `tests/test_domain.py`、`tests/test_sync.py`、`tests/test_web.py`：稳定绑定、冲突、邮箱需人工确认、签名确认防替换与重放、预览不绑定 |
| FR-06 部门 OU | `tests/test_sync.py`：空部门创建、人员换部门后保留 GUID；目录适配器行为仍需真实测试 OU 验证 |
| FR-07 属性与离职 | `tests/test_sync.py`：空值默认保留、显式清除、局部不禁用、全量阈值确认、初始化策略 |
| FR-08 任务与恢复 | `tests/test_sync.py`：计划过期、重复入队、逐人保存、进程中断、局部成功不改变全量标记；本地绑定提交失败后依据建号 GUID 恢复，即使工号改变也不重复建号；无可靠 GUID 时阻断并保留未解决证据 |
| FR-09～10 员工重置 | `tests/test_sspr.py`、`tests/test_browser.py`：未同步未绑定员工可重置、服务端身份、GUID/配置/过期/重放/CSRF 检查；实际密码生效尚未验证 |
| 部署退役保护 | `tests/test_deployment.py`：执行真实部署脚本，替代 Docker/HTTP 命令，验证成功、构建失败、首次发布失败不恢复旧版、Nginx 配置重建失败时回退旧配置与已验证 Django 版本 |
| CI/CD 合约 | `tests/test_operations.py`：原有门禁、部署触发、SHA、备份及数据库检查顺序；需以最新 PR 的远端结果为准 |
| AC-20 备份恢复 | `tests/test_operations.py`：完整 Django 数据库迁移、配置/绑定创建、在线备份、独立目录恢复与 `db_check`；恢复后重新预览，同名异 GUID 的 AD 对象被阻断。AD 部分使用替身，真实恢复演练仍待联调 |

## 完成前仍需核验

- 对照 AC-01～AC-21 逐项复核证据，补足保护账号、网络故障、AD 写入后数据库失败、并发同步与员工重置等实际边界，不以现有绿灯替代全面审查。
- 使用已验证的测试 OU 和指定钉钉测试员工完成真实写入，包括创建、属性、移动、禁用及本人密码重置；需明确测试对象，禁止拿普通员工账号替代。
- 在实际人员与部门规模下记录全量耗时、单人耗时和同步期间员工页面响应。
- 使用测试环境副本演练恢复，并对真实 AD 重新核验；自动化已覆盖完整应用配置、绑定及迁移恢复，仍不能替代真实目录恢复验收。
- 宿主定时任务已核验且当前 `schedule_enabled=false`、`sspr_enabled=false`、调度作业数为 0；修正部署后还需验证网关是否转发真实客户端地址及限流行为，不打印凭据。
- 后续 PR 仍须通过全部六项门禁并取得合并批准；合并后核验新镜像 SHA、就绪、数据库与外部 HTTPS。
- 仅允许回退到已验证 Django 版本；数据库迁移不兼容时保持现场，依据对应备份人工恢复，不能自动恢复退役旧平台。

原始需求仍以 `PRD-django-single-org.md` 为准。本清单不能缩小 PRD 范围，也不能将替身测试等同于真实目录验收。
