# AD Org Sync 3 — Django 单组织版

只做一个组织的钉钉 → AD 同步，以及员工钉钉身份验证后的 LDAPS 自助密码重置。

**密码重置不依赖同步成功、本地人员表或账号绑定。** 服务端用钉钉可信身份字段实时查询 AD；唯一匹配、未禁用且不受保护的账号可由本人确认重置。提交时重新查询并核对 objectGUID。

## 开发运行

Python 3.10+，Django 5.2 LTS。生产镜像使用 Python 3.12。

```powershell
python -m venv .venv
.venv/Scripts/python -m pip install -e ".[test]"
.venv/Scripts/python manage.py migrate
.venv/Scripts/python manage.py createsuperuser
.venv/Scripts/python manage.py shell -c "from sync_app.models import Configuration; Configuration.current()"
.venv/Scripts/python manage.py collectstatic --noinput
.venv/Scripts/python manage.py serve
# 在另一终端执行
.venv/Scripts/python manage.py worker
```

管理员入口 /login，后台 /dashboard，员工入口 /sspr。设置通过 Django Admin 管理。连接凭据通过环境变量提供，参考 deploy/environment.example；应用不自动加载 .env 文件。

钉钉应用须具备部门和人员详情读取权限，配置可信域名和企业内部应用首页：
`https://<对外域名>/sspr?corpid=$CORPID$`。服务器只使用配置的企业身份，不信任查询字符串中的企业或人员。员工验证 Cookie 始终 Secure，员工端需通过 HTTPS 测试。

同步匹配默认使用唯一工号，邮箱及 userId 匹配仅作人工确认候选；账号命名可选工号、userId、邮箱前缀。密码重置匹配单独配置，可选工号→employeeID、邮箱→mail、userId→sAMAccountName。AD 查询范围由 LDAP_BASE_DN 限定，同步写入进一步限制到配置的根 OU。LDAPS 始终加密，默认 `LDAP_VERIFY_CERT=false`，允许未受信任的自签证书；无需 CA 文件。如需验证可信链和主机名，设置 `LDAP_VERIFY_CERT=true`，可选提供 `LDAP_CA_HOST_FILE`（未提供时使用系统信任库）。

## CI/CD

沿用原仓库流程：草稿 PR → Python 3.10/3.12 质量检查、Windows、容器、Wheel/迁移/SBOM、浏览器回归 → 人工批准合并 → main 的自托管 production runner → 全 SHA 镜像 → 备份、部署、就绪和数据库检查。

部署入口仍为 scripts/deploy-production.sh。没有镜像缓存或代理服务，没有 Redis/Celery。Compose 运行 web、worker、Nginx，以及一次性权限初始化服务；web 和 worker 使用同一镜像及数据卷。

生产环境文件、管理员密码文件权限 0600。LDAP CA 可选提供，提供时由初始化服务复制进只读 secrets 卷。对外必须经 HTTPS 网关，Nginx 默认仅监听宿主 127.0.0.1。若网关位于另一台主机，明确配置私有绑定地址和访问限制。切勿将应用 8010 端口直接公开。

首次重构使用新的 django.sqlite3，不接管旧平台 app.db。部署前应停用旧应用；若旧服务仍在运行，旧 CLI 的备份命令与新版本不同，部署脚本将停止，要求先按旧流程完成备份与退役，不会跳过检查。

已退役的旧平台不得自动恢复。仅当上一成功 SHA 同时记录在 `last_successful_django_image_tag` 中时，失败部署才允许回退到该 Django 版本；首次 Django 部署失败会停止新服务并保留数据，等待排查。该标记只能由完成就绪和数据库检查的部署写入。

连接测试和独立通讯录刷新通过同一个后台任务队列执行。连接测试结果会显示检测时间；通讯录刷新不写 AD，也不会更新全量同步成功时间。

定时同步只有一个入口：生产部署脚本为 runner 账号安装宿主 cron，每分钟在运行中的 Web 容器调用 `enqueue_sync --due`，按后台配置间隔入队，任务仍由 worker 执行。安装会保留宿主其他 cron 项，并可重复执行；示例见 deploy/scheduler.cron.example。`schedule_enabled` 默认关闭，完成真实目录验收后才在设置中开启。无需依赖 Actions checkout 或旧发布目录的软链接。

管理员确认只对当前预览生效；来源、配置、绑定或 AD 状态变化后必须重新预览。每个人成功后提交绑定，部分失败不回滚已经发生的 AD 写入。恢复数据库也不等于回滚 AD。失败或中断时核验逐项结果并重新预览。

## 验证

```powershell
python manage.py check
python manage.py makemigrations --check --dry-run
python -m ruff check sync_app tests --select E9,F821,F841,B007,F541
python -m mypy sync_app/domain.py
python -m pytest -q --ignore=tests/test_browser.py
python -m playwright install chromium
python -m pytest -q tests/test_browser.py
python -m build --wheel
```

真实钉钉、AD 账号及企业证书未在测试中使用。自动化测试使用隔离的适配器替身，端到端真实目录验收应在专用测试 OU 和测试员工上进行。

设计与验收见 docs/PRD-django-single-org.md；本次规则取舍见 docs/rebuild-notes.md。
