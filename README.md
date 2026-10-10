# AD Org Sync 3 — Django 单组织版

只做一个组织的钉钉 → AD 同步，以及员工钉钉身份验证后的 LDAPS 自助密码重置。

**密码重置不依赖同步成功、本地人员表或账号绑定。** 服务端用钉钉可信身份字段实时查询 AD；唯一匹配、已启用的本人账号可确认重置，域管理员也可使用。其他受保护账号仍拒绝；域管理员例外必须由实际同域成员关系确认，不根据 `adminCount` 或组名猜测。验证、账号展示和提交时均实时核验，AD 写入前再次按 objectGUID 读取账号与权限状态；同步的账号保护规则不变。

## 开发运行

Python 3.10+，Django 5.2 LTS。生产镜像使用 Python 3.12。

```powershell
python -m venv .venv
$env:PIP_CONSTRAINT = (Resolve-Path ./constraints.txt).Path
$env:PIP_BUILD_CONSTRAINT = $env:PIP_CONSTRAINT
.venv/Scripts/python -m pip install --upgrade pip
.venv/Scripts/python -m pip install -e ".[test]"
.venv/Scripts/python -m pip check
.venv/Scripts/python manage.py migrate
.venv/Scripts/python manage.py createsuperuser
.venv/Scripts/python manage.py shell -c "from sync_app.models import Configuration; Configuration.current()"
.venv/Scripts/python manage.py collectstatic --noinput
.venv/Scripts/python manage.py serve
# 在另一终端执行
.venv/Scripts/python manage.py worker
```

管理员入口 `/login`，日常控制台 `/dashboard`，人员关联 `/people`，部门映射 `/departments`，操作审计 `/logs`，员工入口 `/sspr`。访问 `/admin/` 会进入日常控制台；底层配置编辑仍通过 Django Admin 管理，并按同步边界、账号规则、自助重置和调度分组。连接凭据通过环境变量提供，参考 deploy/environment.example；应用不自动加载 .env 文件。

控制台“员工页面设置”可编辑员工密码页标题、说明、公告、帮助和联系信息，并维护企业 AD 认证平台的名称、认证说明、登录地址、改密生效说明、显示开关及顺序。初始名称为企业已确认的 VPN、Nextcloud、AI知识库；地址和生效说明留空，由管理员补充。平台只在当前 AD 账号实时核验成功后展示，具体平台权限以各平台授权为准。内容以纯文本展示，链接仅接受 HTTP/HTTPS 且不得携带用户名密码；修改会记录审计。展示数据独立于组织配置，不影响已有同步预览或员工验证会话；安全校验、密码策略和改密结果仍由程序控制。

钉钉应用须具备部门和人员详情读取权限，配置可信域名和企业内部微应用首页。新配置可使用规范入口 `https://it-service.tianjizn.com:9443/sspr`；当前移动端和 PC 端首页均在相同 HTTPS 域名和端口的 `/sspr/callback/dingtalk`，该路径与 `/sspr` 映射到同一员工页面，可直接用作工作台入口。打开后页面自动通过[钉钉微应用免登 JSAPI](https://open.dingtalk.com/tools/explorer/jsapi?id=11723)取得授权码，服务端实时唯一匹配 LDAPS 后展示本人当前完整 AD 账号；验证失败时才显示重试入口，未验证访客看不到账号。免登使用 Client ID（原 AppKey）和服务器配置的 CorpId；AgentId 不是 Client ID。首页的 `corpid=$CORPID$` 占位符是可选的传值方式，本应用不依赖它，也不信任查询字符串中的企业或人员。员工验证 Cookie 始终 Secure；生产开关及允许范围由受限配置控制，真实员工验收仍须本人在工作台完成。

同步匹配默认使用唯一工号，邮箱及 userId 匹配仅作人工确认候选；账号命名可选工号、userId、邮箱前缀。密码重置匹配单独配置，可选工号→employeeID、邮箱→mail、userId→sAMAccountName，或工号→sAMAccountName。最后一种用于 AD 登录名是工号但 employeeID 未填写的情况，需要管理员明确选择；不会自动尝试其他身份字段。后台显示当前 LDAPS 目录及员工开放范围，不回显连接凭据。AD 查询范围由 LDAP_BASE_DN 限定，同步写入进一步限制到配置的根 OU。LDAPS 始终加密，默认 `LDAP_VERIFY_CERT=false`，允许未受信任的自签证书；无需 CA 文件。如需验证可信链和主机名，设置 `LDAP_VERIFY_CERT=true`，可选提供 `LDAP_CA_HOST_FILE`（未提供时使用系统信任库）。

选择“钉钉工号 → AD 账号名”后，默认开启自动账号关联。现有 worker 在首次已有完整通讯录、来源或关联设置变化时核验，并每小时复核；人员页也可手动“刷新账号关联”。任务重新读取完整来源与 AD，再复核可信工号和实际 objectGUID，保存真实的本地账号关联。人工及已有对象优先，重复、占用、排除和未完成建号证据不会被自动覆盖。页面区分已关联账号、仅账号关联和已纳入同步；根 OU 外或受保护账号可以维护身份关联，实际 AD 更新仍须通过受管范围预览确认，离职禁用只处理已纳入管理的账号。人工修改仍先核验、再确认并记录原因；员工改密授权继续独立实时匹配。

生产与测试应使用各自确认的 AD 连接和数据目录。Compose 默认仍使用 `/data`；可以在受限环境文件中设置 `AD_ORG_SYNC_DATA_DIR=/data/production`，在同一持久卷中初始化独立数据库，原测试数据仍保留。新目录不会自动复制账号绑定、身份锚点或员工会话；首次启用生产目录前应完成备份、设置及独立验证。不得为了切换目录直接清空或复用旧 AD 绑定。AD 查询使用 [Microsoft Domain Scope 控制](https://learn.microsoft.com/en-us/previous-versions/windows/desktop/ldap/ldap-server-domain-scope-oid)，限定单一命名上下文并避免域根引用；仍拒绝不完整查询和未处理引用。

受控验收可在受限环境文件中配置 `SSPR_ALLOWED_DINGTALK_USER_IDS`，用逗号列出允许的钉钉 userId（不是工号）。配置非空时，只有名单内员工通过钉钉验证后可继续实时 LDAPS 匹配与重置；名单变化使现有验证会话失效。不配置时维持原有的全员匹配行为；无论名单如何，后台的 `sspr_enabled` 开关仍须明确开启。

## 组织架构与根 OU

部门页“预览组织架构”或概览页“仅同步组织架构”生成完整部门树的 OU 计划，包含空部门，不匹配或修改 AD 人员账号，也不受工号、主部门等人员冲突阻断。预览使用只读 LDAP 连接；只有在任务页确认执行后才建立 OU 和部门 GUID 映射。

根 OU 已填写时，以该 DN 为准；留空时，若 `LDAP_BASE_DN` 是域 DN，则使用钉钉根部门名称计算 `OU=<根部门名称>,<LDAP_BASE_DN>`，若查询范围本身是 OU，则直接复用该 OU，避免重复公司层级。只精确查找这个路径，不按同名搜索接管其他子树。已存在则复用，缺失则在预览中列出根 OU 创建及父容器；父容器必须存在并在查询范围内。定时任务遇到缺失根 OU 也会停在待确认状态。

钉钉根部门直接对应本次根 OU，子部门沿父子关系逐层生成；手工部门映射的子部门继承完整映射路径。执行前复核来源、配置、映射、OU 及根父容器 GUID，变化后必须重新预览。已绑定部门改名或调整层级仍需人工核对，不自动移动、删除或重建旧 OU。

现有配置（包括测试名称 OU）不会自动清空或替换。采用默认公司根之前，应在组织配置中清空根 OU，并审查已有部门及受管人员绑定；已有同步绑定时保留范围变更保护。新规则不会删除测试 OU，也不会自动将其中人员迁入新根。升级前的旧计划须重新预览。

## 依赖版本与安装

`pyproject.toml` 固定直接依赖版本，仓库的 `constraints.txt` 固定运行、测试及构建依赖的完整版本集合，并使用环境标记适配 Python 3.10+ 与 Windows/Linux。安装时将 `PIP_CONSTRAINT` 和 `PIP_BUILD_CONSTRAINT` 都指向该文件的绝对路径，再升级到文件指定的 pip 版本。第二项约束同时控制隔离构建环境中的 setuptools、wheel 等依赖；CI 和生产 Docker 构建采用相同规则。

从源码安装运行环境，在仓库根目录执行：

```bash
python3 -m venv .venv
export PIP_CONSTRAINT="$(pwd)/constraints.txt"
export PIP_BUILD_CONSTRAINT="$PIP_CONSTRAINT"
.venv/bin/python -m pip install --upgrade pip
.venv/bin/python -m pip install .
.venv/bin/python -m pip check
```

通过发布包安装时，将同一次发布的 wheel 与 `constraints.txt` 下载到同一目录，在该目录创建虚拟环境并设置上述两个绝对路径变量，然后执行：

```bash
.venv/bin/python -m pip install --upgrade pip
.venv/bin/python -m pip install ./ad_org_sync-3.0.0-py3-none-any.whl
.venv/bin/python -m pip check
```

约束文件选择依赖版本，实际安装集合由源码或 wheel 的依赖声明决定；生产安装仅包含运行所需包，开发和测试使用 `.[test]`。Wheel、SBOM 的 CI 产物及正式 wheel 发布均附带同一约束文件。Docker 仅在 builder 阶段读取该文件，最终运行镜像中的应用按锁定版本安装。

更新依赖时，先修改 `pyproject.toml` 中相应直接依赖或构建组的版本，再重新生成 `constraints.txt`。可选维护工具 uv 仅用于生成该文件：

```bash
uv pip compile pyproject.toml --extra test --group build --universal --python-version 3.10 --no-header --no-annotate --output-file constraints.txt
```

需要更新传递依赖时，在上述命令增加 `--upgrade`，并审查生成的版本及环境标记。两份文件一并提交，经 Python 3.10/3.12、Windows、容器、Wheel/迁移/SBOM 和浏览器六项 CI 验证后，按现有 PR 批准及 CI/CD 流程发布。

## CI/CD

沿用原仓库流程：草稿 PR → Python 3.10/3.12 质量检查、Windows、容器、Wheel/迁移/SBOM、浏览器回归 → 人工批准合并 → main 的自托管 production runner → 全 SHA 镜像 → 备份、部署、就绪和数据库检查。

部署入口仍为 scripts/deploy-production.sh。没有镜像缓存或代理服务，没有 Redis/Celery。Compose 运行 web、worker、Nginx，以及一次性权限初始化服务；web 和 worker 使用同一镜像及数据卷。

生产环境文件、管理员密码文件权限 0600。LDAP CA 可选提供，提供时由初始化服务复制进只读 secrets 卷。对外必须经 HTTPS 网关，Nginx 默认仅监听宿主 127.0.0.1。若网关位于另一台主机，明确配置私有绑定地址和访问限制。切勿将应用 8010 端口直接公开。

密码重置钉钉机器人可选启用：将完整 Webhook 保存到权限 0400 或 0600 的宿主文件，在受限环境文件设置 `DINGTALK_PASSWORD_ROBOT_WEBHOOK_HOST_FILE`；开启机器人加签时另设 `DINGTALK_PASSWORD_ROBOT_SIGN_SECRET_HOST_FILE`。未配置 Webhook 文件才关闭通知；明确配置后文件缺失、为空或不可读则记录通知配置失败，不影响已成功改密。未配置签名文件表示不加签；明确配置后文件失效则记录配置失败，不会自动取消加签。初始化服务复制为 UID/GID 10001、权限 0400，web 和 worker 通过现有只读 secrets 卷访问；未提供时移除对应机器人文件，其他秘密文件保留。直接运行开发服务时使用 `DINGTALK_PASSWORD_ROBOT_WEBHOOK_FILE` 和可选的 `DINGTALK_PASSWORD_ROBOT_SIGN_SECRET_FILE` 指定文件路径，默认关闭。URL、访问令牌和签名秘密不得写入源码、普通环境变量或管理页面。

首次重构使用新的 django.sqlite3，不接管旧平台 app.db。部署前应停用旧应用；若旧服务仍在运行，旧 CLI 的备份命令与新版本不同，部署脚本将停止，要求先按旧流程完成备份与退役，不会跳过检查。

已退役的旧平台不得自动恢复。仅当上一成功 SHA 同时记录在 `last_successful_django_image_tag` 中时，失败部署才允许回退到该 Django 版本；首次 Django 部署失败会停止新服务并保留数据，等待排查。该标记只能由完成就绪和数据库检查的部署写入。

连接测试和独立通讯录刷新通过同一个后台任务队列执行。连接测试结果会显示检测时间；通讯录刷新不写 AD，也不会更新全量同步成功时间。

概览和任务详情在排队、执行期间每 5 秒自动更新任务状态，并显示等待说明及已用时间；概览更新保留未提交的表单。任务详情在结束后自动展示结果或待审阅计划，不会自动确认执行。通讯录读取耗时取决于部门数量和钉钉接口响应；账号关联核验只保存本地身份关系，不创建部门 OU。网络中断时页面提示重试，登录失效时提示重新登录；未启用 JavaScript 时可手动刷新。

定时同步只有一个入口：生产部署脚本为 runner 账号安装宿主 cron，每分钟在运行中的 Web 容器调用 `enqueue_sync --due`，按后台配置间隔入队，任务仍由 worker 执行。同一入口每天清理过期会话、旧快照、任务和审计；即使定时同步关闭也执行清理，遇到正在运行的同步则下一分钟重试。安装会保留宿主其他 cron 项，并可重复执行；示例见 deploy/scheduler.cron.example。`schedule_enabled` 默认关闭，完成真实目录验收后才在设置中开启。无需依赖 Actions checkout 或旧发布目录的软链接。

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

自动化测试使用隔离的适配器替身。真实钉钉来源、隔离数据库与测试 AD 已完成专用员工的新建和重跑；属性更新、人工 OU 映射移动及无绑定重置另有实际目录证据。用户已反馈正式员工实际改密与新密码登录验收完成。当前正式同步预览仍因受管范围、主部门和账号状态等冲突阻断，未执行整批写入；真实来源部门变更、缺失禁用、并发本人改密和机器人实际投递仍待各自验收，详见 [验收状态](docs/acceptance-status.md)。

设计与验收见 docs/PRD-django-single-org.md；本次规则取舍见 docs/rebuild-notes.md。
