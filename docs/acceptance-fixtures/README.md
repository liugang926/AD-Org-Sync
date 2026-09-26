# 隔离的旧版与 Django 预览对照

两支脚本分别在旧版 `65b9cf7a95a164fcdcec6ea70caa7b5c3e7b2e0c` 和 Django `4dbd0d98446a5893041257400a47219bca5bffe6` 的独立检出中运行。旧版脚本复用该提交的 `tests.helpers.runtime_fakes`，以企微适配器提供与钉钉人员等价的规范化字段；新版脚本复用 Django 的 `tests.fakes`。两边都只连接临时 SQLite 和内存 AD 替身，不连接真实钉钉或 LDAPS，也不执行计划。

在两个检出中分别安装其测试依赖后，从对应检出的根目录执行：

```powershell
# 当前目录：旧版 65b9cf7 的检出
python <Django检出绝对路径>/docs/acceptance-fixtures/legacy_preview_probe.py

# 当前目录：Django 4dbd0d9 或后续兼容版本的检出
python docs/acceptance-fixtures/django_preview_probe.py
```

数据中有五名在职来源人员和一名已关联但从完整来源中消失的人员。`alice` 唯一工号对应现有 AD 账号，`bob` 没有账号，`charlie` 已绑定且显示名变化，`dave` 已绑定且主部门从公司调整到工程部门，`eric` 的工号同时存在于两个不同 AD 账号，`gone` 已绑定且应禁用。根 OU 与工程 OU 均在虚构 `DC=example,DC=com` 下。脚本断言各场景的业务操作，并输出计划供人工复核。

这个对照检验两个运行时预览的**计划决策**，不是逐项字节一致性测试。旧版默认新账号命名可得到 `bob1002`，新版配置默认使用工号 `1002`；旧版额外生成部门群组和成员关系操作，Django 精简版没有这类操作。旧版此路径只按其候选账号集选择 `eric` 的一个目标，未检测到样本中另一个同工号 AD 账号；Django 读取完整受管账号列表并阻断。这个发现只对上述替身及旧版提交成立，不能推断真实目录的全部行为。真实钉钉数据和 AD 写入仍需单独验收。
