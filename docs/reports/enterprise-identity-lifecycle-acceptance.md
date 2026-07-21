# 企业身份全生命周期重构验收报告

验收日期：2026-07-21
验收范围：附件要求的 18 个强制业务场景
结果：18/18 场景通过；验收组合共执行 22 个自动化测试，最终复跑 `22 passed in 79.32s`。

## 场景与自动化证据

| # | 验收场景 | 自动化证据 | 结果 |
|---|---|---|---|
| 1 | 唯一工号自动关联 | `tests/test_enterprise_identity_matching.py::test_unique_employee_id_is_the_only_default_automatic_match` | 通过 |
| 2 | 源平台重复工号时禁止自动关联 | `tests/test_enterprise_identity_matching.py::test_duplicate_employee_id_on_source_blocks_automatic_link` | 通过 |
| 3 | AD 重复 employeeID 时禁止自动关联 | `tests/test_enterprise_identity_matching.py::test_duplicate_ad_employee_id_blocks_automatic_link` | 通过 |
| 4 | 同一员工的钉钉和飞书账号归并到同一企业身份 | `tests/test_enterprise_identity_matching.py::test_same_employee_on_dingtalk_and_feishu_can_resolve_to_same_ad_account` | 通过 |
| 5 | 两个平台字段冲突时进入人工确认 | `tests/test_enterprise_identity_matching.py::test_multi_platform_field_conflict_requires_manual_confirmation` | 通过 |
| 6 | AD 账号已绑定其他人员时阻断重新绑定 | `tests/test_enterprise_identity_matching.py::test_ad_account_already_linked_to_another_identity_is_blocked` | 通过 |
| 7 | 仅姓名相同不得自动关联 | `tests/test_enterprise_identity_matching.py::test_name_only_never_creates_a_match` | 通过 |
| 8 | 唯一邮箱匹配只进入建议关联 | `tests/test_enterprise_identity_matching.py::test_unique_email_is_suggested_and_never_default_auto_linked` | 通过 |
| 9 | 人工永久关联优先于自动规则 | `tests/test_enterprise_identity_matching.py::test_permanent_manual_link_has_precedence_over_generated_rules` | 通过 |
| 10 | 新员工创建账号并进入正确 OU | `tests/test_runtime_dry_run.py::RunSyncDryRunTests::test_run_sync_job_applies_basic_scope_and_directory_root_ou_settings` | 通过 |
| 11 | 转岗员工预览 OU 移动 | `tests/test_runtime_user_operation_classification.py::test_existing_identity_in_different_ou_is_planned_as_move` | 通过 |
| 12 | 离职员工进入延迟禁用预览，服务账号不受影响 | `tests/test_runtime_dry_run.py::RunSyncDryRunTests::test_run_sync_job_queues_offboarding_and_uses_last_synced_manager_state_for_notification`；`tests/test_runtime_nonperson_lifecycle.py::test_service_account_binding_is_excluded_from_source_absence_offboarding` | 通过 |
| 13 | 重新入职不会因同名错误恢复他人账号 | `tests/test_runtime_user_operation_classification.py::test_rehire_reactivation_takes_precedence_over_ou_move`；`tests/test_enterprise_identity_matching.py::test_name_only_never_creates_a_match`；`tests/test_runtime_user_operation_classification.py::test_new_ad_identity_is_planned_as_create` | 通过 |
| 14 | 多部门员工按照主部门进入 OU | `tests/test_runtime_dry_run.py::DepartmentPlacementStrategyTests::test_resolve_target_department_accepts_source_and_legacy_primary_department_aliases` | 通过 |
| 15 | 配置修改后旧 Dry Run 不能 Apply | `tests/test_web_sync_policies.py::WebSyncPolicyTests::test_policy_change_invalidates_dry_run_and_blocks_apply` | 通过 |
| 16 | 批量任务部分失败后重试不重复创建成功账号 | `tests/test_runtime_dry_run.py::RunSyncDryRunTests::test_partial_failure_retry_does_not_recreate_successful_account` | 通过 |
| 17 | 导入关联不会静默覆盖已有绑定 | `tests/test_account_takeover.py::test_takeover_import_never_silently_overwrites_active_legacy_binding` | 通过 |
| 18 | 所有高风险操作具有审批、职责分离和审计记录 | `tests/test_web_execution_center.py::WebExecutionCenterTests::test_review_then_apply_binds_the_exact_plan_and_writes_audit`；`tests/test_web_execution_center.py::WebExecutionCenterTests::test_apply_blocks_when_approver_and_executor_are_the_same_user`；`tests/test_web_account_takeover.py::WebAccountTakeoverTests::test_takeover_web_flow_separates_submit_review_and_execute` | 通过 |

## 补充安全证据

- Apply 在运行时再次校验只读 AD 模式、配置/快照指纹、审批有效性及审批人与执行人分离，不能只依赖 Web 路由。
- 每次通过安全门禁的 Apply 都会先创建真实 SQLite 在线备份，并将路径写入任务结果和审计日志。
- 用户更新、OU 移动和禁用操作保留变更前状态、可回滚字段与不可自动回滚警告；新建 AD 账号明确禁止在回滚中静默删除。
- 账号接管采用“校验、预览、独立审批、独立执行”的原子事务，失败的审批或执行尝试同样进入审计日志。
- 服务、共享、测试账号在统一身份匹配和源缺失离职处理中默认失败关闭。

## 验收命令摘要

验收组合使用项目虚拟环境执行，并指定仓库内独立 `--basetemp`，未连接或修改生产 AD：

```text
.venv\Scripts\python.exe -m pytest -q <上述 22 个测试节点> --basetemp=.tmp_pytest_acceptance18
22 passed, 4 warnings in 79.32s
```

警告均来自第三方 `ldap3/pyasn1` 与 `pypinyin` 的弃用提示，不影响测试结果。

## 全量回归

- 非浏览器套件：`623 passed, 428 subtests passed`；发现的 2 个兼容问题已修复，并通过对应结构守卫、目标适配器和连接器 Web 定向回归（`9 passed`）。
- 真实浏览器套件最终复跑：`32 passed, 196 subtests passed in 985.93s`，覆盖桌面端、平板和 390px 移动端、多语言、键盘流、高风险确认及 Apply 职责分离。
- 最终 18 场景验收复跑：`22 passed in 79.32s`。
