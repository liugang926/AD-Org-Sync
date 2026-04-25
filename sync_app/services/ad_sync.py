import configparser
import json
import logging
import ssl
import time
import uuid
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from sync_app.core.config import build_tls_config
from sync_app.core.directory_protection import is_protected_ad_account_name
from sync_app.core.sync_policies import normalize_group_type
from sync_app.core.models import DepartmentGroupInfo, DirectoryGroupRecord, DirectoryUserRecord
from sync_app.infra.ldap_compat import (
    ALL,
    NTLM,
    SIMPLE,
    MODIFY_ADD,
    MODIFY_DELETE,
    MODIFY_REPLACE,
    Connection,
    LDAPBindError,
    LDAPException,
    Server,
    Tls,
    ensure_ldap3_available,
    escape_filter_chars,
)
from sync_app.storage.local_db import ManagedGroupBindingRepository


def build_group_sam(department_id: int | str) -> str:
    return f"WECOM_D{department_id}"


def build_group_cn(ou_name: str, department_id: int | str) -> str:
    return f"{ou_name}__D{department_id}"


def build_group_display_name(path_segments: List[str], department_id: int | str, separator: str = "-") -> str:
    cleaned_segments = [segment.strip() for segment in path_segments if segment and segment.strip()]
    if not cleaned_segments:
        return f"D{department_id}"
    return f"{separator.join(cleaned_segments)} [D{department_id}]"


def build_custom_group_sam(source_type: str, source_key: str) -> str:
    normalized_type = "".join(char for char in str(source_type or "").upper() if char.isalnum())[:8] or "CUSTOM"
    normalized_key = "".join(char for char in str(source_key or "").upper() if char.isalnum())[:32] or "ITEM"
    return f"WECOM_{normalized_type}_{normalized_key}"[:64]


def build_custom_group_cn(source_type: str, display_name: str, source_key: str) -> str:
    base_name = str(display_name or "").strip() or f"{source_type}-{source_key}"
    return f"{base_name}__{source_type}_{source_key}"[:64]



class ADSyncLDAPS:
    """使用LDAPS协议的AD同步类"""
    
    def __init__(
        self,
        server: str,
        domain: str,
        username: str,
        password: str,
        use_ssl: bool = True,
        port: int = None,
        exclude_departments: List[str] = None,
        exclude_accounts: List[str] = None,
        default_password: str = "",
        force_change_password: bool = True,
        password_complexity: str = "strong",
        validate_cert: bool = True,
        ca_cert_path: str = "",
        disabled_users_ou_name: str = "Disabled Users",
        managed_group_type: str = "security",
        managed_group_mail_domain: str = "",
        custom_group_ou_path: str = "Managed Groups",
        user_root_ou_path: str = "",
    ):
        """
        初始化LDAPS连接
        
        参数:
            server: LDAP服务器地址（如：dc.notting.com.cn）
            domain: 域名（如：notting.com.cn）
            username: 管理员账户（如：administrator 或 NOTTING\\administrator）
            password: 管理员密码
            use_ssl: 是否使用SSL/TLS加密连接
            port: LDAP端口（默认：636用于LDAPS，389用于LDAP）
            exclude_departments: 排除的部门列表
            exclude_accounts: 排除的账户列表
        """
        ensure_ldap3_available()

        self.server_address = server
        self.domain = domain
        self.username = username
        self.password = password
        self.use_ssl = use_ssl
        self.exclude_departments = exclude_departments or []
        self.exclude_accounts = exclude_accounts or []
        self.default_password = default_password.strip()
        self.force_change_password = force_change_password
        self.password_complexity = (password_complexity or "strong").strip().lower()
        self.validate_cert = validate_cert
        self.ca_cert_path = ca_cert_path.strip()
        self.user_root_ou_path = self._normalize_ou_path_segments(user_root_ou_path)
        self.disabled_users_ou_path = self._normalize_ou_path_segments(disabled_users_ou_name or "Disabled Users")
        if not self.disabled_users_ou_path:
            self.disabled_users_ou_path = ["Disabled Users"]
        self.disabled_users_ou_name = self.disabled_users_ou_path[-1]
        self.managed_group_type = normalize_group_type(managed_group_type)
        self.managed_group_mail_domain = str(managed_group_mail_domain or "").strip()
        self.custom_group_ou_path = "/".join(
            self._normalize_ou_path_segments(custom_group_ou_path or "Managed Groups")
        ) or "Managed Groups"
        self.logger = logging.getLogger(__name__)
        if self.use_ssl and not self.validate_cert:
            self.logger.warning("LDAPS certificate validation is disabled")
        
        # 设置重试参数
        self.max_retries = 3
        self.retry_delay = 2
        self.base_dn = ','.join([f"DC={part}" for part in domain.split('.') if part])
        self.port = 636 if port is None and use_ssl else 389 if port is None else port
        if use_ssl:
            tls_config = build_tls_config(validate_cert=self.validate_cert, ca_cert_path=self.ca_cert_path)
            self.server = Server(
                server,
                port=self.port,
                use_ssl=True,
                tls=tls_config,
                get_info=ALL
            )
        else:
            self.server = Server(server, port=self.port, get_info=ALL)
        self.connection = None
        self._connect()
        self.ensure_user_root_ou_path()
        self.ensure_disabled_users_ou()

    def _normalize_ou_path_segments(self, raw_value: Any) -> List[str]:
        if isinstance(raw_value, (list, tuple)):
            raw_segments = raw_value
        else:
            raw_text = str(raw_value or "").strip()
            dn_segments = [
                part.split("=", 1)[1].strip()
                for part in raw_text.split(",")
                if "=" in part and part.strip().lower().startswith("ou=") and part.split("=", 1)[1].strip()
            ]
            if dn_segments:
                raw_segments = list(reversed(dn_segments))
            else:
                raw_segments = raw_text.replace("\\", "/").split("/")
        return [str(segment).strip() for segment in raw_segments if str(segment).strip()]

    def _build_ou_dn(self, ou_path: List[str]) -> str:
        normalized_path = self._normalize_ou_path_segments(ou_path)
        if not normalized_path:
            return self.base_dn
        return ','.join([f"OU={ou}" for ou in reversed(normalized_path)] + [self.base_dn])

    def _user_scoped_ou_path(self, ou_path: List[str]) -> List[str]:
        return [*self.user_root_ou_path, *self._normalize_ou_path_segments(ou_path)]

    def _is_protected_account(self, username: str) -> bool:
        return is_protected_ad_account_name(username, self.exclude_accounts)
    
    def _connect(self):
        """建立LDAP连接"""
        try:
            # 转换用户名格式为UPN（user@domain）或DN格式
            username = self._convert_username_format(self.username)
            
            # 优先尝试NTLM认证（Windows AD推荐），失败则使用SIMPLE认证
            auth_method = NTLM
            try:
                self.connection = Connection(
                    self.server,
                    user=self.username,
                    password=self.password,
                    authentication=NTLM,
                    auto_bind=True,
                    receive_timeout=30
                )
                self.logger.info(f"成功连接到LDAP服务器 (NTLM): {self.server_address}:{self.port} (SSL: {self.use_ssl})")
            except Exception as ntlm_error:
                # NTLM失败（可能是MD4问题），尝试SIMPLE认证
                if "MD4" in str(ntlm_error) or "unsupported hash type" in str(ntlm_error):
                    self.logger.warning("NTLM认证失败（MD4不支持），尝试SIMPLE认证...")
                    auth_method = SIMPLE
                    
                    self.connection = Connection(
                        self.server,
                        user=username,
                        password=self.password,
                        authentication=SIMPLE,
                        auto_bind=True,
                        receive_timeout=30
                    )
                    self.logger.info(f"成功连接到LDAP服务器 (SIMPLE): {self.server_address}:{self.port} (SSL: {self.use_ssl})")
                else:
                    raise
                    
        except LDAPBindError as e:
            self.logger.error(f"LDAP认证失败: {str(e)}")
            raise
        except LDAPException as e:
            self.logger.error(f"LDAP连接失败: {str(e)}")
            raise
    
    def _convert_username_format(self, username: str) -> str:
        """
        转换用户名格式为适合SIMPLE认证的格式
        DOMAIN\\username -> username@domain 或 CN=username,CN=Users,DC=domain,DC=com
        """
        # 如果是 DOMAIN\username 格式
        if '\\' in username:
            parts = username.split('\\')
            if len(parts) == 2:
                # 转换为 username@domain 格式
                return f"{parts[1]}@{self.domain}"
        
        # 如果是 username@domain 格式，直接返回
        if '@' in username:
            return username
        
        # 如果是纯用户名，添加域名
        return f"{username}@{self.domain}"
    
    def _reconnect(self):
        """重新连接LDAP"""
        try:
            if self.connection:
                self.connection.unbind()
            self._connect()
        except Exception as e:
            self.logger.error(f"重新连接失败: {str(e)}")
            raise
    
    def _execute_with_retry(self, func, *args, **kwargs):
        """执行LDAP操作并在失败时重试"""
        for attempt in range(self.max_retries):
            try:
                return func(*args, **kwargs)
            except LDAPException as e:
                self.logger.warning(f"LDAP操作失败 (尝试 {attempt+1}/{self.max_retries}): {str(e)}")
                if attempt < self.max_retries - 1:
                    time.sleep(self.retry_delay)
                    self._reconnect()
                else:
                    self.logger.error(f"LDAP操作失败，已达到最大重试次数")
                    raise
        
    def get_ou_dn(self, ou_path: List[str]) -> str:
        """获取OU的Distinguished Name"""
        return self._build_ou_dn(self._user_scoped_ou_path(ou_path))
    
    def ou_exists(self, ou_dn: str) -> bool:
        """检查OU是否存在"""
        try:
            result = self.connection.search(
                ou_dn,
                '(objectClass=organizationalUnit)',
                search_scope='BASE'
            )
            return result and len(self.connection.entries) > 0
        except LDAPException:
            return False
    
    def list_organizational_units(self) -> List[Dict[str, Any]]:
        items: List[Dict[str, Any]] = [
            {
                "name": self.domain or "Domain Root",
                "dn": self.base_dn,
                "path": [],
                "guid": "",
            }
        ]
        try:
            self.connection.search(
                self.base_dn,
                "(objectClass=organizationalUnit)",
                attributes=["ou", "distinguishedName", "objectGUID"],
            )
            for entry in self.connection.entries:
                dn = str(getattr(entry, "entry_dn", "") or "")
                if not dn:
                    continue
                path = self._normalize_ou_path_segments(dn)
                if not path:
                    continue
                guid_value = ""
                raw_guid = getattr(entry, "objectGUID", None)
                guid_bytes = getattr(raw_guid, "value", None)
                if isinstance(guid_bytes, (bytes, bytearray)) and len(guid_bytes) == 16:
                    guid_value = str(uuid.UUID(bytes_le=bytes(guid_bytes)))
                items.append(
                    {
                        "name": str(getattr(getattr(entry, "ou", None), "value", "") or path[-1]),
                        "dn": dn,
                        "path": path,
                        "guid": guid_value,
                    }
                )
        except LDAPException as exc:
            self.logger.error("failed to enumerate organizational units: %s", exc)
            raise
        items.sort(
            key=lambda item: (
                len(item.get("path") or []),
                [segment.lower() for segment in item.get("path") or []],
            )
        )
        return items

    def ensure_ou(self, ou_name: str, parent_dn: str) -> Tuple[bool, str, bool]:
        """确保OU存在，返回(成功, ou_dn, 是否新建)"""
        try:
            ou_name = ou_name.strip() if ou_name else ''
            if not ou_name:
                self.logger.error("OU名称为空，跳过创建")
                return False, '', False

            if ou_name in self.exclude_departments:
                self.logger.info(f"跳过创建OU: {ou_name} (在排除列表中)")
                return True, '', False

            ou_dn = f"OU={ou_name},{parent_dn}"
            if not self.ou_exists(ou_dn):
                ou_attributes = {
                    'objectClass': ['top', 'organizationalUnit'],
                    'ou': ou_name
                }
                if self.connection.add(ou_dn, attributes=ou_attributes):
                    self.logger.info(f"创建OU成功: {ou_name}")
                    return True, ou_dn, True
                else:
                    self.logger.error(f"创建OU失败: {ou_name}, 错误: {self.connection.result}")
                    return False, ou_dn, False

            self.logger.debug(f"OU已存在: {ou_name}")
            return True, ou_dn, False
        except Exception as e:
            self.logger.error(f"创建OU过程出错: {str(e)}")
            return False, '', False

    def create_ou(self, ou_name: str, parent_dn: str) -> bool:
        """保持向后兼容的OU创建接口"""
        success, _, _ = self.ensure_ou(ou_name, parent_dn)
        return success

    def get_group_by_sam(self, group_sam: str) -> Optional[DirectoryGroupRecord]:
        try:
            search_filter = f"(&(objectClass=group)(sAMAccountName={escape_filter_chars(group_sam)}))"
            self.connection.search(
                self.base_dn,
                search_filter,
                attributes=['cn', 'displayName', 'description', 'sAMAccountName', 'distinguishedName']
            )
            if not self.connection.entries:
                return None
            entry = self.connection.entries[0]
            return DirectoryGroupRecord(
                dn=entry.entry_dn,
                cn=entry.cn.value if hasattr(entry, 'cn') else '',
                display_name=entry.displayName.value if hasattr(entry, 'displayName') else '',
                description=entry.description.value if hasattr(entry, 'description') else '',
                group_sam=entry.sAMAccountName.value if hasattr(entry, 'sAMAccountName') else group_sam,
            )
        except Exception as e:
            self.logger.error(f"查询安全组失败 {group_sam}: {str(e)}")
            return None

    def find_legacy_group_in_ou(self, ou_dn: str, legacy_name: str) -> Optional[DirectoryGroupRecord]:
        try:
            safe_name = escape_filter_chars(legacy_name)
            search_filter = f"(&(objectClass=group)(|(sAMAccountName={safe_name})(cn={safe_name})))"
            self.connection.search(
                ou_dn,
                search_filter,
                search_scope='LEVEL',
                attributes=['cn', 'displayName', 'description', 'sAMAccountName', 'distinguishedName']
            )
            if not self.connection.entries:
                return None
            entry = self.connection.entries[0]
            return DirectoryGroupRecord(
                dn=entry.entry_dn,
                cn=entry.cn.value if hasattr(entry, 'cn') else legacy_name,
                display_name=entry.displayName.value if hasattr(entry, 'displayName') else '',
                description=entry.description.value if hasattr(entry, 'description') else '',
                group_sam=entry.sAMAccountName.value if hasattr(entry, 'sAMAccountName') else legacy_name,
            )
        except Exception as e:
            self.logger.debug(f"查询历史安全组失败 {legacy_name}: {str(e)}")
            return None

    def inspect_department_group(
        self,
        department_id: int,
        ou_name: str,
        ou_dn: str,
        full_path: List[str],
        display_separator: str = "-",
    ) -> DepartmentGroupInfo:
        group_sam = build_group_sam(department_id)
        group_cn = build_group_cn(ou_name, department_id)
        display_name = build_group_display_name(full_path, department_id, display_separator)
        description = f"source=directory; dept_id={department_id}; path={'/'.join(full_path)}"

        managed_group = self.get_group_by_sam(group_sam)
        if managed_group:
            return DepartmentGroupInfo(
                exists=True,
                group_sam=managed_group.group_sam,
                group_cn=managed_group.cn,
                group_dn=managed_group.dn,
                display_name=managed_group.display_name or display_name,
                description=managed_group.description or description,
                binding_source='managed',
                created=False,
            )

        legacy_group = self.find_legacy_group_in_ou(ou_dn, ou_name)
        if legacy_group:
            return DepartmentGroupInfo(
                exists=True,
                group_sam=legacy_group.group_sam,
                group_cn=legacy_group.cn,
                group_dn=legacy_group.dn,
                display_name=legacy_group.display_name or display_name,
                description=legacy_group.description or description,
                binding_source='legacy',
                created=False,
            )

        return DepartmentGroupInfo(
            exists=False,
            group_sam=group_sam,
            group_cn=group_cn,
            group_dn=f"CN={group_cn},{ou_dn}",
            display_name=display_name,
            description=description,
            binding_source='new',
            created=False,
        )

    def ensure_department_group(
        self,
        department_id: int,
        parent_department_id: Optional[int],
        ou_name: str,
        ou_dn: str,
        full_path: List[str],
        display_separator: str = "-",
        binding_repo: Optional[ManagedGroupBindingRepository] = None,
    ) -> DepartmentGroupInfo:
        group_info = self.inspect_department_group(
            department_id=department_id,
            ou_name=ou_name,
            ou_dn=ou_dn,
            full_path=full_path,
            display_separator=display_separator,
        )

        if not group_info.exists:
            group_attributes = self._build_group_attributes(
                group_cn=group_info.group_cn,
                group_sam=group_info.group_sam,
                display_name=group_info.display_name,
                description=group_info.description,
            )
            if self.connection.add(group_info.group_dn, attributes=group_attributes):
                self.logger.info(f"创建部门安全组成功: {group_info.group_sam} -> {group_info.group_dn}")
                group_info.exists = True
                group_info.created = True
            else:
                result = self.connection.result
                error_code = result.get('result', 0)
                if error_code == 68:
                    existing_group = self.get_group_by_sam(group_info.group_sam)
                    if existing_group:
                        group_info.exists = True
                        group_info.group_dn = existing_group.dn
                        group_info.group_cn = existing_group.cn
                        group_info.display_name = existing_group.display_name or group_info.display_name
                        group_info.description = existing_group.description or group_info.description
                        group_info.binding_source = 'managed'
                    else:
                        raise Exception(f"安全组 {group_info.group_sam} 已存在但无法解析DN")
                else:
                    raise Exception(f"创建安全组失败: {result}")

        if binding_repo and group_info.exists:
            binding_repo.upsert_binding(
                department_id=str(department_id),
                parent_department_id=str(parent_department_id) if parent_department_id else None,
                group_sam=group_info.group_sam,
                group_dn=group_info.group_dn,
                group_cn=group_info.group_cn,
                display_name=group_info.display_name,
                path_text='/'.join(full_path),
                status='active',
            )

        return group_info

    def _group_type_value(self) -> int:
        if self.managed_group_type == 'distribution':
            return 2
        return -2147483646

    def _build_group_attributes(
        self,
        *,
        group_cn: str,
        group_sam: str,
        display_name: str,
        description: str,
    ) -> Dict[str, Any]:
        attributes: Dict[str, Any] = {
            'objectClass': ['top', 'group'],
            'cn': group_cn,
            'sAMAccountName': group_sam,
            'displayName': display_name,
            'description': description,
            'groupType': self._group_type_value(),
        }
        if self.managed_group_type in {'distribution', 'mail_enabled_security'} and self.managed_group_mail_domain:
            alias = group_sam.lower()
            mail = f"{alias}@{self.managed_group_mail_domain}"
            attributes['mailNickname'] = alias
            attributes['mail'] = mail
            attributes['proxyAddresses'] = [f"SMTP:{mail}"]
        return attributes

    def ensure_ou_path(self, ou_path: List[str]) -> Tuple[bool, str]:
        current_parent_dn = self.base_dn
        current_dn = self.base_dn
        for ou_name in self._normalize_ou_path_segments(ou_path):
            success, current_dn, _ = self.ensure_ou(ou_name, current_parent_dn)
            if not success:
                return False, current_dn
            current_parent_dn = current_dn
        return True, current_dn

    def ensure_user_root_ou_path(self) -> bool:
        if not self.user_root_ou_path:
            return True
        success, _ = self.ensure_ou_path(self.user_root_ou_path)
        return success

    def get_disabled_users_ou_dn(self) -> str:
        return self._build_ou_dn(self.disabled_users_ou_path)

    def inspect_custom_group(
        self,
        *,
        source_type: str,
        source_key: str,
        display_name: str,
        ou_dn: str,
    ) -> DepartmentGroupInfo:
        group_sam = build_custom_group_sam(source_type, source_key)
        existing_group = self.get_group_by_sam(group_sam)
        description = f"source=custom_{source_type}; source_key={source_key}; display_name={display_name}"
        group_cn = build_custom_group_cn(source_type, display_name, source_key)
        if existing_group:
            return DepartmentGroupInfo(
                exists=True,
                group_sam=existing_group.group_sam,
                group_cn=existing_group.cn,
                group_dn=existing_group.dn,
                display_name=existing_group.display_name or display_name,
                description=existing_group.description or description,
                binding_source='managed',
                created=False,
            )

        return DepartmentGroupInfo(
            exists=False,
            group_sam=group_sam,
            group_cn=group_cn,
            group_dn=f"CN={group_cn},{ou_dn}",
            display_name=display_name,
            description=description,
            binding_source='new',
            created=False,
        )

    def ensure_custom_group(
        self,
        *,
        source_type: str,
        source_key: str,
        display_name: str,
        ou_path: Optional[List[str]] = None,
    ) -> DepartmentGroupInfo:
        desired_ou_path = [segment.strip() for segment in (ou_path or []) if str(segment or '').strip()]
        if not desired_ou_path:
            desired_ou_path = [segment.strip() for segment in self.custom_group_ou_path.split('/') if segment.strip()]
        success, ou_dn = self.ensure_ou_path(desired_ou_path)
        if not success:
            raise Exception(f"failed to ensure custom group OU path: {'/'.join(desired_ou_path)}")

        group_info = self.inspect_custom_group(
            source_type=source_type,
            source_key=source_key,
            display_name=display_name,
            ou_dn=ou_dn,
        )
        if group_info.exists:
            return group_info

        group_attributes = self._build_group_attributes(
            group_cn=group_info.group_cn,
            group_sam=group_info.group_sam,
            display_name=group_info.display_name,
            description=group_info.description,
        )
        if self.connection.add(group_info.group_dn, attributes=group_attributes):
            group_info.exists = True
            group_info.created = True
            return group_info

        result = self.connection.result
        if result.get('result', 0) == 68:
            existing_group = self.get_group_by_sam(group_info.group_sam)
            if existing_group:
                group_info.exists = True
                group_info.group_dn = existing_group.dn
                group_info.group_cn = existing_group.cn
                group_info.display_name = existing_group.display_name or group_info.display_name
                group_info.description = existing_group.description or group_info.description
                group_info.binding_source = 'managed'
                return group_info
        raise Exception(f"failed to ensure custom group {source_type}:{source_key}: {result}")
    
    def get_user(self, username: str) -> Optional[dict]:
        """获取AD用户信息"""
        try:
            search_filter = f"(&(objectClass=user)(sAMAccountName={escape_filter_chars(username)}))"
            self.connection.search(
                self.base_dn,
                search_filter,
                attributes=['*']
            )
            
            if self.connection.entries:
                entry = self.connection.entries[0]
                return json.loads(entry.entry_to_json())
            return None
        except Exception as e:
            self.logger.error(f"获取用户信息失败: {str(e)}")
            return None
    
    def get_users_batch(self, usernames: List[str]) -> Dict[str, DirectoryUserRecord]:
        """
        批量获取AD用户信息
        
        参数:
            usernames: 用户名列表
            
        返回:
            字典 {username: user_info}
        """
        try:
            if not usernames:
                return {}
            usernames = [username for username in usernames if not self._is_protected_account(username)]
            if not usernames:
                return {}
            
            # 构建批量查询过滤器
            # (|(sAMAccountName=user1)(sAMAccountName=user2)...)
            user_filters = ''.join([f"(sAMAccountName={escape_filter_chars(u)})" for u in usernames])
            search_filter = f"(&(objectClass=user)(|{user_filters}))"
            
            self.logger.info(f"批量查询 {len(usernames)} 个用户...")
            
            self.connection.search(
                self.base_dn,
                search_filter,
                attributes=['sAMAccountName', 'distinguishedName', 'displayName', 'mail', 'userPrincipalName']
            )
            
            # 构建结果字典
            result: Dict[str, DirectoryUserRecord] = {}
            for entry in self.connection.entries:
                username = entry.sAMAccountName.value if hasattr(entry, 'sAMAccountName') else None
                if username:
                    result[username] = DirectoryUserRecord.from_ldap_json(json.loads(entry.entry_to_json()))
            
            self.logger.info(f"批量查询完成，找到 {len(result)}/{len(usernames)} 个用户")
            return result
            
        except Exception as e:
            self.logger.error(f"批量获取用户信息失败: {str(e)}")
            return {}
    
    def check_email_exists(self, email: str, exclude_user: str = None) -> bool:
        """检查邮箱是否已被其他用户使用"""
        try:
            safe_email = escape_filter_chars(email)
            if exclude_user:
                search_filter = f"(&(objectClass=user)(mail={safe_email})(!(sAMAccountName={escape_filter_chars(exclude_user)})))"
            else:
                search_filter = f"(&(objectClass=user)(mail={safe_email}))"
            
            self.connection.search(
                self.base_dn,
                search_filter,
                attributes=['sAMAccountName']
            )
            
            return len(self.connection.entries) > 0
        except Exception as e:
            self.logger.error(f"检查邮箱是否存在时出错: {str(e)}")
            return False
    
    def get_user_email(self, username: str) -> str:
        """获取AD用户当前的邮箱地址"""
        try:
            search_filter = f"(&(objectClass=user)(sAMAccountName={escape_filter_chars(username)}))"
            self.connection.search(
                self.base_dn,
                search_filter,
                attributes=['mail']
            )
            
            if self.connection.entries:
                mail = self.connection.entries[0].mail.value if hasattr(self.connection.entries[0], 'mail') else ""
                return mail or ""
            return ""
        except Exception as e:
            self.logger.error(f"获取用户邮箱失败 {username}: {str(e)}")
            return ""
    
    def get_user_attribute_values(self, username: str, attributes: List[str]) -> Dict[str, Any]:
        try:
            search_filter = f"(&(objectClass=user)(sAMAccountName={escape_filter_chars(username)}))"
            self.connection.search(
                self.base_dn,
                search_filter,
                attributes=attributes,
            )
            if not self.connection.entries:
                return {}
            entry = self.connection.entries[0]
            values: Dict[str, Any] = {}
            for attribute in attributes:
                if not hasattr(entry, attribute):
                    continue
                raw_value = getattr(entry, attribute).value
                if raw_value is None:
                    continue
                values[attribute] = raw_value
            return values
        except Exception as e:
            self.logger.error(f"failed to fetch user attributes for {username}: {str(e)}")
            return {}

    def _build_user_attribute_changes(
        self,
        username: str,
        *,
        display_name: str,
        email: str,
        extra_attributes: Optional[Dict[str, Dict[str, Any] | Any]] = None,
    ) -> Dict[str, List[Tuple[int, List[Any]]]]:
        desired_attributes: Dict[str, Dict[str, Any]] = {
            'displayName': {'value': display_name, 'mode': 'replace'},
            'userPrincipalName': {'value': f"{username}@{self.domain}", 'mode': 'replace'},
            'mail': {'value': email, 'mode': 'fill_if_empty'},
        }
        for attribute_name, raw_value in dict(extra_attributes or {}).items():
            if isinstance(raw_value, dict):
                desired_attributes[attribute_name] = {
                    'value': raw_value.get('value'),
                    'mode': raw_value.get('mode') or 'replace',
                }
            else:
                desired_attributes[attribute_name] = {
                    'value': raw_value,
                    'mode': 'replace',
                }

        current_values = self.get_user_attribute_values(username, list(desired_attributes.keys()))
        changes: Dict[str, List[Tuple[int, List[Any]]]] = {}
        for attribute_name, config in desired_attributes.items():
            value = config.get('value')
            if value in (None, ''):
                continue
            mode = str(config.get('mode') or 'replace').strip().lower()
            current_value = current_values.get(attribute_name)
            normalized_current = ''
            if isinstance(current_value, (list, tuple)):
                normalized_current = ",".join(str(item).strip() for item in current_value if str(item).strip())
            elif current_value not in (None, ''):
                normalized_current = str(current_value).strip()
            normalized_value = str(value).strip()
            if mode == 'preserve' and normalized_current:
                continue
            if mode == 'fill_if_empty' and normalized_current:
                continue
            if normalized_current == normalized_value:
                continue
            changes[attribute_name] = [(MODIFY_REPLACE, [normalized_value])]
        return changes

    def _set_user_password(self, user_dn: str, password: str) -> bool:
        """设置用户密码"""
        try:
            # AD密码需要用特定格式编码
            password_value = f'"{password}"'.encode('utf-16-le')
            if self.connection.modify(user_dn, {
                'unicodePwd': [(MODIFY_REPLACE, [password_value])]
            }):
                return True
            self.logger.error(f"设置密码失败: {self.connection.result}")
            return False
        except Exception as e:
            self.logger.error(f"设置密码失败: {str(e)}")
            return False

    def reset_user_password(
        self,
        username: str,
        new_password: str,
        *,
        force_change_at_next_login: bool = False,
    ) -> bool:
        try:
            if self._is_protected_account(username):
                self.logger.warning(f"refusing to reset protected AD account password: {username}")
                return False
            if not new_password or not self._validate_password_complexity(new_password):
                self.logger.warning(f"refusing weak password reset for AD user: {username}")
                return False
            user = self.get_user(username)
            if not user:
                self.logger.error(f"user not found in AD while resetting password: {username}")
                return False
            user_dn = user["dn"]
            if not self._set_user_password(user_dn, new_password):
                return False
            if force_change_at_next_login and not self.connection.modify(
                user_dn,
                {"pwdLastSet": [(MODIFY_REPLACE, [0])]},
            ):
                self.logger.error(f"failed to force next-login password change for {username}: {self.connection.result}")
                return False
            self.logger.info(f"reset AD user password: {username}")
            return True
        except Exception as exc:
            self.logger.error(f"failed to reset AD user password {username}: {exc}")
            return False

    def unlock_user(self, username: str) -> bool:
        try:
            if self._is_protected_account(username):
                self.logger.warning(f"refusing to unlock protected AD account: {username}")
                return False
            user = self.get_user(username)
            if not user:
                self.logger.error(f"user not found in AD while unlocking account: {username}")
                return False
            if not self.connection.modify(user["dn"], {"lockoutTime": [(MODIFY_REPLACE, [0])]}):
                self.logger.error(f"failed to unlock AD user {username}: {self.connection.result}")
                return False
            self.logger.info(f"unlocked AD user: {username}")
            return True
        except Exception as exc:
            self.logger.error(f"failed to unlock AD user {username}: {exc}")
            return False
    
    def _validate_password_complexity(self, password: str) -> bool:
        """验证密码复杂度"""
        policy = self.password_complexity or "strong"
        has_length = len(password) >= (12 if policy == "strong" else 10 if policy == "medium" else 8)
        has_upper = any(c.isupper() for c in password)
        has_lower = any(c.islower() for c in password)
        has_digit = any(c.isdigit() for c in password)
        has_special = any(not c.isalnum() for c in password)

        if policy == "basic":
            return has_length
        if policy == "medium":
            return has_length and has_upper and has_lower and has_digit
        return has_length and has_upper and has_lower and has_digit and has_special
    
    def _generate_complex_password(self) -> str:
        """生成符合复杂度要求的随机密码"""
        import secrets
        import string
        
        length = secrets.choice(range(16, 21))
        
        password = [
            secrets.choice(string.ascii_uppercase),
            secrets.choice(string.ascii_lowercase),
            secrets.choice(string.digits),
            secrets.choice('!@#$%^&*()-_=+[]{}|;:,.<>?')
        ]
        
        remaining_length = length - len(password)
        all_chars = string.ascii_letters + string.digits + '!@#$%^&*()-_=+[]{}|;:,.<>?'
        password.extend(secrets.choice(all_chars) for _ in range(remaining_length))
        
        # Use Fisher-Yates shuffle with secrets for cryptographic safety
        for i in range(len(password) - 1, 0, -1):
            j = secrets.randbelow(i + 1)
            password[i], password[j] = password[j], password[i]
        
        return ''.join(password)
    
    def add_user_to_group(self, username: str, group_name: str) -> bool:
        """将用户添加到安全组"""
        try:
            # 清理组名（去除前后空格）
            group_name = group_name.strip() if group_name else ''
            
            if not group_name:
                self.logger.error(f"组名为空，跳过添加用户 {username}")
                return False
            
            # 查找用户DN
            user = self.get_user(username)
            if not user:
                self.logger.error(f"找不到用户: {username}")
                return False
            
            user_dn = user['dn']
            
            # 查找组DN
            search_filter = f"(&(objectClass=group)(sAMAccountName={escape_filter_chars(group_name)}))"
            self.connection.search(
                self.base_dn,
                search_filter,
                attributes=['distinguishedName']
            )
            
            if not self.connection.entries:
                self.logger.error(f"找不到组: {group_name}")
                return False
            
            group_dn = self.connection.entries[0].entry_dn
            
            # 添加用户到组
            changes = {
                'member': [(MODIFY_ADD, [user_dn])]
            }
            
            if self.connection.modify(group_dn, changes):
                self.logger.info(f"添加用户到组成功: {username} -> {group_name}")
                return True
            else:
                # 检查是否用户已在组中（错误码68或entryAlreadyExists）
                result = self.connection.result
                error_code = result.get('result', 0)
                error_desc = result.get('description', '')
                
                if error_code == 68 or 'entryAlreadyExists' in error_desc or 'ALREADY_EXISTS' in str(result):
                    self.logger.debug(f"用户已在组中: {username} -> {group_name}")
                    return True
                
                self.logger.error(f"添加用户到组失败: {username} -> {group_name}, 错误: {result}")
                return False
        except Exception as e:
            self.logger.error(f"添加用户到组过程出错: {str(e)}")
            return False
    
    def add_users_to_group_batch(self, usernames: List[str], group_name: str) -> Dict[str, bool]:
        """
        批量将用户添加到安全组
        
        参数:
            usernames: 用户名列表
            group_name: 组名
            
        返回:
            字典 {username: success}
        """
        try:
            if not usernames:
                return {}
            
            # 查找组DN
            search_filter = f"(&(objectClass=group)(sAMAccountName={escape_filter_chars(group_name)}))"
            self.connection.search(
                self.base_dn,
                search_filter,
                attributes=['distinguishedName', 'member']
            )
            
            if not self.connection.entries:
                self.logger.error(f"找不到组: {group_name}")
                return {u: False for u in usernames}
            
            group_dn = self.connection.entries[0].entry_dn
            
            # 获取组的现有成员
            existing_members = set()
            if hasattr(self.connection.entries[0], 'member'):
                existing_members = set(self.connection.entries[0].member.values)
            
            # 批量查询用户DN
            users_info = self.get_users_batch(usernames)
            
            # 准备要添加的用户DN列表
            users_to_add = []
            results = {}
            
            for username in usernames:
                if username not in users_info:
                    self.logger.warning(f"用户不存在，跳过: {username}")
                    results[username] = False
                    continue
                
                user_dn = users_info[username].dn
                if user_dn in existing_members:
                    self.logger.debug(f"用户已在组中: {username} -> {group_name}")
                    results[username] = True
                else:
                    users_to_add.append(user_dn)
                    results[username] = False  # 待更新
            
            # 批量添加用户到组
            if users_to_add:
                self.logger.info(f"批量添加 {len(users_to_add)} 个用户到组 {group_name}...")
                
                changes = {
                    'member': [(MODIFY_ADD, users_to_add)]
                }
                
                if self.connection.modify(group_dn, changes):
                    self.logger.info(f"批量添加成功: {len(users_to_add)} 个用户 -> {group_name}")
                    # 更新结果
                    for username in usernames:
                        if username in users_info and users_info[username].dn in users_to_add:
                            results[username] = True
                else:
                    self.logger.error(f"批量添加失败: {self.connection.result}")
                    # 如果批量失败，逐个尝试
                    self.logger.info("尝试逐个添加用户...")
                    for username in usernames:
                        if username in users_info and users_info[username].dn in users_to_add:
                            if self.add_user_to_group(username, group_name):
                                results[username] = True
            
            return results
            
        except Exception as e:
            self.logger.error(f"批量添加用户到组失败: {str(e)}")
            return {u: False for u in usernames}

    def add_group_to_group(self, child_group_dn: str, parent_group_dn: str) -> bool:
        """将子组加入父组"""
        try:
            changes = {
                'member': [(MODIFY_ADD, [child_group_dn])]
            }
            if self.connection.modify(parent_group_dn, changes):
                self.logger.info(f"添加组嵌套成功: {child_group_dn} -> {parent_group_dn}")
                return True

            result = self.connection.result
            error_code = result.get('result', 0)
            error_desc = result.get('description', '')
            if error_code == 68 or 'entryAlreadyExists' in error_desc or 'ALREADY_EXISTS' in str(result):
                self.logger.debug(f"组嵌套已存在: {child_group_dn} -> {parent_group_dn}")
                return True

            self.logger.error(f"添加组嵌套失败: {child_group_dn} -> {parent_group_dn}, 错误: {result}")
            return False
        except Exception as e:
            self.logger.error(f"添加组嵌套过程出错: {str(e)}")
            return False

    def remove_group_from_group(self, child_group_dn: str, parent_group_dn: str) -> bool:
        """将子组从父组移除"""
        try:
            changes = {
                'member': [(MODIFY_DELETE, [child_group_dn])]
            }
            if self.connection.modify(parent_group_dn, changes):
                self.logger.info(f"移除组嵌套成功: {child_group_dn} -X-> {parent_group_dn}")
                return True

            result = self.connection.result
            error_code = result.get('result', 0)
            error_desc = result.get('description', '')
            if error_code == 16 or 'NO_SUCH_ATTRIBUTE' in str(result) or 'noSuchAttribute' in error_desc:
                self.logger.debug(f"组嵌套不存在，无需移除: {child_group_dn} -X-> {parent_group_dn}")
                return True

            self.logger.error(f"移除组嵌套失败: {child_group_dn} -X-> {parent_group_dn}, 错误: {result}")
            return False
        except Exception as e:
            self.logger.error(f"移除组嵌套过程出错: {str(e)}")
            return False

    def find_parent_groups_for_member(self, member_dn: str) -> List[DirectoryGroupRecord]:
        """查找包含指定成员DN的父组"""
        try:
            safe_member_dn = escape_filter_chars(member_dn)
            search_filter = f"(&(objectClass=group)(member={safe_member_dn}))"
            self.connection.search(
                self.base_dn,
                search_filter,
                attributes=['cn', 'displayName', 'sAMAccountName', 'distinguishedName']
            )

            result: List[DirectoryGroupRecord] = []
            for entry in self.connection.entries:
                result.append(
                    DirectoryGroupRecord(
                        dn=entry.entry_dn,
                        cn=entry.cn.value if hasattr(entry, 'cn') else '',
                        display_name=entry.displayName.value if hasattr(entry, 'displayName') else '',
                        group_sam=entry.sAMAccountName.value if hasattr(entry, 'sAMAccountName') else '',
                    )
                )
            return result
        except Exception as e:
            self.logger.error(f"查询父组失败 {member_dn}: {str(e)}")
            return []
    
    def get_all_enabled_users(self) -> List[str]:
        """获取所有启用状态的AD用户账户"""
        try:
            # 构建排除账户的过滤条件
            exclude_filter = ''.join([f"(!(sAMAccountName={escape_filter_chars(acc)}))" for acc in self.exclude_accounts])
            search_filter = f"(&(objectClass=user)(!(userAccountControl:1.2.840.113556.1.4.803:=2)){exclude_filter}(!(sAMAccountName=*$)))"
            
            self.connection.search(
                self.base_dn,
                search_filter,
                attributes=['sAMAccountName']
            )
            
            return [entry.sAMAccountName.value for entry in self.connection.entries]
        except Exception as e:
            self.logger.error(f"获取AD用户列表失败: {str(e)}")
            return []

    def search_users(self, query: str, *, limit: int = 20) -> List[DirectoryUserRecord]:
        """按账号、显示名、邮件或 UPN 搜索 AD 用户"""
        normalized_query = str(query or "").strip()
        if not normalized_query:
            return []
        try:
            safe_query = escape_filter_chars(normalized_query)
            search_filter = (
                "(&"
                "(objectCategory=person)"
                "(objectClass=user)"
                "(!(sAMAccountName=*$))"
                "(|"
                f"(sAMAccountName=*{safe_query}*)"
                f"(displayName=*{safe_query}*)"
                f"(mail=*{safe_query}*)"
                f"(userPrincipalName=*{safe_query}*)"
                "))"
            )
            self.connection.search(
                self.base_dn,
                search_filter,
                attributes=[
                    'sAMAccountName',
                    'displayName',
                    'mail',
                    'userPrincipalName',
                    'distinguishedName',
                ],
                size_limit=max(int(limit or 20), 1),
            )
            results: List[DirectoryUserRecord] = []
            for entry in self.connection.entries:
                payload = json.loads(entry.entry_to_json())
                record = DirectoryUserRecord.from_ldap_json(payload)
                if record.username:
                    results.append(record)
            results.sort(
                key=lambda item: (
                    str(item.display_name or item.username or '').lower(),
                    str(item.username or '').lower(),
                )
            )
            return results[: max(int(limit or 20), 1)]
        except Exception as exc:
            self.logger.error(f"搜索AD用户失败 {normalized_query}: {str(exc)}")
            return []
    
    def is_user_active(self, username: str) -> bool:
        """检查用户是否处于启用状态"""
        try:
            search_filter = f"(&(objectClass=user)(sAMAccountName={escape_filter_chars(username)}))"
            self.connection.search(
                self.base_dn,
                search_filter,
                attributes=['userAccountControl']
            )
            
            if self.connection.entries:
                uac = int(self.connection.entries[0].userAccountControl.value)
                # 检查第2位（ACCOUNTDISABLE标志）
                return not (uac & 2)
            return False
        except Exception as e:
            self.logger.error(f"检查用户状态失败 {username}: {str(e)}")
            return False
    
    def create_user(
        self,
        username: str,
        display_name: str,
        email: str,
        ou_dn: str,
        *,
        extra_attributes: Optional[Dict[str, Dict[str, Any] | Any]] = None,
    ) -> bool:
        try:
            if self._is_protected_account(username):
                self.logger.warning(f"refusing to create protected AD account via sync: {username}")
                return False
            password = self.default_password
            if not password or not self._validate_password_complexity(password):
                self.logger.warning("default password missing or too weak; generating a compliant password")
                password = self._generate_complex_password()

            user_dn = f"CN={display_name},{ou_dn}"
            user_attributes: Dict[str, Any] = {
                'objectClass': ['top', 'person', 'organizationalPerson', 'user'],
                'cn': display_name,
                'sAMAccountName': username,
                'userPrincipalName': f"{username}@{self.domain}",
                'displayName': display_name,
                'mail': email,
                'userAccountControl': 512 if not self.force_change_password else 544,
            }
            for attribute_name, raw_value in dict(extra_attributes or {}).items():
                attribute_value = raw_value.get('value') if isinstance(raw_value, dict) else raw_value
                if attribute_value in (None, ''):
                    continue
                user_attributes[attribute_name] = attribute_value

            if self.connection.add(user_dn, attributes=user_attributes):
                self._set_user_password(user_dn, password)
                self.connection.modify(user_dn, {'userAccountControl': [(MODIFY_REPLACE, [512])]})
                self.logger.info(f"created AD user: {username} ({display_name})")
                return True
            self.logger.error(f"failed to create AD user {username} ({display_name}): {self.connection.result}")
            return False
        except Exception as exc:
            self.logger.error(f"failed to create AD user {username}: {exc}")
            return False

    def update_user(
        self,
        username: str,
        display_name: str,
        email: str,
        ou_dn: str,
        *,
        extra_attributes: Optional[Dict[str, Dict[str, Any] | Any]] = None,
    ) -> bool:
        try:
            if self._is_protected_account(username):
                self.logger.warning(f"refusing to update protected AD account via sync: {username}")
                return False
            user = self.get_user(username)
            if not user:
                self.logger.error(f"user not found in AD: {username}")
                return False

            user_dn = user['dn']
            changes = self._build_user_attribute_changes(
                username,
                display_name=display_name,
                email=email,
                extra_attributes=extra_attributes,
            )
            if changes and not self.connection.modify(user_dn, changes):
                self.logger.error(f"failed to update AD user attributes {username}: {self.connection.result}")
                return False

            new_dn = f"CN={display_name},{ou_dn}"
            if user_dn != new_dn:
                if not self.connection.modify_dn(user_dn, f"CN={display_name}", new_superior=ou_dn):
                    self.logger.error(f"failed to move AD user {username}: {self.connection.result}")
                    return False

            self.logger.info(f"updated AD user: {username} ({display_name})")
            return True
        except Exception as exc:
            self.logger.error(f"failed to update AD user {username}: {exc}")
            return False

    def reactivate_user(
        self,
        username: str,
        display_name: str,
        email: str,
        ou_dn: str,
        *,
        extra_attributes: Optional[Dict[str, Dict[str, Any] | Any]] = None,
    ) -> bool:
        try:
            if self._is_protected_account(username):
                self.logger.warning(f"refusing to reactivate protected AD account via sync: {username}")
                return False
            user = self.get_user(username)
            if not user:
                self.logger.error(f"user not found in AD: {username}")
                return False
            user_dn = user['dn']
            if not self.update_user(
                username,
                display_name,
                email,
                ou_dn,
                extra_attributes=extra_attributes,
            ):
                return False
            if not self.connection.modify(
                user_dn,
                {
                    'userAccountControl': [(MODIFY_REPLACE, [512])],
                    'description': [(MODIFY_REPLACE, [f"Account reactivated - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"])],
                },
            ):
                self.logger.error(f"failed to reactivate AD user {username}: {self.connection.result}")
                return False
            self.logger.info(f"reactivated AD user: {username}")
            return True
        except Exception as exc:
            self.logger.error(f"failed to reactivate AD user {username}: {exc}")
            return False

    def remove_user_from_group(self, username: str, group_name: str) -> bool:
        try:
            group_name = str(group_name or '').strip()
            if not group_name:
                return False
            user = self.get_user(username)
            if not user:
                self.logger.warning(f"user not found while removing from group: {username}")
                return True
            user_dn = user['dn']
            search_filter = f"(&(objectClass=group)(sAMAccountName={escape_filter_chars(group_name)}))"
            self.connection.search(
                self.base_dn,
                search_filter,
                attributes=['distinguishedName']
            )
            if not self.connection.entries:
                self.logger.warning(f"group not found while removing membership: {group_name}")
                return True
            group_dn = self.connection.entries[0].entry_dn
            if self.connection.modify(group_dn, {'member': [(MODIFY_DELETE, [user_dn])]}):
                self.logger.info(f"removed AD user from group: {username} -X-> {group_name}")
                return True
            result = self.connection.result
            error_code = result.get('result', 0)
            error_desc = result.get('description', '')
            if error_code == 16 or 'NO_SUCH_ATTRIBUTE' in str(result) or 'noSuchAttribute' in error_desc:
                return True
            self.logger.error(f"failed to remove AD user from group {username} -X-> {group_name}: {result}")
            return False
        except Exception as exc:
            self.logger.error(f"failed to remove AD user from group {username} -X-> {group_name}: {exc}")
            return False

    def disable_user(self, username: str) -> bool:
        try:
            if self._is_protected_account(username):
                self.logger.warning(f"refusing to disable protected AD account via sync: {username}")
                return False
            if not self.ensure_disabled_users_ou():
                self.logger.error(f"failed to ensure disabled users OU: {self.disabled_users_ou_name}")

            if not self.is_user_active(username):
                self.logger.info(f"AD user already disabled or missing: {username}")
                return True

            user = self.get_user(username)
            if not user:
                self.logger.error(f"user not found in AD: {username}")
                return False

            user_dn = user['dn']
            changes = {
                'userAccountControl': [(MODIFY_REPLACE, [514])],
                'description': [
                    (
                        MODIFY_REPLACE,
                        [f"Account disabled - Not found in source directory - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"],
                    )
                ],
            }
            if not self.connection.modify(user_dn, changes):
                self.logger.error(f"failed to disable AD user {username}: {self.connection.result}")
                return False

            disabled_ou = self.get_disabled_users_ou_dn()
            cn = user_dn.split(',', 1)[0].split('=', 1)[1]
            try:
                self.connection.modify_dn(user_dn, f"CN={cn}", new_superior=disabled_ou)
            except Exception as exc:
                self.logger.warning(f"disabled AD user but could not move to {self.disabled_users_ou_name}: {exc}")
            self.logger.info(f"disabled AD user: {username}")
            return True
        except Exception as exc:
            self.logger.error(f"failed to disable AD user {username}: {exc}")
            return False

    def get_user_details(self, username: str) -> Dict:
        attributes = self.get_user_attribute_values(
            username,
            [
                'displayName',
                'mail',
                'title',
                'mobile',
                'description',
                'telephoneNumber',
                'whenCreated',
                'whenChanged',
                'lastLogon',
                'distinguishedName',
                'objectGUID',
            ],
        )
        if not attributes:
            return {}
        return {
            'SamAccountName': username,
            'DisplayName': str(attributes.get('displayName') or ''),
            'Mail': str(attributes.get('mail') or ''),
            'Title': str(attributes.get('title') or ''),
            'Mobile': str(attributes.get('mobile') or ''),
            'TelephoneNumber': str(attributes.get('telephoneNumber') or ''),
            'Description': str(attributes.get('description') or ''),
            'Created': str(attributes.get('whenCreated') or ''),
            'Modified': str(attributes.get('whenChanged') or ''),
            'LastLogonDate': str(attributes.get('lastLogon') or ''),
            'DistinguishedName': str(attributes.get('distinguishedName') or ''),
            'ObjectGUID': str(attributes.get('objectGUID') or ''),
        }

    def ensure_disabled_users_ou(self) -> bool:
        try:
            disabled_ou = self.get_disabled_users_ou_dn()
            if self.ou_exists(disabled_ou):
                return True
            success, _ = self.ensure_ou_path(self.disabled_users_ou_path)
            if success:
                return True
            self.logger.error(f"failed to create disabled users OU {self.disabled_users_ou_name}: {self.connection.result}")
            return False
        except Exception as exc:
            self.logger.error(f"failed to ensure disabled users OU {self.disabled_users_ou_name}: {exc}")
            return False

    def __del__(self):
        """资源清理"""
        try:
            if hasattr(self, 'connection') and self.connection:
                self.connection.unbind()
                self.logger.info("已关闭LDAP连接")
        except Exception as e:
            self.logger.error(f"关闭LDAP连接时出错: {str(e)}")

# 保持向后兼容，ADSync作为ADSyncLDAPS的别名
ADSync = ADSyncLDAPS
