import os
import sys
import logging
import json
import csv
import requests
import subprocess
import configparser
from datetime import datetime
from typing import Dict, List, Optional
import time

# 设置标准输入输出的UTF8编码
try:
    # Python 3.7+ 方式
    if hasattr(sys.stdin, 'reconfigure'):
        sys.stdin.reconfigure(encoding='utf-8')
    if hasattr(sys.stdout, 'reconfigure'):
        sys.stdout.reconfigure(encoding='utf-8')
except Exception:
    # 兼容旧版本或PyInstaller打包环境
    import io
    if sys.stdout.encoding != 'utf-8':
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
    if sys.stdin.encoding != 'utf-8':
        sys.stdin = io.TextIOWrapper(sys.stdin.buffer, encoding='utf-8')

def setup_logging():
    """设置日志配置"""
    global log_filename  # 使变量全局可访问
    
    # 创建日志目录
    log_dir = "logs"
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
    
    # 按日期生成日志文件名
    log_filename = os.path.join(log_dir, f"ad_wecom_sync_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
    
    # 基本配置
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_filename, encoding='utf-8'),
            logging.StreamHandler()
        ]
    )
    
    # 降低第三方库的日志级别
    logging.getLogger('urllib3').setLevel(logging.WARNING)
    logging.getLogger('requests').setLevel(logging.WARNING)
    
    # 添加详细日志配置
    detailed_log = logging.getLogger('detailed')
    detailed_log.setLevel(logging.DEBUG)
    detailed_handler = logging.FileHandler(os.path.join(log_dir, f"ad_wecom_sync_detailed_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"), encoding='utf-8')
    detailed_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(name)s - %(filename)s:%(lineno)d - %(message)s'))
    detailed_log.addHandler(detailed_handler)
    
    return logging.getLogger(__name__)

class WeComAPI:
    def __init__(self, corpid: str, corpsecret: str):
        self.corpid = corpid
        self.corpsecret = corpsecret
        self.access_token = None
        self.token_expires_at = 0
        self.logger = logging.getLogger(__name__)
        self.session = requests.Session()  # 使用会话提高性能
        self.max_retries = 3  # 最大重试次数
        self.retry_delay = 2  # 重试延迟秒数
        
        # 配置请求超时
        self.timeout = 30  # 秒
        
        # 配置请求重试
        retry_strategy = requests.adapters.Retry(
            total=self.max_retries,
            backoff_factor=0.5,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET", "POST"]
        )
        adapter = requests.adapters.HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("https://", adapter)
        
        # 获取初始token
        self._refresh_access_token()

    def _refresh_access_token(self) -> None:
        """获取并刷新企业微信API访问token"""
        try:
            url = f"https://qyapi.weixin.qq.com/cgi-bin/gettoken?corpid={self.corpid}&corpsecret={self.corpsecret}"
            response = self.session.get(url, timeout=self.timeout)
            result = response.json()
            
            if result.get('errcode') == 0:
                self.access_token = result['access_token']
                # token有效期为7200秒，提前200秒刷新
                self.token_expires_at = time.time() + result.get('expires_in', 7200) - 200
                self.logger.info("成功获取企业微信access_token")
            else:
                error_msg = f"获取access_token失败: {result.get('errmsg')}"
                self.logger.error(error_msg)
                raise Exception(error_msg)
        except requests.RequestException as e:
            self.logger.error(f"获取access_token时网络错误: {str(e)}")
            raise
        except Exception as e:
            self.logger.error(f"获取access_token时发生错误: {str(e)}")
            raise

    def _ensure_token_valid(self) -> None:
        """确保access_token有效"""
        if time.time() >= self.token_expires_at:
            self.logger.info("access_token已过期，正在刷新...")
            self._refresh_access_token()
    
    def _request(self, method: str, url: str, **kwargs) -> Dict:
        """统一处理API请求"""
        self._ensure_token_valid()
        
        # 确保有超时设置
        if 'timeout' not in kwargs:
            kwargs['timeout'] = self.timeout
            
        # 进行请求
        for attempt in range(self.max_retries):
            try:
                if method.upper() == "GET":
                    response = self.session.get(url, **kwargs)
                else:
                    response = self.session.post(url, **kwargs)
                
                result = response.json()
                
                # 处理token失效的情况
                if result.get('errcode') == 42001:  # token过期
                    self.logger.info("access_token已失效，正在刷新...")
                    self._refresh_access_token()
                    continue
                
                return result
            except requests.RequestException as e:
                self.logger.warning(f"请求失败 (尝试 {attempt+1}/{self.max_retries}): {str(e)}")
                if attempt < self.max_retries - 1:
                    time.sleep(self.retry_delay)
                else:
                    self.logger.error(f"请求失败，已达到最大重试次数: {str(e)}")
                    raise
        
        # 不应该到达这里，但以防万一
        raise Exception("请求失败，超出重试次数")

    def get_department_list(self) -> List[Dict]:
        """获取部门列表"""
        url = f"https://qyapi.weixin.qq.com/cgi-bin/department/list?access_token={self.access_token}"
        result = self._request("GET", url)
        
        if result.get('errcode') == 0:
            return result["department"]
        else:
            error_msg = f"获取部门列表失败: {result.get('errmsg')}"
            self.logger.error(error_msg)
            raise Exception(error_msg)

    def get_department_users(self, department_id: int) -> List[Dict]:
        """获取部门成员详情"""
        url = f"https://qyapi.weixin.qq.com/cgi-bin/user/list?access_token={self.access_token}&department_id={department_id}&fetch_child=0"
        result = self._request("GET", url)
        
        if result.get('errcode') == 0:
            return result["userlist"]
        else:
            error_msg = f"获取部门成员失败: {result.get('errmsg')}"
            self.logger.error(error_msg)
            raise Exception(error_msg)

    def get_user_detail(self, userid: str) -> Dict:
        """获取成员详情"""
        url = f"https://qyapi.weixin.qq.com/cgi-bin/user/get?access_token={self.access_token}&userid={userid}"
        result = self._request("GET", url)
        
        if result.get('errcode') == 0:
            return result
        else:
            error_msg = f"获取用户详情失败: {userid}, {result.get('errmsg')}"
            self.logger.error(error_msg)
            return {}

    def get_all_users(self) -> List[Dict]:
        """获取所有企业微信用户"""
        all_users = []
        try:
            departments = self.get_department_list()
            for dept in departments:
                users = self.get_department_users(dept['id'])
                all_users.extend(users)
            # 去重处理
            seen_userids = set()
            unique_users = []
            for user in all_users:
                if user['userid'] not in seen_userids:
                    seen_userids.add(user['userid'])
                    unique_users.append(user)
            return unique_users
        except Exception as e:
            self.logger.error(f"获取所有用户失败: {str(e)}")
            return []
    
    def __del__(self):
        """资源清理"""
        try:
            if hasattr(self, 'session') and self.session:
                self.session.close()
                self.logger.debug("已关闭API会话")
        except Exception as e:
            self.logger.error(f"关闭API会话时出错: {str(e)}")

class ADSync:
    def __init__(self, domain: str, exclude_departments: List[str] = None, exclude_accounts: List[str] = None):
        self.domain = domain
        self.exclude_departments = exclude_departments or []
        self.exclude_accounts = exclude_accounts or []
        self.logger = logging.getLogger(__name__)
        
        # 先设置重试参数
        self.max_retries = 3  # 最大重试次数
        self.retry_delay = 2  # 重试延迟秒数
        
        # 然后再调用其他方法
        self.init_powershell_encoding()
        self.ensure_disabled_users_ou()  # 初始化时确保 Disabled Users OU 存在
        
        # 读取默认密码和密码策略
        config = configparser.ConfigParser()
        config.read('config.ini', encoding='utf-8')
        self.default_password = config.get('Account', 'DefaultPassword', fallback='Notting8899')
        self.force_change_password = config.getboolean('Account', 'ForceChangePassword', fallback=True)

    def init_powershell_encoding(self):
        """初始化PowerShell的UTF8编码支持"""
        commands = [
            "$OutputEncoding = [Console]::OutputEncoding = [Text.Encoding]::UTF8",
            "chcp 65001"
        ]
        for cmd in commands:
            self.run_powershell(cmd)

    def run_powershell(self, command: str, retry_count: int = None) -> tuple:
        """执行PowerShell命令并返回结果，支持重试机制"""
        if retry_count is None:
            retry_count = self.max_retries
            
        for attempt in range(retry_count + 1):
            try:
                self.logger.debug(f"执行PowerShell命令 (尝试 {attempt+1}/{retry_count+1})")
                
                full_command = f"""
                $ErrorActionPreference = "Stop"
                $OutputEncoding = [Console]::OutputEncoding = [System.Text.Encoding]::UTF8
                [Console]::OutputEncoding = [System.Text.Encoding]::UTF8
                try {{
                    {command}
                    Write-Output "##COMMAND_SUCCESS##"
                }} catch {{
                    Write-Error "错误详情: $($_.Exception.Message)"
                    Write-Output "##COMMAND_ERROR##"
                    exit 1
                }}
                """
                
                process = subprocess.run(
                    ["powershell", "-WindowStyle", "Hidden", "-Command", full_command],
                    capture_output=True,
                    text=True,
                    encoding='utf-8',
                    timeout=60
                )
                
                output = process.stdout.strip()
                error = process.stderr.strip()
                
                # 检查是否有错误输出
                if process.returncode != 0 or "##COMMAND_ERROR##" in output:
                    if attempt < retry_count:
                        self.logger.warning(f"命令执行失败，将在 {self.retry_delay} 秒后重试: {error}")
                        time.sleep(self.retry_delay)
                        continue
                    else:
                        self.logger.error(f"命令执行失败，已达到最大重试次数: {error}")
                        return False, error or output
                
                # 清理输出中的成功标记
                output = output.replace("##COMMAND_SUCCESS##", "").strip()
                return True, output
                
            except subprocess.TimeoutExpired:
                self.logger.error(f"命令执行超时")
                if attempt < retry_count:
                    time.sleep(self.retry_delay)
                    continue
                return False, "命令执行超时"
                
            except Exception as e:
                self.logger.error(f"执行PowerShell命令失败: {str(e)}")
                if attempt < retry_count:
                    time.sleep(self.retry_delay)
                    continue
                return False, str(e)
        
        # 不应该到达这里，但以防万一
        return False, "未知错误"

    def get_ou_dn(self, ou_path: List[str]) -> str:
        """获取OU的Distinguished Name"""
        return ','.join([f"OU={ou}" for ou in reversed(ou_path)]) + f',DC={self.domain.replace(".", ",DC=")}'

    def ou_exists(self, ou_dn: str) -> bool:
        """检查OU是否存在"""
        command = f"Get-ADOrganizationalUnit -Identity '{ou_dn}' -ErrorAction SilentlyContinue"
        success, _ = self.run_powershell(command)
        return success

    def create_ou(self, ou_name: str, parent_dn: str) -> bool:
        """创建OU和对应的安全组"""
        try:
            # 检查是否在排除列表中
            if ou_name in self.exclude_departments:
                self.logger.info(f"跳过创建OU: {ou_name} (在排除列表中)")
                return True
                
            if not self.ou_exists(f"OU={ou_name},{parent_dn}"):
                command = f"""
                New-ADOrganizationalUnit `
                    -Name "{ou_name}" `
                    -Path "{parent_dn}" `
                    -ProtectedFromAccidentalDeletion $false
                """
                success, output = self.run_powershell(command)
                if success:
                    self.logger.info(f"创建OU成功: {ou_name}")
                    
                    ou_dn = f"OU={ou_name},{parent_dn}"
                    group_command = f"""
                    New-ADGroup `
                        -Name "{ou_name}" `
                        -GroupScope Global `
                        -GroupCategory Security `
                        -Path "{ou_dn}"
                    """
                    group_success, group_output = self.run_powershell(group_command)
                    
                    if group_success:
                        self.logger.info(f"创建同名安全组成功: {ou_name}")
                        return True
                    else:
                        self.logger.error(f"创建安全组失败: {ou_name}, 错误: {group_output}")
                else:
                    self.logger.error(f"创建OU失败: {ou_name}, 错误: {output}")
            else:
                self.logger.info(f"OU已存在: {ou_name}")
            return False
        except Exception as e:
            self.logger.error(f"创建OU过程出错: {str(e)}")
            return False

    def get_user(self, username: str) -> Optional[dict]:
        """获取AD用户信息"""
        command = f"""
        Get-ADUser -Identity '{username}' -Properties * | 
        Select-Object * |
        ConvertTo-Json
        """
        success, output = self.run_powershell(command)
        if success and output:
            try:
                return json.loads(output)
            except json.JSONDecodeError:
                return None
        return None

    def check_email_exists(self, email: str, exclude_user: str = None) -> bool:
        """检查邮箱是否已被其他用户使用"""
        try:
            command = f"""
            Get-ADUser -Filter {{Mail -eq '{email}' -and SamAccountName -ne '{exclude_user}'}} |
            Select-Object -First 1 |
            Select-Object -ExpandProperty SamAccountName
            """
            success, output = self.run_powershell(command)
            return success and bool(output.strip())
        except Exception as e:
            self.logger.error(f"检查邮箱是否存在时出错: {str(e)}")
            return False

    def get_user_email(self, username: str) -> str:
        """获取AD用户当前的邮箱地址"""
        try:
            command = f"""
            Get-ADUser -Identity '{username}' -Properties Mail |
            Select-Object -ExpandProperty Mail
            """
            success, output = self.run_powershell(command)
            return output.strip() if success and output.strip() else ""
        except Exception as e:
            self.logger.error(f"获取用户邮箱失败 {username}: {str(e)}")
            return ""

    def create_user(self, username: str, display_name: str, email: str, ou_dn: str) -> bool:
        """创建AD用户"""
        try:
            # 生成复杂密码或使用配置的默认密码
            password = self.default_password
            
            # 验证密码复杂度
            if not self._validate_password_complexity(password):
                self.logger.warning(f"默认密码不符合复杂度要求，将生成随机密码")
                password = self._generate_complex_password()
            
            command = f"""
            $securePassword = ConvertTo-SecureString -String '{password}' -AsPlainText -Force
            New-ADUser `
                -SamAccountName '{username}' `
                -Name '{display_name}' `
                -DisplayName '{display_name}' `
                -EmailAddress '{email}' `
                -UserPrincipalName '{username}' `
                -Enabled $true `
                -Path '{ou_dn}' `
                -AccountPassword $securePassword `
                -ChangePasswordAtLogon ${str(self.force_change_password).lower()}
            
            # 记录新建用户信息到安全日志
            Write-EventLog -LogName 'Security' -Source 'Microsoft-Windows-Security-Auditing' -EventId 4720 -EntryType Information -Message "创建了新用户账户: $env:USERNAME 创建了 {username} ({display_name})" -ErrorAction SilentlyContinue
            """
            
            success, output = self.run_powershell(command)
            if success:
                self.logger.info(f"创建用户成功: {username} ({display_name})")
                return True
            else:
                self.logger.error(f"创建用户失败: {username} ({display_name}), 错误: {output}")
                return False
        except Exception as e:
            self.logger.error(f"创建用户过程出错: {str(e)}")
            return False

    def _validate_password_complexity(self, password: str) -> bool:
        """验证密码复杂度"""
        # 至少8个字符，包含大小写字母、数字和特殊字符
        has_length = len(password) >= 8
        has_upper = any(c.isupper() for c in password)
        has_lower = any(c.islower() for c in password)
        has_digit = any(c.isdigit() for c in password)
        has_special = any(not c.isalnum() for c in password)
        
        return has_length and has_upper and has_lower and has_digit and has_special
    
    def _generate_complex_password(self) -> str:
        """生成符合复杂度要求的随机密码"""
        import random
        import string
        
        # 密码长度16-20字符
        length = random.randint(16, 20)
        
        # 确保包含各类字符
        password = [
            random.choice(string.ascii_uppercase),  # 至少一个大写字母
            random.choice(string.ascii_lowercase),  # 至少一个小写字母
            random.choice(string.digits),          # 至少一个数字
            random.choice('!@#$%^&*()-_=+[]{}|;:,.<>?')  # 至少一个特殊字符
        ]
        
        # 填充剩余长度
        remaining_length = length - len(password)
        all_chars = string.ascii_letters + string.digits + '!@#$%^&*()-_=+[]{}|;:,.<>?'
        password.extend(random.choice(all_chars) for _ in range(remaining_length))
        
        # 打乱顺序
        random.shuffle(password)
        
        return ''.join(password)

    def update_user(self, username: str, display_name: str, email: str, ou_dn: str) -> bool:
        """更新AD用户信息"""
        try:
            # 检查用户当前是否已有邮箱
            current_email = self.get_user_email(username)
            
            # 检查用户的UPN是否正确
            upn_command = f"""
            Get-ADUser -Identity '{username}' -Properties UserPrincipalName |
            Select-Object -ExpandProperty UserPrincipalName
            """
            upn_success, current_upn = self.run_powershell(upn_command)
            current_upn = current_upn.strip() if upn_success and current_upn.strip() else ""
            upn_needs_update = current_upn != username
            
            if current_email and not upn_needs_update:
                self.logger.info(f"用户 {username} 已有邮箱 {current_email}，保持不变")
                command = f"""
                Get-ADUser -Identity '{username}' | 
                Set-ADUser `
                    -DisplayName '{display_name}'
                """
            elif current_email and upn_needs_update:
                self.logger.info(f"用户 {username} 已有邮箱 {current_email}，需要更新 UPN: {username}")
                command = f"""
                Get-ADUser -Identity '{username}' | 
                Set-ADUser `
                    -DisplayName '{display_name}' `
                    -UserPrincipalName '{username}'
                """
            elif upn_needs_update:
                self.logger.info(f"用户 {username} 无邮箱，设置新邮箱: {email} 并更新 UPN: {username}")
                command = f"""
                Get-ADUser -Identity '{username}' | 
                Set-ADUser `
                    -DisplayName '{display_name}' `
                    -EmailAddress '{email}' `
                    -UserPrincipalName '{username}'
                """
            else:
                self.logger.info(f"用户 {username} 无邮箱，设置新邮箱: {email}")
                command = f"""
                Get-ADUser -Identity '{username}' | 
                Set-ADUser `
                    -DisplayName '{display_name}' `
                    -EmailAddress '{email}'
                """
                
            success, output = self.run_powershell(command)
            
            if success:
                # 移动用户到指定OU
                move_command = f"""
                Move-ADObject `
                    -Identity (Get-ADUser -Identity '{username}').DistinguishedName `
                    -TargetPath '{ou_dn}'
                """
                move_success, move_output = self.run_powershell(move_command)
                
                if move_success:
                    self.logger.info(f"更新用户成功: {username} ({display_name})")
                    return True
                else:
                    self.logger.error(f"移动用户失败: {username}, 错误: {move_output}")
            else:
                self.logger.error(f"更新用户信息失败: {username}, 错误: {output}")
            return False
        except Exception as e:
            self.logger.error(f"更新用户过程出错: {str(e)}")
            return False

    def add_user_to_group(self, username: str, group_name: str) -> bool:
        """将用户添加到安全组"""
        try:
            command = f"""
            Add-ADGroupMember `
                -Identity '{group_name}' `
                -Members '{username}' `
                -ErrorAction SilentlyContinue
            """
            success, output = self.run_powershell(command)
            
            if success:
                self.logger.info(f"添加用户到组成功: {username} -> {group_name}")
                return True
            else:
                self.logger.error(f"添加用户到组失败: {username} -> {group_name}, 错误: {output}")
                return False
        except Exception as e:
            self.logger.error(f"添加用户到组过程出错: {str(e)}")
            return False

    def get_all_enabled_users(self) -> List[str]:
        """获取所有启用状态的AD用户账户（排除系统账户和配置的排除账户）"""
        try:
            # 构建排除账户的过滤条件
            exclude_accounts = '|'.join(self.exclude_accounts)
            command = f"""
            Get-ADUser -Filter {{Enabled -eq $true}} -Properties SamAccountName |
            Where-Object {{
                $_.SamAccountName -notmatch '^({exclude_accounts})$' -and
                $_.SamAccountName -notlike '*$'
            }} |
            Select-Object -ExpandProperty SamAccountName |
            ConvertTo-Json
            """
            success, output = self.run_powershell(command)
            if success and output:
                try:
                    return json.loads(output)
                except json.JSONDecodeError:
                    self.logger.error("解析AD用户列表失败")
                    return []
            return []
        except Exception as e:
            self.logger.error(f"获取AD用户列表失败: {str(e)}")
            return []

    def is_user_active(self, username: str) -> bool:
        """检查用户是否处于启用状态"""
        try:
            command = f"""
            (Get-ADUser -Identity '{username}' -Properties Enabled).Enabled
            """
            success, output = self.run_powershell(command)
            return success and output.strip().lower() == 'true'
        except Exception as e:
            self.logger.error(f"检查用户状态失败 {username}: {str(e)}")
            return False

    def disable_user(self, username: str) -> bool:
        """禁用AD用户账户"""
        try:
            # 确保 Disabled Users OU 存在
            if not self.ensure_disabled_users_ou():
                self.logger.error("无法确保 Disabled Users OU 存在，禁用用户操作可能会失败")
            
            # 首先检查用户是否存在且处于启用状态
            if not self.is_user_active(username):
                self.logger.info(f"用户 {username} 已经处于禁用状态或不存在")
                return True

            # 禁用账户
            disable_command = f"""
            $user = Get-ADUser -Identity '{username}'
            if ($user) {{
                Disable-ADAccount -Identity $user
                Set-ADUser -Identity $user `
                    -Description "Account disabled - Not found in WeChat Work - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
                Write-Output "Success"
            }} else {{
                Write-Error "User not found"
            }}
            """
            success, output = self.run_powershell(disable_command)
            
            if success and "Success" in output:
                self.logger.info(f"成功禁用账户: {username}")
                # 移动到禁用用户OU
                disabled_ou = f"OU=Disabled Users,DC={self.domain.replace('.', ',DC=')}"
                move_command = f"""
                $user = Get-ADUser -Identity '{username}'
                if ($user) {{
                    Move-ADObject -Identity $user.DistinguishedName -TargetPath '{disabled_ou}'
                    Write-Output "Moved"
                }}
                """
                move_success, _ = self.run_powershell(move_command)
                if move_success:
                    self.logger.info(f"已将禁用账户 {username} 移动到 Disabled Users OU")
                return True
            else:
                self.logger.error(f"禁用账户失败 {username}: {output}")
                return False
        except Exception as e:
            self.logger.error(f"禁用账户过程出错 {username}: {str(e)}")
            return False

    def get_user_details(self, username: str) -> Dict:
        """获取AD用户的详细信息"""
        try:
            command = f"""
            Get-ADUser -Identity '{username}' -Properties DisplayName, Mail, Created, Modified, LastLogonDate, Description |
            Select-Object SamAccountName, DisplayName, Mail, Created, Modified, LastLogonDate, Description |
            ConvertTo-Json
            """
            success, output = self.run_powershell(command)
            if success and output:
                try:
                    return json.loads(output)
                except json.JSONDecodeError:
                    return {}
            return {}
        except Exception as e:
            self.logger.error(f"获取用户详情失败: {str(e)}")
            return {}

    def ensure_disabled_users_ou(self) -> bool:
        """确保 Disabled Users OU 存在，不存在则创建"""
        try:
            disabled_ou = f"OU=Disabled Users,DC={self.domain.replace('.', ',DC=')}"
            if not self.ou_exists(disabled_ou):
                self.logger.info("Disabled Users OU 不存在，正在创建...")
                command = f"""
                New-ADOrganizationalUnit `
                    -Name "Disabled Users" `
                    -Path "DC={self.domain.replace('.', ',DC=')}" `
                    -Description "存放已禁用的用户账户" `
                    -ProtectedFromAccidentalDeletion $false
                """
                success, output = self.run_powershell(command)
                if success:
                    self.logger.info("成功创建 Disabled Users OU")
                    return True
                else:
                    self.logger.error(f"创建 Disabled Users OU 失败: {output}")
                    return False
            else:
                self.logger.info("Disabled Users OU 已存在")
                return True
        except Exception as e:
            self.logger.error(f"检查/创建 Disabled Users OU 时出错: {str(e)}")
            return False

    def __del__(self):
        """资源清理"""
        logging.getLogger().info("正在清理AD同步资源...")
        try:
            # 清理临时会话
            cleanup_cmd = """
            Remove-PSSession -Name * -ErrorAction SilentlyContinue
            [System.GC]::Collect()
            """
            self.run_powershell(cleanup_cmd, retry_count=0)
        except Exception as e:
            logging.getLogger().error(f"资源清理过程出错: {str(e)}")

class WeChatBot:
    """企业微信机器人通知类"""
    def __init__(self, webhook_url: str):
        self.webhook_url = webhook_url
        self.logger = logging.getLogger(__name__)
        self.logger.info(f"初始化企业微信机器人: {webhook_url}")
        self.session = requests.Session()  # 使用会话提高性能
        self.max_retries = 3  # 最大重试次数
        self.retry_delay = 2  # 重试延迟秒数

    def send_message(self, content: str) -> bool:
        """发送消息到企业微信机器人"""
        try:
            self.logger.info("开始发送企业微信机器人消息")
            self.logger.debug(f"消息内容: {content}")
            
            data = {
                "msgtype": "markdown",
                "markdown": {
                    "content": content
                }
            }
            
            # 配置请求重试
            retry_strategy = requests.adapters.Retry(
                total=self.max_retries,
                backoff_factor=0.5,
                status_forcelist=[429, 500, 502, 503, 504],
                allowed_methods=["POST"]
            )
            adapter = requests.adapters.HTTPAdapter(max_retries=retry_strategy)
            self.session.mount("https://", adapter)
            
            response = self.session.post(
                self.webhook_url,
                json=data,
                timeout=10  # 添加超时设置
            )
            response.raise_for_status()  # 抛出HTTP错误
            result = response.json()
            
            if result.get('errcode') == 0:
                self.logger.info("机器人消息发送成功")
                return True
            else:
                self.logger.error(f"机器人消息发送失败: {result}")
                return False

        except requests.RequestException as e:
            self.logger.error(f"发送请求失败: {str(e)}")
            return False
        except json.JSONDecodeError as e:
            self.logger.error(f"解析响应JSON失败: {str(e)}")
            return False
        except Exception as e:
            self.logger.error(f"发送机器人消息时出错: {str(e)}")
            return False
    
    def __del__(self):
        """资源清理"""
        try:
            if hasattr(self, 'session') and self.session:
                self.session.close()
                self.logger.debug("已关闭机器人API会话")
        except Exception as e:
            self.logger.error(f"关闭机器人API会话时出错: {str(e)}")

def format_time_duration(seconds: float) -> str:
    """格式化时间间隔"""
    minutes, seconds = divmod(int(seconds), 60)
    hours, minutes = divmod(minutes, 60)
    
    if hours > 0:
        return f"{hours}小时{minutes}分钟{seconds}秒"
    elif minutes > 0:
        return f"{minutes}分钟{seconds}秒"
    else:
        return f"{seconds}秒"

def main(stats_callback=None):
    """
    执行AD同步的主函数
    
    参数:
        stats_callback: 可选的回调函数，用于实时报告进度
                        接收两个参数：stage(阶段)和data(数据)
    """
    start_time = time.time()
    sync_stats = {
        'total_users': 0,
        'processed_users': 0,
        'disabled_users': [],
        'error_count': 0,
        'log_file': ''  # 添加日志文件路径
    }

    # 设置日志
    logger = setup_logging()
    sync_stats['log_file'] = log_filename  # 保存日志文件名
    
    # 读取配置文件
    config_parser = configparser.ConfigParser()
    config_parser.read('config.ini', encoding='utf-8')
    
    # 配置信息
    config = {
        'wecom': {
            'corpid': config_parser.get('WeChat', 'CorpID'),
            'corpsecret': config_parser.get('WeChat', 'CorpSecret')
        },
        'domain': config_parser.get('Domain', 'Name'),
        'exclude_departments': [d.strip() for d in config_parser.get('ExcludeDepartments', 'Names').split(',')],
        'exclude_accounts': [
            *[acc.strip() for acc in config_parser.get('ExcludeUsers', 'SystemAccounts').split(',') if acc.strip()],
            *[acc.strip() for acc in config_parser.get('ExcludeUsers', 'CustomAccounts').split(',') if acc.strip()]
        ],
        'webhook_url': config_parser.get('WeChatBot', 'WebhookUrl')
    }

    try:
        # 验证机器人webhook地址
        if not config['webhook_url'] or 'key=' not in config['webhook_url']:
            logger.error("企业微信机器人webhook地址无效")
            raise ValueError("无效的webhook地址")

        bot = WeChatBot(config['webhook_url'])
        
        # 发送开始执行通知
        start_message = f"""## 企业微信-AD同步开始执行
        
> 开始时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
> 域名: {config['domain']}
"""
        bot.send_message(start_message)

        # 初始化企业微信API和AD同步
        wecom = WeComAPI(config['wecom']['corpid'], config['wecom']['corpsecret'])
        ad_sync = ADSync(
            config['domain'], 
            config['exclude_departments'],
            config['exclude_accounts']
        )

        # 获取所有企业微信用户ID
        wecom_users = set()
        departments = wecom.get_department_list()
        
        for dept in departments:
            users = wecom.get_department_users(dept['id'])
            wecom_users.update(user['userid'] for user in users)
        
        logger.info(f"企业微信中共有 {len(wecom_users)} 个用户账户")
        
        # 通过回调报告总用户数
        if stats_callback:
            stats_callback('total_users', len(wecom_users))
            sync_stats['total_users'] = len(wecom_users)

        # 构建部门树和计算路径的逻辑需要修改
        dept_tree = {}
        for dept in departments:
            dept_tree[dept['id']] = {
                'name': dept['name'],
                'parentid': dept['parentid'],
                'path': []
            }

        # 计算每个部门的完整路径
        for dept_id in dept_tree:
            path = []
            current_id = dept_id
            while (current_id != 0):  # 根部门的 parentid 为 0
                if (current_id not in dept_tree):
                    break
                path.insert(0, dept_tree[current_id]['name'])
                current_id = dept_tree[current_id]['parentid']
            dept_tree[dept_id]['path'] = path

        # 同步OU结构
        total_depts = len(dept_tree)
        dept_count = 0
        
        for dept_id, dept_info in dept_tree.items():
            current_path = []
            for ou_name in dept_info['path']:
                current_path.append(ou_name)
                if len(current_path) > 1:
                    parent_path = current_path[:-1]
                    parent_dn = ad_sync.get_ou_dn(parent_path)
                else:
                    parent_dn = f"DC={config['domain'].replace('.', ',DC=')}"
                ad_sync.create_ou(ou_name, parent_dn)
            
            # 报告部门同步进度
            dept_count += 1
            if stats_callback and dept_count % 5 == 0:  # 每5个部门报告一次进度
                stats_callback('department_progress', dept_count / total_depts)
        
        # 通知部门同步完成
        if stats_callback:
            stats_callback('department_sync_done', True)

        # 同步用户
        logger.info("开始同步用户...")
        processed_users = set()

        # 收集用户的所有部门信息
        user_departments = {}  # userid -> List[dept_info]
        for dept_id in dept_tree:
            users = wecom.get_department_users(dept_id)
            for user in users:
                userid = user['userid']
                if userid not in user_departments:
                    user_departments[userid] = {
                        'user_info': user,
                        'departments': []
                    }
                user_departments[userid]['departments'].append(dept_tree[dept_id])

        # 处理所有用户
        user_count = 0
        for userid, info in user_departments.items():
            if userid in processed_users:
                continue

            user = info['user_info']
            departments = info['departments']
            username = user['userid']
            display_name = user['name']

            # 选择合适的部门作为用户的OU
            target_dept = None
            for dept in departments:
                # 如果部门不在排除列表中，则选择该部门
                if dept['path'] and dept['path'][-1] not in config['exclude_departments']:
                    target_dept = dept
                    break

            if not target_dept:
                logger.warning(f"用户 {username} ({display_name}) 所有部门都在排除列表中，跳过处理")
                processed_users.add(userid)
                continue

            ou_path = target_dept['path']
            ou_dn = ad_sync.get_ou_dn(ou_path)
            
            # 获取用户详细信息
            user_detail = wecom.get_user_detail(username)
            email = user_detail.get('email', '')
            
            # 如果企业微信没有设置邮箱，则使用默认规则生成
            if not email:
                email = f"{username}@{config['domain']}"
                logger.warning(f"用户 {display_name}({username}) 在企业微信中未设置邮箱，使用默认邮箱: {email}")

            logger.info(f"处理用户: {display_name}, 用户ID: {username}, 邮箱: {email}, 选定部门: {ou_path[-1]}")

            # 检查用户是否存在
            existing_user = ad_sync.get_user(username)
            if existing_user:
                # 更新现有用户
                ad_sync.update_user(
                    username,
                    display_name,
                    email,
                    ou_dn
                )
            else:
                # 创建新用户
                if ad_sync.create_user(
                    username,
                    display_name,
                    email,
                    ou_dn
                ):
                    logger.info(f"成功创建用户: {username}")
                else:
                    logger.error(f"创建用户失败: {username}")
                    continue

            # 将用户添加到所有非排除部门的安全组中
            for dept in departments:
                if dept['path'] and dept['path'][-1] not in config['exclude_departments']:
                    ou_name = dept['path'][-1]
                    ad_sync.add_user_to_group(username, ou_name)
            
            processed_users.add(userid)
            
            # 报告进度
            user_count += 1
            if stats_callback:
                stats_callback('user_processed', user_count)

        # 更新统计信息
        sync_stats['total_users'] = len(wecom_users)
        sync_stats['processed_users'] = len(processed_users)

        logger.info(f"用户同步完成，共处理 {len(processed_users)} 个用户")

        # 处理需要禁用的账户
        logger.info("开始处理需要禁用的账户...")
        enabled_ad_users = ad_sync.get_all_enabled_users()
        logger.info(f"AD域控中共有 {len(enabled_ad_users)} 个启用状态的账户")

        # 找出需要禁用的账户（排除特定账户）
        users_to_disable = set(enabled_ad_users) - wecom_users
        
        # 报告禁用用户数量
        if stats_callback:
            stats_callback('users_to_disable', len(users_to_disable))
        
        if users_to_disable:
            logger.info(f"发现 {len(users_to_disable)} 个需要禁用的账户")
            
            # 记录禁用操作的详细日志
            log_dir = "logs"
            if not os.path.exists(log_dir):
                os.makedirs(log_dir)
                
            disable_log_filename = os.path.join(log_dir, f"disabled_accounts_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv")
            with open(disable_log_filename, 'w', encoding='utf-8', newline='') as f:
                # 修改字段名以匹配 AD 用户信息中的实际字段
                writer = csv.DictWriter(f, fieldnames=[
                    'SamAccountName',
                    'DisplayName',
                    'Mail',  # 改为与 AD 返回的字段名一致
                    'Created',
                    'Modified',
                    'LastLogonDate',
                    'Description',
                    'DisableTime'
                ])
                writer.writeheader()
                
                disabled_count = 0
                for username in users_to_disable:
                    user_details = ad_sync.get_user_details(username)
                    if user_details:
                        # 添加禁用时间
                        user_details['DisableTime'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                        # 确保所有字段都存在
                        for field in writer.fieldnames:
                            if field not in user_details:
                                user_details[field] = ''
                        writer.writerow(user_details)
                    
                    # 禁用账户
                    if ad_sync.disable_user(username):
                        disabled_count += 1
                        # 报告禁用进度
                        if stats_callback and disabled_count % 5 == 0:  # 每5个用户报告一次
                            stats_callback('users_disabled', disabled_count)
            
            logger.info(f"已将禁用账户信息记录到文件: {disable_log_filename}")
            sync_stats['disabled_users'] = list(users_to_disable)
        else:
            logger.info("没有需要禁用的账户")

        # 计算执行时间
        end_time = time.time()
        duration = format_time_duration(end_time - start_time)
        
        # 报告完成时间
        if stats_callback:
            stats_callback('sync_duration', duration)

        # 构建通知消息
        result_message = f"""## 企业微信-AD同步执行结果
        
> 执行时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
> 耗时: {duration}

### 同步统计
- 企业微信总用户数: {sync_stats['total_users']}
- 成功同步用户数: {sync_stats['processed_users']}
- 禁用用户数: {len(sync_stats['disabled_users'])}
- 错误数: {sync_stats['error_count']}

{"### 被禁用的账户" if sync_stats['disabled_users'] else ""}
{"".join([f"- {user}\n" for user in sync_stats['disabled_users']])}

详细日志请查看: {sync_stats['log_file']}
"""
        send_result = bot.send_message(result_message)
        if not send_result:
            logger.warning("发送执行结果通知失败")

        logger.info("所有同步操作已完成")
        
        # 返回同步结果统计数据，供UI调用
        return sync_stats

    except Exception as e:
        sync_stats['error_count'] += 1
        error_message = f"""## 企业微信-AD同步执行异常
        
> 执行时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

### 错误信息
{str(e)}

请检查日志文件了解详细信息。
"""
        try:
            bot = WeChatBot(config['webhook_url'])
            bot.send_message(error_message)
        except:
            logger.error("发送错误通知失败")
            
        logger.error(f"同步过程出现错误: {str(e)}")
        raise

if __name__ == '__main__':
    main()
