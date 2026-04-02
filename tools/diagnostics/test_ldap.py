#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
LDAP连接测试脚本
用于验证LDAPS配置是否正确
"""

import configparser
import sys
from ldap3 import Server, Connection, ALL, NTLM, SIMPLE, Tls
import ssl

def test_ldap_connection():
    """测试LDAP连接"""
    print("=" * 60)
    print("  LDAP连接测试工具")
    print("=" * 60)
    print()
    
    # 读取配置
    try:
        config = configparser.ConfigParser()
        config.read('config.ini', encoding='utf-8')
        
        server_addr = config.get('LDAP', 'Server')
        domain = config.get('LDAP', 'Domain')
        username = config.get('LDAP', 'Username')
        password = config.get('LDAP', 'Password')
        use_ssl = config.getboolean('LDAP', 'UseSSL', fallback=True)
        port = config.getint('LDAP', 'Port', fallback=636 if use_ssl else 389)
        
        print(f"📋 配置信息：")
        print(f"   服务器: {server_addr}")
        print(f"   端口: {port}")
        print(f"   域名: {domain}")
        print(f"   用户: {username}")
        print(f"   SSL: {'启用' if use_ssl else '禁用'}")
        print()
        
        if not password:
            print("❌ 错误：未配置LDAP密码")
            print("   请在config.ini的[LDAP]节中设置Password参数")
            return False
            
    except Exception as e:
        print(f"❌ 读取配置失败: {str(e)}")
        return False
    
    # 测试连接
    try:
        print("🔌 正在连接LDAP服务器...")
        
        # 配置SSL
        if use_ssl:
            tls_config = Tls(
                validate=ssl.CERT_NONE,  # 生产环境应该验证证书
                version=ssl.PROTOCOL_TLSv1_2
            )
            server = Server(
                server_addr, 
                port=port, 
                use_ssl=True, 
                tls=tls_config,
                get_info=ALL
            )
        else:
            server = Server(server_addr, port=port, get_info=ALL)
        
        # 转换用户名格式
        def convert_username(user, dom):
            """转换用户名为UPN格式"""
            if '\\' in user:
                parts = user.split('\\')
                if len(parts) == 2:
                    return f"{parts[1]}@{dom}"
            if '@' in user:
                return user
            return f"{user}@{dom}"
        
        # 尝试建立连接（先NTLM，失败则SIMPLE）
        conn = None
        auth_type = "未知"
        
        try:
            # 尝试NTLM认证
            conn = Connection(
                server,
                user=username,
                password=password,
                authentication=NTLM,
                auto_bind=True,
                receive_timeout=30
            )
            auth_type = "NTLM"
            print("✅ LDAP连接成功！(使用NTLM认证)")
        except Exception as ntlm_error:
            # NTLM失败，尝试SIMPLE认证
            if "MD4" in str(ntlm_error) or "unsupported hash type" in str(ntlm_error):
                print("⚠️  NTLM认证失败（MD4不支持），尝试SIMPLE认证...")
                username_upn = convert_username(username, domain)
                conn = Connection(
                    server,
                    user=username_upn,
                    password=password,
                    authentication=SIMPLE,
                    auto_bind=True,
                    receive_timeout=30
                )
                auth_type = "SIMPLE"
                print(f"✅ LDAP连接成功！(使用SIMPLE认证，用户名: {username_upn})")
            else:
                raise
        
        print()
        
        # 显示服务器信息
        print("📊 服务器信息：")
        print(f"   名称: {server.info.naming_contexts}")
        print(f"   供应商: {server.info.vendor_name}")
        print(f"   版本: {server.info.vendor_version}")
        print()
        
        # 测试搜索
        base_dn = ','.join([f"DC={part}" for part in domain.split('.')])
        print(f"🔍 测试搜索（Base DN: {base_dn}）...")
        
        conn.search(
            base_dn,
            '(objectClass=domain)',
            search_scope='BASE',
            attributes=['dc']
        )
        
        if conn.entries:
            print(f"✅ 搜索成功，找到 {len(conn.entries)} 个结果")
            print(f"   域信息: {conn.entries[0]}")
        else:
            print("⚠️  搜索未返回结果")
        
        print()
        
        # 测试用户查询
        print("👥 测试用户查询...")
        conn.search(
            base_dn,
            '(objectClass=user)',
            search_scope='SUBTREE',
            attributes=['sAMAccountName', 'displayName'],
            size_limit=5
        )
        
        if conn.entries:
            print(f"✅ 找到用户，显示前5个：")
            for entry in conn.entries[:5]:
                username = entry.sAMAccountName.value if hasattr(entry, 'sAMAccountName') else 'N/A'
                display = entry.displayName.value if hasattr(entry, 'displayName') else 'N/A'
                print(f"   - {username}: {display}")
        else:
            print("⚠️  未找到用户")
        
        print()
        
        # 关闭连接
        conn.unbind()
        print("✅ 连接已关闭")
        print()
        print("=" * 60)
        print("  测试通过！LDAP配置正确")
        print("=" * 60)
        
        return True
        
    except Exception as e:
        print(f"❌ LDAP连接失败: {str(e)}")
        print()
        print("💡 故障排查建议：")
        print("   1. 检查服务器地址和端口是否正确")
        print("   2. 确认防火墙已开放LDAP端口")
        print("   3. 验证用户名密码是否正确")
        print("   4. 确保LDAPS服务已启动（如果使用SSL）")
        print("   5. 检查域控是否安装了SSL证书")
        print()
        return False

if __name__ == '__main__':
    success = test_ldap_connection()
    sys.exit(0 if success else 1)

