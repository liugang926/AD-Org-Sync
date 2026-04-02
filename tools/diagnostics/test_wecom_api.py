#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
企业微信API连接测试工具
用于诊断API调用问题
"""

import requests
import json
import configparser
from datetime import datetime

def test_wecom_api():
    """测试企业微信API连接（自建应用方式）"""
    print("=" * 80)
    print("  企业微信API连接测试工具（自建应用方式）")
    print("=" * 80)
    print()
    
    # 读取配置
    config = configparser.ConfigParser()
    config.read('config.ini', encoding='utf-8')
    
    corpid = config.get('WeChat', 'corpid')
    corpsecret = config.get('WeChat', 'corpsecret')
    agentid = config.get('WeChat', 'agentid', fallback=None)
    
    print(f"📋 配置信息：")
    print(f"   企业ID (corpid): {corpid}")
    if agentid and agentid != '请填写应用ID':
        print(f"   应用ID (agentid): {agentid}")
        print(f"   认证方式: 🔷 自建应用")
    else:
        print(f"   认证方式: ⚠️ 通用方式（建议配置自建应用）")
    print(f"   Secret (前8位): {corpsecret[:8]}...")
    print()
    
    # 1. 测试获取access_token
    print("🔑 步骤1: 获取access_token...")
    try:
        token_url = f"https://qyapi.weixin.qq.com/cgi-bin/gettoken?corpid={corpid}&corpsecret={corpsecret}"
        response = requests.get(token_url, timeout=10)
        result = response.json()
        
        if result.get('errcode') == 0:
            access_token = result['access_token']
            print(f"   ✅ 获取access_token成功")
            print(f"   Token (前20位): {access_token[:20]}...")
            print(f"   有效期: {result.get('expires_in', 0)} 秒")
        else:
            print(f"   ❌ 获取access_token失败")
            print(f"   错误码: {result.get('errcode')}")
            print(f"   错误信息: {result.get('errmsg')}")
            return False
    except Exception as e:
        print(f"   ❌ 请求失败: {str(e)}")
        return False
    
    print()
    
    # 2. 测试获取部门列表
    print("🏢 步骤2: 获取部门列表...")
    try:
        dept_url = f"https://qyapi.weixin.qq.com/cgi-bin/department/list?access_token={access_token}"
        response = requests.get(dept_url, timeout=10)
        result = response.json()
        
        if result.get('errcode') == 0:
            departments = result.get('department', [])
            print(f"   ✅ 获取部门列表成功")
            print(f"   部门总数: {len(departments)} 个")
            
            # 显示前5个部门
            if departments:
                print(f"\n   前5个部门：")
                for dept in departments[:5]:
                    print(f"      - ID: {dept['id']}, 名称: {dept['name']}, 父部门ID: {dept.get('parentid', 0)}")
                if len(departments) > 5:
                    print(f"      ... 及其他 {len(departments) - 5} 个部门")
        else:
            print(f"   ❌ 获取部门列表失败")
            print(f"   错误码: {result.get('errcode')}")
            print(f"   错误信息: {result.get('errmsg')}")
            
            # 常见错误码解释
            errcode = result.get('errcode')
            if errcode == 60011:
                print(f"\n   ⚠️ 错误原因: 没有通讯录查看权限")
                print(f"   💡 解决方案（自建应用方式）:")
                print(f"      1. 在企业微信管理后台 -> 应用管理 -> 自建应用")
                print(f"      2. 设置应用的「可见范围」，添加需要同步的部门")
                print(f"      3. 确保应用已启用")
            elif errcode == 60020:
                print(f"\n   ⚠️ 错误原因: 部门不在应用可见范围内")
                print(f"   💡 解决方案: 扩大应用的可见范围")
            elif errcode == 40013:
                print(f"\n   ⚠️ 错误原因: CorpID无效")
                print(f"   💡 解决方案: 检查config.ini中的corpid是否正确")
            elif errcode == 40001:
                print(f"\n   ⚠️ 错误原因: Secret无效")
                print(f"   💡 解决方案:")
                print(f"      1. 检查config.ini中的corpsecret是否正确")
                print(f"      2. 确保使用的是【自建应用的Secret】，而不是通讯录管理Secret")
                print(f"      3. 在企业微信管理后台 -> 应用管理 -> 自建应用 -> 查看Secret")
            
            return False
    except Exception as e:
        print(f"   ❌ 请求失败: {str(e)}")
        return False
    
    print()
    
    # 3. 测试获取部门成员
    if departments:
        print("👥 步骤3: 获取部门成员（测试第一个部门）...")
        try:
            test_dept_id = departments[0]['id']
            user_url = f"https://qyapi.weixin.qq.com/cgi-bin/user/list?access_token={access_token}&department_id={test_dept_id}&fetch_child=0"
            response = requests.get(user_url, timeout=10)
            result = response.json()
            
            if result.get('errcode') == 0:
                userlist = result.get('userlist', [])
                print(f"   ✅ 获取部门成员成功")
                print(f"   部门ID: {test_dept_id}")
                print(f"   成员数: {len(userlist)} 个")
                
                # 显示前3个成员
                if userlist:
                    print(f"\n   前3个成员：")
                    for user in userlist[:3]:
                        print(f"      - 用户ID: {user.get('userid')}, 姓名: {user.get('name')}, 部门: {user.get('department', [])}")
                    if len(userlist) > 3:
                        print(f"      ... 及其他 {len(userlist) - 3} 个成员")
            else:
                print(f"   ❌ 获取部门成员失败")
                print(f"   错误码: {result.get('errcode')}")
                print(f"   错误信息: {result.get('errmsg')}")
                return False
        except Exception as e:
            print(f"   ❌ 请求失败: {str(e)}")
            return False
    
    print()
    print("=" * 80)
    if agentid and agentid != '请填写应用ID':
        print("✅ 所有测试通过！企业微信API配置正确（自建应用方式）")
        print(f"   应用ID: {agentid}")
        print(f"   可获取范围: 应用可见范围内的部门和成员")
    else:
        print("✅ 所有测试通过！企业微信API配置正确")
    print("=" * 80)
    return True

if __name__ == '__main__':
    success = test_wecom_api()
    if not success:
        print("\n💡 建议（自建应用方式）：")
        print("1. 检查config.ini中的corpid、agentid和corpsecret是否正确")
        print("2. 确认使用的是【自建应用的Secret】，而不是通讯录管理Secret")
        print("3. 在企业微信管理后台配置应用的「可见范围」")
        print("4. 确认应用已启用且可见范围包含需要同步的部门")
        print("5. 检查网络连接是否正常")
        print("\n📖 配置步骤：")
        print("   1. 登录企业微信管理后台 (work.weixin.qq.com)")
        print("   2. 应用管理 -> 自建 -> 创建应用（或使用现有应用）")
        print("   3. 记录应用的 AgentId 和 Secret")
        print("   4. 设置应用的「可见范围」，添加需要同步的部门")
        print("   5. 将 AgentId 和 Secret 填入 config.ini")

