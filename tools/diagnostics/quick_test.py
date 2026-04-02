#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""快速测试WeComAPI"""

import os
import sys

ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.insert(0, ROOT_DIR)

from sync_app.clients.wecom import WeComAPI
import configparser

print("=" * 60)
print("快速测试 WeComAPI（自建应用方式）")
print("=" * 60)

# 读取配置
config = configparser.ConfigParser()
config.read(os.path.join(ROOT_DIR, 'config.ini'), encoding='utf-8')

corpid = config.get('WeChat', 'corpid')
corpsecret = config.get('WeChat', 'corpsecret')
agentid = config.get('WeChat', 'agentid', fallback=None)

print(f"\n配置信息:")
print(f"  CorpID: {corpid}")
if agentid:
    print(f"  AgentID: {agentid}")
    print(f"  认证方式: 自建应用")
else:
    print(f"  认证方式: 通用")

# 测试初始化
print(f"\n步骤1: 初始化WeComAPI...")
try:
    api = WeComAPI(corpid, corpsecret, agentid)
    print(f"  成功! WeComAPI已初始化")
except Exception as e:
    print(f"  失败: {e}")
    sys.exit(1)

# 测试获取部门列表
print(f"\n步骤2: 获取部门列表...")
try:
    depts = api.get_department_list()
    print(f"  成功! 获取到 {len(depts)} 个部门")
    if depts and len(depts) > 0:
        print(f"\n  前3个部门:")
        for dept in depts[:3]:
            print(f"    - ID:{dept['id']}, 名称:{dept['name']}")
except Exception as e:
    print(f"  失败: {e}")
    sys.exit(1)

print("\n" + "=" * 60)
print("测试通过! WeComAPI工作正常")
print("=" * 60)

