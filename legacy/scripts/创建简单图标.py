#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
创建一个简单的图标文件
如果有专业的图标，可以跳过此步骤
"""

try:
    from PIL import Image, ImageDraw, ImageFont
    
    def create_simple_icon():
        """创建一个简单的同步图标"""
        # 创建一个64x64的图像
        size = 64
        img = Image.new('RGBA', (size, size), (0, 0, 0, 0))
        draw = ImageDraw.Draw(img)
        
        # 背景圆形（蓝色）
        draw.ellipse([2, 2, size-2, size-2], fill=(41, 128, 185, 255), outline=(52, 152, 219, 255), width=2)
        
        # 绘制同步箭头（简化版）
        # 上箭头
        draw.polygon([(32, 15), (42, 25), (37, 25), (37, 35), (27, 35), (27, 25), (22, 25)], fill='white')
        # 下箭头
        draw.polygon([(32, 49), (22, 39), (27, 39), (27, 29), (37, 29), (37, 39), (42, 39)], fill='white')
        
        # 保存为多种尺寸的ICO文件
        icon_sizes = [(16, 16), (32, 32), (48, 48), (64, 64)]
        imgs = []
        for icon_size in icon_sizes:
            imgs.append(img.resize(icon_size, Image.Resampling.LANCZOS))
        
        # 保存为ICO
        imgs[0].save('icon.ico', format='ICO', sizes=[(s[0], s[1]) for s in icon_sizes], append_images=imgs[1:])
        print("✅ 图标创建成功: icon.ico")
        print("   - 包含尺寸: 16x16, 32x32, 48x48, 64x64")
        print("   - 可以重新打包程序以应用图标")
        
except ImportError:
    print("❌ 需要安装 Pillow 库")
    print("   运行: pip install Pillow")
    print("")
    print("或者手动准备图标文件:")
    print("1. 准备一个图标文件（.ico格式）")
    print("2. 命名为 icon.ico")
    print("3. 放在项目根目录")
    print("4. 重新运行打包脚本")

if __name__ == '__main__':
    create_simple_icon()

