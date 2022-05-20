from scrapy.cmdline import execute

import sys
import os

if __name__ == '__main__':
    sys.path.append(os.path.dirname(os.path.abspath(__file__)))  # 大括号内即把该文件路径变为绝对路径
    execute(["scrapy", "crawl", "crawler", "-o", "crw_result.json"])  # 以项目example为例
