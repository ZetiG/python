#!/usr/bin/env python
# -*- coding:utf-8 -*-
import os
import random
import requests
from bs4 import BeautifulSoup
import pymysql
import time
from configparser import ConfigParser
from fake_useragent import UserAgent

'''
爬取(快代理)免费代理ip
'''

ip_num = 1
base_url = "https://www.kuaidaili.com/free/inha/"
headers = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_5) AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/69.0.3497.100 Safari/537.36"
}
proxies = {'http': '115.223.245.117:9000'}

# 读取数据库配置
parser = ConfigParser()
parser.read('base_config.cfg')
host = parser.get('spider_sql_conf', 'host')
port = parser.get('spider_sql_conf', 'port')
user = parser.get('spider_sql_conf', 'user')
password = parser.get('spider_sql_conf', 'password')
db_name = parser.get('spider_sql_conf', 'db')

# 数据库连接
conn = pymysql.connect(host=host, port=int(port), user=user, passwd=password, db=db_name)
cursor = conn.cursor()


def proxies_switch(req_url, req_headers):
    print("正在进行ip的切换...")
    global ip_num, ip_post
    status = False
    while status is False:  # 找到合适的ip——post地址
        sql = "select ip,post from proxy_ip where id = %s" % (ip_num)
        cursor.execute(sql)
        items = cursor.fetchall()
        ip = items[0][0]
        post = items[0][1]
        ip_post = ip + ":" + post
        print("正在验证第{0}个ip地址: {1}".format(ip_num, ip_post))

        # 测试请求连通性
        time.sleep(random.randint(3, 5))  # 暂停3~5秒的整数秒，时间区间：[3,5]
        headers['user-agent'] = UserAgent(path=os.path.join('', 'fake_ua.json')).random
        response = requests.get(req_url, headers=req_headers, proxies={'http': ip_post}, timeout=30)

        status = response.ok and response.url.split('?')[0] == req_url
        ip_num = ip_num + 1
    proxies['http'] = ip_post
    print("第%ds个ip地址测试成功" % ip_num)
    time.sleep(0.5)  # 切换成功之后sleep一秒 防止新的ip_post被封
    return {'http': ip_post}


def reptile_ip(proxy_url):
    ip_list = []
    html = requests.get(proxy_url, headers=headers, proxies=proxies)
    print("连接情况为", html.ok)
    if html.ok is False:
        proxies_switch(proxy_url)
        html = requests.get(proxy_url, headers=headers, proxies=proxies)
    soup = BeautifulSoup(html.content, "html.parser")
    items = soup.find("table", class_="table table-bordered table-striped").find_all("tr")
    items = items[1:]
    for itm in items:
        book = {"ip": itm.find_all("td")[0].text,
                "post": itm.find_all("td")[1].text,
                "type": itm.find_all("td")[3].text,
                "place": itm.find_all("td")[4].text,
                "response_time": itm.find_all("td")[5].text
                }
        ip_list.append(book)
    return ip_list


if __name__ == "__main__":
    url = base_url
    # conn = pymysql.connect(host='106.13.22.217', port=3306, user='root', passwd='root123', db='python_proxy')
    # cursor = conn.cursor()
    cursor.execute("drop table if exists proxy_ip")
    create_table = """create table proxy_ip(
    id integer NOT NULL auto_increment PRIMARY KEY,
    ip char(50) not null ,
    post char(20) not null ,
    type char(20) not null,
    place char(50)not null,
    response_time char(20) not null)"""
    sql = "insert into proxy_ip (ip,post,type,place,response_time) values (%s,%s,%s,%s,%s)"
    cursor.execute(create_table)
    for i in range(1, 70):
        print(i)
        lists = reptile_ip(url + str(i))
        for list in lists:
            try:
                cursor.execute(sql, (list["ip"], list["post"], list["type"], list["place"], list["response_time"]))
                conn.commit()
                print(str(list["ip"]) + " -> has been keeped")
            except:
                conn.rollback()
