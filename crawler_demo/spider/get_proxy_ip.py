#!/usr/bin/env python
# -*- coding:utf-8 -*-
import requests
from bs4 import BeautifulSoup
import pymysql
import time

'''
爬取(快代理)免费代理ip
'''

global ip_num
ip_num = 1
base_url = "https://www.kuaidaili.com/free/inha/"
headers = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_5) AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/69.0.3497.100 Safari/537.36"
}
proxies = {'http': '115.223.245.117:9000'}


def proxies_switch(url):
    print("正在进行ip的切换...")
    global ip_num, ip_post
    status = False
    while status is False:  # 找到合适的ip——post地址
        print("正在验证第%s个ip地址" % (ip_num))
        sql = "select ip,post from proxy_ip where id = %s" % (ip_num)
        cursor.execute(sql)
        items = cursor.fetchall()
        ip = items[0][0]
        post = items[0][1]
        ip_post = ip + ":" + post
        response = requests.get(url, proxies={'http': ip_post})
        status = response.ok
        ip_num = ip_num + 1
    proxies['http'] = ip_post
    print("第%ds个ip地址测试成功" % ip_num)
    time.sleep(0.5)  # 切换成功之后sleep一秒 防止新的ip_post被封


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
    conn = pymysql.connect(host='106.13.22.217', port=3306, user='root', passwd='root123', db='python_proxy')
    cursor = conn.cursor()
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