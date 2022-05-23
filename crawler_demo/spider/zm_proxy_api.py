# coding=utf-8
import requests

# 请求地址
targetUrl = "https://www.baidu.com"

# 代理服务器
proxyHost = "27.150.86.149"
proxyPort = "4245"

proxyMeta = "http://%(host)s:%(port)s" % {

    "host": proxyHost,
    "port": proxyPort,
}

proxies = {
    "http": proxyMeta,
    "https": proxyMeta
}

resp = requests.get(targetUrl, proxies=proxies)
print(resp.status_code)
print(resp.text)

