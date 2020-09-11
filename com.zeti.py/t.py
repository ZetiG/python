from urllib import request, parse
import urllib.error
import ssl

# try:
#     response = urllib.request.urlopen('http://www.xxxxx.com')
#     print(response.read().decode('utf-8'))
# except Exception as err:
#     print(f'Other error occurred: {err}')

# 验证ssl
context = ssl.create_default_context()

#
url = 'https://www.dev-cms-member.mamaqunaer.com/login'
headers = {
    # 假装自己是浏览器
    'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) '
                  'Chrome/71.0.3578.98 Safari/537.36',
}

# request param
dict = {
    'account': '18888888888',
    'password': '123456'
}

# 请求的参数转化为 byte
data = bytes(parse.urlencode(dict), 'utf-8')

# 封装请求
req = request.Request(url, data=dict, headers=headers, method='POST')

# run
response = request.urlopen(req, context=context)
print(response.read().decode('utf-8'))
