import json
import re

import requests


# 循环请求
def main(total_page):
    for i in range(1, total_page):
        url = 'http://bang.dangdang.com/books/fivestars/01.00.00.00.00.00-recent30-0-0-1-' + str(i)
        html = request_dangdang(url)
        items = parse_result(html)  # 解析过滤我们想要的信息

        for item in items:
            write_item_to_file(item)
            # print(item)


# request
def request_dangdang(url):
    try:
        response = requests.get(url)
        if response.status_code == 200:
            return response.text
    except requests.RequestException:
        return None


# 正则过滤html
def parse_result(html):
    pattern = re.compile(
        '<li>.*?list_num.*?(\d+).</div>.*?<img src="(.*?)".*?class="name".*?title="(.*?)">.*?class="star">.*?class="tuijian">(.*?)</span>.*?class="publisher_info">.*?target="_blank">(.*?)</a>.*?class="biaosheng">.*?<span>(.*?)</span></div>.*?<p><span\sclass="price_n">&yen;(.*?)</span>.*?</li>',
        re.S)
    items = re.findall(pattern, html)
    for item in items:
        yield {
            'range': item[0],
            'iamge': item[1],
            'title': item[2],
            'recommend': item[3],
            'author': item[4],
            'times': item[5],
            'price': item[6]
        }


# 写入文件
def write_item_to_file(item):
    print('开始写入数据 ====> ' + str(item))
    with open('book.txt', 'a', encoding='UTF-8') as f:
        f.write(json.dumps(item, ensure_ascii=False) + '\n')
        f.close()


# 存入数据库
def insert_item_to_mysql(item):
    print('开始写入数据库 ==> ' + str(item))


if __name__ == "__main__":
    main(2)
