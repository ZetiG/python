from pathlib import Path

import json
import re

import requests
from com.zeti.py.crawler.book_entity import DangBook, create_tb


# 循环请求
def main(total_page):
    create_tb(DangBook)

    for i in range(1, total_page):
        url = 'http://bang.dangdang.com/books/fivestars/01.00.00.00.00.00-recent30-0-0-1-' + str(i)
        html = request_dangdang(url)
        books = parse_result(html)  # 解析过滤我们想要的信息

        for book in books:
            write_item_to_mysql(book)


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
    books = re.findall(pattern, html)
    for book in books:
        yield {
            'range': book[0],
            'image': book[1],
            'title': book[2],
            'recommend': book[3],
            'author': book[4],
            'times': book[5],
            'price': book[6]
        }


# 写入文件
def write_item_to_file(item):
    print('开始写入数据 ====> ' + str(item))
    with open('book.txt', 'a', encoding='UTF-8') as f:
        f.write(json.dumps(item, ensure_ascii=False) + '\n')
        f.close()


# write mysql
def write_item_to_mysql(book):
    print(book['range'])
    print(book['image'])
    DangBook.insert(range=book['range'], image=book['image'], title=book['title'], recommend=book['recommend'],
                    author=book['author'], times=book['times'], price=book['price']) \
        .execute()


if __name__ == "__main__":
    main(26)
