from pathlib import Path

from peewee import IntegerField, CharField, DecimalField, Model
from com.zeti.py.crawler.mysql_connect import mysql_db

import sys
import os

# print('Running' if __name__ == '__main__' else 'Importing', Path(__file__).resolve())

PACKAGE_PARENT = '..'
SCRIPT_DIR = os.path.dirname(os.path.realpath(os.path.join(os.getcwd(), os.path.expanduser(__file__))))
sys.path.append(os.path.normpath(os.path.join(SCRIPT_DIR, PACKAGE_PARENT)))


# 图书类
class DangBook(Model):
    range = IntegerField(default=0, verbose_name="序号")
    image = CharField(max_length=256, verbose_name="图书封面")
    title = CharField(max_length=256, verbose_name="图书标题", index=True)
    recommend = CharField(max_length=256, verbose_name="推荐程度")
    author = CharField(max_length=256, verbose_name="推荐程度")
    times = CharField(max_length=256, verbose_name="推荐程度")
    price = DecimalField(max_digits=8, decimal_places=2, verbose_name="价格")

    def __init__(self) -> None:
        super().__init__()

    class Meta:
        database = mysql_db


# create table
def create_tb(table):
    if not table.table_exists():
        table.create_table()


if __name__ == "__main__":
    create_tb(DangBook)
