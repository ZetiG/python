from pathlib import Path
from typing import Any

from peewee import IntegerField, CharField, DecimalField
from com.zeti.py.crawler.mysql_connect import mysql_db

import sys
import os

print('Running' if __name__ == '__main__' else 'Importing', Path(__file__).resolve())

PACKAGE_PARENT = '..'
SCRIPT_DIR = os.path.dirname(os.path.realpath(os.path.join(os.getcwd(), os.path.expanduser(__file__))))
sys.path.append(os.path.normpath(os.path.join(SCRIPT_DIR, PACKAGE_PARENT)))


class DangBook:
    range = IntegerField(default=0, verbose_name="序号")
    image = CharField(max_length=256, verbose_name="图书封面")
    title = CharField(max_length=256, verbose_name="图书标题", index=True)
    recommend = CharField(max_length=256, verbose_name="推荐程度")
    author = CharField(max_length=256, verbose_name="推荐程度")
    times = CharField(max_length=256, verbose_name="推荐程度")
    price = DecimalField(max_digits=8, decimal_places=2, verbose_name="价格")

    def __init__(self) -> None:
        super().__init__()

    def __iter__(self):
        return self

    class Meta:
        database = mysql_db


def create_tb():
    # create table
    mysql_db.create_tables(DangBook)


if __name__ == "__main__":
    create_tb()
