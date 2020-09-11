from peewee import IntegerField, CharField, DecimalField
from .mysql_connect import mysql_db

import sys
import os

sys.path.append(os.path.realpath(''))


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

    class Meta:
        database = mysql_db


def create_tb():
    # create table
    mysql_db.create_tables(DangBook)


if __name__ == "__main__":
    create_tb()
