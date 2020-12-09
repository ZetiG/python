from datetime import datetime
from peewee import Column
from peewee import IntegerField, CharField, DecimalField, Model
from peewee import MySQLDatabase
from com.zeti.py.crawler.mysql_connect import mysql_db


class UserEntity:
    id = IntegerField(verbose_name="id", index=True)
    user_name = CharField(max_length=32, verbose_name="用户名")
    password = CharField(max_length=128, verbose_name="密码")
    creator = CharField(max_length=32, verbose_name="创建人")
    created = IntegerField(verbose_name="创建时间")
    updated = IntegerField(verbose_name="修改时间")
    is_deleted = IntegerField(verbose_name="删除: 0正常 1删除")

    def __init__(self) -> None:
        super().__init__()

    class Meta:
        database = mysql_db
