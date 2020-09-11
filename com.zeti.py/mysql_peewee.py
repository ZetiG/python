# connection MySQL By peewee
from typing import Type
from peewee import *
# from flask import Flask
from playhouse.pool import PooledPostgresqlExtDatabase

# Connect to a MySQL database on network.
data_base = {
    'host': '192.168.1.249',
    'port': 3306,
    'user': 'root',
    'passwd': '1234567'
}

mysql_db = MySQLDatabase("test", **data_base)


class Book(Model):
    id = IntegerField()
    author = CharField()
    title = TextField()
    num = IntegerField()

    class Meta:
        database = mysql_db


# query
for book in Book:
    print(str(book.id) + " " + book.author + " " + book.title + " " + str(book.num) + '\n')

# （1）查询所有
s = Book.select()  # s返回的是一个对象，需要对象.属性名获取属性值
print("这是查询所有的")
for i in s:
    print(i.author, i.title, i.num)
print("------------------")

# （2）有条件查询
s = Book.get(Book.id == 5)
print("查询id为5的数据")
print(s.author, s.title, s.num)
print("------------------")

# （3）正序、倒叙查询
s = Book.select().order_by(Book.id.asc())  # 升序，默认为升序
s_ = Book.select().order_by(Book.id.desc())  # 降序
print("id降序查询")
for i in s_:
    print(i.author, i.title, i.num)


# create table
# Book.create_table()

# insert
# book = Book(author="insert_test", title='python wa wa', num=666)
# book.save()

# Book.insert(author="insert_test2", title='python is wa2 wa2', num=667).execute()

# delete
# bk = Book.get(author="me")
# bk.delete_instance()
# Book.delete_by_id(6)
# Book.delete().where(Book.id == 3).execute()
