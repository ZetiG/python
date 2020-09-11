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

# create table
# Book.create_table()

# insert
# book = Book(author="insert_test", title='python wa wa', num=666)
# book.save()
