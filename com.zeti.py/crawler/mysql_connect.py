from peewee import MySQLDatabase
# import sys
# import os
#
# sys.path.append(os.path.realpath('.'))

# mysql connect config
config = {
    'host': '192.168.1.249',
    'port': 3306,
    'user': 'root',
    'passwd': '1234567'
}

# connect
mysql_db = MySQLDatabase('test', config)
