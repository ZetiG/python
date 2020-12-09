from pathlib import Path
from peewee import MySQLDatabase

# print('Running' if __name__ == '__main__' else 'Importing', Path(__file__).resolve())

# mysql connect config
config = {
    'host': '192.168.1.249',
    'port': 3306,
    'user': 'root',
    'passwd': '1234567'
}

config_baidu = {
    'host': '106.13.22.217',
    'port': 3306,
    'user': 'root',
    'passwd': 'mysql123'
}

# connect
mysql_db = MySQLDatabase('py_test', config_baidu)
