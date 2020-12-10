import configparser
from sqlalchemy import create_engine, Table, Column, Integer, String, MetaData, ForeignKey
from sqlalchemy.ext.declarative import declarative_base

# mysql连接引擎，echo开启调试

engine = create_engine("mysql+pymysql://root:1234567@192.168.1.249:3306/test", echo=True)

metadata = MetaData(engine)
print('tables=>', metadata.tables)

Base = declarative_base()
Base.metadata.reflect(engine)
metadata_tables = Base.metadata.tables
print(metadata_tables)
#
# # 配置文件
# mail_config_location = '/Users/zhangmengke/python/com/zeti/py/generator/config.ini'
# config = configparser.ConfigParser()
# config.read(mail_config_location, encoding="utf-8")
# print('host:', config.get('mysql', 'host'))
#
# # mysql connect config
# config = {
#     'host': config.get('mysql', 'host'),
#     'port': config.get('mysql', 'port'),
#     'user': config.get('mysql', 'username'),
#     'passwd': config.get('mysql', 'password'),
# }
#
# # connect
# mysql_db = MySQLDatabase('test', config)
