import json
import flask

from sqlalchemy import create_engine, MetaData, inspect
from sqlalchemy.ext.declarative import declarative_base
from flask import request,Blueprint
from flask_cors import CORS

app = flask.Flask(__name__)
CORS(app, supports_credentials=True)
CORS(app, resources=r'/*')


# mysql连接引擎，echo开启调试
def connect_database(database_host, database_port, database_base_name, database_user_name, database_password):
    print(database_host, database_port, database_base_name, database_user_name, database_password)
    connect_url = "mysql+pymysql://" + database_user_name + ":" + str(database_password) + "@" + str(
        database_host) + ":" + str(database_port) + "/" + database_base_name + ""
    engine = create_engine(connect_url, echo=False)
    return engine


def get_database_tables(engine):
    Base = declarative_base()
    Base.metadata.reflect(engine)
    metadata_tables = Base.metadata.tables
    print('metadata_tables=>', metadata_tables)


# 输出所有表名
def get_database_all_tables(engine):
    names = engine.table_names()
    for name in names:
        print('table: ', name)
    return names


# inspector = inspect(engine)
# schemas = inspector.get_schema_names()  # 该链接下所有的库
# for schema in schemas:
#     if str(schema) == '#mysql50#.local':
#         continue;
#     print("schema: %s" % schema)
#     for table_name in inspector.get_table_names(schema=schema):
#         print("Table: %s" % table_name)
#         for column in inspector.get_columns(table_name, schema=schema):
#             print("Column: %s" % column)


@app.route("/connect", methods=['POST'])
def connect():
    """连接数据库，读取数据库表，返回列表渲染页面"""
    database_host = request.form.get('database_host')
    database_port = request.form.get('database_port')
    database_base_name = request.form.get('database_baseName')
    database_user_name = request.form.get('database_userName')
    database_password = request.form.get('database_password')

    if database_host is None or len(database_host) == 0:
        return json.dumps({'msg': '数据库IP不能为空', 'code': '-1', 'data': ''})

    if database_port is None or len(database_port) == 0:
        database_port = 3306

    if database_base_name is None or len(database_base_name) == 0:
        return json.dumps({'msg': '数据库名称不能为空', 'code': '-1', 'data': ''})

    if database_user_name is None or len(database_user_name) == 0:
        return json.dumps({'msg': '用户名不能为空', 'code': '-1', 'data': ''})

    if database_password is None or len(database_password) == 0:
        return json.dumps({'msg': '密码不能为空', 'code': '-1', 'data': ''})

    database = connect_database(database_host, database_port, database_base_name, database_user_name, database_password)
    tables = get_database_all_tables(database)
    return json.dumps({'msg': 'success', 'code': '1', 'data': tables})


@app.route("/generator/code", methods=['POST'])
def generator():
    package_url = request.form.get('package_url')
    author = request.form.get('author')
    controller = request.form.get('controller')
    service = request.form.get('service')
    dao = request.form.get('dao')
    entity = request.form.get('entity')

    print("package_url：{}", package_url)
    print("author：{}", author)
    print("controller：{}", controller)
    return json.dumps({'msg': 'success', 'code': '1', 'data': "afghjklkjhgfds"})


if __name__ == '__main__':
    app.run(debug=True, port=8001, host='localhost')
