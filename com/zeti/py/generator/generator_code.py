import json
import flask
import time

from sqlalchemy import create_engine, MetaData, inspect
from sqlalchemy.ext.declarative import declarative_base
from flask import request, Blueprint, render_template
from flask_cors import CORS
import os

app = flask.Flask(__name__)
CORS(app, supports_credentials=True)
CORS(app, resources=r'/*')


"""定义全局变量，数据库所有表名"""
database_tables = ''


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
def get_database_all_tables(engine, table=''):
    inspector = inspect(engine)
    table_names = engine.table_names()
    for table_name in table_names:
        if table is not None and table == table_name:
            return inspector.get_columns(table_name)
    return table_names


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

@app.route('/index')
def index():
    return render_template('create_class.html')


@app.route("/connect", methods=['POST'])
def connect():
    """连接数据库，读取数据库表，返回列表渲染页面"""
    database_host = request.form.get('database_host')
    database_port = request.form.get('database_port')
    database_base_name = request.form.get('database_baseName')
    database_user_name = request.form.get('database_userName')
    database_password = request.form.get('database_password')

    # if database_host is None or len(database_host) == 0:
    #     return json.dumps({'msg': '数据库IP不能为空', 'code': '-1', 'data': ''})
    #
    # if database_port is None or len(database_port) == 0:
    #     database_port = 3306
    #
    # if database_base_name is None or len(database_base_name) == 0:
    #     return json.dumps({'msg': '数据库名称不能为空', 'code': '-1', 'data': ''})
    #
    # if database_user_name is None or len(database_user_name) == 0:
    #     return json.dumps({'msg': '用户名不能为空', 'code': '-1', 'data': ''})
    #
    # if database_password is None or len(database_password) == 0:
    #     return json.dumps({'msg': '密码不能为空', 'code': '-1', 'data': ''})

    global database_tables
    # database = connect_database(database_host, database_port, database_base_name, database_user_name, database_password)
    database_tables = connect_database('106.13.22.217', '3306', 'delta_medical', 'root', 'mysql123')
    tables = get_database_all_tables(database_tables)
    return json.dumps({'msg': 'success', 'code': '1', 'data': tables})


@app.route("/generator/code", methods=['POST'])
def generator():
    """package url"""
    package_url = request.form.get('package_url')
    author = request.form.get('author')

    """module"""
    controller = int(request.form.get('controller'))
    service = int(request.form.get('service'))
    dao = int(request.form.get('dao'))
    entity = int(request.form.get('entity'))

    if controller == 0 and service == 0 and dao == 0 and entity == 0:
        return json.dumps({'msg': '请至少选择一个需要生成的module', 'code': '-1', 'data': ''})

    """method"""
    page = int(request.form.get('page'))
    list = int(request.form.get('list'))
    selectById = int(request.form.get('selectById'))
    save = int(request.form.get('save'))
    update = int(request.form.get('update'))
    deleteById = int(request.form.get('deleteById'))

    """select databases"""
    databases = request.form.get('selectDatabases')

    """generator template"""
    if databases is None:
        return json.dumps({'msg': '请至少选择一个需要生成的表', 'code': '-1', 'data': ''})

    split = databases.split(",")
    for tb in split:
        columns = get_database_all_tables(database_tables, tb)
        if entity == 1:
            date = time.strftime("%Y-%m-%d", time.localtime())
            created_entity(package_url, author, tb, columns, 'mysql', date)
        if dao == 1:
            created_dao(package_url, author, tb, date)
        if service == 1:
            print('此处生成service')
        if controller == 1:
            print('此处生成controller')

        # tableName = str2Hump(i)
        # content = render_template('entity_mysql_templates.html', **c)
        # create_file(tableName[0].upper() + tableName[1:], i, content)

    str = '{"package_url": ' + package_url + ', "author": ' + author + ', "databases": ' + databases + '}'

    print(str)

    return json.dumps({'msg': 'success', 'code': '1', 'data': str})


def created_entity(package, author, table_name, columns, db_type, date):
    class_name = big_str(str2Hump(table_name))
    '''实体类生成

    根据表字段生成具体实体类

    Args:
        package (str): 生成实体类的包路径
        author (str): 作者
        table_name (str): 需要生成的表名
        columns (columns): 表字段
        db_type (str): 数据库类型
        db_type (str): 数据库类型

    Returns:
        file : 生成.java文件
    '''
    propertys = []
    if columns:
        for column in columns:
            type_name = column['type'].python_type.__name__
            if type_name == 'str':
                type_name = 'String'
            s = 'private %s %s;' % (type_name, str2Hump(column['name']))
            pro = {}
            pro.setdefault('property', s)
            comment_ = column['comment']
            if comment_ is None:
                comment_ = str2Hump(column['name'])
            pro.setdefault('comment', comment_)
            propertys.append(pro)
    c = {'package': package + '.entity',
         'author': author,
         'entity_package': package + '.entity.' + class_name,
         'table_name': table_name,
         'class_name': class_name,
         'propertys': propertys,
         'date': date}
    if db_type == 'mysql':
        s = render_template('entity_mysql_templates.html', **c)
        create_file(class_name, package + '.entity', s)
        s = render_template('entity_mysql_mapper_templates.html', **c)
        create_file(class_name, package + '.entity', s, 'Mapper.xml')


def created_dao(package, author, table_name, date):
    class_name = big_str(str2Hump(table_name))
    c = {'package': package + '.dao',
         'author': author,
         'entity_package': package + '.dao.' + class_name,
         'table_name': table_name,
         'class_name': class_name,
         'date': date}
    s = render_template('dao_templates.html', **c)
    create_file(class_name, package + '.dao', s, 'Dao.java')


# 创建java文件
def create_file(class_name, package, content, suffix='.java'):
    dirs = os.getcwd() + '/file/' + package.replace('.', '/') + '/'
    if not os.path.exists(dirs):
        os.makedirs(dirs, 0o777)
    fd = os.open(dirs + class_name + suffix, os.O_WRONLY | os.O_CREAT)
    os.write(fd, content.encode(encoding="utf-8", errors="strict"))
    os.close(fd)


# 下划线转驼峰
def str2Hump(table_name):
    arr = filter(None, table_name.lower().split('_'))
    res = ''
    j = 0
    for i in arr:
        if j == 0:
            res = i
        else:
            res = res + i[0].upper() + i[1:]
        j += 1
    return res


# 将首字母转换为大写
def big_str(s):
    if len(s) <= 1:
        return s
    return (s[0:1]).upper() + s[1:]


# 将首字母转换为小写
def small_str(s):
    if len(s) <= 1:
        return s
    return (s[0:1]).lower() + s[1:]


if __name__ == '__main__':
    app.run(debug=True, port=8001, host='localhost')
    # create_file('test', 'package', 'text')
    # print(str2Hump('table_aa_bb_c'))
    # print(str2Hump('table_aa_bb_c')[0].upper() + str2Hump('table_aa_bb_c')[1:])
