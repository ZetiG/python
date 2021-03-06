import flask
import json
from flask import request

# 测试接口

'''
flask： web框架，通过flask提供的装饰器@server.route()将普通函数转换为服务
登录接口，需要传url、username、passwd
'''
# 创建一个服务，把当前这个python文件当做一个服务
server = flask.Flask(__name__)


# server.config['JSON_AS_ASCII'] = False
# @server.route()可以将普通函数转变为服务 登录接口的路径、请求方式
@server.route('/login', methods=['post'])
def login():
    # 获取请求参数
    username = request.values.get('userName')
    pwd = request.values.get('password')

    # 非空判断
    if username and pwd:
        if username == 'test' and pwd == '123':
            ret = {'code': 200, 'message': '登录成功'}
            return json.dumps(ret, ensure_ascii=False)  # 将字典转换为json串, json是字符串
        else:
            ret = {'code': -1, 'message': '账号密码错误'}
            return json.dumps(ret, ensure_ascii=False)
    else:
        ret = {'code': 10001, 'message': '参数不能为空！'}
        return json.dumps(ret, ensure_ascii=False)


if __name__ == '__main__':
    server.run(debug=True, port=8009, host='0.0.0.0')  # 指定端口、host,0.0.0.0代表不管几个网卡，任何ip都可以访问
