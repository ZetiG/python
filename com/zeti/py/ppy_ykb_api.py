#!/usr/local/bin/python
# -*- coding: utf-8 -*-
import sys
import requests
import json
import pymysql
from datetime import datetime, timedelta


# 获取Token
def get_token():
    # 获取Token
    url = "https://dd2.ekuaibao.com/api/openapi/v1/auth/getAccessToken"
    headers = {'Content-Type': 'application/json', 'Accept': 'application/json'}
    data = {"appKey": "742edd7e-2c83-4983-9b59-500a7d94e9ae", "appSecurity": "86980dbf-e479-494a-9c54-ebd8513905b6"}
    res = requests.post(url, headers=headers, json=data)
    result = res.json()
    # print(json.dumps(result1))
    # 调用接口获取Token赋值，为后续接口调用所用
    accessToken1 = result['value']['accessToken']
    print(accessToken1)
    return accessToken1


# 调用API接口获取数据
def request_get(url=None, *args):
    # get 请求使用
    for arg in args:
        url += arg
    print(url)
    # 头部参数
    headers = {'Content-Type': 'application/json', 'Accept': 'application/json'}
    # API接口访问
    res = requests.get(url, headers=headers)
    # ASCII转为中文
    json_str = json.dumps(res.json()['items'], ensure_ascii=False)
    if json.loads(json.dumps(res.json())).get('count') is not None:
        count = res.json()['count']
    else:
        count = -1
    # print(json_str)
    print("接口查询范围内数据总行数：", count)
    # 将 JSON 对象转换为 Python 字典
    data = json.loads(json_str)
    return data, count


# 数据插入目标表
def insert_doris(table_name, data):
    # 获取字典长度，用于循环
    row_count = len(data)
    print("row_count: ", row_count)
    # 连接MySQL数据库
    db = pymysql.connect(
        host='172.16.31.193',
        user='bigdata',
        password='VB8kGYjN',
        db='ods_canal',
        port=9030
    )
    # 将数据存入数据库
    cursor = db.cursor()
    # 通过表名获取目标表插入语句
    query_sql = """select concat('insert into ',table_schema,'.',table_name,' (',GROUP_CONCAT(column_name),', 
                    etl_insert_time, etl_update_time) values (''') as insert_sql_header
                    ,group_concat(column_name_aux2,',') as insert_sql_body
                    from
                    (
                    select table_schema,table_name,column_name,concat(' + str(data[count][''',column_name,''']) + ') 
                    as column_name_aux,concat('str(data[count][''',column_name,'''])') as column_name_aux2
                    ,ordinal_position
                    from information_schema.columns where table_schema='ods_canal' 
                    and table_name='""" + table_name + """' and column_name not in ('etl_insert_time','etl_update_time') 
                    order by ordinal_position
                    ) T group by table_schema,table_name;"""
    # 插入语句赋值
    cursor.execute(query_sql)
    sql_str1 = cursor.fetchone()
    if sql_str1 is None:
        print("目标表不存在请确认书写是否正确，也可手动新增此表！")
        sys.exit(1)
    # 获取插入语句头部信息
    insert_sql_header = sql_str1[0]
    # 循环json内容
    count = 0
    sql_str_aux = ''
    while count < row_count:
        # print(count, " 小于 row")
        # SQL 插入语句
        if table_name == 'ods_ppy_ykb_expense_type':
            sql_str = insert_sql_header \
                      + str(data[count]['id']) + "','" \
                      + str(data[count]['name']) + "','" \
                      + str(data[count]['parentId']) + "','" \
                      + str(data[count]['active']) + "','" \
                      + str(data[count]['code']) + "',now(),now()) "
        elif table_name == 'ods_ppy_ykb_project_type':
            sql_str = insert_sql_header \
                      + str(data[count]['id']) + "','" \
                      + str(data[count]['name']) + "','" \
                      + str(data[count]['active']) + "','" \
                      + str(data[count]['code']) + "','" \
                      + str(data[count]['dimensionId']) + "','" \
                      + str(data[count]['parentId']) + "','" \
                      + str(data[count]['updateTime']) + "','" \
                      + str(data[count]['createTime']) + "',now(),now()) "
        elif table_name == 'ods_ppy_ykb_department_type':
            sql_str = insert_sql_header \
                      + str(data[count]['id']) + "','" \
                      + str(data[count]['name']) + "','" \
                      + str(data[count]['parentId']) + "','" \
                      + str(data[count]['active']) + "','" \
                      + str(data[count]['code']) + "','" \
                      + str(data[count]['updateTime']) + "','" \
                      + str(data[count]['createTime']) + "',now(),now()) "
        elif table_name == 'ods_ppy_ykb_receipt_template_type':
            sql_str = insert_sql_header \
                      + str(data[count]['id']) + "','" \
                      + str(data[count]['name']) + "','" \
                      + str(data[count]['active']) + "',now(),now()) "
        elif table_name == 'ods_ppy_ykb_receipt_list':
            # details内部循环，基于json数组数量进行循环取值
            if json.loads(json.dumps(data[count]['form'])).get('details') is not None:
                row_count_aux = len(json.loads(json.dumps(data[count]['form']))['details'])
            else:
                row_count_aux = 0
            count_aux = 0
            print("内部循环总行数: ", row_count_aux)
            print(insert_sql_header)
            sql_str = insert_sql_header \
                      + datetime.fromtimestamp(int(str(data[count]['createTime'])[0:10:])).strftime("%Y-%m-%d") + "','" \
                      + str(data[count]['id']) + "','" \
                      + str(data[count]['active']) + "','" \
                      + datetime.fromtimestamp(int(str(data[count]['createTime'])[0:10:])).strftime("%Y-%m-%d %H:%M:%S") + "','" \
                      + datetime.fromtimestamp(int(str(data[count]['updateTime'])[0:10:])).strftime("%Y-%m-%d %H:%M:%S") + "','" \
                      + str(data[count]['corporationId']) + "','" \
                      + str(data[count]['ownerId']) + "','" \
                      + str(data[count]['ownerDefaultDepartment']) + "','" \
                      + str(data[count]['state']) + "','" \
                      + str(data[count]['flowType']) + "','" \
                      + str(data[count]['formType']) + "','" \
                      + json.dumps(data[count]['logs'], ensure_ascii=False) + "','" \
                      + json.dumps(data[count]['actions'], ensure_ascii=False) + "',now(),now()) "

            if json.loads(json.dumps(data[count]['form'])) is not None and \
            json.loads(json.dumps(data[count]['form'])).get('details') is not None and \
            len(json.loads(json.loads(json.dumps(json.dumps(json.loads(json.dumps(data[count]['form']))['details']))))) > 0 and \
            json.loads(json.loads(json.dumps(json.dumps(json.loads(json.dumps(data[count]['form']))['details']))))[count_aux]['feeTypeForm'] is not None and \
            json.loads(json.loads(json.dumps(json.dumps(json.loads(json.dumps(data[count]['form']))['details']))))[count_aux]['feeTypeForm'].get('项目') is not None:
                projects = json.loads(json.loads(json.dumps(json.dumps(json.loads(json.dumps(data[count]['form']))['details']))))[count_aux]['feeTypeForm'].get('项目')
            else:
                projects = ''

            if json.loads(json.dumps(data[count]['form'])).get('details') is not None:
                details = json.dumps(json.loads(json.dumps(data[count]['form']))['details'], ensure_ascii=False)
            else:
                details = ''

            if json.loads(json.dumps(data[count]['form'])).get('expenseDate') is not None:
                expenseDate = str(json.loads(json.dumps(data[count]['form']))['expenseDate'])
            else:
                expenseDate = ''

            if json.loads(json.dumps(data[count]['form'])).get('expenseMoney') is not None:
                expenseMoney = json.dumps(json.loads(json.dumps(data[count]['form']))['expenseMoney'],
                                          ensure_ascii=False)
            else:
                expenseMoney = ''

            if json.loads(json.dumps(data[count]['form'])).get('companyRealPay') is not None:
                companyRealPay = json.dumps(json.loads(json.dumps(data[count]['form']))['companyRealPay'],
                                            ensure_ascii=False)
            else:
                companyRealPay = ''

            if json.loads(json.dumps(data[count]['form'])).get('writtenOffMoney') is not None:
                writtenOffMoney = json.dumps(json.loads(json.dumps(data[count]['form']))['writtenOffMoney'],
                                             ensure_ascii=False)
            else:
                writtenOffMoney = ''

            if json.loads(json.dumps(data[count]['form'])).get('expenseDepartment') is not None:
                expenseDepartment = str(json.loads(json.dumps(data[count]['form']))['expenseDepartment'])
            else:
                expenseDepartment = ''

            if json.loads(json.dumps(data[count]['form'])).get('payDate') is not None:
                payDate = str(json.loads(json.dumps(data[count]['form']))['payDate'])
            else:
                payDate = ''

            if json.loads(json.dumps(data[count]['form'])).get('payPlan') is not None:
                payPlan = json.dumps(json.loads(json.dumps(data[count]['form']))['payPlan'], ensure_ascii=False)
            else:
                payPlan = ''

            if json.loads(json.dumps(data[count]['form'])).get('flowEndTime') is not None:
                flowEndTime = str(json.loads(json.dumps(data[count]['form']))['flowEndTime'])
            else:
                flowEndTime = ''

            if json.loads(json.dumps(data[count]['form'])).get('paymentChannel') is not None:
                paymentChannel = json.loads(json.dumps(data[count]['form']))['paymentChannel']
            else:
                paymentChannel = ''

            if json.loads(json.dumps(data[count]['form'])).get('paymentAccountId') is not None:
                paymentAccountId = str(json.loads(json.dumps(data[count]['form']))['paymentAccountId'])
            else:
                paymentAccountId = ''

            if json.loads(json.dumps(data[count]['form'])).get('payeeId') is not None:
                payeeId = json.loads(json.dumps(data[count]['form']))['payeeId']
            else:
                payeeId = ''

            if json.loads(json.dumps(data[count]['form'])).get('payMoney') is not None:
                payMoney = json.dumps(json.loads(json.dumps(data[count]['form']))['payMoney'], ensure_ascii=False)
            else:
                payMoney = ''

            if json.loads(json.dumps(data[count]['form'])).get('timeToEnterPendingPayment') is not None:
                timeToEnterPendingPayment = str(
                    json.loads(json.dumps(data[count]['form']))['timeToEnterPendingPayment'])
            else:
                timeToEnterPendingPayment = ''

            if json.loads(json.dumps(data[count]['form'])).get('preNodeApprovedTime') is not None:
                preNodeApprovedTime = str(json.loads(json.dumps(data[count]['form']))['preNodeApprovedTime'])
            else:
                preNodeApprovedTime = ''

            if json.loads(json.dumps(data[count]['form'])).get('voucherStatus') is not None:
                voucherStatus = json.loads(json.dumps(data[count]['form']))['voucherStatus']
            else:
                voucherStatus = ''

            if json.loads(json.dumps(data[count]['form'])).get('submitDate') is not None:
                submitDate = str(json.loads(json.dumps(data[count]['form']))['submitDate'])
            else:
                submitDate = ''

            if json.loads(json.dumps(data[count]['form'])).get('specificationId') is not None:
                specificationId = str(json.loads(json.dumps(data[count]['form']))['specificationId'])
            else:
                specificationId = ''

            sql_str_aux = "insert into ods_canal.ods_ppy_ykb_receipt_list_detail (dateid, id, code, title, project," \
                          + " details, payDate, payPlan, payeeId, payMoney, submitDate, expenseDate, flowEndTime, " \
                          + "expenseMoney, voucherStatus, companyRealPay, paymentChannel, writtenOffMoney, " \
                          + "paymentAccountId, expenseDepartment, preNodeApprovedTime, specificationId, timeToEnterPendingPayment, " \
                          + "etl_insert_time, etl_update_time) values ('" \
                          + datetime.fromtimestamp(int(str(data[count]['createTime'])[0:10:])).strftime("%Y-%m-%d") \
                          + "','" \
                          + str(data[count]['id']) + "','" \
                          + json.loads(json.dumps(data[count]['form']))['code'] + "','" \
                          + json.loads(json.dumps(data[count]['form']))['title'] + "','" \
                          + projects + "','" \
                          + details + "','" \
                          + payDate + "','" \
                          + payPlan + "','" \
                          + payeeId + "','" \
                          + payMoney + "','" \
                          + submitDate + "','" \
                          + expenseDate + "','" \
                          + flowEndTime + "','" \
                          + expenseMoney + "','" \
                          + voucherStatus + "','" \
                          + companyRealPay + "','" \
                          + paymentChannel + "','" \
                          + writtenOffMoney + "','" \
                          + paymentAccountId + "','" \
                          + expenseDepartment + "','" \
                          + preNodeApprovedTime + "','" \
                          + specificationId + "','" \
                          + timeToEnterPendingPayment + "',now(),now()) "
            while count_aux < row_count_aux:
                # 处理单据内费用明细是分摊的情况
                if json.loads(json.dumps(json.loads(json.dumps(json.loads(json.dumps(data[count]['form']))['details']))[count_aux]['feeTypeForm'])) is not None \
                    and json.loads(json.dumps(json.loads(json.dumps(json.loads(json.dumps(data[count]['form']))['details']))[count_aux]['feeTypeForm'])).get('apportions') is not None \
                    and len(json.loads(json.dumps(json.loads(json.dumps(json.loads(json.dumps(data[count]['form']))['details']))[count_aux]['feeTypeForm']))['apportions']) > 0 :

                    # 分摊到部门的数量
                    apportions_count = len(json.loads(json.dumps(json.loads(json.dumps(json.loads(json.dumps(data[count]['form']))['details']))[count_aux]['feeTypeForm']))['apportions'])
                    print("费用明细分摊，总分摊数：[", apportions_count, "]条！")
                    while apportions_count > 0:
                        apportions_count -= 1
                        sql_str_aux_detail = "insert into ods_canal.ods_ppy_ykb_receipt_list_detail_aux (dateid, id, detailId, feeTypeId, expenseDepartment, " \
                                             + " project, feeTypeForm, amount, specificationId,etl_insert_time, etl_update_time) values ('" \
                                             + datetime.fromtimestamp(int(str(data[count]['createTime'])[0:10:])).strftime("%Y-%m-%d") + "','" \
                                             + str(data[count]['id']) + "','" \
                                             + json.loads(json.dumps(data[count]['form']))['details'][count_aux]['feeTypeForm']['apportions'][apportions_count]['apportionForm']['apportionId'] + "','" \
                                             + str(json.loads(json.dumps(json.loads(json.dumps(data[count]['form']))['details']))[count_aux]['feeTypeId']) + "','" \
                                             + json.loads(json.dumps(json.loads(json.dumps(json.loads(json.dumps(data[count]['form']))['details']))[count_aux]['feeTypeForm']))['apportions'][apportions_count]['apportionForm']['expenseDepartment'] + "','" \
                                             + (str(json.loads(json.dumps(json.loads(json.dumps(data[count]['form']))['details']))[count_aux]['feeTypeForm']['项目']) if json.loads(json.dumps(json.loads(json.dumps(data[count]['form']))['details']))[count_aux]['feeTypeForm'].get('项目') is not None else "") + "','" \
                                             + str(json.dumps(json.loads(json.dumps(json.loads(json.dumps(data[count]['form']))['details']))[count_aux]['feeTypeForm'], ensure_ascii=False)) + "','" \
                                             + json.dumps(json.loads(json.dumps(json.loads(json.dumps(json.loads(json.dumps(data[count]['form']))['details']))[count_aux]['feeTypeForm']))['apportions'][apportions_count]['apportionForm']['apportionMoney'], ensure_ascii=False) + "','" \
                                             + str(json.loads(json.dumps(json.loads(json.dumps(data[count]['form']))['details']))[count_aux]['specificationId']) + "',now(),now()) "
                        print(sql_str_aux_detail)
                        cursor.execute(sql_str_aux_detail)
                        db.commit()
                        print("费用明细分摊，当前未完成分摊剩余：[", apportions_count, "]条！")
                else:
                    sql_str_aux_detail = "insert into ods_canal.ods_ppy_ykb_receipt_list_detail_aux (dateid, id, detailId, feeTypeId, expenseDepartment," \
                                         + " project, feeTypeForm, amount, specificationId,etl_insert_time, etl_update_time) values ('" \
                                         + datetime.fromtimestamp(int(str(data[count]['createTime'])[0:10:])).strftime("%Y-%m-%d") + "','" \
                                         + str(data[count]['id']) + "','" \
                                         + json.loads(json.dumps(data[count]['form']))['details'][count_aux]['feeTypeForm']['detailId'] + "','" \
                                         + str(json.loads(json.dumps(json.loads(json.dumps(data[count]['form']))['details']))[count_aux]['feeTypeId']) + "','" \
                                         + (str(json.loads(json.dumps(data[count]['form']))['expenseDepartment']) if json.loads(json.dumps(data[count]['form'])).get('expenseDepartment') is not None else "") + "','" \
                                         + (str(json.loads(json.dumps(json.loads(json.dumps(data[count]['form']))['details']))[count_aux]['feeTypeForm']['项目']) if json.loads(json.dumps(json.loads(json.dumps(data[count]['form']))['details']))[count_aux]['feeTypeForm'].get('项目') is not None else "") + "','" \
                                         + str(json.dumps(json.loads(json.dumps(json.loads(json.dumps(data[count]['form']))['details']))[count_aux]['feeTypeForm'], ensure_ascii=False)) + "','" \
                                         + json.dumps(json.loads(json.dumps(json.loads(json.dumps(json.loads(json.dumps(data[count]['form']))['details']))[count_aux]['feeTypeForm']))['amount'], ensure_ascii=False) + "','" \
                                         + str(json.loads(json.dumps(json.loads(json.dumps(data[count]['form']))['details']))[count_aux]['specificationId']) + "',now(),now()) "
                    print(sql_str_aux_detail)
                    cursor.execute(sql_str_aux_detail)
                    # print(str(json.dumps(json.loads(json.dumps(json.loads(json.dumps(data[count]['form']))['details']))[count_aux]['feeTypeForm'])))
                    db.commit()
                print("内部数据加载完成第：", count_aux + 1, "条！")
                count_aux = count_aux + 1
        else:
            print("目标表插入语句未配置，请先配置！")
            sys.exit(1)
        print(sql_str)
        print(sql_str_aux)
        # print(json.loads(json.dumps(json.loads(json.dumps(data[count]['form']))['details']))[0]['feeTypeId'])
        # 如果是单据表，则同时进行两个目标表插入，其它情况则是单表插入
        if table_name == 'ods_ppy_ykb_receipt_list':
            cursor.execute(sql_str)
            cursor.execute(sql_str_aux)
        else:
            cursor.execute(sql_str)
        # 提交到数据库执行
        db.commit()
        print("数据加载完成第：", count + 1, "条！")
        count = count + 1
    else:
        print("完成循环!")
    # 关闭数据库连接
    db.close()


# 调度任务
def load_data(table_name, start_date, end_date):
    api_str = ''
    param1 = ''
    param2 = ''
    param3 = ''
    param4 = ''
    param5 = ''
    param6 = ''
    pace_count = 0
    if table_name == 'ods_ppy_ykb_expense_type':
        api_str = 'https://dd2.ekuaibao.com/api/openapi/v1/feeTypes?accessToken='
        param1 = ''
        param2 = ''
    elif table_name == 'ods_ppy_ykb_department_type':
        api_str = 'https://dd2.ekuaibao.com/api/openapi/v1/departments?accessToken='
        param1 = '&start=0'
        param2 = '&count=1000'
    elif table_name == 'ods_ppy_ykb_receipt_template_type':
        api_str = 'https://dd2.ekuaibao.com/api/openapi/v1/specifications/latestByType?accessToken='
        param1 = ''
        param2 = ''
    elif table_name == 'ods_ppy_ykb_project_type':
        api_str = 'https://dd2.ekuaibao.com/api/openapi/v1/dimensions/items?accessToken='
        param1 = '&start=0'
        param2 = '&count=1000'
    elif table_name == 'ods_ppy_ykb_receipt_list':
        api_str = 'https://dd2.ekuaibao.com/api/openapi/v1.1/docs/getApplyList?accessToken='
        param1 = '&start=0'
        # 数据加载步幅
        param2 = '&count=100'
        param3 = '&orderBy=updateTime'
        param4 = '&type=expense'
        param5 = '&startDate=' + start_date + ' 00:00:01'
        param6 = '&endDate=' + end_date + ' 23:59:59'
        # 抓取特定单据 测试
        # param5 = '&startDate=' + start_date + ' 10:28:03'
        # param6 = '&endDate=' + end_date + ' 10:28:04'
        # 数据加载步幅
        pace_count = int(param2[7:])

    # 获取token
    accessToken = get_token()
    # 调用get方法获取数据
    # 单据每次只能抽取一种类型的单据，需要循环把所有单据都加载过来
    if table_name == 'ods_ppy_ykb_receipt_list':
        receipt_list = ['expense', 'loan', 'payment', 'requisition']
        # receipt_list = ['expense']
        # 循环调度各种类型单据
        for i in range(len(receipt_list)):
            param4 = '&type=' + receipt_list[i]
            data1, count_total = request_get(api_str, accessToken, param1, param2, param3, param4, param5, param6)
            print("数据总行数：", count_total)
            # 数据插入目标表
            insert_doris(table_name, data1)
            # 接口每次最多只能拉取100条，如果查询条件范围内数据超过100条，则循环调度，直到数据全部加载完成
            while int(count_total) > pace_count:
                param1 = '&start=' + str(int(param1[7:]) + pace_count)
                count_total -= pace_count
                data1, last_count = request_get(api_str, accessToken, param1, param2, param3, param4, param5, param6)
                print("数据剩余行数：", count_total)
                print("数据加载步幅：", pace_count)
                # 数据插入目标表
                insert_doris(table_name, data1)
            param1 = '&start=0'
    elif table_name == 'ods_ppy_ykb_receipt_template_type':
        receipt_list = ['expense', 'loan', 'payment', 'requisition', 'custom']
        # 循环调度各种类型单据
        for i in range(len(receipt_list)):
            param4 = '&type=' + receipt_list[i]
            data1, count_total = request_get(api_str, accessToken, param1, param2, param3, param4, param5, param6)
            print("数据总行数：", count_total)
            # 数据插入目标表
            insert_doris(table_name, data1)
    else:
        data1, last_count = request_get(api_str, accessToken, param1, param2, param3, param4, param5, param6)
        # 数据插入目标表
        insert_doris(table_name, data1)


# ##################################################################
if len(sys.argv[1:]) == 2:
    end_date1 = (datetime.now() + timedelta(days=1)).strftime('%Y-%m-%d')
elif len(sys.argv[1:]) == 3:
    end_date1 = sys.argv[3]
else:
    print("至少需要两个入参：目标表名、开始日期。最多三个入参：目标表名、开始日期、截止日期(不传默认今天24点)！")
    sys.exit(1)
table_name = sys.argv[1]
start_date1 = sys.argv[2]
# 获取特定单据 测试
# table_name = 'ods_ppy_ykb_receipt_list'
# start_date1 = '2023-09-18'
# end_date1 = '2023-09-18'
if table_name not in (
        'ods_ppy_ykb_project_type', 'ods_ppy_ykb_expense_type', 'ods_ppy_ykb_department_type',
        'ods_ppy_ykb_receipt_template_type', 'ods_ppy_ykb_receipt_list'):
    print("目标表不存在，请先在代码中增加此表！")
    sys.exit(1)
load_data(table_name, start_date1, end_date1)
