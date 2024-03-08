import json
import sys
from datetime import datetime

import pymysql
import requests
from typing import Optional, NamedTuple, List
from enum import Enum

# 全局变量
# global_access_token = None
# access_token_expire = None
global_access_token = 'ID01uUktxWxmpx:ID_3gmIXhB0M38'
access_token_expire = datetime(2023, 11, 28, 12, 40, 9)

# 易快报请求参数
base_ykb_request_url = 'https://dd2.ekuaibao.com/api/openapi'
base_ykb_appKey = {
    "appKey": "742edd7e-2c83-4983-9b59-500a7d94e9ae",
    "appSecurity": "86980dbf-e479-494a-9c54-ebd8513905b6"
}

# 设置数据库连接参数
db_config = {
    'host': '60.190.243.69',
    'user': 'bigdata',
    'password': 'VB8kGYjN',
    'database': 'ods_canal',
    'port': 9030,
}


# 主程序入口
def task_run(task_name, start_date, end_date):
    if task_name is None or task_name == '':
        print('任务表名不能为空！')
        sys.exit(1)

    # 构建请求url
    request_url = build_request(task_name, start_date, end_date)

    print(global_access_token)


# 构建对应请求
def build_request(task_name, start_date, end_date):
    print('开始构建请求：', task_name, start_date, end_date)
    if task_name is None or task_name == '':
        print('任务表名不能为空！')
        sys.exit(1)

    request_url = ''
    if task_name == TableNameEnum.tbName_ykb_receipt_list.table_name and start_date is None:
        print(f"执行任务：[{task_name}]时，至少指定一个开始时间：[{start_date}]")
        sys.exit(1)

    elif task_name == TableNameEnum.tbName_ykb_department_type.table_name:
        request_url = TableNameEnum.tbName_ykb_department_type.url

    elif task_name == TableNameEnum.tbName_ykb_expense_type.table_name:
        print()

    elif task_name == TableNameEnum.tbName_ykb_project_type.table_name:
        print()

    elif task_name == TableNameEnum.tbName_ykb_receipt_template_type.table_name:
        print()

    elif task_name == TableNameEnum.tbName_ykb_receipt_list.table_name:
        request_routing = TableNameEnum.tbName_ykb_receipt_list.routing
        page_start = 0  # 查询起始页
        page_count = 40  # 每次查询总行数

        receipt_type_list = ['expense', 'loan', 'payment', 'requisition']
        for i in range(len(receipt_type_list)):
            params = {
                'type': receipt_type_list[i],
                'start': page_start,
                'count': page_count,
                'orderBy': 'updateTime',
                'startDate': start_date + ' 00:00:01',
                'endDate': end_date + ' 23:59:59'
            }

            total_count = sys.maxsize
            while page_start < total_count:
                # 请求接口
                datas, count = load_data(request_routing, params)

                # 写入数据库
                parse_receipt_list(datas)

                # 计算下次请求的分页起始值
                total_count = convert_to_nearest_multiple(count, page_count)
                page_start += page_count
                params.update({'start': page_start})
                print(count)

    else:
        print(f"表名不存在:[{task_name}]")
        sys.exit(1)
    return request_url


# 解析单据列表接口结果集
def parse_receipt_list(datas):
    if datas is None or len(datas) <= 0:
        print('单据列表结果为空！')
        sys.exit(1)

    list_count = 0
    while list_count < len(datas):
        receipt_object = ReceiptObject(**datas[list_count])
        print(f"解析结果，id:[{receipt_object.id}], num:[{list_count}]")

        list_count += 1


# 获取易快报Token
def get_token():
    global global_access_token, access_token_expire
    routing = '/v1/auth/getAccessToken'
    headers = {
        'Content-Type': 'application/json',
        'Accept': 'application/json'
    }
    # 获取Token
    res = requests.post(base_ykb_request_url + routing, headers=headers, json=base_ykb_appKey)
    if res.status_code == 200:
        result = res.json()
        global_access_token = result['value']['accessToken']
        access_token_expire = datetime.fromtimestamp(result['value']['expireTime']/1000)
        print(f"获取易快报access_token:[{global_access_token}], 过期时间:[{access_token_expire}]")
    else:
        print('获取易快报access_token失败！')
        sys.exit(1)


# 调用API接口获取数据, routing:路由地址
def load_data(routing, params):
    global global_access_token, access_token_expire
    if global_access_token is None or global_access_token == '' or access_token_expire is None \
            or access_token_expire == '' \
            or (access_token_expire - datetime.now()).days < 0 \
            or (access_token_expire - datetime.now()).seconds < 300:
        print('access_token不存在或已失效，准备重新获取token')
        get_token()
    else:
        print(f"当前已有易快报access_token:[{global_access_token}], expire:[{access_token_expire}]")

    # 参数里增加token
    params['accessToken'] = global_access_token
    res = requests.get(base_ykb_request_url + routing, params=params)

    if res.status_code == 200:
        count = json.loads(res.text)['count']
        datas = json.loads(res.text)['items']
        print(f"接口查询范围内数据总行数:[{count}]")
        return datas, count
    else:
        print('易快报请求异常！')


# 连接数据库
try:
    connection = pymysql.connect(**db_config)
    with connection.cursor() as cursor:
        print('Connected to the database')

        # 在这里执行数据库操作，例如查询、插入等

except pymysql.Error as e:
    print(f'Error connecting to the database: {e}')

finally:
    # 关闭数据库连接
    if 'connection' in locals() and connection.open:
        connection.close()
        print('Disconnected from the database')


# 数据表对应url枚举类
class TableNameEnum(Enum):
    # 易快报-部门类型(维度数据)
    tbName_ykb_department_type = ('ods_ppy_ykb_department_type', '/v1/departments')
    # 易快报-费用类型(维度数据)
    tbName_ykb_expense_type = ('ods_ppy_ykb_expense_type', '/v1/feeTypes')
    # 易快报-项目类型(维度数据)
    tbName_ykb_project_type = ('ods_ppy_ykb_project_type', '/v1/dimensions/items')
    # 易快报-单据模板类型(维度数据)
    tbName_ykb_receipt_template_type = ('ods_ppy_ykb_receipt_template_type', '/v1/specifications/latestByType')
    # 易快报-单据清单(事实数据)
    tbName_ykb_receipt_list = ('ods_ppy_ykb_receipt_list', '/v1.1/docs/getApplyList')
    # 易快报-单据清单明细(事实数据)
    tbName_ykb_receipt_list_detail = ('ods_ppy_ykb_receipt_list_detail', None)
    # 易快报-单据清单明细分摊(事实数据)
    tbName_ykb_receipt_list_detail_aux = ('ods_ppy_ykb_receipt_list_detail_aux', None)

    def __init__(self, table_name, routing):
        self.table_name = table_name
        self.routing = routing


# 将t1转化为大于当前值且是最小的f1的倍数的下一个值
def convert_to_nearest_multiple(t1, f1):
    if t1 % f1 == 0:
        return t1  # 如果已经是f1的倍数，则无需转换

    # 计算大于t1且是最小的f1的倍数的下一个值
    return ((t1 // f1) + 1) * f1


class ReceiptFormDetailApportionsObject:
    apportionForm: dict
    specificationId: str

    def __init__(self, **kwargs):
        for key, value in kwargs.items():
            if isinstance(value, dict):
                self.__dict__[key] = ReceiptObject(**value)
            else:
                self.__dict__[key] = value


# 单据表单form明细-费用类型
class ReceiptFormDetailFeeTypeObject:
    amount: dict
    项目: str
    feeDate: int
    invoice: str
    taxRate: str
    detailId: str
    detailNo: int
    taxAmount: dict
    apportions: List[ReceiptFormDetailApportionsObject]
    attachments: list
    invoiceForm: dict
    consumptionReasons: str

    def __init__(self, **kwargs):
        for key, value in kwargs.items():
            if isinstance(value, dict):
                self.__dict__[key] = ReceiptObject(**value)
            else:
                self.__dict__[key] = value


# 单据表单form明细类
class ReceiptFormDetailObject:
    feeTypeId: str
    feeTypeForm: ReceiptFormDetailFeeTypeObject
    specificationId: str
    feeType: dict

    def __init__(self, **kwargs):
        for key, value in kwargs.items():
            if isinstance(value, dict):
                self.__dict__[key] = ReceiptObject(**value)
            else:
                self.__dict__[key] = value


# 单据表单form类
class ReceiptFormObject:
    code: str
    title: str
    项目: str
    details: List[ReceiptFormDetailObject]
    payDate: int
    payPlan: list
    payeeId: str
    payMoney: dict
    voucherNo: str
    printCount: str
    printState: str
    submitDate: int
    attachments: list
    description: str
    expenseDate: int
    flowEndTime: int
    submitterId: str
    expenseLinks: list
    expenseMoney: dict
    receiptState: str
    rejectionNum: str
    法人实体: str
    voucherStatus: str
    companyRealPay: dict
    onlyOwnerPrint: bool
    paymentChannel: str
    specificationId: str
    writtenOffMoney: dict
    paymentAccountId: str
    expenseDepartment: str
    voucherCreateTime: int
    preApprovedNodeName: str
    preNodeApprovedTime: int
    timeToEnterPendingPayment: int
    ownerAndApproverPrintNodeFlag: bool
    writtenOffRecords: list

    def __init__(self, **kwargs):
        for key, value in kwargs.items():
            if isinstance(value, dict):
                self.__dict__[key] = ReceiptObject(**value)
            else:
                self.__dict__[key] = value


# 单据类
class ReceiptObject:
    pipeline: int
    grayver: str
    dbVersion: int
    threadId: str
    version: int
    active: bool
    createTime: int
    updateTime: int
    corporationId: str
    sourceCorporationId: str
    dataCorporationId: str
    ownerId: str
    ownerDefaultDepartment: str
    state: str
    flowType: str
    formType: str
    logs: list
    actions: dict
    invoiceRemind: bool
    appId: str
    id: str
    form: ReceiptFormObject

    def __init__(self, **kwargs):
        for key, value in kwargs.items():
            if isinstance(value, dict):
                self.__dict__[key] = ReceiptObject(**value)
            else:
                self.__dict__[key] = value



if __name__ == '__main__':
    task_run('ods_ppy_ykb_receipt_list', '2023-11-07', '2023-11-07')

