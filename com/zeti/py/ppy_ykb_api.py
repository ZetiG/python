import json
import sys
from datetime import datetime

import pymysql
import requests
from typing import Optional, NamedTuple, List
from enum import Enum
from dbutils.pooled_db import PooledDB

# 全局变量
# global_access_token = None
# access_token_expire = None

global_access_token = 'ID01xJ0ngwZOCX:ID_3gmIXhB0M38'
access_token_expire = datetime(2024, 3, 12, 18, 29, 23)

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

POOL_DB = PooledDB(
    creator=pymysql,  # 使用链接数据库的模块
    maxconnections=20,  # 连接池允许的最大连接数，0和None表示不限制连接数
    mincached=3,  # 初始化时，链接池中至少创建的空闲的链接，0表示不创建
    maxcached=5,  # 链接池中最多闲置的链接，0和None不限制
    maxshared=1,  # 链接池中最多共享的链接数量，0和None表示全部共享
    blocking=True,  # 连接池中如果没有可用连接后，是否阻塞等待。True，等待；False，不等待然后报错
    maxusage=None,  # 一个链接最多被重复使用的次数，None表示无限制
    setsession=[],  # 开始会话前执行的命令列表。如：["set datestyle to ...", "set time zone ..."]
    ping=0,
    # ping MySQL服务端，检查是否服务可用。如：0：never 1：default 2：when a cursor is created 4：when a query is executed 7：always
    host=db_config.get('host'),
    port=db_config.get('port'),
    user=db_config.get('user'),
    password=db_config.get('password'),
    database=db_config.get('database'),
    charset='utf8'
)


# 主程序入口
def task_run(task_name, start_date=None, end_date=None):
    if task_name is None or task_name == '':
        print('待执行任务表名不能为空！')
        sys.exit(1)

    # 获取routing
    request_routing = TableNameEnum.get_routing_by_name(task_name)
    if request_routing is None or request_routing == '':
        print(f"该表名在枚举类中不存在:[{task_name}]")
        sys.exit(1)

    ykb_request_url = base_ykb_request_url + request_routing
    params = {
        'accessToken': get_token()
    }

    if task_name == TableNameEnum.tbName_ykb_receipt_list.table_name:
        if start_date is None or start_date == '':
            print(f"执行任务：[{task_name}]时，至少指定一个开始时间：[{start_date}]")
            sys.exit(1)

        receipt_type_list = ['expense', 'loan', 'payment', 'requisition']
        for receipt_type in receipt_type_list:
            params['type'] = receipt_type,
            params['orderBy'] = 'updateTime',
            params['startDate'] = start_date + ' 00:00:00',
            params['endDate'] = end_date + ' 23:59:59'
            get_data_write_db(ykb_request_url, params, task_name)

    # 单据模板列表(expense:报销单, loan:借款单, requisition:申请单, payment:付款单, custom:通用审批单)
    elif task_name == TableNameEnum.tbName_ykb_receipt_template_type.table_name:
        template_types = ['expense', 'loan', 'requisition', 'payment', 'custom']
        for template_type in template_types:
            params['type'] = template_type
            get_data_write_db(ykb_request_url, params, task_name)

    else:
        get_data_write_db(ykb_request_url, params, task_name)


# 分页请求，并写入数据库
def get_data_write_db(ykb_request_url, params, table_name):
    print(f"分页查询并入库：[{ykb_request_url}]，params：[{params}]，tableName:[{table_name}]")

    page_start = 0  # 查询起始页
    page_count = 100  # 每次查询总行数
    params['start'] = page_start
    params['count'] = page_count

    total_count = sys.maxsize
    while page_start < total_count:
        print(f"分页查询, page_start：[{page_start}]，page_count：[{page_count}]")
        # 请求接口, 获取数据和总行数
        res = requests.get(ykb_request_url, params=params)
        if res.status_code != 200:
            print('易快报请求异常！')
            sys.exit(1)

        count = 0
        datas = None
        if 'count' in json.loads(res.text):
            count = json.loads(res.text)['count']
        if 'items' in json.loads(res.text):
            datas = json.loads(res.text)['items']

        # 写入数据库
        parse_receipt_list(table_name, datas)

        # 无分页，则代表一次性请求完了，直接跳出循环
        if count <= 0:
            break

        # 否则计算下次请求的分页起始值
        total_count = convert_to_nearest_multiple(count, page_count)
        page_start += page_count
        params.update({'start': page_start})


# 解析单据列表接口结果集
def parse_receipt_list(table_name, datas):
    if table_name is None or table_name == '' or datas is None or len(datas) <= 0:
        print(f'待解析数据不能为空, tableName:[{table_name}]')
        sys.exit(1)

    # 拼接insert-SQL（insert into (xxx) value）
    insert_sql_prefix = get_insert_sql_prefix(table_name)

    sql_params = []
    # 员工列表
    if table_name == TableNameEnum.tbName_ykb_staffs_list.table_name:
        for data in datas:
            staffs = Staffs(**data)
            staff_tuple = (staffs.id, staffs.name, staffs.nickName, staffs.code, ','.join(staffs.departments),
                           staffs.defaultDepartment, staffs.cellphone, staffs.active, staffs.userId, staffs.email,
                           staffs.showEmail, staffs.external, staffs.authState, staffs.globalRoaming, staffs.note,
                           json.dumps(staffs.staffCustomForm) if staffs.staffCustomForm is not None else None,
                           staffs.updateTime, staffs.createTime)
            # 将元组中的所有元素转换为字符串，并将 None 值替换为空字符串
            string_tuple = tuple(str(item) if item is not None else '' for item in staff_tuple)
            sql_params.append(string_tuple)

    # 部门列表
    elif table_name == TableNameEnum.tbName_ykb_department_type.table_name:
        for data in datas:
            department = Departments(**data)
            department_tuple = (department.id, department.name, department.parentId, department.active, department.code,
                                json.dumps(department.form) if department.form is not None else None,
                                department.updateTime, department.createTime)
            # 将元组中的所有元素转换为字符串，并将 None 值替换为空字符串
            string_tuple = tuple(str(item) if item is not None else '' for item in department_tuple)
            sql_params.append(string_tuple)

    # 费用类型列表
    elif table_name == TableNameEnum.tbName_ykb_expense_type.table_name:
        for data in datas:
            fee_types = FeeTypes(**data)
            fee_tuple = (fee_types.id, fee_types.name, fee_types.parentId, fee_types.active, fee_types.code)
            sql_params.append(fee_tuple)

    # 项目类型列表
    elif table_name == TableNameEnum.tbName_ykb_project_type.table_name:
        for data in datas:
            project_types = ProjectTypes(**data)
            project_tuple = (project_types.id, project_types.name, project_types.parentId, project_types.active,
                             project_types.code, project_types.dimensionId, project_types.updateTime,
                             project_types.createTime)
            sql_params.append(project_tuple)

    # 模板列表
    elif table_name == TableNameEnum.tbName_ykb_receipt_template_type.table_name:
        for data in datas:
            template_types = TemplateTypes(**data)
            template_tuple = (template_types.id, template_types.name, template_types.active,
                             json.dumps(template_types.visibility) if template_types.visibility is not None else None)
            sql_params.append(template_tuple)

    elif table_name == TableNameEnum.tbName_ykb_receipt_list.table_name:
        for data in datas:
            receipt = ReceiptObject(**data)
            print(receipt)

    # 落库
    execute_insert_sql(insert_sql_prefix, sql_params)


# 获取易快报Token
def get_token():
    global global_access_token, access_token_expire
    if global_access_token is not None and global_access_token != '' and access_token_expire is not None \
            and access_token_expire != '' \
            and (access_token_expire - datetime.now()).days >= 0 \
            and (access_token_expire - datetime.now()).seconds > 300:
        print(f"当前已有易快报access_token:[{global_access_token}], expire:[{access_token_expire}]")
        return global_access_token

    print('access_token不存在或已失效，准备重新获取token')
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
        access_token_expire = datetime.fromtimestamp(result['value']['expireTime'] / 1000)
        print(f"获取易快报access_token:[{global_access_token}], 过期时间:[{access_token_expire}]")
        return global_access_token
    else:
        print('获取易快报access_token失败, 请重新执行任务！')
        sys.exit(1)


# 获取表的schema，并拼接前半句insert语句
def get_insert_sql_prefix(table_name):
    if table_name is None or table_name == '':
        print('获取表的schema时，传入的表名不能为空')
        return ''
    str_sql = """select concat('insert into ',table_schema,'.',table_name,'(',GROUP_CONCAT(column_name),') values (', GROUP_CONCAT('%s'), ')') as insert_sql_prefix
        from (                  
            select 
                table_schema,
                table_name,
                column_name,
                ordinal_position
            from information_schema.columns
            where table_schema='ods_canal' 
            and table_name='""" + table_name + """' and column_name not in ('etl_insert_time','etl_update_time') 
            order by ordinal_position
        ) T group by table_schema,table_name;"""
    return execute_select_one_sql(str_sql)


# SQL查询方法
def execute_select_one_sql(sql, args=None):
    conn = POOL_DB.connection()
    curr = conn.cursor()
    try:
        curr.execute(sql, args)
        result, = curr.fetchone()
        return result
    except Exception as ex:
        print(f'insert exception: {ex}')
        conn.rollback()
    finally:
        conn.close()
        curr.close()


# SQL插入方法
def execute_insert_sql(sql, args=None):
    conn = POOL_DB.connection()
    curr = conn.cursor()
    try:
        result = curr.executemany(sql, args)
        conn.commit()
        print(f'insert sql execute success!, row:[{result}]')
        return result
    except Exception as ex:
        print(f'insert exception: {ex}')
        conn.rollback()
    finally:
        conn.close()
        curr.close()


# 数据表对应url枚举类
class TableNameEnum(Enum):
    # 易快报-员工列表(维度数据)
    tbName_ykb_staffs_list = ('ods_ppy_ykb_new_staffs_list', '/v1/staffs')
    # 易快报-部门类型(维度数据)
    tbName_ykb_department_type = ('ods_ppy_ykb_new_department_type', '/v1/departments')
    # 易快报-费用类型(维度数据)
    tbName_ykb_expense_type = ('ods_ppy_ykb_new_expense_type', '/v1/feeTypes')
    # 易快报-项目类型(维度数据)
    tbName_ykb_project_type = ('ods_ppy_ykb_new_project_type', '/v1/dimensions/items')
    # 易快报-单据模板类型(维度数据)
    tbName_ykb_receipt_template_type = ('ods_ppy_ykb_new_template_type', '/v1/specifications/latestByType')
    # 易快报-单据清单(事实数据)
    tbName_ykb_receipt_list = ('ods_ppy_ykb_new_receipt_list', '/v1.1/docs/getApplyList')
    # 易快报-单据清单明细(事实数据)
    tbName_ykb_receipt_list_detail = ('ods_ppy_ykb_new_receipt_list_detail', None)
    # 易快报-单据清单明细分摊(事实数据)
    tbName_ykb_receipt_list_detail_aux = ('ods_ppy_ykb_new_receipt_list_detail_aux', None)

    def __init__(self, table_name, routing):
        self.table_name = table_name
        self.routing = routing

    @staticmethod
    def get_routing_by_name(table_name):
        for tb in TableNameEnum:
            if tb.table_name == table_name:
                return tb.routing


# 将t1转化为大于当前值且是最小的f1的倍数的下一个值
def convert_to_nearest_multiple(t1, f1):
    if t1 % f1 == 0:
        return t1  # 如果已经是f1的倍数，则无需转换

    # 计算大于t1且是最小的f1的倍数的下一个值
    return ((t1 // f1) + 1) * f1


# 员工列表
class Staffs:
    id: str
    name: str
    nickName: str
    code: str
    departments: List[str]
    defaultDepartment: str
    cellphone: str
    active: bool
    userId: str
    email: str
    showEmail: str
    external: bool
    authState: bool
    globalRoaming: str
    note: str
    staffCustomForm: dict
    updateTime: str
    createTime: str

    def __init__(self, **kwargs):
        for key in self.__annotations__:
            if key in kwargs:
                setattr(self, key, kwargs[key])


# 部门列表
class Departments:
    id: str
    name: str
    parentId: str
    active: str
    code: str
    form: dict
    updateTime: str
    createTime: str

    def __init__(self, **kwargs):
        for key in self.__annotations__:
            if key in kwargs:
                setattr(self, key, kwargs[key])


# 费用类型
class FeeTypes:
    id: str
    name: str
    parentId: str
    active: str
    code: str

    def __init__(self, **kwargs):
        for key in self.__annotations__:
            if key in kwargs:
                setattr(self, key, kwargs[key])


# 项目类型
class ProjectTypes:
    id: str
    name: str
    parentId: str
    active: str
    code: str
    dimensionId: str
    updateTime: str
    createTime: str

    def __init__(self, **kwargs):
        for key in self.__annotations__:
            if key in kwargs:
                setattr(self, key, kwargs[key])


# 模板列表
class TemplateTypes:
    id: str
    name: str
    active: str
    visibility: dict

    def __init__(self, **kwargs):
        for key in self.__annotations__:
            if key in kwargs:
                setattr(self, key, kwargs[key])


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
                self.__dict__[key] = ReceiptFormObject(**value)
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
    id: str
    formType: str
    active: str
    state: str
    flowType: str
    invoiceRemind: str
    corporationId: str
    sourceCorporationId: str
    dataCorporationId: str
    ownerId: str
    ownerDefaultDepartment: str
    logs: list
    actions: dict
    createTime: int
    updateTime: int
    form: ReceiptFormObject

    def __init__(self, **kwargs):
        for key, value in kwargs.items():
            if isinstance(value, dict):
                self.__dict__[key] = ReceiptObject(**value)
            else:
                self.__dict__[key] = value


if __name__ == '__main__':
    # task_run('ods_ppy_ykb_new_receipt_list')
    task_run('ods_ppy_ykb_new_receipt_list', '2023-11-07', '2023-11-07')
