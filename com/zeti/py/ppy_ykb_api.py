import json
import sys
from datetime import datetime, timedelta

import pymysql
import requests
from typing import List
from enum import Enum
from dbutils.pooled_db import PooledDB

# 全局变量
global_access_token = None
access_token_expire = None

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
        print(f"该表名在枚举类中不存在或无须单独同步:[{task_name}]")
        sys.exit(1)

    ykb_request_url = base_ykb_request_url + request_routing
    params = {
        'accessToken': get_token()
    }

    # 单据列表
    if task_name == TableNameEnum.tbName_ykb_receipt_list.table_name:
        if start_date is None or start_date == '':
            print(f"执行任务：[{task_name}]时，至少指定一个开始时间：[{start_date}]")
            sys.exit(1)

        receipt_type_list = ['expense', 'loan', 'payment', 'requisition']
        for receipt_type in receipt_type_list:
            params['type'] = receipt_type
            params['orderBy'] = 'updateTime'
            params['startDate'] = start_date + ' 00:00:00'
            params['endDate'] = end_date + ' 23:59:59'
            get_data_write_db(ykb_request_url, params, task_name)

    # 模板列表(expense:报销单, loan:借款单, requisition:申请单, payment:付款单, custom:通用审批单)
    elif task_name == TableNameEnum.tbName_ykb_receipt_template_type.table_name:
        template_types = ['expense', 'loan', 'requisition', 'payment', 'custom']
        for template_type in template_types:
            params['type'] = template_type
            get_data_write_db(ykb_request_url, params, task_name)

    # 其他表
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
        print(f"表[{table_name}]分页查询, 总行数:[{None if total_count>10000000 else total_count}], "  # total_count过滤初始值
              f"page_start：[{page_start}]，page_count：[{page_count}]")
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
        total_count = count
        # 否则计算下次请求的分页起始值
        page_start += page_count
        params.update({'start': page_start})


# 解析单据列表接口结果集
def parse_receipt_list(table_name, datas):
    if table_name is None or table_name == '' or datas is None or len(datas) <= 0:
        print(f'待解析数据为空, tableName:[{table_name}]')
        return

    # 拼接insert-SQL（insert into (xxx) value(%s,%s)）
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

    # 单据列表
    elif table_name == TableNameEnum.tbName_ykb_receipt_list.table_name:
        sql_params_det = []
        sql_params_aux = []
        for data in datas:
            receipt = ReceiptObject(**data)

            if receipt is None or receipt.form is None:
                print('单据表转换结果为空，退出执行')
                sys.exit(1)
            elif receipt.form.details is not None and len(receipt.form.details) > 0:
                # 处理单据明细
                for detail in receipt.form.details:
                    # 处理单据分摊明细
                    if detail.feeTypeForm is not None and detail.feeTypeForm.apportions is not None:
                        for apportion in detail.feeTypeForm.apportions:
                            # 单据明细分摊结果集
                            receipt_aux_tuple = (datetime.fromtimestamp(receipt.createTime/1000).strftime('%Y-%m-%d'),
                                                 receipt.id, detail.feeTypeForm.detailId,
                                                 apportion.apportionForm.apportionId, apportion.apportionForm.project,
                                                 apportion.specificationId, apportion.apportionForm.apportionPercent,
                                                 apportion.apportionForm.expenseDepartment,
                                                 apportion.apportionForm.apportionMoney.get('standard'),
                                                 json.dumps(apportion.apportionForm.apportionMoney))
                            sql_params_aux.append(receipt_aux_tuple)

                    # 单据明细结果集
                    receipt_det_tuple = (datetime.fromtimestamp(receipt.createTime/1000).strftime('%Y-%m-%d'),
                                         receipt.id, detail.feeTypeForm.detailId, detail.feeTypeId,
                                         detail.specificationId, detail.feeTypeForm.project,
                                         detail.feeTypeForm.amount.get('standard'), json.dumps(detail.feeTypeForm.amount),
                                         detail.feeTypeForm.feeDate, json.dumps(detail.feeTypeForm.taxAmount),
                                         json.dumps([apportion.to_json() for apportion in detail.feeTypeForm.apportions]
                                                    ) if len(detail.feeTypeForm.apportions) > 0 else '{}',
                                         json.dumps(detail.feeTypeForm.attachments),
                                         json.dumps(detail.feeTypeForm.invoiceForm),
                                         json.dumps(detail.feeTypeForm.consumptionReasons),
                                         json.dumps(detail.feeType))
                    sql_params_det.append(receipt_det_tuple)

            # 单据
            receipt_tuple = (datetime.fromtimestamp(receipt.createTime/1000).strftime('%Y-%m-%d'),
                             receipt.id, receipt.form.code, receipt.form.title, receipt.active, receipt.form.project,
                             receipt.form.specificationId, receipt.formType, receipt.state, receipt.flowType,
                             receipt.corporationId, receipt.ownerId, receipt.ownerDefaultDepartment,
                             receipt.form.payDate, receipt.form.payeeId, receipt.form.payMoney.get('standard'),
                             json.dumps(receipt.form.payMoney), receipt.form.voucherNo, receipt.form.submitDate,
                             receipt.form.expenseDate, receipt.form.flowEndTime, receipt.form.description,
                             receipt.form.expenseMoney.get('standard'), json.dumps(receipt.form.expenseMoney),
                             receipt.form.submitterId, receipt.form.legalEntity, receipt.form.voucherStatus,
                             json.dumps(receipt.form.companyRealPay), receipt.form.paymentChannel,
                             json.dumps(receipt.form.writtenOffMoney), receipt.form.paymentAccountId,
                             receipt.form.expenseDepartment, receipt.form.preNodeApprovedTime,
                             receipt.form.timeToEnterPendingPayment,
                             json.dumps(receipt.form.writtenOffRecords),
                             json.dumps(receipt.form.payPlan),
                             json.dumps(receipt.form.attachments),
                             json.dumps(receipt.logs),
                             json.dumps(receipt.actions),
                             datetime.fromtimestamp(receipt.createTime/1000).strftime("%Y-%m-%d %H:%M:%S"),
                             datetime.fromtimestamp(receipt.updateTime / 1000).strftime("%Y-%m-%d %H:%M:%S"))

            sql_params.append(receipt_tuple)

        # 优先落库单据明细和分摊明细
        # 单据明细
        detail_prefix = get_insert_sql_prefix(TableNameEnum.tbName_ykb_receipt_list_detail.table_name)
        sql_params_batch_deal('table_detail', detail_prefix, sql_params_det)

        # 分摊明细
        aux_prefix = get_insert_sql_prefix(TableNameEnum.tbName_ykb_receipt_list_detail_aux.table_name)
        sql_params_batch_deal('table_aux', aux_prefix, sql_params_aux)

    # 落库
    row = execute_insert_sql(insert_sql_prefix, sql_params)
    print(f"表:[{table_name}]处理，待处理行数[{len(sql_params)}], 入库完成[{row}]条")


# 分批处理单据明细、分摊明细
def sql_params_batch_deal(table_name, insert_sql_prefix, sql_params):
    if len(sql_params) <= 0:
        return
    batch_size = 200  # 每批的大小
    count = len(sql_params)
    for start in range(0, count, batch_size):
        end = min(start + batch_size, count)
        batch_params = sql_params[start:end]
        row = execute_insert_sql(insert_sql_prefix, batch_params)
        print(f"表:[{table_name}]分批处理，总数[{count}], 每批[{batch_size}]条, 共[{count//batch_size+1}]批, "
              f"当前第 {start//batch_size+1} 批, 该批入库完成[{row}]条")


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
def execute_insert_sql(sql, params):
    if params is None or len(params) <= 0:
        print('insert sql execute fail! params is None!')
        return

    conn = POOL_DB.connection()
    curr = conn.cursor()
    try:
        result = curr.executemany(sql, params)
        conn.commit()
        print(f'insert sql execute success!')
        return result
    except Exception as ex:
        print(f'insert error: {ex}')
        conn.rollback()
        print('ERROR SQL:', format_insert_sql(sql, params))
        sys.exit(1)
    finally:
        conn.close()
        curr.close()


def format_insert_sql(sql, params):
    # 构建 SQL 语句的值部分
    values = ', '.join([str(row) for row in params])

    # 拼接完整的 SQL 语句
    full_sql = f"{sql[:-1]} values {values};"

    return full_sql


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
    tbName_ykb_receipt_list_detail = ('ods_ppy_ykb_new_receipt_list_details', None)
    # 易快报-单据清单明细分摊(事实数据)
    tbName_ykb_receipt_list_detail_aux = ('ods_ppy_ykb_new_receipt_list_details_aux', None)

    def __init__(self, table_name, routing):
        self.table_name = table_name
        self.routing = routing

    @staticmethod
    def get_routing_by_name(table_name):
        for tb in TableNameEnum:
            if tb.table_name == table_name:
                return tb.routing


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


# 单据表单form明细-分摊明细表单
class ReceiptFormDetailApportionsFormObject:
    project: str
    apportionId: str
    apportionMoney: dict
    apportionPercent: str
    expenseDepartment: str

    def __init__(self, **kwargs):
        self.project = kwargs.get('项目', '')
        self.apportionId = kwargs.get('apportionId', '')
        self.apportionMoney = kwargs.get('apportionMoney', {})
        self.apportionPercent = kwargs.get('apportionPercent', '')
        self.expenseDepartment = kwargs.get('expenseDepartment', '')

    # 自定义 toJSON() 方法，用于将类转换为 JSON 格式的对象
    def to_json(self):
        return {
            'project': self.project,
            'apportionId': self.apportionId,
            'apportionMoney': self.apportionMoney,
            'apportionPercent': self.apportionPercent,
            'expenseDepartment': self.expenseDepartment
        }


# 单据表单form明细-分摊明细
class ReceiptFormDetailApportionsObject:
    apportionForm: ReceiptFormDetailApportionsFormObject
    specificationId: str

    def __init__(self, **kwargs):
        self.apportionForm = ReceiptFormDetailApportionsFormObject(**kwargs.get('apportionForm', {}))
        self.specificationId = kwargs.get('specificationId', '')

    # 自定义 toJSON() 方法，用于将类转换为 JSON 格式的对象
    def to_json(self):
        return {
            'specificationId': self.specificationId,
            'apportionForm': self.apportionForm.to_json()
        }


# 单据表单form明细-费用类型
class ReceiptFormDetailsFeeTypeObject:
    amount: dict
    project: str
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

    def __init__(self, apportions: List[dict] = None, **kwargs):
        self.amount = kwargs.get('amount', {})
        self.project = kwargs.get('项目', '')
        self.feeDate = kwargs.get('feeDate', 0)
        self.invoice = kwargs.get('invoice', '')
        self.taxRate = kwargs.get('taxRate', '')
        self.detailId = kwargs.get('detailId', '')
        self.detailNo = kwargs.get('detailNo', '')
        self.taxAmount = kwargs.get('taxAmount', {})
        self.apportions = [ReceiptFormDetailApportionsObject(**apt) for apt in apportions] if apportions else []
        self.attachments = kwargs.get('attachments', [])
        self.invoiceForm = kwargs.get('invoiceForm', {})
        self.consumptionReasons = kwargs.get('consumptionReasons', '')


# 单据表单form明细类
class ReceiptFormDetailsObject:
    feeTypeId: str
    feeTypeForm: ReceiptFormDetailsFeeTypeObject
    specificationId: str
    feeType: dict

    def __init__(self, **kwargs):
        self.feeTypeId = kwargs.get('feeTypeId', '')
        self.feeTypeForm = ReceiptFormDetailsFeeTypeObject(**kwargs.get('feeTypeForm', {}))
        self.specificationId = kwargs.get('specificationId', '')
        self.feeType = kwargs.get('feeType', {})


# 单据表单form类
class ReceiptFormObject:
    code: str
    title: str
    project: str
    details: List[ReceiptFormDetailsObject]
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
    lastPrinter: str
    submitterId: str
    expenseLinks: list
    expenseMoney: dict
    receiptState: str
    rejectionNum: str
    legalEntity: str
    lastPrintTime: int
    voucherStatus: str
    companyRealPay: dict
    onlyOwnerPrint: str
    paymentChannel: str
    specificationId: str
    writtenOffMoney: dict
    paymentAccountId: str
    expenseDepartment: str
    voucherCreateTime: int
    preApprovedNodeName: str
    preNodeApprovedTime: int
    timeToEnterPendingPayment: int
    ownerAndApproverPrintNodeFlag: str
    writtenOffRecords: list

    def __init__(self, details: List[dict] = None, **kwargs):
        self.code = kwargs.get('code', '')
        self.title = kwargs.get('title', '')
        self.project = kwargs.get('项目', '')
        self.details = [ReceiptFormDetailsObject(**detail) for detail in details] if details else []
        self.payDate = kwargs.get('payDate', 0)
        self.payPlan = kwargs.get('payPlan', [])
        self.payeeId = kwargs.get('payeeId', '')
        self.payMoney = kwargs.get('payMoney', {})
        self.voucherNo = kwargs.get('voucherNo', '')
        self.printCount = kwargs.get('printCount', '')
        self.printState = kwargs.get('printState', '')
        self.submitDate = kwargs.get('submitDate', 0)
        self.attachments = kwargs.get('attachments', [])
        self.description = kwargs.get('description', '')
        self.expenseDate = kwargs.get('expenseDate', 0)
        self.flowEndTime = kwargs.get('flowEndTime', 0)
        self.lastPrinter = kwargs.get('lastPrinter', '')
        self.submitterId = kwargs.get('submitterId', '')
        self.expenseLinks = kwargs.get('expenseLinks', [])
        self.expenseMoney = kwargs.get('expenseMoney', {})
        self.receiptState = kwargs.get('receiptState', '')
        self.rejectionNum = kwargs.get('rejectionNum', '')
        self.legalEntity = kwargs.get('法人实体', '')
        self.lastPrintTime = kwargs.get('lastPrintTime', 0)
        self.voucherStatus = kwargs.get('voucherStatus', '')
        self.companyRealPay = kwargs.get('companyRealPay', {})
        self.onlyOwnerPrint = kwargs.get('onlyOwnerPrint', '')
        self.paymentChannel = kwargs.get('paymentChannel', '')
        self.specificationId = kwargs.get('specificationId', '')
        self.writtenOffMoney = kwargs.get('writtenOffMoney', {})
        self.paymentAccountId = kwargs.get('paymentAccountId', '')
        self.expenseDepartment = kwargs.get('expenseDepartment', '')
        self.voucherCreateTime = kwargs.get('voucherCreateTime', 0)
        self.preApprovedNodeName = kwargs.get('preApprovedNodeName', '')
        self.preNodeApprovedTime = kwargs.get('preNodeApprovedTime', 0)
        self.timeToEnterPendingPayment = kwargs.get('timeToEnterPendingPayment', 0)
        self.ownerAndApproverPrintNodeFlag = kwargs.get('ownerAndApproverPrintNodeFlag', '')
        self.writtenOffRecords = kwargs.get('writtenOffRecords', [])


# 单据类
class ReceiptObject:
    id: str
    formType: str
    active: str
    state: str
    flowType: str
    corporationId: str
    ownerId: str
    ownerDefaultDepartment: str
    logs: list
    actions: dict
    createTime: int
    updateTime: int
    form: ReceiptFormObject

    def __init__(self, **kwargs):
        self.id = kwargs.get('id', '')
        self.formType = kwargs.get('formType', '')
        self.active = kwargs.get('active', '')
        self.state = kwargs.get('state', '')
        self.flowType = kwargs.get('flowType', '')
        self.corporationId = kwargs.get('corporationId', '')
        self.ownerId = kwargs.get('ownerId', '')
        self.ownerDefaultDepartment = kwargs.get('ownerDefaultDepartment', '')
        self.logs = kwargs.get('logs', [])
        self.actions = kwargs.get('actions', {})
        self.createTime = kwargs.get('createTime', 0)
        self.updateTime = kwargs.get('updateTime', 0)
        self.form = ReceiptFormObject(**kwargs.get('form', {}))


# 获取脚本执行参数
tableName = None
startDate = None
endDate = None

if len(sys.argv[1:]) == 0:
    print("缺少必要的请求参数：目标表名(必填)、开始日期(可选)、截止日期(可选，不传默认当天24点)")
    sys.exit(1)
elif len(sys.argv[1:]) == 1 and TableNameEnum.tbName_ykb_receipt_list.table_name == sys.argv[1]:
    print("该表至少需要两个入参：目标表名、开始日期、截止日期(可选，不传默认当天24点)")
    sys.exit(1)
elif len(sys.argv[1:]) == 2:
    startDate = sys.argv[2]
    endDate = (datetime.now() + timedelta(days=1)).strftime('%Y-%m-%d')
elif len(sys.argv[1:]) == 3:
    startDate = sys.argv[2]
    endDate = sys.argv[3]

tableName = sys.argv[1]

print(f'易快报脚本执行入参: tableName:[{tableName}], startDate:[{startDate}], endDate:[{endDate}]')
# 执行程序
task_run(tableName, startDate, endDate)

print(f'易快报脚本执行完成！: tableName:[{tableName}], startDate:[{startDate}], endDate:[{endDate}]')


# if __name__ == '__main__':
    # task_run('ods_ppy_ykb_new_receipt_list')
    # task_run('ods_ppy_ykb_new_receipt_list', '2024-03-15', '2024-03-15')
