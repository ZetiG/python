import configparser
import json
import logging
import os
import urllib.parse
import requests
from fake_useragent import UserAgent
from get_proxy_ip import proxies_switch, conn, cursor
from bs4 import BeautifulSoup

# 安居客，根据小区名称查询小区id
# https://hangzhou.anjuke.com/esf-ajax/community/pc/autocomplete?city_id=18&kw=%E9%98%B3%E5%85%89%E5%9F%8E%E7%BF%A1%E4%B8%BD%E5%85%AC%E5%9B%AD&type=2
# https://hangzhou.anjuke.com/community/?kw=%E9%A1%BA%E5%8F%91%C2%B7%E6%81%92%E5%9B%AD
# 安居客，根据小区id查询小区具体信息
# https://hangzhou.anjuke.com/community/view/1032464

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

# 随机UA
path = os.path.join('', 'fake_ua.json')
# path 为你放置 fake_ua.json文件路径
fake_ua = UserAgent(path=path).random
logger.info('当前UserAgent:[%s]', fake_ua)

headers = {
    "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
    "accept-encoding": "gzip",
    "accept-language": "zh-CN,zh;q=0.9",
    "cache-control": "max-age=0",
    "cookie": "aQQ_ajkguid=7E7059E3-A8A7-4994-14CA-9E6BE6E5D5E9; ctid=18; wmda_uuid=e5da125b3345ee6d4e5a09ea78d31c09; wmda_new_uuid=1; wmda_visited_projects=%3B6289197098934; id58=CpQMQ2JtC5Y/DbwstoReAg==; 58tj_uuid=f1039db5-80cd-4036-b650-a394364e532a; _ga=GA1.2.1398758582.1651313558; als=0; isp=true; cmctid=79; sessid=A3A85D36-6B18-7E3F-0224-FD2FBB0D8FFF; twe=2; _gid=GA1.2.1550625655.1652954225; ajk_member_captcha=725f14718939fb1b3c2ab994ba2e11e4; fzq_js_anjuke_jingjiren_pc=9a6e870b19407a415a0e19f3a212d532_1653040607426_25; fzq_h=052e2bcc62a164a5039f0e7a919e4002_1653040900982_1c17b54a64ae47e29915e24e7d17cec9_2061522738; fzq_js_anjuke_ershoufang_pc=76bb80b0c1ab2fb1b285c439e70b8ec5_1653041805376_24; ajk-appVersion=; obtain_by=2; fzq_js_anjuke_xiaoqu_pc=e651dfaea3e7a046018c27eaea2ec639_1653042593908_23; new_session=1; init_refer=https%253A%252F%252Fhangzhou.anjuke.com%252F; new_uv=8; _gat=1; xxzl_cid=3288c8238883454abbd0c2c82815bc7d; xzuid=06ca0d5e-0292-447e-b2be-4425ebd8ae33",
    "referer": "https://www.anjuke.com/",
    "sec-fetch-mode": "navigate",
    "sec-fetch-site": "none",
    "sec-fetch-user": "?1",
    "upgrade-insecure-requests": "1",
    "user-agent": fake_ua,
}

# 代理ip, 这里随机设置一个，下面调用时会自动根据代理ip库查找一个可用ip
proxies = {'http': '183.247.199.126:30001'}

# 小区信息所需字段字典
community_info_dict = {
    '物业类型': 'properties_types',
    '竣工时间': 'completion_time',
    '总户数': 'total_houses',
    '物业费': 'property_costs',
    '物业公司': 'property_company'
}


# 读取并返回小店周边业态输出文件
def read_shop_community_file():
    with open("../gaode_map/shop_community_result.json") as load_f:
        load_dict = json.load(load_f)
        if load_dict is None or len(load_dict) <= 0:
            logging.warning("读取小店周边业态输出文件为空，请检查文件内容~")
            return

        for community in load_dict:
            cm_name = community['name']
            if cm_name is None or len(cm_name) <= 0:
                continue

            # 调用安居客api，查询小区对应安居客内的编码、平方单价
            community_id, community_price = get_ajk_community_code(cm_name)

            # 爬取小区的基本信息
            community_info = get_attr_detail_by_id(community_id, community_price)

            # 构建入库对象
            build_shop_community_obj(community, community_info)


# 调用安居客api，查询并返回小区对应安居客内的编码
def get_ajk_community_code(community_name):
    logging.info('调用安居客api，查询并返回小区对应安居客内的编码，小区名称:[%s]', community_name)

    global proxies

    ajk_url = 'https://hangzhou.anjuke.com/esf-ajax/community/pc/autocomplete'
    param = {
        'city_id': 18,
        'kw': community_name,
        'type': 2
    }

    response = requests.get(ajk_url, urllib.parse.urlencode(param), headers=headers, proxies=proxies)

    while True:
        if response.ok is False or response.url.split('?')[0] != ajk_url:
            proxies = proxies_switch(ajk_url, headers)
            # 得到可用IP,再次重试
            response = requests.get(ajk_url, headers=headers, proxies=proxies)
        else:
            break

    # 解析返回的数据
    if response.status_code == 200 and json.loads(response.text)['status'] == 'ok':
        data = json.loads(response.text)['data']
        if data is None or len(data) <= 0:
            logging.info('调用安居客api，查询并返回小区对应安居客内的编码为空，根据名称进行模糊匹配, 小区:[%s]', community_name)

            url = 'https://hangzhou.anjuke.com/community/'
            param = {
                'kw': community_name
            }
            resp = requests.get(url, urllib.parse.urlencode(param), headers=headers, proxies=proxies)

            while True:
                if resp.ok is False or resp.url.split('?')[0] != url:
                    proxies = proxies_switch(url, headers)
                    resp = requests.get(url, urllib.parse.urlencode(param), headers=headers, proxies=proxies)
                else:
                    break

            community_code = None
            soup = BeautifulSoup(resp.text, 'lxml')
            find_all = soup.find_all('a', attrs={'class': 'li-row'})
            if find_all is not None and len(find_all) > 0 and find_all[0]['href'] is not None:
                community_code = find_all[0]['href'].split('/')[-1]
                logging.info('模糊匹配到该小区，小区名称:[%s], 小区编码:[%s]', community_name, community_code)
            else:
                logging.info('暂未搜索到该小区信息，小区:[%s]', community_name)
            return community_code, 0

        if data[0] is not None and len(data[0]) > 0 and data[0]['id'] is not None:
            logging.info('调用安居客api，查询并返回小区对应安居客内的编码，小区名称:[%s] 小区编码:[%s]', community_name, data[0]['id'])
            return data[0]['id'], data[0]['price']  # id:小区安居客id，price:小区房价


# 调用安居客api, 通过小区id查询详情 https://hangzhou.anjuke.com/community/view/1032464
def get_attr_detail_by_id(community_id, community_price):
    logging.info('调用安居客api, 通过小区id查询详情start... 小区id:[%s]', community_id)
    if community_id is None:
        return CommunityInfo

    global proxies

    url = 'https://hangzhou.anjuke.com/community/view/' + str(community_id)
    response = requests.get(url, headers=headers, proxies=proxies)

    while True:
        if response.ok is False or response.url.split('?')[0] != url:
            proxies = proxies_switch(url, headers)
            # 得到可用IP,再次重试
            response = requests.get(url, headers=headers, proxies=proxies)
        else:
            break

    # 正常返回，得到详情页，解析
    soup = BeautifulSoup(response.text, 'lxml')

    # 解析小区均价
    divs_price = soup.find_all('div', attrs={'class': 'house-price_compare'})
    if int(community_price) <= 0 < len(divs_price) and divs_price is not None:
        class_ = divs_price[0].next_element.attrs['class'][0]
        if class_ is not None and class_ == 'average':
            community_price = divs_price[0].next_element.next_element.text

    # 解析小区其他信息
    soup_find_all = soup.find_all('div', attrs={'class': 'label'})
    if soup_find_all is None or len(soup_find_all) <= 0:
        return

    community_info = CommunityInfo
    community_info.community_id = community_id
    community_info.community_price = community_price
    # 遍历所有属性，取所需要的字段
    for soup in soup_find_all:
        content_key = soup.text

        # 判断属性是否是所需信息
        if content_key is not None and content_key in community_info_dict.keys():
            content_val = soup.next_element.next_element.next_element.next_element.next_element.text
            content_val = content_val.replace(' ', '').replace('\n', '').replace('\r', '')

            # 查询类内是否包含该属性，并赋值
            community_info.set_attr_val(community_info, community_info_dict.get(content_key), content_val)

    return community_info


# 组装入库数据
def build_shop_community_obj(community_dict, community_info):
    logging.info('开始组装小区信息入库对象，json:[%s], community_info:[%s]',
                 community_dict, community_info)

    try:
        conn.begin()
        insert_sql = "INSERT INTO community_info(`shop_name`, `shop_location`, `community_ajk_id`, `community_name`, " \
                     "`community_price`, `properties_types`, `completion_time`, `total_houses`, `property_costs`, " \
                     "`property_company`, `province_name`,`city_name`, `area_name`, `addr_detail`) " \
                     "values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"

        cursor.execute(insert_sql, (community_dict['shop_name'], community_dict['shop_location'],
                                    community_info.community_id, community_dict['name'], community_info.community_price,
                                    community_info.properties_types, community_info.completion_time,
                                    community_info.total_houses, community_info.property_costs,
                                    community_info.property_company, community_dict['pname'],
                                    community_dict['cityname'], community_dict['adname'], community_dict['address']))
        conn.commit()

    except Exception:
        logging.error('写入小区信息数据库异常:', Exception)
        conn.rollback()
        conn.close()


# 小区信息
class CommunityInfo:
    # 小区对象安居客内id
    community_id = None
    # 小区均价
    community_price = None
    # 物业类型: 公寓住宅
    properties_types = None
    # 竣工时间: 2018年
    completion_time = None
    # 总户数: 1016户
    total_houses = None
    # 物业费:  1.50元/平米/月
    property_costs = None
    # 物业公司
    property_company = None

    def __init__(self):
        pass

    # 判断对象是否包含该属性，并赋值
    def set_attr_val(self, attr, val):
        if hasattr(self, attr):
            setattr(self, attr, val)


if __name__ == '__main__':
    read_shop_community_file()

    # get_attr_detail_by_id('1067956')

    # with open("../gaode_map/shop_community_result.json") as load_f:
    #     load_dict = json.load(load_f)
    #     for community in load_dict:
    #         build_shop_community_obj(community, 0, CommunityInfo())


