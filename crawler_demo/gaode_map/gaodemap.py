import logging

import requests
import json
from configparser import ConfigParser

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# 1. 根据小店经纬度坐标，获取周边1公里范围内住宅信息
# 2. 拿到上步获取的住宅信息，利用爬虫从二手房交易网站获取住宅信息
class Residential:
    def __init__(self):
        self.config = None

    def read_config(self):
        # 高德基础配置
        self.config = ConfigParser()
        self.config.read('gaode_map.cfg')
        gaode_api = self.config.get('gaode', 'def_gaode_api')
        gaode_key = self.config.get('gaode', 'def_gaode_key')

        # 遍历读取小店配置，并返回数组
        shop_list = []
        cfg_sections = self.config.sections()
        for sp in cfg_sections:
            if sp.startswith("shop_"):
                shop = Shop()
                shop.shop_name = self.config.get(sp, 'shop_name')
                shop.location = self.config.get(sp, 'location')
                shop.radius = self.config.get(sp, 'radius')
                shop.page_size = self.config.get(sp, 'page_size')
                shop.page_num = self.config.get(sp, 'page_num')
                shop.types = self.config.get(sp, 'types')
                shop_list.append(shop)

        return gaode_api, gaode_key, shop_list

    @staticmethod
    def get_around_community(self):
        logging.info('开始调用高德api查询小店周边业态...')
        gaode_api, gaode_key, shop_list = self.read_config()

        if gaode_api is None or gaode_key is None or shop_list is None or len(shop_list) <= 0:
            logging.warning('高德参数配置有误，请检查后重试~')
            return

        # 遍历小店配置，逐个调用高德pai查询周边，并将结果汇总写入文件
        result_json_list = []
        for shop in shop_list:
            param = {
                'key': gaode_key,
                'types': shop.types,  # 高德POI值，对应查询周边类型(住宅)
                'location': shop.location,  # 小店经纬度
                'radius': shop.radius,
                'page_size': shop.page_size,
                'page_num': shop.page_num,
            }
            response = requests.get(gaode_api, param)
            if response.status_code == 200 and json.loads(response.text)['status'] == '1':
                for poi in json.loads(response.text)['pois']:
                    # 插入小店信息字段
                    poi['shop_name'] = shop.shop_name
                    poi['shop_location'] = shop.location
                    result_json_list.append(poi)

        # 写入文件
        write_result_to_file('shop_community_result.json', result_json_list)


# 将结果写入文件
def write_result_to_file(file_name, result):
    logging.info('小店周边信息收集完成，行数:[%s]， 开始写入文件:[%s]， start...', len(result), file_name)
    wf = open(file_name, 'w')
    wf.write(json.dumps(result, ensure_ascii=False))
    wf.close()
    logging.info('小店周边信息写入完成')


# 小店配置类
class Shop:
    shop_name = None
    location = None
    radius = None
    page_size = None
    page_num = None
    types = None


if __name__ == '__main__':
    Residential.get_around_community(Residential())
