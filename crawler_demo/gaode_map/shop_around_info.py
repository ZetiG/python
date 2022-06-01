"""
小店周边信息（小区、商场、超市、便利店、写字楼）
"""
import logging
import requests
import json
import get_config

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

gaode_api = get_config.gaode_base_url
gaode_key = get_config.gaode_base_key


# 1. 根据小店经纬度坐标，获取周边1公里范围内住宅信息
# 2. 拿到上步获取的住宅信息，利用爬虫从二手房交易网站获取住宅信息
class Residential:

    @staticmethod
    def get_around_community():
        logging.info('开始调用高德api查询小店周边业态...')
        shop_list = get_config.get_shop_config()

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
            response = requests.get(get_config.gaode_base_url, param)
            logging.info("获取小店周边信息请求地址: %s", response.url)
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


if __name__ == '__main__':
    Residential.get_around_community()
