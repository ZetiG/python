"""
获取天气 api
"""
import logging
import requests
import gaode_map.get_config

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# visualcrossing api, 获取天气
def vc_weathers(addr_name, start_day, end_day):
    if (addr_name or start_day or end_day) is None or (len(addr_name) or len(start_day) or len(end_day)) <= 0:
        raise ValueError("visualcrossing api获取天气, 请求参数无效")

    url = 'https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/' + addr_name + '/' + start_day + '/' + end_day
    params = {
        'unitGroup': 'metric',
        'include': 'days',
        'key': '7GCGWWAZWTQK562DJ69Z57G48',
        'contentType': 'json'
    }

    try:
        resp = requests.get(url, params)
        logging.info("vc获取天气情况请求地址: %s", resp.url)
        if resp.ok is False:
            raise BaseException("vc获取天气情况失败: ", resp.text)

        resp_json = resp.json()

        logging.info(resp_json)
    except Exception as e:
        print('Error: ', e)


# 高德api，获取天气
def gaode_weather(addr_code):
    if addr_code is None or len(addr_code) <= 0:
        raise ValueError("高德api获取天气, 请求参数无效")

    url = 'https://restapi.amap.com/v3/weather/weatherInfo'
    params = {
        'key': 'f7d16f5e0e3a2c832557df258843a41b',
        'city': addr_code,
        'extensions': 'all',
    }

    try:
        resp = requests.get(url, params)
        logging.info("高德获取天气情况请求地址: %s", resp.url)
        if resp.ok is False:
            raise BaseException("高德获取天气情况失败: ", resp.text)

        resp_json = resp.json()

        logging.info(resp_json)
    except Exception as e:
        print('Error: ', e)


if __name__ == '__main__':
    # get_weathers('hangzhou', '2022-5-25', '2022-6-5')
    vc_weathers('hangzhou', '2022-5-26', '2022-6-6')
    gaode_weather('330100')
