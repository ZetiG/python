import configparser
import json
import logging
import os
import urllib.parse
import requests
from fake_useragent import UserAgent
from get_proxy_ip import proxies_switch
from bs4 import BeautifulSoup

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# 随机UA
path = os.path.join('', 'fake_ua.json')
# path 为你放置 fake_ua.json文件路径
fake_ua = UserAgent(path=path).random
logger.info('当前UserAgent:[%s]', fake_ua)

headers = {
    "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
    "accept-encoding": "gzip, deflate, br",
    "accept-language": "zh-CN,zh;q=0.9",
    "cache-control": "max-age=0",
    "cookie": "hng=CN%7Czh-CN%7CCNY%7C156; cookie2=117413313358c473a06b1ea9d24b6487; t=102f78e2d4a2f9f2dc8c6259ad4ca8c1; _tb_token_=148378ea7bef; tk_trace=1; cna=24tMGKtosBwCAXrgVzLq1KDL; xlly_s=1; _l_g_=Ug%3D%3D; login=true; dnk=%5Cu6ED5%5Cu5C0F%5Cu57640201; tracknick=%5Cu6ED5%5Cu5C0F%5Cu57640201; lid=%E6%BB%95%E5%B0%8F%E5%9D%A40201; unb=2298381582; lgc=%5Cu6ED5%5Cu5C0F%5Cu57640201; cookie1=BxFiWWXK9Sab0g1DuJKtNZb1O790waHNoOEEDvQUbX8%3D; cookie17=UUpoZCVnvh283Q%3D%3D; _nk_=%5Cu6ED5%5Cu5C0F%5Cu57640201; sg=122; sm4=330109; csa=8047484560_0_30.172406.120.232747_0_0_0; uc1=existShop=false&cookie15=UIHiLt3xD8xYTw%3D%3D&cookie16=UtASsssmPlP%2Ff1IHDsDaPRu%2BPw%3D%3D&cookie14=Uoe1gqgl3KCCgQ%3D%3D&pas=0&cookie21=URm48syIYB3rzvI4Dim4; uc3=id2=UUpoZCVnvh283Q%3D%3D&nk2=iNE6uNDRfR85Qw%3D%3D&lg2=URm48syIIVrSKA%3D%3D&vt3=F8dCuAAj3fKyDiW7K0c%3D; uc4=nk4=0%40irOKlXRaqzdSZEhA00yifq6SHZDS&id4=0%40U2giSSnvpZu8RVW3%2F7a8yPd75NV6; sgcookie=E100PsYHmXhsOsB%2BUHPhK0QU%2BPK5gssytX6G8vsdBhwdxa6kDu1Bjn9hCkNio0hbDYnxWTMxTUhwTfTZr3aIwcHS6A%3D%3D; csg=0de5f4b9; _m_h5_tk=3cecb72211e66e129d587a6f2a2c2543_1610078471151; _m_h5_tk_enc=e7656a4d8ad931050b2487e656383f76; enc=GRCSL6EUu7pgi1vmFgQoGnDs6A5P92jGW1PhR2x4QiJ8mBNRApjWshHHw1kKBDQ9iAFXGe27lP%2FDiXLdfiQ9vQ%3D%3D; tfstk=ckadBFqQ0OX36vcTgkIMV-YnLKURaYX-CBMkysSkNbf1HKdJNsvborzoqMGWpVCO.; l=eB_iD0oHO7aCW63oBOfZlurza779JIRAguPzaNbMiOCP9UC65EZGWZ8oyz8BCnGVhs9HR3JsgJC4BeYBqCYQ5O976PnzJPkmn; isg=BBcXP6FjBJl2m4AOe-D7DldMpothXOu-YetUQ2lEBuZNmDfacSgPDr-y-jiGcMM2",
    "referer": "https://login.taobao.com/member/login.jhtml?tpl_redirect_url=https%3A%2F%2Fchaoshi.tmall.com%2F%3Fspm%3D875.7931836%2FB.2016004.1.259042651AgjZx%26acm%3Dlb-zebra-148799-667861.1003.4.2429983%26scm%3D1003.4.lb-zebra-148799-667861.OTHER_14561837688591_2429983&style=miniall&enup=true&newMini2=true&full_redirect=true&sub=true&from=tmall&allp=assets_css%3D3.0.10/login_pc.css&pms=1610070891179",
    "sec-fetch-mode": "navigate",
    "sec-fetch-site": "none",
    "sec-fetch-user": "?1",
    "upgrade-insecure-requests": "1",
    "user-agent": fake_ua,
}

# 代理ip
proxies = {'http': '114.24.112.55:8088'}


# 安居客，根据小区名称查询小区id
# https://hangzhou.anjuke.com/esf-ajax/community/pc/autocomplete?city_id=18&kw=%E9%98%B3%E5%85%89%E5%9F%8E%E7%BF%A1%E4%B8%BD%E5%85%AC%E5%9B%AD&type=2
# https://hangzhou.anjuke.com/community/?kw=%E9%A1%BA%E5%8F%91%C2%B7%E6%81%92%E5%9B%AD
# 安居客，根据小区id查询小区具体信息
# https://hangzhou.anjuke.com/community/view/1032464


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

            # 调用安居客api，查询小区对应安居客内的编码
            get_ajk_community_code(cm_name)


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
    if response.ok is False:
        proxies_switch(ajk_url)
    if response.status_code == 200 and json.loads(response.text)['status'] == 'ok':
        data = json.loads(response.text)['data']
        if data is None or len(data) <= 0:
            logging.info('调用安居客api，查询并返回小区对应安居客内的编码为空，根据名称进行模糊匹配, 小区:[%s]', community_name)

            url = 'https://hangzhou.anjuke.com/community/'
            param = {
                'kw': community_name
            }

            while True:
                resp = requests.get(url, urllib.parse.urlencode(param), headers=headers, proxies=proxies)
                if resp.ok is False or resp.url.split('?')[0] != url:
                    proxies = proxies_switch(url)
                    continue
                break

            soup = BeautifulSoup(resp.text, 'lxml')
            find_all = soup.find_all('a', attrs={'class': 'li-row'})
            if find_all is not None and len(find_all) > 0:
                href_ = find_all[0]['href']
                logging.info('模糊匹配到该小区，小区:[%s], 连接:[%s]', community_name, href_)
            else:
                logging.info('暂未搜索到该小区信息，小区:[%s]', community_name)
            return

        if data[0] is not None and len(data[0]) > 0 and data[0]['id'] is not None:
            logging.info('调用安居客api，查询并返回小区对应安居客内的编码，小区名称:[%s] 小区编码:[%s]', community_name, data[0]['id'])
            return data[0]['id']  # price 拓展可获取小区房价


if __name__ == '__main__':
    read_shop_community_file()
