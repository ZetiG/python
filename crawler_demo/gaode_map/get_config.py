#!/usr/bin/env python3
from configparser import ConfigParser
from shop_entity import Shop

config = ConfigParser()
config.read('gaode_map.cfg')

# 高德api, key
gaode_base_url = config.get('gaode', 'def_gaode_api')
gaode_base_key = config.get('gaode', 'def_gaode_key')


# shop基础配置, 并返回数组
def get_shop_config():
    shop_list = []
    cfg_sections = config.sections()
    for sp in cfg_sections:
        if sp.startswith("shop_"):
            shop = Shop()
            shop.shop_name = config.get(sp, 'shop_name')
            shop.location = config.get(sp, 'location')
            shop.radius = config.get(sp, 'radius')
            shop.page_size = config.get(sp, 'page_size')
            shop.page_num = config.get(sp, 'page_num')
            shop.types = config.get(sp, 'types')
            shop_list.append(shop)
    return shop_list

