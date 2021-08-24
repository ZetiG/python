# coding=utf-8

from selenium import webdriver
from selenium.webdriver.common.keys import Keys
import time
import requests
import json
import uuid
from config import *
import logging


def screen():
    LOG_FORMAT = "%(asctime)s - %(levelname)s - %(message)s"
    logging.basicConfig(filename='redash_screen2.log', level=logging.INFO, format=LOG_FORMAT)
    # 无头模式
    png_names = []
    try:
        chrome_options = webdriver.ChromeOptions()
        chrome_options.add_argument('--headless')
        chrome_options.add_argument('--window-size=2400,2600')
        chrome_options.add_argument('--disable-gpu')
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--disable-dev-shm-usage')
        driver = webdriver.Chrome(options=chrome_options)
        driver.maximize_window()
        driver.get(login_url)
        # 输入账号
        driver.find_element_by_id('inputEmail').send_keys('liaoyuan@pinpianyi.com')
        # 输入密码
        driver.find_element_by_id('inputPassword').send_keys('liaoyuan')
        driver.find_element_by_tag_name('button').send_keys(Keys.ENTER)
        driver.get(screen_url)
        time.sleep(2)
        refreshs = driver.find_elements_by_xpath("//*[@class='small hidden-print']")
        # boxs = driver.find_elements_by_xpath("//*[@class='body-row-auto scrollbox']")
        boxs = driver.find_elements_by_xpath("//*[@class='body-row-auto']")
        for refresh, box in zip(refreshs, boxs):
            refresh.click()
            while 1:
                try:
                    TimeAgos = driver.find_elements_by_xpath("//*[@data-test='TimeAgo']")
                    if len(TimeAgos) == len(boxs):
                        break
                except:
                    time.sleep(1)
            png_name = time.strftime("%Y%m%d%H%M%S", time.localtime()) + '_' + str(uuid.uuid1())
            box.screenshot('/ppy/web/data-platform/screenshots/redash_screenshots/%s.png' % png_name)
            png_names.append(png_name)
        driver.close()
    except Exception as e:
        print(e)
    return png_names


# 上传钉钉
def post_to_dingtalk(png_name):
    date = time.strftime('%Y-%m-%d %H:%M', time.localtime(time.time()))
    # 正式群
    webhook_url = 'https://oapi.dingtalk.com/robot/send?access_token={}'.format(webhook_token)
    # 测试群
    # webhook_url = 'https://oapi.dingtalk.com/robot/send?access_token={}'.format(test_webhook_token)
    headers = {"content-type": 'application/json'}
    data = {
        'msgtype': 'markdown',
        'markdown': {
            'title': '拼便宜业绩追踪',
            'text': '#### 拼便宜业绩追踪-%s\n> ![screenshot](http://platformapi.data.pinpianyi.com/screenshots/redash_screenshots/%s.png)\n' % (
                date, png_name)
        }
    }
    requests.post(webhook_url, data=json.dumps(data), headers=headers)


def post_fail_dingtalk():
    test_webhook_url = 'https://oapi.dingtalk.com/robot/send?access_token={}'.format(test_webhook_token)
    headers = {"content-type": 'application/json'}
    data = {
        'msgtype': 'markdown',
        'markdown': {
            'title': '拼便宜业绩追踪截图失败',
            'text': '#### 拼便宜业绩追踪截图失败！'
        }
    }
    requests.post(test_webhook_url, data=json.dumps(data), headers=headers)


if __name__ == '__main__':
    for i in range(3000):
        png_names = screen()
        if png_names:
            for png_name in png_names:
                post_to_dingtalk(png_name)
            break
        else:
            time.sleep(30)
            if i == 2:
                post_fail_dingtalk()
