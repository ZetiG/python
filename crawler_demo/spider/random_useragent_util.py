import os
import random
from fake_useragent import UserAgent

'''
获取随机UserAgent
'''

def generate_ua():
    useragent_list = []
    ua_bar_name = [
        'ua.ie',
        'ua.msie',
        'ua["Internet Explorer"]',
        'ua.opera',
        'ua.chrome',
        'ua.google',
        'ua["google chrome"]',
        'ua.firefox',
        'ua.ff',
        'ua.safari'
    ]
    for item in ua_bar_name:
        for i in range(random.randint(10, 80)):
            # ua = 'Mozilla/5.0 (Windows NT 6.1; Win64; x64; rv:23.0) Gecko/20131011 Firefox/23.0'
            ua = UserAgent()
            if ua in useragent_list:
                continue
            else:
                useragent_list.append(ua)
    return useragent_list


def random_ua(user_agent_list):
    random.shuffle(user_agent_list)
    user_agent = random.choice(user_agent_list)
    return user_agent


if __name__ == '__main__':
    # print(random_ua(generate_ua()))

    # 从文件获取随机UA
    path = os.path.join('', 'fake_ua.json')
    # path 为你放置 fake_ua.json文件路径
    fake_ua = UserAgent(path=path).random
    print(fake_ua)
