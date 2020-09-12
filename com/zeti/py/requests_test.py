import requests

# get
# resp = requests.get('https://www.baidu.com')
# print(resp.text)

# post
data_dict = {
    "account": "18888888888",
    "password": "!Q@W#E$R",
}
headers = {'user-agent': 'my-app/0.0.1'}
resp = requests.post('https://cms-member.mamaqunaer.com/login', data_dict, headers)
print(resp.status_code)
print('----------')
print(resp.headers)
print('----------')
print(resp.json())
print('----------')
print(resp.raw)
print('----------')
print(resp.cookies)

# upload file

