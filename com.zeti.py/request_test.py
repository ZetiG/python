import requests
from requests.exceptions import HTTPError

# post
request_url = "https://cms-member.mamaqunaer.com/login"

headers = {
    'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
}

data_dict = {
    "account": "18888888888",
    "password": "!Q@W#E$R",
}
response = requests.post(request_url, data_dict)
print(response.json())

# --------------------
try:
    response = requests.get('https://httpbin.org/get')
    response.raise_for_status()
    # access JSOn content
    jsonResponse = response.json()
    print("Entire JSON response")
    print(jsonResponse)

    # iterate jsonResponse
    for k, v in jsonResponse.items():
        print("response: k: ", k, " v:", v)

# 异常捕获
except HTTPError as http_err:
    print(f'HTTP error occurred: {http_err}')
except Exception as err:
    print(f'Other error occurred: {err}')
