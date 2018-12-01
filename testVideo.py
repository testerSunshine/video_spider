import time
from collections import OrderedDict

import requests

page_start = 0


def _set_header_default():
    header_dict = OrderedDict()
    header_dict["Accept"] = "application/json, text/plain, */*"
    header_dict["Accept-Encoding"] = "gzip, deflate"
    header_dict[
        "User-Agent"] = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_4) AppleWebKit/537.36 (KHTML, like Gecko) 12306-electron/1.0.1 Chrome/59.0.3071.115 Electron/1.8.4 Safari/537.36"
    header_dict["Content-Type"] = "application/x-www-form-urlencoded; charset=UTF-8"
    return header_dict


requests.packages.urllib3.disable_warnings()
for i in range(20000):
    url = f"https://movie.douban.com/j/search_subjects?type=movie&tag=%E7%83%AD%E9%97%A8&sort=recommend&page_limit=20&page_start={page_start}"
    page_start += 20
    time.sleep(2)
    print(requests.get(url, verify=False, headers=_set_header_default()).content.decode())

