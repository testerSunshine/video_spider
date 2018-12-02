import json
import time
from collections import OrderedDict

import requests


def _set_header_default():
    header_dict = OrderedDict()
    header_dict["Accept"] = "*/*"
    header_dict["Accept-Encoding"] = "gzip, deflate"
    header_dict["X-Requested-With"] = "superagent"

    header_dict[
        "User-Agent"] = "Mozilla/5.0 (iPhone; CPU iPhone OS 11_0 like Mac OS X) AppleWebKit/604.1.38 (KHTML, like Gecko) Version/11.0 Mobile/15A372 Safari/604.1"
    header_dict["Content-Type"] = "application/x-www-form-urlencoded; charset=UTF-8"


def getProxy(session):
    for i in range(2000):
        url = "http://www.shiliuliu.cn/Tools/proxyIP.ashx?OrderNumber=1b2a9207e1638519d7ac96a34e67eb3a&poolIndex=65292&cache=1&qty=1"
        proxyRsp = requests.get(url).content.decode()
        proxie = {
            # 'http://': '{}:{}'.format(proxyRsp["data"][0]["ip"], proxyRsp["data"][0]["port"]),
            # 'http': 'http://{}:{}'.format(proxyRsp["data"][0]["ip"], proxyRsp["data"][0]["port"]),
            # 'https': 'http://{}:{}'.format(proxyRsp["data"][0]["ip"], proxyRsp["data"][0]["port"]),
            'http': 'http://{}'.format(proxyRsp.replace("\r\n", "")),
            'https': 'http://{}'.format(proxyRsp.replace("\r\n", "")),
        }
        print(f"当前代理ip为: {proxie}")
        session.httpClint.proxies = proxie
        time.sleep(297)


def testProxy():
    proxy = getProxy()
    # proxy = {'http': 'http://183.129.207.82:11108'}
    print(proxy)
    # url = "http://icanhazip.com"
    url = "http://httpbin.org/ip"
    print(requests.get(url, proxies=proxy, timeout=2, headers=_set_header_default()).content)


if __name__ == '__main__':
    testProxy()

