# -*- coding: utf8 -*-
import json
import socket
import time
from collections import OrderedDict
from time import sleep
import requests
import urllib3

from config import logger


def _set_header_default():
    header_dict = OrderedDict()
    header_dict["Accept"] = "*/*"
    header_dict["Accept-Encoding"] = "gzip, deflate"
    header_dict["X-Requested-With"] = "superagent"

    header_dict[
        "User-Agent"] = "Mozilla/5.0 (iPhone; CPU iPhone OS 11_0 like Mac OS X) AppleWebKit/604.1.38 (KHTML, like Gecko) Version/11.0 Mobile/15A372 Safari/604.1"
    header_dict["Content-Type"] = "application/x-www-form-urlencoded; charset=UTF-8"
    return header_dict


class HTTPClient(object):

    def __init__(self):
        """
        :param method:
        :param headers: Must be a dict. Such as headers={'Content_Type':'text/html'}
        """
        self.initS()
        self._cdn = None
        self.proxies = None

    def initS(self):
        self._s = requests.Session()
        self._s.headers.update(_set_header_default())
        return self

    def set_cookies(self, **kwargs):
        """
        设置cookies
        :param kwargs:
        :return:
        """
        for k, v in kwargs.items():
            self._s.cookies.set(k, v)

    def get_cookies(self):
        """
        获取cookies
        :return:
        """
        return self._s.cookies.get_dict()

    def del_cookies(self):
        """
        删除所有的key
        :return:
        """
        self._s.cookies.clear()

    def del_cookies_by_key(self, key):
        """
        删除指定key的session
        :return:
        """
        self._s.cookies.set(key, None)

    def setHeaders(self, headers):
        self._s.headers.update(headers)
        return self

    def resetHeaders(self):
        self._s.headers.clear()
        self._s.headers.update(_set_header_default())

    def getHeadersHost(self):
        return self._s.headers["Host"]

    def setHeadersHost(self, host):
        self._s.headers.update({"Host": host})
        return self

    def getHeadersReferer(self):
        return self._s.headers["Referer"]

    def setHeadersReferer(self, referer):
        self._s.headers.update({"Referer": referer})
        return self

    @property
    def cdn(self):
        return self._cdn

    @cdn.setter
    def cdn(self, cdn):
        self._cdn = cdn

    def send(self, urls, data=None, **kwargs):
        """send request to url.If response 200,return response, else return None."""
        allow_redirects = False
        is_logger = urls.get("is_logger", False)
        req_url = urls.get("req_url", "")
        re_try = urls.get("re_try", 0)
        s_time = urls.get("s_time", 0)
        http = urls.get("http", "") or "https"
        error_data = {"code": 99999, "message": u"重试次数达到上限"}
        if data:
            method = "post"
            self.setHeaders({"Content-Length": "{0}".format(len(data))})
        else:
            method = "get"
            self.resetHeaders()
        self.setHeadersReferer(urls["Referer"])
        if is_logger:
            logger.log(
                u"url: {0}\n入参: {1}\n请求方式: {2}\n".format(req_url, data, method, ))
        self.setHeadersHost(urls["Host"])
        if self.cdn:
            url_host = self.cdn
        else:
            url_host = urls["Host"]
        for i in range(re_try):
            try:
                # sleep(urls["s_time"]) if "s_time" in urls else sleep(0.001)
                sleep(s_time)
                requests.packages.urllib3.disable_warnings()
                response = self._s.request(method=method,
                                           timeout=3,
                                           proxies=self.proxies,
                                           url=http + "://" + url_host + req_url,
                                           data=data,
                                           allow_redirects=allow_redirects,
                                           verify=False,
                                           **kwargs)
                if response.status_code == 200:
                    if response.content:
                        if is_logger:
                            logger.log(
                                u"出参：{0}".format(response.content.decode()))
                        return json.loads(response.content) if urls["is_json"] else response.content
                    else:
                        logger.log(
                            u"url: {} 返回参数为空".format(urls["req_url"]))
                        return error_data
                elif response.status_code == 403:
                    print(f"当前http请求异常，状态码为{response.status_code}, 休息一会儿")
                    time.sleep(5)
                else:
                    print(f"当前http请求异常，状态码为{response.status_code}")
                    sleep(urls["re_time"])
            except (requests.exceptions.Timeout, requests.exceptions.ReadTimeout, requests.exceptions.ConnectionError,
                    ConnectionResetError, urllib3.exceptions.ProtocolError, TimeoutError, urllib3.exceptions.NewConnectionError) as e:
                print(f"当前代理连接异常，异常ip：{self.proxies}")
            except socket.error as e:
                print(e)
        return error_data
