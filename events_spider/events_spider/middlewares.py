# -*- coding: utf-8 -*-

import random 
import requests
import base64
from fake_useragent import UserAgent
from utils.tools import APP_CONF, LOGGER

requests.adapters.DEFAULT_RETRIES = 5

class RandomUserAgentMiddleware(object):
    def __init__(self, crawler):
        super(RandomUserAgentMiddleware, self).__init__()

        self.ua = UserAgent()
        #从setting文件中读取RANDOM_UA_TYPE值
        self.ua_type = crawler.settings.get('RANDOM_UA_TYPE', 'random')

    @classmethod
    def from_crawler(cls, crawler):
        return cls(crawler)

    def process_request(self, request, spider):
        def get_ua():
            '''Gets random UA based on the type setting (random, firefox…)'''
            return getattr(self.ua, self.ua_type) 

        user_agent_random=get_ua()
        request.headers.setdefault('User-Agent', user_agent_random)     


class ProxiesSpiderMiddleware(object): 
    def __init__(self, settings):
        self.PROXIES = self.get_ips()
        self.proxy_flag = settings.getbool('PROXY_ENABLED', True)
    
    @classmethod
    def from_crawler(cls, crawler):
        settings = crawler.settings
        return cls(settings)

    def process_request(self, request, spider):  
        if self.proxy_flag:
            thisip = random.choice(self.PROXIES)   
            LOGGER.info('IP Proxy Enabled.')
            request.headers["proxy"]="http://"+thisip
            
            # 用户密码
            proxy_user_pass = APP_CONF['proxy']['username'] + ":" + APP_CONF['proxy']['password']
            # 设置代理认证
            encoded_user_pass = base64.encodestring(proxy_user_pass)
            request.headers['Proxy-Authorization'] = 'Basic ' + encoded_user_pass


    def get_ips(self):
        reponse = requests.get(APP_CONF['proxy']['url'] + '&count=' + str(APP_CONF['proxy']['count']))
        ips = []
        for ip in reponse.text.split('\r\n')[:-1]:
            adr, port = ip.split(':')
            ips.append(ip)    
        return ips