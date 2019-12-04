# -*- coding: utf-8 -*-

import scrapy
from scrapy_redis.spiders import RedisSpider
from events_spider.utils.tools import getCostumLogger

from weibo_spider import WeiboSpider
from news_spider import NewsSpider

import types
import functools
def copy_func(f):
    """Based on http://stackoverflow.com/a/6528148/190597 (Glenn Maynard)"""
    g = types.FunctionType(f.func_code, f.func_globals, name=f.func_name,
                           argdefs=f.func_defaults,
                           closure=f.func_closure)
    g = functools.update_wrapper(g, f)
    return g

BASE = RedisSpider
class GenericSpider(BASE):
    name = "generic"

    url_type_mapping = {
    					# "hupu":HupuSpider,
    					# "zhihu": ZhihuSpider,
    					# "baidu": BaiduSpider,
    					# "sougou": SougouSpider,
    					# "news.so": SoSpider,
    					# "wechat": WechatSpider,
    					"weibo": WeiboSpider,
    }

    custom_settings = {}

    redis_key = 'start_urls:generic'

    def __init__(self, *args, **kwargs):
        logger = getCostumLogger()

    # def parse(self, response):
    #     logger.info('###########parse')

    def make_requests_from_url(self, url):
        # for k, v in self.url_type_mapping.items():
            # if k in url:
            #     BASE = v
            #     return scrapy.Request(url, callback=self.parse, dont_filter=True)
        if "weibo" in url:
            # GenericSpider.__bases__ = (WeiboSpider,)
            parse = copy_func(WeiboSpider.parse)
            custom_settings = WeiboSpider.custom_settings
            return scrapy.Request(url+'api/container/getIndex?containerid=102803&openApp=0', callback=parse, dont_filter=True)
        else:
            BASE = NewsSpider
            return scrapy.Request(url, callback=self.parse_items, dont_filter=True)