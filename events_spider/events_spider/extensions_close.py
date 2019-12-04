# -*- coding: utf-8 -*-

import time
 
from scrapy import signals
from scrapy.exceptions import NotConfigured
 
 
class SpiderOpenCloseLogging(object):
    def __init__(self, item_count, crawler):
        """
        初始化操作
        :param item_count: 程序空闲的最大次数
        :param crawler: 类，用于发送关闭程序信号
        """
        self.item_count = item_count
        self.crawler = crawler
        self.count = 0      # 统计空闲次数
        self.idle_time = None   # 记录时间，可有可无
 
    @classmethod
    def from_crawler(cls, crawler):
 
        # 判断是否启用扩展
        if not crawler.settings.getbool('MYEXT_ENABLED'):
            raise NotConfigured
 
        # MYEXT_ITEMCOUNT 默认1 小时，时间单位为12次每分钟，空闲时5秒进来一次
        item_count = crawler.settings.getint('MYEXT_ITEMCOUNT', 720)
 
        ext = cls(item_count, crawler)
 
        crawler.signals.connect(ext.spider_opened, signal=signals.spider_opened)
 
        crawler.signals.connect(ext.spider_closed, signal=signals.spider_closed)
 
        crawler.signals.connect(ext.spider_idle, signal=signals.spider_idle)  # 加载空闲信号
 
        return ext
 
    def spider_opened(self, spider):
        spider.log("opened spider %s" % spider.name)
 
    def spider_closed(self, spider):
        spider.log("closed spider %s" % spider.name)
 
    def spider_idle(self, spider):
        """
        记录信息，作出关闭选择
        :param spider:
        :return:
        """
        # 记录第一次进入的时间
        if self.count == 0:
            self.idle_time = time.time()
 
        # 判断redis_key中是否为空，如果为空时，则空闲一次，统计 + 1
        if not spider.server.exists(spider.redis_key):
 
            self.count += 1
        else:
            self.count = 0
        
        # 空闲超过指定分钟，结束程序
        if self.count > self.item_count:
            spider.log("spider continued idle number exceed:%s  idle datetiem exceed:%s" % (
            self.count, time.time() - self.idle_time))
            # 发送结束信号
            self.crawler.engine.close_spider(spider, 'close spider')
 
