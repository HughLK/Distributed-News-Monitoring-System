# -*- coding: utf-8 -*-

from utils.tools import APP_CONF

ES_URL = APP_CONF['es']['url']

BOT_NAME = 'events_spider'

WEIBO_ACOUNT = [
    ('15336171392', '7hxGP6l4')
]

SPIDER_MODULES = ['events_spider.spiders']
NEWSPIDER_MODULE = 'events_spider.spiders'

ROBOTSTXT_OBEY = False

CONCURRENT_REQUESTS = 8

COOKIES_ENABLED = False
REDIRECT_ENABLED = False
PROXY_ENABLED = True

DOWNLOADER_MIDDLEWARES = {
    'events_spider.middlewares.RandomUserAgentMiddleware': 100,
    'events_spider.middlewares.ProxiesSpiderMiddleware': 100,
}


MYEXT_ENABLED = True
# 一个单位5s
MYEXT_ITEMCOUNT = 1   
# EXTENSIONS = {
#    'events_spider.extensions_close.SpiderOpenCloseLogging': 540,
# }

# ITEM_PIPELINES = {
# 　　 'scrapy_redis.pipelines.RedisPipeline': 100 ,
# }

RANDOM_UA_TYPE = 'random'
FEED_EXPORT_ENCODING = 'utf-8'
DOWNLOAD_TIMEOUT = 5
DUPEFILTER_CLASS = 'scrapy_redis.dupefilter.RFPDupeFilter'
SCHEDULER = "scrapy_redis.scheduler.Scheduler"

DEPTH_LIMIT = 5

REDIS_HOST = '192.168.1.129' 
REDIS_PORT = 6379
# REDIS_START_URLS_KEY = 'start_urls:news'