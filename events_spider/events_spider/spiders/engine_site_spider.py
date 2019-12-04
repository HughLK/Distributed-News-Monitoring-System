# -*- coding: utf-8 -*-

import re
import datetime
import scrapy
# from bs4 import BeautifulSoup
from scrapy.spiders import Spider
from events_spider.items import NewsItems
from events_spider.sites_extract import site_get_content
from events_spider.utils.tools import LOGGER
 

class BaiduSpider(Spider):
    name = "baidu"
    
    custom_settings = {
        # 'JOBDIR': APP_CONF['config']['news_url_filterd_path'],
        'ITEM_PIPELINES': {
            'events_spider.pipelines.NewsESPipeline': 300,
        }
    }

    allowed_domains = ["baidu.com"]

    def __init__(self, *args, **kwargs):
        logger = LOGGER
        super(BaiduSpider, self).__init__(*args, **kwargs)
        self.start_urls = [kwargs['start_url']]
        # self.allowed_domains = [re.split(r'''www.''', urlsplit(kwargs['start_url'])[1])[-1]]
        
    def parse(self, response):
        item = NewsItems()
        titles = response.xpath('//div[@class="result"]//h3[@class="c-title"]//a')
        url = response.xpath('//div[@class="result"]//h3[@class="c-title"]//a/@href').extract()
        pub_time = response.xpath('//div[@class="result"]//p[@class="c-author"]')
        for i in range(len(url)):
            pub = pub_time[i].xpath('string(.)').extract_first()
            pub = re.sub(r'\t|\s+|\n', "", pub, flags=re.S|re.M)
            source, pub = re.split(r'\xa0\xa0', pub)
            now = datetime.datetime.now()
            if u'分钟' in pub:
                time = re.search(r'\d+', pub)
                delta = datetime.timedelta(minutes=int(time.group()))
                n_days = now - delta
                pub = n_days.strftime('%Y-%m-%dT%H:%M:%S')
            elif u'小时' in pub: 
                time = re.search(r'\d+', pub)
                delta = datetime.timedelta(hours=int(time.group()))
                n_days = now - delta
                pub = n_days.strftime('%Y-%m-%dT%H:%M:%S')
            else:
                pub = re.sub(u'年|月', '-', pub)
                pub = pub.replace(u'日', 'T')+':00'
            item['media_sources'] = source
            item['pub_time'] = pub
            # item['title'] = BeautifulSoup(titles[i].xpath('string(.)')).get_text()
            item['title'] = titles[i].xpath('string(.)').extract_first().replace('\n      ', '').replace('\n    ', '')
            request = scrapy.Request(url=url[i], callback=self.parse2, meta={'item':item})
            yield request
                
    
    def parse2(self, response):
        item = response.meta['item']
        body_text = response.xpath('//body').extract_first()
        item['content'] = site_get_content(body_text)
        item['url'] =  response.url
        item['comment_num'] = 0
        item['repost_num'] = 0
        item['like_num'] = 0
        yield item