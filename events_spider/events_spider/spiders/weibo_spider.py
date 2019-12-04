# -*- coding: utf-8 -*-

import scrapy
from scrapy.spiders import Spider
import re
import datetime
import json
from bs4 import BeautifulSoup
from events_spider.items import NewsItems
from events_spider.utils.tools import LOGGER

class WeiboSpider(Spider):
    name = "weibo"

    custom_settings = {
        # 'JOBDIR': APP_CONF['config']['bbs_url_filterd_path'],
        'ITEM_PIPELINES': {
            'events_spider.pipelines.UpdatePipeline': 300,
        }
    }

    allowed_domains = ["m.weibo.cn"]

    def __init__(self, *args, **kwargs):
        logger = LOGGER
        super(WeiboSpider, self).__init__(*args, **kwargs)
        self.start_urls = [kwargs['start_url']]
        # self.allowed_domains = [re.split(r'''www.''', urlsplit(kwargs['start_url'])[1])[-1]]

    def parse(self, response):
    	item = NewsItems()
        content = json.loads(response.body)
        
        for card in content['data']['cards']:
            if card['card_type'] == 9:
                item['comment_num'] = int(card['mblog']['comments_count'])
                item['repost_num'] = int(card['mblog']['reposts_count'])
                item['like_num'] = int(card['mblog']['attitudes_count'])
                item['media_sources'] = u'新浪微博'

                # pub = card['mblog']['created_at']
                # now = datetime.datetime.now()
                # if u'分钟' in pub:
                #     time = re.search(r'\d+', pub)
                #     delta = datetime.timedelta(minutes=int(time.group()))
                #     n_days = now - delta
                #     item['pub_time'] = n_days.strftime('%Y-%m-%dT%H:%M:%S')
                # elif u'小时' in pub:
                #     time = re.search(r'\d+', pub)
                #     delta = datetime.timedelta(hours=int(time.group()))
                #     n_days = now - delta
                #     item['pub_time'] = n_days.strftime('%Y-%m-%dT%H:%M:%S')
                # else:
                #     year = str(now.date())[:5]
                #     item['pub_time'] = year+'pub'+'T00:00:00'
                item['url'] = card['scheme']
                yield scrapy.Request('https://m.weibo.cn/statuses/show?id='+card['mblog']['bid'], callback=self.parse_content, meta={'item':item}, dont_filter=True)

    def parse_content(self, response):
        content = json.loads(response.body)
        text = BeautifulSoup(content['data']['text']).get_text()
        item = response.meta['item']
        item['content'] = text
        item['title'] = text
        pub = content['data']['created_at'].split(' ')
        month, day, time, year = pub[1], pub[2], pub[3], pub[-1]
        month_mapping = {
            "Jan": 1,
            "Feb": 2,
            "Mar": 3,
            "Apr": 4,
            "May": 5,
            "June": 6,
            "July": 7,
            "Aug": 8,
            "Sept": 9,
            "Oct": 10,
            "Nov": 11,
            "Dec": 12
        }
        item['pub_time'] = year+'-'+str(month_mapping.get(month))+'-'+day+'T'+time
        yield item