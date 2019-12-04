# -*- coding: utf-8 -*-

import re
import scrapy
from scrapy.spiders import Spider
import datetime
import json
from bs4 import BeautifulSoup
from events_spider.items import NewsItems
from events_spider.utils.tools import LOGGER

class ZhihuSpider(Spider):
    name = 'zhihu'
    
    handle_httpstatus_list = [301, 302]
    custom_settings = {
        # 'JOBDIR': APP_CONF['config']['bbs_url_filterd_path'],
        'ITEM_PIPELINES': {
            'events_spider.pipelines.UpdatePipeline': 300,
        },
        'CONCURRENT_REQUESTS': 1,
    }

    allowed_domains = ["zhihu.com"]

    def __init__(self, *args, **kwargs):
        logger = LOGGER
        super(ZhihuSpider, self).__init__(*args, **kwargs)
        self.start_urls = [kwargs['start_url']]
        # self.allowed_domains = [re.split(r'''www.''', urlsplit(kwargs['start_url'])[1])[-1]]

    def parse(self, response):
        item = NewsItems()
        content = json.loads(response.body)

        for data in content['data']:
            if data['type'] == "one_box":
                for d in data['object']['content_list']:
                    item['title'] = BeautifulSoup(d['question']['name']).get_text()
                    item['url'] = 'https://www.zhihu.com/question/'+d['question']['id']
                    item['like_num'] = d['voteup_count']
                    date = datetime.datetime.fromtimestamp(d['created_time'])
                    item['pub_time'] = date.strftime("%Y-%m-%dT%H:%M:%S")
                    item['media_sources'] = u'知乎'
                    yield scrapy.Request(item['url'], callback=self.parse_comment_num, meta={'item':item}, dont_filter=True)
            elif data['type'] == "search_result":
                if data['object']['type'] == 'article':
                    item['title'] = data['object']['title']
                    item['content'] = BeautifulSoup(data['object']['content']).get_text()
                    item['comment_num'] = int(data['object']['comment_count'])
                    item['like_num'] = int(data['object']['voteup_count'])
                    item['repost_num'] = 0
                    item['url'] = 'https://zhuanlan.zhihu.com/p/'+data['object']['id']
                    date = datetime.datetime.fromtimestamp(data['object']['created_time'])
                    item['pub_time'] = date.strftime("%Y-%m-%dT%H:%M:%S")
                    item['media_sources'] = u'知乎'
                    # yield item
                elif data['object']['type'] == 'answer':
                    item['title'] = BeautifulSoup(data['object']['question']['name']).get_text()
                    item['url'] = 'https://www.zhihu.com/question/'+data['object']['question']['id']
                    item['like_num'] = data['object']['voteup_count']
                    date = datetime.datetime.fromtimestamp(data['object']['created_time'])
                    item['pub_time'] = date.strftime("%Y-%m-%dT%H:%M:%S")
                    item['media_sources'] = u'知乎'
                    yield scrapy.Request(item['url'], callback=self.parse_comment_num, meta={'item':item}, dont_filter=True)
            
    def parse_comment_num(self, response):
        content = response.xpath('//script[@id="js-initialData"]//text()').extract_first()
        item = response.meta['item']
        # 请求太频繁，本次未获取到内容
        if content == None:
            return 
        content = json.loads(content)
        question_id = item['url'].split('/')[-1]
        item['comment_num'] = content['initialState']['entities']['questions'][question_id]['answerCount']
        item['content'] = content['initialState']['entities']['questions'][question_id]['detail']
        item['repost_num'] = content['initialState']['entities']['questions'][question_id]['followerCount']
        yield item