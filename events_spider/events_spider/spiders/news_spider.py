# -*- coding: utf-8 -*-

import time
import re
import scrapy
from urlparse import urlsplit
from events_spider.items import NewsItems
from scrapy.spiders.crawl import Rule
from scrapy.spiders import CrawlSpider
from scrapy.linkextractors import LinkExtractor
from events_spider.sites_extract import site_get_content, news_site_get_time
from events_spider.utils.tools import LOGGER

class NewsSpider(CrawlSpider):
    name = "news"

    custom_settings = {
        'REDIRECT_ENABLED': False,
        'ITEM_PIPELINES': {
            'events_spider.pipelines.NewsESPipeline': 300,
        },
        # 'DEPTH_LIMIT': 2,
    }

    rules = [
        Rule(LinkExtractor(allow='/+'), callback='parse_items', follow=False)
    ]

    def __init__(self, *args, **kwargs):
    # def __init__(self, start_urls=None, *args, **kwargs):
        logger = LOGGER
        # domain = kwargs.pop('domain', '')
        # self.allowed_domains = filter(None, domain.split(','))
        # print(self.allowed_domains)
        super(NewsSpider, self).__init__(*args, **kwargs)
        # 传递参数为全字符串
        self.start_urls = [kwargs['start_url']]
        self.allowed_domains = [re.split(r'''www.''', urlsplit(kwargs['start_url'])[1])[-1]]

    def parse_items(self, response):
        body_text = response.xpath('//body').extract_first()
        # pattern = r'''\d+(.html|.htm|.shtml)'''
        # sub_body = re.search(pattern, response.url)
                                
        # if body_text and sub_body:
        if body_text:
            # 提取发表时间
            pub_time = news_site_get_time(body_text)
            # 若有发表时间，则为新闻页
            if pub_time is not None and pub_time != "":
                extracted_content = site_get_content(body_text)
                if extracted_content and len(extracted_content) > 20:
                    item = NewsItems()
                    item['content'] = extracted_content
                    item['url'] = response.url
                    item['pub_time'] = pub_time.replace(' ', 'T')

                    title = response.xpath('//title/text()').extract_first()
                    if title:
                        title_list = re.split(r'''-|\||_|,''', title)
                        title_len_list = map(len,title_list)
                        item['title']= title_list[title_len_list.index(max(title_len_list))]
                    else:
                        item['title'] = ""
                    request = scrapy.Request('http://' + urlsplit(response.url)[1], callback=self.parse_domain, dont_filter=True)
                    request.meta['item'] = item
                    yield request
                                        
    def parse_domain(self, response):
        item = response.meta['item']
        domain = response.xpath('//title/text()').extract_first().strip()
        item['media_sources'] = re.split(r'''-|\||_''', domain)[0] if domain else urlsplit(response.url)[1]
        item['comment_num'] = 0
        item['repost_num'] = 0
        item['like_num'] = 0
        yield item