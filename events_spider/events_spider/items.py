# -*- coding: utf-8 -*-
import scrapy

class NewsItems(scrapy.Item):
    title = scrapy.Field()
    pub_time = scrapy.Field()
    content = scrapy.Field()
    url = scrapy.Field()
    comment_num = scrapy.Field()
    like_num = scrapy.Field()
    media_sources = scrapy.Field()
    repost_num = scrapy.Field()