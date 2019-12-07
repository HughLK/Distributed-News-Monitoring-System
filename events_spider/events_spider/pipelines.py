# -*- coding: utf-8 -*-

# from sentiment.sentiment_classify import sentiment_pridict
from utils.tools import APP_CONF
from es_operation import ES

class NewsESPipeline(object):
    def __init__(self):
        self.news = {}

    def process_item(self, item, spider):
        def __init__(self):
        self.news = {}

    def store_revelance(self, item):
        self.news['url'] = item['url']
        self.news['content'] = item['content']
        self.news['title'] = item['title']
        self.news['pub_time'] = item['pub_time']
        self.news['repost_num'] = item['repost_num']
        self.news['like_num'] = item['like_num']
        self.news['comment_num'] = item['comment_num']
        self.news['media_sources'] = item['media_sources']
        # self.news['emotion'] = sentiment_pridict(item['content'], APP_CONF['config']['word_dict_path'], APP_CONF['config']['model_path'])
        self.news['emotion'] = 0
        self.news['hot'] = item['repost_num']+item['like_num']+item['comment_num']
        self.news['revelance'] = []
        ES.bulk(self.news, 'news_reve')

    def process_item(self, item, spider):
        # 过滤相似
        search_body = {
            "query":{
                "match":{
                    "title":item['title']
                }
            }
        }
        re = ES.get_by_query(search_body, 'news')
        is_update = False
        # if len(re) and re[0]["_score"] > 10 and item['url'] != re[0]['_source']['url']:
        if len(re) and re[0]["_score"] > 10:
            if len(re[0]['_source']['revelance']) > 0:
                for url in re[0]['_source']['revelance']:
                    if url == item['url']:
                        is_update = True
                        re[0]['_source']['revelance'].append(item['url'])
                        self.store_revelance(item)
                        # 查找上次插入时的热度
                        search_body = {
                            "query":{
                                "match":{
                                    "url":item['url']
                                }
                            }
                        }
                        re = ES.get_by_query(search_body, 'news_reve')
                        last_hot = re[0]['_source']['hot'] if len(re)>0 else 0
                        break
            else:
                is_update = True
                last_hot = 0
                re[0]['_source']['revelance'].append(item['url'])
                self.store_revelance(item)

            if is_update:
                    update_body = {
                       "conflicts": "proceed",
                       "script": {
                            "inline": '''ctx._source.revelance = params.revelance;
                                        ctx._source.hot = ctx._source.hot + params.hot''',
                            "params": {
                                "revelance": re[0]['_source']['revelance'],
                                "hot": item['like_num']+item['repost_num']+item['comment_num']-last_hot
                            },
                            "lang":"painless"
                        },
                      "query": {
                        "bool": {
                          "must": [
                            {
                              "match_phrase": {
                                "title": re[0]['_source']['title']
                              }
                            }
                          ]
                        }
                      }
                    }
                    ES.es.update_by_query(index="news", doc_type="news_type", body=update_body)

        self.news['url'] = item['url']
        self.news['content'] = item['content']
        self.news['title'] = item['title']
        self.news['pub_time'] = item['pub_time']
        self.news['repost_num'] = item['repost_num']
        self.news['like_num'] = item['like_num']
        self.news['comment_num'] = item['comment_num']
        self.news['media_sources'] = item['media_sources']
        # self.news['emotion'] = sentiment_pridict(item['content'], APP_CONF['config']['word_dict_path'], APP_CONF['config']['model_path'])
        self.news['emotion'] = 0
        self.news['hot'] = item['repost_num']+item['like_num']+item['comment_num']
        self.news['revelance'] = []
        ES.bulk(self.news, 'news')
        return item

    def close_spider(self, spider):
        ES.bulk(self.news, 'news', -1)


class UpdatePipeline(object):
    def __init__(self):
        self.news = {}

    def store_revelance(self, item):
        self.news['url'] = item['url']
        self.news['content'] = item['content']
        self.news['title'] = item['title']
        self.news['pub_time'] = item['pub_time']
        self.news['repost_num'] = item['repost_num']
        self.news['like_num'] = item['like_num']
        self.news['comment_num'] = item['comment_num']
        self.news['media_sources'] = item['media_sources']
        # self.news['emotion'] = sentiment_pridict(item['content'], APP_CONF['config']['word_dict_path'], APP_CONF['config']['model_path'])
        self.news['emotion'] = 0
        self.news['hot'] = item['repost_num']+item['like_num']+item['comment_num']
        self.news['revelance'] = []
        ES.bulk(self.news, 'news_reve')

    def process_item(self, item, spider):
        # 过滤相似
        search_body = {
            "query":{
                "match":{
                    "title":item['title']
                }
            }
        }
        re = ES.get_by_query(search_body, 'news')
        is_update = False
        # if len(re) and re[0]["_score"] > 10 and item['url'] != re[0]['_source']['url']:
        if len(re) and re[0]["_score"] > 10:
            if len(re[0]['_source']['revelance']) > 0:
                for url in re[0]['_source']['revelance']:
                    if url == item['url']:
                        is_update = True
                        re[0]['_source']['revelance'].append(item['url'])
                        self.store_revelance(item)
                        # 查找上次插入时的热度
                        search_body = {
                            "query":{
                                "match":{
                                    "url":item['url']
                                }
                            }
                        }
                        re = ES.get_by_query(search_body, 'news_reve')
                        last_hot = re[0]['_source']['hot'] if len(re)>0 else 0
                        break
            else:
                is_update = True
                last_hot = 0
                re[0]['_source']['revelance'].append(item['url'])
                self.store_revelance(item)

            if is_update:
                    update_body = {
                       "conflicts": "proceed",
                       "script": {
                            "inline": '''ctx._source.revelance = params.revelance;
                                        ctx._source.hot = ctx._source.hot + params.hot''',
                            "params": {
                                "revelance": re[0]['_source']['revelance'],
                                "hot": item['like_num']+item['repost_num']+item['comment_num']-last_hot
                            },
                            "lang":"painless"
                        },
                      "query": {
                        "bool": {
                          "must": [
                            {
                              "match_phrase": {
                                "title": re[0]['_source']['title']
                              }
                            }
                          ]
                        }
                      }
                    }
                    ES.es.update_by_query(index="news", doc_type="news_type", body=update_body)
        # 更新热度
        search_body = {
            "query":{
                "match_phrase":{
                    "url":item['url']
                }
            }
        }
        re = ES.get_by_query(search_body, 'news')
        if len(re):
            update_body = {
               "conflicts": "proceed",
               "script": {
                    "inline": '''ctx._source.comment_num = params.comment_num;
                                ctx._source.repost_num = params.repost_num;
                                ctx._source.like_num = params.like_num;
                                ctx._source.hot = params.hot + (params.hot - ctx._source.hot);''',
                                # 原有热度加上新增的热度
                    "params": {
                        "comment_num": item['comment_num'],
                        "repost_num": item['repost_num'],
                        "like_num": item['like_num'],
                        "hot": item['like_num']+item['repost_num']+item['comment_num']
                    },
                    "lang":"painless"
                },
              "query": {
                "bool": {
                  "must": [
                    {
                      "match_phrase": {
                        "url": item['url']
                      }
                    }
                  ]
                }
              }
            }
            ES.es.update_by_query(index="news", doc_type="news_type", body=update_body)
            return item

        self.news['url'] = item['url']
        self.news['content'] = item['content']
        self.news['title'] = item['title']
        self.news['pub_time'] = item['pub_time']
        self.news['repost_num'] = item['repost_num']
        self.news['like_num'] = item['like_num']
        self.news['comment_num'] = item['comment_num']
        self.news['media_sources'] = item['media_sources']
        # self.news['emotion'] = sentiment_pridict(item['content'], APP_CONF['config']['word_dict_path'], APP_CONF['config']['model_path'])
        self.news['emotion'] = 0
        self.news['hot'] = item['repost_num']+item['like_num']+item['comment_num']
        self.news['revelance'] = []
        ES.bulk(self.news, 'news')
        return item

    def close_spider(self, spider):
        ES.bulk(self.news, 'news', -1)


if __name__ == '__main__':
    a = NewsESPipeline()
    data = [{"title":"aaaaa", 
    "content":"test_a", 
    "url":"http://a", 
    "pub_time":"2017-05-02", 
    "media_sources":"a_a",
    "emotion":1,
    "hot":12,
    "revelance":["http://ra","http://rb"]},
    {"title":"b", 
    "content":"test_b", 
    "url":"http://body", 
    "pub_time":"2017-05-03", 
    "media_sources":"a_b",
    "emotion":0,
    "hot":5,
    "revelance":["http://rab","http://rbb"]}]
    a.process_item(data, None)