# -*- coding: utf-8 -*-

# from sentiment.sentiment_classify import sentiment_pridict
from utils.tools import APP_CONF
from es_operation import ES

class NewsESPipeline(object):
    def __init__(self):
        self.news = {}

    def process_item(self, item, spider):
        search_body = {
            "query":{
                "match":{
                    "title":item['title']
                }
            }
        }
        re = ES.get_by_query(search_body)
        if len(re) and re[0]["_score"] > 10:
            if re[0]['_source']['revelance']:
                re[0]['_source']['revelance'] = re[0]['_source']['revelance'].append((item['title'], item['url']))
            else:
                re[0]['_source']['revelance'] = []
            update_body = {
               "script": {
                    "inline": "ctx._source.revelance = params.revelance;ctx._source.hot = params.hot",
                    "params": {
                        "revelance": re[0]['_source']['revelance'],
                        "hot": len(re[0]['_source']['revelance'])
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
            return item

        self.news['title'] = item['title']
        self.news['pub_time'] = item['pub_time']
        self.news['content'] = item['content']
        self.news['url'] = item['url']
        self.news['repost_num'] = item['repost_num']
        self.news['like_num'] = item['like_num']
        self.news['comment_num'] = item['comment_num']
        self.news['media_sources'] = item['media_sources']
        # self.news['emotion'] = sentiment_pridict(item['content'], APP_CONF['config']['word_dict_path'], APP_CONF['config']['model_path'])
        self.news['emotion'] = 0
        self.news['hot'] = 0
        self.news['revelance'] = []

        ES.bulk(self.news)

        return item

    def close_spider(self, spider):
        ES.bulk(self.news, -1)


class UpdatePipeline(object):
    def __init__(self):
        self.weibo = {}

    def process_item(self, item, spider):
        search_body = {
            "query":{
                "match_phrase":{
                    "url":item['url']
                }
            }
        }
        re = ES.get_by_query(search_body)
        if len(re):
            update_body = {
               "script": {
                    "inline": "ctx._source.comment_num = params.comment_num;ctx._source.repost_num = params.repost_num;ctx._source.like_num = params.like_num;ctx._source.hot = params.hot;",
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

        self.weibo['url'] = item['url']
        self.weibo['content'] = item['content']
        self.weibo['title'] = item['title']
        self.weibo['pub_time'] = item['pub_time']
        self.weibo['repost_num'] = item['repost_num']
        self.weibo['like_num'] = item['like_num']
        self.weibo['comment_num'] = item['comment_num']
        self.weibo['media_sources'] = item['media_sources']
        # self.weibo['emotion'] = sentiment_pridict(item['content'], APP_CONF['config']['word_dict_path'], APP_CONF['config']['model_path'])
        self.weibo['emotion'] = 0
        self.weibo['hot'] = item['repost_num']+item['like_num']+item['comment_num']
        self.weibo['revelance'] = []
        ES.bulk(self.weibo)
        return item

    def close_spider(self, spider):
        ES.bulk(self.weibo, -1)


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