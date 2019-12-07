# -*- coding: utf-8 -*-
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
from utils.tools import LOGGER, APP_CONF

news_mapping = {
	"mappings":{
		"news":{
			"properties":{
				"title":{
					"type":"text",
					"analyzer":"ik_max_word"				
				},
				"content":{
					"type":"text",
					"analyzer":"ik_max_word"				
				},
				"url":{
					"type":"keyword"				
				},
				"pub_time":{
					"type":"date"				
				},
				"media_sources":{
					"type":"keyword"				
				},
				"like_num":{
					"type":"integer"
				},
				"comment_num":{
					"type":"integer"
				},
				"repost_num":{
					"type":"integer"
				},
				"emotion":{
					"type": "integer"
				},
				"hot":{
					"type": "integer"
				},
				"revelance":{
					"type": "keyword"
				}
			}
		}
	}
}

news_reve_mapping = {
	"mappings":{
		"news_reve":{
			"properties":{
				"title":{
					"type":"text",
					"analyzer":"ik_max_word"				
				},
				"content":{
					"type":"text",
					"analyzer":"ik_max_word"				
				},
				"url":{
					"type":"keyword"				
				},
				"pub_time":{
					"type":"date"				
				},
				"media_sources":{
					"type":"keyword"				
				},
				"like_num":{
					"type":"integer"
				},
				"comment_num":{
					"type":"integer"
				},
				"repost_num":{
					"type":"integer"
				},
				"emotion":{
					"type": "integer"
				},
				"hot":{
					"type": "integer"
				},
				"revelance":{
					"type": "keyword"
				}
			}
		}
	}
}

INDEX = "news"
TYPE = "news_type"
INDEX2 = "news_reve"
TYPE2 = "news_reve_type"

class ESOp(object):
	ACTIONS = []

	def __init__(self):
		self.es = Elasticsearch(hosts=APP_CONF["es"]["url"]+":"+str(APP_CONF["es"]["port"]))
		self.init()
	
	def init(self):
		if not self.es.indices.exists(index=INDEX):
			status = self.es.indices.create(index=INDEX, body=news_mapping)
		if not self.es.indices.exists(index=INDEX2):
			status = self.es.indices.create(index=INDEX2, body=news_reve_mapping)

	def bulk(self, d, index, num=1):
		if not len(d):
			return
		action = {
			"_index":index,
			"_type":index+'_type',
			"_source":{
				"title":d["title"],
				"content":d["content"],
				"url":d["url"],
				"pub_time":d["pub_time"],
				"media_sources":d["media_sources"],
				"like_num":d["like_num"],
				"comment_num":d["comment_num"],
				"repost_num":d["repost_num"],
				"emotion":d["emotion"],
				"hot":d["hot"],
				"revelance":d["revelance"]
			}
		}

		self.ACTIONS.append(action)
		if num > 0 and len(self.ACTIONS) == num:
			success, _ = bulk(self.es, self.ACTIONS[:100], index=index, raise_on_error=True)
			LOGGER.info('es bulk:'+str(success))
			self.ACTIONS = self.ACTIONS[100:]
		elif num < 0 and len(self.ACTIONS) > 0:
			success, _ = bulk(self.es, self.ACTIONS, index=index, raise_on_error=True)
			LOGGER.info('es bulk:'+str(success))

	def get_by_query(self, body, index):
		re = self.es.search(index=index, doc_type=index+'_type', body=body)
		return re['hits']['hits']

ES = ESOp()
# def process_item(item):
# 	search_body = {
#         "query":{
#             "match":{
#                 "title":item['title']
#             }
#         }
# 	}

# 	re = ES.get_by_query("news", "news_type", search_body)
# 	if len(re) and re[0]["_score"] > 10:
# 	    re[0]['_source']['revelance'].append((item['title'], item['url']))
# 	    update_body = {
# 	       "script": {
# 	            "inline": "ctx._source.revelance = params.revelance;ctx._source.hot = params.hot",
# 	            "params": {
# 	                "revelance": re[0]['_source']['revelance'],
# 	                "hot": len(re[0]['_source']['revelance'])
# 	            },
# 	            "lang":"painless"
# 	        },
# 	      "query": {
# 	        "bool": {
# 	          "must": [
# 	            {
# 	              "match_phrase": {
# 	                "title": re[0]['_source']['title'],
# 	              }
# 	            }
# 	          ]
# 	        }
# 	      }
# 	    }
# 	    ES.es.update_by_query(index="news", doc_type="news_type", body=update_body)

if __name__ == '__main__':
	# data = [{"title":"a", 
	# "content":"test_a", 
	# "url":"http://a", 
	# "pub_time":"2017-05-02", 
	# "media_sources":"a_a",
	# "emotion":1,
	# "hot":12,
	# "revelance":["http://ra","http://rb"]},
	# {"title":"b", 
	# "content":"test_b", 
	# "url":"http://body", 
	# "pub_time":"2017-05-03", 
	# "media_sources":"a_b",
	# "emotion":0,
	# "hot":5,
	# "revelance":["http://rab","http://rbb"]}]
	# ES.bulk(data, 'news', -1)

	# body = {"query":{
	# 			"match":{
	# 				"title":"a"
	# 			}
	# }}
	# a = ES.get_by_query("news", "news_type", body)
	# print(a['hits']['hits'])

	data = {"title":"a", 
	"content":"test_a", 
	"url":"http://aaaaa", 
	"pub_time":"2017-05-02", 
	"media_sources":"a_a",
	"emotion":1,
	"hot":12,
	"revelance":["http://rc","http://rb"]}
	# process_item(data)