# -*- coding:utf-8 -*- 

from events_spider.utils.tools import LOGGER, APP_CONF, REDIS
from events_spider.utils.MyDBUtils import MYSQL, SqlComment

# 将start_url 存储到redis中的redis_key中，让爬虫去爬取 
def init_start_urls(redis_host, redis_port, redis_key):
	# 先将redis中的urls全部清空  
	REDIS.delete('start_urls')
	start_urls = [url_dict["url"] for url_dict in MYSQL.select("select * from tbl_data_sources")]

	with REDIS.pipeline(transaction=False) as p:
	    for url in start_urls:
	    	# print("insert %s" % url)
	    	p.lpush(redis_key, url)
	    p.execute() 

if __name__ == '__main__':
	# init_start_urls("192.168.1.129", 6379, "start_urls:news", "start_urls:generic")
	init_start_urls("192.168.1.129", 6379, "start_urls")