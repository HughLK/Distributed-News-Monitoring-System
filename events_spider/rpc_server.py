# -*- coding: utf-8 -*-

import os
import pika
import time
import datetime
import random
import simplejson
from urlparse import urlsplit
from events_spider.utils.tools import LOGGER, APP_CONF, REDIS, SCHEDULER
from events_spider.utils.MyDBUtils import MYSQL, SqlComment
from events_spider.utils.redis_lock import RedisLock
from rpc_client import RpcClient


CLIENT = RpcClient()
LOCK = RedisLock(REDIS)
SPIDER_TYPES_MAPPING = {
    "weibo":"weibo",
    "zhihu":"zhihu"
}


REDIS_LOCK = RedisLock(REDIS)
credentials = pika.PlainCredentials('admin', 'admin')
connection = pika.BlockingConnection(pika.ConnectionParameters(
                                                                host=APP_CONF['mq']['url'], 
                                                                port=APP_CONF['mq']['port'], 
                                                                virtual_host='/', 
                                                                credentials=credentials,
                                                                heartbeat_interval=0
                                                                )
                                                    )

channel = connection.channel()
channel.exchange_declare(exchange='fanout_start_urls',
                        exchange_type='fanout')

result = channel.queue_declare(exclusive=True)
queue_name = result.method.queue
# channel.queue_declare(queue='rpc_queue')

channel.queue_bind(exchange='fanout_start_urls',
                       queue=queue_name,
                       routing_key='')


def crawl(body):
    # shell识别空格后为新的spider参数，去掉空格
    # os.system('scrapy crawl news -a start_urls=%s' % str(urls).replace(' ', ''))

    # proxy_flag = True if body=='true' else False
    # LOGGER.info("Enable Proxy: %s." % proxy_flag)

    while len(REDIS.lrange("start_urls", 0, -1)):
        SPIDER_TYPES = "news"
        url = REDIS.lpop('start_urls')
        for k, v in SPIDER_TYPES_MAPPING.items():
            if k in url:
                SPIDER_TYPES = v
                break

            LOGGER.info("Strat Crawling %s." % SPIDER_TYPES)
            os.system('scrapy crawl %s -a start_url="%s"' % (SPIDER_TYPES, url))
            LOGGER.info("Crawling %s Finish." % SPIDER_TYPES)
            LOGGER.info("Awaiting RPC requests")


def on_request(ch, method, props, body):
    if props.correlation_id == 'notify' and REDIS.get('master_ip') == APP_CONF['config']['localhost']:
        LOGGER.info("New Master Confirmed.")
        CLIENT.call_crawl()
    elif props.correlation_id == 'crawl':
        LOGGER.info("Starting Crawling.")
        crawl(body)


def register():
    MYSQL.execute(SqlComment.CREATE.format(**dict(table='tbl_spiders', 
                                                fields='ip CHAR(20) PRIMARY KEY')))

    nodes = MYSQL.select(SqlComment.SELECT.format(**dict(field='ip', table='tbl_spiders')))
    nodes = map(lambda x:x['ip'], nodes)
    if APP_CONF['config']['localhost'] not in nodes:
        MYSQL.execute(SqlComment.INSERT.format(**dict(table='tbl_spiders',
                                                    fields='ip',
                                                    values='"'+APP_CONF['config']['localhost']+'"')))
        LOGGER.info("Register node:"+APP_CONF['config']['localhost'])
    MYSQL.commit()


def elect():
    host = REDIS.get('master_ip')
    if host == APP_CONF['config']['localhost']:
        LOGGER.info("Ping "+host+' Success.')
        REDIS.lpush('spiders_vote', APP_CONF['config']['localhost']+':0')
    if host != None:
        # host = host[0]['ip']
        result = os.system('ping -c 2 '+ host)
        if result == 0:
            LOGGER.info("Ping "+host+' Success.')
            REDIS.lpush('spiders_vote', APP_CONF['config']['localhost']+':0')
        else:
            LOGGER.info("Ping "+host+' Failed.')
            REDIS.lpush('spiders_vote', APP_CONF['config']['localhost']+':1')

        time.sleep(10)
        votes = REDIS.lrange('spiders_vote', 0, -1)
        ips = []
        score = 0
        for v in votes:
            ip, vote = v.split(':')
            ips.append(ip)
            score += int(vote)

        # 锁一定时间后自动释放
        if LOCK.accquire_lock():
            LOGGER.info('Aqquiring Lock.')
            # 一半以上的node投票ping不到master
            if score >= len(ips)/2: 
                # 从投票成功的主机中随机选择一台作为master
                master = random.choice(ips)
                LOGGER.info('Electing master:'+master)
                REDIS.set('master_ip', master)
                # 通知new master
                CLIENT.call_notify()
                REDIS.delete('spiders_vote')
                # LOCK.relese_lock()
        LOGGER.info('Electing News Master Finished.')
    else:
        ips = MYSQL.select(SqlComment.SELECT.format(**dict(field='ip', table='tbl_spiders')))
        ips = [i['ip'] for i in ips]
        time.sleep(10)
        if LOCK.accquire_lock():
            LOGGER.info('Aqquiring Lock.')
            master = random.choice(ips)
            LOGGER.info('Electing master:'+master)
            REDIS.set('master_ip', master)
            CLIENT.call_notify()
            REDIS.delete('spiders_vote')
            # LOCK.relese_lock()
        LOGGER.info('Electing News Master Finished.')

def monitor():
    start_time = REDIS.get('start_crawl_time')
    if start_time != None:
        start_time = datetime.datetime.strptime(start_time, "%Y-%m-%dT%H:%M:%S")
        delta = datetime.timedelta(minutes=APP_CONF['config']['crawl_frequency'] * 1.5)
        deadline = start_time + delta

    if start_time == None or datetime.datetime.now() > deadline:
        elect()

channel.basic_qos(prefetch_count=1)
channel.basic_consume(on_request, queue=queue_name, no_ack=True)

register()
# SCHEDULER.add_job(monitor, 'interval', id='monitor', minutes=10, next_run_time=datetime.datetime.now())
# SCHEDULER.start()
monitor()
LOGGER.info("Awaiting RPC requests")
channel.start_consuming()