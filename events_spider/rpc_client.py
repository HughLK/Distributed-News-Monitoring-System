# -*- coding: utf-8 -*-

import pika
import uuid
import time
import datetime
import os
from apscheduler.schedulers.blocking import BlockingScheduler
from events_spider.utils.tools import LOGGER, APP_CONF, REDIS
from events_spider.utils.MyDBUtils import MYSQL, SqlComment
from redis_init import init_start_urls

# SCHEDULER = BlockingScheduler()

class RpcClient(object):
    def __init__(self):
        self.SCHEDULER = BlockingScheduler()
        self.credentials = pika.PlainCredentials('admin', 'admin')
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(
                                                                            host=APP_CONF['mq']['url'], 
                                                                            port=APP_CONF['mq']['port'], 
                                                                            virtual_host='/', 
                                                                            credentials=self.credentials,
                                                                            heartbeat_interval=1200
                                                                            )
                                                    )

        self.channel = self.connection.channel()

        # 定义exchange
        self.channel.exchange_declare(exchange='fanout_start_urls',
                                    exchange_type='fanout')

        # 定义reply queue，从中获取爬虫返回的结果
        result = self.channel.queue_declare(exclusive=True)
        self.callback_queue = result.method.queue


    def call_crawl(self):
        if not self.check_master():
            if self.SCHEDULER.state == 1 and self.SCHEDULER.get_job('crawl') != None:
                # self.SCHEDULER.remove_job('crawl')
                self.SCHEDULER.shutdown()
            return

        REDIS.set('start_crawl_time', datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%S'))

        LOGGER.info("Initing start urls.")
        init_start_urls(APP_CONF['redis']['host'], APP_CONF['redis']['port'],
                        "start_urls")

        LOGGER.info("Requesting for spiders.")

        self.channel.basic_publish(
                                    exchange='fanout_start_urls',
                                    properties=pika.BasicProperties(
                                         # reply_to = self.callback_queue,
                                         correlation_id = 'crawl',
                                         ),
                                    routing_key='',
                                    body=''
                                    )


    def call_crawl_scheduel(self):
        self.SCHEDULER.add_job(self.call_crawl, 'interval', id='crawl', minutes=APP_CONF['config']['crawl_frequency'], next_run_time=datetime.datetime.now())
        self.SCHEDULER.start()

    def call_notify(self):
        self.channel.basic_publish(
                                    exchange='fanout_start_urls',
                                    properties=pika.BasicProperties(
                                         correlation_id = 'notify',
                                         ),
                                    routing_key='',
                                    body=''
                                    )
                                    

    def check_master(self):
        master_ip = REDIS.get('master_ip')
        if master_ip == None or master_ip != APP_CONF['config']['localhost']:
            return False
        return True
    
if __name__ == '__main__':
   rpc = RpcClient()
   # SCHEDULER.add_job(rpc.call, 'interval', minutes=APP_CONF['config']['crawl_frequency'], next_run_time=datetime.datetime.now())
   # SCHEDULER.start()