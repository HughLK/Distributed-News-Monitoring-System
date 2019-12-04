# -*- coding: utf-8 -*-

import datetime
from events_spider.utils.tools import LOGGER, APP_CONF, SCHEDULER
from rpc_client import RpcClient
from SimpleXMLRPCServer import SimpleXMLRPCServer

CLIENT = RpcClient()

def callback():
    LOGGER.info("New Master Confirmed.")
    CLIENT.call()
    # SCHEDULER.add_job(CLIENT.call, 'interval', id='call', minutes=APP_CONF['config']['crawl_frequency'], next_run_time=datetime.datetime.now())
    LOGGER.info(SCHEDULER.get_jobs())


server = SimpleXMLRPCServer((APP_CONF['config']['localhost'], 8888))
server.register_function(callback, "call")
LOGGER.info("Awaiting Being Eelcted.")
server.serve_forever()