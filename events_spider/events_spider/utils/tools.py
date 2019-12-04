# -*-coding=UTF-8 -*-
import re
import pika
import redis
import json
import logging
import traceback
from elasticsearch import Elasticsearch
from apscheduler.schedulers.background import BackgroundScheduler

CONF_INFO = '../conf/conf.json'

def getAppConf():
    ret = None
    fp = open(CONF_INFO, 'r')
    if not fp:
        print('the conf info file open error! %s' % CONF_INFO)
        return None
    try:
        ret = json.load(fp)
    except:
        print(str(traceback.format_exc()))
    finally:
        fp.close()
    return ret

APP_CONF = getAppConf()

def getCostumLogger():
    # logging设置level会不打印时间
    # logging.basicConfig(level=logging.NOTSET)
    # 设置logging，在终端打印
    logging.basicConfig(format='%(asctime)s [%(name)s] %(levelname)s: %(message)s')

    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    error_log_handler = logging.FileHandler(APP_CONF['config']['spider_logs_path_err'], mode='a')
    error_log_handler.setLevel(logging.ERROR)
    formatter = logging.Formatter('%(asctime)s [%(name)s] %(levelname)s: %(message)s')
    error_log_handler.setFormatter(formatter)

    all_log_handler = logging.FileHandler(APP_CONF['config']['spider_logs_path_all'], mode='a')
    all_log_handler.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s [%(name)s] %(levelname)s: %(message)s')
    all_log_handler.setFormatter(formatter)
    
    logger.addHandler(error_log_handler)
    logger.addHandler(all_log_handler)
    return logger
    
LOGGER = getCostumLogger()
REDIS = redis.Redis(host=APP_CONF['redis']['host'], port=APP_CONF['redis']['port'], db=0)
SCHEDULER = BackgroundScheduler()