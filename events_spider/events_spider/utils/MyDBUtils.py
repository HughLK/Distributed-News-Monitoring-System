# -*- coding:utf-8 -*-
from DBUtils.PooledDB import PooledDB
import pymysql
import time
import traceback
import tools

APP_CONF = tools.getAppConf()
MYSQL_HOST = APP_CONF['db']['host']
MYSQL_PORT = APP_CONF['db']['port']
MYSQL_USER = APP_CONF['db']['user']
MYSQL_PASSWD = APP_CONF['db']['pwd']
MYSQL_DB = APP_CONF['db']['dbname']


class MysqlDBUtils(object):
    def __init__(self):
        self.pool = PooledDB(creator=pymysql,
                             mincached=1,
                             maxcached=20,
                             host=MYSQL_HOST,
                             port=MYSQL_PORT,
                             user=MYSQL_USER,
                             passwd=MYSQL_PASSWD,
                             db=MYSQL_DB,
                             use_unicode=False,
                             charset="utf8",
                             cursorclass=pymysql.cursors.DictCursor)
        self.retry = 3
        self.conn = self.getConn()
        self.cursor = self.conn.cursor()

    def getConn(self):
        """获得数据库连接"""
        flag = True
        num = 0
        conn = None
        while flag:
            try:
                conn = self.pool.connection()
                flag = False
            except Exception as e:
                print(e)
                time.sleep(0.5)
                num += 1
                if num > self.retry:
                    flag = False
                    raise Exception("Can not connect MYSQL")
        return conn

    def close(self):
        """关闭游标，关闭数据库连接"""
        try:
            if self.cursor:
                self.cursor.close()
            if self.conn:
                self.conn.close()
        except Exception as e:
            print str(e)
            traceback.print_exc()

    def select(self, sqlcomment, params=None):
        """执行查询操作"""
        try:
            if params:
                self.cursor.execute(sqlcomment, params)
            else:
                self.cursor.execute(sqlcomment)
            data = self.cursor.fetchall()
            return data
        except Exception as e:
            print str(e)
            traceback.print_exc()
            raise Exception(traceback.format_exc())

    def commit(self):
        """提交操作"""
        self.conn.commit()

    def rollback(self):
        """回滚操作"""
        self.conn.rollback()

    def execute(self, sqlcomment, params=None):
        """执行插入，修改等sql操作"""
        try:
            if params:
                self.cursor.execute(sqlcomment, params)
            else:
                self.cursor.execute(sqlcomment)
            self.commit()
        except Exception as e:
            self.rollback()
            print str(e)
            traceback.print_exc()
            raise Exception(traceback.format_exc())


class SqlComment(object):
    SELECT = """
            SELECT
                {field}
            FROM
                {table};
        """

    INSERT = """
            INSERT INTO {table}
            ({fields})
            VALUES
            ({values});
        """

    CREATE = """
            CREATE TABLE IF NOT EXISTS {table} 
            (
             {fields}
             );
        """

    SELECT_WHERE = """
            SELECT
                {field}
            FROM
                {table}
            WHERE
                {query};
        """

MYSQL = MysqlDBUtils()