# -*- coding: utf-8 -*-

class RedisLock(object):
    DEL_SCRIPT = '''
        if redis.call('get', KEYS[1]) == ARGV[1] then
        return redis.call('del', KEYS[1])
        else
            return 0
        end
        '''

    def __init__(self, conn, ttl=100):
        self.conn = conn
        self.lock_key = "lock"
        self.lock_value = "lock_val"
        # 锁过期时间s
        self.ttl = ttl
 
    def accquire_lock(self):
        ret = self.conn.set(self.lock_key, self.lock_value, ex=self.ttl, nx=True)
        return ret

    def relese_lock(self):
        self.conn.eval(self.DEL_SCRIPT, 1, self.lock_key, self.lock_value)