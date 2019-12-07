from utils.tools import REDIS, LOGGER
from scrapy.dupefilters import BaseDupeFilter
from scrapy.utils.request import request_fingerprint

class RedisDupeFilter(BaseDupeFilter):
    logger = LOGGER
    PATH = '../tmp/url_fingerprints.tmp'

    def __init__(self, server, key, debug=False):
        self.server = REDIS
        self.key = key
        self.debug = debug
        self.logdupes = True
        with open(self.PATH, 'w+') as f:
            value = f.read()
            if value:
                self.server.restore(self.key, ttl=0, value=value)

    def request_seen(self, request):
        fp = self.request_fingerprint(request)
        # This returns the number of values added, zero if already exists.
        added = self.server.sadd(self.key, fp)
        return added == 0

    def request_fingerprint(self, request):
        return request_fingerprint(request)

    def close(self, reason=''):
        with open(self.PATH, 'w') as f:
            if self.server.smembers(self.key) != None:
                f.write(self.server.dump(self.key))

    @classmethod
    def from_settings(cls, settings):
        server = REDIS
        key = settings.get('DUPEFILTER_KEY', 'url_fingerprints')
        debug = settings.getbool('DUPEFILTER_DEBUG')
        return cls(server, key=key, debug=debug)

    def log(self, request, spider):
        """Logs given request.

        Parameters
        ----------
        request : scrapy.http.Request
        spider : scrapy.spiders.Spider

        """
        if self.debug:
            msg = "Filtered duplicate request: %(request)s"
            self.logger.debug(msg, {'request': request}, extra={'spider': spider})
        elif self.logdupes:
            msg = ("Filtered duplicate request %(request)s"
                   " - no more duplicates will be shown"
                   " (see DUPEFILTER_DEBUG to show all duplicates)")
            self.logger.debug(msg, {'request': request}, extra={'spider': spider})
            self.logdupes = False