import time
from collections import OrderedDict
from collections.abc import MutableMapping

from middleman.cachedtime import CachedTime
from middleman.util import singleton
from middleman.ioloop import IOLoop


class DnsItem:
    def __init__(self, deadline, value):
        self.deadline = deadline
        self.value = value
        self.times = 1


@singleton
class DnsCache(MutableMapping):
    def __init__(self, expire=90):
        self._expire = expire
        self._io_loop = IOLoop.current()
        self._cache = OrderedDict()
        self._cachedtime = CachedTime()

    def __new__(cls):
        return object.__new__(cls)

    def __getitem__(self, key):
        return self._cache[key]

    def __setitem__(self, key, value):
        deadline = self._cachedtime.unixtime + self._expire
        try:
            item = self._cache[key]
            item.deadline = deadline
            item.value = value
            item.times += 1
            self._cache.move_to_end(key)
        except KeyError:
            self._cache[key] = DnsItem(deadline, value)
            if len(self._cache) == 1:
                self._timeout = self._io_loop.add_timeout(deadline,
                                                          self._cron)

    def __delitem__(self, key):
        del self._cache[key]
        if not self._cache and self._timeout is not None:
            self._io_loop.remove_timeout(self._timeout)
            self._timeout = None

    def _cron(self):
        if self._cache:
            now = time.time()
            while self._cache:
                it = iter(self._cache)
                key = next(it)
                item = self._cache[key]
                if item.deadline > now:
                    next_timeout = max(now + 1, item.deadline)
                    self._timeout = self._io_loop.add_timeout(next_timeout,
                                                              self._cron)
                    break
                else:
                    del self._cache[key]
            else:
                self._timeout = None

    def __iter__(self):
        return iter(self._cache)

    def __len__(self):
        return len(self._cache)

    def __contains__(self, key):
        return key in self._cache

    def keys(self):
        return self._cache.keys()

    def items(self):
        return self._cache.items()

    def values(self):
        return self._cache.values()

    def __repr__(self):
        return repr(self._cache)

    def __str__(self):
        return str(self._cache)
