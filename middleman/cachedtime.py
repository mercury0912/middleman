import time

from middleman.util import singleton
from middleman.ioloop import PeriodicCallback


@singleton
class CachedTime:
    def __new__(cls):
        return object.__new__(cls)

    def __init__(self, period=1*1000):
            self._running = False
            self.unixtime = time.time()
            self._cron = PeriodicCallback(self._update_cached_time, period)

    def start(self):
        if self._running:
            return
        self._running = True
        self._cron.start()

    def stop(self):
        if not self._running:
            self._running = False
            self._cron.stop()
            self._cron = None

    def _update_cached_time(self):
        self.unixtime = time.time()
