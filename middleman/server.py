from middleman.log import Log
from middleman.options import options, display_options_info
from middleman.sysplatform import auto
from middleman.util import exit_prog
from middleman import ioloop
from middleman.tcpserver import TCPServer
from middleman.cachedtime import CachedTime
from middleman.dnscache import DnsCache


class Middleman:
    def __init__(self):
        auto.daemonize(options)
        Log().init_logging(options)
        self.event_loop = ioloop.IOLoop.current()
        self.ct = CachedTime()
        self.dc = DnsCache()
        self.tcp_server = TCPServer(options, self)

    def start(self):
        self.ct.start()
        self.tcp_server.bind(options.local_port, options.local)
        self.tcp_server.start()
        self.event_loop.start()


def main():
    print(sys.argv)
    display_options_info(options)
    try:
        server = Middleman()
        server.start()
    except Exception as exc:
        exit_prog(1, 'ERROR: %s' % exc)


if __name__ == "__main__":
    main()
