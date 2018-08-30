from middleman.log import Log
from middleman.options import options, display_options_info
from middleman.platform import auto
from middleman.util import exit_prog
from middleman import ioloop
from middleman.tcpserver import TCPServer


def main():
    display_options_info(options)
    auto.daemonize(options)
    try:
        Log().init_logging(options)
    except Exception as exc:
        exit_prog(1, 'ERROR: %s' % exc)

    event_loop = ioloop.IOLoop.current()
    server = TCPServer(options)
    server.bind(options.local_port, options.local)
    server.start()
    event_loop.start()


if __name__ == "__main__":
    main()
