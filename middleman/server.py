import os
import sys
import time

# sys.argv[1:] = '-c /home/mercury/python/middleman/middleman.conf -V'.split()
# sys.argv[1:] = '-d start -f middleman.pid -V'.split()

from middleman.log import gen_log
from middleman.log import Log
from middleman import options
from middleman.platform import auto
from middleman.util import exit_prog


def main():
    auto.daemonize(options.options)

    try:
        Log().init_logging(options.options)
    except Exception as ex:
        exit_prog(1, 'ERROR: %s' % ex)


if __name__ == "__main__":

    print(sys.argv)

    main()
    n = 0
    while True:
        time.sleep(1)
        n += 1
        if n > 300:
            sys.exit(0)
        gen_log.info("sleep %d" % n)
        # print("bye")
