# import os
import errno
import functools
from multiprocessing import Queue
import socket
import sys
import threading
import time

# sys.argv[1:] = '-c /home/mercury/python/middleman/middleman.conf -V'.split()
sys.argv[1:] = '-B --logging=info'.split()

from middleman.log import gen_log
from middleman.log import Log, logger_thread
from middleman import options
from middleman.platform import auto
from middleman.util import exit_prog
from middleman import ioloop



def hello():
    n = 0
    while True:
        time.sleep(1)
        n += 1
        if n > 300:
            break
        gen_log.info("sleep %d" % n)
        # print("bye")


def pr_hello(connection, a, b):
    loop = ioloop.IOLoop.current()
    gen_log.error("Hello")
    gen_log.error(" %s, %s", a, b)
    time.sleep(2)
    data = a.recv(4096)
    if not data:
        loop.remove_handler(a)
        a.close()
        return
    connection.sendall(data)


def write_data(remote, a, b):
    loop = ioloop.IOLoop.current()
    data = a.recv(4096)
    if not data:
        loop.remove_handler(a)
        a.close()
        return
    remote.sendall(data)


def handle_connection(connection, address):
    loop = ioloop.IOLoop.current()

    sock_remote = socket.socket(socket.AF_INET, socket.SOCK_STREAM, 0)
    sock_remote.connect(("23.106.143.104","8888"))

    call_read = functools.partial(pr_hello, connection)
    loop.add_handler(sock_remote, call_read, loop.READ)
    call_write = functools.partial(write_data, sock_remote)
    loop.add_handler(connection, write_data, ioloop.EVENT_READ)



def connection_ready(sock, fd, events):
    #while True:
    try:
        connection, address = sock.accept()
    except socket.error as e:
        if e.args[0] not in (errno.EWOULDBLOCK, errno.EAGAIN):
            raise
        return
    connection.setblocking(0)
    handle_connection(connection, address)


def main():
    print(sys.argv)
    auto.daemonize(options.options)

    try:
        Log().init_logging(options.options)
    except Exception as ex:
        exit_prog(1, 'ERROR: %s' % ex)

    n = 0
    q = Queue()
    lp = threading.Thread(target=logger_thread, args=(q,))
    lp.start()

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM, 0)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.setblocking(0)
    sock.bind(("", 1080))
    sock.listen(128)


    event_loop = ioloop.IOLoop.current()
    callback = functools.partial(connection_ready, sock)
    event_loop.add_handler(sock, callback, ioloop.EVENT_READ)

    event_loop.start()
    q.put(None)
    lp.join()


if __name__ == "__main__":
    main()
