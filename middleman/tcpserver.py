import enum
import socket
import struct
import time

from middleman.util import fileobj_to_fd
from middleman.log import gen_log
from middleman.ioloop import IOLoop, PeriodicCallback
from middleman.netutil import bind_sockets, add_accept_handler
from middleman.iostream import IOStream
from middleman.platform.auto import set_close_exec


class ProtocolError(Exception):
    pass


class _TcpRelayHandler:
    class Stage(enum.IntEnum):
        INIT = 0,
        PROTOCOL = 0b1111,
        PROTOCOL_METHOD = 0b1001,
        PROTOCOL_CMD = 0b1010,
        PROTOCOL_CONNECTING = 0b1100,
        CONNECTING = 3,
        STREAM = 4

    class _StreamPair:
        def __init__(self, local, remote=None):
            # on behalf of the connection which we 'accept'
            self.local = IOStream(local)
            # connection to remote middleman server or web server"
            self.remote = None if remote is None else IOStream(remote)

    def __init__(self, server, local_socket, opt):
        self._server = server
        self._opt = opt
        self._io_loop = IOLoop.current()
        self._streams = self._StreamPair(local_socket)
        self._remote_address = None
        self._stage = self.Stage.INIT
        self._closed = False

        # time of the last interaction, used for timeout
        self._last_interaction = server.unixtime
        self._cron = PeriodicCallback(self._timeout_cron,
                                      opt.timeout * 1000, 0.1)

    def _timeout_cron(self):
        if (not self.closed() and
                time.time() - self._last_interaction > self._opt.timeout):
            self.stop()
            gen_log.error("Closing idle connection")

    def closed(self):
        return self._closed

    def start(self):
        self._cron.start()
        self._streams.local.add_io_state(IOLoop.READ, self._handle_events)
        if self._opt.client:
            self._stage = self.Stage.PROTOCOL_METHOD

    def stop(self):
        if self._closed:
            return
        self._closed = True
        # stop timer
        self._cron.stop()
        self._cron = None

        # remove handler from server
        self._server.remove_handler(self._streams.local.socket)

        # close connection
        if self._streams.local is not None:
            self._streams.local.close()
            self._streams.local = None
        if self._streams.remote is not None:
            self._streams.remote.close()
            self._streams.remote = None

        self._streams = None
        self._server = None

    def _handle_events(self, fileobj, events):
        if self.closed():
            gen_log.warning("Got events for closed stream %s", fileobj)
            return
        fd = fileobj_to_fd(fileobj)
        if fd == -1:
            gen_log.warning("Got events for closed socket %s", fileobj)
            return

        try:
            if events & IOLoop.READ:
                self._handle_read(fd, events)
            if self.closed():
                return
            if events & IOLoop.WRITE:
                self._handle_write(fd, events)
            if self.closed():
                return
            self._update_stream_pair_state()
        except Exception:
            gen_log.error("Uncaught exception, closing connection.",
                          exc_info=True)
            self.stop()

    @staticmethod
    def _update_stream_state(stream):
        if stream is not None:
            state = IOLoop.READ
            if stream.writing():
                state |= IOLoop.WRITE
            stream.add_io_state(state)

    def _update_stream_pair_state(self):
        self._update_stream_state(self._streams.local)
        self._update_stream_state(self._streams.remote)

    def _handle_read(self, fd, events):
        try:
            if fd == self._streams.local.socket.fileno():
                self._on_local_read()
            else:
                self._on_remote_read()
        except ProtocolError:
            self.stop()
        except OSError:
            self.stop()
        except Exception as e:
            gen_log.error("error on read: %s" % e)
            self.stop()

    def _on_local_read(self):
        nread = self._streams.local.handle_read()
        if nread is None:
            return
        if nread:
            self._update_last_interaction()
            if self._stage == self.Stage.STREAM:
                self._relay(self._streams.local, self._streams.remote)
            elif (self._stage == self.Stage.PROTOCOL_METHOD or
                  self._stage == self.Stage.PROTOCOL_CMD):
                self._parse_sock5()
        else:
            # if self._stage != self.Stage.STREAM:
            self.stop()

    def _on_remote_read(self):
        nread = self._streams.remote.handle_read()
        if nread is None:
            return
        if nread:
            self._update_last_interaction()
            if self._stage == self.Stage.STREAM:
                self._relay(self._streams.remote, self._streams.local)
        else:
            self.stop()

    def _handle_write(self, fd, events):
        try:
            if fd == self._streams.local.socket.fileno():
                self._on_local_write()
            else:
                self._on_remote_write()
        except Exception as e:
            gen_log.error("error on write: %s" % e)
            self.stop()

    def _on_local_write(self):
        count = self._streams.local.handle_write()
        if count > 0:
            self._update_last_interaction()

    def _on_remote_write(self):
        bytes = self._streams.remote.handle_write()
        if bytes > 0:
            self._update_last_interaction()

    def _update_last_interaction(self):
        self._last_interaction = self._server.unixtime

    def _parse_sock5(self):
        if self._stage == self.Stage.PROTOCOL_METHOD:
            self._parse_method()
        elif self._stage == self.Stage.PROTOCOL_CMD:
            self._parse_cmd()

    def _parse_method(self):
        """read version identifier/method selection message"""
        # +-----+----------+----------+
        # | VER | NMETHODS | METHODS |
        # +-----+----------+---------+
        # |  1  |     1   |1 to  255 |
        # +----+----------+----------+
        two_octets = 2  # VER + NMETHODS field
        stream = self._streams.local
        if stream.read_buffer_size < two_octets:
            return
        buf = stream.read_buffer
        buf_pos = stream.read_buffer_pos
        version = buf[buf_pos]
        if version != 0x05:
            gen_log.error("Unsupported SOCKS version %s", version)
            raise ProtocolError
        nmethods = buf[buf_pos + 1]
        total = two_octets + nmethods
        if stream.read_buffer_size < total:
            stream.read_target_bytes = total
            return
        # ================
        #  +----+--------+
        #  |VER | METHOD |
        #  +----+--------+
        #  | 1  |   1    |
        #  +----+--------+
        # ================
        response = bytearray(b'\x05')
        for i in range(nmethods):
            if buf[two_octets + i] == 0x00:  # NO AUTHENTICATION REQUIRED
                response += b'\x00'
                break
        else:
            response += b'\xff'  # NO ACCEPTABLE METHODS
        stream.write(response)
        if response[1]:
            # request failed
            gen_log.error("No supported SOCKS5 authentication method received")
            raise ProtocolError
        if stream.read_target_bytes is not None:
            stream.read_target_bytes = None
        stream.consume(total)
        self._stage = self.Stage.PROTOCOL_CMD

    def _parse_cmd(self):
        """parse client request details"""
        # +----+-----+-------+------+----------+----------+
        # |VER | CMD |  RSV  | ATYP | DST.ADDR | DST.PORT |
        # +----+-----+-------+------+----------+----------+
        # | 1  |  1  | X'00' |  1   | Variable |    2     |
        # +----+-----+-------+------+----------+----------+
        four_octets = 4  # VER + CMD + RSV + ATYP field
        stream = self._streams.local
        if stream.read_buffer_size < four_octets:
            return
        buf = stream.read_buffer
        buf_pos = stream.read_buffer_pos
        close_connection = True
        version, cmd, rsv, atyp = buf[buf_pos:buf_pos+4]
        buf_pos += 4
        resp = bytearray(b'\x05') # response version 5
        if version != 0x05:
            gen_log.error("Invalid SOCKS5 message version %s", version)
            resp += b'0x01' # general SOCKS server failure
        elif cmd == 0x01:  # CONNECT X'01'
            if atyp == 0x01:  # IP v4 address
                if stream.read_buffer_size < 4 + 4 + 2:
                    stream.read_target_bytes = 10
                    return
                else:
                    dest_addr = socket.inet_ntop(socket.AF_INET,
                                                 buf[buf_pos:buf_pos+4])
                    buf_pos += 4
                    dest_port = struct.unpack('>H', buf[buf_pos:buf_pos+2])
            elif atyp == 0x03: # DOMAINNAME
                if stream.read_buffer_size < 4 + 1:
                    stream.read_target_bytes = 5
                    return
                host_len = buf[buf_pos]
                total_octets = 4 + 1 + host_len + 2
                if stream.read_buffer_size < total_octets:
                    stream.read_target_bytes = total_octets
                    return
                stream.read_target_bytes = None
                buf_pos += 1
                dest_host_name = buf[buf_pos:buf_pos+host_len]
                buf_pos += host_len
                dest_port = struct.unpack('>H', buf[buf_pos:buf_pos + 2])
                self._remote_address = (socket.AF_UNSPEC,
                                        (dest_host_name.decode('idna'),
                                         dest_port[0]))
                self._streams.local.consume(total_octets)
                self._stage = self.Stage.CONNECTING
                self._io_loop.run_in_exector(
                    None, self.remote_gotconnected,
                    socket.create_connection, self._remote_address[1], 5)
        else:
            gen_log.error("Unsupported command %s", cmd)

    def remote_gotconnected(self, feature):
        if self.closed():
            return
        try:
            conn = feature.result()
            set_close_exec(conn.fileno())
            peer_addrinfo = conn.getpeername()
            local_addrinfo = conn.getsockname()
            gen_log.info("connected %s: %s:%s from %s:%s",
                         self._remote_address[1][0],
                         peer_addrinfo[0], peer_addrinfo[1],
                         local_addrinfo[0], local_addrinfo[1])
            if conn.family == socket.AF_INET:
                atyp = b'\x01'
            else:
                atyp = b'\x04'
            resp = bytearray(b'\x05\x00\x00')
            resp += atyp
            bnd_addr = socket.inet_pton(conn.family, local_addrinfo[0])
            bnd_port = struct.pack("!H", local_addrinfo[1])
            resp += bnd_addr
            resp += bnd_port
            self._streams.local.write(resp)
            self._streams.remote = IOStream(conn)
            self._streams.remote.add_io_state(IOLoop.READ, self._handle_events)
            self._stage = self.Stage.STREAM
        except Exception as e:
            gen_log.error("SOCK5 failed to connect '%s' (%s)",
                          self._remote_address[1][0], e)
            self.stop()
            return

    @staticmethod
    def _relay(src, dest):
        readbuf = src.read_buffer
        dest.write(readbuf[src.read_buffer_pos:src.read_buffer_size])
        src.consume(src.read_buffer_size)


class TCPServer:
    def __init__(self, opt):
        self._io_loop = IOLoop.current()
        self._sockets = {}  # fd -> socket object
        self._handlers = {}  # fd -> remove_handler callable
        self._pending_sockets = []
        self._started = False
        self._stopped = False
        self.unixtime = time.time()
        self._opt = opt
        self._cron = PeriodicCallback(self._server_cron, 1000)

    def add_sockets(self, sockets):
        """Makes this server start accepting connections on the given sockets.

        The ``sockets`` parameter is a list of socket objects such as
        those returned by `~tornado.netutil.bind_sockets`.
        `add_sockets` is typically used in combination with that
        method and `tornado.process.fork_processes` to provide greater
        control over the initialization of a multi-process server.
        """
        for sock in sockets:
            self._sockets[sock.fileno()] = sock
            self._handlers[sock.fileno()] = add_accept_handler(
                sock, self._handle_connection)

    def add_socket(self, sock):
        """Singular version of `add_sockets`.  Takes a single socket object."""
        self.add_sockets([sock])

    def bind(self, port, address=None, family=socket.AF_UNSPEC, backlog=128,
             reuse_port=False):
        """Binds this server to the given port on the given address.

        To start the server, call `start`. If you want to run this server
        in a single process, you can call `listen` as a shortcut to the
        sequence of `bind` and `start` calls.

        Address may be either an IP address or hostname.  If it's a hostname,
        the server will listen on all IP addresses associated with the
        name.  Address may be an empty string or None to listen on all
        available interfaces.  Family may be set to either `socket.AF_INET`
        or `socket.AF_INET6` to restrict to IPv4 or IPv6 addresses, otherwise
        both will be used if available.

        The ``backlog`` argument has the same meaning as for
        `socket.listen <socket.socket.listen>`. The ``reuse_port`` argument
        has the same meaning as for `.bind_sockets`.

        This method may be called multiple times prior to `start` to listen
        on multiple ports or interfaces.

        .. versionchanged:: 4.4
           Added the ``reuse_port`` argument.
        """
        sockets = bind_sockets(port, address=address, family=family,
                               backlog=backlog, reuse_port=reuse_port)
        if self._started:
            self.add_sockets(sockets)
        else:
            self._pending_sockets.extend(sockets)

    def start(self):
        """Starts this server in the `.IOLoop`.

        By default, we run the server in this process and do not fork any
        additional child process.

        If num_processes is ``None`` or <= 0, we detect the number of cores
        available on this machine and fork that number of child
        processes. If num_processes is given and > 1, we fork that
        specific number of sub-processes.

        Since we use processes and not threads, there is no shared memory
        between any server code.

        Note that multiple processes are not compatible with the autoreload
        module (or the ``autoreload=True`` option to `tornado.web.Application`
        which defaults to True when ``debug=True``).
        When using multiple processes, no IOLoops can be created or
        referenced until after the call to ``TCPServer.start(n)``.
        """
        assert not self._started
        self._started = True
        sockets = self._pending_sockets
        self._pending_sockets = []
        self.add_sockets(sockets)
        self._cron.start()

    def stop(self):
        """Stops listening for new connections.

        Requests currently in progress may still continue after the
        server is stopped.
        """
        if self._stopped:
            return
        self._stopped = True
        # stop timer
        self._cron.stop()
        self._cron = None

        for fd, sock in self._sockets.items():
            assert sock.fileno() == fd
            # Unregister socket from IOLoop
            self._handlers.pop(fd)()
            sock.close()

        # close connection(s) and release resources
        while self._handlers:
            fd, handler = self._handlers.popitem()
            handler()

    def _server_cron(self):
        self._update_cached_time()

    def _update_cached_time(self):
        self.unixtime = time.time()

    def add_handler(self, connection, handler):
        self._handlers[connection.fileno()] = handler

    def remove_handler(self, connection):
        self._handlers.pop(connection.fileno())

    def _handle_connection(self, connection, address):
        gen_log.info("Accepted client connection %s", address)
        relay = _TcpRelayHandler(self, connection, self._opt)
        self.add_handler(connection, relay.stop)
        relay.start()
