import enum
import socket
import struct
import time

from middleman.util import fileobj_to_fd
from middleman.log import gen_log
from middleman.ioloop import IOLoop, PeriodicCallback
from middleman.netutil import bind_sockets, add_accept_handler
from middleman.iostream import IOStream, StreamConnresetError
from middleman.platform.auto import set_close_exec


class ProtocolError(Exception):
    pass


class _TcpRelayHandler:
    class Stage(enum.IntEnum):
        INIT = 0,
        SOCKS5_METHOD = 1,
        SOCK5_CMD = 2,
        CONNECTING = 3,
        STREAM = 4

    SOCKET_TIMEOUT = 5.0

    class _StreamPair:
        def __init__(self, local, remote=None):
            # on behalf of the connection which we 'accept'
            self.local = IOStream(local)
            # connection to remote middleman server or web server"
            self.remote = None if remote is None else IOStream(remote)

        def close(self):
            if self.local is not None:
                self.local.close()
                self.local = None
            if self.remote is not None:
                self.remote.close()
                self.remote = None

    def __init__(self, server, local_socket, opt):
        self._server = server
        self._opt = opt
        self._io_loop = IOLoop.current()
        self._streams = self._StreamPair(local_socket)
        self._remote_address = None
        self._stage = self.Stage.INIT
        self._closed = False

        if not opt.server:
            self._remote_address = (socket.AF_INET,
                                    (opt.remote, opt.remote_port))
        # time of the last interaction, used for timeout
        self._last_interaction = server.unixtime
        self._cron = PeriodicCallback(self._timeout_cron,
                                      opt.timeout * 1000, 0.1)

    def _timeout_cron(self):
        if (not self.closed() and
                time.time() - self._last_interaction > self._opt.timeout):
            self.stop()
            gen_log.info("Closing idle connection")

    def closed(self):
        return self._closed

    def start(self):
        self._cron.start()
        self._streams.local.add_io_state(IOLoop.READ, self._handle_events)
        if self._opt.server:
            self._stage = self.Stage.SOCKS5_METHOD
        else:
            self._stage = self.Stage.CONNECTING

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
        self._streams.close()
        self._streams = None
        self._server = None

    def _handle_events(self, fileobj, events):
        if self.closed():
            gen_log.warning("Got events for closed stream %s", fileobj)
            return
        fd = fileobj_to_fd(fileobj)
        assert fd != -1
        if fd == -1:
            gen_log.warning("Got events for closed socket %s", fileobj)
            return

        islocal = self._islocal(fd)
        try:
            if events & IOLoop.READ:
                self._handle_read(islocal)
            if self.closed():
                return
            if events & IOLoop.WRITE:
                self._handle_write(islocal)
            if self.closed():
                return
            self._update_stream_state(islocal)
        except Exception:
            gen_log.error("Uncaught exception, closing connection.",
                          exc_info=True)
            self.stop()

    def _islocal(self, fd):
        return fd == self._streams.local.socket.fileno()

    def _update_stream_state(self, islocal):
        if islocal:
            stream = self._streams.local
        else:
            stream = self._streams.remote
        if stream is not None:
            state = IOLoop.READ
            if stream.writing():
                state |= IOLoop.WRITE
            stream.update_io_state(state)

    def _handle_read(self, islocal):
        try:
            if islocal:
                self._on_local_read()
            else:
                self._on_remote_read()
        except (ProtocolError, StreamConnresetError):
            self.stop()

    def _on_local_read(self):
        nread = self._streams.local.handle_read()
        if nread is None:
            return
        if nread:
            self._update_last_interaction(nread=nread)
            if self._stage == self.Stage.STREAM:
                self._relay(self._streams.local, self._streams.remote)
            elif (self._stage == self.Stage.SOCKS5_METHOD or
                  self._stage == self.Stage.SOCK5_CMD):
                self._parse_sock5()
            elif self._stage == self.Stage.CONNECTING:
                if not self._opt.server:
                    self._io_loop.run_in_exector(
                        None, self._remote_gotconnected,
                        self._create_connection,
                        self._remote_address[0],
                        self._remote_address[1])
        else:
            # if self._stage != self.Stage.STREAM:
            self.stop()

    def _on_remote_read(self):
        nread = self._streams.remote.handle_read()
        if nread is None:
            return
        if nread:
            self._update_last_interaction(nread=nread)
            assert self._stage == self.Stage.STREAM
            self._relay(self._streams.remote, self._streams.local)
        else:
            self.stop()

    def _handle_write(self, islocal):
        try:
            if islocal:
                self._on_local_write()
            else:
                self._on_remote_write()
        except StreamConnresetError:
            self.stop()

    def _on_local_write(self):
        count = self._streams.local.handle_write()
        self._update_last_interaction(nwrite=count)

    def _on_remote_write(self):
        count = self._streams.remote.handle_write()
        self._update_last_interaction(nwrite=count)

    def _update_last_interaction(self, nread=0, nwrite=0):
        if nread > 0 or nwrite > 0:
            self._last_interaction = self._server.unixtime
            self._server.stat_net_input_bytes += nread
            self._server.stat_net_output_bytes += nwrite

    def _relay(self, src, dest):
        readbuf = src.read_buffer
        nwrite = dest.write(readbuf[src.read_buffer_pos:src.read_buffer_size])
        self._update_last_interaction(nwrite=nwrite)
        dest.remove_event(IOLoop.WRITE)
        src.consume(src.read_buffer_size)

    def _parse_sock5(self):
        if self._stage == self.Stage.SOCKS5_METHOD:
            self._parse_method()
        elif self._stage == self.Stage.SOCK5_CMD:
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
        nwrite = stream.write(response)
        self._update_last_interaction(nwrite=nwrite)
        if response[1]:
            # request failed
            gen_log.error("No supported SOCKS5 authentication method received")
            raise ProtocolError
        if stream.read_target_bytes is not None:
            stream.read_target_bytes = None
        stream.consume(total)
        self._stage = self.Stage.SOCK5_CMD

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
        total_octets = 0
        version, cmd, rsv, atyp = buf[buf_pos:buf_pos+4]
        buf_pos += 4
        resp = bytearray(b'\x05')  # response version 5
        if version != 0x05:
            gen_log.error("Invalid SOCKS5 message version %s", version)
            resp += b'\x01'  # general SOCKS server failure
        elif cmd == 0x01:  # CONNECT X'01'
            if atyp == 0x01:  # IP v4 address
                total_octets = 4 + 4 + 2
                if stream.read_buffer_size < total_octets:
                    stream.read_target_bytes = total_octets
                    return
                dest_addr = socket.inet_ntop(socket.AF_INET,
                                             buf[buf_pos:buf_pos+4])
                buf_pos += 4
                dest_port = struct.unpack('>H', buf[buf_pos:buf_pos+2])
                self._remote_address = (socket.AF_INET,
                                        (dest_addr, dest_port))
                close_connection = False
            elif atyp == 0x03:  # DOMAINNAME
                if stream.read_buffer_size < 4 + 1:
                    stream.read_target_bytes = 5
                    return
                host_len = buf[buf_pos]
                total_octets = 4 + 1 + host_len + 2
                if stream.read_buffer_size < total_octets:
                    stream.read_target_bytes = total_octets
                    return
                buf_pos += 1
                dest_host_name = buf[buf_pos:buf_pos+host_len]
                buf_pos += host_len
                dest_port = struct.unpack('>H', buf[buf_pos:buf_pos + 2])
                self._remote_address = (socket.AF_UNSPEC,
                                        (dest_host_name.decode('idna'),
                                         dest_port[0]))
                close_connection = False
            elif atyp == 0x04:
                total_octets = 4 + 16 + 2
                if stream.read_buffer_size < total_octets:
                    stream.read_target_bytes = total_octets
                    return
                dest_addr = socket.inet_ntop(socket.AF_INET6,
                                             buf[buf_pos:buf_pos+16])
                buf_pos += 16
                dest_port = struct.unpack('>H', buf[buf_pos:buf_pos+2])
                self._remote_address = (socket.AF_INET6,
                                        (dest_addr, dest_port))
                close_connection = False
            else:
                gen_log.info("SOCKS5 unsupported address type: %s" % atyp)
                resp += b'\x08'  # X'08' Address type not supported
        else:
            gen_log.error("Unsupported command %s", cmd)
            resp += b'\x07'  # X'07' Command not supported

        if not close_connection:
            stream.consume(total_octets)
            stream.read_target_bytes = None
            self._stage = self.Stage.CONNECTING
            self._io_loop.run_in_exector(
                None, self._remote_gotconnected, self._create_connection,
                self._remote_address[0], self._remote_address[1])
        else:
            # suppose IP v4 address
            resp += b'\x00\x01\x00\x00\x00\x00\x00\x00'
            nwrite = stream.write(resp)
            self._update_last_interaction(nwrite=nwrite)
            self.stop()
            raise ProtocolError

    @staticmethod
    def _create_connection(af, address, timeout=SOCKET_TIMEOUT):
        if af == socket.AF_UNSPEC:
            return socket.create_connection(address, timeout)
        else:
            sock = None
            try:
                sock = socket.socket(af, socket.SOCK_STREAM, 0)
                sock.settimeout(timeout)
                sock.connect(address)
                return sock
            except OSError:
                if sock is not None:
                    sock.close()
                raise

    def _create_remote_stream(self, sock):
        set_close_exec(sock.fileno())
        self._streams.remote = IOStream(sock)
        self._streams.remote.add_io_state(IOLoop.READ, self._handle_events)
        self._stage = self.Stage.STREAM

    def _remote_gotconnected(self, feature):
        if self.closed():
            return

        hostname, port = self._remote_address[1]
        if self._opt.server:
            exc = None
            close_connection = True
            af = self._remote_address[0]
            hostname, port = self._remote_address[1]
            try:
                conn = feature.result()

                # On IPv6, ignore flow_info and scope_id
                raddr, rport = conn.getpeername()[:2]
                laddr, lport = conn.getsockname()[:2]
                gen_log.info("connected %s: %s:%s from %s:%s",
                             hostname, raddr, rport, laddr, lport)
                bnd_addr = socket.inet_pton(conn.family, laddr)
                bnd_port = struct.pack("!H", lport)
                if conn.family == socket.AF_INET:
                    atyp = b'\x01'
                else:
                    atyp = b'\x04'
                resp = bytearray(b'\x05\x00\x00')  # VER | REP |  RSV
                resp += atyp
                resp += bnd_addr
                resp += bnd_port

                self._create_remote_stream(conn)
                close_connection = False
            except TimeoutError as _:
                exc = _
                resp = bytearray(b'\x05\x06\x00')  # X'06' TTL expired
            except Exception as _:
                exc = _
                resp = bytearray(b'\x05\x03\x00')  # X'03' Network unreachable

            if not close_connection:
                nwrite = self._streams.local.write(resp)
                self._update_last_interaction(nwrite=nwrite)
            else:
                if af == socket.AF_UNSPEC:
                    resp += b'\x03'
                    addr = hostname.encode("idna")
                    resp += bytes([len(addr)])
                else:
                    if af == socket.AF_INET:
                        resp += b'\x01'
                    else:
                        resp += b'\x04'
                    addr = socket.inet_pton(af, hostname)
                resp += addr
                resp += struct.pack("!H", port)
                gen_log.info("SOCK5 failed to connect '%s:%s' (%s)",
                             hostname, port, exc)
                nwrite = self._streams.local.write(resp)
                self._update_last_interaction(nwrite=nwrite)
                self.stop()
        else:
            try:
                conn = feature.result()
                self._create_remote_stream(conn)
                # need to do the first relay
                self._relay(self._streams.local, self._streams.remote)
            except Exception as exc:
                gen_log.error("failed to connect '%s:%s' (%s)",
                              hostname, port, exc)
                self.stop()
            return


class TCPServer:
    def __init__(self, opt):
        self._io_loop = IOLoop.current()
        self._sockets = {}  # fd -> socket object
        self._handlers = {}  # fd -> remove_handler callable
        self._pending_sockets = []
        self._started = False
        self._stopped = False
        self.stat_net_input_bytes = 0
        self.stat_net_output_bytes = 0
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
