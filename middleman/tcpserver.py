import enum
import os
import socket
import time

from middleman import socks5
from middleman.encryption import Secret
from middleman.ioloop import IOLoop, PeriodicCallback
from middleman.util import fileobj_to_fd
from middleman.log import gen_log
from middleman.netutil import bind_sockets, add_accept_handler, ProtocolAddress
from middleman.iostream import IOStream, StreamConnresetError
from middleman.sysplatform.auto import set_close_exec


class _Stage(enum.IntEnum):
    INIT = 0,
    SOCKS5_METHOD = 1,
    SOCK5_CMD = 2,
    CONNECTING = 3,
    CRYPTO = 4,
    STREAM = 5


_SOCKET_TIMEOUT = 10
_MAX_ATTEMPT_TIMES = 3


class _TcpRelayHandler:

    class _StreamPair:
        def __init__(self, handler, lsock, rsock=None):
            # on behalf of the connection which we 'accept'
            self.local = IOStream(lsock, handler=handler)
            # connection to remote middleman server or web server"
            if rsock is not None:
                self.remote = IOStream(rsock, handler=handler)
            else:
                self.remote = None

        def close(self):
            if self.local is not None:
                self.local.close()
                self.local = None
            if self.remote is not None:
                self.remote.close()
                self.remote = None

    def __init__(self, tcpserver, server, local_socket, opt, ioloop=None):
        self._server = server
        self._tcpserver = tcpserver
        self._opt = opt
        self._io_loop = ioloop or IOLoop.current()
        self._streams = self._StreamPair(self._handle_events, local_socket)
        self._remote_address = None
        self._dname = None
        self._stage = _Stage.INIT
        self._closed = False
        self._future = None
        self._secret = Secret(opt.passwd[0])
        # time of the last interaction, used for timeout
        self._last_interaction = time.time()
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
        self._streams.local.add_io_state(IOLoop.READ)
        self._secret.set_encryptor(iv=os.urandom(16))
        if self._opt.server:
            self._stage = _Stage.CRYPTO
            self._socks5 = socks5.Socks5()
        else:
            self._stage = _Stage.CONNECTING
            # TODO: add support for AF_INET and AF_UNSPEC
            self._remote_address = ProtocolAddress(
                socket.AF_INET, self._opt.remote[0], self._opt.remote_port)
            self._io_loop.run_in_exector(
                None, self._remote_gotconnected,
                self._create_connection,
                self._remote_address.af,
                (self._remote_address.address, self._remote_address.port))

    def stop(self):
        if self._closed:
            return
        self._closed = True
        # attempt to cancel the call
        if self._future is not None:
            if not self._future.done():
                self._future.cancel()
            self._future = None
        # stop timer
        self._cron.stop()
        self._cron = None

        # remove handler from server
        self._tcpserver.remove_handler(self._streams.local.socket)

        # close connection
        self._streams.close()
        self._streams = None

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
            self._update_stream(stream)

    @staticmethod
    def _update_stream(stream):
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
        except (socks5.Socks5Error, StreamConnresetError):
            self.stop()

    def _on_local_read(self):
        stream = self._streams.local
        nread = stream.handle_read()
        if nread is None:
            return
        if nread:
            self._update_last_interaction(nread=nread)
            while (stream.read_buffer_size and
                   stream.read_target_bytes is None):
                if self._stage == _Stage.STREAM:
                    self._relay(self._streams.local, self._streams.remote, True)
                elif (self._stage == _Stage.SOCKS5_METHOD or
                      self._stage == _Stage.SOCK5_CMD):
                    self._handle_socks5(stream)
                elif self._stage == _Stage.CRYPTO:
                    if self._opt.server:
                        if self._set_decipher(stream):
                            self._write_chiper_para(stream)
                            self._stage = _Stage.SOCKS5_METHOD
                    else:
                        self._relay(self._streams.local, self._streams.remote, True)
                elif self._stage == _Stage.CONNECTING:
                    break
        else:
            self.stop()

    def _on_remote_read(self):
        stream = self._streams.remote
        nread = stream.handle_read()
        if nread is None:
            return
        if nread:
            self._update_last_interaction(nread=nread)
            while (stream.read_buffer_size and
                   stream.read_target_bytes is None):
                if self._stage == _Stage.STREAM:
                    self._relay(self._streams.remote, self._streams.local, False)
                elif self._stage == _Stage.CRYPTO:
                    if not self._opt.server:
                        if self._set_decipher(stream):
                            self._stage = _Stage.STREAM
                    else:
                        self._relay(self._streams.remote, self._streams.local, False)
                else:
                    break
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

    def _write_chiper_para(self, stream):
        para = bytearray([len(self._secret.eiv) + 1])
        para += self._secret.eiv
        n = stream.write(para)
        self._update_last_interaction(nwrite=n)

    @staticmethod
    def _read_chiper_para(stream):
        buf = stream.read_buffer
        buf_pos = stream.read_buffer_first
        length = buf[buf_pos]
        if stream.read_buffer_size < length:
            stream.read_target_bytes = length
            return None
        buf_pos += 1
        iv = buf[buf_pos:buf_pos+length-1]
        stream.consume(length)
        return iv

    def _set_decipher(self, stream):
        iv = self._read_chiper_para(stream)
        if iv is not None:
            self._secret.set_decryptor(iv)
            return True
        else:
            return False

    @staticmethod
    def _get_stream_readbuff_data(stream):
        readbuf = stream.read_buffer
        data = memoryview(readbuf[stream.read_buffer_first:stream.read_buffer_last])
        stream.consume(len(data))
        return data

    def _decrypt(self, stream):
        data = self._get_stream_readbuff_data(stream)
        return self._secret.decrypt(data)

    def _encrypt(self, stream):
        data = self._get_stream_readbuff_data(stream)
        return self._secret.encrypt(data)

    def _relay(self, src, dest, islocal):
        if ((islocal and self._opt.server) or
                (not islocal and not self._opt.server)):
            data = self._decrypt(src)
        else:
            data = self._encrypt(src)

        nwrite = dest.write(data)
        # update dest io state
        self._update_stream(dest)
        dest.remove_event(IOLoop.WRITE)
        self._update_last_interaction(nwrite=nwrite)

    def _handle_socks5(self, stream):
        msg = self._decrypt(stream)
        try:
            if self._stage == _Stage.SOCKS5_METHOD:
                resp = self._socks5.negotiate_method(msg)
                self._reply_socks5_request(resp, stream)
                self._stage = _Stage.SOCK5_CMD
            else:
                self._remote_address = self._socks5.evaluate_request(msg)
                self._stage = _Stage.CONNECTING
                del self._socks5
                if self._remote_address.af == socket.AF_UNSPEC:
                    self._dname = self._remote_address.address
                    if self._dname in self._server.dc:
                        ditem = self._server.dc[self._dname]
                        rep, af, addr = ditem.value
                        port = self._remote_address.port
                        self._remote_address = ProtocolAddress(af, addr, port)
                        if rep == socks5.REP_SUCCEEDED:
                            gen_log.error("Hit cache: %s (%s)", self._dname, addr)
                        elif ditem.times >= _MAX_ATTEMPT_TIMES:
                            response = socks5.create_response(rep, self._remote_address)
                            self._reply_socks5_request(response, self._streams.local)
                            gen_log.error("failed to connect %s (Rep: %s)", self._dname, rep[0])
                            raise socks5.Socks5Error

                self._future = self._io_loop.run_in_exector(
                    None, self._remote_gotconnected, self._create_connection,
                    self._remote_address.af,
                    (self._remote_address.address, self._remote_address.port),
                    _SOCKET_TIMEOUT)
        except socks5.Socks5SizeError:
            return
        except socks5.Socks5NoSupportError as e:
            resp = e.response
            self._reply_socks5_request(resp, stream)
            raise

    def _reply_socks5_request(self, response, stream):
        response = self._secret.encrypt(response)
        nwrite = stream.write(response)
        self._update_last_interaction(nwrite=nwrite)

    def _update_last_interaction(self, nread=0, nwrite=0):
        if nread > 0 or nwrite > 0:
            self._last_interaction = self._server.ct.unixtime
            self._tcpserver.stat_net_input_bytes += nread
            self._tcpserver.stat_net_output_bytes += nwrite

    @staticmethod
    def _create_connection(af, address, timeout=_SOCKET_TIMEOUT):
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
        self._streams.remote = IOStream(sock, handler=self._handle_events)
        self._streams.remote.add_io_state(IOLoop.READ)

    def _remote_gotconnected(self, future):
        if self._opt.server:
            if self._dname is None and self.closed():
                return
            try:
                conn = future.result()
                rep = socks5.REP_SUCCEEDED
                # On IPv6, ignore flow_info and scope_id
                raddr, rport = conn.getpeername()[:2]
                af = conn.family
                if self._dname is not None:
                    gen_log.info("%s (%s)", self._dname, raddr)
                    self._server.dc[self._dname] = (rep, af, raddr)
                if self.closed():
                    return
                self._remote_address = ProtocolAddress(af, raddr, rport)
                laddr, lport = conn.getsockname()[:2]
                pa = ProtocolAddress(af, laddr, lport)
                response = socks5.create_response(rep, pa)
                self._reply_socks5_request(response, self._streams.local)
                self._create_remote_stream(conn)
                self._stage = _Stage.STREAM
                gen_log.info("connected %s:%s from %s:%s",
                             raddr, rport, laddr, lport)
                return
            except ConnectionRefusedError as _:
                exc = _
                rep = socks5.REP_CONN_REFUSED
            except (socket.timeout, TimeoutError) as _:
                exc = _
                rep = socks5.REP_TTL
            except Exception as _:
                exc = _
                rep = socks5.REP_NETWORK_UNREACHABLE

            af = self._remote_address.af
            host = self._remote_address.address
            port = self._remote_address.port
            if self._dname is not None:
                self._server.dc[self._dname] = (rep, af, host)
            if self.closed():
                return
            gen_log.info("failed to connect '%s:%s' (%s)", host, port, exc)
            pa = ProtocolAddress(af, host, port)
            response = socks5.create_response(rep, pa)
            self._reply_socks5_request(response, self._streams.local)
            self.stop()
        else:
            if self.closed():
                return
            try:
                conn = future.result()
                self._create_remote_stream(conn)
                self._stage = _Stage.CRYPTO
                self._write_chiper_para(self._streams.remote)
                # need to do the first relay
                self._relay(self._streams.local, self._streams.remote, True)
            except Exception as exc:
                gen_log.error("failed to connect %s:%s (%s)",
                              self._remote_address.address,
                              self._remote_address.port, exc)
                self.stop()


class TCPServer:
    def __init__(self, opt, server):
        self._io_loop = IOLoop.current()
        self._sockets = {}  # fd -> socket object
        self._handlers = {}  # fd -> remove_handler callable
        self._pending_sockets = []
        self._started = False
        self._stopped = False
        self.stat_net_input_bytes = 0
        self.stat_net_output_bytes = 0
        self._opt = opt
        self._relay_num = 0
        self._server = server

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

    def stop(self):
        """Stops listening for new connections.

        Requests currently in progress may still continue after the
        server is stopped.
        """
        if self._stopped:
            return
        self._stopped = True

        for fd, sock in self._sockets.items():
            assert sock.fileno() == fd
            # Unregister socket from IOLoop
            self._handlers.pop(fd)()
            sock.close()

        # close connection(s) and release resources
        while self._handlers:
            fd, handler = self._handlers.popitem()
            handler()

    def add_handler(self, connection, handler):
        self._handlers[connection.fileno()] = handler
        self._relay_num += 1

    def remove_handler(self, connection):
        self._handlers.pop(connection.fileno())
        self._relay_num -= 1

    def _handle_connection(self, connection, address):
        gen_log.info("Accepted client connection %s", address)
        relay = _TcpRelayHandler(self, self._server, connection, self._opt, self._io_loop)
        self.add_handler(connection, relay.stop)
        relay.start()
