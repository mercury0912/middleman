import errno
import os
import socket
import sys
from collections import namedtuple

from middleman.ioloop import IOLoop
from middleman.sysplatform.auto import set_close_exec


# ThreadedResolver runs getaddrinfo on a thread. If the hostname is unicode,
# getaddrinfo attempts to import encodings.idna. If this is done at
# module-import time, the import lock is already held by the main thread,
# leading to deadlock. Avoid it by caching the idna encoder on the main
# thread now.
u'foo'.encode('idna')

# For undiagnosed reasons, 'latin1' codec may also need to be preloaded.
u'foo'.encode('latin1')

# These errnos indicate that a non-blocking operation must be retried
# at a later time.  On most platforms they're the same value, but on
# some they differ.
_ERRNO_WOULDBLOCK = (errno.EWOULDBLOCK, errno.EAGAIN)

if hasattr(errno, "WSAEWOULDBLOCK"):
    _ERRNO_WOULDBLOCK += (errno.WSAEWOULDBLOCK,)  # type: ignore

# Default backlog used when calling sock.listen()
_DEFAULT_BACKLOG = 128

RemoteAddress = namedtuple('RemoteAddr', ['af', 'addr'])

def bind_sockets(port, address=None, family=socket.AF_UNSPEC,
                 backlog=_DEFAULT_BACKLOG, flags=None, reuse_port=False):
    """Creates listening sockets bound to the given port and address.

    Returns a list of socket objects (multiple sockets are returned if
    the given address maps to multiple IP addresses, which is most common
    for mixed IPv4 and IPv6 use).

    Address may be either an IP address or hostname.  If it's a hostname,
    the server will listen on all IP addresses associated with the
    name.  Address may be an empty string or None to listen on all
    available interfaces.  Family may be set to either `socket.AF_INET`
    or `socket.AF_INET6` to restrict to IPv4 or IPv6 addresses, otherwise
    both will be used if available.

    The ``backlog`` argument has the same meaning as for
    `socket.listen() <socket.socket.listen>`.

    ``flags`` is a bitmask of AI_* flags to `~socket.getaddrinfo`, like
    ``socket.AI_PASSIVE | socket.AI_NUMERICHOST``.

    ``reuse_port`` option sets ``SO_REUSEPORT`` option for every socket
    in the list. If your platform doesn't support this option ValueError will
    be raised.
    """
    if reuse_port and not hasattr(socket, "SO_REUSEPORT"):
        raise ValueError("the platform doesn't support SO_REUSEPORT")

    sockets = []
    if address == "":
        address = None
    if not socket.has_ipv6 and family == socket.AF_UNSPEC:
        # Python can be compiled with --disable-ipv6, which causes
        # operations on AF_INET6 sockets to fail, but does not
        # automatically exclude those results from getaddrinfo
        # results.
        # http://bugs.python.org/issue16208
        family = socket.AF_INET
    if flags is None:
        flags = socket.AI_PASSIVE
    bound_port = None
    for res in set(socket.getaddrinfo(address, port, family, socket.SOCK_STREAM,
                                      0, flags)):
        af, socktype, proto, canonname, sockaddr = res
        if (sys.platform == 'darwin' and address == 'localhost' and
                af == socket.AF_INET6 and sockaddr[3] != 0):
            # Mac OS X includes a link-local address fe80::1%lo0 in the
            # getaddrinfo results for 'localhost'.  However, the firewall
            # doesn't understand that this is a local address and will
            # prompt for access (often repeatedly, due to an apparent
            # bug in its ability to remember granting access to an
            # application). Skip these addresses.
            continue
        try:
            sock = socket.socket(af, socktype, proto)
        except OSError as e:
            if e.errno == errno.EAFNOSUPPORT:
                continue
            raise
        set_close_exec(sock.fileno())
        if os.name != 'nt':
            try:
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            except socket.error as e:
                if e.errno != errno.ENOPROTOOPT:
                    # Hurd doesn't support SO_REUSEADDR.
                    raise
        if reuse_port:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
        if af == socket.AF_INET6:
            # On linux, ipv6 sockets accept ipv4 too by default,
            # but this makes it impossible to bind to both
            # 0.0.0.0 in ipv4 and :: in ipv6.  On other systems,
            # separate sockets *must* be used to listen for both ipv4
            # and ipv6.  For consistency, always disable ipv4 on our
            # ipv6 sockets and use a separate ipv4 socket when needed.
            #
            # Python 2.x on windows doesn't have IPPROTO_IPV6.
            if hasattr(socket, "IPPROTO_IPV6"):
                sock.setsockopt(socket.IPPROTO_IPV6, socket.IPV6_V6ONLY, 1)

        # automatic port allocation with port=None
        # should bind on the same port on IPv4 and IPv6
        host, requested_port = sockaddr[:2]
        if requested_port == 0 and bound_port is not None:
            sockaddr = tuple([host, bound_port] + list(sockaddr[2:]))

        sock.setblocking(False)
        sock.bind(sockaddr)
        bound_port = sock.getsockname()[1]
        sock.listen(backlog)
        sockets.append(sock)
    return sockets


def add_accept_handler(sock, callback):
    """Adds an `.IOLoop` event handler to accept new connections on ``sock``.

    When a connection is accepted, ``callback(connection, address)`` will
    be run (``connection`` is a socket object, and ``address`` is the
    address of the other end of the connection).  Note that this signature
    is different from the ``callback(fd, events)`` signature used for
    `.IOLoop` handlers.

    A callable is returned which, when called, will remove the `.IOLoop`
    event handler and stop processing further incoming connections.

    .. versionchanged:: 5.0
       The ``io_loop`` argument (deprecated since version 4.1) has been removed.

    .. versionchanged:: 5.0
       A callable is returned (``None`` was returned before).
    """
    io_loop = IOLoop.current()
    removed = [False]

    def accept_handler(_fd, _events):
        # More connections may come in while we're handling callbacks;
        # to prevent starvation of other tasks we must limit the number
        # of connections we accept at a time.  Ideally we would accept
        # up to the number of connections that were waiting when we
        # entered this method, but this information is not available
        # (and rearranging this method to call accept() as many times
        # as possible before running any callbacks would have adverse
        # effects on load balancing in multiprocess configurations).
        # Instead, we use the (default) listen backlog as a rough
        # heuristic for the number of connections we can reasonably
        # accept at once.
        for i in range(_DEFAULT_BACKLOG):
            if removed[0]:
                # The socket was probably closed
                return
            try:
                connection, address = sock.accept()
            except OSError as e:
                # _ERRNO_WOULDBLOCK indicate we have accepted every
                # connection that is available.
                if e.errno in _ERRNO_WOULDBLOCK:
                    return
                # ECONNABORTED indicates that there was a connection
                # but it was closed while still in the accept queue.
                # (observed on FreeBSD).
                if e.errno == errno.ECONNABORTED:
                    continue
                raise
            set_close_exec(connection.fileno())
            callback(connection, address)

    def remove_handler():
        io_loop.remove_handler(sock)
        removed[0] = True

    io_loop.add_handler(sock, IOLoop.READ, accept_handler)
    return remove_handler


def is_valid_ip(ip):
    """Returns true if the given string is a well-formed IP address.

    Supports IPv4 and IPv6.
    """
    if not ip or '\x00' in ip:
        # getaddrinfo resolves empty strings to localhost, and truncates
        # on zero bytes.
        return False
    try:
        res = socket.getaddrinfo(ip, 0, socket.AF_UNSPEC,
                                 socket.SOCK_STREAM,
                                 0, socket.AI_NUMERICHOST)
        return bool(res)
    except socket.gaierror as e:
        if e.args[0] == socket.EAI_NONAME:
            return False
        raise


def resolve_addr(host, port, family=socket.AF_UNSPEC):
    # On Solaris, getaddrinfo fails if the given port is not found
    # in /etc/services and no socket type is given, so we must pass
    # one here.  The socket type used here doesn't seem to actually
    # matter (we discard the one we get back in the results),
    # so the addresses we return should still be usable with SOCK_DGRAM.
    addrinfo = socket.getaddrinfo(host, port, family, socket.SOCK_STREAM)
    results = []
    for family, socktype, proto, canonname, address in addrinfo:
        results.append((family, address))
    return results
