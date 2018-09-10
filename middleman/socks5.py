import socket
import struct

from middleman.netutil import ProtocolAddress
from middleman.log import gen_log

VERSION = 5
VER = b'\x05'
METHOD_NO_AUTH = b'\x00'
METHOD_GSSAPI = b'\x01'
METHOD_USER_PASSWD = b'\x02'
METHOD_NO_ACCEPTABLE = b'\xff'
CMD_CONNECT = b'\x01'
CMD_BIND = b'\x02'
CMD_UDP_ASSOCIATE = b'\x03'
RSV = b'\x00'
ATYP_IPV4 = b'\x01'
ATYP_DOMAINNAME = b'\x03'
ATYP_IPV6 = b'\x04'
REP_SUCCEEDED = b'\x00'
REP_GENERAL = b'\x01'
REP_RULESET = b'\x02'
REP_NETWORK_UNREACHABLE = b'\x03'
REP_HOST_UNREACHABLE = b'\x04'
REP_CONN_REFUSED = b'\x05'
REP_TTL = b'\x06'
REP_CMD = b'\x07'
REP_ADDR_TYPE = b'\x08'

FIELD_VER_SIZE = 1
FIELD_NMETHODS_SIZE = 1
FIELD_RSV_SIZE = 1
FIELD_IPV4_SIZE = 4
FIELD_IPV6_SIZE = 16
FIELD_PORT_SIZE = 2
FIELD_DN_LEN_SIZE = 1


class Socks5Error(Exception):
    """ Base exception class for errors from this module."""


class Socks5SizeError(Socks5Error):
    """Exception raised when a required length cannot be satisfied."""


class Socks5NoSupportError(Socks5Error):
    """Exception raised when not supported requirement"""
    def __init__(self, response):
        self.response = response


class Socks5ValueError(Socks5Error):
    """Exception raised when invalid value"""


class Socks5:
    def __init__(self):
        self._buf = bytearray()

    def negotiate_method(self, msg):
        """version identifier/method selection message"""
        # +-----+----------+----------+
        # | VER | NMETHODS | METHODS |
        # +-----+----------+---------+
        # |  1  |     1   |1 to  255 |
        # +----+----------+----------+
        self._buf += msg
        head_len = FIELD_VER_SIZE + FIELD_NMETHODS_SIZE
        if len(self._buf) < head_len:
            raise Socks5SizeError
        pos = 0
        version = self._buf[pos]
        if version != VERSION:
            gen_log.error("Unsupported SOCKS version %s", version)
            raise Socks5ValueError
        nmethods = self._buf[pos + FIELD_VER_SIZE]
        if nmethods == 0:
            gen_log.error("Invalid number of methods: %d", nmethods)
            raise Socks5ValueError
        octets = head_len + nmethods
        if len(self._buf) < octets:
            raise Socks5SizeError

        #  +----+--------+
        #  |VER | METHOD |
        #  +----+--------+
        #  | 1  |   1    |
        #  +----+--------+
        response = bytearray()
        response += VER
        for i in range(nmethods):
            if self._buf[head_len + i] == METHOD_NO_AUTH[0]:
                response += METHOD_NO_AUTH
                break
        else:
            # request failed
            response += METHOD_NO_ACCEPTABLE
            gen_log.error("No supported SOCKS5 authentication method received")
            raise Socks5NoSupportError(response)
        assert len(self._buf) == octets, len(self._buf)
        self._buf.clear()
        return response

    def evaluate_request(self, msg):
        """parse client request details"""
        # +----+-----+-------+------+----------+----------+
        # |VER | CMD |  RSV  | ATYP | DST.ADDR | DST.PORT |
        # +----+-----+-------+------+----------+----------+
        # | 1  |  1  | X'00' |  1   | Variable |    2     |
        # +----+-----+-------+------+----------+----------+
        self._buf += msg
        head_len = 4  # VER + CMD + RSV + ATYP field size
        if len(self._buf) < head_len:
            raise Socks5SizeError
        version, cmd, rsv, atyp = self._buf[0:head_len]
        error = True
        octets = 0
        remote_address = None
        pos = head_len
        resp = bytearray()
        resp += VER
        if version != VERSION:
            gen_log.error("Invalid SOCKS5 message version %s", version)
            resp += REP_GENERAL
        elif cmd == 0x01:  # CONNECT X'01'
            if atyp == 0x01:  # IP v4 address
                octets = head_len + FIELD_IPV4_SIZE + FIELD_PORT_SIZE
                if len(self._buf) < octets:
                    raise Socks5SizeError
                dest_addr = socket.inet_ntop(socket.AF_INET,
                                             self._buf[pos:pos+FIELD_IPV4_SIZE])
                pos += FIELD_IPV4_SIZE
                dest_port = struct.unpack('!H', self._buf[pos:pos+FIELD_PORT_SIZE])[0]
                remote_address = ProtocolAddress(socket.AF_INET, dest_addr, dest_port)
                error = False
            elif atyp == 0x03:  # DOMAINNAME
                if len(self._buf) < head_len + FIELD_DN_LEN_SIZE:
                    raise Socks5SizeError
                host_len = self._buf[pos]
                octets = head_len + FIELD_DN_LEN_SIZE + host_len + FIELD_PORT_SIZE
                if len(self._buf) < octets:
                    raise Socks5SizeError
                pos += FIELD_DN_LEN_SIZE
                dest_hostname = self._buf[pos:pos+host_len].decode('idna')
                pos += host_len
                dest_port = struct.unpack('!H', self._buf[pos:pos + FIELD_PORT_SIZE])[0]
                remote_address = ProtocolAddress(socket.AF_UNSPEC, dest_hostname, dest_port)
                error = False
            elif atyp == 0x04:
                octets = head_len + FIELD_IPV6_SIZE + FIELD_PORT_SIZE
                if len(self._buf) < octets:
                    raise Socks5SizeError
                dest_addr = socket.inet_ntop(socket.AF_INET6,
                                             self._buf[pos:pos+FIELD_IPV6_SIZE])
                pos += FIELD_IPV6_SIZE
                dest_port = struct.unpack('!H', self._buf[pos:pos+FIELD_PORT_SIZE])[0]
                remote_address = ProtocolAddress(socket.AF_INET6, dest_addr, dest_port)
                error = False
            else:
                gen_log.error("SOCKS5 unsupported address type: %s" % atyp)
                resp += REP_ADDR_TYPE
        else:
            gen_log.error("Unsupported command %s", cmd)
            resp += REP_CMD

        if not error:
            assert len(self._buf) == octets, len(self._buf)
            self._buf.clear()
            return remote_address
        else:
            resp += RSV
            # suppose IP v4 address
            resp += ATYP_IPV4
            resp += b'\x00\x00\x00\x00\x00\x00'
            raise Socks5NoSupportError(resp)


def create_response(rep, remote):
    af = remote.af
    addr = remote.address
    port = remote.port
    response = bytearray()
    response += VER
    response += rep
    response += RSV

    if af == socket.AF_UNSPEC:
        atyp = ATYP_DOMAINNAME
        response += atyp
        bnd_addr = addr.encode("idna")
        response += bytes([len(addr)])
    else:
        if af == socket.AF_INET:
            atyp = ATYP_IPV4
        else:
            atyp = ATYP_IPV6
        response += atyp
        bnd_addr = socket.inet_pton(af, addr)
    response += bnd_addr
    response += struct.pack("!H", port)
    return response
