import hashlib

from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.backends import default_backend

DK_LEN = 16


class Secret:
    key_map = {}

    def __init__(self, passwd):
        self.key = self.get_key(passwd)
        self.encryptor = None
        self.decryptor = None

    def encrypt(self, data):
        if self.encryptor is not None:
            return self.encryptor.update(bytes(data))
        else:
            return data

    def decrypt(self, data):
        if self.decryptor is not None:
            return self.decryptor.update(bytes(data))
        else:
            return data

    def set_encryptor(self, iv):
        self.eiv = bytes(iv)
        chiper = Cipher(algorithms.AES(self.key), modes.CFB(self.eiv),
                        backend=default_backend())
        self.encryptor = chiper.encryptor()

    def set_decryptor(self, iv):
        self.div = bytes(iv)
        chiper = Cipher(algorithms.AES(self.key), modes.CFB(self.div),
                        backend=default_backend())
        self.decryptor = chiper.decryptor()

    @classmethod
    def get_key(cls, passwd):
        key = cls.key_map.get(passwd, None)
        if key is not None:
            return key
        else:
            salt = b'middlemanamelddim'
            key = hashlib.pbkdf2_hmac('sha256', passwd.encode(),
                                      salt, 100000, dklen=DK_LEN)
            cls.key_map[passwd] = key
            return key
