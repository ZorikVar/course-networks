import time
from random import randint as rand

class InMemoryProtocol:
    def __init__(self, other):
        self.other = other
        self.buffer = b''
        self.t = 0

    def set_timeout(self, t):
        self.t = t

    def sendto(self, data):
        a, b, q = 10, 20, 70
        lot = rand(0, q - 1)
        if lot < a:
            pass
        elif lot < b:
            self.other.buffer += data
            self.other.buffer += data
        else:
            self.other.buffer += data

    def recvfrom(self, n):
        if len(self.buffer) < n:
            time.sleep(self.t)
        n = min(n, len(self.buffer))
        ret = self.buffer[:n]
        self.buffer = self.buffer[n:]
        return ret
