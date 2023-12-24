import time
from random import randint as rand

class InMemoryProtocol:
    def __init__(self, other):
        self.other = other
        self.buffer = []
        self.t = 0

    def set_timeout(self, t):
        self.t = t

    def sendto(self, data):
        a, b, q = 40, 20, 70
        lot = rand(0, q - 1)
        lot = q
        if lot < a:
            pass
        elif lot < a + b:
            self.other.buffer += [data]
            self.other.buffer += [data]
        else:
            self.other.buffer += [data]

    def recvfrom(self, n):
        if len(self.buffer) == 0 or len(self.buffer[0]):
            # time.sleep(self.t)
            pass

        if len(self.buffer) == 0:
            return b''

        if rand(1, 10) <= 1:
            n = 64

        n = min(n, len(self.buffer[0]))
        ret = self.buffer[0][:n]
        self.buffer[0] = self.buffer[0][n:]
        if len(self.buffer[0]) == 0:
            self.buffer = self.buffer[1:]
        return ret
