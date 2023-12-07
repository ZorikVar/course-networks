class Encoder:
    def __init__(self):
        self.buffer = b''

    def load(self):
        return self.buffer

    def int8(self, n):
        n = int(n)
        self.buffer += bytes([n & 0xff])

    def int32(self, n):
        n = int(n)
        self.buffer += bytes([(n >> i) & 0xff for i in range(0, 32, 8)])

    def raw_str(self, s):
        self.buffer += s


class Decoder:
    def __init__(self, data):
        if type(data) is not type(b''):
            raise ValueError('expected a byte string')

        self.buffer = data

    def int8(self):
        if len(self.buffer) < 1:
            raise ValueError('not enough input bytes')

        parsed = int(self.buffer[0])
        self.buffer = self.buffer[1:]
        return parsed

    def int32(self):
        if len(self.buffer) < 4:
            raise ValueError('not enough input bytes')

        parsed = sum([x << i for i, x in zip(range(0, 32, 8), self.buffer)])
        self.buffer = self.buffer[4:]
        return parsed

    def raw_str(self, n):
        if len(self.buffer) < n:
            raise ValueError('not enough input bytes')

        parsed = self.buffer[:n]
        self.buffer = self.buffer[n:]
        return parsed
