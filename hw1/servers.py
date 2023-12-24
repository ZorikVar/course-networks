import os

from protocol import MyTCPProtocol


class Base:
    def __init__(self, socket: MyTCPProtocol, iterations: int, msg_size: int):
        self.socket = socket
        self.iterations = iterations
        self.msg_size = msg_size


class EchoServer(Base):

    def run(self):
        for _ in range(self.iterations):
            msg = self.socket.recv(self.msg_size)
            self.socket.send(msg)

def period(s, n):
    s = s * (n // len(s))
    return s + s[:n - len(s)]
            
class EchoClient(Base):

    def run(self):
        for _ in range(self.iterations):
            msg = os.urandom(self.msg_size)
            n = self.socket.send(msg)
            rhs = self.socket.recv(n)
            print(f'n={n} | #msg={len(msg)} | #rhs={len(rhs)}', file=open("args.txt", 'a'))
            i = 0
            while i < n:
                j = i
                while j < len(msg) and msg[j] != rhs[j]:
                    j += 1
                if i < j:
                    print(f'[{i}; {j})', file=open("args.txt", 'a'))
                    print(''.join([str(msg[k]).ljust(4, ' ') for k in range(i, j)]), file=open("args.txt", 'a'))
                    print(''.join([str(rhs[k]).ljust(4, ' ') for k in range(i, j)]), file=open("args.txt", 'a'))
                    i = j
                else:
                    i += 1
            assert n == self.msg_size
            assert msg == rhs
