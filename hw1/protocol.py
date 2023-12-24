from enum import IntEnum
from collections import deque
import time

from timeit import default_timer

from debugging import *
from binary_format import Encoder, Decoder

# log = noop
# log = print

with open('.mode', mode='r') as fd:
    mode = fd.read().strip()
    if mode == 'simple':
        print("=" * 80, " Simple mode ".center(80, '='), "=" * 80, sep='\n')
        from protocol_in_memory import InMemoryProtocol as BaseProtocol
    else:
        print("Hard mode")
        from protocol_udp_based import UDPBasedProtocol as BaseProtocol
        print = log

import sys
sys.path.insert(0, "/home/dais/networks/hw1/build")
from wise_protocol import WiseProtocol

def clock():
    return time.time_ns() // 1_000_000

def clock_ms():
    return time.time_ns() / 1000_000
time_zero = clock_ms()

fdJ = open('logJ', 'w')
fdB = open('logB', 'w')
fdX = open('log', 'w')

no_log = False
no_log_roles = ['recv']
no_log_roles = []

T = print_fmt
def print_fmt(*args, **kwargs):
    T(f'{clock_ms() - time_zero:.3f}', *args, **kwargs)
if no_log:
    print_fmt = lambda *args, **kwargs: None

def logJ(*args):
    print_fmt(*args, file=fdJ)
    print_fmt(*args, file=fdX)
def logB(*args):
    print_fmt(*args, file=fdB)
    print_fmt(*args, file=fdX)

class MyTCPProtocol(BaseProtocol):
    nr_nodes = 0

    def log(self, *args):
        if self.role in no_log_roles:
            return
        # if self.name.find('J') != -1:
        #     logJ(self.name, *args)
        # else:
        #     logB(self.name, *args)

    def __init__(self, *args, **kwargs):
        self.name = '\033[34;1mMr B\033[0m' if MyTCPProtocol.nr_nodes % 2 == 0 else '\033[31;1mMr J\033[0m'
        MyTCPProtocol.nr_nodes += 1

        super().__init__(*args, **kwargs)
        self.muscle = WiseProtocol(super())

        self.inter_idx = 0

    def send(self, data):
        self.role = 'send'

        start_time = clock_ms()
        self.log(f'ready for transaction ${self.inter_idx} as sender')
        self.inter_idx += 1

        self.muscle.send(data)

        self.log(f'send() in {clock_ms() - start_time:.5f} ms')
        return len(data)

    def recv(self, n: int):
        self.role = 'recv'

        start_time = clock_ms()

        self.inter_idx += 1
        self.log(f'ready for transaction ${self.inter_idx} as listener')

        data = self.muscle.recv(n)

        self.log(f'recv() in {clock_ms() - start_time:.5f} ms')

        return data
