from enum import IntEnum
from collections import deque
from threading import Thread
from timeit import default_timer

import socket
from debugging import *
from binary_format import Encoder, Decoder

with open('.mode', mode='r') as fd:
    mode = fd.read().strip()

if mode == 'simple':
    print("=" * 80, " Simple mode ".center(80, '='), "=" * 80, sep='\n')
    from debug_protocol import InMemoryProtocol as BaseProtocol
else:
    print("Hard mode")
    from judge_protocol import UDPBasedProtocol as BaseProtocol



# log = noop
# log = print

"""
GENERAL:
    - INTER_IDX:      int32
    - LENGTH:         int32
    - SEED:           int32
    - TYPE:           int8
    - NR_SEEDS:       int8
    - ...SEEDS:       ...int32
    - SEGMENT_START:  int32
    - PAYLOAD:        bytes
"""

class PackageType(IntEnum):
    GENERAL       = 0
    FINAL_SEGMENT = 1
    STOP_SENDING  = 2


class Package:
    def __init__(self, raw, dbg_id = 0):
        self.parser = Decoder(raw)

        self.INTER_IDX = self.parser.int32()
        self.LENGTH = self.parser.int32()
        self.SEED = self.parser.int32()
        self.TYPE = self.parser.int8()
        self.NR_SEEDS = self.parser.int8()
        self.SEEDS = [self.parser.int32() for _ in range(self.NR_SEEDS)]
        self.SEGMENT_START = -1
        self.PAYLOAD = b''

        nr_left_bytes = self.LENGTH - 10 - 4 * self.NR_SEEDS
        if nr_left_bytes > 0:
            self.SEGMENT_START = self.parser.int32()
            self.PAYLOAD = self.parser.raw_str(nr_left_bytes - 4)


class Segment:
    def __init__(self, start: int, value: bytes, is_final: bool, seed):
        self.start = start
        self.value = value
        self.is_final = is_final
        self.seed = seed

class MyTCPProtocol(BaseProtocol):
    nr_nodes = 0

    def log(self, *args):
        print(self.name, *args)

    def __init__(self, *args, **kwargs):
        self.name = '\033[34;1mMr B\033[0m' if MyTCPProtocol.nr_nodes == 0 else '\033[31;1mMr J\033[0m'
        MyTCPProtocol.nr_nodes += 1
        self.id = MyTCPProtocol.nr_nodes

        super().__init__(*args, **kwargs)

        # Options:
        self.o_nr_hanging_confirms = 8
        self.o_nr_hanging_segments = 10
        self.o_segment_len = 500
        self.o_nr_max_seeds = 10
        self.o_epoch_min = 0.00001
        self.o_epoch_max = 0.004
        self.o_idle_duration = 2

        self.set_epoch(self.o_epoch_min)

        self.inter_idx = 0

        self.i_recv = False
        self.i_send = False
        self.should_stop_sending = False
        self.asking_to_stop = False

        self.last_seed = 555 if self.id == 1 else 7777
        self.received_packages = deque()
        self.seeds_to_confirm = []
        self.sent = {}

        self.passive_listener = None

    def set_epoch(self, t):
        self.epoch = t
        super().set_timeout(t)

    def gen_seed(self):
        self.last_seed += 1
        return self.last_seed

    def listen(self):
        try:
            if self.epoch > self.o_epoch_min:
                self.epoch //= 2

            raw = self.recvfrom(1500)
            if len(raw) == 0:
                return None
            self.log(f'received raw {len(raw)} bytes')
            package = Package(raw)
            self.log(f'received segment {format(package.PAYLOAD)}; seed=${package.SEED}!{self.inter_idx}')

            for seed in package.SEEDS:
                self.log(f"have confirmed {seed}")
                if seed in self.sent:
                    del self.sent[seed]

            if self.i_send:
                if package.INTER_IDX > self.inter_idx:
                    self.log(f"partner has moved on")
                    self.should_stop_sending = True
                if package.TYPE == PackageType.STOP_SENDING:
                    if package.INTER_IDX == self.inter_idx:
                        self.log(f"have received request to stop")
                        self.should_stop_sending = True
                    else:
                        self.log(f"have received a deprecated request to stop")

            if not self.asking_to_stop:
                self.seeds_to_confirm.append(package.SEED)

            if len(package.PAYLOAD):
                self.received_packages.append(package)
        except TimeoutError:
            if self.epoch * 2 <= self.o_epoch_max:
                self.epoch *= 2
            # self.log("TIME OUT ERROR")
        except Exception as e:
            self.log("EXCEPTION")
            self.log(e)
            raise

    def idle_function(self):
        s_time = default_timer()
        self.try_confirm(True)
        while not self.i_send and not self.i_recv and (default_timer() - s_time) < self.o_idle_duration:
            self.listen()
            if self.received_packages:
                package = self.received_packages.popleft()
                if package.INTER_IDX == self.inter_idx:
                    self.ask_to_stop()

    def start_idle(self):
        self.passive_listener = Thread(target=MyTCPProtocol.idle_function, args=(self,))
        self.passive_listener.start()

    def stop_idle(self):
        if self.passive_listener is None:
            return
        self.passive_listener.join()

    def ask_to_stop(self):
        SEED = self.gen_seed()
        TYPE = PackageType.STOP_SENDING
        if len(self.seeds_to_confirm):
            raise Exception("logical error")
        NR_SEEDS = 0
        LENGTH = 10

        out = Encoder()
        out.int32(self.inter_idx)
        out.int32(LENGTH)
        out.int32(SEED)
        out.int8(TYPE)
        out.int8(NR_SEEDS)

        encoded = out.load()
        self.log(f'send "please, stop" seed=${SEED}!{self.inter_idx}')
        self.sendto(encoded)

    def try_send_payload(self, segment: Segment):
        SEED = self.gen_seed()
        TYPE = PackageType.FINAL_SEGMENT if segment.is_final else PackageType.GENERAL
        NR_SEEDS = min(len(self.seeds_to_confirm), self.o_nr_max_seeds)
        SEGMENT_START = segment.start
        LENGTH = 14 + NR_SEEDS * 4 + len(segment.value)

        out = Encoder()
        out.int32(self.inter_idx)
        out.int32(LENGTH)
        out.int32(SEED)
        out.int8(TYPE)
        out.int8(NR_SEEDS)
        for i in range(NR_SEEDS):
            self.log(f"confirm seed=${self.seeds_to_confirm[i]}!{self.inter_idx}")
            out.int32(self.seeds_to_confirm[i])
        del self.seeds_to_confirm[:NR_SEEDS]
        if len(segment.value):
            out.int32(segment.start)
            out.raw_str(segment.value)

        encoded = out.load()
        maybe_fin = ' final' if TYPE == PackageType.FINAL_SEGMENT else ''
        self.log(f"is sending ${self.inter_idx}'s{maybe_fin} segment (\033[32;3mseed=${SEED}!{self.inter_idx})\033[0m: {format(segment.value)}; start={segment.start}")
        self.sendto(encoded)
        self.sent[SEED] = encoded

    def retry_sent(self):
        if len(self.sent) == 0:
            return
        arbitrary = next(iter(self.sent.keys()))
        self.log(f'retry to send seed=${arbitrary}!{self.inter_idx}')
        self.sendto(self.sent[arbitrary])

    def send(self, data: bytes):
        self.inter_idx += 1
        self.i_send = True
        self.log(f'ready for transacion ${self.inter_idx} to send {format(bytearray(data))}')

        i = 0
        while len(self.sent) or i < len(data):
            if self.should_stop_sending:
                raise Exception("logical error")

            if len(self.sent) < self.o_nr_hanging_segments and i < len(data):
                start = i
                end = min(len(data), start + self.o_segment_len)
                i += self.o_segment_len
                self.try_send_payload(Segment(start, data[start:end], end == len(data), None))
            else:
                self.retry_sent()

            self.listen()
            if self.should_stop_sending:
                self.sent = {}
                self.seeds = []
        self.should_stop_sending = False
        if len(self.seeds_to_confirm):
            self.seeds_to_confirm = []
            # raise Exception("logical error")

        self.i_send = False
        return len(data)

    def try_confirm(self, force = False):
        if len(self.seeds_to_confirm) < self.o_nr_hanging_confirms or force:
            return

        SEED = self.gen_seed()
        TYPE = PackageType.GENERAL
        NR_SEEDS = min(len(self.seeds_to_confirm), self.o_nr_max_seeds)
        SEGMENT_START = 0
        LENGTH = 10 + NR_SEEDS * 4

        out = Encoder()
        out.int32(self.inter_idx)
        out.int32(LENGTH)
        out.int32(SEED)
        out.int8(TYPE)
        out.int8(NR_SEEDS)
        for i in range(NR_SEEDS):
            self.log(f"confirm seed=${self.seeds_to_confirm[i]}!{self.inter_idx}")
            out.int32(self.seeds_to_confirm[i])
        del self.seeds_to_confirm[:NR_SEEDS]

        encoded = out.load()
        self.log(f"send a segment <none> seed=${SEED}!{self.inter_idx}")
        self.sendto(encoded)
        self.sent[SEED] = encoded

    def recv(self, n: int):
        self.inter_idx += 1
        self.i_recv = True

        self.stop_idle()
        self.asking_to_stop = False

        self.log(f'ready for transacion ${self.inter_idx} as listener')

        res = bytearray(n)
        for i in range(n):
            res[i] = 0
        nr_collected = 0
        nr_required = None
        while nr_required is None or nr_collected < nr_required:
            self.listen()
            self.try_confirm()
            while self.received_packages:
                package = self.received_packages.popleft()
                if package.INTER_IDX < self.inter_idx:
                    self.log(f'drop an old package ({package.INTER_IDX} < {self.inter_idx})')
                    continue


                i = package.SEGMENT_START
                s = package.PAYLOAD
                nr_collected += len(s)
                if package.TYPE == PackageType.FINAL_SEGMENT:
                    nr_required = i + len(s)
                res[i:i + len(s)] = s
        if self.received_packages:
            raise Exception("logical error")
        self.seeds_to_confirm = []
        self.sent = {}
        self.i_recv = False
        self.asking_to_stop = True
        self.start_idle()

        self.log(f"Received ${self.inter_idx}: {format(res)}\n")
        return res
