from enum import IntEnum
from collections import deque
from threading import Thread
from timeit import default_timer
from random import randint as rand

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

def clock():
    return time.time_ns() // 1_000_000

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

        # print(len(raw), 'to parse')

        self.INTER_IDX = self.parser.int32()
        self.LENGTH = self.parser.int32()
        self.SEED = self.parser.int32()
        self.TYPE = self.parser.int8()
        self.NR_SEEDS = self.parser.int8()

        header = [f'self.INTER_IDX={self.INTER_IDX}',
        f'self.LENGTH={self.LENGTH}',
        f'self.SEED={self.SEED}',
        f'self.TYPE={self.TYPE}',
        f'self.NR_SEEDS={self.NR_SEEDS}']
        header = [x.ljust(24, ' ') for x in header]
        header = header[0:2] + ['\n'] + header[2:4] + ['\n'] + header[4:5]
        header = ''.join(header)
        # print(48 * '-')
        # print(header)
        # print(48 * '-')

        self.SEEDS = [self.parser.int32() for _ in range(self.NR_SEEDS)]
        self.SEGMENT_START = -1
        self.PAYLOAD = b''

        nr_left_bytes = self.LENGTH - 14 - 4 * self.NR_SEEDS
        if nr_left_bytes > 0:
            self.SEGMENT_START = self.parser.int32()
            self.PAYLOAD = self.parser.raw_str(nr_left_bytes - 4)
            # TODO

class PackageWrapper:
    def __init__(self):
        self.half_ass_chunk = None

    def feed(self, chunk):
        if self.half_ass_chunk is not None:
            chunk = self.half_ass_chunk + chunk
            self.half_ass_chunk = None

        try:
            package = Package(chunk)
        except ValueError as e:
            if str(e) != 'not enough input bytes':
                raise e
            self.half_ass_chunk = chunk
            return None

        return package


class Segment:
    def __init__(self, start: int, value: bytes, is_final: bool, seed):
        self.start = start
        self.value = value
        self.is_final = is_final
        self.seed = seed

class Pipe:
    seed = 0

    def __init__(self, channel):
        self.channel = channel
        self.next_seed = 555 if Pipe.seed == 0 else 7777

        Pipe.seed += 1

    def send_package(self, segment, seeds_to_confirm, *, inter_idx, message_type = None):
        o_nr_max_seeds = 10

        INTER_IDX = inter_idx
        self.next_seed += 1
        SEED = self.next_seed
        if segment is not None:
            TYPE = PackageType.FINAL_SEGMENT if segment.is_final else PackageType.GENERAL
        elif message_type is not None:
            TYPE = message_type
        else:
            TYPE = PackageType.GENERAL
        NR_SEEDS = min(len(seeds_to_confirm), o_nr_max_seeds)
        LENGTH = 14 + NR_SEEDS * 4 + (4 + len(segment.value) if segment is not None else 0)

        out = Encoder()
        out.int32(INTER_IDX)
        out.int32(LENGTH)
        out.int32(SEED)
        out.int8(TYPE)
        out.int8(NR_SEEDS)

        for _ in range(NR_SEEDS):
            seed = seeds_to_confirm.pop(0)
            out.int32(seed)

        if segment is not None and len(segment.value):
            out.int32(segment.start)
            out.raw_str(segment.value)

        encoded = out.load()
        self.channel.sendto(encoded)

        return SEED, encoded


class MyTCPProtocol(BaseProtocol):
    nr_nodes = 0

    def llog(self, *args):
        print(self.name, *args)

    def log(self, *args):
        # print(self.name, *args)
        pass

    def log_sent_package(self, encoded):
        package = PackageWrapper().feed(encoded)
        if package is None:
            self.log('can\'t parse his own message')
            raise Exception('logical error')

        INTER_IDX = package.INTER_IDX
        SEED = package.SEED
        TYPE = package.TYPE

        for confirmed in package.SEEDS:
            self.log(f"confirms seed=${confirmed}!{INTER_IDX}")

        if len(package.PAYLOAD):
            maybe_fin = ' final' if TYPE == PackageType.FINAL_SEGMENT else ''
            self.log(f"is sending ${INTER_IDX}'s{maybe_fin} segment (\033[32;3mseed=${SEED}!{INTER_IDX})\033[0m: {format(package.PAYLOAD)}; start={package.SEGMENT_START}")
            self.sent[SEED] = (encoded, INTER_IDX)
        elif TYPE == PackageType.STOP_SENDING:
            self.log(f'send "please, stop" seed=${SEED}!{INTER_IDX}')
        else:
            self.log(f"send a segment <none> seed=${SEED}!{INTER_IDX}")
            self.sent[SEED] = (encoded, INTER_IDX)

    def __init__(self, *args, **kwargs):
        self.name = '\033[34;1mMr B\033[0m' if MyTCPProtocol.nr_nodes == 0 else '\033[31;1mMr J\033[0m'
        MyTCPProtocol.nr_nodes += 1

        super().__init__(*args, **kwargs)

        self.pipe_out = Pipe(super())

        self.inter_idx = 0

        self.package_wrapper = PackageWrapper()
        self.received_packages = deque()
        self.sent = {}

        self.passive_listener = None

    def old_listen(self, max_duration = 0.00001):
        try:
            self.set_timeout(max_duration)
            chunk = self.recvfrom(1500)
            if len(chunk) == 0:
                return None
            package = self.package_wrapper.feed(chunk)
            if package is not None:
                self.log(f'received segment {format(package.PAYLOAD)}; seed=${package.SEED}!{self.inter_idx}')
                self.received_packages.append(package)
        except TimeoutError:
            pass
        except Exception as e:
            self.log("EXCEPTION")
            self.log(e)
            raise

    def recv(self, n: int):
        self.inter_idx += 1
        # self.idle_stop()
        self.log(f'ready for transacion ${self.inter_idx} as listener')

        res = bytearray(n)
        for i in range(n):
            res[i] = 0

        seeds_to_confirm = []
        nr_collected = 0
        nr_required = None
        while nr_required is None or nr_collected < nr_required:
            self.old_listen()
            self.try_confirm(seeds_to_confirm)

            while self.received_packages:
                package = self.received_packages.popleft()
                if package.INTER_IDX < self.inter_idx:
                    self.llog('got a message from a weirdly old interaction')
                    self.ask_to_stop(package.INTER_IDX)

                for seed in package.SEEDS:
                    self.log(f"considers confirmed {seed}")
                    if seed in self.sent:
                        del self.sent[seed]

                seeds_to_confirm.append(package.SEED)

                if package.INTER_IDX < self.inter_idx:
                    self.log(f'drops an old package ({package.INTER_IDX} < {self.inter_idx})')
                    continue

                i = package.SEGMENT_START
                s = package.PAYLOAD
                nr_collected += len(s)
                if package.TYPE == PackageType.FINAL_SEGMENT:
                    nr_required = i + len(s)
                res[i:i + len(s)] = s

        if self.received_packages:
            raise Exception("logical error")
        self.sent = {}

        self.log(f"received ${self.inter_idx}: {format(res)}\n")
        return res

    def ask_to_stop(self, inter_idx):
        last_time = None
        try:
            last_time = self.last_time
        except:
            pass
        curr_time = clock()
        self.last_time = curr_time

        if last_time is not None and curr_time - last_time < 200:
            return

        seed, encoded = self.pipe_out.send_package(None, [], inter_idx=inter_idx, message_type=PackageType.STOP_SENDING)
        self.log_sent_package(encoded)

    def try_confirm(self, seeds_to_confirm, force = False):
        o_nr_hanging_confirms = 5

        if len(seeds_to_confirm) < o_nr_hanging_confirms or force:
            return

        seed, encoded = self.pipe_out.send_package(None, seeds_to_confirm, inter_idx=self.inter_idx)
        self.log_sent_package(encoded)

        self.sent[seed] = (encoded, self.inter_idx)

    def send_segment(self, segment: Segment, seeds_to_confirm):
        seed, encoded = self.pipe_out.send_package(segment, seeds_to_confirm, inter_idx=self.inter_idx)
        self.log_sent_package(encoded)

        self.sent[seed] = (encoded, self.inter_idx)

    def send(self, data: bytes):
        o_nr_hanging_segments = 10
        o_segment_len = 500

        self.inter_idx += 1
        self.log(f'ready for transaction ${self.inter_idx} to send {format(bytearray(data))}')

        retry_ms = 0.002
        seeds_to_confirm = []
        first_time = None

        done = False
        i = 0
        while len(self.sent) or i < len(data):
            if len(self.sent) < o_nr_hanging_segments and i < len(data):
                start, end = i, i + o_segment_len
                end = min(end, len(data))
                segment = Segment(start, data[start:end], end == len(data), None)

                self.send_segment(segment, seeds_to_confirm)
                i = end
                continue

            self.old_listen(retry_ms)

            if not self.received_packages and i == len(data):
                curr_time = clock()
                if first_time is None:
                    first_time = curr_time
                if curr_time - first_time > 800:
                    done = True
                    break
            else:
                first_time = None

            new_pac = []
            while self.received_packages:
                package = self.received_packages.popleft()
                if package.INTER_IDX < self.inter_idx:
                    self.llog('got a message from a weirdly old interaction')
                    self.ask_to_stop(package.INTER_IDX)

                for seed in package.SEEDS:
                    self.log(f"considers confirmed {seed}")
                    if seed in self.sent:
                        del self.sent[seed]

                seeds_to_confirm.append(package.SEED)

                if len(package.PAYLOAD):
                    new_pac.append(package)

                if package.INTER_IDX > self.inter_idx:
                    self.log(f"sees his partner has moved on")
                    done = True
                    break

                if package.TYPE == PackageType.STOP_SENDING:
                    if package.INTER_IDX == self.inter_idx:
                        self.log(f"has received request to stop")
                        done = True
                        break
                    else:
                        self.log(f"has received a deprecated request to stop")
            for package in new_pac:
                self.received_packages.append(package)

            if done:
                self.sent = {}
                break

            if len(self.sent) > 0:
                seed = next(iter(self.sent.keys()))
                message, inter_idx = self.sent[seed]
                if inter_idx < self.inter_idx:
                    self.sent[seed]
                    continue

                self.log(f'retry to send seed=${seed}!{self.inter_idx}')
                self.sendto(message)

        return len(data)

    def spin(self):
        chunks = []
        packages = deque()
        seeds_to_confirm = []
        sent = {}
