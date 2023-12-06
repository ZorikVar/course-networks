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

        nr_left_bytes = self.LENGTH - 10 - 4 * self.NR_SEEDS
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

class MyTCPProtocol(BaseProtocol):
    nr_nodes = 0

    def llog(self, *args):
        print(self.name, *args)

    def log(self, *args):
        # print(self.name, *args)
        pass

    def __init__(self, *args, **kwargs):
        self.name = '\033[34;1mMr B\033[0m' if MyTCPProtocol.nr_nodes == 0 else '\033[31;1mMr J\033[0m'
        MyTCPProtocol.nr_nodes += 1
        self.id = MyTCPProtocol.nr_nodes

        super().__init__(*args, **kwargs)

        self.package_wrapper = PackageWrapper()

        self.inter_idx = 0

        self.received_chunks = []
        self.half_ass_chunk = None

        self.next_seed = 555 if self.id == 1 else 7777
        self.received_packages = deque()
        self.seeds_to_confirm = []
        self.sent = {}

        self.passive_listener = None

    def listen(self, max_duration = 0.00001):
        try:
            self.set_timeout(max_duration)
            chunk = self.recvfrom(1500)
            if len(chunk) == 0:
                return None
            self.received_chunks.append(chunk)
        except TimeoutError:
            pass
        except Exception as e:
            self.log("EXCEPTION")
            self.log(e)
            raise

    def idle_function(self):
        o_idle_duration = 2

        s_time = default_timer()
        self.try_confirm(True)
        while not self.stop_idle and (default_timer() - s_time) < o_idle_duration:
            self.listen()
            while len(self.received_chunks) > 0:
                package = self.package_wrapper.feed(self.received_chunks.pop(0))
                if package is None:
                    continue
                self.log(f'received segment {format(package.PAYLOAD)}; seed=${package.SEED}!{self.inter_idx}')

                for seed in package.SEEDS:
                    self.log(f"considers confirmed {seed}")
                    if seed in self.sent:
                        del self.sent[seed]

                if len(package.PAYLOAD):
                    self.received_packages.append(package)
            if self.received_packages:
                package = self.received_packages.popleft()
                if package.INTER_IDX == self.inter_idx:
                    self.ask_to_stop()

    def idle_start(self):
        self.stop_idle = False
        self.passive_listener = Thread(target=MyTCPProtocol.idle_function, args=(self,))
        self.passive_listener.start()

    def idle_stop(self):
        self.stop_idle = True
        if self.passive_listener is None:
            return
        self.passive_listener.join()


    def recv(self, n: int):
        self.inter_idx += 1
        self.idle_stop()
        self.log(f'ready for transacion ${self.inter_idx} as listener')

        res = bytearray(n)
        for i in range(n):
            res[i] = 0

        nr_collected = 0
        nr_required = None
        while nr_required is None or nr_collected < nr_required:
            self.listen()
            self.try_confirm()

            while len(self.received_chunks) > 0:
                package = self.package_wrapper.feed(self.received_chunks.pop(0))
                if package is None:
                    continue
                self.log(f'received segment {format(package.PAYLOAD)}; seed=${package.SEED}!{self.inter_idx}')

                if package.INTER_IDX < self.inter_idx:
                    self.llog('got a message from a weirdly old interaction')

                for seed in package.SEEDS:
                    self.log(f"considers confirmed {seed}")
                    if seed in self.sent:
                        del self.sent[seed]

                self.seeds_to_confirm.append(package.SEED)

                if len(package.PAYLOAD):
                    self.received_packages.append(package)

            while self.received_packages:
                package = self.received_packages.popleft()
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
        self.seeds_to_confirm = []
        self.sent = {}
        self.idle_start()

        self.log(f"received ${self.inter_idx}: {format(res)}\n")
        return res




    def send_message(self, segment, *, message_type = None):
        o_nr_max_seeds = 10

        self.next_seed += 1
        SEED = self.next_seed
        if segment is not None:
            TYPE = PackageType.FINAL_SEGMENT if segment.is_final else PackageType.GENERAL
        elif message_type is not None:
            TYPE = message_type
        else:
            TYPE = PackageType.GENERAL
        NR_SEEDS = min(len(self.seeds_to_confirm), o_nr_max_seeds)
        LENGTH = 14 + NR_SEEDS * 4 + (len(segment.value) if segment is not None else 0)

        out = Encoder()
        out.int32(self.inter_idx)
        out.int32(LENGTH)
        out.int32(SEED)
        out.int8(TYPE)
        out.int8(NR_SEEDS)

        for _ in range(NR_SEEDS):
            seed = self.seeds_to_confirm.pop(0)
            self.log(f"confirm seed=${seed}!{self.inter_idx}")
            out.int32(seed)

        if segment is not None and len(segment.value):
            out.int32(segment.start)
            out.raw_str(segment.value)

        encoded = out.load()
        self.sendto(encoded)

        if segment is not None:
            maybe_fin = ' final' if TYPE == PackageType.FINAL_SEGMENT else ''
            self.log(f"is sending ${self.inter_idx}'s{maybe_fin} segment (\033[32;3mseed=${SEED}!{self.inter_idx})\033[0m: {format(segment.value)}; start={segment.start}")
            self.sent[SEED] = (encoded, self.inter_idx)
        elif message_type == PackageType.STOP_SENDING:
            self.log(f'send "please, stop" seed=${SEED}!{self.inter_idx}')
        else:
            self.log(f"send a segment <none> seed=${SEED}!{self.inter_idx}")
            self.sent[SEED] = (encoded, self.inter_idx)

    def ask_to_stop(self):
        return self.send_message(None, message_type=PackageType.STOP_SENDING)

    def try_confirm(self, force = False):
        o_nr_hanging_confirms = 5

        if len(self.seeds_to_confirm) < o_nr_hanging_confirms or force:
            return
        return self.send_message(None)

    def send_segment(self, segment: Segment):
        return self.send_message(segment)

    def send(self, data: bytes):
        o_nr_hanging_segments = 10
        o_segment_len = 500

        self.inter_idx += 1
        self.idle_stop()
        self.log(f'ready for transaction ${self.inter_idx} to send {format(bytearray(data))}')

        retry_ms = 0.002

        done = False
        i = 0
        while len(self.sent) or i < len(data):
            if len(self.sent) < o_nr_hanging_segments and i < len(data):
                start = i
                end = min(len(data), start + o_segment_len)
                i = end
                self.send_segment(Segment(start, data[start:end], end == len(data), None))
                continue

            self.listen(retry_ms)

            while len(self.received_chunks) > 0:
                package = self.package_wrapper.feed(self.received_chunks.pop(0))
                if package is None:
                    continue
                self.log(f'received segment {format(package.PAYLOAD)}; seed=${package.SEED}!{self.inter_idx}')

                if package.INTER_IDX < self.inter_idx:
                    self.llog('got a message from a weirdly old interaction')

                for seed in package.SEEDS:
                    self.log(f"considers confirmed {seed}")
                    if seed in self.sent:
                        del self.sent[seed]

                self.seeds_to_confirm.append(package.SEED)

                if len(package.PAYLOAD):
                    self.received_packages.append(package)

                if package.INTER_IDX > self.inter_idx:
                    self.log(f"sees his partner has moved on")
                    done = True

                if package.TYPE == PackageType.STOP_SENDING:
                    if package.INTER_IDX == self.inter_idx:
                        self.log(f"has received request to stop")
                        done = True
                    else:
                        self.log(f"has received a deprecated request to stop")

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
