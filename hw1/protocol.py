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
        from debug_protocol import InMemoryProtocol as BaseProtocol
    else:
        print("Hard mode")
        from judge_protocol import UDPBasedProtocol as BaseProtocol
        print = log


def clock():
    return time.time_ns() // 1_000_000

"""
Each 
- INTER_IDX:       int32
- LENGTH:          int32
- SEED:            int32
- META:            int8
- NR_SEEDS:        int8
- ...SEEDS:        ...int32
- ?SEGMENT_START:  int32
- ?PAYLOAD:        bytes
"""

class Metadata(IntEnum):
    GENERAL       = 0
    FINAL_SEGMENT = 1
    STOP_SENDING  = 2


class Package:
    def __init__(self, raw, dbg_id = 0):
        self.parser = Decoder(raw)

        self.INTER_IDX = self.parser.int32()
        self.LENGTH = self.parser.int32()
        self.SEED = self.parser.int32()
        self.META = self.parser.int8()
        self.NR_SEEDS = self.parser.int8()

        self.SEEDS = [self.parser.int32() for _ in range(self.NR_SEEDS)]
        self.SEGMENT_START = -1
        self.PAYLOAD = b''

        nr_left_bytes = self.LENGTH - 14 - 4 * self.NR_SEEDS
        if nr_left_bytes > 0:
            self.SEGMENT_START = self.parser.int32()
            self.PAYLOAD = self.parser.raw_str(nr_left_bytes - 4)


class PackageWrapper:
    def __init__(self):
        self.incomplete = None

    def feed(self, chunk):
        if self.incomplete is not None:
            chunk = self.incomplete + chunk
            self.incomplete = None

        try:
            return Package(chunk)
        except ValueError as e:
            if str(e) != 'not enough input bytes':
                raise e
            self.incomplete = chunk
            return None


class Pipe:
    seed = 0

    def __init__(self, channel):
        self.channel = channel
        self.next_seed = 555 if Pipe.seed % 2 == 0 else 7777
        self.package_wrapper = PackageWrapper()

        Pipe.seed += 1

    def incoming(self, max_duration = 0.00001):
        try:
            self.channel.set_timeout(max_duration)
            chunk = self.channel.recvfrom(999999999)
            package = self.package_wrapper.feed(chunk)
            return [] if package is None else [package]
        except TimeoutError:
            return []

    def send_package(self, segment, seeds_to_confirm, *, inter_idx, meta = None):
        o_nr_max_seeds = 30

        INTER_IDX = inter_idx
        SEED = self.next_seed
        self.next_seed += 1
        META = Metadata.GENERAL
        if meta is not None:
            META = meta
        elif segment is not None and segment.is_final:
            META = Metadata.FINAL_SEGMENT
        NR_SEEDS = min(len(seeds_to_confirm), o_nr_max_seeds)
        LENGTH = 14 + NR_SEEDS * 4 + (0 if segment is None else 4 + len(segment.value))

        out = Encoder()
        out.int32(INTER_IDX)
        out.int32(LENGTH)
        out.int32(SEED)
        out.int8(META)
        out.int8(NR_SEEDS)

        for _ in range(NR_SEEDS):
            seed = seeds_to_confirm.pop(0)
            out.int32(seed)

        if segment is not None and len(segment.value):
            out.int32(segment.start)
            out.raw_str(segment.value)

        message = out.load()
        self.channel.sendto(message)
        return SEED, message


class Segment:
    def __init__(self, start: int, value: bytes, is_final: bool, seed: int):
        self.start = start
        self.value = value
        self.is_final = is_final
        self.seed = seed


def fmt_seed(seed, inter_idx):
    return f'\033[32;3mseed=${seed}!{inter_idx}\033[0m'


o_no_hear = 300
o_retry_ms = 45
o_no_hear = 1500
o_retry_ms = 350

class MyTCPProtocol(BaseProtocol):
    nr_nodes = 0

    def log2(self, *args):
        return
        print(self.name, *args)

    def log1(self, *args):
        return
        print(self.name, *args)

    def log(self, *args):
        return
        print(self.name, *args)

    def log_sent_package(self, message):
        package = PackageWrapper().feed(message)
        if package is None:
            self.log('can\'t parse his own message')
            raise Exception('logical error')

        INTER_IDX = package.INTER_IDX
        SEED = package.SEED
        META = package.META

        for confirmed in package.SEEDS:
            self.log(f"confirms {fmt_seed(confirmed, INTER_IDX)}")

        if len(package.PAYLOAD):
            maybe_fin = ' final' if META == Metadata.FINAL_SEGMENT else ''
            self.log1(f"is sending ${INTER_IDX}'s{maybe_fin} segment ({fmt_seed(SEED,  package.INTER_IDX)}): {format(package.PAYLOAD)}; start={package.SEGMENT_START}")
        elif META == Metadata.STOP_SENDING:
            self.log1(f'is sending "please, stop" {fmt_seed(SEED, INTER_IDX)}')
        else:
            self.log1(f"is sending a segment <none> {fmt_seed(SEED, INTER_IDX)}")

    def __init__(self, *args, **kwargs):
        self.name = '\033[34;1mMr B\033[0m' if MyTCPProtocol.nr_nodes % 2 == 0 else '\033[31;1mMr J\033[0m'
        MyTCPProtocol.nr_nodes += 1

        super().__init__(*args, **kwargs)

        self.pipe = Pipe(super())
        self.received_packages = []
        self.inter_idx = 0

    def send_segment(self, segment: Segment, seeds_to_confirm):
        seed, message = self.pipe.send_package(segment, seeds_to_confirm, inter_idx=self.inter_idx)
        self.log_sent_package(message)
        return seed, message

    def send(self, data):
        start_time = clock()

        self.inter_idx += 1

        o_nr_hanging_segments = 10
        o_segment_len = 1500

        NEXT_SEGMENT = 1
        CHECK_INCOMING = 2
        RETRY_SEGMENT = 3
        DONE = 4

        chunks = []
        packages = []
        seeds_to_confirm = []
        sent = {}
        i = 0

        state = NEXT_SEGMENT
        latest_retry = clock()
        last_heard = clock()

        next_inter = []

        while state != DONE:
            # self.log(f'is in state {state}')
            if state == NEXT_SEGMENT:
                if i == len(data):
                    state = DONE if len(sent) == 0 else CHECK_INCOMING
                    continue

                if len(sent) == o_nr_hanging_segments:
                    state = CHECK_INCOMING
                    continue

                start, end = i, i + o_segment_len
                end = min(end, len(data))
                segment = Segment(start, data[start:end], end == len(data), None)

                seed, message = self.send_segment(segment, seeds_to_confirm)
                i = end
                sent[seed] = message

            elif state == CHECK_INCOMING:
                if clock() - last_heard > o_no_hear:
                    self.log1('GOT FUCKING TIRED')
                    state = DONE
                    continue

                packages = self.pipe.incoming(0.01)
                if len(packages) == 0:
                    state = RETRY_SEGMENT
                    continue

                last_heard = clock()
                for package in packages:
                    self.log1(f'received segment {format(package.PAYLOAD)}; {fmt_seed(package.SEED, package.INTER_IDX)}')

                    if package.INTER_IDX < self.inter_idx:
                        self.log1(f'got a message from a weirdly old interaction {fmt_seed(package.SEED, package.INTER_IDX)}')
                        continue
                    elif package.INTER_IDX > self.inter_idx:
                        self.log(f"sees his partner has moved on")
                        next_inter.append(package)
                        state = DONE
                        continue
                    elif package.META == Metadata.STOP_SENDING:
                        self.log(f"has received a request to stop")
                        state = DONE
                        continue

                    for seed in package.SEEDS:
                        self.log(f"considers confirmed {seed}")
                        if seed in sent:
                            del sent[seed]
                    seeds_to_confirm.append(package.SEED)

                if state == CHECK_INCOMING:
                    state = NEXT_SEGMENT

            elif state == RETRY_SEGMENT:
                now = clock()
                if now - latest_retry < o_retry_ms:
                    state = CHECK_INCOMING
                    continue
                latest_retry = now

                if len(sent) == 0:
                    raise Exception('logic error')

                seed = next(iter(sent.keys()))
                self.log1(f'retries to send {fmt_seed(seed, self.inter_idx)}')
                self.sendto(sent[seed])

            else:
                raise Exception('invalid state')

        self.log(f'collected {next_inter} for the next interaction')
        self.received_packages = next_inter

        self.log2(clock() - start_time)

        return len(data)

    def recv(self, n: int):
        start_time = clock()

        self.inter_idx += 1
        self.log1(f'ready for transaction ${self.inter_idx} as listener')

        buff = bytearray(n)
        for i in range(n):
            buff[i] = 0

        received = self.received_packages
        self.log(f'started listening with {received} packages')
        self.received_packages = []

        sent = set()
        seen = set()
        seeds_to_confirm = []
        nr_collected = 0
        nr_required = None
        latest_ask_to_stop = clock()
        last_heard = clock()

        LISTEN = 1
        PROCESS_RECEIVED = 2
        CONFIRM = 3
        ASK_TO_STOP = 4
        DONE = 5

        state = PROCESS_RECEIVED

        while state != DONE:
            # self.log(f'is in state {state}')
            if state == LISTEN:
                if clock() - last_heard > o_no_hear:
                    self.log1('GOT FUCKING TIRED')
                    state = DONE
                    continue

                for package in self.pipe.incoming():
                    self.log1(f'received segment {format(package.PAYLOAD)}; {fmt_seed(package.SEED, package.INTER_IDX)}')
                    received.append(package)
                    state = PROCESS_RECEIVED
                    last_heard = clock()

            elif state == PROCESS_RECEIVED:
                for package in received:
                    for seed in package.SEEDS:
                        self.log(f"considers confirmed {seed}")
                        if seed in sent:
                            del sent[seed]

                    if package.INTER_IDX < self.inter_idx:
                        self.log1(f'got a message from a weirdly old interaction {fmt_seed(package.SEED, package.INTER_IDX)}')
                        state = ASK_TO_STOP
                        continue

                    seeds_to_confirm.append(package.SEED)

                    if package.SEGMENT_START in seen:
                        continue
                    seen.add(package.SEGMENT_START)

                    i = package.SEGMENT_START
                    s = package.PAYLOAD
                    nr_collected += len(s)
                    if package.META == Metadata.FINAL_SEGMENT:
                        nr_required = i + len(s)
                    buff[i:i + len(s)] = s

                    if nr_collected == nr_required:
                        state = DONE
                received = []
                if state == PROCESS_RECEIVED:
                    state = CONFIRM

            elif state == CONFIRM:
                seed, message = self.pipe.send_package(None, seeds_to_confirm, inter_idx=self.inter_idx)
                self.log_sent_package(message)
                state = LISTEN

            elif state == ASK_TO_STOP:
                now = clock()
                if now - latest_ask_to_stop < 200:
                    state = LISTEN
                    continue
                latest_ask_to_stop = now

                seed, message = self.pipe.send_package(None, [], inter_idx=self.inter_idx, meta=Metadata.STOP_SENDING)
                self.log_sent_package(message)
                state = LISTEN

        self.log1(f"received ${self.inter_idx}: {format(buff)}\n")

        if nr_required is None or nr_collected < nr_required:
            self.log1("RECEIVED INCOMPLETE DATA")
            raise Exception("RECEIVED INCOMPLETE DATA")

        self.log2(clock() - start_time)

        return buff
