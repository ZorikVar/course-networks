with open('.mode', mode='w') as fd:
    fd.write('simple')

import time
from protocol import MyTCPProtocol
from threading import Thread

import protocol

a = MyTCPProtocol(None)
b = MyTCPProtocol(a)
a.other = b

NR_INTER = 800
BIG_SIZE = 10

message_pool = ["I can see you.", "I'm afraid you're going to jail", "You. Not me.",
                "Be not afraid as the end has passed and you're still here."]
message_pool = [(x + ' ') * (1 + BIG_SIZE // len(x)) for x in message_pool]
message_pool = [x[:BIG_SIZE] for x in message_pool]


def fmt_truncate(s):
    bound = 65
    if len(s) < bound:
        return s
    return s[:bound - 3] + '...'


def server(x, idx):
    A, B = 1, 2
    name = '\033[34;1mMr B\033[0m' if idx == 0 else '\033[31;1mMr J\033[0m'
    for transaction in range(0, NR_INTER):
        if (transaction % B < A) != (idx == 0):
            message = message_pool[transaction % len(message_pool)]
            # print(f'{name} sends {fmt_truncate(message)}')
            x.send(str.encode(message))
        else:
            message = x.recv(BIG_SIZE).decode('utf-8')
            # print(f'{name} heard {fmt_truncate(message)}')


def main():
    thread_a = Thread(target=server, args=(a,0))
    thread_b = Thread(target=server, args=(b,1))
    thread_a.start()
    thread_b.start()
    thread_a.join()
    thread_b.join()


start_time = time.time()
main()
duration = time.time() - start_time
print(f"\033[34m--- \033[1;35m{duration:.3f} \033[0;34mseconds ---\033[0m")
