with open('.mode', mode='w') as fd:
    fd.write('simple')

import protocol
from protocol import MyTCPProtocol
from threading import Thread

a = MyTCPProtocol(None)
b = MyTCPProtocol(a)
a.other = b

BIG_SIZE = 20000

message_pool = ["I can see you.", "I'm afraid you're going to jail", "You. Not me.",
                "Be not afraid as the end has passed and you're still here."]

message_pool = [(x + ' ') * BIG_SIZE for x in message_pool]
message_pool = [x[:BIG_SIZE] for x in message_pool]

def truncate(s):
    bound = 65
    if len(s) < bound:
        return s
    return s[:bound - 3] + '...'

def server(x, idx):
    name = '\033[34;1mMr B\033[0m' if idx == 0 else '\033[31;1mMr J\033[0m'
    for transaction in range(0 if idx == 0 else 3, 999999):
        if transaction % 6 < 3:
            message = message_pool[transaction % len(message_pool)]
            print(f'{name} sends {truncate(message)}')
            x.send(str.encode(message))
        else:
            message = x.recv(BIG_SIZE).decode('utf-8')
            print(f'{name} heard {truncate(message)}')

thread_a = Thread(target=server, args=(a,0))
thread_b = Thread(target=server, args=(b,1))
thread_a.start()
thread_b.start()
thread_a.join()
thread_b.join()
