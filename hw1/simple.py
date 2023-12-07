with open('.mode', mode='w') as fd:
    fd.write('simple')

import protocol
from protocol import MyTCPProtocol
from threading import Thread

a = MyTCPProtocol(None)
b = MyTCPProtocol(a)
a.other = b

BIG_SIZE = 10_0

message_pool = ["I can see you.", "I'm afraid you're going to jail", "You. Not me.",
                "Be not afraid as the end has passed and you're still here."]
message_pool = [(x + ' ') * (BIG_SIZE // len(x)) for x in message_pool]
message_pool = [x[:BIG_SIZE] for x in message_pool]

def truncate(s):
    bound = 65
    if len(s) < bound:
        return s
    return s[:bound - 3] + '...'

def server(x, idx):
    A, B, C = 0, 3, 6
    A, B, C = 0, 1, 2
    name = '\033[34;1mMr B\033[0m' if idx == 0 else '\033[31;1mMr J\033[0m'
    for transaction in range(0, 99):
        if (transaction + (A if idx == 0 else B)) % C < B:
            message = message_pool[transaction % len(message_pool)]
            print(f'{name} sends {truncate(message)}')
            x.send(str.encode(message))
        else:
            message = x.recv(BIG_SIZE).decode('utf-8')
            print(f'{name} heard {truncate(message)}')

def main():
    thread_a = Thread(target=server, args=(a,0))
    thread_b = Thread(target=server, args=(b,1))
    thread_a.start()
    thread_b.start()
    thread_a.join()
    thread_b.join()


# cProfile.run('main()')

# yappi.start()
main()
# yappi.stop()
#
# # retrieve thread stats by their thread id (given by yappi)
# threads = yappi.get_thread_stats()
# for thread in threads:
#     print(
#         "Function stats for (%s) (%d)" % (thread.name, thread.id)
#     )  # it is the Thread.__class__.__name__
#     yappi.get_func_stats(ctx_id=thread.id).print_all()
#
