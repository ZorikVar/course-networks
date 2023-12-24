from protocol_in_memory import *

def format(x):
    if type(x) is bytearray or type(x) is bytes:
        return [int(x) for x in x[:5]] + ['...']
    return x


nr_logged = 0


def noop(*args, **kwargs):
    pass


def log(*args, **kwargs):
    global nr_logged
    if nr_logged > 100000000000:
        return
    nr_logged += 1
    args = (format(x) for x in args)
    print(*args, **kwargs, file=open("log.txt", "a"))


def print_fmt(*args, **kwargs):
    args = (format(x) for x in args)
    print(*args, **kwargs)
