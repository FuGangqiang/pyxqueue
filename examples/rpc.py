import redis
from pyxqueue import TaskQueue

client = redis.Redis()
queue = TaskQueue(client, stream_key='rpc')


@queue.task
def fib(n):
    a, b = 0, 1
    for _ in range(n):
        a, b = b, a + b
    return b


if __name__ == '__main__':
    import sys
    usage = 'Usage: python rpc.py (worker | test)'
    if len(sys.argv) != 2:
        print(usage)
    elif sys.argv[1] == 'worker':
        queue.run(4)
    elif sys.argv[1] == 'test':
        fib_100k = fib(100_000)
        fib_200k = fib(200_000)
        fib_300k = fib(300_000)
        fib_400k = fib(400_000)

        print('100kth fib number starts ends with: %s' % str(fib_100k.result())[-6:])
        print('200kth fib number starts ends with: %s' % str(fib_200k.result())[-6:])
        print('300kth fib number starts ends with: %s' % str(fib_300k.result())[-6:])
        print('400kth fib number starts ends with: %s' % str(fib_400k.result())[-6:])
    else:
        print(usage)
