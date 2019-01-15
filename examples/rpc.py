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
        queue.run()
    elif sys.argv[1] == 'test':
        fib_100k_result = fib(100_000)
        fib_200k_result = fib(200_000)
        fib_300k_result = fib(300_000)
        fib_400k_result = fib(400_000)
        print(f'100kth fib number starts ends with: {str(fib_100k_result.get())[-6:]}')
        print(f'200kth fib number starts ends with: {str(fib_100k_result.get())[-6:]}')
        print(f'300kth fib number starts ends with: {str(fib_100k_result.get())[-6:]}')
        print(f'400kth fib number starts ends with: {str(fib_100k_result.get())[-6:]}')
    else:
        print(usage)
