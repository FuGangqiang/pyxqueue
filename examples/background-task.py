import time

import redis
from pyxqueue import TaskQueue

client = redis.Redis()
queue = TaskQueue(client, stream_key='background-task')


@queue.task
def sleep(n):
    print('going to sleep for %s seconds' % n)
    time.sleep(n)
    print('woke up after %s seconds' % n)


if __name__ == '__main__':
    import sys
    usage = 'Usage: python background-task.py (worker | test)'
    if len(sys.argv) != 2:
        print(usage)
    elif sys.argv[1] == 'worker':
        queue.run()
    elif sys.argv[1] == 'test':
        sleep(2)
    else:
        print(usage)
