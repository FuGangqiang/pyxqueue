import time

import redis
from pyxqueue import TaskQueue

client = redis.Redis()
queue = TaskQueue(client, stream_key='progress')


@queue.task
def long_work():
    for i in range(100):
        queue.update_task_progress(i)
        time.sleep(1)


if __name__ == '__main__':
    import sys
    usage = 'Usage: python progress.py (worker | test)'
    if len(sys.argv) != 2:
        print(usage)
    elif sys.argv[1] == 'worker':
        queue.run()
    elif sys.argv[1] == 'test':
        long_work()
    else:
        print(usage)
