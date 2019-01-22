# pyxqueue

a multi-processes task queue using redis streams.

heavily inspired from: [http://charlesleifer.com/blog/multi-process-task-queue-using-redis-streams/](http://charlesleifer.com/blog/multi-process-task-queue-using-redis-streams/)

## install

```
pip install pyxqueue
```

## Usage

### background task

```
import time

import redis
from pyxqueue import TaskQueue

client = redis.Redis()
queue = TaskQueue(client, stream_key='background-task')


@queue.task
def sleep(n):
    print(f'going to sleep for {n} seconds')
    time.sleep(n)
    print(f'woke up after {n} seconds')


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
```

### rpc

```
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
        print(f'100kth fib number starts ends with: {str(fib_100k_result.get())[-6:]}')
    else:
        print(usage)
```

### progress

```
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
```

to get task progress:

```
queue.get_task_progress(task_id)
```
