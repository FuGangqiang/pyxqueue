# pyxqueue

一个异步多进程任务队列，利用 redis streams 功能实现消息存储。


## 安装

`pyxqueue` 使用了 python 3.7 的一些特性，你需要使用 python 3.7 解释器或者更高版本。

```
pip install pyxqueue
```


## 使用


### 后台异步任务

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


### 远程过程调用(RPC)

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


### task 属性

```
>>> task_id = b'1551943344215-0'
>>> queue.get_task(task_id)
{
    'task_id': b'1551943344215-0',
    'info': {
        'state': 2,
        'value': None,
        'worker': 'worker-1-66202570-40a9-11e9-bc87-00163e0eb975',
        'update_time': 1551943344
    },
    'data': {
        b'task': b'{"task_name": "queues.spider.download", "args": [75], "kwargs": {}}'
    }
}
```


### 更新任务进度

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


## 备注

原理参考于[http://charlesleifer.com/blog/multi-process-task-queue-using-redis-streams/](http://charlesleifer.com/blog/multi-process-task-queue-using-redis-streams/)。
