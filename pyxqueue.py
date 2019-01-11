import json
import time
import multiprocessing
from functools import wraps


class TaskError(Exception):

    def __init__(self, error):
        super().__init__()
        self.error = error

    def __str__(self):
        return str(self.error)


class TaskQueue:

    def __init__(self, client, stream_key='xtasks', consumer_group='cg', worker_prefix=''):
        self.client = client  #  Redis client.
        self.stream_key = stream_key
        self.worker_prefix = worker_prefix
        self.consumer_group = consumer_group
        self.result_key = stream_key + '.results'  # Store results in a Hash
        self.signal = multiprocessing.Event()  # Used to signal shutdown
        self.signal.set()  # Indicate the server is not running
        self._tasks = dict()
        self.ensure_stream_and_consumer_group()

    def ensure_stream_and_consumer_group(self):
        if not self.client.exists(self.stream_key):
            self.client.xgroup_create(self.stream_key, self.consumer_group, mkstream=True)
        else:
            group_name_set = [x['name'].decode() for x in self.client.xinfo_groups(self.stream_key)]
            if self.consumer_group not in group_name_set:
                self.client.xgroup_create(self.stream_key, self.consumer_group)

    def task(self, fn):
        self._tasks[fn.__name__] = fn

        @wraps(fn)
        def inner(*args, **kwargs):
            message = self.serialize_message(fn, args, kwargs)
            task_id = self.client.xadd(self.stream_key, {'task': message})
            return AsyncResult(self, task_id)

        return inner

    def deserialize_message(self, message):
        message = json.loads(message)
        task_name = message['task_name']
        args = message['args']
        kwargs = message['kwargs']
        if message['task_name'] not in self._tasks:
            raise Exception('task "{}" not registered with queue.'.format(task_name))
        return self._tasks[task_name], args, kwargs

    def serialize_message(self, task, args=None, kwargs=None):
        return json.dumps(dict(task_name=task.__name__, args=args, kwargs=kwargs))

    def store_result(self, task_id, result):
        if result is not None:
            self.client.hset(self.result_key, task_id, json.dumps(result))

    def get_result(self, task_id):
        pipe = self.client.pipeline()
        pipe.hexists(self.result_key, task_id)
        pipe.hget(self.result_key, task_id)
        pipe.hdel(self.result_key, task_id)
        exists, val, _ = pipe.execute()
        return json.loads(val) if exists else None

    def run(self, nworkers=1):
        if not self.signal.is_set():
            raise Exception('workers are already running')

        self._pool = []
        self.signal.clear()
        for _ in range(nworkers):
            worker = TaskWorker(self)
            worker_t = multiprocessing.Process(target=worker.run)
            worker_t.start()
            self._pool.append(worker_t)

        import sys
        import signal

        def int_handler(_sig, _frame):
            self.shutdown()
            sys.exit(0)

        signal.signal(signal.SIGINT, int_handler)
        print('Press Ctrl+C')
        signal.pause()


    def shutdown(self):
        if self.signal.is_set():
            raise Exception('workers are not running')

        self.signal.set()
        for worker_t in self._pool:
            worker_t.join()


class TaskWorker:
    _worker_idx = 0

    def __init__(self, queue):
        TaskWorker._worker_idx += 1
        self.queue = queue
        self.client = queue.client
        self.stream_key = queue.stream_key
        self.consumer_group = queue.consumer_group
        self.worker_name = '{worker_prefix}worker-{index}'.format(worker_prefix=queue.worker_prefix,
                                                                  index=TaskWorker._worker_idx)

    def run(self):
        while not self.queue.signal.is_set():
            pending_resp = self.client.xpending_range(
                self.stream_key,
                self.consumer_group,
                consumername=self.worker_name,
                count=1,
            )
            if pending_resp:
                task_id = pending_resp[0]['message_id']
                xrange_resp = self.client.xrange(self.stream_key, task_id, count=1)
                _task_id, data = xrange_resp[0]
                if task_id == _task_id:
                    self.execute(task_id.decode(), data[b'task'])
                continue

            resp = self.client.xreadgroup(
                self.consumer_group,
                self.worker_name,
                {self.stream_key: '>'},
                count=1,
                block=1000,
            )
            for _stream_key, message_list in resp:
                task_id, data = message_list[0]
                self.execute(task_id.decode(), data[b'task'])

    def execute(self, task_id, message):
        task, args, kwargs = self.queue.deserialize_message(message)
        try:
            ret = task(*(args or ()), **(kwargs or {}))
        except Exception as e:
            self.queue.store_result(task_id, TaskError(e))
        else:
            self.queue.store_result(task_id, ret)
            self.client.xack(self.stream_key, self.consumer_group, task_id)


class AsyncResult:

    def __init__(self, queue, task_id):
        self.queue = queue
        self.task_id = task_id
        self._result = None

    def result(self, block=True, timeout=None):
        if self._result is None:
            if not block:
                result = self.queue.get_result(self.task_id)
            else:
                start = time.time()
                while timeout is None or (start + timeout) > time.time():
                    result = self.queue.get_result(self.task_id)
                    if result is None:
                        time.sleep(0.1)
                    else:
                        break
            if result is not None:
                self._result = result

        if self._result is not None and isinstance(self._result, TaskError):
            raise Exception('task failed: {}'.format(self._result))
        return self._result
