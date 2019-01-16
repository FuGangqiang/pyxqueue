import os
import enum
import json
import time
import traceback
import multiprocessing
from functools import wraps


class TaskError(Exception):

    def __init__(self, error, exc_info):
        super().__init__()
        self.error = error
        self.exc_info = exc_info

    def __str__(self):
        return 'TaskError: {}\n{}'.format(self.error, self.exc_info)


class TaskStatus(enum.Enum):
    PENDING = 1
    STARTED = 2
    RETRY = 3
    FAILURE = 4
    SUCCESS = 5


class TaskQueue:

    def __init__(self, client, stream_key='stream', consumer_group='cg', worker_prefix=''):
        self.client = client  #  Redis client
        self.stream_key = 'xqueue.' + stream_key  # Store tasks in a stream
        self.result_key = self.stream_key + '.results'  # Store results in a Hash
        self.worker_key = self.stream_key + '.workers'  # Store workers in a Hash
        self.worker_prefix = worker_prefix
        self.consumer_group = consumer_group
        self.shutdown_flag = multiprocessing.Event()
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
        fn_key = self.get_fn_key(fn)
        if fn_key in self._tasks:
            raise Exception('this function name already register, use other name instead')
        self._tasks[fn_key] = fn

        @wraps(fn)
        def inner(*args, **kwargs):
            message = self.serialize_message(fn, args, kwargs)
            task_id = self.create_task(message)
            return AsyncResult(self, task_id)

        return inner

    def get_fn_key(self, fn):
        mod = fn.__module__
        fn = fn.__name__
        return '{}.{}'.format(mod, fn)

    def serialize_message(self, task, args=None, kwargs=None):
        task_key = self.get_fn_key(task)
        return json.dumps(dict(task_name=task_key, args=args, kwargs=kwargs))

    def deserialize_message(self, message):
        message = json.loads(message)
        task_name = message['task_name']
        args = message['args']
        kwargs = message['kwargs']
        if message['task_name'] not in self._tasks:
            raise Exception('task "{}" not registered with queue.'.format(task_name))
        return self._tasks[task_name], args, kwargs

    def create_task(self, data):
        task_id = self.client.xadd(self.stream_key, {'task': data})
        self.update_task(task_id, TaskStatus.PENDING)
        return task_id

    def update_task(self, task_id, state, value=None, worker=None):
        body = {'state': state.value, 'value': value, 'worker': worker}
        self.client.hset(self.result_key, task_id, json.dumps(body))

    def update_worker(self, worker_id):
        now = int(time.time())
        pid = os.getpid()
        data = {'update_time': now, 'pid': pid}
        self.client.hset(self.worker_key, worker_id, json.dumps(data))

    def delete_worker(self, worker_id):
        self.client.hdel(self.worker_key, worker_id)

    def store_result(self, task_id, result, worker=None):
        if isinstance(result, TaskError):
            failed = True
            result = str(result)
        else:
            failed = False
        state = TaskStatus.FAILURE if failed else TaskStatus.SUCCESS
        self.update_task(task_id, state, result, worker=worker)

    def get_result(self, task_id):
        pipe = self.client.pipeline()
        pipe.hexists(self.result_key, task_id)
        pipe.hget(self.result_key, task_id)
        exists, body = pipe.execute()
        return json.loads(body) if exists else None

    def run(self, nworkers=0):
        import signal
        nworkers = nworkers or multiprocessing.cpu_count()

        _ = signal.signal(signal.SIGINT, signal.SIG_IGN)
        self._pool = []
        self.shutdown_flag.clear()
        for _ in range(nworkers):
            worker = TaskWorker(self)
            worker_t = multiprocessing.Process(target=worker.run)
            worker_t.start()
            self._pool.append(worker_t)

        def int_handler(_sig, _frame):
            import sys
            self.shutdown()
            sys.exit(0)

        signal.signal(signal.SIGINT, int_handler)
        print('{} worker processes started.'.format(nworkers))
        print('Press Ctrl+C to exit.')
        signal.pause()

    def shutdown(self):
        self.shutdown_flag.set()
        for worker_t in self._pool:
            worker_t.join()

    def task_total(self):
        return self.client.xlen(self.stream_key)

    def get_tasks(self, start='-', end='+', count=10):
        tasks = self.client.xrange(self.stream_key, start, end, count)
        infos = []
        for task_id, _data in tasks:
            info = json.loads(self.client.hget(self.result_key, task_id))
            infos.append(dict(task_id=task_id, info=info))
        return infos

    def get_task(self, task_id):
        info = json.loads(self.client.hget(self.result_key, task_id))
        return dict(task_id=task_id, info=info)

    def retry_task(self, task_id):
        _task_id, data = self.client.xrange(task_id, '+', count=1)[0]
        self.create_task(data[b'task'])
        self.client.xdel(self.stream_key, task_id)
        self.client.hdel(self.result_key, task_id)

    def clear_tasks(self, start='-', end='+', count=None):
        tasks = self.client.xrange(self.stream_key, start, end, count)
        task_ids = [x[0] for x in tasks]
        self.client.xdel(self.stream_key, *task_ids)
        self.client.hdel(self.result_key, *task_ids)

    def get_workers(self):
        workers = self.client.hgetall(self.worker_key)
        for k, v in workers.items():
            workers[k] = json.loads(v)
        return workers


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

    def update(self):
        self.queue.update_worker(self.worker_name)

    def delete(self):
        self.queue.delete_worker(self.worker_name)

    def run(self):
        while not self.queue.shutdown_flag.is_set():
            self.update()
            pending_resp = self.client.xpending_range(
                self.stream_key,
                self.consumer_group,
                consumername=self.worker_name,
                count=1,
            )
            if pending_resp:
                task_id = pending_resp[0]['message_id']
                self.queue.update_task(task_id, TaskStatus.RETRY, worker=self.worker_name)
                xrange_resp = self.client.xrange(self.stream_key, task_id, count=1)
                _task_id, data = xrange_resp[0]
                if task_id == _task_id:
                    print('pyxqueue: restart task {}: {}'.format(task_id, json.loads(data[b'task'])))
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
                self.queue.update_task(task_id, TaskStatus.STARTED, worker=self.worker_name)
                print('pyxqueue: start task {}: {}'.format(task_id, json.loads(data[b'task'])))
                self.execute(task_id.decode(), data[b'task'])
        self.delete()

    def execute(self, task_id, message):
        task, args, kwargs = self.queue.deserialize_message(message)
        try:
            ret = task(*(args or ()), **(kwargs or {}))
        except Exception as e:
            exc_info = traceback.format_exc()
            self.queue.store_result(task_id, TaskError(e, exc_info), worker=self.worker_name)
        else:
            self.queue.store_result(task_id, ret, worker=self.worker_name)
        self.client.xack(self.stream_key, self.consumer_group, task_id)


class AsyncResult:

    def __init__(self, queue, task_id):
        self.queue = queue
        self.task_id = task_id
        # {'state': int, 'value': result }
        self._result = None

    def _check_result(self):
        if self._result:
            return
        self._result = self.queue.get_result(self.task_id)

    @property
    def state(self):
        self._check_result()
        if self._result is None:
            return TaskStatus.PENDING
        return TaskStatus(self._result['state'])

    def get(self, timeout=None):
        start = time.time()
        while timeout is None or start + timeout > time.time():
            result = self.queue.get_result(self.task_id)
            if result is None:
                time.sleep(0.2)
                continue
            self._result = result
            if self.state == TaskStatus.SUCCESS:
                return result['value']
            if self.state == TaskStatus.FAILURE:
                raise Exception(self._result['value'])
        raise TimeoutError()
