from concurrent import futures


def thread_pool_wrapper(func):

    def get_thread_pool_executor(*args, **kwargs) -> futures.Future:
        if 'key' not in kwargs:
            raise Exception('key is passed as named argument in current function call')
        key = kwargs['key']
        self = args[0]
        key = hash(key) % len(self.thread_pool)
        # submit is non-blocking and immediately returns future object
        return self.thread_pool[key].submit(func, *args, **kwargs)
        # to wait till all future are completed
        # concurrent.futures.wait([future1, future2, future3])
    return get_thread_pool_executor


def syncronized(func):
    def wrapper(*args, **kwargs):
        obj = args[0]

        if not hasattr(obj, 'lock'):
            raise Exception('lock attribute is missing in current class object')
        lock = obj.lock
        with lock:
            return func(*args, **kwargs)
    return wrapper
