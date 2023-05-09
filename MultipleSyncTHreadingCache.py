import concurrent
import time
from concurrent.futures.thread import ThreadPoolExecutor
from concurrent import futures
import threading
from Data_structure import *
from contants import *
import math
from datetime import datetime, timedelta
from copy import deepcopy

from utils import thread_pool_wrapper, syncronized


# here we will be having multiple threading, where each thread will update data synchronously to avoid
# read-your-own-writes issue.

# in this case we are using multiple threads, where each thread works synchronously, and each thread is
# responsible to update some set of keys. because each key always goes to specific thread and each thread works
# synchronously internally, that's why we don't need locks

# so,  here basically all read and write operations for a specific will always go to specific thread, due this
# consistency is maintained. but if you noticed,we are forced to perform delete operation by any thread,
# like in expire function, or when cache is full, we are deleting other keys from current thread.
# but deleting form anywhere is fine because, if entry is not there then we are fetching fresh data from db.
# as of now we have syncronized delete operation using locks, i.e only one thread can perform delete
# operation at a time. but that is not needed, instead we can check keys exists or not before deletion. because
# when it is not syncronized, then it may be possible that while delete operation other thread already deleted all data
# and then we can get key error in current thread delete operation, so we just need to handle that, if we are not using
# syncronization.

class MultiPleThreadCache:

    # all time is in UTC
    def __init__(self, expiry_time: int, max_size: int, fetch_algo,  eviction_algo, keys_to_early_load=None,sleep_period=1200, pool_size=5):
        self.expiry_time = timedelta(seconds=expiry_time)
        self.max_size = max_size
        self.fetch_algo = fetch_algo
        self.event_queue = []
        self.eviction_algo = eviction_algo
        self.keys_to_eager_load = keys_to_early_load
        self.cache = dict()

        self.keys_to_update = set()
        self.sleep_period = sleep_period
        self.priority_queue = PriorityQueue(max_size=max_size, comparator=self.expiry_queue_priority_generator)
        self.expiry_queue = PriorityQueue(max_size=max_size, comparator=lambda record: record.upload_time.timestamp())
        self.eager_load_data()
        self.thread_pool = []
        # if you have noticed we have few functions which are updating/removing more than one key item from cache,
        # in that case we have to call another thread from below functions which is already running on separate thread.
        # In that case we can get dead-lock.
        # let's consider an example of add operation, let say for key k1 we are using t1 thread but queue is full now
        # to we need to delete k2 for that we have to call thread t2 to delete k2 from t1
        # thread and we have to wait t1 till t2 completed it's delete operation, then only t1 can add data.
        # now suppose at the same time user initiated request to add k5 which used thread t2, and queue is full
        # so we need to delete k4 which will be performed on t1.
        # and this is happening concurrently, So now t1 is waiting for t2 thread to complete and t2 is waiting for
        # t1 thread to complete its task.
        # so to avoid this dead-locks we are forced to use global locks in places where we are updating more than one
        # keys. by adding locks we will remove the hold and wait condition.
        # now only one thread will be able to access these shared methods which update multiple keys and that
        # thread which hold resource, does not have to wait for anything  and
        # other thread will wait till it's free(waiting threads does not hold any resource as we have only
        # one global lock).
        self.lock = threading.Lock()
        for i in range(pool_size):
            self.thread_pool.append(ThreadPoolExecutor(max_workers=1))
        if self.fetch_algo == FetchAlgorithm.WRITE_BACK:
            self.cron_to_periodically_sync_with_db_for_write_back()

    def expiry_queue_priority_generator(self, data: PriorityQueueRecord):
        # we build max head, so need to multiply by -1 ,so that least recently used will be on top
        if self.eviction_algo == EvictionAlgo.LRU:
            return data.access_time.timestamp() * -1
        else:
            number_of_left_digits_from_decimal = math.floor(math.log(data.access_time.timestamp(), 10))+1
            res = data.access_time.timestamp()/math.pow(10, number_of_left_digits_from_decimal)
            # as we know frequency will always be a whole number, as we only increment it by one.
            # so here we can get cases where frequency is same, incase frequency is same we need to again use LRU.
            # that's why we converted access_time into decimal value and moved all integer to right side of decimal.
            # and added it to frequency, so now if frequency is same then  values after decimal which is access_time
            # will be compared.
            return (data.frequency + res)*-1

    def has_expired(self, data: ExpiryQueueRecord):

        return data.upload_time + self.expiry_time < datetime.utcnow()

    @thread_pool_wrapper
    def set_data(self, key: any, record: Record):
        result = True
        if self.fetch_algo == FetchAlgorithm.WRITE_THROUGH:
            result = self.persist_to_db(record, key)
        if result:
            self.manage_data(key)
            if key in self.cache:
                self.cache[key] = record
                self._update_element_access_info(key)
                self.event_queue.append((Events.UPDATE.value, key))
            else:
                self._add_data_in_cache(record=record, key=key)
                self.event_queue.append((Events.ADD.value, key))
        else:
            raise Exception('unable to add data to cache')
        if self.fetch_algo == FetchAlgorithm.WRITE_BACK:
            # calling it last, because save to db cron is also running in parallel, and if entry is added
            # to updated_keys list and not added to cache yet, will cause key error, because we are
            # fetching key data from cache dict and then saving to db
            self.persist_to_db(record, key)

    def remove_expired_entries(self):
        while len(self.expiry_queue) and self.has_expired(self.expiry_queue.get_max().item):
            key = self.expiry_queue.get_max().item.key
            self._remove_data_from_all_queue(key)
            self.event_queue.append((Events.EXPIRE.value, key))

    @thread_pool_wrapper
    def get_data(self, key):
        self.manage_data(key)
        self.event_queue.append((Events.FETCH.value, key))

        if key in self.cache:
            self._update_element_access_info(key)

        else:
            data = self.load_from_db(key=key, data_source=None)
            self._add_data_in_cache(data, key)
            self.event_queue.append((Events.ADD.value, key))

    @syncronized
    def manage_data(self, key):
        # removing all required items from queue and cache
        self.remove_expired_entries()
        # if key is not in cache, that means it is going to be added in cache later, we need to make some space for it.
        if key not in self.cache and self.is_cache_full():
            delete_record: PriorityQueueRecord = self.priority_queue.get_max().item
            self._remove_data_from_all_queue(delete_record.key)

    def _update_element_access_info(self, key):
        data: Record = self.cache[key]
        old_record: Record = deepcopy(data)
        data.access_time = datetime.utcnow()
        data.frequency += 1

        # update queues
        self.priority_queue.change_priority(
            queue_data=PriorityQueueRecord(key=key, access_time=old_record.access_time,frequency=old_record.frequency),
            new_data=PriorityQueueRecord(key=key, access_time=data.access_time,frequency=data.frequency))
        self.cache[key] = data

    def persist_to_db(self, record: Record, key):
        if self.fetch_algo == FetchAlgorithm.WRITE_THROUGH:

            try:
                self._save_record_to_db(record)
                return True
            except:
                return False

        else:
            # don't wait to complete
            self.keys_to_update.add(key)
            return True

    def _save_record_to_db(self, record: Record):
        # in actual code, do not forgot to use transactions and row level locks to update DB record.
        # this method will return True and commit to DB in case of success write operation and
        # do rollback and return False in case of Failures
        # this method is responsible for both update adn write operations
        try:
            print('waiting for db operation to complete')
            print(record.data)
            print(record.access_time)
            time.sleep(3)

            print('wait completed')
        except Exception as e :
            print(e)
            # do roll back
            raise e

    def load_from_db(self, data_source, key) -> Record:
        print('waiting for db operation to complete')
        time.sleep(3)
        print('wait completed')
        return Record(data=99)

    @syncronized
    def change_eviction_algo(self, algo: EvictionAlgo):
        # build new priority queue
        self.eviction_algo = algo.value
        old_queue = self.priority_queue
        self.priority_queue = PriorityQueue(max_size=self.max_size, comparator=self.expiry_queue_priority_generator)
        while len(old_queue):
            old_data: PriorityQueueRecord = old_queue.pop_max()
            # as record will be same only priority is changed
            self.priority_queue.insert_to_queue(old_data)

    def _remove_data_from_all_queue(self, key):
        if key in self.keys_to_update:
            self._save_record_to_db(self.cache[key])
            self.keys_to_update.remove(key)
        record = self.cache.pop(key)
        self.expiry_queue.remove(ExpiryQueueRecord(upload_time=record.upload_time, key=key))
        self.priority_queue.remove(PriorityQueueRecord(access_time=record.access_time, frequency=record.frequency, key=key))
        self.event_queue.append((Events.DELETE.value, key))

    def _add_data_in_cache(self, record: Record, key: any):
        record.frequency = 1 if not record.frequency else record.frequency + 1
        record.access_time = datetime.utcnow()
        record.upload_time = datetime.utcnow()

        self.cache[key] = record
        self.expiry_queue.insert_to_queue(ExpiryQueueRecord(key=key, upload_time=record.upload_time))
        self.priority_queue.insert_to_queue(PriorityQueueRecord(key=key, frequency=record.frequency, access_time=record.access_time))

    def is_cache_full(self):
        return self.max_size <= len(self.cache)

    def eager_load_data(self):
        if self.keys_to_eager_load:
            if len(self.keys_to_eager_load) > self.max_size:
                raise Exception('keys length is greater than cache size')
            for key in self.keys_to_eager_load:
                data = self.load_from_db(data_source=None, key=key)
                self._add_data_in_cache(data, key)

    @thread_pool_wrapper
    @syncronized
    def remove_from_cache(self, key):

        self._remove_data_from_all_queue(key)

    def cron_to_periodically_sync_with_db_for_write_back(self):
        # no need of locks also,
        # as even if keys_to_update is updated while this code was running, it will update that entry to db.
        # or if some entry was removed, that means that is already updated to db, or going to be updated
        # by some other methods.
        while len(self.keys_to_update):
            key = self.keys_to_update.pop()
            if key in self.cache:
                data = self.cache[key]
                # todo: make it a bulk insert statement to avoid multiple insert queries
                self._save_record_to_db(data)
        # this will call itself periodically without blocking main thread
        threading.Timer(self.sleep_period, self.cron_to_periodically_sync_with_db_for_write_back).start()

    def __del__(self):
        for i in self.thread_pool:
            i.shutdown()


if __name__ == '__main__':
    c = MultiPleThreadCache(max_size=3, pool_size=2, expiry_time=100, fetch_algo=FetchAlgorithm.WRITE_BACK,
                            eviction_algo=EvictionAlgo.LFU, sleep_period=10)
    f1 = c.set_data(key=1, record=Record(data=1, ))
    f2 = c.set_data(key=2, record=Record(data=2, ))
    f3 = c.set_data(key=3, record=Record(data=3, ))
    f4 = c.set_data(key=4, record=Record(data=4, ))
    f6 = c.get_data(key=6)
    f7 = c.remove_from_cache(key=4)
    f8 = c.set_data(key=6, record=Record(data=6))
    f9 = c.change_eviction_algo(EvictionAlgo.LRU)
    concurrent.futures.wait([f1, f2, f3, f4, f6, f7, f8])

    print(c.event_queue)
    print(c.cache)
    print(c.keys_to_update)
