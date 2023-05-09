import pickle
import time

from Data_structure import *
from contants import *
import math
from datetime import datetime, timedelta
from copy import deepcopy
import asyncio


# here we are going to use locks for each key, so that Read, write and delete operations will
# be read-your-own writes consistent.
# so here we are trying to perform all async db calls first, and once all data is ready, then only we are doing
# current cache operations which are synchronous
class CacheAsync:
    # all time is in UTC
    def __init__(self, expiry_time: timedelta, max_size, fetch_algo, eviction_algo, keys_to_early_load=None,sleep_period=1200):
        self.expiry_time = expiry_time
        self.max_size = max_size
        self.fetch_algo = fetch_algo
        self.event_queue = []
        self.eviction_algo = eviction_algo
        self.keys_to_eager_load = keys_to_early_load
        self.cache = dict()
        self.locks = dict()

        self.keys_to_update = list()
        self.sleep_period = sleep_period
        self.priority_queue = PriorityQueue(max_size=max_size, comparator=self.expiry_queue_priority_generator)
        self.expiry_queue = PriorityQueue(max_size=max_size, comparator=lambda record: record.upload_time.timestamp())
        if self.keys_to_eager_load:
            asyncio.create_task(self.eager_load_data())
        if self.fetch_algo == FetchAlgorithm.WRITE_BACK:
            asyncio.create_task(self.cron_to_periodically_sync_with_db_for_write_back())

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

    async def set_data(self, record: Record, key: any):
        if key not in self.locks:
            self.locks[key] = asyncio.Lock()
        # locking current key so that, operations for this specific key will run in
        # same sequential manner. just to avoid read-your-own write consistency.
        async with self.locks[key]:
            # asynchronous part first

            result = True
            if self.fetch_algo == FetchAlgorithm.WRITE_THROUGH:
                result = await self.persist_to_db(record, key)
            if result:
                await self.manage_data(key)
                # delete from cache in manage_data and below part is synchronous code.
                # so deletion of other key if cache is full and adding new data in cache will be synchronous code.
                if key in self.cache:
                    old_data = self.cache[key]
                    old_data.data = record.data
                    self.cache[key] = old_data
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

                # in actual, it will only run synchronous code, as in case of write-back,
                # we are just adding to list not db
                await self.persist_to_db(record, key)

    async def remove_expired_entries(self):
        while len(self.expiry_queue) and self.has_expired(self.expiry_queue.get_max().item):
            key = self.expiry_queue.get_max().item.key
            while key in self.cache and not await self._remove_data_from_all_queue(key):
                continue
            self.event_queue.append((Events.EXPIRE.value, key))

    async def get_data(self, key):

        if key not in self.locks:
            self.locks[key] = asyncio.Lock()
        async with self.locks[key]:
            # loading data in advance, to perform async operations first.
            if key not in self.cache or (key in self.cache and self.has_expired(self.cache[key])):
                # if key is not there or data is expired, then advance load the cache data
                data = await self.load_from_db(key=key, data_source=None)
                # we are advance loading data, before delete data from cache, otherwise, if we deleted data from
                # cache first to make some space, then in that case, by the time we are loading data from db it may
                # be possible that some-one else already used that space, which is freed by this call.
                # so all async calls are done ahead, to make sure our internal queue and cache operations will run
                # synchronously, without breaking or losing the execution control in the middle. (or without
                # context switching)
            await self.manage_data(key)
            # now synchronous code
            self.event_queue.append((Events.FETCH.value, key))
            if key in self.cache:
                self._update_element_access_info(key)
            else:
                self._add_data_in_cache(data, key)
                self.event_queue.append((Events.ADD.value, key))
            return self.cache[key ]

    async def manage_data(self, key):
        # removing all required items from queue and cache
        await self.remove_expired_entries()
        # if key is not in cache, that means it is going to be added in cache later, we need to make some space for it.
        if key not in self.cache and self.is_cache_full():
            while self.is_cache_full():
                delete_record: PriorityQueueRecord = self.priority_queue.get_max().item
                # it might be possible that below method haven't deleted anything from cache, as either it was already
                # deleted or updated. so we will keep deleting until something get deleted and we got some space.
                # and once something is deleted from queue, then other adding data operations are synchronous.
                # so whatever space we created here, will be utilized by same, intended key,
                # who initiated this cleanup operation, because after delete from cache, other operations are sync,
                # so current thread will keep having the control, till addition is completed.
                await self._remove_data_from_all_queue(delete_record.key)

    def _update_element_access_info(self, key):
        data: Record = self.cache[key]
        old_record: Record = deepcopy(data)
        time.sleep(1)
        data.access_time = datetime.utcnow()
        data.frequency += 1

        # update queues
        self.priority_queue.change_priority(
            queue_data=PriorityQueueRecord(key=key, access_time=old_record.access_time,frequency=old_record.frequency),
            new_data=PriorityQueueRecord(key=key, access_time=data.access_time,frequency=data.frequency))
        self.cache[key] = data

    async def persist_to_db(self, record: Record, key):
        if self.fetch_algo == FetchAlgorithm.WRITE_THROUGH:

            try:
                await self._save_record_to_db(record)
                return True
            except:
                return False

        else:
            # don't wait to complete
            self.keys_to_update.append(key)
            return True

    async def _save_record_to_db(self, record: Record):
        # in actual code, do not forgot to use transactions and row level locks to update DB record.
        # this method will return True and commit to DB in case of success write operation and
        # do rollback and return False in case of Failures
        # this method is responsible for both update adn write operations
        try:
            print('waiting for db operation to complete')
            print(record.data)
            print(record.access_time)
            await asyncio.sleep(3)

            print('wait completed')
        except Exception as e:
            print(e)
            # do roll back
            raise e

    async def load_from_db(self, data_source, key) -> Record:
        print('waiting for db operation to complete')
        await asyncio.sleep(3)
        print('wait completed')
        return Record(data=1)

    def change_eviction_algo(self, algo: EvictionAlgo):
        # build new priority queue
        self.eviction_algo = algo.value
        old_queue = self.priority_queue
        self.priority_queue = PriorityQueue(max_size=self.max_size, comparator=self.expiry_queue_priority_generator)
        while len(old_queue):
            old_data: PriorityQueueRecord = old_queue.pop_max().item
            # as record will be same only priority is changed
            self.priority_queue.insert_to_queue(old_data)

    async def _remove_data_from_all_queue(self, key):
        old_data = self.cache[key]
        old_data = pickle.dumps(old_data)
        old_data = hash(old_data)
        if key in self.keys_to_update:
            await self._save_record_to_db(self.cache[key])
            # deleting only when db is updated
            if key in self.keys_to_update:
                self.keys_to_update.remove(key)

        # it may be possible while we are wait for lock, someone else already deleted it, so just making sure if
        # key is there before deleting it.
        # or while were writing old data, someone already updated this data in cache, which is yet not saved to db.
        # and now if we remove from cache, then may lose the original data, to avoid that we can either use locks or
        # check, if we are deleting old data or new data.
        # but if we use locks then it can cause hold and wait condition which can cause deadlock for get and set methods
        # as in get() and set() we are already holding lock for another key. so we will simply go with second approach
        # if we got same old data then only delete, otherwise skip.
        record = self.cache.pop(key) if key in self.cache and hash(pickle.dumps(self.cache[key])) == old_data else None
        if record is not None:
            self.expiry_queue.remove(ExpiryQueueRecord(upload_time=record.upload_time, key=key))
            self.priority_queue.remove(PriorityQueueRecord(access_time=record.access_time, frequency=record.frequency, key=key))
            return True
        else:
            return False

    def _add_data_in_cache(self, record: Record, key: any):
        record.frequency = 1 if not record.frequency else record.frequency + 1
        record.access_time = datetime.utcnow()
        record.upload_time = datetime.utcnow()

        self.cache[key] = record
        self.expiry_queue.insert_to_queue(ExpiryQueueRecord(key=key, upload_time=record.upload_time))
        self.priority_queue.insert_to_queue(PriorityQueueRecord(key=key, frequency=record.frequency, access_time=record.access_time))

    def is_cache_full(self):
        return self.max_size <= len(self.cache)

    async def eager_load_data(self):
        if len(self.keys_to_eager_load) > self.max_size:
            raise Exception('keys length is greater than cache size')
        for key in self.keys_to_eager_load:
            data = await self.load_from_db(data_source=None, key=key)
            self._add_data_in_cache(data, key)

    async def remove_from_cache(self, key):
        if key not in self.locks:
            self.locks[key] = asyncio.Lock()
        async with self.locks[key]:
            while key in self.cache and not await self._remove_data_from_all_queue(key):
                continue

    async def cron_to_periodically_sync_with_db_for_write_back(self):

        # no need of locks also,
        # as even if keys_to_update is updated while this code was running, it will update that entry to db.
        # or if some entry was removed, that means that is already updated to db, or going to be updated
        # by some other methods.
        while len(self.keys_to_update):
            key = self.keys_to_update.pop()
            if key in self.cache:
                data = self.cache[key]
                # todo: make it a bulk insert statement to avoid multiple insert queries
                await self._save_record_to_db(data)
        # todo: remove unused locks also.otherwise self.locks list will become too large
        # this will call itself periodically without blocking main thread
        await asyncio.sleep(self.sleep_period)
        await self.cron_to_periodically_sync_with_db_for_write_back()


async def main():

    c = CacheAsync(expiry_time=timedelta(seconds=100), max_size=2, fetch_algo=FetchAlgorithm.WRITE_BACK, eviction_algo=EvictionAlgo.LFU, sleep_period=100)
    c1 = asyncio.create_task(c.get_data(key=1))
    # await c1
    # print(c1.result())
    c2 = asyncio.create_task(c.set_data(Record(data=2), key=2))
    c3 = asyncio.create_task(c.set_data(Record(data=3), key=3))
    c4 = asyncio.create_task(c.set_data(Record(data=4), key=4))
    c5 = asyncio.create_task(c.set_data(Record(data=5), key=2))

    await asyncio.gather(c1,c2,c3,c4,c5)
    print(c.cache)
    print(c.event_queue)
    # print(c1.result())

if __name__ == '__main__':
    asyncio.run(main())