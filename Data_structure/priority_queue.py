# for priority queue most optimized implementation is max-heap because insertion and deletion is faster.
# if we implement priority queue using list then insertion and deletion will be O(n).
import dataclasses
import pickle
from optparse import Option
from threading import Lock, Thread, Condition
from typing import Callable

from Exceptions import *


# if we have used only LRU then best approach will be to use ordered dict. because in case of LRU
# we are not putting new elements in any specific index, it's always insertion at beginning and removal from last.
# so insertion, deletion and fetching element always will be o(1). and auxiliary space will be o(n)
# for extra double linked list to store dict elements ordering. but ordered-dict
# (in this case it will not be ordered-dict, because pushing elements at specific index and ordering on the basis of key
# is out of scope for ordered dict, so it will be priority queue again which is implemented using linked-list)
# will perform poorly if we are using some key to define ordering, let say in LFU, we need to sort on the basis of
# frequency of elements accessed, so every time we do insertion/update we need to check all the elements and
# find new/updated correction position and put it there. so that will take O(n) in worst case,
# but heap will take O(log(n)). but deletion will be faster, it will be O(1), but here it will be O(log(n)).

# so here we have to use 2 things first is dict to store cache
# and second is priority queue to store cache data ordering.
# insertion -> O(log(n)) or Height of tree/heap for swapping operations + O(1) (for dict)
# deletion -> O(log(n))  or Height of tree/heap(as we need to reorder that branch again) + O(1) (for dict)
# getting max value -> O(1) + O(1) (for dict)
# to build complete heap from scratch -> O(nlog(n)) (this is only for heap, in case we want to change algo
#                                       for cache replacement or changed key for priority,
#                                       because then we have to create heap from scratch)
# auxiliary space -> O(n) (because our data will be stored in separate dict(which is input data) and
#                           in max heap we will just keep reference or keys of our cache data in order
#                           (so heap data will be extra ))


# ### priority queue using python lib ####################
# from queue import PriorityQueue
# # it is same as heapq in python
#
# pqueue = PriorityQueue(maxsize=40)
# # first item is priority and second item is value in given tuple of put
# pqueue.put(item=(1, 23), block=True, timeout=20)
# # so if queue is full it will wait till given timeout, otherwise we can set block to False.
# # and raise QueueFull exception immediately. behind the scenes it will put thread lock in the queue,
# # in case queue full and wait till timeout or if some entry is evicted from queue then it notified by
# # another thread to continue and add new data (its like conditions we discussed in thread_pause_resume file)
# pqueue.put(item=(2, 223), block=True, timeout=20)
# pqueue.put(item=(1, 34), block=True, timeout=20)
#
# while pqueue:
#     print(pqueue.get())
#     if pqueue.qsize() ==0:
#         break

@dataclasses.dataclass
class PriorityQueueItem:
    item: any
    priority: int


# as heap is binary tree, so we are using array to represent it, because array representation is easy and
# space optimized as no need to store left and right child pointer details, we will just calculate it on the fly.

# todo: raise exceptions instead of returning -1, as -1 will be risky in python case, it will return last elements
#  so to avoid unnecessary -1 checks raise exception directly

# note: our priority queue class is not thread safe as of now, so we will use locks from caller
# side to make it thread-safe.
# note: we just used thread to wait in case queue is full, we could have used simple sleep till timeout but in case
# some another thread is making insert operation then our main thread will not be paused, only the thread who called
# insert will have to wait till queue have some space or timeout. so we just made our Queue to be utilized by multiple,
# thread, but still insert/delete and changing priority are still not thread safe.
class PriorityQueue:

    def __init__(self, max_size: int = -1, comparator: callable = None):
        self._data: list[PriorityQueueItem] = list()
        self._size = -1
        self.max_size = max_size
        self.compartor = comparator
        self.even_condition = Condition(Lock())

    def __len__(self):
        return self._size+1

    def insert_to_queue(self, queue_data: any, priority: any = None, blocking: bool = True, timeout: int = 0):
        if priority is None:
            priority = self.lambda_to_int_convertor(queue_data)
        queue_item = PriorityQueueItem(item=queue_data, priority=priority)
        # insert at leaf and then shift up
        if self.max_size != -1:
            # then we need to either block or raise exception
            if blocking:
                self.even_condition.acquire()
                xx = self.even_condition.wait_for(lambda: len(self) < self.max_size, timeout=timeout)
                self.even_condition.release()
                if not xx:
                    raise QueueFullException('timed out waiting for queue, to have some space,'
                                             ' so that we can insert new data')

            elif self.max_size >= len(self):
                raise QueueFullException('Queue is full')
        self._size += 1
        self._data.append(queue_item)

        self._shift_up(self._size)

    def change_priority(self, queue_data: any, new_data: any = None, new_priority: any = None, old_priority: any = None):
        if old_priority is None:
            old_priority = self.lambda_to_int_convertor(queue_data)
        if new_priority is None:
            new_priority = self.lambda_to_int_convertor(new_data)
        queue_item = PriorityQueueItem(item=queue_data, priority=old_priority)
        elem_index = self._find_elem_in_heap(queue_item)
        if elem_index == -1:
            raise DataNotFound('element does not exits in heap, try to provide correct data and priority in function, '
                               'if priority is not generated by comparator')
        # we will shift up or shift down based on new priority
        self._data[elem_index].priority = new_priority
        if new_priority > queue_item.priority:
            self._shift_up(elem_index)
        elif new_priority < queue_item.priority:
            self._shift_down(elem_index)

    def remove(self, queue_data: any, priority: int = None):
        # To remove at given index:
        # first give that element max priority and move it to top.
        # then use pop_max operation to remove top element

        if not priority:
            queue_item = PriorityQueueItem(item=queue_data, priority=self.lambda_to_int_convertor(queue_data))

        else:
            queue_item = PriorityQueueItem(item=queue_data, priority=priority)

        elem_index = self._find_elem_in_heap(queue_item)
        if elem_index == -1:
            raise DataNotFound('element does not exits in heap, try to provide correct data and priority in function, '
                               'if priority is not generated by comparator')

        # setting max priority
        self._data[elem_index].priority = self._data[0].priority + 1
        self._shift_up(elem_index)
        result = self.pop_max()

        return result

    def _find_elem_in_heap(self, queue_item: PriorityQueueItem, start_index: int = 0) -> int:
        # if you want to avoid this function and optimize it,
        # we could have used a simple dict to track our queue intex indicies
        # where key will be queue_item and value will be it's
        # index in our internal list
        # for example:
        # r = dict()
        # w = PriorityQueueItem(item=1, priority=1)
        # r[pickle.dumps(w)] = 1
        # w = PriorityQueueItem(item=2, priority=2)
        # r[pickle.dumps(w)] = 2
        # print(r[pickle.dumps(PriorityQueueItem(item=1, priority=1))])

        # so for finding element in heap complexity will be log(n), because not all elements are sorted
        # so we have to look one by one. to optimize it further we can do dfs and check if current element is smaller
        # then required element, then we can ignore whole subtree below it. because in heap we know all child elements
        # will be smaller than current parent and same applies for child of its children elements. so current root will
        # always be grater than below subtree elements

        # sometimes priority only is not enough to find an element because there may be case where priority are same
        # so for that purpose, I will first serialize element data into string do it's hash and then look for value.
        hashed_value = pickle.dumps(queue_item.item)
        hashed_value = hash(hashed_value)
        if start_index > self._size:
            return -1

        if hashed_value == hash(pickle.dumps(self._data[start_index].item)) and self._data[start_index].priority == queue_item.priority:
            return start_index

        elif queue_item.priority > self._data[start_index].priority:
            return -1
        else:
            left_res = self._find_elem_in_heap(queue_item, self._possible_left_child_index(start_index))
            right_res = self._find_elem_in_heap(queue_item, self._possible_right_child_index(start_index))

            return left_res if left_res != -1 else right_res

    def get_max(self) -> PriorityQueueItem | int:
        if self._size >= 0:
            return self._data[0]
        else:
            return -1
    
    def pop_max(self) -> any:
        # replace leaf node with top element and then do shift down
        # to balance tree again
        if len(self) == 0:
            return

        self.even_condition.acquire()
        result = self._data[0].item
        self._data[0] = self._data[-1]
        self._size -= 1
        self._data.pop(-1)

        self._shift_down(0)
        # signalling waiting threads to insert new data, who are waiting queue to get some space
        self.even_condition.notify()
        self.even_condition.release()
        return result
    
    def _shift_up(self, index: int):
        # compare with parent, if greater then swap
        if index <= 0:
            return
        if self._data[index].priority > self._data[self._parent(index)].priority:
            # swap parent
            self._data[index], self._data[self._parent(index)] = self._data[self._parent(index)], self._data[index]
            self._shift_up(self._parent(index))
    
    def _shift_down(self, index: int):
        # compare parents with child elements if any of just child is grater than parent then,
        # replace otherwise stop there

        current_index = index
        while current_index <= self._size:
            current_parent = current_index
            current_max = self._data[current_parent].priority
            left_child = self._possible_left_child_index(current_index)
            if left_child <= self._size and self._data[left_child].priority > current_max:
                current_index = left_child
            right_child = self._possible_right_child_index(current_index)
            if right_child <= self._size and self._data[right_child].priority > current_max:
                current_index = right_child
            
            if current_parent == current_index:
                break
            else:
                self._data[current_index], self._data[current_parent] = self._data[current_parent], self._data[current_index]
    
    @staticmethod
    def _parent(index: int):
        return (index-1)//2
    
    @staticmethod
    def _possible_left_child_index(index: int):
        return (index*2)+1
    
    @staticmethod
    def _possible_right_child_index(index: int):
        return (index*2)+2

    def lambda_to_int_convertor(self, data: any):
        if self.compartor is None:
            raise ComparatorNotSetException('comparator function is not in current queue object')

        try:
            result = self.compartor(data)
            return result
        except TypeError as e:
            raise DataAsArgumentMissingInComparatorFunction('Queue data as argument is missing from comparator function') from e



# # ################ Sample code ##############
# q = PriorityQueue(max_size=3, comparator=lambda data: data+1)
#
# q.insert_to_queue(3)
# q.insert_to_queue(4)
# q.insert_to_queue(1, priority=30)
# q.change_priority(4, new_priority=50)
# # try to provide old priority,  if we passed old priority manually while insertion,
# # without help of comparator function
# # otherwise below function will not be able to find the data in heap
# q.change_priority(1, old_priority=30, new_priority=100)
# q.remove(4, priority=50)
#
# while len(q):
#     print(q.pop_max())


# instead of storing priority as int, we could have created custom less_than, equal_to, greater_than function in
# queue class , and whenever we want to compare queue elements we will use those custom functions instead of
# traditional >,<,= operators. and now user whoever calling our queue class will define which value we need to
# compare and we are going to compare,
# so now user will provide something like this

# class Queue:
#     def __init__(self, comparator=None, max_value=float('inf'), min_vale=float('-inf')):
#         self.comparator = comparator
#         self.min_value = min_vale
#         # another benefit of custom comparator is that, we can define our custom min and max values
#         self.ma_value = max_value
#         self.data = []
#
#        this function could be the dunder __lt__ method for Data dataclass.then in that case we don't need to pass
#       comparator to Queue class as Data class object is intelligent enough to  decide how to compare,
#        But in our case Queue class will decide how to compare that's why we are forced to created custom methods here
#     def is_less_than(self, first, second):
#
#         if first == self.min_value or second == self.ma_value:
#             return True
#         elif second == self.min_value or first == self.ma_value:
#             return False
#
#         if self.comparator:
#             first, second = self.comparator(first, second)
#              # now if comparator is defined then we will get modified first and second values.
#         return first < second
#
#     def check_data(self):
#         return self.is_less_than(self.data[0], self.data[1])
#
#
# def comparator(first, second):
#     if first.feild1 == second.feild1:
#         return first.feild2, second.feild2
#     else:
#         return int(first.feild1), int(second.feild1)
#
#
# @dataclasses.dataclass
# class Data:
#     feild1: int
#     feild2: int
#
#
# q1 = Queue(comparator=comparator)
# q1.data.append(Data(feild1=1, feild2=1, ))
# q1.data.append(Data(feild1=2, feild2=1, ))
# print(q1.check_data())
#
# so basically we will give data to queue and also tell how we want to make comparisons in it.
# in this case we don't need to store priority in dataclass,
# as we are dynamically comparing elements on different fields.



# d = [1,2]
# import time
# locks_list = []
#
# def a(w):
#     locks_list.append(Lock())
#     locks_list[0].acquire()
#     d[0] = 9
#     print(d)
#     time.sleep(9)
#     locks_list[0].release()
#
#
# t = Thread(target=a, args=[d])
# t.start()
# t.join(1)
# locks_list.append(Lock())
# locks_list[1].acquire()
# d[1] = 5
# print(d)
# locks_list[1].release()