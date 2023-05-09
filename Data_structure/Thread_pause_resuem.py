from threading import Thread, Condition, Lock, Event


# here we will use Thread conditions for inter-thread communication.
# it is upgrade on events

# Events usage
# event = Event()
# # setting an event
# event.set()
# # checking if event is set
# event.is_set()
# # clearing set event
# event.clear()
# # putting current thread on sleep till event is set or given timeout is reached
# event.wait(timeout=20)


class MyThread(Thread):
    # class level Thread, every object of this class will act as new thread and
    # code we want to run in new thread will be put inside run method.
    def __init__(self):
        super().__init__()
        # by default condition uses Rlock object to put locks on threads and stop and resume them. but we can pass Lock
        # object as well which is more efficient
        self.condition = Condition(Lock())

        # as you have noticed, run method does not return anything, that means we can not return anything from
        # another thread nor in class based thread and not in function based thread, so instead can use some attributes
        # or global objects like list or dict so set results from another thread.
        self.success = []
        self.error = []

    def run(self) -> None:
        # code inside this method will run in another thread

        count_of_operations = 0
        while True:
            print('running new thread')
            # we always first need to Acquire condition to start communication
            self.condition.acquire()
            # Putting this thread on sleep till some other thread call notify for same condition
            self.condition.wait()
            print('after continue')
            self.success.append(count_of_operations)
            # releasing condition object because now our communication is done
            self.condition.release()

            # stopping another thread on some sample condition
            count_of_operations += 1
            if count_of_operations == 2:
                break

    def continue_once(self, thread:Thread):
        # code inside this method will work on the thread who called this method, like in our case
        # it will be main thread.
        self.condition.acquire()
        # notifying only one thread, if we put n=2 then it will resume 2 threads
        self.condition.notify(n=1)
        # it will notify all waiting threads
        # self.condition.notify_all()
        self.condition.release()
        # now as we called notify, our other thread can continue work, but currently main thread is running and
        # now OS will decide when to switch to other thread, so in this case our notify can lost, because let's say,
        # since starting only main thread is keep running and another thread didn't get a chance to reach to wait()
        # method step so without wait. or let say initially run() function ran and now it's waiting for notify.
        # now from main thread we called notify, but still main thread has some remaining work so it will keep continue
        # and again it called notify method multiple times, but another thread is not working because main thread still
        # has some work to do or OS decided to give control to another thread yet. So in this
        # scenario all the old notify calls will be lost because another thread only waiting for one notify call,
        # so only last one will be utilized or whenever OS gives control to another thread which is very unpredictable
        # , but we want to run another thread as many time as continue_once called.
        # so avoid this we called join method below, with least possible timeout. so that another thread will get a
        # chance to run his code. so with every continue_once method call we are forcefully returning control to another
        # thread.
        thread.join(0.0001)

t = MyThread()
x = t.start()

# giving another thread to chance to reach to wait condition
t.join(0.00001)

t.continue_once(t)
t.continue_once(t)

# now to get output of above thread we can wait using join(), till another thread ends.
# t.join()
# or instead of waiting till thread ends we can wait till any events happens in thread, for a given timeout
c = Condition(Lock())
c.acquire()
# basically wait_for is similar to wait(), but it will trigger either we got notify call or given
# function is returns True
# it is equivalent to
# while not predicate():
#     cv.wait()
# so if predicate is not ture when we called wait_for and it took some time to being true then in that
# case we have to pass notify signal also to continue it. that means later both predicate should be true and notify()
# signal is also needed
c.wait_for(lambda : t.error or t.success, timeout=None)
c.release()
print(t.success)
print(t.error)

# Well locks are works on mutual threads corporation, if both threads are using same lock to access same resource then
# locks will work, but one of the thread is not using any lock or using some other lock which is not used by
# other thread, then it can access same element without wait.

# As of now a lock can be aquired by only one thread at a time, but what if we want multiple thread to aquire and
# work on critial section at a time, in that case we can use semaphores

# from threading import *
# obj = Semaphore(3)
# def a ():
    # obj.acquire()
    # print()
    # obj.release()
# so now 3 threads can simultaneously run a() method and call print() and  other remaining threads will wait

# function based thread
# def a():
#     return 'output'
#
# w = Thread(target=a)
# e = w.start()
# f = w.join()
