from Queue import Queue
from threading import RLock

class SetQueue(Queue):
    """
    Queue class is thread-safe, so it can be used in scenarios where multiple threads will try accessing the queue.
    But default implementation has a downside, it is not iterable and you can't check if it contains an item or not,
    so you can't see if you are adding a duplicate item or not.
    To overcome this problem, a SetQueue implementation is propsed.
    This implementation is grabbed from here: http://stackoverflow.com/questions/1581895/how-check-if-a-task-is-already-in-python-queue
    A set is used alongside the Queue itself to proved an iterator.
    Also since the base implementation is thread-safe, this implementation will be thread-safe, too.
    """

    def _init(self, maxsize=0):
        """
        SetQueue constructor constructing the base Queue object and an internal set object
        """
        Queue._init(self, maxsize)
        self.all_items = set()

    def _put(self, item):
        """
        A thread-safe implementation override of enqueueing.
        New item is added to the queue only if it hasn't been added before
        """
        if item not in self.all_items:
            Queue._put(self, item)
            self.all_items.add(item)

    def _get(self):
        """
        A thread-safe implementation override of dequeueing.
        When queue pops an item, it is also removed from the set

        """
        item = Queue._get(self)
        self.all_items.remove(item)
        return item

    def contains(self, item):
        """
        Attempt to implement thread-safety for contains method using Re-entrant locks.
        ## I'm still not sure whether this implementation is correct.
        """
        lock = RLock()
        isPresent = False
        lock.acquire()
        isPresent = item in self.all_items
        lock.release()
        return isPresent
