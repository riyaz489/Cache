class QueueFullException(Exception):
    """
    Queue full error.
    """
    pass


class ComparatorNotSetException(Exception):
    """
    Comparator function not set error.
    """
    pass


class DataAsArgumentMissingInComparatorFunction(Exception):
    """
    Data object as argument is missing in comparator error.
    """
    pass


class DataNotFound(Exception):
    """
    Data not found in Heap error.
    """
    pass
