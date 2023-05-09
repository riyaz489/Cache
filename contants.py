from  dataclasses import dataclass
from datetime import datetime
from enum import Enum


@dataclass
class Record:
    data: any
    access_time: datetime = None
    frequency: int = None
    upload_time: datetime = None



@dataclass
class PriorityQueueRecord:
    key: any
    access_time: datetime = None
    frequency: int = None


@dataclass
class ExpiryQueueRecord:
    key: any
    upload_time: datetime = None


class EvictionAlgo(Enum):
    LRU = 'LRU'
    LFU = 'LFU'


class FetchAlgorithm(Enum):
    WRITE_THROUGH = 'WRITE_THROUGH'
    WRITE_BACK = 'WRITE_BACK'

class Events(Enum):
    ADD = 'Add'
    EXPIRE = 'Expire'
    DELETE = 'Delete'
    UPDATE = 'Update'
    FETCH = 'Fetch'