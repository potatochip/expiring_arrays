'''thread-safe array objects with expiring items'''
import json
import threading
import time
from collections import deque
from datetime import datetime

lock = threading.Lock()


def expire(func):
    def func_wrapper(obj, *args, **kwargs):
        if obj.threadsafe:
            with lock:
                obj._check_expired()
                return func(obj, *args, **kwargs)
        else:
            obj._check_expired()
            return func(obj, *args, **kwargs)
    return func_wrapper


def expire_post(func):
    '''expire items after function has finished'''
    def func_wrapper(obj, *args, **kwargs):
        if obj.threadsafe:
            with lock:
                f = func(obj, *args, **kwargs)
                obj._check_expired()
        else:
            f = func(obj, *args, **kwargs)
            obj._check_expired()
        return f
    return func_wrapper


def synchronized(lock):
    '''Synchronization decorator for thread locking'''
    def wrap(func, *args, **kwargs):
        def func_wrapper(obj, *args, **kwargs):
            if obj.threadsafe:
                with lock:
                    return func(obj, *args, **kwargs)
            else:
                return func(obj, *args, **kwargs)
        return func_wrapper
    return wrap


class BaseExpiringArray(object):
    def __init__(self, max_length=100, threadsafe=True, *args, **kwargs):
        self.threadsafe = threadsafe
        self.max_length = max_length

    def add_iter(self, iterable):
        for i in iterable:
            self.add(i)

    def __iter__(self):
        for i in self.items:
            yield i

    def __len__(self):
        return len(self.items)

    def __repr__(self):
        return str(self.items)

    def _check_max_length(self):
        return len(self._items) > self.max_length

    @expire
    def add(self):
        raise NotImplementedError('add method not implemented')

    @property
    @expire
    def items(self):
        raise NotImplementedError('items method not implemented')


class LRUArray(BaseExpiringArray):
    '''
    Least Recently Used Array
    Discards item that was added longest ago in time when need to make space
    '''
    def __init__(self, *args, **kwargs):
        super(LRUArray, self).__init__(*args, **kwargs)
        self._items = deque()

    @expire
    def add(self, item):
        if item in self._items:
            self._items.remove(item)
        self._items.appendleft(item)

    def _check_expired(self):
        while self._check_max_length():
            self._remove_item()

    def _remove_item(self):
        self._items.pop()

    @property
    @expire
    def items(self):
        return list(self._items)

    @expire
    def dump(self):
        return self._items

    @expire_post
    def load(self, l):
        if isinstance(l, deque):
            self._items = l
        else:
            self._items = deque(l)


class TTLArray(BaseExpiringArray):
    '''
    items expire after timeout (in seconds).
    oldest items will get dropped when hit max_length for the array.
    adding items already in the list cause the original to get updated with a new date
    retain_one allows the array to always have at least one item stored in it even if it is expired
    '''
    def __init__(self, timeout=60, retain_one=False, *args, **kwargs):
        super(TTLArray, self).__init__(*args, **kwargs)
        self._timing = deque()
        self._items = dict()
        self.timeout = timeout  # in seconds
        self.retain_one = retain_one

    @staticmethod
    def _no_right_now():
        '''method named after my wife'''
        return datetime.today()

    def _check_expired(self):
        if self._check_retain_one():
            return
        while self._check_max_length():
            self._remove_expired_item()
        for date_obj, _ in list(self._timing):
            elapsed_time = self._no_right_now() - date_obj
            if elapsed_time.seconds > self.timeout:
                self._remove_expired_item()
            else:
                break

    def _check_retain_one(self):
        return self.retain_one and len(self._items) == 1

    @expire
    def add(self, item):
        now = self._no_right_now()
        if item in self._items:
            time = self._items[item]
            self._timing.remove([time, item])
            self._timing.appendleft([now, item])
        else:
            self._timing.appendleft([now, item])
        self._items.update({item: now})

    def _remove_expired_item(self):
        item = self._timing.pop()[1]
        del self._items[item]

    @property
    @expire
    def items(self):
        return [i[1] for i in self._timing]

    @expire
    def dump(self, include_params=False):
        if not self._timing:
            return
        if include_params:
            d = {'params': {
                '_timeout': self.timeout,
                '_max_length': self.max_length,
                '_retain_one': str(self.retain_one),
                '_threadsafe': str(self.threadsafe)
            }}
        else:
            d = {}
        t, i = zip(*self._timing)
        d.update([('timing_list', t), ('item_list', i)])
        return d

    @expire_post
    def load(self, dictionary):
        '''expects a dictionary with a timing_list and item_list key and optional params'''
        item_list = dictionary['item_list']
        timing_list = dictionary['timing_list']
        self._load_lists(item_list, timing_list)
        params = dictionary.get('params')
        if params:
            self._load_params(params)

    @expire
    def dump_timing_list(self):
        return list(self._timing)

    @expire_post
    def load_from_timing_list(self, t):
        self._timing = deque(t)
        self._rebuild_items(t)

    def _load_lists(self, item_list, timing_list):
        self._items = dict(zip(item_list, timing_list))
        self._timing = deque(list(i) for i in zip(timing_list, item_list))

    def _load_params(self, params):
        self.timeout = params['_timeout']
        self.max_length = params['_max_length']
        self.retain_one = True if params['_retain_one'] == 'True' else False
        self.threadsafe = True if params['_threadsafe'] == 'True' else False

    @expire
    def serialize(self):
        '''
        persist using a string if your db doesnt allow array-type columns
        a little slower than dump
        '''
        return json.dumps(list(self._timing))

    @expire_post
    def deserialize(self, s):
        self._timing = deque(json.loads(s))

    def _rebuild_items(self, t):
        self._items = dict([(v, k) for k, v in t])


class STLArray(TTLArray):
    '''
    Seconds To Live Array
    Sacrificies clarity for speed using seconds instead of datetime
    '''
    @staticmethod
    def _no_right_now():
        '''
        method named after my wife
        i'm hilarious
        the result of working from home
        '''
        return int(time.time())

    def _check_expired(self):
        if self._check_retain_one():
            return
        while self._check_max_length():
            self._remove_expired_item()
        for date_obj, _ in list(self._timing):
            elapsed_time = self._no_right_now() - date_obj
            if elapsed_time > self.timeout:
                self._remove_expired_item()
            else:
                break
