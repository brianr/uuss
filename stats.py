import logging
from uuss import proto
from lolapps.util import json, flatten
from lolapps.util.multithreading import count, count_cls
import os
import threading
import time

stats_log = logging.getLogger(__name__)


_counted_funcs = {}
_counted_clses = set()

thread = None
stop_event = None


def _func_name(func):
    return "%s.%s" % (func.im_class.__name__, func.im_func.__name__)


def decorate_class_functions(funcs_to_count):
    """
    Pass in a list of functions which are defined on a class. These will be
    decorated and all calls to them counted and output in the stats log.
    """
    for func in funcs_to_count:
        wrapped = count(func)
        func_name = func.im_func.__name__
        cls = func.im_class
        setattr(cls, func_name, wrapped)
        _counted_funcs[_func_name(func)] = (func, wrapped)
        _counted_clses.add(cls)
    for cls in _counted_clses:
        pkg = __import__(cls.__module__)
        setattr(pkg, cls.__name__, count_cls(cls))


decorate_class_functions(
    [proto.UUSSProtocol.send_message, proto.UUSSProtocol.recv_message]
    +
    flatten([[[cls.ParseFromString, cls.SerializeToString] for cls in msg_types] for msg_types in proto.MSG_TYPES.values()])
    )


def emit_stats():
    stats_log.info(json.dumps(
        {
            'pid': os.getpid(),
            'time': time.time(),
            'counts': dict([
                (func_name, wrapped._counter.get())
                for (func_name, (func, wrapped))
                in sorted(_counted_funcs.iteritems(), lambda a, b: cmp(a[0], b[0]))
                if wrapped._counter is not None])
        }))


def _run_emit_stats(interval=30):
    while True:
        stop_event.wait(interval)
        if stop_event.is_set():
            break
        emit_stats()


def start(event_class=threading.Event):
    global thread, stop_event
    if thread:
        return
    stop_event = event_class()
    thread = threading.Thread(target=_run_emit_stats, name='UUSS-Stats')
    thread.start()


def stop():
    global thread
    if not thread:
        return
    stop_event.set()
    thread = None
