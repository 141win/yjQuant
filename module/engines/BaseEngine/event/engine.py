# _*_ coding: UTF-8 _*_
# @Time : 2025/8/25 14:29
# @Author : yyj
# @Email : 1410959463@qq.com
# @File : engine.py
# @Project : yjQuant
from collections.abc import Callable
from collections import defaultdict
from queue import Queue, Empty
from threading import Thread
from time import sleep

from typing import Any

EVENT_LOG = "eLog"
EVENT_EMAIL = "eEmail"
EVENT_TIMER = "eTimer"
EVENT_ADD_TASK = "eAddTask"

# EVENT_CLOCK = "eClock."

class Event:
    """
    事件类，有事件类型字符串、事件对象数据
    """

    def __init__(self, type: str, data: Any) -> None:
        self.type = type
        self.data = data


HandlerType = Callable[[Event], None]


class EventEngine:
    """
    事件引擎类，根据事件类型将事件对象分发给 在对应事件类型下的处理函数列表 已注册的处理函数

    """

    def __init__(self, interval: int = 1) -> None:
        self._interval: int = interval
        self._queue: Queue = Queue()
        self._active: bool = False
        self._thread: Thread = Thread(target=self._run)
        self._timer: Thread = Thread(target=self._run_timer)
        self._handlers: defaultdict = defaultdict(list)

    def _run(self):
        """
        从队列获取事件并处理
        :return:
        """
        while self._active:
            try:
                # 从事件队列中获取一个事件对象，并处理
                event = self._queue.get(timeout=0.1)
                # print(f"取到事件{event.type}")
                self._process(event)
            except Empty:
                pass

    def _process(self, event: Event) -> None:
        """
        将事件分发给注册监听此类型事件的处理函数
        :param event:
        :return:
        """
        # print(self._handlers)
        if event.type in self._handlers:
            # print("开始处理事件")
            [handler(event) for handler in self._handlers[event.type]]

    def _run_timer(self) -> None:
        """
        Sleep by interval second(s) and then generate a timer event.

        休眠指定的秒数，然后生成一个定时器事件。
        """
        while self._active:
            sleep(self._interval)
            event: Event = Event(EVENT_TIMER, None)
            self.put(event)

    def start(self):
        self._active = True
        self._thread.start()
        self._timer.start()

    def stop(self):
        self._active = False
        self._thread.join()
        self._thread.join()

    def put(self, event: Event) -> None:
        """
        Put an event object into event queue.

        将事件对象放入事件队列。
        """
        self._queue.put(event)

    def register(self, type: str, handler: HandlerType) -> None:
        """
        为特定事件类型注册新的处理函数。每个函数只能为每个事件类型注册一次。
        :param type:
        :param handler:
        :return:
        """
        handler_list: list = self._handlers[type]
        if handler not in handler_list:
            handler_list.append(handler)

    def unregister(self, type: str, handler: HandlerType) -> None:
        """
        Unregister an existing handler function from event engine.

        从事件引擎中注销现有的处理函数。
        """
        handler_list: list = self._handlers[type]

        if handler in handler_list:
            handler_list.remove(handler)

        if not handler_list:
            self._handlers.pop(type)
