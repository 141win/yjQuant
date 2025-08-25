# _*_ coding: UTF-8 _*_
# @Time : 2025/8/25 14:33
# @Author : yyj
# @Email : 1410959463@qq.com
# @File : mainengine.py
# @Project : yjQuant
from module.engines.BaseEngine.event import EVENT_ADD_TASK
from module.engines.BaseEngine.event import (Event, EventEngine, HandlerType,
                   EVENT_LOG,
                   EVENT_EMAIL)
from abc import ABC, abstractmethod

from typing import TypeVar

EngineType = TypeVar("EngineType", bound="BaseEngine")

from module.engines.log import INFO
from module.data import LogData, EmailData, TaskData

from queue import Queue


# 基础引擎
class BaseEngine(ABC):
    """
    Abstract class for implementing a function engine.
    确保所有引擎都有统一的初始化参数
    建立引擎与主引擎、事件引擎的关联关系
    提供引擎的唯一标识符
    """

    # 抽象方法，子类必须实现该方法
    @abstractmethod
    def __init__(
            self,
            main_engine: "MainEngine",
            engine_name: str,
    ) -> None:
        """"""
        self.main_engine: MainEngine = main_engine
        self.engine_name: str = engine_name

    def close(self) -> None:
        """
        定义引擎的标准关闭接口
        子类可以重写此方法实现自定义清理逻辑
        支持优雅关闭，确保资源正确释放
        :return:
        """
        return


# 主引擎
class MainEngine:
    """
    主引擎
    """

    def __init__(self, event_engine: EventEngine | None = None) -> None:
        if event_engine:
            self.event_engine: EventEngine = event_engine
        else:
            self.event_engine = EventEngine()
        self.event_engine.start()
        self.engines: dict[str, BaseEngine] = {}
        self.email_queue: Queue = Queue()
        self.init_engines()

    def init_engines(self) -> None:
        """
        初始化所有引擎
        :return:
        """

        # log_engine: LogEngine = self.add_engine(LogEngine)
        # self.process_log_event = log_engine.process_log_event

    def add_engine(self, engine_class: type[EngineType]) -> EngineType:
        """
        添加引擎到self.engines<dict[str, BaseEngine]>
        :return: engine
        """
        # engine_class是传入的引擎类地址，加上括号初始化引擎
        engine: EngineType = engine_class(self)
        self.engines[engine.engine_name] = engine
        return engine

    def get_engine(self, engine_name: str) -> BaseEngine:
        engine: BaseEngine | None = self.engines.get(engine_name, None)
        if not engine:
            self.write_log("找不到引擎：{}".format(engine_name))
        return engine

    def write_log(self, msg: str, level: int = INFO) -> None:
        """
        Put log event with specific message.
        """
        log: LogData = LogData(level=level, msg=msg)
        event: Event = Event(EVENT_LOG, log)
        self.event_engine.put(event)

    def send_email(self, subject: str, content: str, receiver: str | None = None) -> None:
        email: EmailData = EmailData(subject=subject, content=content, receiver=receiver)
        # event: Event = Event(EVENT_EMAIL, email)
        self.email_queue.put(email)

    def add_task(self,task: TaskData) -> None:
        event: Event = Event(EVENT_ADD_TASK, task)
        self.event_engine.put(event)

    def register(self, type: str, handler: HandlerType) -> None:
            """
            对外提供注册接口
            :param type:
            :param handler:
            :return:
            """
            self.event_engine.register(type, handler)

    def close(self) -> None:
        # Stop event engine first to prevent new timer event.

        for engine in self.engines.values():
            engine.close()
        self.event_engine.stop()
