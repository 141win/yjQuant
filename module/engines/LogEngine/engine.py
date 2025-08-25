# _*_ coding: UTF-8 _*_
# @Time : 2025/8/23 19:13
# @Author : yyj
# @Email : 1410959463@qq.com
# @File : engine.py
# @Project : yjQuant
from __future__ import annotations

from module.engines.BaseEngine import (Event,
                                       BaseEngine,
                                       MainEngine,
                                       EVENT_LOG)

from module.engines.log import (DEBUG,
                                INFO,
                                WARNING,
                                ERROR,
                                CRITICAL)

from module.data import LogData
from module.config import SETTINGS
from loguru import logger

"""
日志引擎
实例化后，主动调用主引擎的注册接口，向事件引擎的注册表注册process_log_event函数
"""


# 日志引擎
class LogEngine(BaseEngine):
    """
    Provides log event output function.
    """
    level_map: dict[int, str] = {
        DEBUG: "DEBUG",
        INFO: "INFO",
        WARNING: "WARNING",
        ERROR: "ERROR",
        CRITICAL: "CRITICAL",
    }

    def __init__(self, main_engine: MainEngine) -> None:
        """"""
        super().__init__(main_engine, "log")
        self.active = SETTINGS["log.active"]

        self.register_log(EVENT_LOG)

        self.main_engine.write_log("log引擎初始化成功")

    def process_log_event(self, event: Event) -> None:
        """Process log event"""
        if not self.active:
            return

        log: LogData = event.data
        level: str | int = self.level_map.get(log.level, log.level)
        logger.log(level, log.msg)

    def register_log(self, event_type: str) -> None:
        """Register log event handler"""
        self.main_engine.register(event_type, self.process_log_event)
        self.main_engine.write_log("注册日志事件监听")
