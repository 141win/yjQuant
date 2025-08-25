# _*_ coding: UTF-8 _*_
# @Time : 2025/8/23 19:16
# @Author : yyj
# @Email : 1410959463@qq.com
# @File : __init__.py.py
# @Project : yjQuant
"""
日志数据

"""
from dataclasses import dataclass
from collections.abc import Callable
HandlerType = Callable[[], None]

@dataclass
class LogData:
    """
    日志数据
    """
    level: int
    msg: str


@dataclass
class EmailData:
    """
    邮件数据
    """
    subject: str
    content: str
    receiver: str

@dataclass
class TaskData:
    """
    任务数据
    interval:
        seconds: int
        time_str: str
        when: datetime
    type:
        interval
        daily
        at
    """
    type: str
    func: HandlerType
    interval: any

__all__ = ['LogData',"EmailData","TaskData"]
