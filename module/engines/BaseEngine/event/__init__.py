# _*_ coding: UTF-8 _*_
# @Time : 2025/8/23 16:59
# @Author : yyj
# @Email : 1410959463@qq.com
# @File : __init__.py.py
# @Project : yjQuant

from module.engines.BaseEngine.event.engine import Event, EventEngine, HandlerType, EVENT_LOG, EVENT_EMAIL, EVENT_TIMER, EVENT_ADD_TASK

# EVENT_TIMER = "eTimer"

# EVENT_TICK = "eTick."
# EVENT_TRADE = "eTrade."
# EVENT_ORDER = "eOrder."
# EVENT_POSITION = "ePosition."
# EVENT_ACCOUNT = "eAccount."
# EVENT_QUOTE = "eQuote."
# EVENT_CONTRACT = "eContract."
# EVENT_LOG = "eLog"
# EVENT_CLOCK = "eClock."


__all__ = [
    "Event",
    "EventEngine",
    "HandlerType",

    "EVENT_TIMER",

    # "EVENT_TICK",
    #
    # "EVENT_TRADE",
    # "EVENT_ORDER",

    "EVENT_LOG",
    "EVENT_EMAIL",
    "EVENT_ADD_TASK"
    # "EVENT_CLOCK"
]
