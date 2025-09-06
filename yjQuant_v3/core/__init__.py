"""
核心组件模块

包含系统的基础架构组件:
- EventEngine: 事件引擎
- ClockEngine: 时钟引擎
- ConfigManager: 配置管理器
- StrategyEngine: 策略引擎
- DataEngine: 数据引擎
- EmailEngine: 邮件引擎
"""

from .event_engine import EventEngine
from .clock_engine import ClockEngine
from .config_manager import ConfigManager
from .strategy_engine import StrategyEngine
from .data_engine import DataEngine
from .email_engine import EmailEngine
try:
    from .network.server import NetworkServer
    from .network.client import NetworkClient
except Exception:
    # 网络模块为可选
    NetworkServer = None
    NetworkClient = None

__all__ = [
    "EventEngine",
    "ClockEngine",
    "ConfigManager",
    "StrategyEngine",
    "DataEngine",
    "EmailEngine",
    "NetworkServer",
    "NetworkClient",
]
