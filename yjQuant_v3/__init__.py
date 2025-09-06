"""
yjQuant v3.0 - 量化交易系统核心组件

主要组件:
- EventEngine: 事件引擎 - 纯事件分发器
- ClockEngine: 时钟引擎 - 纯定时事件发布器  
- ConfigManager: 配置管理器 - 纯配置管理器
- StrategyEngine: 策略引擎 - 策略管理器
- DataEngine: 数据引擎 - 数据管理器
- EmailEngine: 邮件引擎 - 邮件管理器
"""

__version__ = "3.0.0"
__author__ = "yjQuant Team"

from .core.event_engine import EventEngine
from .core.clock_engine import ClockEngine
from .core.config_manager import ConfigManager
from .core.strategy_engine import StrategyEngine
from .core.data_engine import DataEngine
from .core.email_engine import EmailEngine

__all__ = ["EventEngine", "ClockEngine", "ConfigManager", "StrategyEngine", "DataEngine", "EmailEngine"]
