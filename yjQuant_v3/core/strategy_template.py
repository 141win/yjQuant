"""
策略模板基类 - 基于BaseStrategy设计

所有策略必须继承此类并实现抽象方法。提供统一的数据获取、
计算逻辑和结果返回接口。
"""

import logging
import pandas as pd
from abc import ABC, abstractmethod
from typing import Dict, Any, Optional
# from datetime import datetime, timezone, timedelta
# import re

logger = logging.getLogger(__name__)


class StrategyResult:
    """策略返回的统一结构定义"""
    
    def __init__(self, strategy_name: str, description: str, timeframe: str, result_df: pd.DataFrame):
        self.strategy_name = strategy_name
        self.description = description
        self.timeframe = timeframe
        self.result_df = result_df
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "strategy_name": self.strategy_name,
            "description": self.description,
            "timeframe": self.timeframe,
            "result_df": self.result_df.to_dict('records') if not self.result_df.empty else []
        }
    
    def __str__(self) -> str:
        return f"StrategyResult({self.strategy_name}, hits={len(self.result_df)})"


class StrategyContext:
    """策略上下文：统一注入策略运行所需依赖"""
    
    def __init__(
        self,
        *,
        db_manager: Optional[Any] = None,
        redis_manager: Optional[Any] = None,
        strategy_config: Optional[Any] = None,
    ) -> None:
        self.db_manager = db_manager # 提供数据库读操作
        self.redis_manager = redis_manager # 提供redis读写操作
        self.strategy_config = strategy_config # 策略配置

class StrategyTemplate(ABC):
    """策略模板基类 - 基于BaseStrategy设计"""
    
    def __init__(self, name: str, description: str, timeframe: str, context: StrategyContext):
        self.name = name
        self.description = description
        self.timeframe = timeframe
        self.context = context
        
        # 监控清单
        self._monitored_map: Dict[str, set] = {}
        
        logger.info(f"策略模板初始化完成: {name}, timeframe: {timeframe}")

    # < ----------------实时函数---------------------->
    @abstractmethod
    async def on_realtime(self, df: pd.DataFrame) -> Optional[StrategyResult]:
        """处理实时行情并返回结果"""
        raise NotImplementedError
    
    def initialize(self) -> None:
        """初始化钩子（可选）"""
        pass
    
    def __str__(self) -> str:
        return f"StrategyTemplate({self.name}, timeframe={self.timeframe})"
    
    def __repr__(self) -> str:
        return self.__str__()
