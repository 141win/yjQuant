"""
策略模板基类 - 基于BaseStrategy设计

所有策略必须继承此类并实现抽象方法。提供统一的数据获取、
计算逻辑和结果返回接口。
"""

import logging
import asyncio
import pandas as pd
from abc import ABC, abstractmethod
from typing import Dict, Any, Optional, List, Tuple
from datetime import datetime, timezone, timedelta
import re

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
        global_config: Any,
        event_engine: Any,
        db_reader: Optional[Any] = None,
        cache: Optional[Any] = None,
        logger: Optional[logging.Logger] = None,
        strategy_item: Optional[Any] = None,
    ) -> None:
        self.global_config = global_config
        self.event_engine = event_engine
        self.db_reader = db_reader
        self.cache = cache
        self.logger = logger or logging.getLogger(f"{__name__}.strategy")
        self.strategy_item = strategy_item
        self.current_unit_epoch: Optional[int] = None
        self.prefilter_data: Dict[str, Any] = {}


class StrategyTemplate(ABC):
    """策略模板基类 - 基于BaseStrategy设计"""
    
    def __init__(self, name: str, description: str, timeframe: str, context: StrategyContext):
        self.name = name
        self.description = description
        self.timeframe = timeframe
        self.context = context
        
        # 监控清单
        self._monitored_map: Dict[str, set] = {}
        # self._parse_monitored_pairs()
        
        logger.info(f"策略模板初始化完成: {name}, timeframe: {timeframe}")
    
    # def _parse_monitored_pairs(self):
    #     """解析监控清单"""
    #     try:
    #         si = self.context.strategy_item
    #         if si and hasattr(si, "exchange_configs"):
    #             for ex_cfg in si.exchange_configs:
    #                 ex = getattr(ex_cfg, "exchange", None)
    #                 pairs = getattr(ex_cfg, "trading_pairs", set())
    #                 if ex:
    #                     self._monitored_map.setdefault(ex, set()).update(pairs)
    #     except Exception as e:
    #         self.context.logger.warning(f"解析策略监控清单失败，默认不过滤: {e}")

    # < ----------------实时函数---------------------->
    @abstractmethod
    def on_realtime(self, df: pd.DataFrame) -> Optional[StrategyResult]:
        """处理实时行情并返回结果"""
        raise NotImplementedError


    # <------------------预筛选函数-------------------->
    def compute_unit_prefilter(self, unit_start_ts: int) -> Optional[Dict[str, Any]]:
        """计算预筛数据（可选）"""
        return None
    
    def initialize(self) -> None:
        """初始化钩子（可选）"""
        pass

    # 过滤交易对
    def filter_pairs(self, df: pd.DataFrame) -> pd.DataFrame:
        """按监控清单过滤数据"""
        if not isinstance(df, pd.DataFrame) or df.empty:
            return df
        
        if not self._monitored_map:
            return df
        
        required_cols = {"exchange", "symbol"}
        missing = required_cols - set(df.columns)
        if missing:
            self.context.logger.warning(f"输入数据缺少必要列: {missing}，将跳过过滤")
            return df
        
        mask = pd.Series(False, index=df.index)
        for ex, pairs in self._monitored_map.items():
            sub = (df["exchange"] == ex) & (df["symbol"].isin(pairs))
            mask = mask | sub
        return df[mask]
    
    # 工具方法
    def get_monitored_pairs(self) -> List[Tuple[str, str]]:
        """获取监控的交易对列表"""
        pairs = []
        for ex, syms in self._monitored_map.items():
            for sym in syms:
                pairs.append((str(sym), str(ex)))
        return pairs

    def align_timeframe_boundary(self, unit_start_ts: int, timeframe: str, window_steps: int = 1) -> Tuple[datetime, datetime]:
        """按时间粒度对齐边界"""
        m = re.match(r"^(\d+)([smhdw])$", timeframe.strip().lower())
        if not m:
            raise ValueError(f"无效的 timeframe: {timeframe!r}")
        val, unit = int(m.group(1)), m.group(2)
        factor = {"s": 1, "m": 60, "h": 3600, "d": 86400, "w": 7 * 86400}[unit]
        sec = max(1, val * factor)
        unit_dt = datetime.fromtimestamp(int(unit_start_ts), tz=timezone.utc)
        epoch = int(unit_dt.timestamp())
        aligned_epoch = epoch - (epoch % sec)
        end_dt = datetime.fromtimestamp(aligned_epoch, tz=timezone.utc)
        start_dt = end_dt - timedelta(seconds=sec * max(1, int(window_steps)))
        return start_dt, end_dt
    
    # 缓存操作
    def cache_get(self, key: str) -> Any:
        """获取缓存值"""
        cache = getattr(self.context, "cache", None)
        if cache is None:
            return None
        
        async def _get():
            return await cache.get(key)
        
        return self._await_in_main_loop(_get())
    
    def cache_set(self, key: str, value: Any, expire: Optional[int] = None) -> bool:
        """设置缓存值"""
        cache = getattr(self.context, "cache", None)
        if cache is None:
            return False
        
        async def _set():
            return await cache.set(key, value, expire)
        
        return bool(self._await_in_main_loop(_set()))
    
    def cache_zrevrangebyscore(self, key: str, max_score: Any = "+inf", min_score: Any = "-inf", 
                               start: int = 0, num: int = 100, withscores: bool = False) -> List[Any]:
        """从Redis ZSET按分数范围获取数据"""
        cache = getattr(self.context, "cache", None)
        if cache is None:
            return []
        
        async def _zr():
            return await cache.zrevrangebyscore(key, max_score, min_score, start=start, num=num, withscores=withscores)
        
        res = self._await_in_main_loop(_zr())
        return res if isinstance(res, list) else []
    
    # def _await_in_main_loop(self, coro: Any, timeout: Optional[float] = 30) -> Any:
    #     """在主事件循环中等待协程"""
    #     try:
    #         loop = self._get_main_loop()
    #         if loop is not None and loop.is_running():
    #             fut = asyncio.run_coroutine_threadsafe(coro, loop)
    #             return fut.result(timeout=timeout)
    #     except RuntimeError:
    #         pass
    #     return asyncio.run(coro)
    #
    # def _get_main_loop(self) -> Optional[asyncio.AbstractEventLoop]:
    #     """获取主事件循环"""
    #     get_loop = getattr(self.context.event_engine, "get_main_loop", None)
    #     return get_loop() if callable(get_loop) else None
    
    def __str__(self) -> str:
        return f"StrategyTemplate({self.name}, timeframe={self.timeframe})"
    
    def __repr__(self) -> str:
        return self.__str__()
