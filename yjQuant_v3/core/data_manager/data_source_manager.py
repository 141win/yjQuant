"""
数据源管理器 - 负责从外部数据源获取K线数据

职责:
- 管理多个数据源连接
- 实现CCXT接口获取K线数据
- 支持分钟级和小时级K线数据获取
- 处理数据源配置变更
"""

import asyncio
import logging
from typing import Dict, Any, List
from datetime import datetime, timedelta, timezone
import ccxt.async_support as ccxt

logger = logging.getLogger(__name__)


class DataSourceManager:
    """
    数据源管理器 - 负责从外部数据源获取K线数据
    
    功能:
    - 支持分钟级和小时级K线数据获取 (fetch方法)
    - 通过timeframe参数指定时间框架 ("1m" 或 "1h")
    - 并发获取多个交易所数据
    - 自动重试机制
    """

    def __init__(self, config: List[Dict[str, Any]]):
        """
        初始化数据源管理器
        
        Args:
            config: 数据源配置列表
        """
        self.config = config
        self.exchanges_config = {}
        for con in self.config:
            self.exchanges_config[con["exchange_id"]] = con
        # 重试配置
        self.max_retries = 3
        self.retry_base_delay = 0.8

        logger.info(f"数据源管理器初始化完成，配置了 {len(config)} 个数据源")

    # 对外开放：更新配置
    async def update_config(self, new_config: List[Dict[str, Any]]) -> None:
        """更新配置"""
        try:
            # 更新配置
            self.exchanges_config.clear()
            self.config = new_config
            for config in self.config:
                self.exchanges_config[config["exchange_id"]] = config
            logger.info("数据源配置更新完成")

        except Exception as e:
            logger.error(f"数据源配置更新失败: {e}")

    # 内部方法：通用的K线数据获取方法
    async def _fetch_klines_by_timeframe(self, timeframe: str, timestamp_func, data_type: str):
        """
        通用的K线数据获取方法
        
        Args:
            timeframe: 时间框架 ("1m" 或 "1h")
            timestamp_func: 时间戳获取函数
            data_type: 数据类型描述（用于日志）
        
        Returns:
            List[Tuple[str, str, List]]: [(exchange_id, symbol, kline_data), ...]
        """
        ts = timestamp_func()
        # asyncio.gather并发执行多个任务，完成先后顺序可能不固定，但会确保返回结果的顺序与传入的协程顺序一致
        tasks = []
        for exchange_id, config in self.exchanges_config.items():
            tasks.append(asyncio.create_task(self._fetch_all_klines(exchange_id, config.get("symbols"), timeframe, ts)))
        results = await asyncio.gather(*tasks)
        
        all_klines = []
        for (exchange_id, config), klines in zip(self.exchanges_config.items(), results):
            for symbol, kline in klines:
                if kline:
                    all_klines.append((exchange_id, symbol, kline))
        
        logger.info(f"获取到 {len(all_klines)} 条{data_type}K线数据")
        return all_klines
    
    # 对外方法：获取K线数据
    async def fetch(self, timeframe: str = "1m"):
        """
        获取所有交易所的K线数据
        
        Args:
            timeframe: 时间框架，支持 "1m"(分钟级) 或 "1h"(小时级)，默认为 "1m"
        
        Returns:
            List[Tuple[str, str, List]]: [(exchange_id, symbol, kline_data), ...]
        
        Raises:
            ValueError: 当timeframe参数不支持时
        """
        if timeframe == "1m":
            return await self._fetch_klines_by_timeframe("1m", self._get_last_minute_timestamp, "分钟级")
        elif timeframe == "1h":
            return await self._fetch_klines_by_timeframe("1h", self._get_last_hour_timestamp, "小时级")
        else:
            raise ValueError(f"不支持的时间框架: {timeframe}，支持的时间框架: '1m', '1h'")

    # 内部方法：从指定交易所获取指定交易对列表数据
    async def _fetch_all_klines(self, exchange_id: Any, symbols: str, timeframe, since):
        """
        输入：交易所ID、符号列表、周期、起始时间
        输出：[(symbol, kline或None), ...]
        主要逻辑：创建并预加载市场，批量并发抓取K线
        """
        exchange = self._create_exchange(exchange_id)
        try:
            # 预加载市场，带重试
            await self._async_retry(exchange.load_markets)
            # 异步批量请求所有交易对
            tasks = [self._fetch_ohlcv(exchange, symbol, timeframe, since) for symbol in symbols]
            results = await asyncio.gather(*tasks)
            return results
        finally:
            await exchange.close()

    # 内部方法：异步重试
    async def _async_retry(self, fn, *args, **kwargs):
        """异步重试机制"""
        for attempt in range(1, self.max_retries + 1):
            try:
                return await fn(*args, **kwargs)
            except Exception as e:
                if attempt == self.max_retries:
                    raise
                delay = self.retry_base_delay * (2 ** (attempt - 1))
                logger.warning(f"重试 {attempt}/{self.max_retries} 失败: {repr(e)}; 等待 {delay:.2f}s")
                await asyncio.sleep(delay)
                return None
        return None

    """----------------------------辅助函数---------------------------------"""
    # 内部方法：从指定交易所获取指定交易对数据，主要由_fetch_all_klines()调用
    @staticmethod
    async def _fetch_ohlcv(exchange, symbol, timeframe, since):
        try:
            ohlcv = await exchange.fetch_ohlcv(symbol, timeframe=timeframe, since=since, limit=1)
            return symbol, ohlcv[0] if ohlcv else None
        except Exception as e:
            import traceback
            print(f"{exchange.id} {symbol} error: {repr(e)}")
            traceback.print_exc()
            return symbol, None

    # 内部方法：创建交易所实例
    @staticmethod
    def _create_exchange(exchange_id: str):
        """创建CCXT交易所实例"""
        try:
            # 代理配置
            PROXY_CONFIG = {
                'http': 'http://127.0.0.1:7890',
                'https': 'http://127.0.0.1:7890',
            }
            # 超时时间
            HTTP_TIMEOUT_MS = 20000
            # 使用 async.py 的稳定配置
            common_kwargs = {
                'enableRateLimit': True,
                'sandbox': False,
                'proxies': PROXY_CONFIG,
                'aiohttp_proxy': PROXY_CONFIG['https'],
                'adjustForTimeDifference': True,
                'timeout': HTTP_TIMEOUT_MS,
            }

            # 创建交易所实例
            if exchange_id == 'binance':
                return ccxt.binance({**common_kwargs, 'options': {'defaultType': 'spot'}})
            elif exchange_id == 'gateio':
                return ccxt.gateio({**common_kwargs, 'options': {'defaultType': 'spot'}})
            else:
                raise ValueError("unsupported exchange id")

        except Exception as e:
            logger.error(f"创建CCXT交易所失败: {exchange_id}, 错误: {e}")
            return None


    # 内部方法：获取当前时间的前一分钟时间戳，毫秒单位
    @staticmethod
    def _get_last_minute_timestamp():
        """
        输入：无
        输出：上一个完整分钟的UTC毫秒时间戳
        主要逻辑：取当前UTC时间-1分钟，并抹去秒与微秒
        """
        now = datetime.now(timezone.utc)
        last_min = now - timedelta(minutes=1)
        ts = int(last_min.replace(second=0, microsecond=0).timestamp() * 1000)
        return ts
    
    # 内部方法：获取当前时间的前一小时时间戳，毫秒单位
    @staticmethod
    def _get_last_hour_timestamp():
        """
        输入：无
        输出：上一个完整小时的UTC毫秒时间戳
        主要逻辑：取当前UTC时间-1小时，并抹去分钟、秒与微秒
        """
        now = datetime.now(timezone.utc)
        last_hour = now - timedelta(hours=1)
        ts = int(last_hour.replace(minute=0, second=0, microsecond=0).timestamp() * 1000)
        return ts