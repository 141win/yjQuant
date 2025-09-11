"""
Redis管理器 - 负责Redis连接管理和K线数据缓存

职责:
- 管理Redis连接池
- 批量写入K线数据到ZSET
- 提供配置变更接口
"""

import logging
import json
import asyncio
from typing import Dict, Any, List, Tuple, Optional
from datetime import datetime
import redis.asyncio as redis

logger = logging.getLogger(__name__)


class RedisManager:
    """Redis管理器 - 负责Redis连接管理和K线数据缓存"""
    
    def __init__(self, config: Dict[str, Any]):
        """
        初始化Redis管理器
        
        Args:
            config: Redis配置
        """
        self.config = config
        self.redis_client = None
        self.connection_pool = None
        self._health_check_task = None
        
        # Redis连接参数
        self.host = config.get("host", "localhost")
        self.port = config.get("port", 6379)
        self.db = config.get("db", 0)
        self.password = config.get("password")
        self.max_connections = config.get("max_connections", 10)
        self.cache_ttl_minute = config.get("caching_ttl_minute", 4320)
        
        # 连接超时和重试配置
        self.socket_timeout = config.get("socket_timeout", 5)  # 套接字超时（秒）
        self.socket_connect_timeout = config.get("socket_connect_timeout", 60)  # 连接超时（秒）
        self.retry_on_timeout = config.get("retry_on_timeout", True)  # 超时时重试
        self.health_check_interval = config.get("health_check_interval", 30)  # 健康检查间隔（秒）
        
        logger.info(f"Redis管理器初始化完成: {self.host}:{self.port}")

    # 检查连接
    async def _check_connection(self) -> bool:
        """检查Redis连接是否健康"""
        try:
            if not self.redis_client:
                return False
            await self.redis_client.ping()
            return True
        except Exception as e:
            logger.warning(f"Redis连接检查失败: {e}")
            return False

    # 确保连接
    async def _ensure_connection(self) -> bool:
        """确保Redis连接可用，如果连接断开则重新连接"""
        if await self._check_connection():
            return True
        
        logger.warning("Redis连接已断开，尝试重新连接...")
        try:
            # 重新创建连接
            await self.stop()
            await self.start()
            return True
        except Exception as e:
            logger.error(f"Redis重连失败: {e}")
            return False

    # 健康检查循环
    async def _health_check_loop(self) -> None:
        """定期健康检查循环"""
        while True:
            try:
                await asyncio.sleep(self.health_check_interval)
                
                if not await self._check_connection():
                    logger.warning("健康检查发现Redis连接异常，尝试重连...")
                    await self._ensure_connection()
                    
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"健康检查循环出错: {e}")
                await asyncio.sleep(5)  # 出错后等待5秒再继续

    # 启动
    async def start(self) -> None:
        """启动Redis管理器"""
        try:
            # 创建连接池
            self.connection_pool = redis.ConnectionPool.from_url(
                f"redis://{self.host}:{self.port}/{self.db}",
                password=self.password,
                max_connections=self.max_connections,
                decode_responses=True,
                socket_timeout=self.socket_timeout,
                socket_connect_timeout=self.socket_connect_timeout,
                retry_on_timeout=self.retry_on_timeout
            )
            
            # 创建Redis客户端
            self.redis_client = redis.Redis(connection_pool=self.connection_pool)
            
            # 测试连接
            await self.redis_client.ping()
            
            # 启动健康检查任务
            self._health_check_task = asyncio.create_task(self._health_check_loop())
            
            logger.info("Redis管理器启动成功")
            
        except Exception as e:
            logger.error(f"Redis管理器启动失败: {e}")
            raise

    # 停止
    async def stop(self) -> None:
        """停止Redis管理器"""
        try:
            # 停止健康检查任务
            if self._health_check_task:
                self._health_check_task.cancel()
                try:
                    await self._health_check_task
                except asyncio.CancelledError:
                    pass
                self._health_check_task = None
            
            if self.redis_client:
                await self.redis_client.close()
                self.redis_client = None
            
            if self.connection_pool:
                await self.connection_pool.disconnect()
                self.connection_pool = None
            
            logger.info("Redis管理器已停止")
            
        except Exception as e:
            logger.error(f"Redis管理器停止失败: {e}")

    # 更新配置
    async def update_config(self, new_config: Dict[str, Any]) -> None:
        """更新配置"""
        try:
            # old_config = self.config.copy()
            self.config.update(new_config)
            
            # 如果连接参数有变化，重新连接
            if (self.host != new_config.get("host", self.host) or
                self.port != new_config.get("port", self.port) or
                self.db != new_config.get("db", self.db) or
                self.password != new_config.get("password", self.password)):
                
                logger.info("Redis连接参数有变化，重新连接")
                await self.stop()
                await self.start()
            
            logger.info("Redis配置更新成功")
            
        except Exception as e:
            logger.error(f"Redis配置更新失败: {e}")

    # 对外开放：提供批量化插入redis
    async def batch_store_klines(self, klines_data: List[Tuple[str, str, List]]) -> bool:
        """
        批量写入K线数据到Redis ZSET
        
        Args:
            klines_data: K线数据列表，格式为 [(exchange, symbol, kline), ...]
            # expire_minutes: 过期时间（分钟），默认3天  expire_minutes: int = 3 * 24 * 60
            
        Returns:
            写入是否成功
        """
        try:
            if not klines_data:
                return True
            
            # 确保连接可用
            if not await self._ensure_connection():
                logger.error("无法建立Redis连接")
                return False
            
            # 以毫秒为单位的时间窗口
            window_ms = self.config.get("cache_ttl_minute")* 60 * 1000 # expire_minutes * 60 * 1000
            cutoff = int(datetime.now().timestamp() * 1000) - window_ms
            
            pipe = self.redis_client.pipeline()
            
            for exchange, symbol, kline in klines_data:
                # 生成Redis键
                key = f'kline:{exchange}:{symbol.replace("/", "_").replace(":", "_")}'
                
                # 转为JSON字符串
                value = json.dumps({
                    'timestamp': kline[0],
                    'open': kline[1],
                    'high': kline[2],
                    'low': kline[3],
                    'close': kline[4],
                    'volume': kline[5]
                })
                
                # 添加到ZSET
                pipe.zadd(key, {value: kline[0]})
                # 删除过期数据
                pipe.zremrangebyscore(key, 0, cutoff)
            
            # 执行管道命令
            if klines_data:
                await pipe.execute()
                logger.info(f"批量写入Redis ZSET成功，共 {len(klines_data)} 条数据")
            
            return True
            
        except Exception as e:
            logger.error(f"批量写入Redis ZSET失败: {e}")
            # 如果是连接相关错误，尝试重连
            if "网络" in str(e) or "连接" in str(e) or "timeout" in str(e).lower():
                logger.info("检测到连接错误，尝试重新连接...")
                try:
                    await self.stop()
                    await self.start()
                    logger.info("Redis重连成功")
                except Exception as reconnect_error:
                    logger.error(f"Redis重连失败: {reconnect_error}")
            return False

    async def store_strategy_trigger_prices(self, strategy_name: str, trigger_data: List[Dict[str, Any]]) -> bool:
        """
        存储策略触发价格数据（专门用于存储上次满足条件的交易对价格）
        
        Args:
            strategy_name: 策略名称
            trigger_data: 触发数据列表，格式为 [{"exchange": "binance", "symbol": "BTC/USDT", "price": 50000, "timestamp": 1640995200000}, ...]
            
        Returns:
            存储是否成功
        """
        try:
            if not await self._ensure_connection():
                logger.error("无法建立Redis连接")
                return False
            
            # 生成策略触发价格键
            key = f"strategy:{strategy_name}:trigger_prices"
            
            # 添加更新时间戳
            data_with_timestamp = {
                "trigger_prices": trigger_data,
                "updated_at": int(datetime.now().timestamp() * 1000)
            }
            
            # 存储为JSON字符串
            value = json.dumps(data_with_timestamp, ensure_ascii=False)
            
            # 设置键值对和过期时间  _build_email_content  策略上次价格数据永不过期
            await self.redis_client.set(key, value)
            
            logger.debug(f"策略触发价格存储成功: {strategy_name}, 共 {len(trigger_data)} 条")
            return True
            
        except Exception as e:
            logger.error(f"存储策略触发价格失败: {e}")
            return False

    async def get_strategy_trigger_prices(self, strategy_name: str) -> Optional[List[Dict[str, Any]]]:
        """
        从Redis读取策略触发价格数据
        
        Args:
            strategy_name: 策略名称
            
        Returns:
            触发价格数据列表，如果不存在或读取失败返回None
        """
        try:
            if not await self._ensure_connection():
                logger.error("无法建立Redis连接")
                return None
            
            # 生成策略触发价格键
            key = f"strategy:{strategy_name}:trigger_prices"
            
            # 获取数据
            value = await self.redis_client.get(key)
            if not value:
                logger.debug(f"策略触发价格数据不存在: {strategy_name}")
                return None
            
            # 解析JSON
            data = json.loads(value)
            trigger_prices = data.get("trigger_prices", [])
            
            logger.debug(f"策略触发价格读取成功: {strategy_name}, 共 {len(trigger_prices)} 条")
            return trigger_prices
            
        except Exception as e:
            logger.error(f"读取策略触发价格失败: {e}")
            return None

    async def update_strategy_trigger_prices_batch(self, strategy_name: str, trigger_data: List[Dict[str, Any]]) -> bool:
        """
        批量更新策略触发价格（增量更新，保留其他交易对数据）
        
        Args:
            strategy_name: 策略名称
            trigger_data: 触发数据列表，格式为 [{"exchange": "binance", "symbol": "BTC/USDT", "price": 50000, "timestamp": 1640995200000}, ...]
        Returns:
            更新是否成功
        """
        try:
            if not await self._ensure_connection():
                logger.error("无法建立Redis连接")
                return False
            
            # 先读取现有数据
            existing_data = await self.get_strategy_trigger_prices(strategy_name)
            if existing_data is None:
                existing_data = []
            
            # 高效的数据合并
            # 使用字典结构 {exchange_symbol: data} 进行快速查找和更新
            # 避免重复遍历，提高性能
            
            # 创建现有数据的字典，便于快速查找
            existing_dict = {}
            for item in existing_data:
                key = f"{item.get('exchange')}_{item.get('symbol')}"
                existing_dict[key] = item
            
            # 更新或添加新的触发价格数据
            for new_item in trigger_data:
                key = f"{new_item.get('exchange')}_{new_item.get('symbol')}"
                existing_dict[key] = new_item
            
            # 转换回列表格式
            updated_data = list(existing_dict.values())
            
            # 生成策略触发价格键
            redis_key = f"strategy:{strategy_name}:trigger_prices"
            
            # 添加更新时间戳
            data_with_timestamp = {
                "trigger_prices": updated_data,
                "updated_at": int(datetime.now().timestamp() * 1000)
            }
            
            # 存储为JSON字符串
            value = json.dumps(data_with_timestamp, ensure_ascii=False)
            
            # 设置键值对（永不过期）
            await self.redis_client.set(redis_key, value)
            
            logger.debug(f"策略触发价格批量更新成功: {strategy_name}, 更新 {len(trigger_data)} 条，总计 {len(updated_data)} 条")
            return True
            
        except Exception as e:
            logger.error(f"批量更新策略触发价格失败: {e}")
            return False

    async def update_strategy_trigger_price(self, strategy_name: str, exchange: str, symbol: str, price: float, timestamp: int) -> bool:
        """
        更新单个交易对的触发价格（增量更新）
        
        Args:
            strategy_name: 策略名称
            exchange: 交易所
            symbol: 交易对
            price: 触发价格
            timestamp: 时间戳（毫秒）
            
        Returns:
            更新是否成功
        """
        try:
            # 先读取现有数据
            existing_data = await self.get_strategy_trigger_prices(strategy_name)
            if existing_data is None:
                existing_data = []
            
            # 查找并更新或添加
            updated = False
            for item in existing_data:
                if item.get("exchange") == exchange and item.get("symbol") == symbol:
                    item["price"] = price
                    item["timestamp"] = timestamp
                    updated = True
                    break
            
            # 如果没找到，添加新记录
            if not updated:
                existing_data.append({
                    "exchange": exchange,
                    "symbol": symbol,
                    "price": price,
                    "timestamp": timestamp
                })
            
            # 存储更新后的数据
            return await self.store_strategy_trigger_prices(strategy_name, existing_data)
            
        except Exception as e:
            logger.error(f"更新策略触发价格失败: {e}")
            return False

    def get_status(self) -> Dict[str, Any]:
        """获取状态信息"""
        return {
            "host": self.host,
            "port": self.port,
            "db": self.db,
            "max_connections": self.max_connections,
            "connected": self.redis_client is not None
        }
