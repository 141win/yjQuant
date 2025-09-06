"""
Redis管理器 - 负责Redis连接管理和K线数据缓存

职责:
- 管理Redis连接池
- 批量写入K线数据到ZSET
- 提供配置变更接口
"""

import logging
import json
from typing import Dict, Any, List, Tuple
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
        
        # Redis连接参数
        self.host = config.get("host", "localhost")
        self.port = config.get("port", 6379)
        self.db = config.get("db", 0)
        self.password = config.get("password")
        self.max_connections = config.get("max_connections", 10)
        self.cache_ttl_minute = config.get("caching_ttl_minute", 4320)
        
        logger.info(f"Redis管理器初始化完成: {self.host}:{self.port}")
    
    async def start(self) -> None:
        """启动Redis管理器"""
        try:
            # 创建连接池
            self.connection_pool = redis.ConnectionPool.from_url(
                f"redis://{self.host}:{self.port}/{self.db}",
                password=self.password,
                max_connections=self.max_connections,
                decode_responses=True
            )
            
            # 创建Redis客户端
            self.redis_client = redis.Redis(connection_pool=self.connection_pool)
            
            # 测试连接
            await self.redis_client.ping()
            
            logger.info("Redis管理器启动成功")
            
        except Exception as e:
            logger.error(f"Redis管理器启动失败: {e}")
            raise
    
    async def stop(self) -> None:
        """停止Redis管理器"""
        try:
            if self.redis_client:
                await self.redis_client.close()
                self.redis_client = None
            
            if self.connection_pool:
                await self.connection_pool.disconnect()
                self.connection_pool = None
            
            logger.info("Redis管理器已停止")
            
        except Exception as e:
            logger.error(f"Redis管理器停止失败: {e}")
    
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
