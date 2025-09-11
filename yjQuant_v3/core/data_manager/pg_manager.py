"""
PostgreSQL管理器 - 负责PostgreSQL连接管理和K线数据存储

职责:
- 管理PostgreSQL连接池
- 提供K线数据批量插入功能
- 处理数据库连接配置更新
- 提供连接状态查询
"""

import asyncpg

from typing import Dict, Any, List, Tuple
from datetime import datetime, timedelta, timezone
import logging

logger = logging.getLogger(__name__)


class PgManager:
    """PostgreSQL管理器 - 负责PostgreSQL连接管理和K线数据存储"""

    def __init__(self, config: Dict[str, Any]):
        """
        初始化PostgreSQL管理器

        Args:
            config: PostgreSQL配置
        """
        self.config = config
        self.connection_pool = None

        # PostgreSQL连接参数
        self.host = config.get("host", "localhost")
        self.port = config.get("port", 5432)
        self.database = config.get("database", "postgres")
        self.table_name = config.get("table", "_1h")
        self.user = config.get("user", "postgres")
        self.password = config.get("password", "")
        self.min_connections = config.get("min_connections", 1)
        self.max_connections = config.get("max_connections", 10)

        logger.info(f"PostgreSQL管理器初始化完成: {self.host}:{self.port}/{self.database}")

    async def start(self) -> None:
        """启动PostgreSQL管理器"""
        try:
            # 创建PostgreSQL连接池
            self.connection_pool = await asyncpg.create_pool(
                host=self.host,
                port=self.port,
                database=self.database,
                user=self.user,
                password=self.password,
                min_size=self.min_connections,
                max_size=self.max_connections
            )

            # 测试连接
            async with self.connection_pool.acquire() as conn:
                await conn.fetchval('SELECT 1')

            logger.info("PostgreSQL管理器启动成功")

        except Exception as e:
            logger.error(f"PostgreSQL管理器启动失败: {e}")
            raise

    async def stop(self) -> None:
        """停止PostgreSQL管理器"""
        try:
            if self.connection_pool:
                await self.connection_pool.close()
                self.connection_pool = None

            logger.info("PostgreSQL管理器已停止")

        except Exception as e:
            logger.error(f"PostgreSQL管理器停止失败: {e}")

    async def update_config(self, new_config: Dict[str, Any]) -> None:
        """更新配置"""
        try:
            # old_config = self.config.copy()
            self.config.update(new_config)

            # 如果连接参数有变化，重新连接
            if (self.host != new_config.get("host", self.host) or
                    self.port != new_config.get("port", self.port) or
                    self.database != new_config.get("database", self.database) or
                    self.user != new_config.get("user", self.user) or
                    self.password != new_config.get("password", self.password)):
                logger.info("PostgreSQL连接参数有变化，重新连接")
                await self.stop()
                await self.start()

            logger.info("PostgreSQL配置更新成功")

        except Exception as e:
            logger.error(f"PostgreSQL配置更新失败: {e}")

    # 对外开放：提供批量化插入pg数据库
    async def batch_store_klines(self, klines_data: List[Tuple[str, str, List]]) -> bool:
        """
        批量写入K线数据到PostgreSQL数据库

        Args:
            klines_data: K线数据列表，格式为 [(exchange, symbol, kline), ...]

        Returns:
            写入是否成功
        """
        try:
            if not klines_data:
                return True

            if not self.connection_pool:
                logger.error("PostgreSQL连接池未初始化")
                return False

            # 准备批量插入的数据
            rows = []
            # timestamp_dt = self._ms_to_china_datetime(int(klines_data[0][2][0]))
            for exchange, symbol, kline in klines_data:
                # 确保kline数据格式正确: [timestamp_ms, open, high, low, close, volume]
                if len(kline) >= 6:
                    # 将毫秒时间戳转换为中国时间（无时区的datetime）

                    timestamp_dt = self._ms_to_china_datetime(int(kline[0]))

                    rows.append((
                        timestamp_dt,  # timestamp (TIMESTAMP WITHOUT TIME ZONE)
                        float(kline[1]),  # open
                        float(kline[2]),  # high
                        float(kline[3]),  # low
                        float(kline[4]),  # close
                        float(kline[5]),  # volume
                        exchange,  # exchange
                        symbol.replace("/", "_").replace(":", "_")  # pair
                    ))

            if not rows:
                logger.warning("没有有效的K线数据需要插入")
                return True

            # 使用连接池执行批量插入
            async with self.connection_pool.acquire() as conn:
                async with conn.transaction():
                    await conn.executemany(
                        f"INSERT INTO {self.table_name} "
                        "(timestamp, open, high, low, close, volume, exchange, pair) "
                        "VALUES ($1, $2, $3, $4, $5, $6, $7, $8) "
                        "ON CONFLICT (timestamp, exchange, pair) DO UPDATE SET "
                        "open = EXCLUDED.open, high = EXCLUDED.high, "
                        "low = EXCLUDED.low, close = EXCLUDED.close, volume = EXCLUDED.volume",
                        rows
                    )

            logger.info(f"批量写入PostgreSQL成功，共 {len(rows)} 条数据")
            return True

        except Exception as e:
            logger.error(f"批量写入PostgreSQL失败: {e}")
            return False

    # 对外开放：获取状态信息
    def get_status(self) -> Dict[str, Any]:
        """获取状态信息"""
        return {
            "host": self.host,
            "port": self.port,
            "database": self.database,
            "user": self.user,
            "min_connections": self.min_connections,
            "max_connections": self.max_connections,
            "connected": self.connection_pool is not None
        }

    # 通用数据库查询方法
    async def execute_query(self, sql: str, params: List[Any] = None) -> List[Dict]:
        """
        执行SQL查询并返回结果
        
        Args:
            sql: SQL查询语句
            params: 查询参数列表
            
        Returns:
            查询结果列表，每行为字典格式
        """
        try:
            if not self.connection_pool:
                logger.error("PostgreSQL连接池未初始化")
                return []
            
            async with self.connection_pool.acquire() as conn:
                if params:
                    rows = await conn.fetch(sql, *params)
                else:
                    rows = await conn.fetch(sql)
            
            # 转换结果格式
            result = []
            for row in rows:
                try:
                    # 将行数据转换为字典，处理特殊类型
                    row_dict = {}
                    for key, value in row.items():
                        if hasattr(value, 'timestamp'):
                            # datetime对象转换为毫秒时间戳
                            row_dict[key] = int(value.timestamp() * 1000)
                        elif hasattr(value, '__float__'):
                            # Decimal等数值类型转换为float
                            row_dict[key] = float(value)
                        else:
                            # 其他类型直接使用
                            row_dict[key] = value
                    
                    result.append(row_dict)
                except Exception as row_error:
                    logger.error(f"处理行数据失败: {row_error}, 行数据: {dict(row)}")
                    continue
            
            logger.debug(f"SQL查询执行成功，返回 {len(result)} 条数据")
            return result
            
        except Exception as e:
            logger.error(f"SQL查询执行失败: {e}")
            return []

    async def execute_query_with_timeframe(self, sql_template: str, timeframe: str, params: List[Any] = None) -> List[Dict]:
        """
        执行带时间框架的SQL查询
        
        Args:
            sql_template: SQL模板，包含 {timeframe} 占位符
            timeframe: 时间框架，如 "1h", "2h", "1d"
            params: 查询参数列表
            
        Returns:
            查询结果列表，每行为字典格式
        """
        try:
            # 替换时间框架占位符
            sql = sql_template.format(timeframe=timeframe)
            
            # 执行查询
            return await self.execute_query(sql, params)
            
        except Exception as e:
            logger.error(f"带时间框架的SQL查询执行失败: {e}")
            return []

    """-----------------辅助函数---------------"""

    # 内部方法：ms单位时间戳转为中国时间，用于插入数据库前的数据清理
    @staticmethod
    def _ms_to_china_datetime(ms: int) -> datetime:
        """
        Convert epoch milliseconds to Asia/Shanghai timezone-aware datetime.
        """
        # China Standard Time is UTC+8, no DST
        utc_dt = datetime.fromtimestamp(ms / 1000, tz=timezone.utc)
        china_dt = utc_dt.astimezone(timezone(timedelta(hours=8))).replace(tzinfo=None)
        return china_dt
