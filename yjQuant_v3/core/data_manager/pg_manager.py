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
            table_name: 表名，默认为 "klines"

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

    # 辅助方法：创建表结构（如果需要）
    async def create_klines_table(self, table_name: str = "klines") -> bool:
        """
        创建K线数据表
        
        Args:
            table_name: 表名
            
        Returns:
            创建是否成功
        """
        try:
            if not self.connection_pool:
                logger.error("PostgreSQL连接池未初始化")
                return False

            create_table_sql = f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                timestamp TIMESTAMP WITHOUT TIME ZONE NOT NULL,
                open DECIMAL NOT NULL,
                high DECIMAL NOT NULL,
                low DECIMAL NOT NULL,
                close DECIMAL NOT NULL,
                volume DECIMAL NOT NULL,
                exchange VARCHAR(50) NOT NULL,
                pair VARCHAR(50) NOT NULL,
                PRIMARY KEY (timestamp, exchange, pair)
            );
            
            CREATE INDEX IF NOT EXISTS idx_{table_name}_timestamp ON {table_name}(timestamp);
            CREATE INDEX IF NOT EXISTS idx_{table_name}_exchange_pair ON {table_name}(exchange, pair);
            """

            async with self.connection_pool.acquire() as conn:
                await conn.execute(create_table_sql)

            logger.info(f"K线数据表 {table_name} 创建成功")
            return True

        except Exception as e:
            logger.error(f"创建K线数据表失败: {e}")
            return False

    # 辅助方法：查询K线数据
    async def query_klines(self, exchange: str, symbol: str,
                           start_time: int = None, end_time: int = None,
                           limit: int = 1000, table_name: str = "klines") -> List[Dict]:
        """
        查询K线数据
        
        Args:
            exchange: 交易所名称
            symbol: 交易对
            start_time: 开始时间戳
            end_time: 结束时间戳
            limit: 限制返回条数
            table_name: 表名
            
        Returns:
            K线数据列表
        """
        try:
            if not self.connection_pool:
                logger.error("PostgreSQL连接池未初始化")
                return []

            pair = symbol.replace("/", "_").replace(":", "_")

            sql = f"""
            SELECT timestamp, open, high, low, close, volume, exchange, pair
            FROM {table_name}
            WHERE exchange = $1 AND pair = $2
            """
            params = [exchange, pair]
            param_count = 2

            if start_time is not None:
                param_count += 1
                sql += f" AND timestamp >= ${param_count}"

                start_dt = self._ms_to_china_datetime(int(start_time))
                params.append(start_dt)

            if end_time is not None:
                param_count += 1
                sql += f" AND timestamp <= ${param_count}"
                end_dt = self._ms_to_china_datetime(int(end_time))
                params.append(end_dt)

            sql += f" ORDER BY timestamp DESC LIMIT ${param_count + 1}"
            params.append(limit)

            async with self.connection_pool.acquire() as conn:
                rows = await conn.fetch(sql, *params)

            return [dict(row) for row in rows]

        except Exception as e:
            logger.error(f"查询K线数据失败: {e}")
            return []

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

    """-----------------辅助函数---------------"""

    @staticmethod
    def _ms_to_china_datetime(ms: int) -> datetime:
        """
        Convert epoch milliseconds to Asia/Shanghai timezone-aware datetime.
        """
        # China Standard Time is UTC+8, no DST
        utc_dt = datetime.fromtimestamp(ms / 1000, tz=timezone.utc)
        china_dt = utc_dt.astimezone(timezone(timedelta(hours=8))).replace(tzinfo=None)
        return china_dt
