"""
数据引擎 - 协调数据请求、存储、查询功能

职责:
1. 向时钟引擎注册定时"请求数据"任务
2. 向事件引擎订阅"数据请求"事件、"数据引擎配置变更"事件
3. 发布"数据到达"事件
"""

import logging
from typing import Any, List,Tuple
from datetime import datetime, timedelta, timezone


logger = logging.getLogger(__name__)


class DataEngine:
    """数据引擎 - 协调数据请求、存储、查询功能"""
    # 时钟引擎 → 数据请求事件 → 数据源管理器 → Redis存储 → 数据到达事件 → 策略引擎
    def __init__(self, config_manager):
        """
        初始化数据引擎
        
        Args:
            config_manager: 配置管理器实例
        """
        self.config_manager = config_manager
        
        # 组件引用
        self._event_engine = None
        self._clock_engine = None
        
        # 子管理器
        self._redis_manager = None
        self._data_source_manager = None
        self._db_manager = None
        
        # 任务ID
        self._data_request_task_id = None
        
        # 数据请求配置
        self.data_engine_config = {}

        # 数据请求任务配置
        self.data_request_config = {}

        logger.info("数据引擎初始化完成")

    # 对外开放
    async def start(self, event_engine, clock_engine) -> None:
        """启动数据引擎"""
        if self._event_engine or self._clock_engine:
            logger.warning("数据引擎已经在运行")
            return
        
        self._event_engine = event_engine
        self._clock_engine = clock_engine

        # 获取数据引擎配置
        self.data_engine_config = self.config_manager.get_config("data_engine")
        self.data_request_config = self.data_engine_config.get("data_request")

        # 初始化子管理器
        await self._initialize_managers()
        
        # 订阅事件
        self._event_engine.subscribe("data_request", self._handle_data_request)
        self._event_engine.subscribe("data_engine_config_changed", self._handle_config_change)
        
        # 从配置获取数据请求频率
        interval = self.data_request_config.get("interval", 3600)  # 默认1h
        
        # 向时钟引擎注册数据请求任务
        self._data_request_task_id = self._clock_engine.register_task(
            name="data_request",
            event_type="data_request",
            interval=interval,
            event_data={"source": "data_engine"}
        )
        
        logger.info("数据引擎启动成功")

    # 对外开放
    async def stop(self) -> None:
        """停止数据引擎"""
        if not self._event_engine and not self._clock_engine:
            logger.warning("数据引擎已经停止")
            return
        
        # 取消数据请求任务
        if self._data_request_task_id and self._clock_engine:
            self._clock_engine.unregister_task(self._data_request_task_id)
        
        # 取消事件订阅
        if self._event_engine:
            self._event_engine.unsubscribe_by_handler("data_request", self._handle_data_request)
            self._event_engine.unsubscribe_by_handler("data_engine_config_changed", self._handle_config_change)
        
        # 停止子管理器
        await self._stop_managers()
        
        # 清理组件引用
        self._event_engine = None
        self._clock_engine = None
        
        logger.info("数据引擎已停止")

    # 内部方法：初始化，start()调用
    async def _initialize_managers(self) -> None:
        """初始化子管理器"""
        try:
            # # 初始化Redis管理器
            # from yjQuant_v3.core.data_manager.redis_manager import RedisManager
            # self._redis_manager = RedisManager(self.data_engine_config.get("redis", {}))
            # await self._redis_manager.start()

            # 初始化数据源管理器
            from yjQuant_v3.core.data_manager.data_source_manager import DataSourceManager
            self._data_source_manager = DataSourceManager(self.data_engine_config.get("exchanges", []))

            
            # 初始化数据库管理器
            from yjQuant_v3.core.data_manager.pg_manager import PgManager
            self._db_manager = PgManager(self.data_engine_config.get("postgresql", {}))
            await self._db_manager.start()

            logger.info("子管理器初始化完成")
            
        except Exception as e:
            logger.error(f"初始化子管理器失败: {e}")
            raise

    # 内部方法：暂停，stop()调用
    async def _stop_managers(self) -> None:
        """停止子管理器"""
        try:
            # if self._redis_manager:
            #     await self._redis_manager.stop()

            # data_source_manager没有stop
            # if self._data_source_manager:
            #     await self._data_source_manager.stop()
            
            if self._db_manager:
                await self._db_manager.stop()
                
            logger.info("子管理器已停止")
            
        except Exception as e:
            logger.error(f"停止子管理器失败: {e}")

    # < ------------------  处理事件函数 ----------->
    # 处理定时每分钟请求数据的事件
    # 1、直接调用封装好的数据源管理器的fetch()方法，该方法自动根据数据源管理器内配置好的交易所：数据对进行异步请求，并返回所有数据
    # fetch()返回的数据格式[(exchange_id, symbol, kline或None), ...]  ,对应（交易所，交易对，kline数据）
    #             'timestamp': kline[0],
    #             'open': kline[1],
    #             'high': kline[2],
    #             'low': kline[3],
    #             'close': kline[4],
    #             'volume': kline[5]
    # 2、调用redis管理器方法batch_store_klines()方法，该方法自动将传入数据[(exchange_id, symbol, kline或None), ...]批量插入redis
    # 3、发布数据到达事件——该事件订阅者：策略引擎，会执行所有策略
    # 对外开放：请求数据事件处理器（注册用）
    async def _handle_data_request(self, event_data: Any = None) -> None:
        """
        处理定时数据请求事件
        
        流程:
        1. 调用数据源管理器的fetch()方法获取K线数据
        2. 将数据存储到Redis
        3. 发布数据到达事件
        """
        try:
            logger.info("开始处理数据请求事件...")
            
            # 1. 从数据源管理器获取K线数据
            if not self._data_source_manager:
                logger.error("数据源管理器未初始化")
                return
            
            klines_data = await self._data_source_manager.fetch(timeframe=self.data_request_config["timeframe"])
            
            if not klines_data:
                logger.warning("未获取到K线数据")
                return
            
            logger.info(f"成功获取 {len(klines_data)} 条K线数据")
            
            # # 2. 将数据存储到Redis
            # if not self._redis_manager:
            #     logger.error("Redis管理器未初始化")
            #     return
            #
            # success = await self._redis_manager.batch_store_klines(
            #     klines_data,
            #     # expire_minutes=expire_minutes redis内部有配置，只提供数据，redis读取配置计算过期时间
            # )
            
            # if success:
            #     logger.info("K线数据成功存储到Redis")
            #
            #     # 3. 发布数据到达事件
            #     await self._publish_data_arrived_event(klines_data)
            #
            # else:
            #     logger.error("K线数据存储到Redis失败")

            # 3. 将数据存储到pg数据库
            if not self._db_manager:
                logger.error("pg数据库管理器未初始化")
                return

            success = await self._db_manager.batch_store_klines(
                klines_data,
            )

            if success:
                logger.info("K线数据成功存储到pg数据库")

            else:
                logger.error("K线数据存储到pg数据库失败")

        except Exception as e:
            logger.error(f"处理数据请求事件失败: {e}")
            import traceback
            traceback.print_exc()

    # 对外开放：处理配置变更事件
    # 1、从事件数据中读取最新配置，并调用所有子组件的配置修改方法，将新配置传入，不关心子组件内部更新配置细节
    async def _handle_config_change(self, event_data: Any = None) -> None:
        """
        处理数据引擎配置变更事件
        
        流程:
        1. 从事件数据中读取最新配置
        2. 调用所有子组件的配置更新方法
        """
        try:
            logger.info("开始处理数据引擎配置变更事件...")
            
            if not event_data:
                logger.warning("配置变更事件数据为空")
                return
            
            # 解析新配置
            new_config = event_data.get("new_config", {})
            if not new_config:
                logger.warning("新配置为空")
                return
            
            logger.info(f"接收到新配置: {list(new_config.keys())}")
            logger.info(f"配置：: {new_config}")
            
            # 更新数据请求配置
            if "data_request" in new_config:
                self.data_engine_config.update(new_config["data_request"])
                logger.info("数据请求配置已更新")


            # 更新Redis配置
            if "redis" in new_config and self._redis_manager:
                await self._redis_manager.update_config(new_config["redis"])
                logger.info("Redis配置已更新")

            # 更新数据源配置
            if "exchanges" in new_config and self._data_source_manager:
                await self._data_source_manager.update_config(new_config["exchanges"])
                logger.info("数据源配置已更新")
            
            # 更新数据库配置（如果启用）
            if "postgresql" in new_config and self._db_manager:
                await self._db_manager.update_config(new_config["postgresql"])
                logger.info("数据库配置已更新")
            
            # # 如果数据请求频率有变化，重新注册任务
            # if "data_request" in new_config:
            #     data_request_config = new_config["data_request"]
            #     new_interval = data_request_config.get("interval")
            #
            #     if new_interval and new_interval != self._data_request_config.get("interval"):
            #         # 取消旧任务
            #         if self._data_request_task_id and self._clock_engine:
            #             self._clock_engine.unregister_task(self._data_request_task_id)
            #
            #         # 注册新任务
            #         self._data_request_task_id = self._clock_engine.register_task(
            #             name="data_request",
            #             event_type="data_request",
            #             interval=new_interval,
            #             event_data={"source": "data_engine"}
            #         )
            #
            #         logger.info(f"数据请求任务已更新，新间隔: {new_interval}秒")
            
            logger.info("数据引擎配置变更处理完成")
            
        except Exception as e:
            logger.error(f"处理配置变更事件失败: {e}")
            import traceback
            traceback.print_exc()

    # 内部方法：发布数据到达事件
    # <---------------------发布事件函数 ----------->
    # 发布数据到达事件
    # 1、调用事件引擎发布事件方法，发布数据到达事件（只是通知下游数据已经到达并存储完毕，可以获取，下游可以从事件数据中获取，也可以自行从redis、数据库获取）
    # event_engine 发布事件接口： async def publish(self, event_type: str, event_data: Any = None) -> None:
    async def _publish_data_arrived_event(self, klines_data: List[Tuple[str, str, List]]) -> None:
        """
        发布数据到达事件
        
        该事件通知下游（如策略引擎）数据已经到达并存储完毕，
        下游可以自行从Redis或数据库获取数据
        
        Args:
            klines_data: K线数据列表，格式为 [(exchange, symbol, kline), ...]
        """
        try:
            if not self._event_engine:
                logger.error("事件引擎未初始化，无法发布事件")
                return
            
            # 构建事件数据，包含数据摘要信息（优化：减少数据量）
            event_data = {
                "timestamp": datetime.now().isoformat(),
                "data_count": len(klines_data),
                "exchanges": list(set(exchange for exchange, _, _ in klines_data)),
                "symbols": list(set(symbol for _, symbol, _ in klines_data))[:10],  # 只显示前10个交易对
                "klines_data": klines_data[:5],  # 只发送前5条K线数据
                "message": "K线数据已到达并存储完毕，可从Redis获取"
            }
            
            # 发布数据到达事件
            await self._event_engine.publish("data_arrived", event_data)
            
            logger.info(f"数据到达事件已发布，数据量: {len(klines_data)}")
            
        except Exception as e:
            logger.error(f"发布数据到达事件失败: {e}")
            import traceback
            traceback.print_exc()

    """------------------------辅助函数-------------------------------"""
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