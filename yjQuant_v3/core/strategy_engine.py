"""策略引擎 - 管理所有策略类

设计备注（2025-09）：
- 目标：策略引擎每分钟向交易所请求所有交易对的实时数据（非K线），并执行所有策略。
- 当前状态：仍保留事件驱动的K线到达处理（_handle_data_arrived）以兼容旧流程，但标注为LEGACY。
- 新增：分钟级轮询框架与占位方法，后续接入真实实时行情获取实现。
"""

import asyncio
import logging
import pandas as pd
from typing import Dict, Any

logger = logging.getLogger(__name__)


class StrategyEngine:
    """策略引擎 - 管理所有策略类"""

    def __init__(self, config_manager):
        self.config_manager = config_manager  # 配置管理器
        self._event_engine = None  # 事件引擎
        self._clock_engine = None  # 时钟引擎
        self.strategies: Dict[str, Any] = {}  # 存储策略实例
        # self._strategy_check_task_id = None #
        self._strategy_config = self.config_manager.get_config("strategy")["strategies"]  # 从配置引擎获取策略配置
        # 数据到达事件
        self._event_type = "data_arrived"

        # # 启动时验证策略配置
        # self._validate_strategy_config()

        logger.info("策略引擎初始化完成")

    async def start(self, event_engine, clock_engine) -> None:
        """启动策略引擎"""
        self._event_engine = event_engine
        self._clock_engine = clock_engine

        # 实例化策略
        self._instantiate_strategies()

        # 初始化策略
        self._initialize_strategies()

        # 订阅策略配置变更事件
        self._event_engine.subscribe("strategy_config_changed", self._strategy_changes)

        # 订阅分钟级实时数据到达事件
        self._event_engine.subscribe(self._event_type, self._on_minute_tick)

        logger.info("策略引擎启动成功")

    # 停止策略引擎
    async def stop(self) -> None:
        """停止策略引擎"""

        # 取消事件订阅
        if self._event_engine:
            self._event_engine.unsubscribe_by_handler("strategy_config_changed", self._strategy_changes)
            self._event_engine.unsubscribe_by_handler(self._event_type, self._on_minute_tick)

        # 清除所有策略实例

        logger.info("策略引擎已停止")

    # 实例化策略
    def _instantiate_strategies(self) -> None:
        """
        实例化所有策略
        依据配置文件，导入所有策略并实例化
        """
        try:
            # strategies = {}
            count_strategy = 0
            for strategy_name, strategy_config in self._strategy_config.items():
                try:
                    # 获取策略类
                    strategy_class = self._get_strategy_class(strategy_config.get("class"))
                    if not strategy_class:
                        logger.error(f"策略 {strategy_name} 类不存在: {strategy_config.get('class')}")
                        continue

                    # 创建策略上下文
                    context = self._create_strategy_context(strategy_config)

                    # 实例化策略
                    strategy = strategy_class(
                        name=strategy_name,
                        description=strategy_config.get("description", ""),
                        timeframe=strategy_config.get("timeframe", "1m"),
                        context=context
                    )

                    self.strategies[strategy_name] = strategy
                    logger.info(f"策略 {strategy_name} 实例化成功")
                    count_strategy += 1
                except Exception as e:
                    logger.error(f"策略 {strategy_name} 实例化失败: {e}")
                    continue

            logger.info(f"策略实例化完成，共 {count_strategy} 个策略")
            # return strategies

        except Exception as e:
            logger.error(f"策略实例化失败: {e}")
            # return {}

    # 初始化策略
    def _initialize_strategies(self) -> None:
        """初始化所有策略"""
        try:
            for strategy_name, strategy in self.strategies.items():
                try:
                    if hasattr(strategy, 'initialize'):
                        strategy.initialize()
                        logger.info(f"策略 {strategy_name} 初始化成功")
                    else:
                        logger.debug(f"策略 {strategy_name} 无需初始化")

                except Exception as e:
                    logger.error(f"策略 {strategy_name} 初始化失败: {e}")
                    # 初始化失败不影响其他策略
                    continue

            logger.info("策略初始化完成")

        except Exception as e:
            logger.error(f"策略初始化失败: {e}")

    # 获取策略类
    @staticmethod
    def _get_strategy_class(class_name: str):
        """动态导入策略类"""
        try:
            import importlib.util
            import os

            from pathlib import Path
            strategies_dir = Path("./strategies")

            if not os.path.exists(strategies_dir):
                logger.error(f"策略目录不存在: {strategies_dir}")
                return None

            # 构建策略文件路径
            strategy_file = f"{class_name}.py"
            strategy_path = os.path.join(strategies_dir, strategy_file)

            if not os.path.exists(strategy_path):
                logger.error(f"策略文件不存在: {strategy_path}")
                return None

            try:
                # 使用importlib动态加载模块
                spec = importlib.util.spec_from_file_location(class_name, strategy_path)
                if spec is None or spec.loader is None:
                    logger.error(f"无法创建模块规范: {class_name}")
                    return None

                # 创建模块对象
                module = importlib.util.module_from_spec(spec)

                # 执行模块
                spec.loader.exec_module(module)

                # 从模块中获取策略类
                if hasattr(module, class_name):
                    strategy_class = getattr(module, class_name)
                    logger.info(f"成功动态导入策略类: {class_name}")
                    return strategy_class
                else:
                    logger.error(f"模块 {class_name} 中未找到类 {class_name}")
                    return None

            except Exception as e:
                logger.error(f"动态导入策略类 {class_name} 失败: {e}")
                return None

        except Exception as e:
            logger.error(f"获取策略类时发生未知错误: {e}")
            return None

    # 创建策略上下文
    def _create_strategy_context(self, strategy_config: Dict[str, Any]):
        """创建策略上下文"""
        try:
            # 导入策略上下文类
            from yjQuant_v3.core.strategy_template import StrategyContext

            # 获取数据库读取器和缓存管理器
            db_reader = self._get_db_reader()
            cache = self._get_cache_manager()

            # 创建上下文
            context = StrategyContext(
                global_config=self.config_manager,
                event_engine=self._event_engine,
                db_reader=db_reader,
                cache=cache,
                logger=None,  # 使用默认日志器
                strategy_item=strategy_config
            )

            return context

        except Exception as e:
            logger.error(f"创建策略上下文失败: {e}")

            # 返回一个简单的模拟上下文
            class MockContext:
                def __init__(self, config_manager, event_engine, strategy_config):
                    self.global_config = config_manager
                    self.event_engine = event_engine
                    self.db_reader = None
                    self.cache = None
                    self.logger = logger
                    self.strategy_item = strategy_config
                    self.current_unit_epoch = None
                    self.prefilter_data = {}

            return MockContext(self.config_manager, self._event_engine, strategy_config)

    # 后去数据库读取器
    def _get_db_reader(self):
        """获取数据库读取器"""
        try:
            # 从配置管理器获取数据库配置
            db_config = self.config_manager.get_config("data_engine").get("postgresql", {})

            # 这里需要实际的PgManager实例
            # 在实际使用中，应该通过依赖注入的方式获取
            # 暂时返回None，实际实现时需要注入PgManager
            logger.debug("数据库读取器暂时未实现，需要注入PgManager")
            return None
        except Exception as e:
            logger.error(f"获取数据库读取器失败: {e}")
            return None

    # 获取缓存管理器
    def _get_cache_manager(self):
        """获取缓存管理器"""
        try:
            # 从配置管理器获取Redis配置
            redis_config = self.config_manager.get_config("data_engine").get("redis", {})

            # 这里需要实际的RedisCacheManager实例
            # 在实际使用中，应该通过依赖注入的方式获取
            # 暂时返回None，实际实现时需要注入RedisCacheManager
            logger.debug("缓存管理器暂时未实现，需要注入RedisCacheManager")
            return None
        except Exception as e:
            logger.error(f"获取缓存管理器失败: {e}")
            return None

    """-----------------------------事件处理函数---------------------------------------"""
    # <--------------------- 配置变更事件处理 ------------------->
    # 策略变更函数
    # 1、检查strategy_config是否变化，如果变化，则重新加载策略
    # 2、检查是否有新增策略，如果有，则实例化策略
    async def _strategy_changes(self, event_data: Any = None) -> None:
        """
        检查策略变更事件
        
        流程:
        1. 检查策略配置是否变化
        2. 如果有变化，重新加载策略
        3. 检查是否有新增策略，如果有则实例化
        4. 检查是否有删除的策略，如果有则清理
        """
        try:
            logger.info("开始检查策略变更...")

            # 获取最新配置
            new_strategy_config = self.config_manager.get_config("strategy").get("strategies", {})

            # 检查配置是否有变化
            if new_strategy_config == self._strategy_config:
                logger.debug("策略配置无变化")
                return

            logger.info("检测到策略配置变化，开始重新加载...")

            # 备份旧配置和策略
            old_strategies = self.strategies.copy()
            old_config = self._strategy_config.copy()

            # 更新配置
            self._strategy_config = new_strategy_config

            # 重新实例化策略（包含动态导入）
            self._instantiate_strategies()

            # 初始化新策略
            self._initialize_strategies()

            # 清理已删除的策略
            removed_strategies = set(old_strategies.keys()) - set(self.strategies.keys())
            for strategy_name in removed_strategies:
                old_strategy = old_strategies[strategy_name]
                if hasattr(old_strategy, 'cleanup'):
                    try:
                        old_strategy.cleanup()
                        logger.info(f"策略 {strategy_name} 清理完成")
                    except Exception as e:
                        logger.error(f"策略 {strategy_name} 清理失败: {e}")

            # 记录变化统计
            added_strategies = set(self.strategies.keys()) - set(old_strategies.keys())
            updated_strategies = set(self.strategies.keys()) & set(old_strategies.keys())

            logger.info(f"策略重新加载完成:")
            logger.info(f"  - 新增策略: {list(added_strategies)}")
            logger.info(f"  - 更新策略: {list(updated_strategies)}")
            logger.info(f"  - 删除策略: {list(removed_strategies)}")
            logger.info(f"  - 总策略数: {len(self.strategies)}")

        except Exception as e:
            logger.error(f"检查策略变更失败: {e}")
            import traceback
            traceback.print_exc()

    # <--------------------- 数据到达事件处理 ------------------->
    async def _on_minute_tick(self, event_data: Any = None) -> None:
        """分钟级时钟事件回调：获取实时数据并执行所有策略"""
        try:
            # 实时数据通过event_data获取
            # [("交易所"，“交易对”，“最新价”，“ms时间戳”)...]
            realtime_data = event_data["data"]
            if not realtime_data:
                return
            # 转换实时数据格式——>dataframe
            df = self._convert_realtime_to_dataframe(realtime_data)
            if df.empty:
                return
            # 执行策略，收集结果
            strategy_results = await self._run_all_strategies(df)
            if strategy_results:
                # 处理结果
                await self._process_strategy_results(strategy_results)
        except Exception as e:
            logger.error(f"分钟级事件处理失败: {e}")

    # def _collect_monitored_pairs(self) -> list:
    #     """收集所有策略监控的交易所-交易对列表 [(exchange, symbol), ...]"""
    #     pairs: list = []
    #     try:
    #         for strategy in self.strategies.values():
    #             if hasattr(strategy, "get_monitored_pairs"):
    #                 for sym, ex in strategy.get_monitored_pairs():
    #                     pairs.append((ex, sym))
    #     except Exception as e:
    #         logger.error(f"收集监控交易对失败: {e}")
    #     return list({(ex, sym) for ex, sym in pairs})  # 去重

    # 将实时数据列表转换为Dataframe格式

    """-----------------------------------辅助函数-----------------------------------"""
    # 将实时数据转为Dataframe格式
    def _convert_realtime_to_dataframe(self, snapshots: list) -> pd.DataFrame:
        """将实时快照转换为策略输入DataFrame（列包含: exchange, symbol, price, timestamp）"""
        """[("交易所"，“交易对”，(“最新价”，“ms时间戳”))...]"""
        try:
            if not snapshots:
                return pd.DataFrame()
            records = []
            for exchange, symbol, snap in snapshots:
                # 期待snap至少含: [last_price, ts_ms]
                if isinstance(snap, (list, tuple)) and len(snap) >= 2:
                    records.append({
                        "timestamp": snap[1],
                        "exchange": exchange,
                        "symbol": symbol,
                        "price": float(snap[0])
                    })
            return pd.DataFrame(records)
        except Exception as e:
            logger.error(f"转换实时快照失败: {e}")
            return pd.DataFrame()


    # def _convert_klines_to_dataframe(self, klines_data: list) -> pd.DataFrame:
    #     """将K线数据转换为DataFrame格式"""
    #     try:
    #         if not klines_data:
    #             return pd.DataFrame()
    #
    #         # 解析K线数据格式: [(exchange, symbol, kline), ...]
    #         # kline格式: [timestamp, open, high, low, close, volume]
    #         records = []
    #         for exchange, symbol, kline in klines_data:
    #             if len(kline) >= 6:
    #                 records.append({
    #                     "timestamp": kline[0],
    #                     "exchange": exchange,
    #                     "symbol": symbol,
    #                     "open": kline[1],
    #                     "high": kline[2],
    #                     "low": kline[3],
    #                     "close": kline[4],
    #                     "volume": kline[5],
    #                     "price": kline[4]  # 使用close价格作为当前价格
    #                 })
    #
    #         df = pd.DataFrame(records)
    #
    #         # 确保时间戳列是数值类型
    #         if not df.empty and "timestamp" in df.columns:
    #             df["timestamp"] = pd.to_numeric(df["timestamp"], errors='coerce')
    #             df = df.dropna(subset=["timestamp"])
    #
    #         return df
    #
    #     except Exception as e:
    #         logger.error(f"转换K线数据失败: {e}")
    #         return pd.DataFrame()

    """-----------------------------------执行策略-----------------------------------"""
    # 执行所有策略
    async def _run_all_strategies(self, df: pd.DataFrame) -> list:
        """运行所有启用的策略"""
        try:
            if df.empty:
                logger.warning("输入数据为空，跳过策略执行")
                return []

            # enabled_strategies = [s for s in self.strategies.values()]

            logger.info(f"开始执行 {len(self.strategies)} 个启用策略...")

            # 并发执行所有策略
            tasks = []
            for strategy in self.strategies.values():
                task = self._execute_single_strategy(strategy, df)
                tasks.append(task)

            # 等待所有策略执行完成
            results = await asyncio.gather(*tasks, return_exceptions=True)

            strategy_results = []
            # 处理结果
            for i, result in enumerate(results):
                if isinstance(result, Exception):
                    logger.error(f"策略 {result.strategy_name} 执行失败: {result}")
                elif result is not None:
                    # 策略返回了有效结果
                    strategy_results.append(result)
                    logger.info(f"策略 {result.strategy_name} 生成信号: {len(result.result_df)} 个")
                else:
                    # 策略无信号
                    # strategy._update_run_stats(success=True)
                    logger.debug(f"策略 {result.strategy_name} 无信号")

            logger.info(f"策略执行完成，共生成 {len(strategy_results)} 个策略结果")
            return strategy_results

        except Exception as e:
            logger.error(f"运行策略失败: {e}")
            return []

    # 执行单个策略
    async def _execute_single_strategy(self, strategy, df: pd.DataFrame):
        """执行单个策略"""
        try:
            # 调用策略的on_realtime方法
            result = strategy.on_realtime(df)
            return result

        except Exception as e:
            logger.error(f"执行策略 {strategy.name} 失败: {e}")
            raise

    # 处理策略执行结果
    async def _process_strategy_results(self, strategy_results: list) -> None:
        """处理策略执行结果"""
        try:
            if not strategy_results:
                return

            logger.info(f"开始处理 {len(strategy_results)} 个策略结果...")

            # 一个策略执行结果发送一次邮件
            for result in strategy_results: # 处理每一个策略的结果
                if hasattr(result, 'result_df') and not result.result_df.empty: # 结果df不为空
                    # 转换为标准格式
                    # 将dataframe格式的数据，每行转为一个字典，所有字典组成列表
                    signals = result.result_df.to_dict("records") # 'records'
                    for signal in signals:
                        signal['strategy_name'] = result.strategy_name
                        signal['timeframe'] = result.timeframe

                    # 发送邮件事件
                    await self._publish_email_notification(signals)
                else:
                    logger.info("没有有效的交易信号")

        except Exception as e:
            logger.error(f"处理策略结果失败: {e}")
            import traceback
            traceback.print_exc()

    """---------------------------------发布邮件事件------------------------------------"""
    # 发布邮件事件
    async def _publish_email_notification(self, signals: list) -> None:
        """发布邮件通知事件"""
        try:
            if not self._event_engine:
                logger.error("事件引擎未初始化，无法发布事件")
                return

            # 构建邮件内容
            subject = f"交易信号提醒 - {len(signals)} 个信号"
            content = self._build_email_content(signals)

            event_data = {
                "receiver": "13086397065@163.com",
                "subject": subject,
                "content": content,
                "content_type": "text"
            }

            # 直接发布 email_event 事件
            await self._event_engine.publish("email_event", event_data)
            logger.info(f"邮件事件已发布: {len(signals)} 个信号")

        except Exception as e:
            logger.error(f"发布邮件事件失败: {e}")

    # 构建邮件文本
    @staticmethod
    def _build_email_content(signals: list) -> str:
        """构建邮件内容"""
        try:
            if not signals:
                return "没有交易信号"

            content_lines = [
                f"策略检测到 {len(signals)} 个交易信号：",
                "",
                "信号详情："
            ]

            for i, signal in enumerate(signals, 1):
                exchange = signal.get("exchange", "Unknown")
                symbol = signal.get("symbol", "Unknown")
                price = signal.get("price", "Unknown")
                strategy_name = signal.get("strategy_name", "Unknown")

                content_lines.append(
                    f"{i}. {exchange}:{symbol} - 价格: {price} "
                    f"(策略: {strategy_name})"
                )

            return "\n".join(content_lines)

        except Exception as e:
            logger.error(f"构建邮件内容失败: {e}")
            return f"策略信号邮件内容构建失败: {e}"
