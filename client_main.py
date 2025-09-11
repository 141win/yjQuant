"""
客户端主程序 - 策略计算服务

模拟真实场景：
- 独立进程运行客户端
- 连接到服务端接收数据
- 执行策略计算逻辑
"""

import logging
import sys
import os
import asyncio
import argparse
from datetime import datetime

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'logs/client_{os.getpid()}.log', encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

async def main():
    """客户端主程序"""
    # 解析命令行参数
    parser = argparse.ArgumentParser(description='客户端程序')
    parser.add_argument('--client-id', type=int, default=1, help='客户端ID')
    parser.add_argument('--server-host', type=str, default='127.0.0.1', help='服务端地址')
    parser.add_argument('--server-port', type=int, default=8765, help='服务端端口')
    args = parser.parse_args()

    client_id = args.client_id
    server_host = args.server_host
    server_port = args.server_port

    try:
        # 导入核心模块
        from yjQuant_v3.core.event_engine import EventEngine
        from yjQuant_v3.core.clock_engine import ClockEngine
        from yjQuant_v3.core.config_manager import ConfigManager
        from yjQuant_v3.core.strategy_engine import StrategyEngine
        from yjQuant_v3.core import NetworkClient

        logger.info("=" * 50)
        logger.info(f"启动客户端程序 {client_id} - 策略计算服务")
        logger.info(f"连接到服务端: {server_host}:{server_port}")
        logger.info("=" * 50)

        # 1. 初始化事件引擎
        event_engine = EventEngine()
        await event_engine.start()
        logger.info("✓ 事件引擎启动完成")

        # 2. 初始化时钟引擎
        clock_engine = ClockEngine(event_engine)
        await clock_engine.start(event_engine)
        logger.info("✓ 时钟引擎启动完成")

        # 3. 初始化配置管理器
        config_manager = ConfigManager("./config")
        await config_manager.start(event_engine, clock_engine)
        logger.info("✓ 配置管理器启动完成")

        # 4. 初始化策略引擎
        strategy_engine = StrategyEngine(config_manager)
        await strategy_engine.start(event_engine, clock_engine)
        logger.info("✓ 策略引擎启动完成")

        # # 5. 初始化网络客户端
        # network_client = NetworkClient(
        #     host=server_host,
        #     port=server_port,
        #     reconnect_interval=5.0,
        #     max_reconnect_attempts=10
        # )
        # await network_client.start(event_engine)
        # logger.info("✓ 网络客户端启动完成")
        #
        # # 6. 订阅数据到达事件，执行策略
        # data_received_count = 0
        #
        # async def on_data_arrived(event_data=None):
        #     nonlocal data_received_count
        #     data_received_count += 1
        #
        #     logger.info(f"客户端{client_id} 收到数据到达事件 #{data_received_count}")
        #     logger.info(f"  时间戳: {event_data.get('timestamp', 'N/A')}")
        #     logger.info(f"  数据量: {event_data.get('data_count', 0)}")
        #     logger.info(f"  交易所: {event_data.get('exchanges', [])}")
        #     logger.info(f"  交易对: {event_data.get('symbols', [])}")
        #
        #     # 这里可以添加具体的策略计算逻辑
        #     # 例如：分析K线数据、计算技术指标、生成交易信号等
        #     await simulate_strategy_calculation(event_data, client_id)
        #
        # event_engine.subscribe("data_arrived", on_data_arrived)
        #
        # # 7. 显示客户端状态
        # logger.info("=" * 50)
        # logger.info(f"客户端{client_id} 启动完成，等待服务端数据...")
        # logger.info("=" * 50)
        #
        # # 8. 定期显示统计信息
        # async def show_stats():
        #     while True:
        #         await asyncio.sleep(30)  # 每30秒显示一次
        #         stats = network_client.get_stats()
        #         logger.info(f"客户端{client_id} 统计: 连接状态={stats['connected']}, 重连次数={stats['reconnect_attempts']}, 收到数据={data_received_count}次")
        #
        # # 启动统计显示任务
        # stats_task = asyncio.create_task(show_stats())
        #
        # # 9. 等待中断信号
        try:
            while True:
                await asyncio.sleep(1)
        except KeyboardInterrupt:
            logger.info(f"收到中断信号，正在关闭客户端{client_id}...")
        finally:
            # 清理资源
            # stats_task.cancel()
            
            # await network_client.stop()
            await strategy_engine.stop()
            await config_manager.stop()
            await clock_engine.stop()
            await event_engine.stop()
            
            logger.info(f"✓ 客户端{client_id} 已完全关闭")

    except Exception as e:
        logger.error(f"客户端{client_id} 启动失败: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
if __name__ == '__main__':
    asyncio.run(main())
