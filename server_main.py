"""
服务端主程序 - 数据请求和存储服务

模拟真实场景：
- 独立进程运行服务端
- 包含数据引擎、配置管理器、事件引擎等
- 通过网络模块向客户端广播数据
"""

import logging
import sys
import os
import asyncio
from datetime import datetime

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/server.log', encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger(__name__)

# 获取当前目录
current_dir = os.path.dirname(os.path.abspath(__file__))
config_dir = os.path.join(current_dir, 'config')

if not os.path.exists(config_dir):
    logger.error(f"配置目录不存在: {config_dir}")
    raise FileNotFoundError(f"配置目录不存在: {config_dir}")

async def main():
    """服务端主程序"""
    try:
        # 导入核心模块
        from yjQuant_v3.core.event_engine import EventEngine
        from yjQuant_v3.core.clock_engine import ClockEngine
        from yjQuant_v3.core.config_manager import ConfigManager
        from yjQuant_v3.core.data_engine import DataEngine
        from yjQuant_v3.core import NetworkServer

        logger.info("=" * 50)
        logger.info("启动服务端程序 - 数据请求和存储服务")
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
        config_manager = ConfigManager(config_dir)
        await config_manager.start(event_engine, clock_engine)
        logger.info("✓ 配置管理器启动完成")

        # 4. 初始化数据引擎
        data_engine = DataEngine(config_manager)
        await data_engine.start(event_engine, clock_engine)
        logger.info("✓ 数据引擎启动完成")

        # # 5. 初始化网络服务端
        # network_server = NetworkServer(
        #     host="0.0.0.0",
        #     port=8765,
        #     heartbeat_interval=30.0,
        #     client_timeout=60.0
        # )
        # await network_server.start(event_engine)
        # logger.info("✓ 网络服务端启动完成")

        # 6. 显示服务端状态
        # logger.info("=" * 50)
        # logger.info("服务端启动完成，等待客户端连接...")
        # logger.info(f"监听地址: 0.0.0.0:8765")
        # logger.info("=" * 50)

        # # 7. 定期显示统计信息
        # async def show_stats():
        #     while True:
        #         await asyncio.sleep(30)  # 每30秒显示一次
        #         stats = network_server.get_stats()
        #         logger.info(f"服务端统计: 连接数={stats['total_clients']}")
        #         if stats['clients']:
        #             for client_info in stats['clients']:
        #                 logger.info(f"  客户端 {client_info['peer']}: 消息数={client_info['message_count']}, 运行时间={client_info['uptime']:.1f}s")

        # 启动统计显示任务
        # stats_task = asyncio.create_task(show_stats())

        # 9. 等待中断信号
        try:
            while True:
                await asyncio.sleep(1)
        except KeyboardInterrupt:
            logger.info("收到中断信号，正在关闭服务端...")
        finally:
            # 清理资源
            # stats_task.cancel()
            # data_task.cancel()
            
            # await network_server.stop()
            await data_engine.stop()
            await config_manager.stop()
            await clock_engine.stop()
            await event_engine.stop()
            
            logger.info("✓ 服务端已完全关闭")

    except Exception as e:
        logger.error(f"服务端启动失败: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == '__main__':
    asyncio.run(main())
