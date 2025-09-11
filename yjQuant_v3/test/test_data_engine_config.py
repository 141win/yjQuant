"""
测试当数据引擎配置变更时，配置管理器能否发现、正确发布对应事件，数据引擎能否正确处理该事件和数据
"""
"""
测试结果符合预期
"""
import logging
import sys

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('../../logs/yjquant.log', encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger(__name__)
import os
# 获取当前main.py文件所在目录
current_dir = os.path.dirname(os.path.abspath(__file__))
# 配置文件目录在yjQuant_v3/config
config_dir = os.path.join(current_dir, 'config')

if not os.path.exists(config_dir):
    logger.error(f"配置目录不存在: {config_dir}")
    raise FileNotFoundError(f"配置目录不存在: {config_dir}")

from yjQuant_v3.core.config_manager import ConfigManager
from yjQuant_v3.core.event_engine import EventEngine
from yjQuant_v3.core.clock_engine import ClockEngine
from yjQuant_v3.core.data_engine import DataEngine

import asyncio
_shutdown_event = asyncio.Event()

async def main():

    # 初始化、启动事件引擎
    event_engine = EventEngine()
    await event_engine.start()

    # 初始化、启动时钟引擎
    clock_engine = ClockEngine(event_engine)
    await clock_engine.start(event_engine)
    logger.info(f"使用配置目录: {config_dir}")

    # 初始化、启动配置管理器
    config_manager = ConfigManager(config_dir)
    await config_manager.start(event_engine, clock_engine)

    logger.info("正在启动数据引擎...")
    data_engine = DataEngine(config_manager)
    await data_engine.start(event_engine, clock_engine)
    logger.info("✓ 数据引擎启动成功")

    # 等待关闭信号
    await _shutdown_event.wait()

if __name__ == '__main__':
    import asyncio
    asyncio.run(main())