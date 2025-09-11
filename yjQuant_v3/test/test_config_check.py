"""
测试配置检查事件是否正常工作
"""

import asyncio
import logging
from core.event_engine import EventEngine
from core.clock_engine import ClockEngine
from core.config_manager import ConfigManager
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


async def config_check_handler(event_data):
    """配置检查事件处理器"""
    logger.info(f"收到配置检查事件: {event_data}")
    
    # 这里可以添加实际的配置检查逻辑
    # 比如检查文件是否被修改等


async def main():
    """测试主函数"""
    logger.info("开始测试配置检查事件")
    
    # 创建组件
    event_engine = EventEngine()
    clock_engine = ClockEngine(event_engine)
    config_manager = ConfigManager("../config")
    
    try:
        # 启动组件
        await event_engine.start()
        logger.info("事件引擎启动成功")
        
        await clock_engine.start(event_engine)
        logger.info("时钟引擎启动成功")
        
        await config_manager.start(event_engine, clock_engine)
        logger.info("配置管理器启动成功")
        
        # 注册事件处理器
        # event_engine.subscribe("config_check", config_check_handler)
        logger.info("事件处理器注册完成")
        
        # 运行一段时间观察配置检查事件
        logger.info("系统运行中，观察配置检查事件...")
        logger.info("配置检查任务每10秒执行一次")
        
        # 运行1分钟观察效果
        await asyncio.sleep(180)
        
    except KeyboardInterrupt:
        logger.info("收到中断信号，开始关闭系统")
    except Exception as e:
        logger.error(f"系统运行出错: {e}")
    finally:
        # 关闭组件
        logger.info("开始关闭系统组件")
        
        await config_manager.stop()
        logger.info("配置管理器已关闭")
        
        await clock_engine.stop()
        logger.info("时钟引擎已关闭")
        
        await event_engine.stop()
        logger.info("事件引擎已关闭")
        
        logger.info("系统已完全关闭")


if __name__ == "__main__":
    asyncio.run(main())
