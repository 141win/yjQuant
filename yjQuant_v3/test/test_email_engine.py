"""
测试邮件引擎工作流程

演示:
1. 策略引擎发布邮件事件
2. 邮件引擎订阅邮件事件并处理
3. 邮件发送功能
"""

import asyncio
import logging
from core.event_engine import EventEngine
from core.clock_engine import ClockEngine
from core.config_manager import ConfigManager
from core.strategy_engine import StrategyEngine
from core.email_engine import EmailEngine

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)


async def email_event_handler(event_data):
    """邮件事件处理器（用于测试）"""
    logger.info(f"收到邮件事件: {event_data}")


async def main():
    """测试主函数"""
    logger.info("开始测试邮件引擎")
    
    # 创建组件
    event_engine = EventEngine()
    clock_engine = ClockEngine(event_engine)
    config_manager = ConfigManager("../config")
    strategy_engine = StrategyEngine(config_manager)
    email_engine = EmailEngine(config_manager)
    
    try:
        # 启动组件
        await event_engine.start()
        logger.info("事件引擎启动成功")
        
        await clock_engine.start(event_engine)
        logger.info("时钟引擎启动成功")
        
        await config_manager.start(event_engine, clock_engine)
        logger.info("配置管理器启动成功")
        
        await strategy_engine.start(event_engine, clock_engine)
        logger.info("策略引擎启动成功")
        
        await email_engine.start(event_engine)
        logger.info("邮件引擎启动成功")
        
        # 注册事件处理器（用于测试）
        event_engine.subscribe("email_event", email_event_handler)
        logger.info("事件处理器注册完成")
        
        # 模拟策略引擎发布邮件事件
        logger.info("模拟策略引擎发布邮件事件...")
        await event_engine.publish("email_event", {
            "receiver": "13086397065@163.com",  # 使用配置文件中的收件人
            "subject": "策略执行报告",
            "content": "策略执行完成，请查看详细报告。",
            "content_type": "text"
        })
        
        # 模拟HTML格式邮件
        await event_engine.publish("email_event", {
            "receiver": "13086397065@163.com",  # 使用配置文件中的收件人
            "subject": "系统状态报告",
            "content": "<h1>系统状态</h1><p>所有组件运行正常</p>",
            "content_type": "html"
        })
        
        # 运行一段时间观察事件流程
        logger.info("系统运行中，观察邮件事件流程...")
        await asyncio.sleep(10)
        
    except KeyboardInterrupt:
        logger.info("收到中断信号，开始关闭系统")
    except Exception as e:
        logger.error(f"系统运行出错: {e}")
    finally:
        # 关闭组件
        logger.info("开始关闭系统组件")
        
        await email_engine.stop()
        logger.info("邮件引擎已关闭")
        
        await strategy_engine.stop()
        logger.info("策略引擎已关闭")
        
        await config_manager.stop()
        logger.info("配置管理器已关闭")
        
        await clock_engine.stop()
        logger.info("时钟引擎已关闭")
        
        await event_engine.stop()
        logger.info("事件引擎已关闭")
        
        logger.info("系统已完全关闭")


if __name__ == "__main__":
    asyncio.run(main())
