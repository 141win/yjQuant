"""
yjQuant v3.0 主程序 - 启动整个量化交易系统

启动顺序：
1. 配置管理器 (ConfigManager)
2. 事件引擎 (EventEngine) 
3. 时钟引擎 (ClockEngine)
4. Redis管理器 (RedisManager)
5. 数据源管理器 (DataSourceManager)
6. 数据引擎 (DataEngine)
7. 策略引擎 (StrategyEngine)
8. 邮件引擎 (EmailEngine)

关闭顺序（反向）：
8. 邮件引擎
7. 策略引擎
6. 数据引擎
5. 数据源管理器
4. Redis管理器
3. 时钟引擎
2. 事件引擎
1. 配置管理器
"""

import asyncio
import logging
import signal
import sys
from typing import Dict, Any
from datetime import datetime

import yjQuant_v3
# 导入核心组件
from yjQuant_v3.core.config_manager import ConfigManager
from yjQuant_v3.core.event_engine import EventEngine
from yjQuant_v3.core.clock_engine import ClockEngine
from yjQuant_v3.core.data_engine import DataEngine
from yjQuant_v3.core.strategy_engine import StrategyEngine
from yjQuant_v3.core.email_engine import EmailEngine

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/yjquant.log', encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger(__name__)


class YJQuantSystem:
    """yjQuant 系统主控制器"""
    
    def __init__(self):
        self.config_manager = None
        self.event_engine = None
        self.clock_engine = None
        self.redis_manager = None
        self.data_source_manager = None
        self.data_engine = None
        self.strategy_engine = None
        self.email_engine = None
        
        self._running = False
        self._shutdown_event = asyncio.Event()
        
        # 注册信号处理器
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
    
    def _signal_handler(self, signum, frame):
        """信号处理器"""
        logger.info(f"收到信号 {signum}，开始优雅关闭...")
        self._shutdown_event.set()
    
    async def start(self) -> bool:
        """启动整个系统"""
        try:
            logger.info("=" * 60)
            logger.info("开始启动 yjQuant v3.0 量化交易系统")
            logger.info("=" * 60)
            
            # 1. 启动事件引擎（最基础，无依赖）
            await self._start_event_engine()
            
            # 2. 启动时钟引擎（依赖事件引擎）
            await self._start_clock_engine()
            
            # 3. 启动配置管理器（依赖事件引擎和时钟引擎）
            await self._start_config_manager()
            
            # 4. 启动数据引擎（依赖配置管理器、事件引擎、时钟引擎）
            await self._start_data_engine()
            
            # 5. 启动策略引擎（依赖配置管理器、事件引擎、时钟引擎）
            await self._start_strategy_engine()
            
            # 6. 启动邮件引擎（依赖配置管理器、事件引擎）
            await self._start_email_engine()
            
            # 8. 启动完成
            self._running = True
            logger.info("=" * 60)
            logger.info("yjQuant v3.0 系统启动完成！")
            logger.info("=" * 60)
            
            return True
            
        except Exception as e:
            logger.error(f"系统启动失败: {e}")
            await self.stop()
            return False
    
    async def _start_event_engine(self):
        """启动事件引擎（最基础组件）"""
        try:
            logger.info("正在启动事件引擎...")
            self.event_engine = EventEngine()
            await self.event_engine.start()
            logger.info("✓ 事件引擎启动成功")
        except Exception as e:
            logger.error(f"事件引擎启动失败: {e}")
            raise
    
    async def _start_clock_engine(self):
        """启动时钟引擎（依赖事件引擎）"""
        try:
            logger.info("正在启动时钟引擎...")
            self.clock_engine = ClockEngine(self.event_engine)
            await self.clock_engine.start(self.event_engine)
            logger.info("✓ 时钟引擎启动成功")
        except Exception as e:
            logger.error(f"时钟引擎启动失败: {e}")
            raise
    
    async def _start_config_manager(self):
        """启动配置管理器（依赖事件引擎和时钟引擎）"""
        try:
            logger.info("正在启动配置管理器...")
            self.config_manager = ConfigManager("./config")
            await self.config_manager.start(self.event_engine, self.clock_engine)
            logger.info("✓ 配置管理器启动成功")
        except Exception as e:
            logger.error(f"配置管理器启动失败: {e}")
            raise
    
    async def _start_data_engine(self):
        """启动数据引擎（依赖配置管理器、事件引擎、时钟引擎）"""
        try:
            logger.info("正在启动数据引擎...")
            self.data_engine = DataEngine(self.config_manager)
            await self.data_engine.start(self.event_engine, self.clock_engine)
            logger.info("✓ 数据引擎启动成功")
            
            # 获取数据引擎管理的组件引用（如果需要的话）
            self.redis_manager = getattr(self.data_engine, '_redis_manager', None)
            self.db_manager = getattr(self.data_engine, '_db_manager', None)

        except Exception as e:
            logger.error(f"数据引擎启动失败: {e}")
            raise
    
    async def _start_strategy_engine(self):
        """启动策略引擎（依赖配置管理器、事件引擎、时钟引擎）"""
        try:
            logger.info("正在启动策略引擎...")
            self.strategy_engine = StrategyEngine(self.config_manager)
            await self.strategy_engine.start(self.event_engine, self.clock_engine,self.db_manager,self.redis_manager)
            logger.info("✓ 策略引擎启动成功")
        except Exception as e:
            logger.error(f"策略引擎启动失败: {e}")
            raise
    
    async def _start_email_engine(self):
        """启动邮件引擎（依赖配置管理器、事件引擎）"""
        try:
            logger.info("正在启动邮件引擎...")
            self.email_engine = EmailEngine(self.config_manager)
            await self.email_engine.start(self.event_engine)
            logger.info("✓ 邮件引擎启动成功")
        except Exception as e:
            logger.error(f"邮件引擎启动失败: {e}")
            raise
    
    async def _register_system_tasks(self):
        """注册系统级定时任务"""
        try:
            # 策略配置检查任务
            self.clock_engine.register_task(
                name="strategy_config_check",
                event_type="strategy_check",
                interval=300,  # 5分钟检查一次
                event_data={"type": "strategy_config_check"}
            )
            
            # 系统状态报告任务
            self.clock_engine.register_task(
                name="system_status_report",
                event_type="system_status",
                interval=600,  # 10分钟报告一次
                event_data={"type": "status_report"}
            )
            
            # 数据源健康检查任务
            self.clock_engine.register_task(
                name="data_source_health_check",
                event_type="data_source_check",
                interval=180,  # 3分钟检查一次
                event_data={"type": "health_check"}
            )
            
            logger.info("✓ 系统定时任务注册完成")
            
        except Exception as e:
            logger.error(f"注册系统定时任务失败: {e}")
    
    async def run(self):
        """运行系统"""
        if not self._running:
            logger.error("系统未启动，无法运行")
            return
        
        try:
            logger.info("系统运行中...")
            
            # 等待关闭信号
            await self._shutdown_event.wait()
            
        except Exception as e:
            logger.error(f"系统运行出错: {e}")
        finally:
            await self.stop()
    
    async def stop(self):
        """停止整个系统"""
        if not self._running:
            return
        
        logger.info("=" * 60)
        logger.info("开始关闭 yjQuant v3.0 系统")
        logger.info("=" * 60)
        
        try:
            # 按相反顺序关闭各组件
            
            # 6. 关闭邮件引擎
            if self.email_engine:
                logger.info("正在关闭邮件引擎...")
                await self.email_engine.stop()
                logger.info("✓ 邮件引擎已关闭")
            
            # 5. 关闭策略引擎
            if self.strategy_engine:
                logger.info("正在关闭策略引擎...")
                await self.strategy_engine.stop()
                logger.info("✓ 策略引擎已关闭")
            
            # 4. 关闭数据引擎（包含Redis管理器和数据源管理器）
            if self.data_engine:
                logger.info("正在关闭数据引擎...")
                await self.data_engine.stop()
                logger.info("✓ 数据引擎已关闭")

            # 3. 关闭配置管理器
            if self.config_manager:
                logger.info("正在关闭配置管理器...")
                await self.config_manager.stop()
                logger.info("✓ 配置管理器已关闭")

            # 2. 关闭时钟引擎
            if self.clock_engine:
                logger.info("正在关闭时钟引擎...")
                await self.clock_engine.stop()
                logger.info("✓ 时钟引擎已关闭")
            
            # 1. 关闭事件引擎
            if self.event_engine:
                logger.info("正在关闭事件引擎...")
                await self.event_engine.stop()
                logger.info("✓ 事件引擎已关闭")

            
            self._running = False
            logger.info("=" * 60)
            logger.info("yjQuant v3.0 系统已完全关闭")
            logger.info("=" * 60)
            
        except Exception as e:
            logger.error(f"系统关闭过程中出错: {e}")
    
    def get_system_status(self) -> Dict[str, Any]:
        """获取系统状态"""
        return {
            "running": self._running,
            "components": {
                "config_manager": self.config_manager is not None,
                "event_engine": self.event_engine is not None,
                "clock_engine": self.clock_engine is not None,
                "redis_manager": self.redis_manager is not None,
                "data_source_manager": self.data_source_manager is not None,
                "data_engine": self.data_engine is not None,
                "strategy_engine": self.strategy_engine is not None,
                "email_engine": self.email_engine is not None
            },
            "timestamp": datetime.now().isoformat()
        }


async def main():
    """主函数"""
    system = YJQuantSystem()
    
    try:
        # 启动系统
        success = await system.start()
        if not success:
            logger.error("系统启动失败，退出")
            return
        
        # 运行系统
        await system.run()
        
    except KeyboardInterrupt:
        logger.info("收到键盘中断信号")
    except Exception as e:
        logger.error(f"主程序运行出错: {e}")
    finally:
        # 确保系统被正确关闭
        if system._running:
            await system.stop()


if __name__ == "__main__":
    # 运行主程序
    asyncio.run(main())
