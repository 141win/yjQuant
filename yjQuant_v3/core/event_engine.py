"""
事件引擎 - 纯事件分发器

职责:
- 不定义事件类型，不关心事件内容
- 维护事件订阅表
- 接收事件并分发给所有订阅者
- 处理订阅者的异常，不影响其他订阅者
"""

import asyncio
import logging
from typing import Dict, List, Any, Callable, Optional
from dataclasses import dataclass
from datetime import datetime
import uuid

logger = logging.getLogger(__name__)


@dataclass
class Subscription:
    """事件订阅信息"""
    id: str
    event_type: str
    handler: Callable
    created_at: datetime
    enabled: bool = True


class EventEngine:
    """事件引擎 - 纯事件分发器，不定义事件类型，只负责事件处理"""
    
    def __init__(self):
        """初始化事件引擎"""
        self._active = False
        self._subscriptions: Dict[str, List[Subscription]] = {}  # {event_type: [subscriptions]}
        self._subscription_map: Dict[str, Subscription] = {}    # {subscription_id: subscription}
        self._event_queue: asyncio.Queue = asyncio.Queue()
        self._main_loop: Optional[asyncio.Task] = None
        
        logger.info("事件引擎初始化完成")
    
    async def start(self) -> None:
        """启动事件引擎，开始监听和处理事件"""
        if self._active:
            logger.warning("事件引擎已经在运行")
            return
        
        self._active = True
        self._main_loop = asyncio.create_task(self._run_event_loop())
        logger.info("事件引擎启动成功")
    
    async def stop(self) -> None:
        """停止事件引擎"""
        if not self._active:
            logger.warning("事件引擎已经停止")
            return
        
        self._active = False
        if self._main_loop:
            self._main_loop.cancel()
            try:
                await self._main_loop
            except asyncio.CancelledError:
                pass
        
        # 清理订阅
        self._subscriptions.clear()
        self._subscription_map.clear()
        logger.info("事件引擎已停止")
    
    def subscribe(self, event_type: str, handler: Callable) -> str:
        """注册事件订阅 - 返回订阅ID"""
        if not callable(handler):
            raise ValueError("handler必须是可调用对象")
        
        # 生成唯一订阅ID
        subscription_id = str(uuid.uuid4())
        
        # 创建订阅对象
        subscription = Subscription(
            id=subscription_id,
            event_type=event_type,
            handler=handler,
            created_at=datetime.now()
        )
        
        # 添加到订阅表
        if event_type not in self._subscriptions:
            self._subscriptions[event_type] = []
        self._subscriptions[event_type].append(subscription)
        self._subscription_map[subscription_id] = subscription
        
        logger.info(f"事件订阅注册成功: {event_type} -> {subscription_id}")
        return subscription_id
    
    def unsubscribe(self, subscription_id: str) -> bool:
        """取消事件订阅"""
        if subscription_id not in self._subscription_map:
            logger.warning(f"订阅ID不存在: {subscription_id}")
            return False
        
        subscription = self._subscription_map[subscription_id]
        event_type = subscription.event_type
        
        # 从订阅表中移除
        if event_type in self._subscriptions:
            self._subscriptions[event_type] = [
                sub for sub in self._subscriptions[event_type] 
                if sub.id != subscription_id
            ]
            # 如果没有订阅者了，删除该事件类型
            if not self._subscriptions[event_type]:
                del self._subscriptions[event_type]
        
        # 从映射表中移除
        del self._subscription_map[subscription_id]
        
        logger.info(f"事件订阅取消成功: {subscription_id}")
        return True
    
    def unsubscribe_by_handler(self, event_type: str, handler: Callable) -> bool:
        """通过处理器取消事件订阅"""
        if event_type not in self._subscriptions:
            return False
        
        # 查找匹配的订阅
        for subscription in self._subscriptions[event_type]:
            if subscription.handler == handler:
                return self.unsubscribe(subscription.id)
        
        return False
    
    async def publish(self, event_type: str, event_data: Any = None) -> None:
        """发布事件到所有订阅者"""
        if not self._active:
            logger.warning("事件引擎未启动，无法发布事件")
            return
        
        # 将事件放入队列
        await self._event_queue.put((event_type, event_data))
        logger.debug(f"事件已加入队列: {event_type}")
    
    async def _run_event_loop(self) -> None:
        """事件处理主循环"""
        logger.info("事件处理循环启动")
        
        while self._active:
            try:
                # 从队列获取事件
                event_type, event_data = await asyncio.wait_for(
                    self._event_queue.get(), timeout=1.0
                )
                
                # 处理事件
                await self._process_event(event_type, event_data)
                
            except asyncio.TimeoutError:
                # 超时是正常的，继续循环
                continue
            except Exception as e:
                logger.error(f"事件处理循环出错: {e}")
                await asyncio.sleep(0.1)
        
        logger.info("事件处理循环已停止")
    
    async def _process_event(self, event_type: str, event_data: Any) -> None:
        """处理事件 - 查询注册表，调用对应的处理器"""
        if event_type not in self._subscriptions:
            logger.debug(f"事件类型无订阅者: {event_type}")
            return
        
        subscriptions = self._subscriptions[event_type]
        logger.debug(f"处理事件: {event_type}, 订阅者数量: {len(subscriptions)}")
        
        # 调用所有订阅者
        for subscription in subscriptions:
            if not subscription.enabled:
                continue
            
            try:
                # 检查处理器是否为异步函数
                if asyncio.iscoroutinefunction(subscription.handler):
                    await subscription.handler(event_data)
                else:
                    # 同步函数在线程池中执行
                    loop = asyncio.get_event_loop()
                    await loop.run_in_executor(None, subscription.handler, event_data)
                
                logger.debug(f"事件处理器执行成功: {subscription.id}")
                
            except Exception as e:
                logger.error(f"事件处理器执行失败: {subscription.id}, 错误: {e}")
                # 异常不影响其他订阅者，继续处理
    
    def get_subscription_count(self, event_type: str) -> int:
        """获取指定事件类型的订阅者数量"""
        return len(self._subscriptions.get(event_type, []))
    
    def get_total_subscription_count(self) -> int:
        """获取总订阅数量"""
        return len(self._subscription_map)