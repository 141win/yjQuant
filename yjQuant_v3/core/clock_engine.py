"""
时钟引擎 - 纯定时事件发布器

职责:
- 不执行具体任务逻辑
- 维护定时任务列表
- 按时间调度任务
- 到时间向事件引擎发布事件
"""

import asyncio
import logging
from typing import Dict, List, Any, Optional, Callable
from dataclasses import dataclass
from datetime import datetime, timedelta
import heapq

logger = logging.getLogger(__name__)


@dataclass
class ClockTask:
    """时钟任务定义"""
    id: str
    name: str
    event_type: str
    interval: int  # 间隔秒数
    event_data: Any = None
    enabled: bool = True
    last_run: Optional[datetime] = None
    next_run: Optional[datetime] = None
    run_count: int = 0
    error_count: int = 0
    last_error: Optional[str] = None
    
    def __post_init__(self):
        """初始化后计算下次运行时间"""
        if self.next_run is None:
            self.next_run = self._calculate_next_run_time(datetime.now())
    
    def _is_minute_level_task(self) -> bool:
        """判断是否为分钟级任务（>= 60秒且能被60整除）"""
        return self.interval >= 60 and self.interval % 60 == 0
    
    def _align_to_minute_boundary(self, target_time: datetime) -> datetime:
        """将时间对齐到分钟整点（秒和微秒归零）"""
        return target_time.replace(second=0, microsecond=0)
    
    def _calculate_next_run_time(self, current_time: datetime) -> datetime:
        """计算下次执行时间，分钟级任务对齐到整点"""
        if self._is_minute_level_task():
            # 分钟级任务：对齐到下一个分钟整点
            next_minute = current_time.replace(second=0, microsecond=0) + timedelta(minutes=1)
            return next_minute
        else:
            # 非分钟级任务：保持原有逻辑
            return current_time + timedelta(seconds=self.interval)
    
    def __lt__(self, other):
        """用于堆排序的比较方法"""
        if self.next_run is None:
            return False
        if other.next_run is None:
            return True
        return self.next_run < other.next_run


class ClockEngine:
    """时钟引擎 - 纯定时事件发布器，不执行具体任务逻辑"""
    
    def __init__(self, event_engine):
        """
        初始化时钟引擎
        
        Args:
            event_engine: 事件引擎实例，用于发布定时事件
        """
        self._active = False
        self._event_engine = event_engine
        self._tasks: Dict[str, ClockTask] = {}  # {task_id: ClockTask}
        self._task_heap: List[ClockTask] = []   # 按next_run时间排序的堆
        self._main_loop: Optional[asyncio.Task] = None
        self._task_id_counter = 0
        
        logger.info("时钟引擎初始化完成")
    
    async def start(self, event_engine) -> None:
        """启动时钟引擎，需要事件引擎引用用于发布事件"""
        if self._active:
            logger.warning("时钟引擎已经在运行")
            return
        
        self._event_engine = event_engine
        self._active = True
        self._main_loop = asyncio.create_task(self._run_scheduler_loop())
        logger.info("时钟引擎启动成功")
    
    async def stop(self) -> None:
        """停止时钟引擎"""
        if not self._active:
            logger.warning("时钟引擎已经停止")
            return
        
        self._active = False
        if self._main_loop:
            self._main_loop.cancel()
            try:
                await self._main_loop
            except asyncio.CancelledError:
                pass
        
        # 清理任务
        self._tasks.clear()
        self._task_heap.clear()
        logger.info("时钟引擎已停止")
    
    def register_task(self, name: str, event_type: str, interval: int, event_data: Any = None) -> str:
        """注册定时任务 - 返回任务ID，任务到时发布指定事件"""
        if interval <= 0:
            raise ValueError("任务间隔必须大于0")
        
        # 生成任务ID
        task_id = f"task_{self._task_id_counter:06d}"
        self._task_id_counter += 1
        
        # 创建任务
        task = ClockTask(
            id=task_id,
            name=name,
            event_type=event_type,
            interval=interval,
            event_data=event_data
        )
        
        # 添加到任务表
        self._tasks[task_id] = task
        
        # 添加到堆中
        heapq.heappush(self._task_heap, task)
        
        logger.info(f"定时任务注册成功: {name} ({task_id}), 间隔: {interval}秒")
        return task_id
    
    def unregister_task(self, task_id: str) -> bool:
        """取消定时任务"""
        if task_id not in self._tasks:
            logger.warning(f"任务ID不存在: {task_id}")
            return False
        
        task = self._tasks[task_id]
        
        # 从任务表中移除
        del self._tasks[task_id]
        
        # 从堆中移除（标记为禁用，在调度时过滤）
        task.enabled = False
        
        logger.info(f"定时任务取消成功: {task_id}")
        return True
    
    async def _run_scheduler_loop(self) -> None:
        """调度器主循环"""
        logger.info("任务调度循环启动")
        
        while self._active:
            try:
                await self._schedule_tasks()
                await asyncio.sleep(0.1)  # 100ms检查间隔
                
            except Exception as e:
                logger.error(f"任务调度循环出错: {e}")
                await asyncio.sleep(1)
        
        logger.info("任务调度循环已停止")
    
    async def _schedule_tasks(self) -> None:
        """调度任务 - 按执行时间排序，到时发布事件"""
        now = datetime.now()
        
        # 检查堆顶任务是否应该执行
        while self._task_heap and self._active:
            task = self._task_heap[0]
            
            # 跳过禁用的任务
            if not task.enabled:
                heapq.heappop(self._task_heap)
                continue
            
            # 检查是否到达执行时间
            if task.next_run and task.next_run <= now:
                # 执行任务（发布事件）
                await self._publish_timed_event(task)
                
                # 从堆中移除
                heapq.heappop(self._task_heap)
                
                # 如果任务仍然启用，重新计算下次执行时间并重新加入堆
                if task.enabled:
                    task.last_run = now
                    task.run_count += 1
                    task.next_run = task._calculate_next_run_time(now)
                    heapq.heappush(self._task_heap, task)
            else:
                # 堆顶任务还没到时间，其他任务更不可能到时间
                break
    
    async def _publish_timed_event(self, task: ClockTask) -> None:
        """发布定时事件 - 通过事件引擎发布事件，不执行具体逻辑"""
        try:
            # 通过事件引擎发布事件
            await self._event_engine.publish(task.event_type, task.event_data)
            
            logger.debug(f"定时事件发布成功: {task.name} -> {task.event_type}")
            
        except Exception as e:
            task.error_count += 1
            task.last_error = str(e)
            logger.error(f"定时事件发布失败: {task.name}, 错误: {e}")
    
    def get_task_count(self) -> int:
        """获取任务总数"""
        return len(self._tasks)
    
    def get_enabled_task_count(self) -> int:
        """获取启用的任务数量"""
        return sum(1 for task in self._tasks.values() if task.enabled)
    
    def get_task_info(self, task_id: str) -> Optional[ClockTask]:
        """获取任务信息"""
        return self._tasks.get(task_id)
    
    def enable_task(self, task_id: str) -> bool:
        """启用任务"""
        if task_id not in self._tasks:
            return False
        
        task = self._tasks[task_id]
        task.enabled = True
        logger.info(f"任务已启用: {task_id}")
        return True
    
    def disable_task(self, task_id: str) -> bool:
        """禁用任务"""
        if task_id not in self._tasks:
            return False
        
        task = self._tasks[task_id]
        task.enabled = False
        logger.info(f"任务已禁用: {task_id}")
        return True
    
    def is_active(self) -> bool:
        """检查时钟引擎是否处于活动状态"""
        return self._active
