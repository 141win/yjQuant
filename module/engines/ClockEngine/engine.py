# _*_ coding: UTF-8 _*_
# @Time : 2025/8/25 15:06
# @Author : yyj
# @Email : 1410959463@qq.com
# @File : engine.py
# @Project : yjQuant
from module.engines.BaseEngine import (Event,
                                       EVENT_ADD_TASK,
                                       BaseEngine,
                                       MainEngine,
                                       )
import uuid
from datetime import datetime, timedelta, time as dtime
from threading import Lock
from threading import Thread
from time import sleep
from module.data import TaskData


# 时钟引擎
class ClockEngine(BaseEngine):
    def __init__(self, main_engine: MainEngine) -> None:
        super().__init__(main_engine, "clock")
        self._jobs: dict[str, dict] = {}
        self._lock: Lock = Lock()

        # 添加内部定时器线程，精确控制时间间隔
        self._timer_thread: Thread = Thread(target=self._run_timer, daemon=True)
        self._active: bool = False

        # 启动内部定时器
        self._active = True
        self._timer_thread.start()

        self.register(EVENT_ADD_TASK)

        self.main_engine.write_log("clock引擎初始化成功")

    @staticmethod
    def _gen_id() -> str:
        """
        生成唯一的任务ID

        Returns:
            str: 8位十六进制字符串作为任务ID
        """
        return uuid.uuid4().hex[:8]

    # def cancel(self, job_id: str) -> bool:
    #     """
    #     取消指定的定时任务
    #
    #     Args:
    #         job_id: 要取消的任务ID
    #
    #     Returns:
    #         bool: 取消成功返回True，任务不存在返回False
    #     """
    #     with self._lock:
    #         return self._jobs.pop(job_id, None) is not None

    def list_jobs(self) -> list[dict]:
        """
        列出所有定时任务的基本信息

        Returns:
            list[dict]: 任务信息列表，每个任务包含job_id、type、next_run
        """
        with self._lock:
            return [
                {"job_id": k, "type": v["type"], "next_run": v["next_run"]}
                for k, v in self._jobs.items()
            ]

    def _reschedule(self, job_id: str, job: dict, now: datetime) -> None:
        """
        重新调度任务，计算下次执行时间

        Args:
            job_id: 任务ID
            job: 任务信息字典
            now: 当前时间
        """
        with self._lock:
            if job["type"] == "interval":
                # 间隔任务：下次执行时间 = 当前时间 + 间隔秒数
                # print(now)
                job["next_run"] = now + timedelta(seconds=max(1, job["interval"]))
            elif job["type"] == "daily":
                # 每日任务：下次执行时间 = 明天的指定时间点
                hh, mm, ss = job["time"]
                nxt = datetime.combine(now.date(), dtime(hh, mm, ss))
                if nxt <= now:
                    nxt += timedelta(days=1)
                job["next_run"] = nxt
            else:  # once类型任务执行完毕后自动删除
                self._jobs.pop(job_id, None)

    def _run_timer(self) -> None:
        """内部定时器，精确控制检查频率"""
        while self._active:
            now = datetime.now()
            self._check_and_execute_jobs(now)

            # 计算到下次检查的最短时间间隔
            next_check = self._get_next_check_time(now)
            now = datetime.now()
            sleep_time = (next_check - now).total_seconds()
            if sleep_time > 0:
                sleep(sleep_time)

    def _get_next_check_time(self, now: datetime) -> datetime:
        """计算下次检查时间"""
        if not self._jobs:
            return now + timedelta(seconds=60)  # 默认1分钟

        # 找到最近的任务执行时间
        next_times = [job["next_run"] for job in self._jobs.values()]
        if next_times:
            return min(next_times)

        return now + timedelta(seconds=60)

    def _check_and_execute_jobs(self, now: datetime) -> None:
        """检查并执行到期任务"""
        due: list[tuple[str, dict]] = []

        with self._lock:
            for job_id, job in self._jobs.items():
                if job["next_run"] <= now:
                    due.append((job_id, job))

        # 执行到期的任务
        for job_id, job in due:
            try:
                job["func"](*job["args"], **job["kwargs"])
            except Exception as ex:
                self.main_engine.write_log(f"时钟任务执行异常: {ex}")
            finally:
                self._reschedule(job_id, job, now)

    def _add_interval(self, seconds: int, func, *args, **kwargs) -> str:
        """
        添加间隔执行任务

        Args:
            seconds: 执行间隔秒数
            func: 要执行的函数
            *args: 函数的位置参数
            **kwargs: 函数的关键字参数

        Returns:
            str: 任务ID，用于后续取消任务

        Example:
            # 每60秒执行一次check_status函数
            job_id = clock_engine._add_interval(60, check_status)
        """
        job_id = self._gen_id()
        next_run = datetime.now() + timedelta(seconds=max(1, seconds))
        with self._lock:
            self._jobs[job_id] = {
                "type": "interval",  # 任务类型：间隔执行
                "interval": seconds,  # 间隔秒数
                "next_run": next_run,  # 下次执行时间
                "func": func,  # 执行函数
                "args": args,  # 函数参数
                "kwargs": kwargs  # 函数关键字参数
            }
        return job_id

    def _add_daily(self, time_str: str, func, *args, **kwargs) -> str:
        """
        添加每日定时执行任务

        Args:
            time_str: 每日执行时间，格式为 "HH:MM:SS"
            func: 要执行的函数
            *args: 函数的位置参数
            **kwargs: 函数的关键字参数

        Returns:
            str: 任务ID，用于后续取消任务

        Example:
            # 每天14:30:00执行daily_report函数
            job_id = clock_engine.add_daily("14:30:00", daily_report)
        """
        hh, mm, ss = map(int, time_str.split(":"))
        job_id = self._gen_id()
        now = datetime.now()
        run_today = datetime.combine(now.date(), dtime(hh, mm, ss))
        # 如果今天的时间已过，则设置为明天执行
        next_run = run_today if run_today > now else run_today + timedelta(days=1)
        with self._lock:
            self._jobs[job_id] = {
                "type": "daily",  # 任务类型：每日执行
                "time": (hh, mm, ss),  # 执行时间点
                "next_run": next_run,  # 下次执行时间
                "func": func,  # 执行函数
                "args": args,  # 函数参数
                "kwargs": kwargs  # 函数关键字参数
            }
        return job_id

    def _add_at(self, when: datetime, func, *args, **kwargs) -> str:
        """
        添加指定时间执行一次的任务

        Args:
            when: 执行时间点
            func: 要执行的函数
            *args: 函数的位置参数
            **kwargs: 函数的关键字参数

        Returns:
            str: 任务ID，用于后续取消任务

        Example:
            # 在2024年1月1日10:00:00执行once_task函数
            target_time = datetime(2024, 1, 1, 10, 0, 0)
            job_id = clock_engine.add_at(target_time, once_task)
        """
        job_id = self._gen_id()
        with self._lock:
            self._jobs[job_id] = {
                "type": "once",  # 任务类型：执行一次
                "next_run": when,  # 执行时间
                "func": func,  # 执行函数
                "args": args,  # 函数参数
                "kwargs": kwargs  # 函数关键字参数
            }
        return job_id

    def add_task_event(self, event: Event):

        task: TaskData = event.data
        job_id = None
        if task.type == "interval":
            job_id = self._add_interval(task.interval, task.func)
        elif task.type == "daily":
            job_id = self._add_daily(task.interval, task.func)
        elif task.type == "at":
            job_id = self._add_at(task.interval, task.func)
        else:
            print("没有该任务")
        return job_id

    def register(self, event_type: str) -> None:
        """Register log event handler"""
        self.main_engine.register(event_type, self.add_task_event)
        self.main_engine.write_log("注册添加任务事件监听")

    def close(self) -> None:
        """关闭时钟引擎"""
        self._active = False
        self.main_engine.write_log("clock引擎正在关闭")
        if self._timer_thread.is_alive():
            self._timer_thread.join()
        with self._lock:
            self._jobs.clear()
        self.main_engine.write_log("clock引擎关闭成功")
    # def _on_timer(self, event: Event) -> None:
    #     """
    #     定时器事件处理器，检查并执行到期的任务
    #
    #     Args:
    #         event: 定时器事件对象
    #     """
    #     now = datetime.now()
    #     due: list[tuple[str, dict]] = []  # 存储到期的任务 [(job_id, job_info)]
    #
    #     # 查找所有到期的任务
    #     with self._lock:
    #         for job_id, job in self._jobs.items():
    #             if job["next_run"] <= now:
    #                 due.append((job_id, job))
    #
    #     # 执行到期的任务
    #     for job_id, job in due:
    #         try:
    #             job["func"](*job["args"], **job["kwargs"])
    #         except Exception as ex:
    #             # 记录任务执行异常，但不中断其他任务的执行
    #             self.main_engine.write_log("时钟任务执行异常: {}".format(ex))
    #         finally:
    #             # 重新调度任务（对于interval和daily类型）
    #             self._reschedule(job_id, job, now)
