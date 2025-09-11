import asyncio
import logging
import time
from typing import Optional, Dict, Any

from .protocol import read_json, send_json

logger = logging.getLogger(__name__)


class NetworkClient:
    """网络通信客户端 - 支持长时间连接和自动重连

    主要逻辑：
    - 连接到远端 NetworkServer，持续读取 JSON Lines 消息；
    - 将收到的 "data_arrived" 事件透传到本地事件引擎（publish）；
    - 支持心跳响应和自动重连机制；
    - 处理连接异常和网络中断情况；

    必要解释：
    - 使用重连机制确保长时间稳定连接；
    - 心跳响应：收到服务端心跳包时更新最后活跃时间；
    - 自动重连：连接断开时自动尝试重新连接；
    - 指数退避：重连失败时使用指数退避策略避免频繁重试。
    """
    def __init__(self, host: str = "127.0.0.1", port: int = 8765, 
                 reconnect_interval: float = 5.0, max_reconnect_attempts: int = 10):
        self.host = host
        self.port = port
        self.reconnect_interval = reconnect_interval  # 重连间隔（秒）
        self.max_reconnect_attempts = max_reconnect_attempts  # 最大重连次数
        
        self._reader: Optional[asyncio.StreamReader] = None
        self._writer: Optional[asyncio.StreamWriter] = None
        self._task: Optional[asyncio.Task] = None
        self._event_engine = None
        self._connected = False
        self._last_heartbeat = 0
        self._reconnect_attempts = 0
        self._should_reconnect = True

    async def start(self, event_engine) -> None:
        if self._task:
            logger.warning("NetworkClient already running")
            return

        self._event_engine = event_engine
        self._should_reconnect = True
        self._reconnect_attempts = 0
        self._task = asyncio.create_task(self._run_with_reconnect())
        logger.info(f"NetworkClient starting connection to {self.host}:{self.port}")

    async def stop(self) -> None:
        self._should_reconnect = False
        
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None

        await self._disconnect()
        logger.info("NetworkClient stopped")

    async def _connect(self) -> bool:
        """尝试连接到服务器"""
        try:
            self._reader, self._writer = await asyncio.open_connection(self.host, self.port)
            self._connected = True
            self._last_heartbeat = time.time()
            self._reconnect_attempts = 0
            logger.info(f"NetworkClient connected to {self.host}:{self.port}")
            return True
        except Exception as e:
            logger.warning(f"Connection failed: {e}")
            return False

    async def _disconnect(self):
        """断开连接"""
        self._connected = False
        if self._writer:
            try:
                self._writer.close()
                await self._writer.wait_closed()
            except Exception:
                pass
            self._writer = None
            self._reader = None

    async def _run_with_reconnect(self):
        """带重连机制的主循环"""
        while self._should_reconnect:
            try:
                # 尝试连接
                if await self._connect():
                    # 连接成功，开始读取消息
                    await self._run()
                else:
                    # 连接失败，等待重连
                    if self._reconnect_attempts < self.max_reconnect_attempts:
                        self._reconnect_attempts += 1
                        wait_time = min(self.reconnect_interval * (2 ** (self._reconnect_attempts - 1)), 60)
                        logger.info(f"Reconnecting in {wait_time}s (attempt {self._reconnect_attempts}/{self.max_reconnect_attempts})")
                        await asyncio.sleep(wait_time)
                    else:
                        logger.error(f"Max reconnection attempts reached ({self.max_reconnect_attempts}), giving up")
                        break
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Unexpected error in reconnect loop: {e}")
                await asyncio.sleep(5)

    async def _run(self):
        """主消息循环"""
        try:
            while self._connected and self._should_reconnect:
                msg = await read_json(self._reader)
                if msg is None:
                    # 连接断开
                    logger.warning("Connection lost, will attempt to reconnect")
                    break
                await self._handle_message(msg)
        except asyncio.CancelledError:
            pass
        except Exception as e:
            logger.error(f"NetworkClient error: {e}")
        finally:
            await self._disconnect()

    async def _handle_message(self, msg: Dict[str, Any]):
        """处理接收到的消息"""
        msg_type = msg.get("type")
        payload = msg.get("payload")
        
        if msg_type == "data_arrived" and self._event_engine:
            # 转发数据到达事件到本地事件引擎
            await self._event_engine.publish("data_arrived", payload)
            logger.debug("Forwarded data_arrived event to local event engine")
            
        elif msg_type == "heartbeat":
            # 处理心跳包
            self._last_heartbeat = time.time()
            logger.debug("Received heartbeat from server")
            
        else:
            logger.debug(f"Unknown message type: {msg_type}")

    def get_stats(self) -> Dict[str, Any]:
        """获取客户端统计信息"""
        return {
            "host": self.host,
            "port": self.port,
            "connected": self._connected,
            "last_heartbeat": self._last_heartbeat,
            "reconnect_attempts": self._reconnect_attempts,
            "max_reconnect_attempts": self.max_reconnect_attempts,
            "reconnect_interval": self.reconnect_interval
        }


