import asyncio
import logging
import time
from typing import Dict, Any, Set, Optional, Tuple
from dataclasses import dataclass

from .protocol import send_json

logger = logging.getLogger(__name__)


@dataclass
class ClientInfo:
    """客户端连接信息"""
    writer: asyncio.StreamWriter
    reader: asyncio.StreamReader
    peer: Tuple[str, int]
    connected_at: float
    last_heartbeat: float
    message_count: int = 0


class NetworkServer:
    """网络通信服务端 - 支持长时间连接和一对多广播

    主要逻辑：
    - 启动一个 asyncio TCP 服务器，接收多个客户端连接；
    - 订阅事件引擎的 "data_arrived" 事件，并将事件广播给所有已连接的客户端；
    - 维护客户端连接信息，包括心跳检测和连接统计；
    - 支持心跳机制保持连接活跃，自动清理断线客户端；

    必要解释：
    - 使用 ClientInfo 记录每个连接的详细信息；
    - 心跳机制：定期发送心跳包，检测客户端是否在线；
    - 广播机制：将事件同时推送给所有活跃客户端；
    - 错误恢复：连接异常时自动清理，不影响其他客户端。
    """
    def __init__(self, host: str = "0.0.0.0", port: int = 8765, 
                 heartbeat_interval: float = 30.0, client_timeout: float = 60.0):
        self.host = host
        self.port = port
        self.heartbeat_interval = heartbeat_interval  # 心跳间隔（秒）
        self.client_timeout = client_timeout  # 客户端超时时间（秒）
        
        self._server: Optional[asyncio.AbstractServer] = None
        self._clients: Dict[asyncio.StreamWriter, ClientInfo] = {}
        self._event_engine = None
        self._heartbeat_task: Optional[asyncio.Task] = None
        self._cleanup_task: Optional[asyncio.Task] = None

    async def start(self, event_engine) -> None:
        if self._server:
            logger.warning("NetworkServer already running")
            return

        self._event_engine = event_engine
        self._server = await asyncio.start_server(self._handle_client, self.host, self.port)

        # Subscribe to data_arrived and broadcast
        if self._event_engine:
            self._event_engine.subscribe("data_arrived", self._on_data_arrived)

        # 启动心跳和清理任务
        self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())
        self._cleanup_task = asyncio.create_task(self._cleanup_loop())

        logger.info(f"NetworkServer listening on {self.host}:{self.port}")

    async def stop(self) -> None:
        if not self._server:
            return

        # 停止后台任务
        if self._heartbeat_task:
            self._heartbeat_task.cancel()
            try:
                await self._heartbeat_task
            except asyncio.CancelledError:
                pass

        if self._cleanup_task:
            self._cleanup_task.cancel()
            try:
                await self._cleanup_task
            except asyncio.CancelledError:
                pass

        if self._event_engine:
            self._event_engine.unsubscribe_by_handler("data_arrived", self._on_data_arrived)

        # 关闭所有客户端连接
        for client_info in list(self._clients.values()):
            try:
                client_info.writer.close()
                await client_info.writer.wait_closed()
            except Exception:
                pass
        self._clients.clear()

        self._server.close()
        await self._server.wait_closed()
        self._server = None
        logger.info("NetworkServer stopped")

    async def _handle_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        peer = writer.get_extra_info('peername')
        current_time = time.time()
        
        # 创建客户端信息
        client_info = ClientInfo(
            writer=writer,
            reader=reader,
            peer=peer,
            connected_at=current_time,
            last_heartbeat=current_time
        )
        self._clients[writer] = client_info
        
        logger.info(f"Client connected: {peer} (total: {len(self._clients)})")
        
        try:
            while True:
                # 读取客户端消息（心跳响应或数据）
                data = await reader.readline()
                if not data:
                    break
                    
                # 更新最后心跳时间
                client_info.last_heartbeat = time.time()
                client_info.message_count += 1
                
                # 可以在这里处理客户端主动发送的消息
                # 目前主要用于心跳检测
                
        except Exception as e:
            logger.warning(f"Client error {peer}: {e}")
        finally:
            # 清理客户端连接
            self._clients.pop(writer, None)
            try:
                writer.close()
                await writer.wait_closed()
            except Exception:
                pass
            logger.info(f"Client disconnected: {peer} (remaining: {len(self._clients)})")

    async def _on_data_arrived(self, event: Dict[str, Any]):
        """广播 data_arrived 事件给所有客户端"""
        message = {"type": "data_arrived", "payload": event}
        dead_clients = []
        
        for writer, client_info in list(self._clients.items()):
            try:
                await send_json(writer, message)
                client_info.message_count += 1
            except Exception as e:
                logger.warning(f"Send failed to {client_info.peer}, dropping client: {e}")
                dead_clients.append(writer)
        
        # 清理失败的连接
        for writer in dead_clients:
            self._clients.pop(writer, None)
            try:
                writer.close()
                await writer.wait_closed()
            except Exception:
                pass
        
        if dead_clients:
            logger.info(f"Cleaned up {len(dead_clients)} dead connections, active: {len(self._clients)}")

    async def _heartbeat_loop(self):
        """心跳循环：定期向所有客户端发送心跳包"""
        while True:
            try:
                await asyncio.sleep(self.heartbeat_interval)
                
                if not self._clients:
                    continue
                    
                heartbeat_msg = {"type": "heartbeat", "timestamp": time.time()}
                dead_clients = []
                
                for writer, client_info in list(self._clients.items()):
                    try:
                        await send_json(writer, heartbeat_msg)
                    except Exception as e:
                        logger.debug(f"Heartbeat failed to {client_info.peer}: {e}")
                        dead_clients.append(writer)
                
                # 清理心跳失败的连接
                for writer in dead_clients:
                    self._clients.pop(writer, None)
                    try:
                        writer.close()
                        await writer.wait_closed()
                    except Exception:
                        pass
                        
                if dead_clients:
                    logger.info(f"Heartbeat cleanup: removed {len(dead_clients)} clients, active: {len(self._clients)}")
                    
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Heartbeat loop error: {e}")

    async def _cleanup_loop(self):
        """清理循环：定期清理超时的客户端连接"""
        while True:
            try:
                await asyncio.sleep(10)  # 每10秒检查一次
                
                current_time = time.time()
                timeout_clients = []
                
                for writer, client_info in list(self._clients.items()):
                    if current_time - client_info.last_heartbeat > self.client_timeout:
                        timeout_clients.append(writer)
                
                # 清理超时连接
                for writer in timeout_clients:
                    client_info = self._clients.pop(writer, None)
                    if client_info:
                        logger.info(f"Client timeout: {client_info.peer}")
                        try:
                            writer.close()
                            await writer.wait_closed()
                        except Exception:
                            pass
                
                if timeout_clients:
                    logger.info(f"Timeout cleanup: removed {len(timeout_clients)} clients, active: {len(self._clients)}")
                    
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Cleanup loop error: {e}")

    def get_stats(self) -> Dict[str, Any]:
        """获取服务器统计信息"""
        current_time = time.time()
        return {
            "host": self.host,
            "port": self.port,
            "total_clients": len(self._clients),
            "heartbeat_interval": self.heartbeat_interval,
            "client_timeout": self.client_timeout,
            "clients": [
                {
                    "peer": client_info.peer,
                    "connected_at": client_info.connected_at,
                    "last_heartbeat": client_info.last_heartbeat,
                    "message_count": client_info.message_count,
                    "uptime": current_time - client_info.connected_at
                }
                for client_info in self._clients.values()
            ]
        }


