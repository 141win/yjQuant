"""
协议模块（protocol）

主要逻辑：
- 采用 JSON Lines（每条消息一行 JSON，以 "\n" 分隔）作为轻量级消息封装协议；
- 提供 send_json 与 read_json 两个协程工具函数，分别用于发送与读取一条 JSON 消息；

必要解释：
- 选择 JSON Lines 的原因：实现简单、可流式传输、便于调试；
- 发送端通过 writer.write 写入一行 JSON 字节并调用 drain 刷新；
- 接收端通过 reader.readline 读取一行，再做 JSON 解析，读取到空行/EOF 返回 None；
- 出错时（JSONDecodeError）返回 None 以便上层决定忽略或断开连接。
"""

import asyncio
import json
import logging
from typing import Any, Optional, Dict

logger = logging.getLogger(__name__)


async def send_json(writer: asyncio.StreamWriter, message: Dict[str, Any]) -> None:
    """发送一条 JSON 消息（行分隔）

    Args:
        writer: asyncio StreamWriter
        message: 可序列化为 JSON 的字典
    """
    data = (json.dumps(message, ensure_ascii=False) + "\n").encode("utf-8")
    writer.write(data)
    await writer.drain()


async def read_json(reader: asyncio.StreamReader) -> Optional[Dict[str, Any]]:
    """读取一条 JSON 消息（行分隔）

    Returns:
        字典或 None（EOF/解析失败）
    """
    try:
        # 设置更大的读取限制，避免 "chunk is longer than limit" 错误
        line = await reader.readline()
        if not line:
            return None
        
        # 检查行长度，如果过长则跳过
        if len(line) > 1024 * 1024:  # 1MB 限制
            logger.warning(f"Received line too long ({len(line)} bytes), skipping")
            return None
            
        try:
            return json.loads(line.decode("utf-8").rstrip("\n"))
        except json.JSONDecodeError as e:
            logger.warning(f"JSON decode error: {e}")
            return None
    except Exception as e:
        logger.warning(f"Read error: {e}")
        return None


