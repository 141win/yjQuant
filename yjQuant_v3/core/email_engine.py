"""
邮件引擎 - 负责邮件发送功能

职责:
- 向事件引擎订阅邮件事件
- 处理邮件发送逻辑
"""

import logging
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from typing import Dict, Any, Optional,List

logger = logging.getLogger(__name__)


class EmailEngine:
    """邮件引擎 - 负责邮件发送功能"""
    
    def __init__(self, config_manager):
        """初始化邮件引擎"""
        self.config_manager = config_manager
        self._event_engine = None
        self._email_config = {}
        logger.info("邮件引擎初始化完成")
    
    async def start(self, event_engine) -> None:
        """启动邮件引擎"""
        if self._event_engine:
            logger.warning("邮件引擎已经在运行")
            return
        
        self._event_engine = event_engine
        
        # # 加载邮件配置
        # await self._load_email_config()
        self._email_config = self.config_manager.get_config("email")

        # 订阅邮件事件、邮件配置变更事件
        self._event_engine.subscribe("email_event", self.handle_email_event)
        self._event_engine.subscribe("email_config_changed", self.handle_config_change)
        logger.info("邮件引擎启动成功")
    
    async def stop(self) -> None:
        """停止邮件引擎"""
        if not self._event_engine:
            logger.warning("邮件引擎已经停止")
            return
        
        # 取消事件订阅
        self._event_engine.unsubscribe_by_handler("email_event", self.handle_email_event)
        self._event_engine.unsubscribe_by_handler("email_config_changed", self.handle_config_change)
        self._event_engine = None
        
        logger.info("邮件引擎已停止")
    
    # async def _load_email_config(self) -> None:
    #     """加载邮件配置"""
    #     try:
    #         config = self.config_manager.get_config("email")
    #         if config:
    #             self._email_config = config
    #             logger.info("邮件配置加载完成")
    #         else:
    #             logger.warning("未找到邮件配置")
    #     except Exception as e:
    #         logger.error(f"加载邮件配置失败: {e}")
    
    async def handle_email_event(self, event_data: Any) -> None:
        """处理邮件事件"""
        try:
            logger.info(f"收到邮件事件: {event_data}")
            
            # 解析邮件数据
            email_data = self._parse_email_data(event_data)
            if not email_data:
                logger.warning("邮件数据格式无效")
                return
            
            # 发送邮件
            success = await self._send_email(email_data)
            logger.info(f"邮件发送{'成功' if success else '失败'}: {email_data.get('subject', 'Unknown')}")
            
        except Exception as e:
            logger.error(f"处理邮件事件失败: {e}")

    async def handle_config_change(self, event_data: Any) -> None:
        """处理邮件配置变更事件"""
        logger.info("开始处理邮件引擎配置变更事件...")

        if not event_data:
            logger.warning("配置变更事件数据为空")
            return

        # 解析新配置
        new_config = event_data.get("new_config", {})
        if not new_config:
            logger.warning("新配置为空")
            return

        # 更新新配置
        self._email_config = new_config

        logger.info(f"接收到新配置：: {new_config}")
        logger.info("邮件引擎配置变更处理完成")

    """---------------------------发送邮件方法-----------------------------------"""
    async def _send_email(self, email_data: Dict[str, Any]) -> bool:
        """发送邮件"""
        try:
            # 获取邮件配置
            smtp_server = self._email_config.get("smtp_server")
            smtp_port = self._email_config.get("smtp_port")
            username = self._email_config.get("username")
            password = self._email_config.get("password")
            sender = self._email_config.get("from_email")
            receiver = self._email_config.get("to_email")

            if not all([smtp_server, smtp_port, username, password, sender, receiver]):
                logger.error("邮件配置不完整")
                return False
            
            # 构建邮件
            msg = self._build_email_message(email_data, sender, receiver)
            
            # 发送邮件
            success = self._send_via_smtp(smtp_server, smtp_port, username, password, msg)
            return success
            
        except Exception as e:
            logger.error(f"发送邮件失败: {e}")
            return False

    @staticmethod
    def _parse_email_data(event_data: Any) -> Optional[Dict[str, Any]]:
        """解析邮件数据"""
        try:
            if isinstance(event_data, dict):
                return event_data
            elif hasattr(event_data, '__dict__'):
                return {
                    'subject': getattr(event_data, 'subject', ''),
                    'content': getattr(event_data, 'content', ''),
                    'content_type': getattr(event_data, 'content_type', 'text')
                }
            else:
                logger.warning(f"不支持的邮件数据格式: {type(event_data)}")
                return None

        except Exception as e:
            logger.error(f"解析邮件数据失败: {e}")
            return None

    @staticmethod
    def _build_email_message(email_data: Dict[str, Any], sender: str, receiver: List) -> MIMEMultipart:
        """构建邮件消息"""
        msg = MIMEMultipart()
        msg["From"] = sender
        msg["To"] = receiver
        msg["Subject"] = email_data.get("subject", "")
        
        # 设置邮件内容
        content = email_data.get("content", "")
        content_type = email_data.get("content_type", "text")
        
        if content_type == "html":
            msg.attach(MIMEText(content, "html", "utf-8"))
        else:
            msg.attach(MIMEText(content, "plain", "utf-8"))
        
        return msg
    
    @staticmethod
    def _send_via_smtp(smtp_server: str, smtp_port: int,
                       username: str, password: str, msg: MIMEMultipart) -> bool:
        """通过SMTP发送邮件"""
        try:
            if smtp_port == 465:
                # SSL连接
                with smtplib.SMTP_SSL(smtp_server, smtp_port) as smtp:
                    smtp.login(username, password)
                    smtp.send_message(msg)
                    # 强制关闭连接
                    smtp.quit()
            else:
                # 非SSL连接
                with smtplib.SMTP(smtp_server, smtp_port) as smtp:
                    smtp.starttls()  # 启用TLS
                smtp.login(username, password)
                smtp.send_message(msg)
                # 强制关闭连接
                smtp.quit()
            
            logger.info(f"邮件发送成功: {msg['Subject']}")
            return True
            
        except Exception as e:
            # 忽略QQ邮箱的SSL关闭错误
            if "(-1, b'\\x00\\x00\\x00')" in str(e) or "(-1, b'')" in str(e):
                logger.info(f"邮件发送成功: {msg['Subject']} (忽略SSL关闭错误)")
                return True
            else:
                logger.error(f"SMTP发送失败: {e}")
                return False
