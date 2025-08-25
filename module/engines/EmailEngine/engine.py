# _*_ coding: UTF-8 _*_
# @Time : 2025/8/25 15:09
# @Author : yyj
# @Email : 1410959463@qq.com
# @File : EmailEngine.py
# @Project : yjQuant

import traceback
from email.message import EmailMessage
import smtplib
from threading import Thread

from module.data import EmailData
from module.engines.BaseEngine import (BaseEngine,
                                       MainEngine, )
from module.engines.log import (ERROR, INFO)
from queue import Empty
from module.config import SETTINGS


# 邮件引擎
class EmailEngine(BaseEngine):
    """
    Provides email sending function.
    """

    def __init__(self, main_engine: MainEngine) -> None:
        """"""
        super().__init__(main_engine, "email")

        self.thread: Thread = Thread(target=self.run)

        self.active: bool = False
        self.main_engine.write_log("email引擎初始化成功")
        self.start()

    def run(self) -> None:
        """"""
        server: str = SETTINGS["email.server"]
        port: int = SETTINGS["email.port"]
        username: str = SETTINGS["email.username"]
        password: str = SETTINGS["email.password"]

        while self.active:
            try:
                email: EmailData = self.main_engine.email_queue.get(block=True, timeout=1)

                if not email.receiver:
                    email.receiver = SETTINGS["email.receiver"]

                msg: EmailMessage = EmailMessage()
                msg["From"] = SETTINGS["email.sender"]
                msg["To"] = email.receiver
                msg["Subject"] = email.subject
                msg.set_content(email.content)

                try:
                    with smtplib.SMTP_SSL(server, port) as smtp:
                        smtp.login(username, password)
                        smtp.send_message(msg)
                        smtp.close()
                    self.main_engine.write_log("email发送成功")
                except Exception:
                    log_msg: str = "邮件发送失败: {}".format(traceback.format_exc())
                    self.main_engine.write_log(log_msg, ERROR)
            except Empty:
                pass

    def start(self) -> None:
        """"""
        self.active = SETTINGS["email.active"]
        if self.active:
            self.thread.start()
            self.main_engine.write_log("email引擎启动成功", INFO)

    def close(self) -> None:
        """"""
        if not self.active:
            return
        self.main_engine.write_log("email引擎正在关闭")
        self.active = False
        self.thread.join()
        self.main_engine.write_log("email引擎关闭成功", INFO)
