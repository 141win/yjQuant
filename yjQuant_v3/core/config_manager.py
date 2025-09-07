"""
配置管理器 - 纯配置管理器

职责:
- 加载和管理所有配置文件
- 提供配置查询接口
- 定时检查配置变更
- 通过事件引擎通知配置变更
"""

import json
# import os
import asyncio
from pathlib import Path
from typing import Dict, Any#, Optional
import logging
from datetime import datetime
import hashlib

logger = logging.getLogger(__name__)


class ConfigManager:
    """配置管理器 - 纯配置管理器"""

    def __init__(self, config_dir: str = "./config"):
        """
        初始化配置管理器
        
        Args:
            config_dir: 配置文件目录
        """
        self.config_dir = Path(config_dir)
        self.configs: Dict[str, Any] = {}  # {config_name: config_data} 存储配置
        self.config_hashes: Dict[str, str] = {}  # {config_name: file_hash} 存储文件哈希
        self.config_files: Dict[str, Path] = {}  # {config_name: file_path} 存储文件路径

        # 配置检查间隔,默认60秒
        self.config_check_interval = 60
        # 组件引用
        self._event_engine = None
        self._clock_engine = None
        self._config_check_task_id = None

        # 确保配置目录存在
        self.config_dir.mkdir(exist_ok=True)

        logger.info(f"配置管理器初始化完成，配置目录: {self.config_dir}")

    # 对外开放：启动配置管理器
    async def start(self, event_engine, clock_engine) -> None:
        """启动配置管理器，注册配置检查任务"""
        if self._event_engine or self._clock_engine:
            logger.warning("配置管理器已经在运行")
            return

        self._event_engine = event_engine
        self._clock_engine = clock_engine

        # 加载所有配置文件
        await self._load_all_configs()

        # 获取检查间隔配置
        self.config_check_interval = self.configs["system"]["config_check_interval"]

        # 向事件引擎订阅配置检查事件
        self._event_engine.subscribe("config_check", self._check_config_changes)

        # 向时钟引擎注册配置检查任务
        self._config_check_task_id = self._clock_engine.register_task(
            name="config_check",
            event_type="config_check",
            interval=self.config_check_interval,
            event_data={"source": "config_manager"}
        )

        logger.info("配置管理器启动成功")

    # 对外开放：停止配置管理器
    async def stop(self) -> None:
        """停止配置管理器"""
        if not self._event_engine and not self._clock_engine:
            logger.warning("配置管理器已经停止")
            return

        # 取消配置检查任务
        if self._config_check_task_id and self._clock_engine:
            self._clock_engine.unregister_task(self._config_check_task_id)
            self._config_check_task_id = None

        # 取消事件订阅
        if self._event_engine:
            self._event_engine.unsubscribe_by_handler("config_check", self._check_config_changes)

        # 清理组件引用
        self._event_engine = None
        self._clock_engine = None

        logger.info("配置管理器已停止")

    # 内部方法：从 system.json 加载配置文件列表
    def _load_config_files_list(self) -> Dict[str, str]:
        """从 system.json 文件加载配置文件列表"""
        system_config_file = self.config_dir / "system.json"
        
        if not system_config_file.exists():
            logger.warning("system.json 文件不存在，使用默认配置文件列表")
            return {
                "system": "system.json",
                "email": "email.json", 
                "strategy": "strategy.json",
                "data_engine": "data_engine.json"
            }
        
        try:
            with open(system_config_file, 'r', encoding='utf-8') as f:
                system_config = json.load(f)
            
            config_files = system_config.get("config_files", {})
            if not config_files:
                logger.warning("system.json 中未找到 config_files 配置，使用默认配置文件列表")
                return {
                    "system": "system.json",
                    "email": "email.json",
                    "strategy": "strategy.json", 
                    "data_engine": "data_engine.json"
                }
            
            logger.info(f"从 system.json 加载配置文件列表: {list(config_files.keys())}")
            return config_files
            
        except Exception as e:
            logger.error(f"加载 system.json 失败: {e}，使用默认配置文件列表")
            return {
                "system": "system.json",
                "email": "email.json",
                "strategy": "strategy.json",
                "data_engine": "data_engine.json"
            }

    # 内部方法：加载所有配置文件
    async def _load_all_configs(self) -> None:
        """加载所有配置文件"""
        logger.info("开始加载所有配置文件")

        # 从 system.json 获取配置文件列表
        config_files = self._load_config_files_list()

        for config_name, filename in config_files.items():
            config_file = self.config_dir / filename

            if config_file.exists():
                try:
                    # 读取配置文件
                    with open(config_file, 'r', encoding='utf-8') as f:
                        config_data = json.load(f)

                    # 保存配置和文件路径
                    self.configs[config_name] = config_data
                    self.config_files[config_name] = config_file

                    # 计算文件哈希
                    file_hash = self._calculate_file_hash(config_file)
                    self.config_hashes[config_name] = file_hash

                    logger.info(f"配置文件加载成功: {config_name}")

                except Exception as e:
                    logger.error(f"加载配置文件失败: {config_name}, 错误: {e}")
                    # 使用默认配置
                    # self.configs[config_name] = self._get_default_config(config_name)
                    # self.config_files[config_name] = config_file
            else:
                logger.warning(f"配置文件不存在: {config_file}")
                # 使用默认配置
                # self.configs[config_name] = self._get_default_config(config_name)
                # self.config_files[config_name] = config_file

        logger.info(f"配置文件加载完成，共加载 {len(self.configs)} 个配置")

    # 对外开放（注册用）：检查配置变更
    async def _check_config_changes(self, event_data=None) -> None:
        """检查配置变更 - 定时任务回调"""
        """
        通过计算文件哈希值判断文件是否修改
        如有修改，则调用重加载方法读取文件并存储、发布对应配置修改事件
        """
        logger.debug("开始检查配置变更")

        for config_name, config_file in self.config_files.items():
            if not config_file.exists():
                continue

            try:
                # 计算当前文件哈希
                current_hash = self._calculate_file_hash(config_file)
                old_hash = self.config_hashes.get(config_name)

                # 检查是否有变更
                if old_hash != current_hash:
                    logger.info(f"检测到配置文件变更: {config_name}")

                    # 重新加载配置
                    if self._reload_config(config_name,current_hash):
                        logger.info(f"配置文件变更处理成功: {config_name}")
                    else:
                        logger.error(f"配置文件变更处理失败: {config_name}")

            except Exception as e:
                logger.error(f"检查配置文件变更失败: {config_name}, 错误: {e}")

    # 内部方法：重新加载配置文件
    def _reload_config(self, config_name: str,new_hash:str) -> bool:
        """重新加载指定配置文件"""
        """
        调用该方法前，理应已经对文件存在性、是否修改进行判断后再决定调用
        """
        try:
            # 确保程序健壮性，进行文件加载前做：配置文件路径检查
            config_file = self.config_files[config_name]
            if not config_file.exists():
                logger.error(f"配置文件不存在: {config_file}")
                return False

            # 读取配置文件
            with open(config_file, 'r', encoding='utf-8') as f:
                new_config = json.load(f)

            # 配置有变更
            old_config = self.configs.get(config_name)
            self.configs[config_name] = new_config
            self.config_hashes[config_name] = new_hash

            logger.info(f"配置文件重新加载成功: {config_name}")

            # 发布配置变更事件
            if self._event_engine:
                asyncio.create_task(
                    self._publish_config_change_event(config_name, old_config, new_config)
                )

            return True

        except Exception as e:
            logger.error(f"重新加载配置文件失败: {config_name}, 错误: {e}")
            return False

    # 内部方法：计算文件哈希值，用于判断文件是否修改
    @staticmethod
    def _calculate_file_hash(file_path: Path) -> str:
        """计算文件哈希值"""
        try:
            with open(file_path, 'rb') as f:
                file_content = f.read()
                return hashlib.md5(file_content).hexdigest()
        except Exception as e:
            logger.error(f"计算文件哈希失败: {file_path}, 错误: {e}")
            return ""

    # 内部方法：发布事件用
    async def _publish_config_change_event(self, config_name: str, old_value: Any, new_value: Any) -> None:
        """发布配置变更事件"""
        if not self._event_engine:
            logger.warning("事件引擎未初始化，无法发布配置变更事件")
            return

        try:
            event_data = {
                "config_name": config_name,
                # "old_config": old_value,
                "new_config": new_value,
                "timestamp": datetime.now().isoformat(),
                "source": "config_manager"
            }

            await self._event_engine.publish(f"{config_name}_config_changed", event_data)
            logger.info(f"配置变更事件发布成功: {config_name}_config_changed")

        except Exception as e:
            logger.error(f"发布配置变更事件失败: {config_name}, 错误: {e}")

    # 对外开放：查询配置总数
    def get_config_count(self) -> int:
        """获取配置总数"""
        return len(self.configs)

    # 对外开放：查询配置接口
    def get_config(self, config_name: str) -> Any:
        """查询配置"""
        if config_name not in self.configs:
            logger.warning(f"配置不存在: {config_name}")
            return None

        return self.configs[config_name]

    # def get_config_names(self) -> list[str]:
    #     """获取所有配置名称"""
    #     return list(self.configs.keys())

    # def get_config_file_path(self, config_name: str) -> Optional[Path]:
    #     """获取配置文件路径"""
    #     return self.config_files.get(config_name)
    #
    # def is_config_loaded(self, config_name: str) -> bool:
    #     """检查配置是否已加载"""
    #     return config_name in self.configs
    # def _get_default_config(self, config_name: str) -> Dict[str, Any]:
    #     """获取默认配置"""
    #     default_configs = {
    #         "system": {
    #             "name": "yjQuant v3.0",
    #             "version": "3.0.0",
    #             "debug": False,
    #             "log_level": "INFO",
    #             "config_check_interval": 60
    #         },
    #         "email": {
    #             "enabled": False,
    #             "smtp_server": "localhost",
    #             "smtp_port": 587,
    #             "username": "",
    #             "password": "",
    #             "from_email": "",
    #             "to_emails": []
    #         },
    #         "strategy": {
    #             "enabled": False,
    #             "strategies": []
    #         },
    #         "data_engine": {
    #             "redis": {
    #                 "host": "localhost",
    #                 "port": 6379,
    #                 "db": 0,
    #                 "password": ""
    #             },
    #             "data_sources": []
    #         }
    #     }
    #
    #     return default_configs.get(config_name, {})