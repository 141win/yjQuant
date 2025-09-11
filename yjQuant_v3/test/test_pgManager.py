"""
测试能否通过Pg管理器把数据正常插入数据库
"""
"""
测试结果符合预期
"""
import logging
import sys

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('../../logs/yjquant.log', encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger(__name__)
import os

# 获取当前main.py文件所在目录
current_dir = os.path.dirname(os.path.abspath(__file__))
# 配置文件目录在yjQuant_v3/config
config_dir = os.path.join(current_dir, 'config')

if not os.path.exists(config_dir):
    logger.error(f"配置目录不存在: {config_dir}")
    raise FileNotFoundError(f"配置目录不存在: {config_dir}")

from yjQuant_v3.core.data_manager.pg_manager import PgManager

import asyncio

_shutdown_event = asyncio.Event()

import json

from datetime import datetime, timedelta, timezone


def _ms_to_china_datetime(ms: int) -> datetime:
    """
    Convert epoch milliseconds to Asia/Shanghai timezone-aware datetime.
    """
    # China Standard Time is UTC+8, no DST
    utc_dt = datetime.fromtimestamp(ms / 1000, tz=timezone.utc)
    china_dt = utc_dt.astimezone(timezone(timedelta(hours=8))).replace(tzinfo=None)
    return china_dt

def _get_last_minute_timestamp():
    """
    输入：无
    输出：上一个完整分钟的UTC毫秒时间戳
    主要逻辑：取当前UTC时间-1分钟，并抹去秒与微秒
    """
    now = datetime.now(timezone.utc)
    last_min = now - timedelta(minutes=1)
    ts = int(last_min.replace(second=0, microsecond=0).timestamp() * 1000)
    return ts


async def main():
    with open(r"/config/data_engine.json") as f:
        config = json.load(f)
    pg_manager_config = config.get("postgresql", {})
    pg_manager = PgManager(pg_manager_config)
    await pg_manager.start()
    await pg_manager.batch_store_klines(
        [("binance", "ROSE/USDT", [_ms_to_china_datetime(_get_last_minute_timestamp()), 110385.74, 110425.76, 110380.00, 110383.45, 5.77481])])
    await pg_manager.stop()
    # 等待关闭信号
    await _shutdown_event.wait()


if __name__ == '__main__':
    import asyncio

    asyncio.run(main())
