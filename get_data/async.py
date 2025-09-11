# _*_ coding: UTF-8 _*_
# @Time : 2025/8/28 14:47
# @Author : yyj
# @Email : 1410959463@qq.com
# @File : async.py
# @Project : yjQuant

import asyncio
import ccxt.async_support as ccxt
import redis.asyncio as aioredis  # 推荐写法，aioredis已合并进redis-py
import json
from datetime import datetime, timedelta, timezone

# 1. 创建交易所对象（以Binance为例）

REDIS_URI = "redis://47.108.199.234:6379"
REDIS_EXPIRE = 3 * 24 * 60  # 分钟数

# 代理与HTTP配置
PROXY_CONFIG = {
    'http': 'http://127.0.0.1:7890',
    'https': 'http://127.0.0.1:7890',
}
HTTP_TIMEOUT_MS = 20000
MAX_RETRIES = 3
RETRY_BASE_DELAY_SEC = 0.8

BINANCE_SYMBOLS = ['BTC/USDT', 'ETH/USDT']
GATEIO_SYMBOLS = ['BTC/USDT', 'DOGE/USDT']


def get_last_minute_timestamp():
    """
    输入：无
    输出：上一个完整分钟的UTC毫秒时间戳
    主要逻辑：取当前UTC时间-1分钟，并抹去秒与微秒
    """
    now = datetime.now(timezone.utc)
    last_min = now - timedelta(minutes=1)
    ts = int(last_min.replace(second=0, microsecond=0).timestamp() * 1000)
    return ts


def create_exchange(exchange_id: str):
    """
    输入：exchange_id in {"binance", "gateio"}
    输出：已设置为现货、启用限速、代理与超时的 ccxt 异步实例
    主要逻辑：spot 模式 + 代理 + 超时
    """
    common_kwargs = {
        'enableRateLimit': True,
        'sandbox': False,
        'proxies': PROXY_CONFIG,
        'aiohttp_proxy': PROXY_CONFIG['https'],
        'adjustForTimeDifference': True,
        'timeout': HTTP_TIMEOUT_MS,
    }
    if exchange_id == 'binance':
        return ccxt.binance({**common_kwargs, 'options': {'defaultType': 'spot'}})
    elif exchange_id == 'gateio':
        return ccxt.gateio({**common_kwargs, 'options': {'defaultType': 'spot'}})
    else:
        raise ValueError("unsupported exchange id")


async def async_retry(fn, *args, retries: int = MAX_RETRIES, base_delay: float = RETRY_BASE_DELAY_SEC, **kwargs):
    """
    输入：协程函数及其参数、最大重试次数、基础退避
    输出：函数成功返回值；重试后仍失败则抛出最后一次异常
    主要逻辑：指数退避重试，适配临时网络波动或交易所限流
    """
    for attempt in range(1, retries + 1):
        try:
            return await fn(*args, **kwargs)
        except Exception as e:
            if attempt == retries:
                raise
            delay = base_delay * (2 ** (attempt - 1))
            print(f"retry {attempt}/{retries} after error: {repr(e)}; sleeping {delay:.2f}s")
            await asyncio.sleep(delay)
            return None
    return None


async def fetch_ohlcv(exchange, symbol, timeframe, since):
    try:
        ohlcv = await exchange.fetch_ohlcv(symbol, timeframe=timeframe, since=since, limit=1)
        return symbol, ohlcv[0] if ohlcv else None
    except Exception as e:
        import traceback
        print(f"{exchange.id} {symbol} error: {repr(e)}")
        traceback.print_exc()
        return symbol, None


async def fetch_all_klines(exchange_id, symbols, timeframe, since):
    """
    输入：交易所ID、符号列表、周期、起始时间
    输出：[(symbol, kline或None), ...]
    主要逻辑：创建并预加载市场，批量并发抓取K线
    """
    exchange = create_exchange(exchange_id)
    try:
        # 预加载市场，带重试
        await async_retry(exchange.load_markets)
        # 异步批量请求所有交易对
        tasks = [fetch_ohlcv(exchange, symbol, timeframe, since) for symbol in symbols]
        results = await asyncio.gather(*tasks)
        return results
    finally:
        await exchange.close()


async def main():
    ts = get_last_minute_timestamp()
    # asyncio.gather并发执行多个任务，完成先后顺序可能不固定，但会确保返回结果的顺序与传入的协程顺序一致
    results = await asyncio.gather(
        fetch_all_klines('binance', BINANCE_SYMBOLS, '1m', ts),
        fetch_all_klines('gateio', GATEIO_SYMBOLS, '1m', ts)
    )
    all_klines = []
    for ex, klines in zip(['binance', 'gateio'], results):
        for symbol, kline in klines:
            if kline:
                all_klines.append((ex, symbol, kline))
    redis = await aioredis.from_url(REDIS_URI, max_connections=10, decode_responses=True)
    try:
        pipe = redis.pipeline()
        # 以毫秒为单位的时间窗口
        window_ms = REDIS_EXPIRE * 60 * 1000
        cutoff = ts - window_ms
        for exchange, symbol, kline in all_klines:
            key = f'kline:{exchange}:{symbol.replace("/", "_").replace(":", "_")}'
            # 转为json字符串
            value = json.dumps({
                'timestamp': kline[0],
                'open': kline[1],
                'high': kline[2],
                'low': kline[3],
                'close': kline[4],
                'volume': kline[5]
            })
            pipe.zadd(key, {value: kline[0]})
            pipe.zremrangebyscore(key, 0, cutoff)
        if all_klines:
            await pipe.execute()
    finally:
        await redis.aclose()
        print(f"已批量写入{len(all_klines)}条数据到Redis")


if __name__ == "__main__":
    asyncio.run(main())
