import asyncio
import json
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Sequence, Tuple

import ccxt.async_support as ccxt

try:
    import asyncpg  # Async PostgreSQL driver
except Exception:  # pragma: no cover
    asyncpg = None  # The user will install dependencies as needed

# =============== User-fill configuration ===============
# Add/modify exchanges and symbols as needed, e.g.:
# {
#   'binance': ['BTC/USDT', 'ETH/USDT'],
#   'gateio': ['BTC/USDT', 'DOGE/USDT']
# }
EXCHANGES_TO_SYMBOLS: Dict[str, List[str]] = {
    'binance': ['ROSE/USDT', 'GAS/USDT', "BEL/USDT"],
    'gateio': ['WXT/USDT', 'GAS/USDT', 'STX/USDT'],
}

# PostgreSQL connection placeholders — fill these in
POSTGRES_CONFIG = {
    'host': '127.0.0.1',  # e.g. '127.0.0.1'
    'port': 5432,  # e.g. 5432
    'user': 'postgres',
    'password': '123456',
    'database': 'Cryptocurrency',
}

# Target table for 1m pairs
POSTGRES_TABLE_1M = '_1m'

# Optional proxy settings for ccxt (fill if needed)
PROXY_CONFIG = {
    'http': 'http://127.0.0.1:7890',
    'https': 'http://127.0.0.1:7890',
}

# Networking/retry configs
HTTP_TIMEOUT_MS = 20000
MAX_RETRIES = 3
RETRY_BASE_DELAY_SEC = 0.8


# 创建交易所
def create_exchange(exchange_id: str):
    """
    Create an async ccxt instance for spot market with optional proxy and timeout.
    """
    common_kwargs = {
        'enableRateLimit': True,
        'sandbox': False,
        'adjustForTimeDifference': True,
        'timeout': HTTP_TIMEOUT_MS,
    }
    # apply proxies if provided
    if PROXY_CONFIG:
        common_kwargs['proxies'] = PROXY_CONFIG
        # aiohttp proxy for ccxt
        if 'https' in PROXY_CONFIG:
            common_kwargs['aiohttp_proxy'] = PROXY_CONFIG['https']

    if exchange_id == 'binance':
        return ccxt.binance({**common_kwargs, 'options': {'defaultType': 'spot'}})
    elif exchange_id == 'gateio':
        return ccxt.gateio({**common_kwargs, 'options': {'defaultType': 'spot'}})
    else:
        # Add more exchanges if needed
        raise ValueError(f"Unsupported exchange id: {exchange_id}")


# 异步重试
async def async_retry(fn, *args, retries: int = MAX_RETRIES, base_delay: float = RETRY_BASE_DELAY_SEC, **kwargs):
    """
    Exponential backoff retry helper for async functions.
    """
    for attempt in range(1, retries + 1):
        try:
            return await fn(*args, **kwargs)
        except Exception as e:  # pragma: no cover - network variability
            if attempt == retries:
                raise
            delay = base_delay * (2 ** (attempt - 1))
            print(f"retry {attempt}/{retries} after error: {repr(e)}; sleeping {delay:.2f}s")
            await asyncio.sleep(delay)
    return None

# 获取一个交易所中的所有交易对
async def fetch_all_klines(exchange_id: str, symbols: Sequence[str], timeframe: str, since_ms: int):
    exchange = create_exchange(exchange_id)
    try:
        await async_retry(exchange.load_markets)
        tasks = [fetch_ohlcv(exchange, symbol, timeframe, since_ms) for symbol in symbols]
        results = await asyncio.gather(*tasks)
        return results
    finally:
        await exchange.close()

# 获取一个交易对数据
async def fetch_ohlcv(exchange, symbol: str, timeframe: str, since_ms: int) -> Tuple[str, List[float] | None]:
    try:
        ohlcv = await exchange.fetch_ohlcv(symbol, timeframe=timeframe, since=since_ms, limit=1)
        return symbol, (ohlcv[0] if ohlcv else None)
    except Exception as e:  # pragma: no cover - network variability
        print(f"{exchange.id} {symbol} error: {repr(e)}")
        return symbol, None


# 异步插入数据——pg数据库
async def insert_rows_asyncpg(rows: Sequence[Tuple], table: str):
    """
    Insert rows into PostgreSQL using asyncpg.

    Expected row tuple order:
    (ts_china, open, high, low, close, volume, exchange, symbol)
    """
    if asyncpg is None:
        raise RuntimeError("asyncpg is not installed. Please install it to use PostgreSQL insertion.")

    sql = (
        f"INSERT INTO {table} "
        "(timestamp, open, high, low, close, volume, exchange, pair) "
        "VALUES ($1, $2, $3, $4, $5, $6, $7, $8)"
    )

    pool = await asyncpg.create_pool(**POSTGRES_CONFIG, min_size=1, max_size=4)
    try:
        async with pool.acquire() as conn:
            async with conn.transaction():
                await conn.executemany(sql, rows)
    finally:
        await pool.close()


# 获取当前时间的前一分钟时间戳，毫秒单位
def get_last_minute_timestamp_ms() -> int:
    """
    Returns the UTC timestamp (ms) of the last completed minute.
    """
    now = datetime.now(timezone.utc)
    last_min = now - timedelta(minutes=1)
    ts_ms = int(last_min.replace(second=0, microsecond=0).timestamp() * 1000)
    return ts_ms


# ms单位时间戳转为中国时间
def ms_to_china_datetime(ms: int) -> datetime:
    """
    Convert epoch milliseconds to Asia/Shanghai timezone-aware datetime.
    """
    # China Standard Time is UTC+8, no DST
    utc_dt = datetime.fromtimestamp(ms / 1000, tz=timezone.utc)
    china_dt = utc_dt.astimezone(timezone(timedelta(hours=8))).replace(tzinfo=None)
    return china_dt


async def main():
    if not EXCHANGES_TO_SYMBOLS:
        print("Please fill EXCHANGES_TO_SYMBOLS with your exchanges and symbols.")
        return

    since_ms = get_last_minute_timestamp_ms()

    # Fetch concurrently per-exchange
    fetch_tasks = [
        fetch_all_klines(ex_id, symbols, '1m', since_ms)
        for ex_id, symbols in EXCHANGES_TO_SYMBOLS.items()
    ]
    per_exchange_results = await asyncio.gather(*fetch_tasks)

    # Flatten and transform
    rows_to_insert: List[Tuple] = []
    for (exchange_id, symbols), results in zip(EXCHANGES_TO_SYMBOLS.items(), per_exchange_results):
        for symbol, kline in results:
            if not kline:
                continue
            # kline = [t, o, h, l, c, v]
            ts_china = ms_to_china_datetime(int(kline[0]))
            o, h, l, c, v = kline[1], kline[2], kline[3], kline[4], kline[5]
            rows_to_insert.append((ts_china, o, h, l, c, v, exchange_id, symbol))

    if not rows_to_insert:
        print("No rows fetched. Nothing to insert.")
        return

    # Insert into PostgreSQL
    print(f"Preparing to insert {len(rows_to_insert)} rows into table '{POSTGRES_TABLE_1M}'.")
    await insert_rows_asyncpg(rows_to_insert, POSTGRES_TABLE_1M)
    print(f"Inserted {len(rows_to_insert)} rows into PostgreSQL table '{POSTGRES_TABLE_1M}'.")


if __name__ == "__main__":
    asyncio.run(main())
    # print(ms_to_china_datetime(1662316800000))

