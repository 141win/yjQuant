# _*_ coding: UTF-8 _*_
# @Time : 2025/8/28 11:33
# @Author : yyj
# @Email : 1410959463@qq.com
# @File : fetch_data.py
# @Project : yjQuant
import ccxt
import pandas as pd
import requests
# 1. 创建交易所对象（以Binance为例）

session = requests.Session()
session.proxies = {
    'http': 'socks5://127.0.0.1:7890',
    'https': 'socks5://127.0.0.1:7890',
}

exchange = ccxt.binance({'session': session})

# 2. 设置交易对和时间周期
symbol = 'BTC/USDT'  # 交易对
timeframe = '1m'  # 1分钟K线

# 3. 获取K线数据（可指定limit，最大1000）
ohlcv = exchange.fetch_ohlcv(symbol, timeframe=timeframe, limit=100)

# 4. 转换为DataFrame并设置列名
df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
# df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
print(df)

# # 转为北京时间并去掉时区
# df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms').dt.tz_localize('UTC').dt.tz_convert('Asia/Shanghai').dt.tz_localize(None)
# print(df)
#
# df.to_csv("BTC_USDT_.csv", index=False)
