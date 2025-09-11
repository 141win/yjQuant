"""
AB下跌策略实现

基于 StrategyTemplate，实现 on_realtime 入口：
输入 df 列要求（最小集）：
- exchange: 交易所字符串
- symbol: 交易对字符串
- ts_B: 当前时刻毫秒时间戳（或秒，需与 A 一致单位）
- price_B: 当前最新价

此外，假设外部环境已提供：
- dfA: 满足 A 条件的 DataFrame，列包含：exchange, symbol, ts_A, low_A, close_A
- dfLast: 上次满足条件的触发价 DataFrame，列包含：exchange, symbol, last_trigger_price

本文件中通过占位方法 _load_prefilter_A() 与 _load_last_trigger()
模拟数据获取，真实环境由调用方注入或替换。
"""

import logging
import pandas as pd
import numpy as np
from typing import Optional


from yjQuant_v3.core.strategy_template import StrategyTemplate, StrategyResult

logger = logging.getLogger(__name__)


class ABDropStrategy(StrategyTemplate):
    """AB 下跌策略"""

    def __init__(self, name: str, description: str, timeframe: str, context):
        super().__init__(name, description, timeframe, context)
        self.parameters = self.context.strategy_config.get("parameters")
        self.redis_manager = context.redis_manager  # 从上下文获取Redis管理器
        self.db_manager = context.db_manager

    def _get_interval_string(self) -> str:
        """
        根据时间框架获取对应的PostgreSQL时间间隔字符串
        
        Args:
            # timeframe: 时间框架，支持 "1h", "2h", "1d"
            
        Returns:
            PostgreSQL时间间隔字符串
        """
        interval_map = {
            "1h": "1 hour",
            "2h": "2 hours", 
            "1d": "1 day"
        }
        return interval_map.get(self.timeframe, "1 hour")

    # ------------------ 核心入口 ------------------
    async def on_realtime(self, df: pd.DataFrame) -> Optional[StrategyResult]:
        """
        入参 df 列: ['timestamp','exchange','symbol','price']
        - timestamp: B 时刻时间戳（毫秒或秒，但需与 A 一致单位；若不一致请在外部转换）
        - exchange: 交易所
        - symbol: 交易对
        - price: 当前价格（B 价格）

        依赖（占位伪代码）:
        - dfA: 预筛过的 A 候选（DB侧已实现 A 的全部条件：虚降、前6与AB区间 exist 条件）
                列至少包含: ['exchange','symbol','ts_A','low_A','close_A']
                如未预筛，需在DB侧按前述SQL生成
        - recent_min_8: 最近8个单位时间最低价（CAGG/函数）
                列: ['exchange','symbol','min_low_8']
        - recent_max_close_10: 最近10个单位时间收盘价最大值（CAGG/函数）
                列: ['exchange','symbol','max_close_10']
        - last_trigger: 上次满足条件价格（Redis/DB）
                列: ['exchange','symbol','last_trigger_price']
        """
        # 基本校验，df 不为空
        if df is None or df.empty:
            return StrategyResult(self.name, self.description, self.timeframe, pd.DataFrame())

        # 规范列名
        b = df.rename(columns={'timestamp': 'ts_B', 'price': 'price_B'})[
            ['exchange', 'symbol', 'ts_B', 'price_B']].copy()

        # ===== 优先处理触发价格逻辑（提前判断，避免无效计算） =====
        last_trigger = await self._load_last_trigger()  # 占位
        has_last_trigger = last_trigger is not None and not last_trigger.empty
        
        if not has_last_trigger:
            # 没有上次价格：用当前价格作为"假"的上次价格，直接存储并返回空结果
            logger.info(f"策略 {self.name} 没有上次触发价格，使用当前价格作为初始触发价格")
            await self._save_initial_trigger_prices(b)
            return StrategyResult(self.name, self.description, self.timeframe, pd.DataFrame())
        
        # 有上次价格：先进行触发价格判断，不满足直接返回
        b_with_trigger = b.merge(last_trigger, on=['exchange', 'symbol'], how='left')
        
        # 触发价格条件判断
        is_binance = b_with_trigger['exchange'].eq('binance')
        mask_trigger_binance = is_binance & (b_with_trigger['price_B'] < b_with_trigger['last_trigger_price'] * self.parameters.get("last_price_threshold"))
        mask_trigger_nonbin = (~is_binance) & (b_with_trigger['price_B'] < b_with_trigger['last_trigger_price'] * self.parameters.get("non_binance_last_price_threshold"))
        mask_trigger = mask_trigger_binance | mask_trigger_nonbin
        
        # 不满足触发价格条件，直接返回空结果
        b_filtered = b_with_trigger.loc[mask_trigger].copy()
        if b_filtered.empty:
            return StrategyResult(self.name, self.description, self.timeframe, pd.DataFrame())
        
        # 更新 b 为过滤后的数据
        b = b_filtered[['exchange', 'symbol', 'ts_B', 'price_B']].copy()

        # ===== 从数据库获取策略数据 =====
        # A 候选：DB预筛（已包含：A虚降>=2%、A前6 exist 条件、AB区间 exist 条件）
        dfA = await self._load_prefilter_a()

        # 最近8个单位时间最低价
        recent_min_8 = await self._load_recent_min_8()

        # 最近10个单位时间收盘价最大值
        recent_max_close_10 = await self._load_recent_max_close_10()

        # 缺数据直接返回空
        if dfA is None or dfA.empty:
            logger.error(f"策略 {self.name} 没有A候选数据")
            return StrategyResult(self.name, self.description, self.timeframe, pd.DataFrame())

        # ===== 与 A 合并并做时间与首个价格条件 =====
        merged = b.merge(dfA, on=['exchange', 'symbol'], how='inner')  # 会产生 (B x 多个 A)
        if merged.empty:
            logger.error(f"策略 {self.name} 没有满足条件的A数据")
            return StrategyResult(self.name, self.description, self.timeframe, pd.DataFrame())

        # 时间窗：A 和 B 间隔不超过 20 个单位时间
        max_gap = int(self.parameters.get("max_gap_units") * self.parameters.get("unit_ms"))  # 若你的 A/B 时间单位为“小时=秒”，请将 unit_ms 改为 3600
        gap = merged['ts_B'] - merged['ts_A']
        mask_time = gap.between(0, max_gap)

        # 价格条件1：B 价格 <= A最低价的96%
        mask_b_vs_a = merged['price_B'] <= (merged['low_A'] * 0.96)

        merged = merged.loc[mask_time & mask_b_vs_a].copy()
        if merged.empty:
            logger.error(f"策略 {self.name} 没有满足时间窗和价格条件的A数据")
            return StrategyResult(self.name, self.description, self.timeframe, pd.DataFrame())

        # 对每个 (exchange, symbol, ts_B) 选“距离 B 最近的 A”
        merged['gap_abs'] = (merged['ts_B'] - merged['ts_A']).abs()
        merged.sort_values(['exchange', 'symbol', 'ts_B', 'gap_abs'], inplace=True)
        merged = merged.groupby(['exchange', 'symbol', 'ts_B'], as_index=False).first()

        # ===== 合并最近8个单位最低价 & 最近10收盘最大值 =====
        if recent_min_8 is not None and not recent_min_8.empty:
            merged = merged.merge(recent_min_8, on=['exchange', 'symbol'], how='left')
        else:
            merged['min_low_8'] = np.nan

        if recent_max_close_10 is not None and not recent_max_close_10.empty:
            merged = merged.merge(recent_max_close_10, on=['exchange', 'symbol'], how='left')
        else:
            merged['max_close_10'] = np.nan

        # 条件：当前价格 <= 最近8个单位时间内的最低价
        mask_recent_min = merged['price_B'] <= merged['min_low_8']

        # 条件：当前价格 <= 最近10个单位时间收盘价的最大值的93%
        mask_recent_max_close = merged['price_B'] <= (merged['max_close_10'] * 0.93)

        merged = merged.loc[mask_recent_min & mask_recent_max_close].copy()
        if merged.empty:
            return StrategyResult(self.name, self.description, self.timeframe, pd.DataFrame())

        # 计算完成后统一更新触发价格
        await self._save_trigger_prices(merged)

        # 输出列（只保留必要字段）
        result_df = merged[['exchange', 'symbol', 'price_B']].copy()
        # ['exchange', 'symbol', 'price']
        result_df.rename(columns={'price_B': 'price'}, inplace=True)

        return StrategyResult(self.name, self.description, self.timeframe, result_df)

    # ------------------ 数据库数据加载 ------------------
    async def _load_prefilter_a(self) -> pd.DataFrame:
        """从数据库获取A候选数据"""
        try:
            # 获取时间间隔字符串
            interval_str = self._get_interval_string()
            
            # 定义A候选数据的SQL查询
            sql = f"""
            WITH params AS (
              SELECT
                date_trunc('hour', now()) - interval '{interval_str}' AS last_closed_bucket,
                20::int AS gap_units,
                6::int  AS lookback_units
            ),
            base AS (
              SELECT
                c.exchange,
                c.pair,
                c.bucket,
                c.open, c.high, c.low, c.close,
                LAG(c.close) OVER (PARTITION BY c.exchange, c.pair ORDER BY c.bucket) AS prev_close,
                (c.close / NULLIF(LAG(c.close) OVER (PARTITION BY c.exchange, c.pair ORDER BY c.bucket), 0) - 1) AS ret
              FROM cagg_{self.timeframe} c
              JOIN params p ON c.bucket <= p.last_closed_bucket
              WHERE c.bucket >= p.last_closed_bucket - (p.gap_units - 1) * interval '{interval_str}'
            ),
            a_candidates AS (
              SELECT
                a.exchange,
                a.pair,
                a.bucket AS ts_a,
                a.low    AS low_a,
                a.close  AS close_a
              FROM base a
              JOIN params p ON TRUE
              WHERE
                -- A 虚降 >= 2%（按你的定义：open>close时用close-low，否则用open-low）
                (
                  (CASE WHEN a.open > a.close THEN (a.close - a.low)
                        ELSE (a.open - a.low)
                   END) / NULLIF(a.open, 0)
                ) >= 0.02

                -- A 之前 6 个单位时间内，存在一条：跌幅<=-2% 且该时 close > A.close
                AND EXISTS (
                  SELECT 1
                  FROM base b_prev
                  WHERE b_prev.exchange = a.exchange
                    AND b_prev.pair     = a.pair
                    AND b_prev.bucket  >= a.bucket - p.lookback_units * interval '{interval_str}'
                    AND b_prev.bucket  <  a.bucket
                    AND b_prev.ret     <= -0.02
                    AND b_prev.close    > a.close
                )

                -- A 与"最近闭合时间"之间（最多 20 个单位时间），存在一条：跌幅<=-2% 且 A.close > 该时 close
                AND EXISTS (
                  SELECT 1
                  FROM base b_mid
                  WHERE b_mid.exchange = a.exchange
                    AND b_mid.pair     = a.pair
                    AND b_mid.bucket   >  a.bucket
                    AND b_mid.bucket  <= LEAST(p.last_closed_bucket, a.bucket + p.gap_units * interval '{interval_str}')
                    AND b_mid.ret     <= -0.02
                    AND a.close        > b_mid.close
                )
            )
            SELECT exchange, pair, ts_a, low_a, close_a
            FROM a_candidates
            ORDER BY exchange, pair, ts_a DESC
            """
            
            # 执行查询（SQL已内联时间框架与间隔）
            data = await self.db_manager.execute_query(sql)
            
            if not data:
                return pd.DataFrame(columns=['exchange', 'symbol', 'ts_A', 'low_A', 'close_A'])
            
            # 转换数据格式
            processed_data = []
            for item in data:
                processed_data.append({
                    "exchange": item["exchange"],
                    "symbol": item["pair"].replace("_", "/"),
                    "ts_A": item["ts_a"],
                    "low_A": item["low_a"],
                    "close_A": item["close_a"]
                })
            
            # 转换为DataFrame
            df = pd.DataFrame(processed_data)
            logger.debug(f"加载A候选数据成功，共 {len(df)} 条")
            return df
            
        except Exception as e:
            logger.error(f"加载A候选数据失败: {e}")
            return pd.DataFrame(columns=['exchange', 'symbol', 'ts_A', 'low_A', 'close_A'])

    async def _load_recent_min_8(self) -> pd.DataFrame:
        """从数据库获取最近8个单位时间的最低价"""
        try:
            # 获取时间间隔字符串
            interval_str = self._get_interval_string()
            
            # 定义最近8个单位时间最低价的SQL查询
            sql = f"""
            WITH last_closed AS (
              SELECT date_trunc('hour', now()) - interval '{interval_str}' AS last_closed_bucket
            ),
            range AS (
              SELECT
                (SELECT last_closed_bucket FROM last_closed) - (i * interval '{interval_str}') AS bucket
              FROM generate_series(0, 7) AS s(i)
            )
            SELECT c.exchange, c.pair, MIN(c.low) AS min_low_8
            FROM cagg_{self.timeframe} c
            JOIN range r ON c.bucket = r.bucket
            GROUP BY c.exchange, c.pair
            ORDER BY c.exchange, c.pair
            """
            
            # 执行查询（SQL已内联时间框架与间隔）
            data = await self.db_manager.execute_query(sql)
            
            if not data:
                return pd.DataFrame(columns=['exchange', 'symbol', 'min_low_8'])
            
            # 转换数据格式
            processed_data = []
            for item in data:
                processed_data.append({
                    "exchange": item["exchange"],
                    "symbol": item["pair"].replace("_", "/"),
                    "min_low_8": item["min_low_8"]
                })
            
            # 转换为DataFrame
            df = pd.DataFrame(processed_data)
            logger.debug(f"加载最近8个单位时间最低价成功，共 {len(df)} 条")
            return df
            
        except Exception as e:
            logger.error(f"加载最近8个单位时间最低价失败: {e}")
            return pd.DataFrame(columns=['exchange', 'symbol', 'min_low_8'])

    async def _load_recent_max_close_10(self) -> pd.DataFrame:
        """从数据库获取最近10个单位时间收盘价的最大值"""
        try:
            # 获取时间间隔字符串
            interval_str = self._get_interval_string()
            
            # 定义最近10个单位时间收盘价最大值的SQL查询
            sql = f"""
            WITH last_closed AS (
              SELECT date_trunc('hour', now()) - interval '{interval_str}' AS last_closed_bucket
            ),
            range AS (
              SELECT
                (SELECT last_closed_bucket FROM last_closed) - (i * interval '{interval_str}') AS bucket
              FROM generate_series(0, 9) AS s(i)
            )
            SELECT c.exchange, c.pair, MAX(c.close) AS max_close_10
            FROM cagg_{self.timeframe} c
            JOIN range r ON c.bucket = r.bucket
            GROUP BY c.exchange, c.pair
            ORDER BY c.exchange, c.pair
            """
            
            # 执行查询（SQL已内联时间框架与间隔）
            data = await self.db_manager.execute_query(sql)
            
            if not data:
                return pd.DataFrame(columns=['exchange', 'symbol', 'max_close_10'])
            
            # 转换数据格式
            processed_data = []
            for item in data:
                processed_data.append({
                    "exchange": item["exchange"],
                    "symbol": item["pair"].replace("_", "/"),
                    "max_close_10": item["max_close_10"]
                })
            
            # 转换为DataFrame
            df = pd.DataFrame(processed_data)
            logger.debug(f"加载最近10个单位时间收盘价最大值成功，共 {len(df)} 条")
            return df
            
        except Exception as e:
            logger.error(f"加载最近10个单位时间收盘价最大值失败: {e}")
            return pd.DataFrame(columns=['exchange', 'symbol', 'max_close_10'])

    # ------------------ redis数据存取 ------------------
    async def _load_last_trigger(self) -> pd.DataFrame:
        """从Redis加载上次满足条件的触发价格"""
        try:
            # 加载上次满足条件的价格
            trigger_data = await self.redis_manager.get_strategy_trigger_prices(self.name)
            if not trigger_data:
                return pd.DataFrame(columns=["exchange", "symbol", "last_trigger_price"])
            
            # 转换为DataFrame 字典转dataframe
            df = pd.DataFrame(trigger_data)
            if 'price' in df.columns:
                df = df.rename(columns={'price': 'last_trigger_price'})
            
            return df[["exchange", "symbol", "last_trigger_price"]]
            
        except Exception as e:
            logger.error(f"加载触发价格失败: {e}")
            return pd.DataFrame(columns=["exchange", "symbol", "last_trigger_price"])

    async def _save_initial_trigger_prices(self, b_df: pd.DataFrame) -> None:
        """保存初始触发价格（没有上次价格时使用当前价格）"""
        try:
            if b_df.empty:
                return
            
            # 直接构建触发价格数据，避免DataFrame操作
            # 先过滤缺失值，防止 NaN 转换为 int 时报错
            b_df = b_df.dropna(subset=["ts_B", "price_B"])
            trigger_data = []
            for _, row in b_df.iterrows():
                # 双重校验，避免非法值
                if pd.isna(row.get("ts_B")) or pd.isna(row.get("price_B")):
                    continue
                try:
                    ts_b_ms = int(float(row["ts_B"]))
                except Exception:
                    continue
                trigger_data.append({
                    "exchange": row["exchange"],
                    "symbol": row["symbol"],
                    "price": float(row["price_B"]),
                    "timestamp": ts_b_ms
                })
            # merged_df.rename(columns={'price_B': 'price', 'ts_B': 'timestamp'}, inplace=True)
            # trigger_data = merged_df.to_dict("records")
            
            # 直接存储到Redis（覆盖模式）
            await self.redis_manager.store_strategy_trigger_prices(self.name, trigger_data)
            logger.info(f"策略 {self.name} 初始化触发价格完成，共 {len(trigger_data)} 个交易对")
            
        except Exception as e:
            logger.error(f"保存初始触发价格失败: {e}")

    async def _save_trigger_prices(self, result_df: pd.DataFrame) -> None:
        """保存满足条件的触发价格到Redis（统一更新）"""
        try:
            if result_df.empty:
                return
            
            # 直接构建触发价格数据，避免DataFrame重命名操作
            # 先过滤缺失值，防止 NaN 转换为 int 时报错
            result_df = result_df.dropna(subset=["ts_B", "price_B"])
            trigger_data = []
            for _, row in result_df.iterrows():
                if pd.isna(row.get("ts_B")) or pd.isna(row.get("price_B")):
                    continue
                try:
                    ts_b_ms = int(float(row["ts_B"]))
                except Exception:
                    continue
                trigger_data.append({
                    "exchange": row["exchange"],
                    "symbol": row["symbol"],
                    "price": float(row["price_B"]),
                    "timestamp": ts_b_ms
                })

            # 统一存储到Redis（覆盖所有数据）
            await self.redis_manager.update_strategy_trigger_prices_batch(self.name, trigger_data)
            logger.info(f"策略 {self.name} 更新触发价格完成，共 {len(trigger_data)} 个交易对")
            
        except Exception as e:
            logger.error(f"保存触发价格失败: {e}")
