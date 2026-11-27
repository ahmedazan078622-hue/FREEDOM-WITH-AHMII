import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Set, Tuple

import numpy as np
import pandas as pd
import requests
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse
from pydantic import BaseModel

MARKET_TYPE = "BINANCE_SPOT_USDT"
BINANCE_ENDPOINTS = [
    "https://data-api.binance.vision",
    "https://api.binance.com",
]
OWNER_LABEL = "03254112787"
PKT_TZ = timezone(timedelta(hours=5))

SIGNAL_TTL_SECONDS = 60 * 60
ALERT_COOLDOWN_SECONDS = 900
SYMBOL_REFRESH_SECONDS = 900
MIN_AGE_DAYS_1D = 30
MIN_ACTIVE_PAIRS_PER_BASE = 3
TOP_GAINERS_LIMIT = 25

EXCLUDED_BASES = {
    "USDT", "USDC", "USDS", "BUSD", "FDUSD", "TUSD", "USDP", "DAI",
    "UST", "USTC", "USDE", "USD1", "PYUSD", "USDD", "EUR", "TRY",
    "BRL", "GBP", "AUD", "BIDR", "PAXG",
}

app = FastAPI(title="FREEDOM WITH AHMII")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

BASE_SIGNALS: List[Dict[str, Any]] = []
CUSTOM_SIGNALS: List[Dict[str, Any]] = []
ALERT_HISTORY: Dict[str, float] = {}
CPA_LAST_PRICE: Dict[str, float] = {}

CUSTOM_ENABLED: Dict[str, bool] = {
    "CV1": False,
    "CICT": False,
    "CHB": False,
    "CPA": False,
}

SCAN_STATUS: Dict[str, Any] = {
    "running": False,
    "last_cycle": None,
    "scanned": 0,
    "total": 0,
    "market": MARKET_TYPE,
    "working_url": BINANCE_ENDPOINTS[0],
    "owner": OWNER_LABEL,
}

_lock = threading.Lock()
_stop_event = threading.Event()


class CustomControlPayload(BaseModel):
    scanner: Optional[str] = None
    enabled: Optional[bool] = None
    clear_custom: bool = False


def format_pkt_time(dt_utc: datetime) -> str:
    return dt_utc.astimezone(PKT_TZ).strftime("%H:%M:%S PKT")


def prune_base_signals_locked(now_ts: float) -> None:
    global BASE_SIGNALS
    BASE_SIGNALS = [
        s for s in BASE_SIGNALS
        if (now_ts - float(s.get("created_ts", now_ts))) <= SIGNAL_TTL_SECONDS
    ]


def calculate_rsi(series: pd.Series, period: int = 14) -> pd.Series:
    delta = series.diff()
    gain = delta.clip(lower=0.0)
    loss = -delta.clip(upper=0.0)
    avg_gain = gain.ewm(alpha=1.0 / period, min_periods=period, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1.0 / period, min_periods=period, adjust=False).mean()
    rs = avg_gain / avg_loss.replace(0.0, np.nan)
    rsi = 100.0 - (100.0 / (1.0 + rs))
    rsi = rsi.where(avg_loss != 0.0, 100.0)
    rsi = rsi.where(~((avg_gain == 0.0) & (avg_loss == 0.0)), 50.0)
    return rsi


def ema_list(values: List[float], length: int) -> List[float]:
    if not values:
        return []
    alpha = 2.0 / (length + 1.0)
    out = [float(values[0])]
    for v in values[1:]:
        out.append(float(v) * alpha + out[-1] * (1.0 - alpha))
    return out


def smoothed_heikin_ashi_green(df: pd.DataFrame, smooth_len: int = 6) -> Optional[bool]:
    if df.empty or len(df) < smooth_len + 2:
        return None
    s_open = df["open"].ewm(span=smooth_len, adjust=False).mean()
    s_high = df["high"].ewm(span=smooth_len, adjust=False).mean()
    s_low = df["low"].ewm(span=smooth_len, adjust=False).mean()
    s_close = df["close"].ewm(span=smooth_len, adjust=False).mean()

    ha_close = (s_open + s_high + s_low + s_close) / 4.0
    hc_arr = ha_close.to_numpy(dtype=float)
    ha_open = np.empty(len(df), dtype=float)
    ha_open[0] = (float(s_open.iloc[0]) + float(s_close.iloc[0])) / 2.0
    for i in range(1, len(df)):
        ha_open[i] = (ha_open[i - 1] + hc_arr[i - 1]) / 2.0
    return bool(hc_arr[-1] > ha_open[-1])


def fetch_json_with_fallback(
    session: requests.Session,
    path: str,
    params: Optional[Dict[str, Any]] = None,
    preferred_url: Optional[str] = None,
    timeout: int = 6,
):
    urls = list(BINANCE_ENDPOINTS)
    if preferred_url and preferred_url in urls:
        urls.remove(preferred_url)
        urls.insert(0, preferred_url)
    last_error = None
    for base_url in urls:
        try:
            resp = session.get(f"{base_url}{path}", params=params, timeout=timeout)
            if resp.status_code == 429:
                raise RuntimeError("RATE_LIMIT_429")
            if resp.status_code == 418:
                last_error = RuntimeError("418_BLOCK")
                continue
            resp.raise_for_status()
            return resp.json(), base_url
        except RuntimeError as r_err:
            if str(r_err) == "RATE_LIMIT_429":
                raise
            last_error = r_err
        except Exception as exc:
            last_error = exc
    raise RuntimeError(str(last_error or "All endpoints failed"))


def get_spot_usdt_universe(
    session: requests.Session,
) -> Tuple[List[str], Dict[str, float], Dict[str, int], str]:
    info_data, working_url = fetch_json_with_fallback(session, "/api/v3/exchangeInfo", timeout=8)
    symbols_raw = info_data.get("symbols", [])

    base_pair_count: Dict[str, int] = {}
    for s in symbols_raw:
        if s.get("status") == "TRADING" and s.get("isSpotTradingAllowed") is True:
            b = s.get("baseAsset", "")
            if b:
                base_pair_count[b] = base_pair_count.get(b, 0) + 1

    valid_spot: Set[str] = set()
    pair_counts_map: Dict[str, int] = {}
    for s in symbols_raw:
        base = s.get("baseAsset", "")
        quote = s.get("quoteAsset", "")
        sym = s.get("symbol", "")
        p_cnt = base_pair_count.get(base, 0)
        if (
            quote == "USDT"
            and sym.endswith("USDT")
            and s.get("status") == "TRADING"
            and s.get("isSpotTradingAllowed") is True
            and base not in EXCLUDED_BASES
            and not any(base.endswith(x) for x in ("UP", "DOWN", "BULL", "BEAR"))
            and p_cnt >= MIN_ACTIVE_PAIRS_PER_BASE
        ):
            valid_spot.add(sym)
            pair_counts_map[sym] = p_cnt

    change_pct_map: Dict[str, float] = {}
    vol_map: Dict[str, float] = {}
    try:
        tickers, working_url = fetch_json_with_fallback(
            session, "/api/v3/ticker/24hr", preferred_url=working_url, timeout=8
        )
        for t in tickers:
            sym = t.get("symbol", "")
            if sym in valid_spot:
                try:
                    vol_map[sym] = float(t.get("quoteVolume", 0.0))
                except ValueError:
                    vol_map[sym] = 0.0
                try:
                    change_pct_map[sym] = float(t.get("priceChangePercent", 0.0))
                except ValueError:
                    change_pct_map[sym] = 0.0
        ordered = sorted(valid_spot, key=lambda x: vol_map.get(x, 0.0), reverse=True)
        return ordered, change_pct_map, pair_counts_map, working_url
    except Exception:
        return sorted(valid_spot), change_pct_map, pair_counts_map, working_url


def fetch_klines_df(
    session: requests.Session, symbol: str, base_url: str, interval: str, limit: int
) -> pd.DataFrame:
    raw, _ = fetch_json_with_fallback(
        session,
        "/api/v3/klines",
        params={"symbol": symbol, "interval": interval, "limit": limit},
        preferred_url=base_url,
        timeout=6,
    )
    if not raw:
        return pd.DataFrame()
    cols = [
        "open_time", "open", "high", "low", "close", "volume",
        "close_time", "quote_asset_volume", "number_of_trades",
        "taker_buy_base", "taker_buy_quote", "ignore",
    ]
    df = pd.DataFrame(raw, columns=cols)
    df["open_time"] = pd.to_datetime(df["open_time"], unit="ms", utc=True)
    df.set_index("open_time", inplace=True)
    for col in ["open", "high", "low", "close", "volume"]:
        df[col] = df[col].astype(float)
    return df[["open", "high", "low", "close", "volume"]]


def _trend_up(df: pd.DataFrame) -> bool:
    if df.empty or len(df) < 208:
        return False
    closes = df["close"].iloc[:-1].tolist()
    e50 = ema_list(closes, 50)[-1]
    e200 = ema_list(closes, 200)[-1]
    return closes[-1] > e50 and closes[-1] > e200


def _detect_trend_1h(df_1h: pd.DataFrame, lookback: int = 36) -> Optional[str]:
    if len(df_1h) < lookback + 5:
        return None
    sub = df_1h.iloc[-(lookback + 1) : -1]
    start_p = float(sub["close"].iloc[0])
    end_p = float(sub["close"].iloc[-1])
    min_l = float(sub["low"].min())
    max_h = float(sub["high"].max())
    if start_p <= 0 or min_l <= 0:
        return None
    net_pct = ((end_p - start_p) / start_p) * 100.0
    range_pct = ((max_h - min_l) / min_l) * 100.0
    ema_fast = sub["close"].ewm(span=12, adjust=False).mean().iloc[-1]
    ema_slow = sub["close"].ewm(span=26, adjust=False).mean().iloc[-1]
    if net_pct >= 2.0 and ema_fast >= ema_slow:
        return "UP"
    if abs(net_pct) <= 8.0 and range_pct <= 18.0:
        return "SIDEWAYS"
    return None


def check_v1_rule(df_1h: pd.DataFrame, df_1d: pd.DataFrame):
    if not _trend_up(df_1h) or not _trend_up(df_1d):
        return None
    rsi_1h = float(calculate_rsi(df_1h["close"], 14).iloc[-1])
    if np.isnan(rsi_1h) or not (53.0 <= rsi_1h <= 57.0):
        return None
    price = float(df_1h["close"].iloc[-1])
    return {
        "scanner": "V1",
        "timeframe": "60",
        "metric": f"1H RSI {rsi_1h:.1f}",
        "detail": "1D+1H UP",
        "price": f"{price:.8f}".rstrip("0").rstrip("."),
    }


def check_hb_rule(df_1h: pd.DataFrame, df_30m: pd.DataFrame):
    if df_1h.empty or df_30m.empty or len(df_1h) < 30 or len(df_30m) < 30:
        return None
    rsi_1h = float(calculate_rsi(df_1h["close"], 14).iloc[-1])
    rsi_30m = float(calculate_rsi(df_30m["close"], 14).iloc[-1])
    if np.isnan(rsi_1h) or np.isnan(rsi_30m):
        return None
    if not (25.0 <= rsi_1h <= 40.0) or not (rsi_30m > rsi_1h):
        return None
    price = float(df_1h["close"].iloc[-1])
    return {
        "scanner": "HB",
        "timeframe": "30",
        "metric": f"1H {rsi_1h:.1f} | 30m {rsi_30m:.1f}",
        "detail": "30m > 1H",
        "price": f"{price:.8f}".rstrip("0").rstrip("."),
    }


def check_pa_rule(df_1h: pd.DataFrame, trend_1h: Optional[str]):
    if df_1h.empty or len(df_1h) < 30 or trend_1h not in ("UP", "SIDEWAYS"):
        return None
    rsi_1h = float(calculate_rsi(df_1h["close"], 14).iloc[-1])
    if np.isnan(rsi_1h):
        return None
    if trend_1h == "UP" and not (30.0 <= rsi_1h <= 40.0):
        return None
    if trend_1h == "SIDEWAYS" and not (25.0 <= rsi_1h <= 40.0):
        return None
    price = float(df_1h["close"].iloc[-1])
    return {
        "scanner": "PA",
        "timeframe": "60",
        "metric": f"{trend_1h} · RSI {rsi_1h:.1f}",
        "detail": "1H Zone",
        "price": f"{price:.8f}".rstrip("0").rstrip("."),
    }


def check_ict_rule(df_1h: pd.DataFrame, trend_1h: Optional[str]):
    if df_1h.empty or len(df_1h) < 30 or trend_1h != "SIDEWAYS":
        return None
    rsi_1h = float(calculate_rsi(df_1h["close"], 14).iloc[-1])
    if np.isnan(rsi_1h) or not (30.0 <= rsi_1h <= 40.0):
        return None
    price = float(df_1h["close"].iloc[-1])
    return {
        "scanner": "ICT",
        "timeframe": "60",
        "metric": f"SIDEWAYS · RSI {rsi_1h:.1f}",
        "detail": "1H 30-40",
        "price": f"{price:.8f}".rstrip("0").rstrip("."),
    }


def is_established_non_delisting_coin(df_1d: pd.DataFrame, pair_count: int) -> bool:
    if pair_count < MIN_ACTIVE_PAIRS_PER_BASE:
        return False
    if df_1d.empty or len(df_1d) < MIN_AGE_DAYS_1D:
        return False
    recent_vols = df_1d["volume"].iloc[-7:].to_numpy(dtype=float)
    if len(recent_vols) == 0 or np.any(recent_vols <= 0.0):
        return False
    return True


def check_custom_v1(df_1h: pd.DataFrame, df_1d: pd.DataFrame, df_30m: pd.DataFrame):
    if not _trend_up(df_1h) or not _trend_up(df_1d) or df_30m.empty or len(df_30m) < 25:
        return None
    rsi_1h = float(calculate_rsi(df_1h["close"], 14).iloc[-1])
    rsi_30m = float(calculate_rsi(df_30m["close"], 14).iloc[-1])
    if np.isnan(rsi_1h) or np.isnan(rsi_30m):
        return None
    if not (53.0 <= rsi_1h <= 57.0) or not (rsi_30m > rsi_1h):
        return None
    if smoothed_heikin_ashi_green(df_1h, 6) is not True or smoothed_heikin_ashi_green(df_30m, 6) is not True:
        return None
    price = float(df_1h["close"].iloc[-1])
    return {
        "scanner": "CV1",
        "timeframe": "60",
        "price": f"{price:.8f}".rstrip("0").rstrip("."),
        "metric": f"1H RSI {rsi_1h:.1f} · 30m {rsi_30m:.1f}",
        "detail": "1D+1H UP · 1H+30m SHA Green · 30m RSI>1H",
    }


def _no_body_touch_between(
    opens: np.ndarray, closes: np.ndarray, idx_a: int, idx_b: int, y_a: float, y_b: float
) -> bool:
    start_i = min(idx_a, idx_b)
    end_i = max(idx_a, idx_b)
    if end_i - start_i <= 1:
        return True
    for k in range(start_i + 1, end_i):
        t = (k - idx_a) / float(idx_b - idx_a)
        line_y = y_a + t * (y_b - y_a)
        if min(opens[k], closes[k]) <= line_y <= max(opens[k], closes[k]):
            return False
    return True


def check_custom_hb(df_1h: pd.DataFrame, df_30m: pd.DataFrame):
    if df_1h.empty or df_30m.empty or len(df_1h) < 40 or len(df_30m) < 25:
        return None
    rsi_1h_s = calculate_rsi(df_1h["close"], 14)
    rsi_30m_s = calculate_rsi(df_30m["close"], 14)
    rsi_1h = float(rsi_1h_s.iloc[-1])
    rsi_30m = float(rsi_30m_s.iloc[-1])
    if np.isnan(rsi_1h) or np.isnan(rsi_30m):
        return None
    if not (25.0 <= rsi_1h <= 40.0) or not (rsi_30m > rsi_1h):
        return None

    opens = df_1h["open"].to_numpy(dtype=float)
    closes = df_1h["close"].to_numpy(dtype=float)
    lows = df_1h["low"].to_numpy(dtype=float)
    rsi_arr = rsi_1h_s.to_numpy(dtype=float)

    curr_idx = len(df_1h) - 1
    curr_low = lows[curr_idx]
    curr_rsi = rsi_arr[curr_idx]
    if np.isnan(curr_rsi):
        return None

    for prev_idx in range(curr_idx - 4, max(1, curr_idx - 35), -1):
        if lows[prev_idx] <= lows[prev_idx - 1] and lows[prev_idx] <= lows[prev_idx + 1]:
            prev_low = lows[prev_idx]
            prev_rsi = rsi_arr[prev_idx]
            if not np.isnan(prev_rsi) and curr_low < prev_low and curr_rsi > prev_rsi:
                if _no_body_touch_between(opens, closes, prev_idx, curr_idx, prev_low, curr_low):
                    price = float(df_1h["close"].iloc[-1])
                    return {
                        "scanner": "CHB",
                        "timeframe": "60",
                        "price": f"{price:.8f}".rstrip("0").rstrip("."),
                        "metric": f"1H RSI {rsi_1h:.1f} · 30m {rsi_30m:.1f}",
                        "detail": f"4+ Candles ({curr_idx - prev_idx}c) · Low<PrevLow · RSI>PrevRSI · 30m>1H",
                    }
    return None


def check_custom_pa(symbol: str, df_1h: pd.DataFrame, trend_1h: Optional[str]):
    if df_1h.empty or len(df_1h) < 40 or trend_1h not in ("UP", "SIDEWAYS"):
        return None
    rsi_1h = float(calculate_rsi(df_1h["close"], 14).iloc[-1])
    if np.isnan(rsi_1h):
        return None
    if trend_1h == "UP" and not (30.0 <= rsi_1h <= 40.0):
        return None
    if trend_1h == "SIDEWAYS" and not (25.0 <= rsi_1h <= 40.0):
        return None

    window = df_1h.iloc[-40:-1]
    lows = window["low"].to_numpy(dtype=float)
    highs = window["high"].to_numpy(dtype=float)
    curr_price = float(df_1h["close"].iloc[-1])
    curr_low = float(df_1h["low"].iloc[-1])
    if curr_price <= 0:
        return None

    supports = [
        float(lows[i]) for i in range(1, len(window) - 1)
        if lows[i] <= lows[i - 1] and lows[i] <= lows[i + 1]
    ]
    resistances = [
        float(highs[i]) for i in range(1, len(window) - 1)
        if highs[i] >= highs[i - 1] and highs[i] >= highs[i + 1]
    ]
    if not supports or not resistances:
        return None

    strong_sup = min(supports, key=lambda lv: abs(lv - curr_low))
    strong_res = max(resistances)
    if strong_res <= strong_sup:
        return None

    near_sup = (curr_low <= strong_sup * 1.006) and (curr_price >= strong_sup * 0.992)
    if not near_sup:
        return None

    with _lock:
        prev_p = CPA_LAST_PRICE.get(symbol)
        if prev_p is not None and prev_p > 0:
            pump_pct = ((curr_price - prev_p) / prev_p) * 100.0
            if pump_pct < 1.0:
                return None
        CPA_LAST_PRICE[symbol] = curr_price

    return {
        "scanner": "CPA",
        "timeframe": "60",
        "price": f"{curr_price:.8f}".rstrip("0").rstrip("."),
        "metric": f"{trend_1h} · RSI {rsi_1h:.1f}",
        "detail": f"Sup {strong_sup:.6f}".rstrip("0").rstrip(".")
        + f" · Res {strong_res:.6f}".rstrip("0").rstrip(".")
        + " · +1% Pump Rule",
    }


def check_custom_ict(df_1h: pd.DataFrame, trend_1h: Optional[str]):
    if df_1h.empty or len(df_1h) < 30 or trend_1h != "SIDEWAYS":
        return None
    rsi_1h = float(calculate_rsi(df_1h["close"], 14).iloc[-1])
    if np.isnan(rsi_1h) or not (30.0 <= rsi_1h <= 40.0):
        return None

    opens = df_1h["open"].to_numpy(dtype=float)
    closes = df_1h["close"].to_numpy(dtype=float)
    n = len(df_1h)

    if not (closes[-1] > opens[-1]):
        return None
    curr_red_idx = n - 2
    if curr_red_idx < 5 or not (closes[curr_red_idx] < opens[curr_red_idx]):
        return None

    g_end = curr_red_idx - 1
    while g_end >= 1 and closes[g_end] <= opens[g_end]:
        g_end -= 1
    if g_end < 1 or not (closes[g_end] > opens[g_end]):
        return None

    g_start = g_end
    green_count = 0
    while g_start >= 0 and closes[g_start] > opens[g_start] and green_count < 5:
        green_count += 1
        g_start -= 1
    g_start += 1
    if green_count < 2 or opens[g_start] <= 0:
        return None

    pump_pct = ((closes[g_end] - opens[g_start]) / opens[g_start]) * 100.0
    if pump_pct < 1.0:
        return None

    prev_red_idx = g_start - 1
    while prev_red_idx >= 0 and closes[prev_red_idx] >= opens[prev_red_idx]:
        prev_red_idx -= 1
    if prev_red_idx < 0:
        return None

    if not _no_body_touch_between(opens, closes, prev_red_idx, curr_red_idx, closes[prev_red_idx], closes[curr_red_idx]):
        return None

    price = float(df_1h["close"].iloc[-1])
    return {
        "scanner": "CICT",
        "timeframe": "60",
        "price": f"{price:.8f}".rstrip("0").rstrip("."),
        "metric": f"SIDEWAYS · RSI {rsi_1h:.1f} · +{pump_pct:.2f}%",
        "detail": f"{green_count}G Block >=1% · Clean Red Trendline · Next Green Started",
    }


def _scan_symbol(
    symbol: str,
    base_url: str,
    shared_session: requests.Session,
    change_pct_24h: float,
    pair_count: int,
    check_gainer_1d: bool,
) -> Tuple[List[Dict[str, Any]], Optional[Dict[str, Any]], List[Dict[str, Any]]]:
    if _stop_event.is_set() or not symbol.endswith("USDT"):
        return [], None, []

    with _lock:
        c_v1 = CUSTOM_ENABLED["CV1"]
        c_ict = CUSTOM_ENABLED["CICT"]
        c_hb = CUSTOM_ENABLED["CHB"]
        c_pa = CUSTOM_ENABLED["CPA"]

    base_found: List[Dict[str, Any]] = []
    custom_found: List[Dict[str, Any]] = []
    gainer_cand: Optional[Dict[str, Any]] = None

    try:
        df_1h = fetch_klines_df(shared_session, symbol, base_url, "1h", 250)
        if df_1h.empty or len(df_1h) < 50:
            return [], None, []

        rsi_1h_val = float(calculate_rsi(df_1h["close"], 14).iloc[-1])
        trend_1h = _detect_trend_1h(df_1h)

        need_v1_1d = (not np.isnan(rsi_1h_val) and 53.0 <= rsi_1h_val <= 57.0 and _trend_up(df_1h))
        need_1d = need_v1_1d or check_gainer_1d or c_v1
        need_30m = (not np.isnan(rsi_1h_val) and 25.0 <= rsi_1h_val <= 40.0) or c_v1 or c_hb

        df_1d = fetch_klines_df(shared_session, symbol, base_url, "1d", 250) if need_1d else pd.DataFrame()
        df_30m = fetch_klines_df(shared_session, symbol, base_url, "30m", 100) if need_30m else pd.DataFrame()

        if need_v1_1d:
            s = check_v1_rule(df_1h, df_1d)
            if s:
                base_found.append(s)
        if not np.isnan(rsi_1h_val) and 25.0 <= rsi_1h_val <= 40.0:
            s = check_hb_rule(df_1h, df_30m)
            if s:
                base_found.append(s)
        s = check_pa_rule(df_1h, trend_1h)
        if s:
            base_found.append(s)
        s = check_ict_rule(df_1h, trend_1h)
        if s:
            base_found.append(s)

        if check_gainer_1d and is_established_non_delisting_coin(df_1d, pair_count):
            price = float(df_1h["close"].iloc[-1])
            gainer_cand = {
                "scanner": "GN",
                "timeframe": "240",
                "change_pct": change_pct_24h,
                "metric": f"+{change_pct_24h:.2f}% 24h",
                "detail": f"{pair_count} Pairs · {len(df_1d)}d",
                "price": f"{price:.8f}".rstrip("0").rstrip("."),
            }

        if c_v1:
            cs = check_custom_v1(df_1h, df_1d, df_30m)
            if cs:
                custom_found.append(cs)
        if c_ict:
            cs = check_custom_ict(df_1h, trend_1h)
            if cs:
                custom_found.append(cs)
        if c_hb:
            cs = check_custom_hb(df_1h, df_30m)
            if cs:
                custom_found.append(cs)
        if c_pa:
            cs = check_custom_pa(symbol, df_1h, trend_1h)
            if cs:
                custom_found.append(cs)

        now_dt = datetime.now(timezone.utc)
        now_str = format_pkt_time(now_dt)
        now_ts = now_dt.timestamp()
        for item in base_found + custom_found:
            item["symbol"] = symbol
            item["market"] = MARKET_TYPE
            item["detected_at"] = now_str
            item["created_ts"] = now_ts

        if gainer_cand:
            gainer_cand["symbol"] = symbol
            gainer_cand["market"] = MARKET_TYPE
            gainer_cand["detected_at"] = now_str
            gainer_cand["created_ts"] = now_ts

        return base_found, gainer_cand, custom_found
    except Exception:
        return base_found, None, custom_found


def _sync_gainers_locked(gainers: List[Dict[str, Any]], now_ts: float) -> None:
    global BASE_SIGNALS
    gainers_sorted = sorted(gainers, key=lambda x: float(x.get("change_pct", 0.0)), reverse=True)[:TOP_GAINERS_LIMIT]
    BASE_SIGNALS = [s for s in BASE_SIGNALS if s.get("scanner") != "GN"]
    for g in reversed(gainers_sorted):
        BASE_SIGNALS.insert(0, g)
    prune_base_signals_locked(now_ts)


def _bg_loop() -> None:
    session = requests.Session()
    cached_symbols: List[str] = []
    change_pct_map: Dict[str, float] = {}
    pair_counts_map: Dict[str, int] = {}
    last_sym_fetch = 0.0
    working_url = BINANCE_ENDPOINTS[0]

    while not _stop_event.is_set():
        try:
            now = time.time()
            if not cached_symbols or (now - last_sym_fetch) >= SYMBOL_REFRESH_SECONDS:
                cached_symbols, change_pct_map, pair_counts_map, working_url = get_spot_usdt_universe(session)
                last_sym_fetch = time.time()

            top_movers = [
                sym
                for sym, chg in sorted(change_pct_map.items(), key=lambda kv: kv[1], reverse=True)
                if chg > 0.0 and pair_counts_map.get(sym, 0) >= MIN_ACTIVE_PAIRS_PER_BASE
            ]
            top_mover_candidates = set(top_movers[:50])

            with _lock:
                prune_base_signals_locked(time.time())
                SCAN_STATUS["running"] = True
                SCAN_STATUS["scanned"] = 0
                SCAN_STATUS["total"] = len(cached_symbols)
                SCAN_STATUS["market"] = MARKET_TYPE
                SCAN_STATUS["working_url"] = working_url
                SCAN_STATUS["owner"] = OWNER_LABEL

            cycle_gainers: List[Dict[str, Any]] = []
            with ThreadPoolExecutor(max_workers=8) as pool:
                futures = [
                    pool.submit(
                        _scan_symbol,
                        sym,
                        working_url,
                        session,
                        change_pct_map.get(sym, 0.0),
                        pair_counts_map.get(sym, 0),
                        sym in top_mover_candidates,
                    )
                    for sym in cached_symbols
                ]
                for fut in as_completed(futures):
                    if _stop_event.is_set():
                        break
                    b_list, g_cand, c_list = fut.result()
                    if g_cand:
                        cycle_gainers.append(g_cand)

                    now_ts = time.time()
                    with _lock:
                        prune_base_signals_locked(now_ts)
                        for item in b_list:
                            cd_key = f"B:{item['scanner']}:{item['symbol']}"
                            if (now_ts - ALERT_HISTORY.get(cd_key, 0.0)) >= ALERT_COOLDOWN_SECONDS:
                                ALERT_HISTORY[cd_key] = now_ts
                                BASE_SIGNALS.insert(0, item)
                                del BASE_SIGNALS[500:]
                        for c_item in c_list:
                            dup = any(
                                s.get("scanner") == c_item["scanner"]
                                and s.get("symbol") == c_item["symbol"]
                                and s.get("metric") == c_item["metric"]
                                for s in CUSTOM_SIGNALS[:30]
                            )
                            if not dup:
                                CUSTOM_SIGNALS.insert(0, c_item)
                                del CUSTOM_SIGNALS[1000:]
                        if g_cand:
                            _sync_gainers_locked(cycle_gainers, now_ts)
                        SCAN_STATUS["scanned"] += 1

            now_ts = time.time()
            with _lock:
                _sync_gainers_locked(cycle_gainers, now_ts)
                SCAN_STATUS["last_cycle"] = format_pkt_time(datetime.now(timezone.utc))
        except Exception:
            pass

        for _ in range(5):
            if _stop_event.is_set():
                break
            with _lock:
                prune_base_signals_locked(time.time())
            time.sleep(1)

    with _lock:
        SCAN_STATUS["running"] = False
    session.close()


@app.on_event("startup")
def on_startup() -> None:
    _stop_event.clear()
    threading.Thread(target=_bg_loop, daemon=True).start()


@app.on_event("shutdown")
def on_shutdown() -> None:
    _stop_event.set()


@app.get("/api/signals")
def get_signals():
    now_ts = time.time()
    with _lock:
        prune_base_signals_locked(now_ts)
        return {
            "status": dict(SCAN_STATUS),
            "custom_enabled": dict(CUSTOM_ENABLED),
            "base_signals": list(BASE_SIGNALS),
            "custom_signals": list(CUSTOM_SIGNALS),
        }


@app.post("/api/custom-control")
def custom_control(payload: CustomControlPayload):
    with _lock:
        if payload.clear_custom:
            CUSTOM_SIGNALS.clear()
            CPA_LAST_PRICE.clear()
        if payload.scanner in CUSTOM_ENABLED and payload.enabled is not None:
            CUSTOM_ENABLED[payload.scanner] = bool(payload.enabled)
        return JSONResponse(
            {
                "ok": True,
                "custom_enabled": dict(CUSTOM_ENABLED),
                "custom_signals_count": len(CUSTOM_SIGNALS),
            }
        )


HTML_PAGE = """<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>FREEDOM WITH AHMII</title>
  <style>
    * { box-sizing: border-box; font-family: 'Segoe UI', Arial, sans-serif; }
    body {
      margin: 0;
      background: #05080e;
      color: #f8fafc;
      min-height: 100vh;
      padding: 16px 16px 44px;
    }
    .wrap { max-width: 1560px; margin: 0 auto; }
    .top-header {
      display: flex;
      justify-content: space-between;
      align-items: flex-start;
      gap: 16px;
      padding-bottom: 14px;
      margin-bottom: 16px;
      border-bottom: 1px solid #1b2434;
      flex-wrap: wrap;
    }
    .status-area {
      flex: 1 1 420px;
      min-width: 280px;
      padding-top: 4px;
    }
    .status-line {
      font-size: 12px;
      color: #94a3b8;
      display: flex;
      gap: 16px;
      flex-wrap: wrap;
      align-items: center;
    }
    .status-line b { color: #38bdf8; }
    .top-right-box {
      width: 270px;
      flex-shrink: 0;
      background: linear-gradient(180deg, #0b1915 0%, #090e18 100%);
      border: 1px solid #1c352c;
      border-radius: 14px;
      padding: 14px 16px;
      text-align: center;
      box-shadow: 0 10px 28px rgba(0, 0, 0, 0.45);
    }
    .bismillah-top {
      font-size: 12px;
      font-weight: 800;
      letter-spacing: 1.6px;
      color: #6ee7b7;
      margin-bottom: 6px;
    }
    .brand-name {
      font-size: 17px;
      font-weight: 900;
      letter-spacing: 0.8px;
      color: #ffffff;
      line-height: 1.25;
    }
    .contact-num {
      margin-top: 6px;
      font-size: 14px;
      font-weight: 800;
      letter-spacing: 0.8px;
      color: #34d399;
    }
    .stack-list {
      width: 100%;
      display: flex;
      flex-direction: column;
      gap: 14px;
    }
    .custom-box {
      background: #090e1a;
      border: 1px solid #1f2d46;
      border-radius: 12px;
      padding: 14px;
    }
    .mini-tabs {
      display: flex;
      gap: 8px;
      flex-wrap: wrap;
      align-items: center;
      margin-bottom: 10px;
    }
    .mini-tab {
      background: #0d1422;
      color: #cbd5e1;
      border: 1px solid #24324a;
      border-radius: 7px;
      padding: 5px 10px;
      font-size: 11px;
      font-weight: 800;
      letter-spacing: 0.5px;
      cursor: pointer;
    }
    .mini-tab.active {
      border-color: #34d399;
      color: #34d399;
      background: #0d1f1b;
    }
    .clear-btn {
      background: #1e1318;
      color: #fda4af;
      border: 1px solid #881337;
      border-radius: 7px;
      padding: 5px 12px;
      font-size: 11px;
      font-weight: 700;
      cursor: pointer;
      margin-left: auto;
    }
    .clear-btn:hover { background: #3b0d1c; }
    .start-panel {
      background: #0b1220;
      border: 1px dashed #2b3d5c;
      border-radius: 10px;
      padding: 12px 14px;
      margin-bottom: 12px;
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 12px;
      flex-wrap: wrap;
    }
    .start-btn {
      background: #10b981;
      color: #04110c;
      border: none;
      border-radius: 8px;
      padding: 7px 14px;
      font-size: 12px;
      font-weight: 900;
      cursor: pointer;
    }
    .start-btn.running {
      background: #ef4444;
      color: #ffffff;
    }
    .section-row {
      background: #090d16;
      border: 1px solid #1e293b;
      border-radius: 12px;
      padding: 14px;
    }
    .section-head {
      display: flex;
      justify-content: space-between;
      align-items: center;
      margin-bottom: 10px;
      padding-bottom: 8px;
      border-bottom: 1px solid #141c2b;
      gap: 10px;
      flex-wrap: wrap;
    }
    .section-title {
      font-size: 15px;
      font-weight: 800;
      letter-spacing: 1px;
    }
    .t-V1 { color: #34d399; }
    .t-HB { color: #60a5fa; }
    .t-PA { color: #c084fc; }
    .t-ICT { color: #fbbf24; }
    .t-GN { color: #4ade80; }
    .section-cnt {
      font-size: 12px;
      font-weight: 700;
      color: #cbd5e1;
      background: #111827;
      border: 1px solid #263248;
      padding: 3px 10px;
      border-radius: 999px;
    }
    .coin-strip {
      display: flex;
      flex-wrap: wrap;
      gap: 10px;
      min-height: 50px;
      align-items: center;
    }
    .coin-box {
      background: #101726;
      border: 1px solid #28354d;
      border-radius: 9px;
      padding: 8px 12px;
      min-width: 150px;
      max-width: 280px;
      cursor: pointer;
      user-select: none;
      transition: 0.15s;
    }
    .coin-box:hover {
      border-color: #38bdf8;
      background: #152036;
    }
    .coin-sym {
      font-size: 13px;
      font-weight: 800;
      color: #ffffff;
    }
    .coin-sub {
      font-size: 11px;
      color: #34d399;
      margin-top: 2px;
      font-weight: 700;
    }
    .coin-meta {
      font-size: 10.5px;
      color: #cbd5e1;
      margin-top: 2px;
      line-height: 1.3;
    }
    .coin-time {
      font-size: 10px;
      color: #94a3b8;
      margin-top: 2px;
    }
    .empty-msg {
      color: #64748b;
      font-size: 12px;
    }
  </style>
</head>
<body>
  <div class="wrap">
    <div class="top-header">
      <div class="status-area">
        <div class="status-line">
          <span>Market: <b>SPOT USDT</b></span>
          <span>Scanned: <b id="stScan">0/0</b></span>
          <span>Signals: <b id="stTotal">0</b></span>
          <span>Cycle: <b id="stCycle">--</b></span>
          <span>Click coin to open <b>TradingView</b></span>
        </div>
      </div>

      <div class="top-right-box">
        <div class="bismillah-top">ALHUMDULLILHA</div>
        <div class="brand-name">FREEDOM WITH AHMII</div>
        <div class="contact-num">03254112787</div>
      </div>
    </div>

    <div class="stack-list">
      <div class="custom-box">
        <div class="mini-tabs">
          <button class="mini-tab" id="tab-CV1" onclick="selectCustom('CV1')">V1 (SHA+RSI)</button>
          <button class="mini-tab" id="tab-CICT" onclick="selectCustom('CICT')">ICT (TRENDLINE)</button>
          <button class="mini-tab" id="tab-CHB" onclick="selectCustom('CHB')">HIDDEN BULLISH (SWING)</button>
          <button class="mini-tab" id="tab-CPA" onclick="selectCustom('CPA')">PRICE ACTION (+1%)</button>
          <button class="clear-btn" onclick="clearCustomSignals()">Clear Custom Signals</button>
        </div>

        <div class="start-panel">
          <div>
            <b id="activeCustomLabel">V1 (SHA+RSI)</b>:
            <span id="activeCustomState" class="empty-msg">Stopped — click Start Scan</span>
          </div>
          <button class="start-btn" id="toggleCustomBtn" onclick="toggleActiveCustom()">Start Scan</button>
        </div>

        <div class="coin-strip" id="list-CUSTOM"></div>
      </div>

      <div class="section-row">
        <div class="section-head">
          <span class="section-title t-V1">V1 METHOD</span>
          <span class="section-cnt" id="cnt-V1">0</span>
        </div>
        <div class="coin-strip" id="list-V1"></div>
      </div>

      <div class="section-row">
        <div class="section-head">
          <span class="section-title t-HB">HIDDEN BULLISH</span>
          <span class="section-cnt" id="cnt-HB">0</span>
        </div>
        <div class="coin-strip" id="list-HB"></div>
      </div>

      <div class="section-row">
        <div class="section-head">
          <span class="section-title t-PA">PRICE ACTION</span>
          <span class="section-cnt" id="cnt-PA">0</span>
        </div>
        <div class="coin-strip" id="list-PA"></div>
      </div>

      <div class="section-row">
        <div class="section-head">
          <span class="section-title t-ICT">ICT</span>
          <span class="section-cnt" id="cnt-ICT">0</span>
        </div>
        <div class="coin-strip" id="list-ICT"></div>
      </div>

      <div class="section-row">
        <div class="section-head">
          <span class="section-title t-GN">GAINERS</span>
          <span class="section-cnt" id="cnt-GN">0</span>
        </div>
        <div class="coin-strip" id="list-GN"></div>
      </div>
    </div>
  </div>

  <script>
    const BASE_CODES = ["V1", "HB", "PA", "ICT", "GN"];
    const CUSTOM_CODES = ["CV1", "CICT", "CHB", "CPA"];
    const CUSTOM_NAMES = {
      CV1: "V1 (SHA+RSI)",
      CICT: "ICT (TRENDLINE)",
      CHB: "HIDDEN BULLISH (SWING)",
      CPA: "PRICE ACTION (+1%)"
    };

    let selectedCustom = "CV1";
    let customEnabled = { CV1: false, CICT: false, CHB: false, CPA: false };
    let latestCustomSignals = [];

    function updateCustomUI() {
      CUSTOM_CODES.forEach(c => {
        const tab = document.getElementById(`tab-${c}`);
        if (tab) tab.classList.toggle("active", c === selectedCustom);
      });
      const isOn = !!customEnabled[selectedCustom];
      document.getElementById("activeCustomLabel").textContent = CUSTOM_NAMES[selectedCustom];
      document.getElementById("activeCustomState").textContent =
        isOn ? "Running (Binance Spot USDT)" : "Stopped — click Start Scan";
      const btn = document.getElementById("toggleCustomBtn");
      btn.textContent = isOn ? "Stop Scan" : "Start Scan";
      btn.classList.toggle("running", isOn);
      renderCustomStrip();
    }

    function selectCustom(code) {
      selectedCustom = code;
      updateCustomUI();
    }

    async function toggleActiveCustom() {
      const nextState = !customEnabled[selectedCustom];
      customEnabled[selectedCustom] = nextState;
      updateCustomUI();
      try {
        await fetch("/api/custom-control", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ scanner: selectedCustom, enabled: nextState })
        });
      } catch (e) {}
      poll();
    }

    async function clearCustomSignals() {
      try {
        await fetch("/api/custom-control", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ clear_custom: true })
        });
      } catch (e) {}
      poll();
    }

    function renderCustomStrip() {
      const wrap = document.getElementById("list-CUSTOM");
      const items = latestCustomSignals.filter(x => x.scanner === selectedCustom);
      wrap.innerHTML = "";
      if (!items.length) {
        const em = document.createElement("div");
        em.className = "empty-msg";
        em.textContent = customEnabled[selectedCustom]
          ? "Scanning Spot USDT..."
          : "Stopped — click Start Scan to run this scanner";
        wrap.appendChild(em);
        return;
      }
      for (const s of items) {
        const card = document.createElement("div");
        card.className = "coin-box";
        card.onclick = () => {
          window.open(
            `https://www.tradingview.com/chart/?symbol=BINANCE:${s.symbol}&interval=${s.timeframe || 60}`,
            "_blank"
          );
        };
        card.innerHTML = `
          <div class="coin-sym">${s.symbol}</div>
          <div class="coin-sub">${s.price || ""} · ${s.metric || ""}</div>
          <div class="coin-meta">${s.detail || ""}</div>
          <div class="coin-time">Time: ${s.detected_at || ""}</div>
        `;
        wrap.appendChild(card);
      }
    }

    function renderBaseSection(code, items) {
      const boxWrap = document.getElementById(`list-${code}`);
      const cntEl = document.getElementById(`cnt-${code}`);
      cntEl.textContent = String(items.length);
      boxWrap.innerHTML = "";

      if (!items.length) {
        const em = document.createElement("div");
        em.className = "empty-msg";
        em.textContent = "No coins yet";
        boxWrap.appendChild(em);
        return;
      }

      for (const s of items) {
        const card = document.createElement("div");
        card.className = "coin-box";
        card.onclick = () => {
          window.open(
            `https://www.tradingview.com/chart/?symbol=BINANCE:${s.symbol}&interval=${s.timeframe || 60}`,
            "_blank"
          );
        };
        card.innerHTML = `
          <div class="coin-sym">${s.symbol}</div>
          <div class="coin-sub">${s.price || ""}</div>
          <div class="coin-meta">${s.metric || ""} · ${s.detail || ""}</div>
        `;
        boxWrap.appendChild(card);
      }
    }

    async function poll() {
      try {
        const r = await fetch("/api/signals");
        const d = await r.json();
        const st = d.status || {};
        if (d.custom_enabled) {
          customEnabled = { ...customEnabled, ...d.custom_enabled };
        }
        const baseAll = d.base_signals || [];
        latestCustomSignals = d.custom_signals || [];

        document.getElementById("stScan").textContent = `${st.scanned || 0}/${st.total || 0}`;
        document.getElementById("stTotal").textContent = String(baseAll.length + latestCustomSignals.length);
        document.getElementById("stCycle").textContent = st.last_cycle || "--";

        updateCustomUI();
        BASE_CODES.forEach(c => {
          renderBaseSection(c, baseAll.filter(x => x.scanner === c));
        });
      } catch (e) {}
    }

    updateCustomUI();
    poll();
    setInterval(poll, 2000);
  </script>
</body>
</html>
"""


@app.get("/", response_class=HTMLResponse)
def home() -> str:
    return HTML_PAGE
