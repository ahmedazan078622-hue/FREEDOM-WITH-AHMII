import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Set

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

SCANNER_KEYS = ["V1", "ICT", "HB", "PA"]
SYMBOL_REFRESH_SECONDS = 900

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

LATEST_SIGNALS: List[Dict[str, Any]] = []
PA_LAST_SIGNAL_PRICE: Dict[str, float] = {}
SCANNER_ENABLED: Dict[str, bool] = {
    "V1": False,
    "ICT": False,
    "HB": False,
    "PA": False,
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


class ControlPayload(BaseModel):
    scanner: Optional[str] = None
    enabled: Optional[bool] = None
    clear_all: bool = False


def format_pkt_time(dt_utc: datetime) -> str:
    return dt_utc.astimezone(PKT_TZ).strftime("%H:%M:%S PKT")


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


def smoothed_heikin_ashi(df: pd.DataFrame, smooth_len: int = 6) -> pd.DataFrame:
    if df.empty or len(df) < smooth_len + 2:
        return pd.DataFrame()
    s_open = df["open"].ewm(span=smooth_len, adjust=False).mean()
    s_high = df["high"].ewm(span=smooth_len, adjust=False).mean()
    s_low = df["low"].ewm(span=smooth_len, adjust=False).mean()
    s_close = df["close"].ewm(span=smooth_len, adjust=False).mean()

    ha_close = (s_open + s_high + s_low + s_close) / 4.0
    ha_open = np.zeros(len(df), dtype=float)
    ha_open[0] = (float(s_open.iloc[0]) + float(s_close.iloc[0])) / 2.0
    for i in range(1, len(df)):
        ha_open[i] = (ha_open[i - 1] + float(ha_close.iloc[i - 1])) / 2.0

    ha_open_s = pd.Series(ha_open, index=df.index)
    ha_high_s = pd.concat([s_high, ha_open_s, ha_close], axis=1).max(axis=1)
    ha_low_s = pd.concat([s_low, ha_open_s, ha_close], axis=1).min(axis=1)

    out = pd.DataFrame(index=df.index)
    out["ha_open"] = ha_open_s
    out["ha_high"] = ha_high_s
    out["ha_low"] = ha_low_s
    out["ha_close"] = ha_close
    out["is_green"] = out["ha_close"] > out["ha_open"]
    return out


def fetch_json_with_fallback(
    session: requests.Session,
    path: str,
    params: Optional[Dict[str, Any]] = None,
    preferred_url: Optional[str] = None,
    timeout: int = 8,
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


def get_spot_usdt_symbols(session: requests.Session):
    info_data, working_url = fetch_json_with_fallback(session, "/api/v3/exchangeInfo", timeout=10)
    valid_spot: Set[str] = set()
    for s in info_data.get("symbols", []):
        base = s.get("baseAsset", "")
        quote = s.get("quoteAsset", "")
        sym = s.get("symbol", "")
        if (
            quote == "USDT"
            and sym.endswith("USDT")
            and s.get("status") == "TRADING"
            and s.get("isSpotTradingAllowed") is True
            and base not in EXCLUDED_BASES
            and not any(base.endswith(x) for x in ("UP", "DOWN", "BULL", "BEAR"))
        ):
            valid_spot.add(sym)
    try:
        tickers, working_url = fetch_json_with_fallback(
            session, "/api/v3/ticker/24hr", preferred_url=working_url, timeout=10
        )
        vol_map: Dict[str, float] = {}
        for t in tickers:
            sym = t.get("symbol", "")
            if sym in valid_spot:
                try:
                    vol_map[sym] = float(t.get("quoteVolume", 0.0))
                except ValueError:
                    vol_map[sym] = 0.0
        return sorted(valid_spot, key=lambda x: vol_map.get(x, 0.0), reverse=True), working_url
    except Exception:
        return sorted(valid_spot), working_url


def fetch_klines_df(
    session: requests.Session, symbol: str, base_url: str, interval: str, limit: int
) -> pd.DataFrame:
    raw, _ = fetch_json_with_fallback(
        session,
        "/api/v3/klines",
        params={"symbol": symbol, "interval": interval, "limit": limit},
        preferred_url=base_url,
        timeout=8,
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


def check_v1_rule(df_1h: pd.DataFrame, df_1d: pd.DataFrame, df_30m: pd.DataFrame):
    """
    V1:
    - 1D & 1H trend UP
    - 1H RSI 53..57
    - 1H Smoothed Heikin Ashi green
    - Pre-check 30m Smoothed Heikin Ashi green & 30m RSI > 1H RSI
    """
    if not _trend_up(df_1h) or not _trend_up(df_1d):
        return None
    if df_30m.empty or len(df_30m) < 25:
        return None

    rsi_1h_s = calculate_rsi(df_1h["close"], 14)
    rsi_30m_s = calculate_rsi(df_30m["close"], 14)
    rsi_1h = float(rsi_1h_s.iloc[-1])
    rsi_30m = float(rsi_30m_s.iloc[-1])
    if np.isnan(rsi_1h) or np.isnan(rsi_30m):
        return None
    if not (53.0 <= rsi_1h <= 57.0):
        return None
    if not (rsi_30m > rsi_1h):
        return None

    ha_1h = smoothed_heikin_ashi(df_1h, smooth_len=6)
    ha_30m = smoothed_heikin_ashi(df_30m, smooth_len=6)
    if ha_1h.empty or ha_30m.empty:
        return None
    if not bool(ha_1h["is_green"].iloc[-1]) or not bool(ha_30m["is_green"].iloc[-1]):
        return None

    price = float(df_1h["close"].iloc[-1])
    return {
        "scanner": "V1",
        "timeframe": "60",
        "price": f"{price:.8f}".rstrip("0").rstrip("."),
        "metric": f"1H RSI {rsi_1h:.1f} · 30m {rsi_30m:.1f}",
        "detail": "1D+1H UP · 1H+30m SHA Green · 30m RSI>1H",
    }


def check_pa_rule(symbol: str, df_1h: pd.DataFrame, trend_1h: Optional[str]):
    """
    PA:
    - UP trend with 1H RSI 30..40 OR SIDEWAYS trend with 1H RSI 25..40
    - Find strong support & resistance on 1H
    - Signal when price reaches support zone
    - After a signal on a symbol, block re-signal until price pumps >= 1% from last signaled price
    """
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

    # Identify swing lows as support and swing highs as resistance
    support_levels: List[float] = []
    resistance_levels: List[float] = []
    for i in range(1, len(window) - 1):
        if lows[i] <= lows[i - 1] and lows[i] <= lows[i + 1]:
            support_levels.append(float(lows[i]))
        if highs[i] >= highs[i - 1] and highs[i] >= highs[i + 1]:
            resistance_levels.append(float(highs[i]))

    if not support_levels or not resistance_levels:
        return None

    strong_support = min(support_levels, key=lambda lv: abs(lv - curr_low))
    strong_resistance = max(resistance_levels)
    if strong_resistance <= strong_support:
        return None

    # Price touching / inside support zone (within 0.6% buffer)
    near_support = (curr_low <= strong_support * 1.006) and (curr_price >= strong_support * 0.992)
    if not near_support:
        return None

    with _lock:
        prev_sig_price = PA_LAST_SIGNAL_PRICE.get(symbol)
        if prev_sig_price is not None and prev_sig_price > 0:
            pump_pct = ((curr_price - prev_sig_price) / prev_sig_price) * 100.0
            if pump_pct < 1.0:
                return None
        PA_LAST_SIGNAL_PRICE[symbol] = curr_price

    return {
        "scanner": "PA",
        "timeframe": "60",
        "price": f"{curr_price:.8f}".rstrip("0").rstrip("."),
        "metric": f"{trend_1h} · RSI {rsi_1h:.1f}",
        "detail": f"Sup {strong_support:.6f}".rstrip("0").rstrip(".")
        + f" · Res {strong_resistance:.6f}".rstrip("0").rstrip(".")
        + " · +1% Pump Rule",
    }


def _no_body_touch_between(
    opens: np.ndarray, closes: np.ndarray, idx_a: int, idx_b: int, y_a: float, y_b: float
) -> bool:
    """Ensure no intermediate candle body [min(open, close), max(open, close)] crosses the line between idx_a and idx_b."""
    start_i = min(idx_a, idx_b)
    end_i = max(idx_a, idx_b)
    if end_i - start_i <= 1:
        return True
    for k in range(start_i + 1, end_i):
        t = (k - idx_a) / float(idx_b - idx_a)
        line_y = y_a + t * (y_b - y_a)
        b_low = min(opens[k], closes[k])
        b_high = max(opens[k], closes[k])
        if b_low <= line_y <= b_high:
            return False
    return True


def check_hb_rule(df_1h: pd.DataFrame, df_30m: pd.DataFrame):
    """
    HB (Hidden Bullish):
    - 1H: current swing low < previous swing low, current RSI at low > previous RSI at low
    - No candle body touch between the two swing lows (wicks allowed)
    - At least 4 candles spacing between the two lows
    - 1H RSI 25..40 and 30m RSI > 1H RSI
    """
    if df_1h.empty or df_30m.empty or len(df_1h) < 40 or len(df_30m) < 25:
        return None

    rsi_1h_s = calculate_rsi(df_1h["close"], 14)
    rsi_30m_s = calculate_rsi(df_30m["close"], 14)
    rsi_1h = float(rsi_1h_s.iloc[-1])
    rsi_30m = float(rsi_30m_s.iloc[-1])
    if np.isnan(rsi_1h) or np.isnan(rsi_30m):
        return None
    if not (25.0 <= rsi_1h <= 40.0):
        return None
    if not (rsi_30m > rsi_1h):
        return None

    opens = df_1h["open"].to_numpy(dtype=float)
    closes = df_1h["close"].to_numpy(dtype=float)
    lows = df_1h["low"].to_numpy(dtype=float)
    rsi_arr = rsi_1h_s.to_numpy(dtype=float)

    n = len(df_1h)
    curr_idx = n - 1
    curr_low = lows[curr_idx]
    curr_rsi = rsi_arr[curr_idx]
    if np.isnan(curr_rsi):
        return None

    for prev_idx in range(curr_idx - 4, max(1, curr_idx - 35), -1):
        if lows[prev_idx] <= lows[prev_idx - 1] and lows[prev_idx] <= lows[prev_idx + 1]:
            prev_low = lows[prev_idx]
            prev_rsi = rsi_arr[prev_idx]
            if np.isnan(prev_rsi):
                continue
            # Price makes lower low while RSI makes higher low
            if curr_low < prev_low and curr_rsi > prev_rsi:
                if _no_body_touch_between(opens, closes, prev_idx, curr_idx, prev_low, curr_low):
                    candles_apart = curr_idx - prev_idx
                    price = float(df_1h["close"].iloc[-1])
                    return {
                        "scanner": "HB",
                        "timeframe": "60",
                        "price": f"{price:.8f}".rstrip("0").rstrip("."),
                        "metric": f"1H RSI {rsi_1h:.1f} · 30m {rsi_30m:.1f}",
                        "detail": f"4+ Candles ({candles_apart}c) · Low<PrevLow · RSI>PrevRSI · 30m>1H",
                    }
    return None


def check_ict_rule(df_1h: pd.DataFrame, trend_1h: Optional[str]):
    """
    ICT:
    - 1H market SIDEWAYS, 1H RSI 30..40
    - Prior to current red candle, consecutive green candles (2..5 green candles treated as one leg/candle) >= 1% pump
    - Trendline connecting prior red candle to current red candle has no candle body touch in between
    - Triggers as soon as next green candle begins
    """
    if df_1h.empty or len(df_1h) < 30 or trend_1h != "SIDEWAYS":
        return None
    rsi_1h = float(calculate_rsi(df_1h["close"], 14).iloc[-1])
    if np.isnan(rsi_1h) or not (30.0 <= rsi_1h <= 40.0):
        return None

    opens = df_1h["open"].to_numpy(dtype=float)
    closes = df_1h["close"].to_numpy(dtype=float)
    n = len(df_1h)

    # Current candle must just be starting as green, preceded by a red candle
    if not (closes[-1] > opens[-1]):
        return None
    curr_red_idx = n - 2
    if curr_red_idx < 5:
        return None
    if not (closes[curr_red_idx] < opens[curr_red_idx]):
        return None

    # Walk backwards from curr_red_idx - 1 to find consecutive green candles (treated as 2-5 green block)
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

    if green_count < 2:
        return None

    leg_start_open = opens[g_start]
    leg_end_close = closes[g_end]
    if leg_start_open <= 0:
        return None
    pump_pct = ((leg_end_close - leg_start_open) / leg_start_open) * 100.0
    if pump_pct < 1.0:
        return None

    # Find prior red candle before the green cluster
    prev_red_idx = g_start - 1
    while prev_red_idx >= 0 and closes[prev_red_idx] >= opens[prev_red_idx]:
        prev_red_idx -= 1
    if prev_red_idx < 0:
        return None

    y_prev_red = closes[prev_red_idx]
    y_curr_red = closes[curr_red_idx]
    if not _no_body_touch_between(opens, closes, prev_red_idx, curr_red_idx, y_prev_red, y_curr_red):
        return None

    price = float(df_1h["close"].iloc[-1])
    return {
        "scanner": "ICT",
        "timeframe": "60",
        "price": f"{price:.8f}".rstrip("0").rstrip("."),
        "metric": f"SIDEWAYS · RSI {rsi_1h:.1f} · +{pump_pct:.2f}%",
        "detail": f"{green_count}G Block >=1% · Clean Red Trendline · Next Green Started",
    }


def _scan_symbol(symbol: str, base_url: str, shared_session: requests.Session) -> List[Dict[str, Any]]:
    if _stop_event.is_set() or not symbol.endswith("USDT"):
        return []

    with _lock:
        en_v1 = SCANNER_ENABLED["V1"]
        en_ict = SCANNER_ENABLED["ICT"]
        en_hb = SCANNER_ENABLED["HB"]
        en_pa = SCANNER_ENABLED["PA"]

    if not (en_v1 or en_ict or en_hb or en_pa):
        return []

    found: List[Dict[str, Any]] = []
    try:
        df_1h = fetch_klines_df(shared_session, symbol, base_url, "1h", 250)
        if df_1h.empty or len(df_1h) < 50:
            return []

        trend_1h = _detect_trend_1h(df_1h) if (en_pa or en_ict) else None

        df_30m = (
            fetch_klines_df(shared_session, symbol, base_url, "30m", 100)
            if (en_v1 or en_hb) else pd.DataFrame()
        )
        df_1d = (
            fetch_klines_df(shared_session, symbol, base_url, "1d", 250)
            if en_v1 else pd.DataFrame()
        )

        if en_v1:
            sig_v1 = check_v1_rule(df_1h, df_1d, df_30m)
            if sig_v1:
                found.append(sig_v1)

        if en_hb:
            sig_hb = check_hb_rule(df_1h, df_30m)
            if sig_hb:
                found.append(sig_hb)

        if en_pa:
            sig_pa = check_pa_rule(symbol, df_1h, trend_1h)
            if sig_pa:
                found.append(sig_pa)

        if en_ict:
            sig_ict = check_ict_rule(df_1h, trend_1h)
            if sig_ict:
                found.append(sig_ict)

        now_dt = datetime.now(timezone.utc)
        now_str = format_pkt_time(now_dt)
        now_ts = now_dt.timestamp()
        for item in found:
            item["symbol"] = symbol
            item["market"] = MARKET_TYPE
            item["detected_at"] = now_str
            item["created_ts"] = now_ts
        return found
    except Exception:
        return found


def _bg_loop() -> None:
    session = requests.Session()
    cached_symbols: List[str] = []
    last_sym_fetch = 0.0
    working_url = BINANCE_ENDPOINTS[0]

    while not _stop_event.is_set():
        try:
            with _lock:
                any_enabled = any(SCANNER_ENABLED.values())
                SCAN_STATUS["running"] = any_enabled
                SCAN_STATUS["market"] = MARKET_TYPE
                SCAN_STATUS["owner"] = OWNER_LABEL

            if not any_enabled:
                time.sleep(1)
                continue

            now = time.time()
            if not cached_symbols or (now - last_sym_fetch) >= SYMBOL_REFRESH_SECONDS:
                cached_symbols, working_url = get_spot_usdt_symbols(session)
                last_sym_fetch = time.time()

            with _lock:
                SCAN_STATUS["scanned"] = 0
                SCAN_STATUS["total"] = len(cached_symbols)
                SCAN_STATUS["working_url"] = working_url

            with ThreadPoolExecutor(max_workers=14) as pool:
                futures = [pool.submit(_scan_symbol, sym, working_url, session) for sym in cached_symbols]
                for fut in as_completed(futures):
                    if _stop_event.is_set():
                        break
                    with _lock:
                        if not any(SCANNER_ENABLED.values()):
                            break
                    res_list = fut.result()
                    now_ts = time.time()
                    with _lock:
                        for item in res_list:
                            # Deduplicate exact scanner+symbol+metric while keeping history until Clear
                            dup = any(
                                s.get("scanner") == item["scanner"]
                                and s.get("symbol") == item["symbol"]
                                and s.get("metric") == item["metric"]
                                for s in LATEST_SIGNALS[:30]
                            )
                            if not dup:
                                LATEST_SIGNALS.insert(0, item)
                                del LATEST_SIGNALS[1000:]
                        SCAN_STATUS["scanned"] += 1

            with _lock:
                SCAN_STATUS["last_cycle"] = format_pkt_time(datetime.now(timezone.utc))
        except Exception:
            pass

        for _ in range(3):
            if _stop_event.is_set():
                break
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
    with _lock:
        return {
            "status": dict(SCAN_STATUS),
            "enabled": dict(SCANNER_ENABLED),
            "signals": list(LATEST_SIGNALS),
        }


@app.post("/api/control")
def control_scanners(payload: ControlPayload):
    with _lock:
        if payload.clear_all:
            LATEST_SIGNALS.clear()
            PA_LAST_SIGNAL_PRICE.clear()
        if payload.scanner in SCANNER_ENABLED and payload.enabled is not None:
            SCANNER_ENABLED[payload.scanner] = bool(payload.enabled)
            SCAN_STATUS["running"] = any(SCANNER_ENABLED.values())
        return JSONResponse(
            {
                "ok": True,
                "status": dict(SCAN_STATUS),
                "enabled": dict(SCANNER_ENABLED),
                "signals_count": len(LATEST_SIGNALS),
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
    .wrap {
      max-width: 1560px;
      margin: 0 auto;
    }
    .top-brand {
      text-align: center;
      padding: 12px 16px 14px;
      margin-bottom: 16px;
      border-bottom: 1px solid #1b2434;
    }
    .top-brand h1 {
      margin: 0;
      font-size: 26px;
      letter-spacing: 2px;
      font-weight: 900;
      color: #ffffff;
    }
    .status-line {
      margin-top: 8px;
      font-size: 12px;
      color: #94a3b8;
      display: flex;
      justify-content: center;
      gap: 18px;
      flex-wrap: wrap;
      align-items: center;
    }
    .status-line b { color: #38bdf8; }
    .clear-btn {
      background: #1e1318;
      color: #fda4af;
      border: 1px solid #881337;
      border-radius: 7px;
      padding: 5px 12px;
      font-size: 12px;
      font-weight: 700;
      cursor: pointer;
    }
    .clear-btn:hover { background: #3b0d1c; }

    .main-layout {
      display: flex;
      flex-direction: row;
      gap: 16px;
      align-items: flex-start;
      width: 100%;
    }
    .stack-list {
      flex: 1 1 auto;
      min-width: 0;
      display: flex;
      flex-direction: column;
      gap: 14px;
    }
    .side-panel {
      flex: 0 0 270px;
      width: 270px;
      position: sticky;
      top: 16px;
    }
    .brand-card {
      background: linear-gradient(180deg, #0b1915 0%, #090e18 100%);
      border: 1px solid #1c352c;
      border-radius: 14px;
      padding: 20px 16px;
      text-align: center;
      box-shadow: 0 10px 28px rgba(0, 0, 0, 0.45);
    }
    .bismillah-top {
      font-size: 12px;
      font-weight: 800;
      letter-spacing: 1.6px;
      color: #6ee7b7;
      margin-bottom: 12px;
      padding-bottom: 10px;
      border-bottom: 1px solid rgba(110, 231, 183, 0.16);
    }
    .brand-bars {
      display: inline-flex;
      align-items: flex-end;
      gap: 3px;
      height: 18px;
      margin-bottom: 8px;
    }
    .brand-bars span {
      width: 4px;
      border-radius: 2px;
      background: #34d399;
    }
    .brand-bars span:nth-child(1) { height: 7px; opacity: 0.65; }
    .brand-bars span:nth-child(2) { height: 12px; opacity: 0.85; }
    .brand-bars span:nth-child(3) { height: 18px; }
    .brand-name {
      font-size: 17px;
      font-weight: 800;
      letter-spacing: 0.8px;
      color: #ffffff;
      line-height: 1.25;
    }
    .brand-sub {
      font-size: 11px;
      color: #94a3b8;
      margin-top: 6px;
    }
    .contact-box {
      margin-top: 14px;
      padding-top: 12px;
      border-top: 1px solid #1c2738;
    }
    .contact-lbl {
      font-size: 10px;
      font-weight: 700;
      letter-spacing: 1.4px;
      color: #64748b;
      text-transform: uppercase;
    }
    .contact-num {
      margin-top: 5px;
      font-size: 15px;
      font-weight: 800;
      letter-spacing: 0.8px;
      color: #34d399;
    }

    .mini-tabs {
      display: flex;
      gap: 8px;
      flex-wrap: wrap;
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
      letter-spacing: 0.6px;
      cursor: pointer;
    }
    .mini-tab.active {
      border-color: #34d399;
      color: #34d399;
      background: #0d1f1b;
    }
    .start-panel {
      background: #0b1220;
      border: 1px dashed #2b3d5c;
      border-radius: 10px;
      padding: 14px 16px;
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
      padding: 8px 16px;
      font-size: 12px;
      font-weight: 900;
      cursor: pointer;
      letter-spacing: 0.6px;
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
    .t-ICT { color: #fbbf24; }
    .t-HB { color: #60a5fa; }
    .t-PA { color: #c084fc; }
    .sec-actions {
      display: flex;
      align-items: center;
      gap: 8px;
    }
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
      min-height: 54px;
      align-items: center;
    }
    .coin-box {
      background: #101726;
      border: 1px solid #28354d;
      border-radius: 9px;
      padding: 8px 12px;
      min-width: 170px;
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
      margin-top: 3px;
      line-height: 1.3;
    }
    .coin-time {
      font-size: 10px;
      color: #94a3b8;
      margin-top: 3px;
    }
    .empty-msg {
      color: #64748b;
      font-size: 12px;
    }
    @media (max-width: 960px) {
      .main-layout { flex-direction: column; }
      .side-panel { flex: 1 1 auto; width: 100%; position: static; }
    }
  </style>
</head>
<body>
  <div class="wrap">
    <div class="top-brand">
      <h1>FREEDOM WITH AHMII</h1>
      <div class="status-line">
        <span>Market: <b>SPOT USDT</b></span>
        <span>Scanned: <b id="stScan">0/0</b></span>
        <span>Signals: <b id="stTotal">0</b></span>
        <span>Cycle: <b id="stCycle">--</b></span>
        <span>Click coin to open <b>TradingView</b></span>
        <button class="clear-btn" onclick="clearAllSignals()">Clear Signals</button>
      </div>
    </div>

    <div class="main-layout">
      <div class="stack-list">
        <div class="mini-tabs">
          <button class="mini-tab" id="tab-V1" onclick="selectScanner('V1')">V1</button>
          <button class="mini-tab" id="tab-ICT" onclick="selectScanner('ICT')">ICT</button>
          <button class="mini-tab" id="tab-HB" onclick="selectScanner('HB')">HIDDEN BULLISH</button>
          <button class="mini-tab" id="tab-PA" onclick="selectScanner('PA')">PRICE ACTION</button>
        </div>

        <div class="start-panel">
          <div>
            <b id="activeScannerLabel">V1</b> scanner:
            <span id="activeScannerState" class="empty-msg">Stopped (Start Scan to run)</span>
          </div>
          <button class="start-btn" id="toggleScanBtn" onclick="toggleActiveScanner()">Start Scan</button>
        </div>

        <div class="section-row">
          <div class="section-head">
            <span class="section-title t-V1">V1 METHOD</span>
            <div class="sec-actions">
              <button class="start-btn" id="btn-V1" onclick="toggleScanner('V1')">Start Scan</button>
              <span class="section-cnt" id="cnt-V1">0</span>
            </div>
          </div>
          <div class="coin-strip" id="list-V1"></div>
        </div>

        <div class="section-row">
          <div class="section-head">
            <span class="section-title t-ICT">ICT</span>
            <div class="sec-actions">
              <button class="start-btn" id="btn-ICT" onclick="toggleScanner('ICT')">Start Scan</button>
              <span class="section-cnt" id="cnt-ICT">0</span>
            </div>
          </div>
          <div class="coin-strip" id="list-ICT"></div>
        </div>

        <div class="section-row">
          <div class="section-head">
            <span class="section-title t-HB">HIDDEN BULLISH</span>
            <div class="sec-actions">
              <button class="start-btn" id="btn-HB" onclick="toggleScanner('HB')">Start Scan</button>
              <span class="section-cnt" id="cnt-HB">0</span>
            </div>
          </div>
          <div class="coin-strip" id="list-HB"></div>
        </div>

        <div class="section-row">
          <div class="section-head">
            <span class="section-title t-PA">PRICE ACTION</span>
            <div class="sec-actions">
              <button class="start-btn" id="btn-PA" onclick="toggleScanner('PA')">Start Scan</button>
              <span class="section-cnt" id="cnt-PA">0</span>
            </div>
          </div>
          <div class="coin-strip" id="list-PA"></div>
        </div>
      </div>

      <aside class="side-panel">
        <div class="brand-card">
          <div class="bismillah-top">ALHUMDULLILHA</div>
          <div class="brand-bars">
            <span></span><span></span><span></span>
          </div>
          <div class="brand-name">FREEDOM WITH AHMII</div>
          <div class="brand-sub">Binance Spot USDT Scanner</div>
          <div class="contact-box">
            <div class="contact-lbl">Contact Number</div>
            <div class="contact-num">03254112787</div>
          </div>
        </div>
      </aside>
    </div>
  </div>

  <script>
    const CODES = ["V1", "ICT", "HB", "PA"];
    const NAMES = { V1: "V1", ICT: "ICT", HB: "HIDDEN BULLISH", PA: "PRICE ACTION" };
    let selectedScanner = "V1";
    let enabledMap = { V1: false, ICT: false, HB: false, PA: false };

    function updateControlsUI() {
      CODES.forEach(c => {
        const tab = document.getElementById(`tab-${c}`);
        if (tab) tab.classList.toggle("active", c === selectedScanner);
        const btn = document.getElementById(`btn-${c}`);
        if (btn) {
          const isOn = !!enabledMap[c];
          btn.textContent = isOn ? "Stop Scan" : "Start Scan";
          btn.classList.toggle("running", isOn);
        }
      });

      const activeOn = !!enabledMap[selectedScanner];
      document.getElementById("activeScannerLabel").textContent = NAMES[selectedScanner];
      const stEl = document.getElementById("activeScannerState");
      stEl.textContent = activeOn ? "Running (Binance Spot USDT)" : "Stopped — click Start Scan";
      const mainBtn = document.getElementById("toggleScanBtn");
      mainBtn.textContent = activeOn ? "Stop Scan" : "Start Scan";
      mainBtn.classList.toggle("running", activeOn);
    }

    function selectScanner(code) {
      selectedScanner = code;
      updateControlsUI();
    }

    async function setScannerState(code, enabled) {
      enabledMap[code] = enabled;
      updateControlsUI();
      try {
        await fetch("/api/control", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ scanner: code, enabled })
        });
      } catch (e) {}
      poll();
    }

    function toggleScanner(code) {
      setScannerState(code, !enabledMap[code]);
    }

    function toggleActiveScanner() {
      toggleScanner(selectedScanner);
    }

    async function clearAllSignals() {
      try {
        await fetch("/api/control", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ clear_all: true })
        });
      } catch (e) {}
      poll();
    }

    function renderSection(code, items) {
      const boxWrap = document.getElementById(`list-${code}`);
      const cntEl = document.getElementById(`cnt-${code}`);
      cntEl.textContent = String(items.length);
      boxWrap.innerHTML = "";

      if (!items.length) {
        const em = document.createElement("div");
        em.className = "empty-msg";
        em.textContent = enabledMap[code] ? "Scanning Spot USDT..." : "Click Start Scan to run";
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
          <div class="coin-sub">${s.price || ""} · ${s.metric || ""}</div>
          <div class="coin-meta">${s.detail || ""}</div>
          <div class="coin-time">Time: ${s.detected_at || ""}</div>
        `;
        boxWrap.appendChild(card);
      }
    }

    async function poll() {
      try {
        const r = await fetch("/api/signals");
        const d = await r.json();
        const st = d.status || {};
        if (d.enabled) {
          enabledMap = { ...enabledMap, ...d.enabled };
          updateControlsUI();
        }
        const all = d.signals || [];

        document.getElementById("stScan").textContent = `${st.scanned || 0}/${st.total || 0}`;
        document.getElementById("stTotal").textContent = String(all.length);
        document.getElementById("stCycle").textContent = st.last_cycle || "--";

        CODES.forEach(c => {
          renderSection(c, all.filter(x => x.scanner === c));
        });
      } catch (e) {}
    }

    updateControlsUI();
    poll();
    setInterval(poll, 2000);
  </script>
</body>
</html>
"""


@app.get("/", response_class=HTMLResponse)
def home() -> str:
    return HTML_PAGE
