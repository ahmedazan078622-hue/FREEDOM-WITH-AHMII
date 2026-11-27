import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List

import numpy as np
import pandas as pd
import requests
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse

BINANCE_ENDPOINTS = [
    "https://data-api.binance.vision",
    "https://api.binance.com",
]
BINANCE_FUTURES_URL = "https://fapi.binance.com"

OWNER_LABEL = "03254112787"
PKT_TZ = timezone(timedelta(hours=5))
SIGNAL_TTL_SECONDS = 60 * 60
ALERT_COOLDOWN_SECONDS = 900
SYMBOL_REFRESH_SECONDS = 900

EXCLUDED_BASES = {
    "USDT", "USDC", "USDS", "BUSD", "FDUSD", "TUSD", "USDP",
    "DAI", "UST", "USTC", "USDE", "USD1", "PYUSD", "USDD",
    "EUR", "TRY", "BRL", "GBP", "AUD", "BIDR", "PAXG",
}

app = FastAPI(title="FREEDOM WITH AHMII | 4-in-1 Live Crypto Scanners")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

LATEST_SIGNALS: List[Dict[str, Any]] = []
ALERT_HISTORY: Dict[str, float] = {}
SCAN_STATUS: Dict[str, Any] = {
    "running": False,
    "last_cycle": None,
    "scanned": 0,
    "total": 0,
    "working_url": BINANCE_ENDPOINTS[0],
    "owner": OWNER_LABEL,
}
_lock = threading.Lock()
_stop_event = threading.Event()


def format_pkt_time(dt_utc: datetime) -> str:
    return dt_utc.astimezone(PKT_TZ).strftime("%H:%M:%S PKT")


def prune_expired_signals_locked(now_ts: float):
    global LATEST_SIGNALS
    LATEST_SIGNALS = [
        s for s in LATEST_SIGNALS
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


def fetch_json_with_fallback(session: requests.Session, path: str, params=None, preferred_url=None, timeout=12):
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
    info_data, working_url = fetch_json_with_fallback(session, "/api/v3/exchangeInfo", timeout=15)
    valid_spot = set()
    for s in info_data.get("symbols", []):
        base = s.get("baseAsset", "")
        sym = s.get("symbol", "")
        if (
            s.get("quoteAsset") == "USDT"
            and s.get("status") == "TRADING"
            and s.get("isSpotTradingAllowed", True)
            and base not in EXCLUDED_BASES
            and not any(base.endswith(x) for x in ("UP", "DOWN", "BULL", "BEAR"))
        ):
            valid_spot.add(sym)

    try:
        tickers, working_url = fetch_json_with_fallback(
            session, "/api/v3/ticker/24hr", preferred_url=working_url, timeout=15
        )
        vol_map = {}
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


def fetch_klines_df(session: requests.Session, symbol: str, base_url: str, interval: str, limit: int) -> pd.DataFrame:
    raw, _ = fetch_json_with_fallback(
        session,
        "/api/v3/klines",
        params={"symbol": symbol, "interval": interval, "limit": limit},
        preferred_url=base_url,
        timeout=12,
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


def _trend_is_up_ema(df: pd.DataFrame) -> bool:
    if df.empty or len(df) < 208:
        return False
    closes = df["close"].iloc[:-1].tolist()
    e50 = ema_list(closes, 50)[-1]
    e200 = ema_list(closes, 200)[-1]
    return closes[-1] > e50 and closes[-1] > e200


def _has_fast_movement(df_1h: pd.DataFrame, min_move_pct: float = 0.4) -> bool:
    if len(df_1h) < 16:
        return False
    sub = df_1h.iloc[-13:-1]
    moves = []
    for _, row in sub.iterrows():
        if row["low"] > 0:
            moves.append(((row["high"] - row["low"]) / row["low"]) * 100.0)
    return bool(moves) and (sum(moves) / len(moves)) >= min_move_pct


def _sha_green(df: pd.DataFrame, smooth_len: int = 10, after_len: int = 10) -> bool:
    if df.empty or len(df) < 40:
        return False
    opens = df["open"].tolist()
    highs = df["high"].tolist()
    lows = df["low"].tolist()
    closes = df["close"].tolist()

    so = ema_list(opens, smooth_len)
    sh = ema_list(highs, smooth_len)
    sl = ema_list(lows, smooth_len)
    sc = ema_list(closes, smooth_len)

    ha_close = [(so[i] + sh[i] + sl[i] + sc[i]) / 4.0 for i in range(len(df))]
    ha_open = [0.0] * len(df)
    ha_open[0] = (so[0] + sc[0]) / 2.0
    for i in range(1, len(df)):
        ha_open[i] = (ha_open[i - 1] + ha_close[i - 1]) / 2.0

    final_open = ema_list(ha_open, after_len)
    final_close = ema_list(ha_close, after_len)
    return final_close[-1] > final_open[-1]


def check_v1_method(df_1h: pd.DataFrame, df_30m: pd.DataFrame, df_1d: pd.DataFrame):
    if not _trend_is_up_ema(df_1h):
        return None
    if not _has_fast_movement(df_1h, 0.4):
        return None
    if not _sha_green(df_1h):
        return None

    rsi_1h_s = calculate_rsi(df_1h["close"], 14)
    rsi_1h = float(rsi_1h_s.iloc[-1])
    prev_rsi_1h = float(rsi_1h_s.iloc[-2])
    if np.isnan(rsi_1h) or np.isnan(prev_rsi_1h):
        return None
    if not (rsi_1h > prev_rsi_1h and 52.0 <= rsi_1h <= 58.0):
        return None

    if not _trend_is_up_ema(df_1d):
        return None
    if not _sha_green(df_30m):
        return None

    rsi_30m_s = calculate_rsi(df_30m["close"], 14)
    rsi_30m = float(rsi_30m_s.iloc[-1])
    prev_rsi_30m = float(rsi_30m_s.iloc[-2])
    if np.isnan(rsi_30m) or np.isnan(prev_rsi_30m):
        return None
    if not (rsi_30m > rsi_1h and rsi_30m >= prev_rsi_30m and rsi_30m <= 80.0):
        return None

    price = float(df_1h["close"].iloc[-1])
    target = price * 1.01
    return {
        "scanner": "V1",
        "timeframe": "30",
        "col1": f"{rsi_1h:.2f}",
        "col2": f"{rsi_30m:.2f}",
        "col3": f"{target:.8f}".rstrip("0").rstrip("."),
        "col4": "1D+1H UP | SHA",
        "price": f"{price:.8f}".rstrip("0").rstrip("."),
    }


def check_hideen_bullish(df_1h: pd.DataFrame, df_30m: pd.DataFrame):
    if df_1h.empty or df_30m.empty or len(df_1h) < 130 or len(df_30m) < 30:
        return None

    rsi_1h = calculate_rsi(df_1h["close"], 14)
    rsi_30m = calculate_rsi(df_30m["close"], 14)

    live_1h_rsi = float(rsi_1h.iloc[-1])
    prev_1h_rsi = float(rsi_1h.iloc[-2])
    live_30m_rsi = float(rsi_30m.iloc[-1])

    if np.isnan(live_1h_rsi) or np.isnan(prev_1h_rsi) or np.isnan(live_30m_rsi):
        return None
    if not (25.0 <= live_1h_rsi <= 40.0):
        return None
    if not (live_30m_rsi > live_1h_rsi and live_1h_rsi > prev_1h_rsi):
        return None

    lows = df_1h["low"].to_numpy(dtype=float)
    highs = df_1h["high"].to_numpy(dtype=float)
    closes = df_1h["close"].to_numpy(dtype=float)
    rsis = rsi_1h.to_numpy(dtype=float)

    n = len(df_1h)
    live_idx = n - 1
    curr_low = min(float(lows[-1]), float(lows[-2]))

    for prev_pos in range(live_idx - 4, max(1, live_idx - 101), -1):
        if np.isnan(rsis[prev_pos]):
            continue
        if not (lows[prev_pos] <= min(lows[prev_pos - 2 : prev_pos]) and lows[prev_pos] <= min(lows[prev_pos + 1 : prev_pos + 3])):
            continue

        bar_diff = live_idx - prev_pos
        prev_low = float(lows[prev_pos])
        prev_rsi = float(rsis[prev_pos])

        if not (18.0 <= prev_rsi <= 48.0):
            continue

        mid_high = float(np.max(highs[prev_pos + 1 : live_idx]))
        if mid_high <= max(prev_low, curr_low) * 1.003:
            continue

        if not (curr_low < prev_low and (live_1h_rsi - prev_rsi) >= 1.0):
            continue

        rsi_slope = (live_1h_rsi - prev_rsi) / bar_diff
        broken = any(
            rsis[prev_pos + j] < (prev_rsi + rsi_slope * j - 0.25)
            for j in range(1, bar_diff)
        )
        if broken:
            continue

        price = float(closes[-1])
        return {
            "scanner": "HB",
            "timeframe": "60",
            "col1": f"{live_1h_rsi:.2f}",
            "col2": f"{live_30m_rsi:.2f}",
            "col3": f"{prev_rsi:.2f}",
            "col4": f"{bar_diff} bars",
            "price": f"{price:.8f}".rstrip("0").rstrip("."),
        }
    return None


def _detect_1h_trend_pa(df_1h: pd.DataFrame, lookback: int = 36):
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


def _count_1h_reactions(prices_arr, level, tol=0.006):
    if level is None or level <= 0 or len(prices_arr) == 0:
        return 0
    return int(np.sum(np.abs(prices_arr - level) / max(level, 1e-12) <= tol))


def check_price_action(df_1h: pd.DataFrame, df_30m: pd.DataFrame, df_12h: pd.DataFrame, session: requests.Session, symbol: str):
    if df_1h.empty or df_30m.empty or df_12h.empty or len(df_1h) < 50 or len(df_30m) < 30 or len(df_12h) < 15:
        return None

    trend_1h = _detect_1h_trend_pa(df_1h)
    if trend_1h not in ("UP", "SIDEWAYS"):
        return None

    rsi_1h_now = float(calculate_rsi(df_1h["close"], 14).iloc[-1])
    rsi_30m_now = float(calculate_rsi(df_30m["close"], 14).iloc[-1])
    if np.isnan(rsi_1h_now) or np.isnan(rsi_30m_now):
        return None

    if trend_1h == "SIDEWAYS" and not (25.0 <= rsi_1h_now <= 40.0):
        return None
    if trend_1h == "UP" and not (30.0 <= rsi_1h_now <= 40.0):
        return None
    if not (rsi_30m_now > rsi_1h_now):
        return None

    curr_close = float(df_1h["close"].iloc[-1])
    curr_low = float(df_1h["low"].iloc[-1])

    highs = df_12h["high"].to_numpy(dtype=float)
    lows = df_12h["low"].to_numpy(dtype=float)
    closes = df_12h["close"].to_numpy(dtype=float)
    vols = df_12h["volume"].to_numpy(dtype=float)
    n = len(df_12h)

    avg_vol = float(np.mean(vols)) if n > 0 else 1.0
    liq_low_scores = {}
    liq_high_scores = {}

    for i in range(2, n - 1):
        vw = max(1.0, vols[i] / max(avg_vol, 1e-9))
        if lows[i] <= lows[i - 1] and lows[i] <= lows[i + 1]:
            lvl = float(lows[i])
            liq_low_scores[lvl] = liq_low_scores.get(lvl, 0.0) + vw
        if highs[i] >= highs[i - 1] and highs[i] >= highs[i + 1]:
            lvl = float(highs[i])
            liq_high_scores[lvl] = liq_high_scores.get(lvl, 0.0) + vw

    for lvl in list(liq_low_scores.keys()):
        c = sum(1 for o in liq_low_scores if abs(o - lvl) / max(lvl, 1e-12) <= 0.007)
        if c >= 2:
            liq_low_scores[lvl] *= (1.0 + 0.65 * c)

    for lvl in list(liq_high_scores.keys()):
        c = sum(1 for o in liq_high_scores if abs(o - lvl) / max(lvl, 1e-12) <= 0.007)
        if c >= 2:
            liq_high_scores[lvl] *= (1.0 + 0.65 * c)

    try:
        oi_resp = session.get(f"{BINANCE_FUTURES_URL}/fapi/v1/openInterest", params={"symbol": symbol}, timeout=4)
        if oi_resp.status_code == 200 and float(oi_resp.json().get("openInterest", 0.0)) > 0:
            for ap in closes[-4:]:
                for pct in (0.035, 0.060, 0.090):
                    ll = round(float(ap * (1.0 - pct)), 8)
                    sl = round(float(ap * (1.0 + pct)), 8)
                    liq_low_scores[ll] = liq_low_scores.get(ll, 0.0) + 1.25
                    liq_high_scores[sl] = liq_high_scores.get(sl, 0.0) + 1.25
    except Exception:
        pass

    valid_lows = {k: v for k, v in liq_low_scores.items() if k <= curr_close * 1.006}
    valid_highs = {k: v for k, v in liq_high_scores.items() if k > curr_close}

    h1_lows = df_1h["low"].iloc[:-1].to_numpy(dtype=float)
    h1_highs = df_1h["high"].iloc[:-1].to_numpy(dtype=float)

    cand_sup = []
    for z, zs in valid_lows.items():
        nb = h1_lows[np.abs(h1_lows - z) / max(z, 1e-12) <= 0.010]
        lvl = float(np.min(nb)) if len(nb) > 0 else float(z)
        touches = _count_1h_reactions(h1_lows, lvl) + len(nb)
        st = float(zs) + 0.6 * touches
        if lvl <= curr_close * 1.006 and (st >= 2.0 or touches >= 2):
            cand_sup.append((lvl, st))

    for i in range(2, len(h1_lows) - 1):
        if h1_lows[i] <= h1_lows[i - 1] and h1_lows[i] <= h1_lows[i + 1]:
            sl = float(h1_lows[i])
            touches = _count_1h_reactions(h1_lows, sl)
            near_12 = any(abs(sl - z) / max(z, 1e-12) <= 0.015 for z in valid_lows)
            st = 0.7 * touches + (1.5 if near_12 else 0.0)
            if sl <= curr_close * 1.006 and touches >= 2 and st >= 2.0:
                cand_sup.append((sl, st))

    if not cand_sup:
        return None

    best_sup, _ = max(cand_sup, key=lambda it: (round(it[1], 2), -abs(curr_close - it[0]) / max(curr_close, 1e-12)))
    min_req_res = best_sup * 1.011

    cand_res = []
    for z, zs in valid_highs.items():
        nb = h1_highs[np.abs(h1_highs - z) / max(z, 1e-12) <= 0.010]
        lvl = float(np.max(nb)) if len(nb) > 0 else float(z)
        touches = _count_1h_reactions(h1_highs, lvl) + len(nb)
        st = float(zs) + 0.6 * touches
        if lvl >= min_req_res and lvl > curr_close and (st >= 2.0 or touches >= 2):
            cand_res.append((lvl, st))

    for i in range(2, len(h1_highs) - 1):
        if h1_highs[i] >= h1_highs[i - 1] and h1_highs[i] >= h1_highs[i + 1]:
            sh = float(h1_highs[i])
            touches = _count_1h_reactions(h1_highs, sh)
            near_12 = any(abs(sh - z) / max(z, 1e-12) <= 0.015 for z in valid_highs)
            st = 0.7 * touches + (1.5 if near_12 else 0.0)
            if sh >= min_req_res and sh > curr_close and touches >= 2 and st >= 2.0:
                cand_res.append((sh, st))

    if not cand_res:
        return None

    best_res, _ = max(cand_res, key=lambda it: (round(it[1], 2), -it[0]))
    gap_pct = ((best_res - best_sup) / best_sup) * 100.0
    if gap_pct < 1.10:
        return None

    upper_zone = best_sup * 1.004
    lower_zone = best_sup * 0.996
    if not (curr_low <= upper_zone and curr_close >= lower_zone):
        return None

    return {
        "scanner": "PA",
        "timeframe": "60",
        "col1": f"{trend_1h} ({rsi_1h_now:.1f})",
        "col2": f"{best_sup:.6f}".rstrip("0").rstrip("."),
        "col3": f"{best_res:.6f}".rstrip("0").rstrip("."),
        "col4": f"{gap_pct:.2f}%",
        "price": f"{curr_close:.8f}".rstrip("0").rstrip("."),
    }


def _is_sideways_ict(df: pd.DataFrame, anchor_idx: int) -> bool:
    end_idx = max(1, anchor_idx)
    start_idx = max(0, end_idx - 24)
    if (end_idx - start_idx) < 10:
        return False
    sub = df.iloc[start_idx:end_idx]
    min_low = float(sub["low"].min())
    max_high = float(sub["high"].max())
    if min_low <= 0:
        return False
    range_pct = ((max_high - min_low) / min_low) * 100.0
    start_p = float(sub["open"].iloc[0])
    end_p = float(sub["close"].iloc[-1])
    net_pct = abs((end_p - start_p) / max(start_p, 1e-12)) * 100.0
    return (range_pct <= 15.0) and (net_pct <= 7.0)


def check_ict(df_1h: pd.DataFrame):
    if df_1h.empty or len(df_1h) < 50:
        return None

    live_rsi = float(calculate_rsi(df_1h["close"], 14).iloc[-1])
    if np.isnan(live_rsi) or not (30.0 <= live_rsi <= 40.0):
        return None

    opens = df_1h["open"].to_numpy(dtype=float)
    highs = df_1h["high"].to_numpy(dtype=float)
    lows = df_1h["low"].to_numpy(dtype=float)
    closes = df_1h["close"].to_numpy(dtype=float)

    curr_idx = len(df_1h) - 1
    if not (closes[curr_idx] < opens[curr_idx]):
        return None

    prev_idx = curr_idx - 1
    if not (closes[prev_idx] > opens[prev_idx]):
        return None

    prev_pump = ((closes[prev_idx] - opens[prev_idx]) / max(opens[prev_idx], 1e-12)) * 100.0
    if prev_pump < 1.0:
        return None

    green_pumps = [round(prev_pump, 2)]
    pullbacks_used = 0
    anchor_idx = None
    earliest = max(2, curr_idx - 25)
    idx = prev_idx - 1

    while idx >= earliest:
        if closes[idx] > opens[idx]:
            p_pct = ((closes[idx] - opens[idx]) / max(opens[idx], 1e-12)) * 100.0
            if p_pct < 1.0:
                return None
            green_pumps.append(round(p_pct, 2))
            idx -= 1
        elif closes[idx] < opens[idx]:
            drop_pct = ((opens[idx] - closes[idx]) / max(opens[idx], 1e-12)) * 100.0
            pb_before = idx - 1
            has_earlier_green = (
                pb_before >= earliest
                and closes[pb_before] > opens[pb_before]
                and ((closes[pb_before] - opens[pb_before]) / max(opens[pb_before], 1e-12)) * 100.0 >= 1.0
            )
            if pullbacks_used < 1 and drop_pct <= 0.8 and has_earlier_green:
                pullbacks_used += 1
                idx -= 1
                continue
            anchor_idx = idx
            break
        else:
            return None

    if anchor_idx is None or (curr_idx - anchor_idx) < 2:
        return None
    if not _is_sideways_ict(df_1h, anchor_idx):
        return None

    p1 = float(closes[anchor_idx])
    p2 = float(closes[curr_idx])
    span = curr_idx - anchor_idx
    wick_touches = 0

    for k in range(1, span):
        mid = anchor_idx + k
        line_val = p1 + (p2 - p1) * (k / span)
        tol = line_val * 0.0015
        body_low = min(opens[mid], closes[mid])
        body_high = max(opens[mid], closes[mid])

        if (body_low - tol) <= line_val <= (body_high + tol):
            return None
        if (lows[mid] - tol) <= line_val <= (highs[mid] + tol):
            wick_touches += 1

    if wick_touches < 1:
        return None

    price = float(closes[curr_idx])
    return {
        "scanner": "ICT",
        "timeframe": "60",
        "col1": f"{live_rsi:.2f}",
        "col2": f"{len(green_pumps)}G / {pullbacks_used}PB",
        "col3": "/".join(str(x) for x in reversed(green_pumps)) + "%",
        "col4": f"{wick_touches} Wick",
        "price": f"{price:.8f}".rstrip("0").rstrip("."),
    }


def _scan_symbol(symbol: str, base_url: str, shared_session: requests.Session) -> List[Dict[str, Any]]:
    if _stop_event.is_set():
        return []
    found: List[Dict[str, Any]] = []
    try:
        df_1h = fetch_klines_df(shared_session, symbol, base_url, "1h", 250)
        if df_1h.empty or len(df_1h) < 50:
            return []

        rsi_1h_val = float(calculate_rsi(df_1h["close"], 14).iloc[-1])
        if np.isnan(rsi_1h_val):
            return []

        if 30.0 <= rsi_1h_val <= 40.0:
            sig_ict = check_ict(df_1h)
            if sig_ict:
                found.append(sig_ict)

        need_30m = (25.0 <= rsi_1h_val <= 40.0) or (52.0 <= rsi_1h_val <= 58.0)
        df_30m = fetch_klines_df(shared_session, symbol, base_url, "30m", 150) if need_30m else pd.DataFrame()

        if 52.0 <= rsi_1h_val <= 58.0 and len(df_1h) >= 210 and not df_30m.empty:
            if _trend_is_up_ema(df_1h) and _has_fast_movement(df_1h, 0.4) and _sha_green(df_1h):
                df_1d = fetch_klines_df(shared_session, symbol, base_url, "1d", 250)
                sig_v1 = check_v1_method(df_1h, df_30m, df_1d)
                if sig_v1:
                    found.append(sig_v1)

        if 25.0 <= rsi_1h_val <= 40.0 and not df_30m.empty:
            sig_hb = check_hideen_bullish(df_1h, df_30m)
            if sig_hb:
                found.append(sig_hb)

        trend_pa = _detect_1h_trend_pa(df_1h)
        if trend_pa in ("UP", "SIDEWAYS"):
            df_12h = fetch_klines_df(shared_session, symbol, base_url, "12h", 60)
            sig_pa = check_price_action(df_1h, df_30m, df_12h, shared_session, symbol)
            if sig_pa:
                found.append(sig_pa)

        now_dt = datetime.now(timezone.utc)
        now_str = format_pkt_time(now_dt)
        now_ts = now_dt.timestamp()
        for item in found:
            item["symbol"] = symbol
            item["detected_at"] = now_str
            item["created_ts"] = now_ts
        return found
    except Exception:
        return []


def _bg_loop():
    session = requests.Session()
    cached_symbols: List[str] = []
    last_sym_fetch = 0.0
    working_url = BINANCE_ENDPOINTS[0]

    while not _stop_event.is_set():
        try:
            now = time.time()
            if not cached_symbols or (now - last_sym_fetch) >= SYMBOL_REFRESH_SECONDS:
                cached_symbols, working_url = get_spot_usdt_symbols(session)
                last_sym_fetch = time.time()

            with _lock:
                prune_expired_signals_locked(time.time())
                SCAN_STATUS["running"] = True
                SCAN_STATUS["scanned"] = 0
                SCAN_STATUS["total"] = len(cached_symbols)
                SCAN_STATUS["working_url"] = working_url
                SCAN_STATUS["owner"] = OWNER_LABEL

            with ThreadPoolExecutor(max_workers=10) as pool:
                futures = [pool.submit(_scan_symbol, sym, working_url, session) for sym in cached_symbols]
                for fut in as_completed(futures):
                    if _stop_event.is_set():
                        break
                    res_list = fut.result()
                    now_ts = time.time()
                    with _lock:
                        prune_expired_signals_locked(now_ts)
                        for item in res_list:
                            cd_key = f"{item['scanner']}:{item['symbol']}"
                            if (now_ts - ALERT_HISTORY.get(cd_key, 0.0)) >= ALERT_COOLDOWN_SECONDS:
                                ALERT_HISTORY[cd_key] = now_ts
                                LATEST_SIGNALS.insert(0, item)
                                del LATEST_SIGNALS[400:]
                        SCAN_STATUS["scanned"] += 1

            with _lock:
                prune_expired_signals_locked(time.time())
                SCAN_STATUS["last_cycle"] = format_pkt_time(datetime.now(timezone.utc))
        except Exception:
            pass

        for _ in range(10):
            if _stop_event.is_set():
                break
            with _lock:
                prune_expired_signals_locked(time.time())
            time.sleep(1)

    with _lock:
        SCAN_STATUS["running"] = False
    session.close()


@app.on_event("startup")
def on_startup():
    _stop_event.clear()
    threading.Thread(target=_bg_loop, daemon=True).start()


@app.on_event("shutdown")
def on_shutdown():
    _stop_event.set()


@app.get("/api/signals")
def get_signals():
    now_ts = time.time()
    with _lock:
        prune_expired_signals_locked(now_ts)
        return {
            "status": dict(SCAN_STATUS),
            "signals": list(LATEST_SIGNALS),
        }


HTML_PAGE = """<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>FREEDOM WITH AHMII | 4 Live Scanners</title>
  <style>
    :root {
      --font-main: 'Segoe UI', Arial, sans-serif;
      --bg: #080d1a;
      --card: #111827;
      --card-elevated: #0d1424;
      --head: #162033;
      --border: #334155;
      --border-soft: #223047;
      --text: #f8fafc;
      --muted: #94a3b8;
    }
    * { box-sizing: border-box; font-family: var(--font-main); }
    body {
      margin: 0;
      font-family: var(--font-main);
      font-size: 14px;
      line-height: 1.5;
      background: radial-gradient(circle at top, #131e36 0%, var(--bg) 62%);
      color: var(--text);
      min-height: 100vh;
    }
    .container {
      max-width: 1540px;
      margin: 0 auto;
      padding: 24px 20px 36px;
    }

    /* Section Cards with Clear Border, Inner Padding & Outer Spacing */
    .section-card {
      background: var(--card);
      border: 1.5px solid var(--border);
      border-radius: 16px;
      padding: 20px 22px;
      margin-bottom: 22px;
      box-shadow: 0 12px 28px rgba(0, 0, 0, 0.35);
    }

    .top-header-row {
      display: flex;
      justify-content: space-between;
      align-items: center;
      flex-wrap: wrap;
      gap: 16px;
      padding-bottom: 18px;
      margin-bottom: 18px;
      border-bottom: 1px solid var(--border-soft);
    }
    .brand h1 {
      margin: 0;
      font-size: 22px;
      font-weight: 700;
      letter-spacing: 0.8px;
      line-height: 1.25;
      background: linear-gradient(90deg, #34d399, #60a5fa);
      -webkit-background-clip: text;
      -webkit-text-fill-color: transparent;
    }
    .brand p {
      margin: 6px 0 0;
      font-size: 13px;
      color: var(--muted);
    }
    .owner-pill {
      padding: 8px 16px;
      border-radius: 999px;
      background: #1d4ed8;
      border: 1px solid #60a5fa;
      color: #ffffff;
      font-size: 14px;
      font-weight: 700;
      letter-spacing: 0.6px;
      margin-left: auto;
    }

    .top-controls-row {
      display: flex;
      justify-content: space-between;
      align-items: center;
      flex-wrap: wrap;
      gap: 16px;
    }
    .meta-strip {
      display: flex;
      gap: 22px;
      flex-wrap: wrap;
      font-size: 13px;
      color: #cbd5e1;
      background: var(--card-elevated);
      padding: 12px 18px;
      border-radius: 12px;
      border: 1px solid var(--border-soft);
    }
    .meta-strip > span {
      display: inline-flex;
      align-items: center;
      gap: 6px;
    }
    .meta-strip b {
      color: #34d399;
      font-weight: 700;
    }
    .actions {
      display: flex;
      gap: 10px;
      flex-wrap: wrap;
      align-items: center;
    }
    button {
      border: 1px solid var(--border);
      background: #1e293b;
      color: var(--text);
      padding: 9px 14px;
      border-radius: 9px;
      font-size: 13px;
      font-weight: 600;
      cursor: pointer;
      transition: 0.15s;
    }
    button:hover { border-color: #60a5fa; }
    button.active-green { background: #059669; border-color: #10b981; }
    button.active-blue { background: #2563eb; border-color: #60a5fa; }

    .scanners-grid {
      display: grid;
      grid-template-columns: repeat(2, minmax(0, 1fr));
      gap: 22px;
    }
    @media (max-width: 1080px) {
      .scanners-grid { grid-template-columns: 1fr; gap: 20px; }
    }

    .scanner-box {
      background: var(--card);
      border: 1.5px solid var(--border);
      border-radius: 16px;
      padding: 18px;
      display: flex;
      flex-direction: column;
      gap: 14px;
      box-shadow: 0 12px 26px rgba(0, 0, 0, 0.35);
      min-height: 350px;
    }
    .scanner-header {
      padding: 14px 16px;
      background: var(--head);
      border: 1px solid var(--border-soft);
      border-radius: 12px;
      display: flex;
      justify-content: space-between;
      align-items: center;
      gap: 14px;
    }
    .scanner-title {
      font-size: 15px;
      font-weight: 700;
      display: flex;
      align-items: center;
      gap: 8px;
    }
    .scanner-sub {
      font-size: 12px;
      color: var(--muted);
      margin-top: 5px;
      line-height: 1.45;
    }
    .dot {
      width: 9px;
      height: 9px;
      border-radius: 50%;
      display: inline-block;
      flex-shrink: 0;
    }
    .c-V1 { color: #34d399; } .d-V1 { background: #10b981; box-shadow: 0 0 8px #10b981; }
    .c-HB { color: #60a5fa; } .d-HB { background: #3b82f6; box-shadow: 0 0 8px #3b82f6; }
    .c-PA { color: #c084fc; } .d-PA { background: #a855f7; box-shadow: 0 0 8px #a855f7; }
    .c-ICT { color: #fbbf24; } .d-ICT { background: #f59e0b; box-shadow: 0 0 8px #f59e0b; }

    .count-pill {
      font-size: 12px;
      font-weight: 700;
      padding: 6px 12px;
      border-radius: 999px;
      background: var(--card-elevated);
      border: 1px solid var(--border);
      white-space: nowrap;
    }
    .table-scroll {
      background: var(--card-elevated);
      border: 1px solid var(--border-soft);
      border-radius: 12px;
      padding: 6px 8px;
      overflow-x: auto;
      max-height: 280px;
      overflow-y: auto;
      flex: 1;
    }
    table {
      width: 100%;
      border-collapse: separate;
      border-spacing: 0 4px;
    }
    th, td {
      padding: 10px 10px;
      text-align: center;
      font-size: 13px;
      white-space: nowrap;
    }
    th {
      background: #10192b;
      color: #cbd5e1;
      font-size: 11.5px;
      font-weight: 700;
      letter-spacing: 0.4px;
      text-transform: uppercase;
      position: sticky;
      top: 0;
      border-bottom: 1px solid var(--border);
    }
    td {
      border-bottom: 1px solid #1b273b;
      color: #e2e8f0;
    }
    tbody tr:hover td {
      background: rgba(59, 130, 246, 0.18);
      cursor: pointer;
    }
    tr.flash td { background: rgba(16, 185, 129, 0.25); }
    .pair {
      font-weight: 700;
      color: #ffffff;
      letter-spacing: 0.3px;
    }
    .key-metric {
      font-weight: 700;
      color: #34d399;
    }
    .empty-box {
      padding: 52px 18px;
      text-align: center;
      color: var(--muted);
      font-size: 13px;
    }
  </style>
</head>
<body>
  <div class="container">
    <div class="section-card topbar">
      <div class="top-header-row">
        <div class="brand">
          <h1>FREEDOM WITH AHMII</h1>
          <p>4 Parallel Live Scanners: V1 METHOD &bull; HIDEEN BULLISH &bull; PRICE ACTION &bull; ICT &bull; Auto-Hide: 20m</p>
        </div>
        <span class="owner-pill" id="ownerBadge">03254112787</span>
      </div>
      <div class="top-controls-row">
        <div class="meta-strip">
          <span>Scanned: <b id="stProgress">0/0</b></span>
          <span>Total Signals: <b id="stCount">0</b></span>
          <span>Cycle: <b id="stCycle">Scanning...</b></span>
          <span>Status: <b id="stFeed">LIVE</b></span>
        </div>
        <div class="actions">
          <button id="soundBtn" class="active-green" onclick="toggleSound()">Sound Alert: ON</button>
          <button onclick="playBeep()">Test Sound</button>
          <button id="notifBtn" onclick="enableNotif()">Desktop Pop-up</button>
        </div>
      </div>
    </div>

    <div class="scanners-grid">
      <div class="scanner-box">
        <div class="scanner-header">
          <div>
            <div class="scanner-title c-V1"><span class="dot d-V1"></span> V1 METHOD</div>
            <div class="scanner-sub">1D+1H Uptrend &bull; 1H RSI (52-58) &bull; 1H+30M SHA (10/10) Green &bull; 30M RSI &gt; 1H RSI</div>
          </div>
          <span class="count-pill" id="cnt-V1">0 Signals</span>
        </div>
        <div class="table-scroll">
          <table>
            <thead><tr><th>Time (PKT)</th><th>Pair</th><th>1H RSI</th><th>30M RSI</th><th>1% Target</th><th>Setup</th><th>Entry</th></tr></thead>
            <tbody id="body-V1"></tbody>
          </table>
          <div class="empty-box" id="empty-V1">Scanning Binance Spot USDT for V1 METHOD...</div>
        </div>
      </div>

      <div class="scanner-box">
        <div class="scanner-header">
          <div>
            <div class="scanner-title c-HB"><span class="dot d-HB"></span> HIDEEN BULLISH</div>
            <div class="scanner-sub">1H Regular Bullish Divergence (RSI 25-40 Rising) &bull; 30M RSI &gt; 1H RSI &bull; Unbroken Line</div>
          </div>
          <span class="count-pill" id="cnt-HB">0 Signals</span>
        </div>
        <div class="table-scroll">
          <table>
            <thead><tr><th>Time (PKT)</th><th>Pair</th><th>1H RSI</th><th>30M RSI</th><th>Prev RSI</th><th>Bars</th><th>Close</th></tr></thead>
            <tbody id="body-HB"></tbody>
          </table>
          <div class="empty-box" id="empty-HB">Scanning Binance Spot USDT for HIDEEN BULLISH...</div>
        </div>
      </div>

      <div class="scanner-box">
        <div class="scanner-header">
          <div>
            <div class="scanner-title c-PA"><span class="dot d-PA"></span> PRICE ACTION</div>
            <div class="scanner-sub">12H Liq + Strong 1H S/R (Gap 1.10%+) &bull; SW (25-40) / UP (30-40) &bull; 30M &gt; 1H RSI</div>
          </div>
          <span class="count-pill" id="cnt-PA">0 Signals</span>
        </div>
        <div class="table-scroll">
          <table>
            <thead><tr><th>Time (PKT)</th><th>Pair</th><th>Trend (RSI)</th><th>1H Support</th><th>1H Resist</th><th>Gap %</th><th>Price</th></tr></thead>
            <tbody id="body-PA"></tbody>
          </table>
          <div class="empty-box" id="empty-PA">Scanning Binance Spot USDT for PRICE ACTION...</div>
        </div>
      </div>

      <div class="scanner-box">
        <div class="scanner-header">
          <div>
            <div class="scanner-title c-ICT"><span class="dot d-ICT"></span> ICT</div>
            <div class="scanner-sub">1H Sideways + 1%+ Green Pump Candles + Wick-Only Trendline Touch (RSI 30-40)</div>
          </div>
          <span class="count-pill" id="cnt-ICT">0 Signals</span>
        </div>
        <div class="table-scroll">
          <table>
            <thead><tr><th>Time (PKT)</th><th>Pair</th><th>1H RSI</th><th>Candles</th><th>Pump %</th><th>Touch</th><th>Price</th></tr></thead>
            <tbody id="body-ICT"></tbody>
          </table>
          <div class="empty-box" id="empty-ICT">Scanning Binance Spot USDT for ICT...</div>
        </div>
      </div>
    </div>
  </div>

  <script>
    let soundEnabled = true;
    let seenKeys = new Set();
    let freshKeys = new Set();
    let firstLoad = true;
    let audioCtx = null;

    function toPktString(tsSec, fallbackStr) {
      if (typeof tsSec === "number" && !Number.isNaN(tsSec)) {
        const d = new Date(tsSec * 1000);
        const hh = d.toLocaleTimeString("en-GB", {
          timeZone: "Asia/Karachi",
          hour12: false,
          hour: "2-digit",
          minute: "2-digit",
          second: "2-digit",
        });
        return `${hh} PKT`;
      }
      return fallbackStr || "";
    }

    function unlockAudio() {
      if (!audioCtx) audioCtx = new (window.AudioContext || window.webkitAudioContext)();
      if (audioCtx.state === "suspended") audioCtx.resume();
    }
    document.addEventListener("click", unlockAudio);

    function playBeep() {
      if (!soundEnabled) return;
      try {
        unlockAudio();
        const now = audioCtx.currentTime;
        [950, 1250, 1550].forEach((freq, idx) => {
          const osc = audioCtx.createOscillator();
          const gain = audioCtx.createGain();
          osc.type = "triangle";
          osc.frequency.setValueAtTime(freq, now + idx * 0.16);
          gain.gain.setValueAtTime(0.2, now + idx * 0.16);
          gain.gain.exponentialRampToValueAtTime(0.001, now + idx * 0.16 + 0.15);
          osc.connect(gain);
          gain.connect(audioCtx.destination);
          osc.start(now + idx * 0.16);
          osc.stop(now + idx * 0.16 + 0.16);
        });
      } catch (e) {}
    }

    function toggleSound() {
      unlockAudio();
      soundEnabled = !soundEnabled;
      const btn = document.getElementById("soundBtn");
      btn.textContent = soundEnabled ? "Sound Alert: ON" : "Sound Alert: OFF";
      btn.className = soundEnabled ? "active-green" : "";
      if (soundEnabled) playBeep();
    }

    async function enableNotif() {
      if (!("Notification" in window)) return;
      if (Notification.permission !== "granted") await Notification.requestPermission();
      const btn = document.getElementById("notifBtn");
      if (Notification.permission === "granted") {
        btn.textContent = "Desktop Pop-up: ON";
        btn.className = "active-blue";
      }
    }

    function showDesktopAlert(s) {
      const names = { V1: "V1 METHOD", HB: "HIDEEN BULLISH", PA: "PRICE ACTION", ICT: "ICT" };
      if ("Notification" in window && Notification.permission === "granted") {
        new Notification(`FREEDOM WITH AHMII [${names[s.scanner] || s.scanner}]`, {
          body: `${s.symbol} | ${s.col1} | Price: ${s.price}`
        });
      }
    }

    function renderBox(code, list) {
      const tbody = document.getElementById(`body-${code}`);
      const empty = document.getElementById(`empty-${code}`);
      const cnt = document.getElementById(`cnt-${code}`);

      cnt.textContent = `${list.length} Signal${list.length === 1 ? "" : "s"}`;
      tbody.innerHTML = "";

      if (list.length === 0) {
        empty.style.display = "block";
        return;
      }
      empty.style.display = "none";

      for (const s of list) {
        const key = `${s.scanner}|${s.symbol}|${s.detected_at}`;
        const tr = document.createElement("tr");
        if (freshKeys.has(key)) tr.className = "flash";
        tr.onclick = () => window.open(
          `https://www.tradingview.com/chart/?symbol=BINANCE:${s.symbol}&interval=${s.timeframe || 60}`,
          "_blank"
        );
        const pktTime = toPktString(s.created_ts, s.detected_at);
        tr.innerHTML = `
          <td>${pktTime}</td>
          <td class="pair">${s.symbol}</td>
          <td class="key-metric">${s.col1}</td>
          <td>${s.col2}</td>
          <td>${s.col3}</td>
          <td>${s.col4}</td>
          <td class="key-metric">${s.price}</td>
        `;
        tbody.appendChild(tr);
      }
    }

    async function refresh() {
      try {
        const res = await fetch("/api/signals");
        const data = await res.json();
        const st = data.status || {};
        const all = data.signals || [];

        if (st.owner) {
          document.getElementById("ownerBadge").textContent = st.owner;
        }
        document.getElementById("stProgress").textContent = `${st.scanned || 0}/${st.total || 0}`;
        document.getElementById("stCount").textContent = all.length;
        document.getElementById("stCycle").textContent = st.last_cycle || "Scanning...";
        document.getElementById("stFeed").textContent = st.running ? "LIVE" : "IDLE";

        let incoming = [];
        for (const s of all) {
          const key = `${s.scanner}|${s.symbol}|${s.detected_at}`;
          if (!seenKeys.has(key)) {
            seenKeys.add(key);
            if (!firstLoad) incoming.push(s);
          }
        }

        if (incoming.length > 0) {
          freshKeys = new Set(incoming.map(x => `${x.scanner}|${x.symbol}|${x.detected_at}`));
          playBeep();
          showDesktopAlert(incoming[0]);
        }

        ["V1", "HB", "PA", "ICT"].forEach(code => {
          renderBox(code, all.filter(x => x.scanner === code));
        });

        firstLoad = false;
      } catch (e) {
        document.getElementById("stFeed").textContent = "OFFLINE";
      }
    }

    refresh();
    setInterval(refresh, 2000);
  </script>
</body>
</html>
"""


@app.get("/", response_class=HTMLResponse)
def home():
    return HTML_PAGE
