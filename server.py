import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd
import requests
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse

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
ALERT_HISTORY: Dict[str, float] = {}
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


def format_pkt_time(dt_utc: datetime) -> str:
    return dt_utc.astimezone(PKT_TZ).strftime("%H:%M:%S PKT")


def prune_expired_signals_locked(now_ts: float) -> None:
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


def fetch_json_with_fallback(
    session: requests.Session,
    path: str,
    params: Optional[Dict[str, Any]] = None,
    preferred_url: Optional[str] = None,
    timeout: int = 10,
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
    """Fetch only active Binance Spot USDT pairs."""
    info_data, working_url = fetch_json_with_fallback(session, "/api/v3/exchangeInfo", timeout=12)
    valid_spot = set()
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
            session, "/api/v3/ticker/24hr", preferred_url=working_url, timeout=12
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


def fetch_klines_df(
    session: requests.Session, symbol: str, base_url: str, interval: str, limit: int
) -> pd.DataFrame:
    raw, _ = fetch_json_with_fallback(
        session,
        "/api/v3/klines",
        params={"symbol": symbol, "interval": interval, "limit": limit},
        preferred_url=base_url,
        timeout=10,
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
    """V1: 1D aur 1H trend UP, aur 1H RSI 53 se 57."""
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
    """HB: 1H RSI 25 se 40 zone mein, aur 30m RSI > 1H RSI."""
    if df_1h.empty or df_30m.empty or len(df_1h) < 30 or len(df_30m) < 30:
        return None
    rsi_1h = float(calculate_rsi(df_1h["close"], 14).iloc[-1])
    rsi_30m = float(calculate_rsi(df_30m["close"], 14).iloc[-1])
    if np.isnan(rsi_1h) or np.isnan(rsi_30m):
        return None
    if not (25.0 <= rsi_1h <= 40.0):
        return None
    if not (rsi_30m > rsi_1h):
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
    """PA: UP trend with 1H RSI 30-40, OR SIDEWAYS trend with 1H RSI 25-40."""
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
    """ICT: SIDEWAYS trend with 1H RSI 30-40."""
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


def check_oc_gap_4h_rule(df_4h: pd.DataFrame):
    """OC GAP: 4H timeframe par recent bullish open-close / imbalance gap."""
    if df_4h.empty or len(df_4h) < 8:
        return None
    opens = df_4h["open"].to_numpy(dtype=float)
    closes = df_4h["close"].to_numpy(dtype=float)
    lows = df_4h["low"].to_numpy(dtype=float)
    highs = df_4h["high"].to_numpy(dtype=float)
    n = len(df_4h)
    curr_price = closes[-1]
    if curr_price <= 0:
        return None

    # Check last 5 4H candles for an active bullish OC gap / FVG
    for idx in range(n - 1, max(2, n - 6), -1):
        prev_close = closes[idx - 1]
        c_open = opens[idx]
        c_close = closes[idx]
        if prev_close <= 0 or c_close <= c_open:
            continue

        oc_gap_pct = ((c_open - prev_close) / prev_close) * 100.0
        fvg_gap_pct = ((lows[idx] - highs[idx - 2]) / max(highs[idx - 2], 1e-12)) * 100.0 if highs[idx - 2] < lows[idx] else 0.0

        if oc_gap_pct >= 0.15 or fvg_gap_pct >= 0.15:
            gap_val = max(oc_gap_pct, fvg_gap_pct)
            zone_low = min(prev_close, c_open, lows[idx])
            zone_high = max(prev_close, c_open, highs[idx])
            if zone_low * 0.985 <= curr_price <= zone_high * 1.03:
                return {
                    "scanner": "OC",
                    "timeframe": "240",
                    "metric": f"4H Gap +{gap_val:.2f}%",
                    "detail": f"Zone {zone_low:.6f}".rstrip("0").rstrip("."),
                    "price": f"{curr_price:.8f}".rstrip("0").rstrip("."),
                }
    return None


def _scan_symbol(symbol: str, base_url: str, shared_session: requests.Session) -> List[Dict[str, Any]]:
    if _stop_event.is_set() or not symbol.endswith("USDT"):
        return []
    found: List[Dict[str, Any]] = []
    try:
        df_1h = fetch_klines_df(shared_session, symbol, base_url, "1h", 250)
        if df_1h.empty or len(df_1h) < 50:
            return []

        rsi_1h_val = float(calculate_rsi(df_1h["close"], 14).iloc[-1])
        trend_1h = _detect_trend_1h(df_1h)

        if not np.isnan(rsi_1h_val) and 53.0 <= rsi_1h_val <= 57.0 and _trend_up(df_1h):
            df_1d = fetch_klines_df(shared_session, symbol, base_url, "1d", 250)
            sig_v1 = check_v1_rule(df_1h, df_1d)
            if sig_v1:
                found.append(sig_v1)

        if not np.isnan(rsi_1h_val) and 25.0 <= rsi_1h_val <= 40.0:
            df_30m = fetch_klines_df(shared_session, symbol, base_url, "30m", 100)
            sig_hb = check_hb_rule(df_1h, df_30m)
            if sig_hb:
                found.append(sig_hb)

        sig_pa = check_pa_rule(df_1h, trend_1h)
        if sig_pa:
            found.append(sig_pa)

        sig_ict = check_ict_rule(df_1h, trend_1h)
        if sig_ict:
            found.append(sig_ict)

        df_4h = fetch_klines_df(shared_session, symbol, base_url, "4h", 30)
        sig_oc = check_oc_gap_4h_rule(df_4h)
        if sig_oc:
            found.append(sig_oc)

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
            now = time.time()
            if not cached_symbols or (now - last_sym_fetch) >= SYMBOL_REFRESH_SECONDS:
                cached_symbols, working_url = get_spot_usdt_symbols(session)
                last_sym_fetch = time.time()

            with _lock:
                prune_expired_signals_locked(time.time())
                SCAN_STATUS["running"] = True
                SCAN_STATUS["scanned"] = 0
                SCAN_STATUS["total"] = len(cached_symbols)
                SCAN_STATUS["market"] = MARKET_TYPE
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
                                del LATEST_SIGNALS[500:]
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
  <title>FREEDOM WITH AHMII</title>
  <style>
    * { box-sizing: border-box; font-family: 'Segoe UI', Arial, sans-serif; }
    body {
      margin: 0;
      background: #000000;
      color: #f8fafc;
      min-height: 100vh;
      padding: 18px 16px 44px;
    }
    .wrap {
      max-width: 1480px;
      margin: 0 auto;
    }
    .top-brand {
      text-align: center;
      padding: 14px 16px 18px;
      margin-bottom: 18px;
      border-bottom: 1px solid #1e293b;
    }
    .top-brand h1 {
      margin: 0;
      font-size: 28px;
      letter-spacing: 2px;
      font-weight: 800;
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
    }
    .status-line b { color: #38bdf8; }
    .stack-list {
      display: flex;
      flex-direction: column;
      gap: 16px;
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
      margin-bottom: 12px;
      padding-bottom: 8px;
      border-bottom: 1px solid #141c2b;
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
    .t-OC { color: #fb7185; }
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
      min-width: 136px;
      cursor: pointer;
      user-select: none;
      transition: 0.15s;
    }
    .coin-box:hover {
      border-color: #38bdf8;
      background: #152036;
    }
    .coin-box.flash {
      border-color: #34d399;
      box-shadow: 0 0 10px rgba(52, 211, 153, 0.35);
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
      font-weight: 600;
    }
    .coin-meta {
      font-size: 10.5px;
      color: #94a3b8;
      margin-top: 2px;
    }
    .empty-msg {
      color: #475569;
      font-size: 12px;
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
        <span>Tip: <b>Double-click coin for TradingView</b></span>
      </div>
    </div>

    <div class="stack-list">
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
          <span class="section-title t-OC">OC GAP</span>
          <span class="section-cnt" id="cnt-OC">0</span>
        </div>
        <div class="coin-strip" id="list-OC"></div>
      </div>
    </div>
  </div>

  <script>
    const CODES = ["V1", "HB", "PA", "ICT", "OC"];
    let seen = new Set();
    let fresh = new Set();
    let first = true;

    function renderSection(code, items) {
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
        const key = `${s.scanner}|${s.symbol}|${s.detected_at}`;
        const card = document.createElement("div");
        card.className = "coin-box" + (fresh.has(key) ? " flash" : "");
        card.ondblclick = () => {
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
        const all = d.signals || [];

        document.getElementById("stScan").textContent = `${st.scanned || 0}/${st.total || 0}`;
        document.getElementById("stTotal").textContent = String(all.length);
        document.getElementById("stCycle").textContent = st.last_cycle || "--";

        const newly = [];
        for (const s of all) {
          const k = `${s.scanner}|${s.symbol}|${s.detected_at}`;
          if (!seen.has(k)) {
            seen.add(k);
            if (!first) newly.push(s);
          }
        }
        fresh = new Set(newly.map(x => `${x.scanner}|${x.symbol}|${x.detected_at}`));

        CODES.forEach(c => {
          renderSection(c, all.filter(x => x.scanner === c));
        });
        first = false;
      } catch (e) {}
    }

    poll();
    setInterval(poll, 2000);
  </script>
</body>
</html>
"""


@app.get("/", response_class=HTMLResponse)
def home() -> str:
    return HTML_PAGE
