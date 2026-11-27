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
    .top-brand {
      background: linear-gradient(180deg, #0b1915 0%, #090e18 100%);
      border: 1px solid #1c352c;
      border-radius: 14px;
      text-align: center;
      padding: 14px 16px 16px;
      margin-bottom: 16px;
      box-shadow: 0 10px 28px rgba(0, 0, 0, 0.45);
    }
    .bismillah-top {
      font-size: 13px;
      font-weight: 800;
      letter-spacing: 2px;
      color: #6ee7b7;
      margin-bottom: 6px;
    }
    .top-brand h1 {
      margin: 0;
      font-size: 26px;
      letter-spacing: 2px;
      font-weight: 900;
      color: #ffffff;
    }
    .top-contact {
      margin-top: 4px;
      font-size: 13px;
      font-weight: 800;
      letter-spacing: 1px;
      color: #34d399;
    }
    .status-line {
      margin-top: 10px;
      padding-top: 10px;
      border-top: 1px solid #1b2434;
      font-size: 12px;
      color: #94a3b8;
      display: flex;
      justify-content: center;
      gap: 18px;
      flex-wrap: wrap;
      align-items: center;
    }
    .status-line b { color: #38bdf8; }
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
    <div class="top-brand">
      <div class="bismillah-top">ALHUMDULLILHA</div>
      <h1>FREEDOM WITH AHMII</h1>
      <div class="top-contact">Contact: 03254112787</div>
      <div class="status-line">
        <span>Market: <b>SPOT USDT</b></span>
        <span>Scanned: <b id="stScan">0/0</b></span>
        <span>Signals: <b id="stTotal">0</b></span>
        <span>Cycle: <b id="stCycle">--</b></span>
        <span>Click coin to open <b>TradingView</b></span>
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
