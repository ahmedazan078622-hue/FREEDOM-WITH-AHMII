import threading
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse

OWNER_LABEL = "03254112787"
PKT_TZ = timezone(timedelta(hours=5))

app = FastAPI(title="FREEDOM WITH AHMII | Clean Workspace")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

ITEMS: List[Dict[str, Any]] = []
STATUS: Dict[str, Any] = {
    "running": False,
    "last_updated": None,
    "owner": OWNER_LABEL,
}

_lock = threading.Lock()
_stop_event = threading.Event()


def now_pkt_str() -> str:
    return datetime.now(timezone.utc).astimezone(PKT_TZ).strftime("%H:%M:%S PKT")


def _worker_loop() -> None:
    with _lock:
        STATUS["running"] = True
        STATUS["last_updated"] = now_pkt_str()

    while not _stop_event.is_set():
        with _lock:
            STATUS["last_updated"] = now_pkt_str()
        time.sleep(5)

    with _lock:
        STATUS["running"] = False


@app.on_event("startup")
def on_startup() -> None:
    _stop_event.clear()
    threading.Thread(target=_worker_loop, daemon=True).start()


@app.on_event("shutdown")
def on_shutdown() -> None:
    _stop_event.set()


@app.get("/api/state")
def get_state() -> Dict[str, Any]:
    with _lock:
        return {
            "status": dict(STATUS),
            "items": list(ITEMS),
        }


HTML_PAGE = """<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>FREEDOM WITH AHMII | Clean Start</title>
  <style>
    * { box-sizing: border-box; font-family: Arial, sans-serif; }
    body {
      margin: 0;
      background: #0b101b;
      color: #f1f5f9;
      padding: 20px;
    }
    .wrap { max-width: 1200px; margin: 0 auto; }
    .card {
      background: #131b2e;
      border: 1px solid #26344f;
      border-radius: 10px;
      padding: 16px;
      margin-bottom: 14px;
    }
    h1 { margin: 0 0 6px; font-size: 20px; color: #38bdf8; }
    p { margin: 0; color: #94a3b8; font-size: 13px; }
    .meta {
      display: flex;
      gap: 18px;
      flex-wrap: wrap;
      margin-top: 12px;
      font-size: 13px;
    }
    .meta b { color: #34d399; }
  </style>
</head>
<body>
  <div class="wrap">
    <div class="card">
      <h1>FREEDOM WITH AHMII</h1>
      <p>Clean workspace ready for new features.</p>
      <div class="meta">
        <span>Owner: <b id="stOwner">03254112787</b></span>
        <span>Updated: <b id="stTime">--</b></span>
        <span>Status: <b id="stRun">IDLE</b></span>
      </div>
    </div>

    <div class="card" id="workspace">
      <p>Workspace is clean. Tell me what you want to add next.</p>
    </div>
  </div>

  <script>
    async function syncState() {
      try {
        const res = await fetch("/api/state");
        const data = await res.json();
        const st = data.status || {};
        if (st.owner) document.getElementById("stOwner").textContent = st.owner;
        document.getElementById("stTime").textContent = st.last_updated || "--";
        document.getElementById("stRun").textContent = st.running ? "RUNNING" : "IDLE";
      } catch (e) {
        document.getElementById("stRun").textContent = "OFFLINE";
      }
    }
    syncState();
    setInterval(syncState, 2000);
  </script>
</body>
</html>
"""


@app.get("/", response_class=HTMLResponse)
def home() -> str:
    return HTML_PAGE
