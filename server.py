from fastapi import FastAPI
from fastapi.responses import HTMLResponse

app = FastAPI()

HTML_PAGE = """<!DOCTYPE html>
<html>
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <style>
    html, body {
      margin: 0;
      padding: 0;
      width: 100%;
      height: 100%;
      background: #000000;
      color: #ffffff;
      font-family: Arial, sans-serif;
      display: flex;
      align-items: center;
      justify-content: center;
      overflow: hidden;
    }
    .brand {
      font-size: 28px;
      font-weight: 700;
      letter-spacing: 2px;
      text-align: center;
    }
  </style>
</head>
<body>
  <div class="brand">FREEDOM WITH AHMII</div>
</body>
</html>
"""


@app.get("/", response_class=HTMLResponse)
def home() -> str:
  return HTML_PAGE
