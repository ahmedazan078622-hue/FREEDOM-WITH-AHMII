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
      overflow: hidden;
    }
  </style>
</head>
<body></body>
</html>
"""


@app.get("/", response_class=HTMLResponse)
def home() -> str:
  return HTML_PAGE
