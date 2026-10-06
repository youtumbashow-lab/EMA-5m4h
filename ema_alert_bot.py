#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
EMA High/Low Alert Bot — BTC-USDT-SWAP (5m / 4h) — OKX
=======================================================
Аналог индикатора "High/Low EMA Area" (Pine v6), но в виде Python-бота.

Логика сигнала:
    maLow  = EMA(low,  len)
    maHigh = EMA(high, len)
    len = 2400 для 5m, 200 для 4h.

Сигнал: на последней ЗАКРЫТОЙ свече close пересёк maHigh сверху вниз:
    close[prev] >= maHigh[prev]  И  close[curr] < maHigh[curr]

Запускается внешним триггером (cron-job.org -> workflow_dispatch).
Дедуп по свече — через state.json (кэшируется между запусками Actions).

Секреты (Settings -> Secrets and variables -> Actions):
  EMAIL_TO, EMAIL_USER, EMAIL_APP_PASSWORD
"""

import json
import os
import smtplib
import ssl
import sys
from datetime import datetime, timezone
from email.header import Header
from email.mime.text import MIMEText
from pathlib import Path

import pandas as pd
import requests

BASE_DIR = Path(__file__).resolve().parent
STATE_PATH = BASE_DIR / "state.json"

TICKER = "BTC-USDT-SWAP"
EMA_TYPE = "EMA"

TF_LEN = {
    "5m":  2400,
    "4h":  200,
}

TF_TO_OKX_BAR = {
    "5m": "5m",
    "4h": "4H",
}

CANDLES_LIMIT = 300


def log(msg: str) -> None:
    stamp = datetime.now(timezone.utc).strftime("%H:%M:%S UTC")
    print(f"[{stamp}] {msg}", flush=True)


def fail(msg: str) -> None:
    log(f"ОШИБКА: {msg}")
    sys.exit(1)


def load_state() -> dict:
    if STATE_PATH.exists():
        try:
            with open(STATE_PATH, encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return {}
    return {}


def save_state(state: dict) -> None:
    STATE_PATH.write_text(
        json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8"
    )


def fetch_data(symbol: str, bar: str) -> pd.DataFrame:
    log(f"Загрузка {symbol} {bar} с OKX...")
    url = "https://www.okx.com/api/v5/market/candles"
    params = {"instId": symbol, "bar": bar, "limit": str(CANDLES_LIMIT)}
    try:
        resp = requests.get(url, params=params, timeout=20)
        resp.raise_for_status()
        payload = resp.json()
    except Exception as e:
        fail(f"Ошибка запроса к OKX: {e}")

    if payload.get("code") != "0":
        fail(f"OKX вернул ошибку: {payload.get('msg', 'неизвестно')}")

    raw = payload.get("data") or []
    if not raw:
        fail(f"OKX вернул пустой ответ для {symbol} {bar}")

    rows = []
    for k in raw:
        if k[8] != "1":
            continue
        rows.append({
            "open_time": pd.to_datetime(int(k[0]), unit="ms", utc=True),
            "Open":  float(k[1]),
            "High":  float(k[2]),
            "Low":   float(k[3]),
            "Close": float(k[4]),
            "Volume": float(k[5]),
        })

    if not rows:
        fail("OKX не вернул ни одной закрытой свечи")

    return pd.DataFrame(rows).set_index("open_time").sort_index()


def ema(series: pd.Series, length: int) -> pd.Series:
    return series.ewm(span=length, adjust=False).mean()


def sma(series: pd.Series, length: int) -> pd.Series:
    return series.rolling(length).mean()


def compute_ma(series: pd.Series, length: int, ma_type: str) -> pd.Series:
    mt = ma_type.upper()
    if mt == "EMA":
        return ema(series, length)
    if mt == "SMA":
        return sma(series, length)
    fail(f"Неподдерживаемый тип MA: {ma_type}")
    return series


def check_signal(df: pd.DataFrame, length: int) -> dict | None:
    if len(df) < 3:
        return None

    df = df.copy()
    df["maHigh"] = compute_ma(df["High"], length, EMA_TYPE)
    df["maLow"]  = compute_ma(df["Low"],  length, EMA_TYPE)

    curr = df.iloc[-1]
    prev = df.iloc[-2]

    if pd.isna(curr["maHigh"]) or pd.isna(prev["maHigh"]):
        return None

    crossed_below = (prev["Close"] >= prev["maHigh"]) and (curr["Close"] < curr["maHigh"])
    if not crossed_below:
        return None

    return {
        "candle_time": str(df.index[-1]),
        "close": float(curr["Close"]),
        "maHigh": float(curr["maHigh"]),
        "maLow": float(curr["maLow"]),
        "length": length,
        "bar_high": float(curr["High"]),
        "bar_low": float(curr["Low"]),
    }


def send_email(subject: str, body: str, smtp_host: str, smtp_port: int) -> None:
    email_to   = os.environ.get("EMAIL_TO", "").strip()
    email_user = os.environ.get("EMAIL_USER", "").strip()
    email_pass = os.environ.get("EMAIL_APP_PASSWORD", "").replace(" ", "")
    missing = [n for n, v in (("EMAIL_TO", email_to),
                              ("EMAIL_USER", email_user),
                              ("EMAIL_APP_PASSWORD", email_pass)) if not v]
    if missing:
        fail(f"Не заданы секреты: {', '.join(missing)}")
    msg = MIMEText(body, "plain", "utf-8")
    msg["Subject"] = Header(subject, "utf-8")
    msg["From"] = email_user
    msg["To"] = email_to
    ctx = ssl.create_default_context()
    with smtplib.SMTP_SSL(smtp_host, smtp_port, context=ctx) as s:
        s.login(email_user, email_pass)
        s.send_message(msg)


def process_timeframe(tf: str, smtp_host: str, smtp_port: int, state: dict) -> None:
    bar = TF_TO_OKX_BAR.get(tf)
    length = TF_LEN.get(tf)
    if not bar or not length:
        log(f"Таймфрейм {tf} не поддерживается — пропуск.")
        return

    df = fetch_data(TICKER, bar)
    sig = check_signal(df, length)
    if not sig:
        log(f"[{tf}] Пересечения close ниже maHigh({length}) нет.")
        return

    candle_time = sig["candle_time"]
    state_key = f"last_signal_candle_{tf}"
    if state.get(state_key) == candle_time:
        log(f"[{tf}] Сигнал по свече {candle_time} уже отправлялся — пропуск.")
        return

    subject = f"[EMA] {TICKER} ({tf}) — цена закрылась ниже верхней границы"
    body = (
        f"Сигнал EMA High/Low.\n\n"
        f"Инструмент:          {TICKER}\n"
        f"Таймфрейм:           {tf}\n"
        f"Длина MA:            {length}\n"
        f"Свеча (UTC):         {candle_time}\n"
        f"Close:               {sig['close']:.2f}\n"
        f"High бара:           {sig['bar_high']:.2f}\n"
        f"Low бара:            {sig['bar_low']:.2f}\n"
        f"MA High (верхняя):   {sig['maHigh']:.2f}\n"
        f"MA Low  (нижняя):    {sig['maLow']:.2f}\n\n"
        f"Условие: close предыдущей свечи >= MA High, "
        f"close текущей свечи < MA High.\n\n"
        f"— EMA Alert Bot (OKX / GitHub Actions)"
    )

    try:
        send_email(subject, body, smtp_host, smtp_port)
    except Exception as e:
        fail(f"Не удалось отправить email: {e}")

    state[state_key] = candle_time
    state[f"last_signal_{tf}"] = {
        "time_utc": candle_time,
        "close": sig["close"],
        "maHigh": sig["maHigh"],
        "maLow": sig["maLow"],
    }
    log(f"[{tf}] EMAIL ОТПРАВЛЕН: {subject}")


def main() -> None:
    smtp_host = "smtp.gmail.com"
    smtp_port = 465

    state = load_state()
    for tf in ("5m", "4h"):
        try:
            process_timeframe(tf, smtp_host, smtp_port, state)
        except SystemExit:
            raise
        except Exception as e:
            log(f"[{tf}] Ошибка обработки: {e}")

    save_state(state)
    log("Готово.")


if __name__ == "__main__":
    try:
        main()
    except SystemExit:
        raise
    except Exception as e:
        log(f"Непредвиденная ошибка: {e}")
        sys.exit(1)
