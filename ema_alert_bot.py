#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
EMA High/Low Alert Bot — BTC-USDT-SWAP (5m / 4h) — OKX
=======================================================
Аналог индикатора "High/Low EMA Area" (Pine v6).

Логика сигнала на последней ЗАКРЫТОЙ свече:
    maHigh = EMA(high, len)
    Сигнал: close[prev] >= maHigh[prev]  И  close[curr] < maHigh[curr]
    len = 2400 для 5m, 200 для 4h.

Данные тянутся через /api/v5/market/history-candles с пагинацией,
чтобы EMA(2400) на 5m считалась честно, а не по 300 свечам.

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
import time
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
    "5m": 2400,
    "4h": 200,
}

TF_TO_OKX_BAR = {
    "5m": "5m",
    "4h": "4H",
}

# Множитель запаса: сколько свечей тянем относительно длины EMA.
# Нужен, чтобы EMA "разогрелась" и последние значения совпадали с TradingView.
WARMUP_FACTOR = 3

# OKX history-candles: лимит одной страницы
PAGE_LIMIT = 100

# Максимум страниц на всякий случай (защита от бесконечного цикла)
MAX_PAGES = 100

# Пауза между запросами (OKX rate limit: 20 req / 2 sec на history-candles)
SLEEP_BETWEEN_PAGES = 0.12


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


def fetch_page(symbol: str, bar: str, after_ms: int | None) -> list[list]:
    """
    Одна страница history-candles.
    after_ms = timestamp в мс: вернёт свечи СТАРШЕ этого времени.
    Если after_ms is None — вернёт самые свежие.
    """
    url = "https://www.okx.com/api/v5/market/history-candles"
    params = {"instId": symbol, "bar": bar, "limit": str(PAGE_LIMIT)}
    if after_ms is not None:
        params["after"] = str(after_ms)

    try:
        resp = requests.get(url, params=params, timeout=20)
        resp.raise_for_status()
        payload = resp.json()
    except Exception as e:
        fail(f"Ошибка запроса к OKX: {e}")

    if payload.get("code") != "0":
        fail(f"OKX вернул ошибку: {payload.get('msg', 'неизвестно')}")

    return payload.get("data") or []


def fetch_data(symbol: str, bar: str, needed: int) -> pd.DataFrame:
    """
    Тянет нужное количество закрытых свечей через пагинацию.
    Возвращает DataFrame, отсортированный по возрастанию времени.
    """
    log(f"Загрузка {symbol} {bar} с OKX (нужно ~{needed} свечей)...")

    collected: dict[int, dict] = {}
    after_ms: int | None = None
    pages = 0

    while len(collected) < needed and pages < MAX_PAGES:
        page = fetch_page(symbol, bar, after_ms)
        pages += 1

        if not page:
            # Дальше истории нет
            break

        new_added = 0
        for k in page:
            if k[8] != "1":  # только закрытые
                continue
            ts = int(k[0])
            if ts in collected:
                continue
            collected[ts] = {
                "open_time": pd.to_datetime(ts, unit="ms", utc=True),
                "Open":  float(k[1]),
                "High":  float(k[2]),
                "Low":   float(k[3]),
                "Close": float(k[4]),
                "Volume": float(k[5]),
            }
            new_added += 1

        # Курсор — самая старая свеча на странице (минимальный ts)
        oldest_ts = min(int(k[0]) for k in page)
        after_ms = oldest_ts

        if new_added == 0:
            # Страница не дала новых данных — на всякий случай выходим
            break

        if len(collected) < needed:
            time.sleep(SLEEP_BETWEEN_PAGES)

    if not collected:
        fail("OKX не вернул ни одной закрытой свечи")

    df = (
        pd.DataFrame(list(collected.values()))
        .set_index("open_time")
        .sort_index()
    )

    log(f"Получено {len(df)} закрытых свечей за {pages} стр.")
    return df


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
        "candles_used": len(df),
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

    needed = length * WARMUP_FACTOR
    df = fetch_data(TICKER, bar, needed)

    if len(df) < length + 5:
        log(f"[{tf}] Недостаточно свечей для EMA({length}): {len(df)} — пропуск.")
        return

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
        f"Свечей в расчёте:    {sig['candles_used']}\n"
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
        "candles_used": sig["candles_used"],
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
