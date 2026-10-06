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

Свечи кэшируются в state.json: при повторном запуске тянем только новые.
Так 5m-прогон после первого раза занимает <1 сек вместо ~22 сек.

Что обрабатывать — задаётся переменной окружения TF_FILTER
(например "5m" или "4h"). Если не задана — обрабатываются оба ТФ.

Дедуп по свече — через state.json.

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

# ============================================================
#  ПАРЫ: таймфрейм -> длина EMA
#  Здесь же добавляй новые пары, если захочешь.
# ============================================================
TF_LEN = {
    "5m":  2400,
    "4h":  200,
    # "15m": 800,   # пример: раскомментируй и настрой под себя
    # "1h":  400,
}

TF_TO_OKX_BAR = {
    "5m":  "5m",
    "4h":  "4H",
    "15m": "15m",
    "1h":  "1H",
    # "1d": "1D",
}

WARMUP_FACTOR = 3          # сколько свечей тянем сверх длины EMA (для разогрева)
PAGE_LIMIT = 100           # лимит OKX history-candles за 1 запрос
MAX_PAGES = 200            # защита от бесконечного цикла
SLEEP_BETWEEN_PAGES = 0.12 # пауза между запросами, OKX rate limit

# Сколько свечей хранить в state.json (len * WARMUP_FACTOR + запас)
KEEP_EXTRA = 200


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
        json.dumps(state, ensure_ascii=False), encoding="utf-8"
    )


# ---------- Работа с OKX ----------

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


def candles_from_raw(raw: list[list]) -> dict[int, dict]:
    out = {}
    for k in raw:
        if k[8] != "1":
            continue
        ts = int(k[0])
        out[ts] = {
            "t": ts,
            "o": float(k[1]),
            "h": float(k[2]),
            "l": float(k[3]),
            "c": float(k[4]),
            "v": float(k[5]),
        }
    return out


def update_cache(tf: str, bar: str, state: dict, needed: int) -> pd.DataFrame:
    """
    Обновляет кэш свечей для таймфрейма tf.
    - Если кэша нет: полная пагинация (нужно ~ needed свечей).
    - Если кэш есть: тянем только свечи новее самого свежего ts в кэше.
    Возвращает DataFrame, отсортированный по возрастанию времени.
    """
    cache_key = f"candles_{tf}"
    cache: dict[int, dict] = {}
    for item in state.get(cache_key, []):
        try:
            cache[int(item["t"])] = {
                "t": int(item["t"]),
                "o": float(item["o"]),
                "h": float(item["h"]),
                "l": float(item["l"]),
                "c": float(item["c"]),
                "v": float(item["v"]),
            }
        except Exception:
            continue

    if cache:
        newest_ts = max(cache.keys())
        log(f"[{tf}] Кэш: {len(cache)} свечей, самая свежая {pd.to_datetime(newest_ts, unit='ms', utc=True)}")
        # Тянем только то, что новее newest_ts. У OKX history-candles
        # параметр 'after' = "старше указанного ts". Нам нужны свечи НОВЕЕ,
        # поэтому просто делаем один запрос без 'after' — вернёт самые свежие,
        # а потом отфильтруем.
        raw = fetch_page(symbol=TICKER, bar=bar, after_ms=None)
        fresh = candles_from_raw(raw)
        added = 0
        for ts, c in fresh.items():
            if ts > newest_ts:
                cache[ts] = c
                added += 1
        log(f"[{tf}] Дотянуто новых свечей: {added}")
    else:
        log(f"[{tf}] Кэша нет — полная загрузка (нужно ~{needed} свечей)...")
        cache = {}
        after_ms: int | None = None
        pages = 0
        while len(cache) < needed and pages < MAX_PAGES:
            page = fetch_page(TICKER, bar, after_ms)
            pages += 1
            if not page:
                break
            new_added = 0
            for ts, c in candles_from_raw(page).items():
                if ts not in cache:
                    cache[ts] = c
                    new_added += 1
            oldest_ts = min(int(k[0]) for k in page)
            after_ms = oldest_ts
            if new_added == 0:
                break
            if len(cache) < needed:
                time.sleep(SLEEP_BETWEEN_PAGES)
        log(f"[{tf}] Получено {len(cache)} закрытых свечей за {pages} стр.")

    # Обрезаем до разумного размера, чтобы state.json не пух
    keep_n = needed + KEEP_EXTRA
    if len(cache) > keep_n:
        for ts in sorted(cache.keys())[:-keep_n]:
            cache.pop(ts, None)
        log(f"[{tf}] Кэш обрезан до {len(cache)} свечей")

    # Сохраняем обратно в state
    state[cache_key] = [cache[ts] for ts in sorted(cache.keys())]

    df = (
        pd.DataFrame([cache[ts] for ts in sorted(cache.keys())])
        .rename(columns={"t": "ts", "o": "Open", "h": "High", "l": "Low",
                         "c": "Close", "v": "Volume"})
        .assign(open_time=lambda d: pd.to_datetime(d["ts"], unit="ms", utc=True))
        .set_index("open_time")
        .sort_index()
    )
    return df


# ---------- MA / сигнал ----------

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


# ---------- Email ----------

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


# ---------- Обработка одного ТФ ----------

def process_timeframe(tf: str, smtp_host: str, smtp_port: int, state: dict) -> None:
    bar = TF_TO_OKX_BAR.get(tf)
    length = TF_LEN.get(tf)
    if not bar or not length:
        log(f"Таймфрейм {tf} не поддерживается — пропуск.")
        return

    needed = length * WARMUP_FACTOR
    df = update_cache(tf, bar, state, needed)

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


# ---------- main ----------

def main() -> None:
    smtp_host = "smtp.gmail.com"
    smtp_port = 465

    # TF_FILTER позволяет разделить запуски: "5m" или "4h".
    # Если не задан — обрабатываются все ТФ из TF_LEN.
    tf_filter = os.environ.get("TF_FILTER", "").strip()
    if tf_filter:
        if tf_filter not in TF_LEN:
            fail(f"TF_FILTER='{tf_filter}' не найден в TF_LEN")
        timeframes = [tf_filter]
    else:
        timeframes = list(TF_LEN.keys())

    state = load_state()
    for tf in timeframes:
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
