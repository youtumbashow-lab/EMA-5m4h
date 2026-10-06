#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
EMA High/Low Alert Bot — multi-ticker, 5m / 4h — OKX
=====================================================
Аналог индикатора "High/Low EMA Area" (Pine v6).

Логика сигнала на последней ЗАКРЫТОЙ свече:
    maHigh = EMA(high, len)
    Сигнал: close[prev] >= maHigh[prev]  И  close[curr] < maHigh[curr]

Все параметры — в config.json:
  - tickers         : список инструментов
  - timeframes      : { "5m": {"ema_length": 2400}, "4h": {"ema_length": 200} }
  - ema_type        : EMA | SMA
  - instrument_suffix: -USDT-SWAP (своп) или -USDT (спот)
  - warmup_factor   : множитель запаса свечей для разогрева EMA

Свечи кэшируются в state.json: при повторном запуске тянем только новые.
Дедуп сигналов — на пару (ticker, timeframe).

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
CONFIG_PATH = BASE_DIR / "config.json"
STATE_PATH = BASE_DIR / "state.json"

PAGE_LIMIT = 100
MAX_PAGES = 300
SLEEP_BETWEEN_PAGES = 0.12
KEEP_EXTRA = 200

TF_TO_OKX_BAR = {
    "1m":  "1m",
    "3m":  "3m",
    "5m":  "5m",
    "15m": "15m",
    "30m": "30m",
    "1h":  "1H",
    "2h":  "2H",
    "4h":  "4H",
    "6h":  "6H",
    "12h": "12H",
    "1d":  "1D",
    "1w":  "1W",
}


def log(msg: str) -> None:
    stamp = datetime.now(timezone.utc).strftime("%H:%M:%S UTC")
    print(f"[{stamp}] {msg}", flush=True)


def fail(msg: str) -> None:
    log(f"ОШИБКА: {msg}")
    sys.exit(1)


def load_config() -> dict:
    if not CONFIG_PATH.exists():
        fail(f"Не найден {CONFIG_PATH}")
    with open(CONFIG_PATH, encoding="utf-8") as f:
        cfg = json.load(f)
    if not cfg.get("tickers"):
        fail("В config.json пустой список tickers")
    if not cfg.get("timeframes"):
        fail("В config.json пустой объект timeframes")
    return cfg


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


# ---------- OKX ----------

def to_okx_inst(ticker: str, suffix: str) -> str:
    """
    'BTC-USD'   + '-USDT-SWAP'  -> 'BTC-USDT-SWAP'
    'BTC-USDT'  + ''            -> 'BTC-USDT'
    Если тикер уже в формате OKX ('BTC-USDT-SWAP') — возвращаем как есть.
    """
    if ticker.endswith(("-SWAP", "-USDT", "-USDC")):
        return ticker
    base = ticker.split("-")[0]
    return f"{base}{suffix}"


def fetch_page(inst_id: str, bar: str, after_ms: int | None) -> list[list]:
    url = "https://www.okx.com/api/v5/market/history-candles"
    params = {"instId": inst_id, "bar": bar, "limit": str(PAGE_LIMIT)}
    if after_ms is not None:
        params["after"] = str(after_ms)
    try:
        resp = requests.get(url, params=params, timeout=20)
        resp.raise_for_status()
        payload = resp.json()
    except Exception as e:
        raise RuntimeError(f"Ошибка запроса к OKX: {e}")

    if payload.get("code") != "0":
        raise RuntimeError(f"OKX ошибка: {payload.get('msg', 'неизвестно')}")

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


def update_cache(inst_id: str, bar: str, cache_key: str,
                 state: dict, needed: int) -> pd.DataFrame:
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
        log(f"    кэш: {len(cache)} свечей, самая свежая "
            f"{pd.to_datetime(newest_ts, unit='ms', utc=True)}")
        raw = fetch_page(inst_id, bar, after_ms=None)
        fresh = candles_from_raw(raw)
        added = sum(1 for ts in fresh if ts > newest_ts)
        for ts, c in fresh.items():
            if ts > newest_ts:
                cache[ts] = c
        log(f"    дотянуто новых: {added}")
    else:
        log(f"    кэша нет — полная загрузка (~{needed} свечей)...")
        cache = {}
        after_ms: int | None = None
        pages = 0
        while len(cache) < needed and pages < MAX_PAGES:
            page = fetch_page(inst_id, bar, after_ms)
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
        log(f"    получено {len(cache)} закрытых свечей за {pages} стр.")

    keep_n = needed + KEEP_EXTRA
    if len(cache) > keep_n:
        for ts in sorted(cache.keys())[:-keep_n]:
            cache.pop(ts, None)

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
    raise RuntimeError(f"Неподдерживаемый тип MA: {ma_type}")


def check_signal(df: pd.DataFrame, length: int, ma_type: str) -> dict | None:
    if len(df) < 3:
        return None

    df = df.copy()
    df["maHigh"] = compute_ma(df["High"], length, ma_type)
    df["maLow"]  = compute_ma(df["Low"],  length, ma_type)

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


# ---------- Обработка одного тикера ----------

def process_ticker(ticker: str, tf: str, ema_length: int, cfg: dict,
                   state: dict, smtp_host: str, smtp_port: int) -> None:
    suffix = cfg.get("instrument_suffix", "-USDT-SWAP")
    ma_type = cfg.get("ema_type", "EMA")
    warmup_factor = int(cfg.get("warmup_factor", 3))

    bar = TF_TO_OKX_BAR.get(tf)
    if not bar:
        log(f"  [{ticker} {tf}] Неизвестный ТФ — пропуск.")
        return

    inst_id = to_okx_inst(ticker, suffix)
    needed = ema_length * warmup_factor
    cache_key = f"candles_{inst_id}_{tf}"

    log(f"  [{ticker} {tf}] inst={inst_id}, EMA={ema_length}, нужно ~{needed}")

    try:
        df = update_cache(inst_id, bar, cache_key, state, needed)
    except Exception as e:
        log(f"  [{ticker} {tf}] Ошибка загрузки: {e}")
        return

    if len(df) < ema_length + 5:
        log(f"  [{ticker} {tf}] мало свечей для EMA({ema_length}): {len(df)}")
        return

    sig = check_signal(df, ema_length, ma_type)
    if not sig:
        log(f"  [{ticker} {tf}] пересечения нет.")
        return

    candle_time = sig["candle_time"]
    dedup_key = f"last_signal_candle_{inst_id}_{tf}"
    if state.get(dedup_key) == candle_time:
        log(f"  [{ticker} {tf}] сигнал по свече {candle_time} уже отправлялся.")
        return

    subject = f"[EMA] {inst_id} ({tf}) — цена закрылась ниже верхней границы"
    body = (
        f"Сигнал EMA High/Low.\n\n"
        f"Инструмент:          {inst_id}\n"
        f"Таймфрейм:           {tf}\n"
        f"Длина MA:            {ema_length}\n\n"
        f"— EMA Alert Bot (OKX / GitHub Actions)"
    )

    try:
        send_email(subject, body, smtp_host, smtp_port)
    except Exception as e:
        log(f"  [{ticker} {tf}] Не удалось отправить email: {e}")
        return

    state[dedup_key] = candle_time
    log(f"  [{ticker} {tf}] EMAIL ОТПРАВЛЕН")


# ---------- main ----------

def main() -> None:
    smtp_host = "smtp.gmail.com"
    smtp_port = 465

    cfg = load_config()
    tickers = cfg["tickers"]
    timeframes = cfg["timeframes"]

    state = load_state()

    log(f"Тикеров: {len(tickers)}, ТФ: {list(timeframes.keys())}")

    for tf, tf_cfg in timeframes.items():
        ema_length = int(tf_cfg["ema_length"])
        log(f"=== ТФ {tf} (EMA {ema_length}) ===")
        for ticker in tickers:
            try:
                process_ticker(ticker, tf, ema_length, cfg,
                               state, smtp_host, smtp_port)
            except SystemExit:
                raise
            except Exception as e:
                log(f"  [{ticker} {tf}] Ошибка: {e}")

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
