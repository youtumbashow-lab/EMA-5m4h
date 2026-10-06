#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
EMA High/Low + MACD Alert Bot — multi-ticker, 5m / 4h — OKX
============================================================
Аналог индикатора "High/Low EMA Area" (Pine v6) + MACD-фильтр.

Сигнал на последней ЗАКРЫТОЙ свече при ОДНОВРЕМЕННОМ выполнении:

  EMA:   close[prev] >= maHigh[prev]  И  close[curr] < maHigh[curr]
         (maHigh = MA(high, ema_length))
  MACD1: signal(9) < 0
  MACD2: смена тёмно-красной -> светло-красной гистограммы
         (hist < 0: падение сменилось ростом)

Все параметры — в config.json:
  - tickers, timeframes
  - ema_type, instrument_suffix, warmup_factor
  - macd.fast / macd.slow / macd.signal

Все сработавшие сигналы за прогон собираются в один список
и отправляются ОДНИМ письмом. Если сигналов нет — письмо не шлётся.

Кэш свечей хранится в state.json в пределах последних KEEP_CANDLES свечей.

Логи:
  - по умолчанию печатаются только итоги и сигналы;
  - при DEBUG=1 — подробный лог по каждому тикеру и страницам OKX.

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

KEEP_CANDLES = 5000
RETRY_DELAY_SEC = 20

DEBUG = os.environ.get("DEBUG", "").strip() == "1"

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


def dbg(msg: str) -> None:
    if DEBUG:
        log(msg)


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
    cfg.setdefault("macd", {"fast": 12, "slow": 26, "signal": 9})
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


def _trim_cache(cache: dict[int, dict]) -> None:
    if len(cache) > KEEP_CANDLES:
        for ts in sorted(cache.keys())[:-KEEP_CANDLES]:
            cache.pop(ts, None)


def _update_from_okx(cache: dict[int, dict], inst_id: str, bar: str,
                     newest_ts: int | None) -> int:
    raw = fetch_page(inst_id, bar, after_ms=None)
    fresh = candles_from_raw(raw)
    added = 0
    for ts, c in fresh.items():
        if newest_ts is None or ts > newest_ts:
            if ts not in cache:
                cache[ts] = c
                added += 1
    return added


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
        dbg(f"    кэш: {len(cache)} свечей, самая свежая "
            f"{pd.to_datetime(newest_ts, unit='ms', utc=True)}")
        added = _update_from_okx(cache, inst_id, bar, newest_ts)
        dbg(f"    дотянуто новых: {added}")

        if added == 0:
            dbg(f"    новых свечей нет — retry через {RETRY_DELAY_SEC} сек...")
            time.sleep(RETRY_DELAY_SEC)
            added2 = _update_from_okx(cache, inst_id, bar, newest_ts)
            dbg(f"    после retry дотянуто: {added2}")
    else:
        dbg(f"    кэша нет — полная загрузка (~{needed} свечей)...")
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
        dbg(f"    получено {len(cache)} закрытых свечей за {pages} стр.")

    _trim_cache(cache)

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


# ---------- MA / MACD / сигнал ----------

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


def compute_macd(df: pd.DataFrame, fast: int, slow: int, sig_len: int) -> pd.DataFrame:
    close = df["Close"]
    ema_fast = close.ewm(span=fast, adjust=False).mean()
    ema_slow = close.ewm(span=slow, adjust=False).mean()
    macd_line = ema_fast - ema_slow
    signal_line = macd_line.ewm(span=sig_len, adjust=False).mean()
    out = df.copy()
    out["macd"] = macd_line
    out["macd_signal"] = signal_line
    out["hist"] = macd_line - signal_line
    return out


def hist_color(hist: float, hist_prev: float) -> str:
    if hist == 0:
        return "neutral"
    if hist > 0 and hist < hist_prev:
        return "light_green"
    if hist > 0:
        return "dark_green"
    if hist < 0 and hist > hist_prev:
        return "light_red"
    return "dark_red"


def check_signal(df: pd.DataFrame, length: int, ma_type: str,
                 fast: int, slow: int, sig_len: int) -> dict | None:
    """
    Проверяет три условия на последней закрытой свече:
      1) close[prev] >= maHigh[prev] и close[curr] < maHigh[curr]
      2) macd_signal[curr] < 0
      3) hist-цвет сменился dark_red -> light_red
    """
    min_bars = max(length, slow + sig_len) + 5
    if len(df) < min_bars:
        return None

    df = df.copy()
    df["maHigh"] = compute_ma(df["High"], length, ma_type)
    df["maLow"]  = compute_ma(df["Low"],  length, ma_type)
    df = compute_macd(df, fast, slow, sig_len)

    curr = df.iloc[-1]
    prev = df.iloc[-2]
    before = df.iloc[-3]

    if pd.isna(curr["maHigh"]) or pd.isna(prev["maHigh"]):
        return None
    if pd.isna(curr["macd_signal"]) or pd.isna(curr["hist"]) or pd.isna(prev["hist"]):
        return None

    # 1) пересечение close ниже maHigh
    crossed_below = (prev["Close"] >= prev["maHigh"]) and (curr["Close"] < curr["maHigh"])
    if not crossed_below:
        return None

    # 2) signal < 0
    if not (float(curr["macd_signal"]) < 0):
        return None

    # 3) смена dark_red -> light_red
    c_curr = hist_color(float(curr["hist"]), float(prev["hist"]))
    c_prev = hist_color(float(prev["hist"]), float(before["hist"]))
    if not (c_prev == "dark_red" and c_curr == "light_red"):
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
        "macd_signal": float(curr["macd_signal"]),
        "hist": float(curr["hist"]),
        "hist_prev": float(prev["hist"]),
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
                   state: dict, signals: list) -> None:
    suffix = cfg.get("instrument_suffix", "-USDT-SWAP")
    ma_type = cfg.get("ema_type", "EMA")
    warmup_factor = int(cfg.get("warmup_factor", 3))
    macd_cfg = cfg.get("macd", {})
    macd_fast = int(macd_cfg.get("fast", 12))
    macd_slow = int(macd_cfg.get("slow", 26))
    macd_sig  = int(macd_cfg.get("signal", 9))

    bar = TF_TO_OKX_BAR.get(tf)
    if not bar:
        dbg(f"  [{ticker} {tf}] Неизвестный ТФ — пропуск.")
        return

    inst_id = to_okx_inst(ticker, suffix)
    needed = max(ema_length, macd_slow + macd_sig) * warmup_factor
    cache_key = f"candles_{inst_id}_{tf}"

    dbg(f"  [{ticker} {tf}] inst={inst_id}, EMA={ema_length}, "
        f"MACD={macd_fast}/{macd_slow}/{macd_sig}, нужно ~{needed}")

    try:
        df = update_cache(inst_id, bar, cache_key, state, needed)
    except Exception as e:
        log(f"  [{ticker} {tf}] Ошибка загрузки: {e}")
        return

    if len(df) < max(ema_length, macd_slow + macd_sig) + 5:
        dbg(f"  [{ticker} {tf}] мало свечей: {len(df)}")
        return

    sig = check_signal(df, ema_length, ma_type, macd_fast, macd_slow, macd_sig)
    if not sig:
        dbg(f"  [{ticker} {tf}] условий нет.")
        return

    candle_time = sig["candle_time"]
    dedup_key = f"last_signal_candle_{inst_id}_{tf}"
    if state.get(dedup_key) == candle_time:
        dbg(f"  [{ticker} {tf}] сигнал по свече {candle_time} уже отправлялся.")
        return

    state[dedup_key] = candle_time
    signals.append({
        "inst_id": inst_id,
        "tf": tf,
        "ema_length": ema_length,
        "candle_time": candle_time,
        "close": sig["close"],
        "maHigh": sig["maHigh"],
        "macd_signal": sig["macd_signal"],
        "hist": sig["hist"],
        "hist_prev": sig["hist_prev"],
    })
    log(f"  [{ticker} {tf}] СИГНАЛ "
        f"(close {sig['close']:.6f} < maHigh {sig['maHigh']:.6f}, "
        f"signal {sig['macd_signal']:.4f} < 0, hist {sig['hist_prev']:.6f} -> {sig['hist']:.6f})")


# ---------- main ----------

def main() -> None:
    smtp_host = "smtp.gmail.com"
    smtp_port = 465

    cfg = load_config()
    tickers = cfg["tickers"]
    timeframes = cfg["timeframes"]

    state = load_state()
    signals: list = []

    log(f"Тикеров: {len(tickers)}, ТФ: {list(timeframes.keys())}"
        + ("" if DEBUG else " (DEBUG=1 для подробного лога)"))

    for tf, tf_cfg in timeframes.items():
        ema_length = int(tf_cfg["ema_length"])
        dbg(f"=== ТФ {tf} (EMA {ema_length}) ===")
        for ticker in tickers:
            try:
                process_ticker(ticker, tf, ema_length, cfg, state, signals)
            except SystemExit:
                raise
            except Exception as e:
                log(f"  [{ticker} {tf}] Ошибка: {e}")

    save_state(state)

    if not signals:
        log("Сигналов нет — письмо не отправляется.")
        log("Готово.")
        return

    log(f"СИГНАЛОВ: {len(signals)} — отправляю одно письмо.")

    subject = f"[EMA+MACD] Сигналы: {len(signals)}"
    if len(signals) == 1:
        s = signals[0]
        subject = f"[EMA+MACD] {s['inst_id']} ({s['tf']})"

    now_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    lines = [f"Сигналы EMA High/Low + MACD — {now_utc}", ""]
    for i, s in enumerate(signals, 1):
        lines.append(
            f"{i}. {s['inst_id']} ({s['tf']}) — "
            f"close {s['close']:.6f} < MA High {s['maHigh']:.6f}, "
            f"signal(9) {s['macd_signal']:.4f} < 0, "
            f"hist {s['hist_prev']:.6f} -> {s['hist']:.6f}"
        )
    lines.append("")
    lines.append("— EMA+MACD Alert Bot (OKX / GitHub Actions)")
    body = "\n".join(lines)

    try:
        send_email(subject, body, smtp_host, smtp_port)
    except Exception as e:
        fail(f"Не удалось отправить email: {e}")

    log(f"EMAIL ОТПРАВЛЕН: {subject}")
    log("Готово.")


if __name__ == "__main__":
    try:
        main()
    except SystemExit:
        raise
    except Exception as e:
        log(f"Непредвиденная ошибка: {e}")
        sys.exit(1)
