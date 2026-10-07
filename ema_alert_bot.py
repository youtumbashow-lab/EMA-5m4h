#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
EMA High/Low + MACD Alert Bot — multi-ticker, 5m / 4h — OKX
============================================================
Аналог индикатора "High/Low EMA Area" (Pine v6) + MACD-фильтр.

Сигнал на последней ЗАКРЫТОЙ свече при ОДНОВРЕМЕННОМ выполнении:
  EMA:   close[curr] < maMid[curr]
  MACD1: signal(9) < 0
  MACD2: hist: dark_red -> light_red
  ДОП:   для 5m — signal(9) на 4h тоже < 0

Режимы (переменная окружения MODE):
  - "alerts" (по умолчанию) — проверка сигналов, письмо при срабатывании.
  - "weekly_report"          — статистика за последние N дней, одно письмо.

Журнал сигналов хранится в state.json (signal_log) и чистится до 30 дней.

Секреты (Settings -> Secrets and variables -> Actions):
  EMAIL_TO, EMAIL_USER, EMAIL_APP_PASSWORD
"""

import json
import os
import smtplib
import ssl
import sys
import time
from datetime import datetime, timedelta, timezone
from email.header import Header
from email.mime.multipart import MIMEMultipart
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
HTTP_RETRIES = 3
HTTP_BACKOFF = (1, 2, 4)

TOP_N = 5

CONFIRM_TF = {
    "5m": "4h",
}

SIGNAL_LOG_KEEP_DAYS = 30

DEBUG = os.environ.get("DEBUG", "").strip() == "1"
MODE = os.environ.get("MODE", "alerts").strip() or "alerts"

TF_TO_OKX_BAR = {
    "1m":  "1m",  "3m": "3m",  "5m": "5m",  "15m": "15m", "30m": "30m",
    "1h": "1H",   "2h": "2H",  "4h": "4H",  "6h": "6H",   "12h": "12H",
    "1d": "1D",   "1w": "1W",
}

TF_TO_TV = {
    "1m": "1", "3m": "3", "5m": "5", "15m": "15", "30m": "30",
    "1h": "60", "2h": "120", "4h": "240", "6h": "360",
    "12h": "720", "1d": "D", "1w": "W",
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
    cfg.setdefault("weekly_report", {})
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

def _http_get(url: str, params: dict) -> dict:
    last_err = None
    for attempt in range(HTTP_RETRIES):
        try:
            resp = requests.get(url, params=params, timeout=20)
            resp.raise_for_status()
            payload = resp.json()
            if payload.get("code") != "0":
                raise RuntimeError(f"OKX ошибка: {payload.get('msg', 'неизвестно')}")
            return payload
        except Exception as e:
            last_err = e
            if attempt < HTTP_RETRIES - 1:
                pause = HTTP_BACKOFF[attempt] if attempt < len(HTTP_BACKOFF) else HTTP_BACKOFF[-1]
                dbg(f"    HTTP ошибка ({e}), retry через {pause}s...")
                time.sleep(pause)
    raise RuntimeError(f"Ошибка запроса к OKX после {HTTP_RETRIES} попыток: {last_err}")


def fetch_instruments(inst_type: str) -> set:
    url = "https://www.okx.com/api/v5/public/instruments"
    payload = _http_get(url, {"instType": inst_type})
    return {item["instId"] for item in payload.get("data", [])}


def validate_tickers(tickers: list) -> list:
    log("Проверка тикеров через /public/instruments...")
    try:
        spot = fetch_instruments("SPOT")
        swap = fetch_instruments("SWAP")
    except Exception as e:
        log(f"  ⚠️  не удалось получить список инструментов: {e}")
        log("  → пропускаю валидацию, работаю со всеми тикерами")
        return tickers

    available = spot | swap
    valid = []
    for t in tickers:
        if t in available:
            valid.append(t)
        else:
            log(f"  ⚠️  {t} не найден на OKX — пропуск")
    log(f"Валидных тикеров: {len(valid)} из {len(tickers)}")
    return valid


def fetch_page(inst_id: str, bar: str, after_ms: int | None) -> list[list]:
    url = "https://www.okx.com/api/v5/market/history-candles"
    params = {"instId": inst_id, "bar": bar, "limit": str(PAGE_LIMIT)}
    if after_ms is not None:
        params["after"] = str(after_ms)
    payload = _http_get(url, params)
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


def load_cache_from_state(state: dict, cache_key: str) -> dict:
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
    return cache


def full_load(inst_id: str, bar: str, needed: int) -> dict:
    cache: dict[int, dict] = {}
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
    return cache


def cache_to_df(cache: dict[int, dict]) -> pd.DataFrame:
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


def compute_macd(close: pd.Series, fast: int, slow: int, sig_len: int) -> pd.DataFrame:
    ema_fast = close.ewm(span=fast, adjust=False).mean()
    ema_slow = close.ewm(span=slow, adjust=False).mean()
    macd_line = ema_fast - ema_slow
    signal_line = macd_line.ewm(span=sig_len, adjust=False).mean()
    hist = macd_line - signal_line
    return pd.DataFrame({
        "macd": macd_line,
        "macd_signal": signal_line,
        "hist": hist,
    })


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


def check_price_vs_mid(df: pd.DataFrame, length: int, ma_type: str) -> dict | None:
    if len(df) < length + 5:
        return None

    df = df.copy()
    maHigh = compute_ma(df["High"], length, ma_type)
    maLow  = compute_ma(df["Low"],  length, ma_type)
    maMid  = (maHigh + maLow) / 2

    curr = df.iloc[-1]

    if pd.isna(curr["Close"]) or pd.isna(maMid.iloc[-1]):
        return None

    return {
        "candle_time": str(df.index[-1]),
        "close": float(curr["Close"]),
        "maMid": float(maMid.iloc[-1]),
    }


def check_macd_condition(df: pd.DataFrame,
                         fast: int, slow: int, sig_len: int) -> dict | None:
    min_bars = slow + sig_len + 5
    if len(df) < min_bars:
        return None

    macd_df = compute_macd(df["Close"], fast, slow, sig_len)

    curr_hist = float(macd_df["hist"].iloc[-1])
    prev_hist = float(macd_df["hist"].iloc[-2])
    before_hist = float(macd_df["hist"].iloc[-3])
    curr_signal = float(macd_df["macd_signal"].iloc[-1])

    if pd.isna(curr_hist) or pd.isna(prev_hist) or pd.isna(before_hist) or pd.isna(curr_signal):
        return None

    if not (curr_signal < 0):
        return None

    c_curr = hist_color(curr_hist, prev_hist)
    c_prev = hist_color(prev_hist, before_hist)
    if not (c_prev == "dark_red" and c_curr == "light_red"):
        return None

    return {
        "macd_signal": curr_signal,
        "hist": curr_hist,
        "hist_prev": prev_hist,
    }


def get_macd_signal_from_cache(state: dict, inst_id: str, tf: str,
                               fast: int, slow: int, sig_len: int) -> float | None:
    cache_key = f"candles_{inst_id}_{tf}"
    cache = load_cache_from_state(state, cache_key)
    if not cache:
        return None
    df = cache_to_df(cache)
    if len(df) < slow + sig_len + 5:
        return None
    macd_df = compute_macd(df["Close"], fast, slow, sig_len)
    val = macd_df["macd_signal"].iloc[-1]
    if pd.isna(val):
        return None
    return float(val)


# ---------- Журнал сигналов ----------

def append_signal_log(state: dict, inst_id: str, tf: str, candle_time: str) -> None:
    log_list = state.setdefault("signal_log", [])
    log_list.append({
        "inst_id": inst_id,
        "tf": tf,
        "candle_time": candle_time,
        "recorded_at": datetime.now(timezone.utc).isoformat(),
    })
    cutoff = datetime.now(timezone.utc) - timedelta(days=SIGNAL_LOG_KEEP_DAYS)
    cleaned = []
    for entry in log_list:
        try:
            ts = datetime.fromisoformat(entry.get("recorded_at", ""))
        except Exception:
            continue
        if ts >= cutoff:
            cleaned.append(entry)
    state["signal_log"] = cleaned


# ---------- Email ----------

def _tv_url(inst_id: str, tf: str) -> str:
    base = inst_id.replace("-SWAP", "").replace("-", "")
    is_swap = inst_id.endswith("-SWAP")
    tv_sym = f"BYBIT:{base}.P" if is_swap else f"BYBIT:{base}"
    tv_tf = TF_TO_TV.get(tf, "240")
    return f"https://www.tradingview.com/chart/?symbol={tv_sym}&interval={tv_tf}"


def _sort_signals_by_strength(signals: list) -> list:
    return sorted(signals, key=lambda s: s["macd_signal"])


def build_html_body(signals: list, now_utc: str) -> str:
    top = _sort_signals_by_strength(signals)[:TOP_N]
    rest = _sort_signals_by_strength(signals)[TOP_N:]

    rows = []
    for s in top:
        tv = _tv_url(s["inst_id"], s["tf"])
        rows.append(f"""
        <tr>
          <td style="padding:8px 12px;border-bottom:1px solid #eee;">
            <a href="{tv}" style="color:#0366d6;text-decoration:none;font-weight:600;">
              {s['inst_id']}
            </a>
          </td>
          <td style="padding:8px 12px;border-bottom:1px solid #eee;text-align:center;">
            {s['tf']}
          </td>
        </tr>""")

    rest_line = ""
    if rest:
        rest_items = " · ".join(
            f"{s['inst_id'].replace('-USDT-SWAP', '').replace('-USDT', '')} ({s['tf']})"
            for s in rest
        )
        rest_line = (
            f'<div style="margin-top:16px;padding-top:12px;'
            f'border-top:1px solid #eee;color:#586069;font-size:13px;">'
            f'и ещё {len(rest)}: {rest_items}'
            f'</div>'
        )

    return f"""<!DOCTYPE html>
<html><head><meta charset="utf-8"></head>
<body style="margin:0;padding:0;background:#f6f8fa;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;color:#24292e;">
  <div style="max-width:600px;margin:20px auto;background:#fff;border-radius:8px;padding:24px;box-shadow:0 1px 3px rgba(0,0,0,0.08);">
    <h2 style="margin:0 0 4px 0;font-size:18px;">LONG [EMA+MACD]</h2>
    <div style="color:#586069;font-size:13px;margin-bottom:18px;">{now_utc} · всего: {len(signals)}</div>
    <table style="border-collapse:collapse;width:100%;font-size:14px;">
      <thead><tr style="background:#f6f8fa;text-align:left;">
        <th style="padding:10px 12px;">Инструмент</th>
        <th style="padding:10px 12px;text-align:center;">ТФ</th>
      </tr></thead>
      <tbody>{''.join(rows)}</tbody>
    </table>
    {rest_line}
    <div style="margin-top:16px;padding-top:12px;border-top:1px solid #eee;color:#586069;font-size:12px;">
      EMA+MACD Alert Bot (OKX / GitHub Actions). Ссылки ведут в TradingView (Bybit).
    </div>
  </div>
</body></html>"""


def build_text_body(signals: list, now_utc: str) -> str:
    top = _sort_signals_by_strength(signals)[:TOP_N]
    rest = _sort_signals_by_strength(signals)[TOP_N:]

    lines = [f"LONG [EMA+MACD] — {now_utc} · всего: {len(signals)}", ""]
    for i, s in enumerate(top, 1):
        lines.append(f"{i}. {s['inst_id']} ({s['tf']})")

    if rest:
        rest_items = ", ".join(
            f"{s['inst_id'].replace('-USDT-SWAP', '').replace('-USDT', '')} ({s['tf']})"
            for s in rest
        )
        lines.append("")
        lines.append(f"и ещё {len(rest)}: {rest_items}")

    lines.append("")
    lines.append("— EMA+MACD Alert Bot (OKX / GitHub Actions)")
    return "\n".join(lines)


def build_weekly_html(stats: dict, days: int, now_utc: str,
                      tickers: list, timeframes: list) -> str:
    rows = []
    total = 0
    for t in tickers:
        cells = []
        row_total = 0
        for tf in timeframes:
            n = stats.get(f"{t}|{tf}", 0)
            cells.append(
                f'<td style="padding:6px 10px;border-bottom:1px solid #eee;'
                f'text-align:center;font-family:monospace;">{n}</td>'
            )
            row_total += n
        total += row_total
        rows.append(
            f'<tr>'
            f'<td style="padding:6px 10px;border-bottom:1px solid #eee;font-weight:600;">{t}</td>'
            f'{"".join(cells)}'
            f'<td style="padding:6px 10px;border-bottom:1px solid #eee;'
            f'text-align:center;font-family:monospace;font-weight:700;">{row_total}</td>'
            f'</tr>'
        )

    totals_row = []
    grand = 0
    for tf in timeframes:
        col_sum = sum(stats.get(f"{t}|{tf}", 0) for t in tickers)
        grand += col_sum
        totals_row.append(
            f'<td style="padding:6px 10px;border-top:2px solid #ccc;'
            f'text-align:center;font-family:monospace;font-weight:700;">{col_sum}</td>'
        )

    header_tf_cells = "".join(
        f'<th style="padding:8px 10px;text-align:center;">{tf}</th>' for tf in timeframes
    )

    return f"""<!DOCTYPE html>
<html><head><meta charset="utf-8"></head>
<body style="margin:0;padding:0;background:#f6f8fa;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;color:#24292e;">
  <div style="max-width:700px;margin:20px auto;background:#fff;border-radius:8px;padding:24px;box-shadow:0 1px 3px rgba(0,0,0,0.08);">
    <h2 style="margin:0 0 4px 0;font-size:18px;">Weekly [EMA+MACD]</h2>
    <div style="color:#586069;font-size:13px;margin-bottom:18px;">{now_utc} · окно: последние {days} дн. · всего сигналов: {grand}</div>
    <table style="border-collapse:collapse;width:100%;font-size:14px;">
      <thead>
        <tr style="background:#f6f8fa;text-align:left;">
          <th style="padding:8px 10px;">Инструмент</th>
          {header_tf_cells}
          <th style="padding:8px 10px;text-align:center;">Всего</th>
        </tr>
      </thead>
      <tbody>
        {''.join(rows)}
        <tr>
          <td style="padding:8px 10px;border-top:2px solid #ccc;font-weight:700;">ИТОГО</td>
          {''.join(totals_row)}
          <td style="padding:8px 10px;border-top:2px solid #ccc;text-align:center;font-family:monospace;font-weight:700;">{grand}</td>
        </tr>
      </tbody>
    </table>
    <div style="margin-top:16px;padding-top:12px;border-top:1px solid #eee;color:#586069;font-size:12px;">
      EMA+MACD Alert Bot (OKX / GitHub Actions).
    </div>
  </div>
</body></html>"""


def build_weekly_text(stats: dict, days: int, now_utc: str,
                      tickers: list, timeframes: list) -> str:
    lines = [f"Weekly [EMA+MACD] — {now_utc} · окно: последние {days} дн.", ""]
    header = "Инструмент".ljust(22) + "".join(tf.rjust(6) for tf in timeframes) + "  Всего"
    lines.append(header)
    lines.append("-" * len(header))
    grand = 0
    for t in tickers:
        row_total = 0
        row = t.ljust(22)
        for tf in timeframes:
            n = stats.get(f"{t}|{tf}", 0)
            row += str(n).rjust(6)
            row_total += n
        row += f"  {row_total}"
        grand += row_total
        lines.append(row)
    lines.append("-" * len(header))
    totals = "".join(
        str(sum(stats.get(f"{t}|{tf}", 0) for t in tickers)).rjust(6)
        for tf in timeframes
    )
    lines.append("ИТОГО".ljust(22) + totals + f"  {grand}")
    lines.append("")
    lines.append("— EMA+MACD Alert Bot (OKX / GitHub Actions)")
    return "\n".join(lines)


def send_email(subject: str, text_body: str, html_body: str,
               to_addr: str, smtp_host: str, smtp_port: int) -> None:
    email_user = os.environ.get("EMAIL_USER", "").strip()
    email_pass = os.environ.get("EMAIL_APP_PASSWORD", "").replace(" ", "")
    if not email_user or not email_pass:
        fail("Не заданы EMAIL_USER или EMAIL_APP_PASSWORD")
    if not to_addr:
        fail("Не задан адрес получателя (EMAIL_TO или weekly_report.to)")

    msg = MIMEMultipart("alternative")
    msg["Subject"] = Header(subject, "utf-8")
    msg["From"] = email_user
    msg["To"] = to_addr

    msg.attach(MIMEText(text_body, "plain", "utf-8"))
    msg.attach(MIMEText(html_body, "html", "utf-8"))

    ctx = ssl.create_default_context()
    with smtplib.SMTP_SSL(smtp_host, smtp_port, context=ctx) as s:
        s.login(email_user, email_pass)
        s.send_message(msg)


# ---------- Обработка одного тикера ----------

def process_ticker(inst_id: str, tf: str, ema_length: int, cfg: dict,
                   state: dict, signals: list) -> bool:
    ma_type = cfg.get("ema_type", "EMA")
    warmup_factor = int(cfg.get("warmup_factor", 3))
    macd_cfg = cfg.get("macd", {})
    macd_fast = int(macd_cfg.get("fast", 12))
    macd_slow = int(macd_cfg.get("slow", 26))
    macd_sig  = int(macd_cfg.get("signal", 9))

    bar = TF_TO_OKX_BAR.get(tf)
    if not bar:
        dbg(f"  [{inst_id} {tf}] Неизвестный ТФ — пропуск.")
        return True

    needed = max(ema_length, macd_slow + macd_sig) * warmup_factor
    cache_key = f"candles_{inst_id}_{tf}"

    dbg(f"  [{inst_id} {tf}] EMA={ema_length}, "
        f"MACD={macd_fast}/{macd_slow}/{macd_sig}, нужно ~{needed}")

    cache = load_cache_from_state(state, cache_key)
    fresh_added = False

    try:
        if cache:
            newest_ts = max(cache.keys())
            dbg(f"    кэш: {len(cache)} свечей, самая свежая "
                f"{pd.to_datetime(newest_ts, unit='ms', utc=True)}")
            added = _update_from_okx(cache, inst_id, bar, newest_ts)
            dbg(f"    дотянуто новых: {added}")
            if added > 0:
                fresh_added = True
        else:
            dbg(f"    кэша нет — полная загрузка (~{needed} свечей)...")
            cache = full_load(inst_id, bar, needed)
            fresh_added = True
    except Exception as e:
        log(f"  [{inst_id} {tf}] Ошибка загрузки: {e}")
        return False

    _trim_cache(cache)
    state[cache_key] = [cache[ts] for ts in sorted(cache.keys())]

    df = cache_to_df(cache)

    pv = check_price_vs_mid(df, ema_length, ma_type)
    if not pv:
        dbg(f"  [{inst_id} {tf}] мало данных.")
        return fresh_added

    close_now = pv["close"]
    ma_mid = pv["maMid"]
    candle_time = pv["candle_time"]

    if close_now >= ma_mid:
        dbg(f"  [{inst_id} {tf}] close {close_now:.6f} >= maMid {ma_mid:.6f} — пропуск.")
        return fresh_added

    dbg(f"  [{inst_id} {tf}] close {close_now:.6f} < maMid {ma_mid:.6f}, проверяю MACD...")

    macd_sig_data = check_macd_condition(df, macd_fast, macd_slow, macd_sig)
    if not macd_sig_data:
        dbg(f"  [{inst_id} {tf}] MACD-условия нет.")
        return fresh_added

    confirm_tf = CONFIRM_TF.get(tf)
    if confirm_tf:
        higher_signal = get_macd_signal_from_cache(
            state, inst_id, confirm_tf, macd_fast, macd_slow, macd_sig
        )
        if higher_signal is None:
            dbg(f"  [{inst_id} {tf}] нет данных {confirm_tf} для подтверждения — пропуск.")
            return fresh_added
        if not (higher_signal < 0):
            dbg(f"  [{inst_id} {tf}] {confirm_tf} signal {higher_signal:.4f} >= 0 — пропуск.")
            return fresh_added
        dbg(f"  [{inst_id} {tf}] {confirm_tf} signal {higher_signal:.4f} < 0 — OK.")

    dedup_key = f"last_signal_candle_{inst_id}_{tf}"
    if state.get(dedup_key) == candle_time:
        dbg(f"  [{inst_id} {tf}] сигнал по свече {candle_time} уже отправлялся.")
        return fresh_added

    state[dedup_key] = candle_time
    append_signal_log(state, inst_id, tf, candle_time)
    signals.append({
        "inst_id": inst_id,
        "tf": tf,
        "ema_length": ema_length,
        "candle_time": candle_time,
        "close": close_now,
        "maMid": ma_mid,
        "macd_signal": macd_sig_data["macd_signal"],
        "hist": macd_sig_data["hist"],
        "hist_prev": macd_sig_data["hist_prev"],
    })
    log(f"  [{inst_id} {tf}] СИГНАЛ "
        f"(close {close_now:.6f} < maMid {ma_mid:.6f}, "
        f"signal {macd_sig_data['macd_signal']:.4f} < 0, "
        f"hist {macd_sig_data['hist_prev']:.6f} -> {macd_sig_data['hist']:.6f})")
    return fresh_added


# ---------- Прогон ----------

def run_pass(tickers: list, timeframes: dict, cfg: dict, state: dict) -> tuple[list, bool]:
    confirm_tfs = set(CONFIRM_TF.values())
    ordered_tfs = (
        [tf for tf in timeframes if tf in confirm_tfs]
        + [tf for tf in timeframes if tf not in confirm_tfs]
    )

    signals: list = []
    any_fresh = False

    for tf in ordered_tfs:
        tf_cfg = timeframes[tf]
        ema_length = int(tf_cfg["ema_length"])
        dbg(f"=== ТФ {tf} (EMA {ema_length}) ===")
        for inst_id in tickers:
            try:
                fresh = process_ticker(inst_id, tf, ema_length, cfg, state, signals)
                if fresh:
                    any_fresh = True
            except SystemExit:
                raise
            except Exception as e:
                log(f"  [{inst_id} {tf}] Ошибка: {e}")

    return signals, any_fresh


# ---------- Режимы ----------

def run_alerts(cfg: dict, tickers: list, timeframes: dict,
               smtp_host: str, smtp_port: int) -> None:
    state = load_state()

    signals, any_fresh = run_pass(tickers, timeframes, cfg, state)

    if not any_fresh:
        log(f"Новых свечей нет ни по одной паре — retry через {RETRY_DELAY_SEC} сек...")
        time.sleep(RETRY_DELAY_SEC)
        signals2, _ = run_pass(tickers, timeframes, cfg, state)
        seen = {(s["inst_id"], s["tf"], s["candle_time"]) for s in signals}
        for s in signals2:
            key = (s["inst_id"], s["tf"], s["candle_time"])
            if key not in seen:
                signals.append(s)
                seen.add(key)

    save_state(state)

    if not signals:
        log("Сигналов нет — письмо не отправляется.")
        log("Готово.")
        return

    log(f"СИГНАЛОВ: {len(signals)} — отправляю одно письмо.")

    subject = f"LONG [EMA+MACD] 5m/4h — сигналов: {len(signals)}"
    now_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    text_body = build_text_body(signals, now_utc)
    html_body = build_html_body(signals, now_utc)

    email_to = os.environ.get("EMAIL_TO", "").strip()
    try:
        send_email(subject, text_body, html_body, email_to, smtp_host, smtp_port)
    except Exception as e:
        fail(f"Не удалось отправить email: {e}")

    log(f"EMAIL ОТПРАВЛЕН: {subject}")
    log("Готово.")


def run_weekly_report(cfg: dict, tickers: list,
                      smtp_host: str, smtp_port: int) -> None:
    state = load_state()
    log_list = state.get("signal_log", [])
    log(f"Записей в журнале сигналов: {len(log_list)}")

    wr_cfg = cfg.get("weekly_report", {})
    days = int(wr_cfg.get("days_back", 7))
    to_addr = str(wr_cfg.get("to", "")).strip() or os.environ.get("EMAIL_TO", "").strip()

    cutoff = datetime.now(timezone.utc) - timedelta(days=days)

    stats: dict[str, int] = {}
    for entry in log_list:
        try:
            ts = datetime.fromisoformat(entry.get("recorded_at", ""))
        except Exception:
            continue
        if ts < cutoff:
            continue
        key = f"{entry.get('inst_id')}|{entry.get('tf')}"
        stats[key] = stats.get(key, 0) + 1

    timeframes = list(cfg["timeframes"].keys())

    now_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    subject = f"Weekly [EMA+MACD] — {days} дн."
    text_body = build_weekly_text(stats, days, now_utc, tickers, timeframes)
    html_body = build_weekly_html(stats, days, now_utc, tickers, timeframes)

    try:
        send_email(subject, text_body, html_body, to_addr, smtp_host, smtp_port)
    except Exception as e:
        fail(f"Не удалось отправить weekly report: {e}")

    log(f"WEEKLY REPORT ОТПРАВЛЕН на {to_addr}")
    log("Готово.")


# ---------- main ----------

def main() -> None:
    smtp_host = "smtp.gmail.com"
    smtp_port = 465

    cfg = load_config()
    tickers = cfg["tickers"]

    if MODE == "weekly_report":
        log(f"РЕЖИМ: weekly_report | Тикеров: {len(tickers)} | "
            f"ТФ: {list(cfg['timeframes'].keys())}")
        run_weekly_report(cfg, tickers, smtp_host, smtp_port)
        return

    # MODE = alerts
    timeframes = cfg["timeframes"]
    log(f"РЕЖИМ: alerts | Тикеров: {len(tickers)} | ТФ: {list(timeframes.keys())}"
        + ("" if DEBUG else " (DEBUG=1 для подробного лога)"))

    tickers = validate_tickers(tickers)
    if not tickers:
        fail("Ни одного валидного тикера — нечего обрабатывать.")

    run_alerts(cfg, tickers, timeframes, smtp_host, smtp_port)


if __name__ == "__main__":
    try:
        main()
    except SystemExit:
        raise
    except Exception as e:
        log(f"Непредвиденная ошибка: {e}")
        sys.exit(1)
