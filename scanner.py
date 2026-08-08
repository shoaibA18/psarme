import os
import json
import time
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor

import requests

# ============================= CONFIG (edit these) =============================
TOP_N = 50
UNIVERSE_MODE = "volume"
TIMEFRAMES = ["15m", "1h"]
KLINES_LIMIT = 300
MAX_WORKERS = 8
STATE_FILE = "state.json"
STATE_MAX_AGE_DAYS = 3

TELEGRAM_BOT_TOKEN = os.environ["TELEGRAM_BOT_TOKEN"]
TELEGRAM_CHAT_ID = os.environ["TELEGRAM_CHAT_ID"]

STABLE_BASES = {"USDC", "FDUSD", "TUSD", "BUSD", "DAI", "USDP", "PAX",
                "EUR", "GBP", "TRY", "BRL", "UST", "USTC", "PYUSD", "AEUR"}
BINANCE_PROXIES = [
    "https://api.binance.com",
    "https://api.codetabs.com/v1/proxy?url=https://api.binance.com",
    "https://cors.sh/https://api.binance.com",
]


# ============================= INDICATORS =============================
def ema(values, period):
    n = len(values)
    out = [None] * n
    if n < period:
        return out
    k = 2 / (period + 1)
    prev = sum(values[:period]) / period
    out[period - 1] = prev
    for i in range(period, n):
        prev = values[i] * k + prev * (1 - k)
        out[i] = prev
    return out


def macd(closes, fast=12, slow=26, signal=9):
    ef, es = ema(closes, fast), ema(closes, slow)
    macd_line = [(ef[i] - es[i]) if (ef[i] is not None and es[i] is not None) else None
                 for i in range(len(closes))]
    start = next((i for i, v in enumerate(macd_line) if v is not None), None)
    signal_line = [None] * len(closes)
    if start is not None:
        sig_valid = ema(macd_line[start:], signal)
        for i, v in enumerate(sig_valid):
            signal_line[start + i] = v
    return macd_line, signal_line


def psar(highs, lows, start=0.02, inc=0.02, maximum=0.2):
    n = len(highs)
    sar = [None] * n
    if n < 3:
        return sar
    trend_up = (highs[1] + lows[1]) > (highs[0] + lows[0])
    af = start
    ep = max(highs[0], highs[1]) if trend_up else min(lows[0], lows[1])
    sar[1] = min(lows[0], lows[1]) if trend_up else max(highs[0], highs[1])
    for i in range(2, n):
        prev_sar = sar[i - 1]
        cand = prev_sar + af * (ep - prev_sar)
        if trend_up:
            cand = min(cand, lows[i - 1], lows[i - 2])
            if lows[i] < cand:
                trend_up = False
                sar[i] = ep
                ep = lows[i]
                af = start
            else:
                sar[i] = cand
                if highs[i] > ep:
                    ep = highs[i]
                    af = min(af + inc, maximum)
        else:
            cand = max(cand, highs[i - 1], highs[i - 2])
            if highs[i] > cand:
                trend_up = True
                sar[i] = ep
                ep = highs[i]
                af = start
            else:
                sar[i] = cand
                if lows[i] < ep:
                    ep = lows[i]
                    af = min(af + inc, maximum)
    return sar


# ============================= BINANCE DATA =============================
def is_eligible(symbol):
    if not symbol.endswith("USDT"):
        return False
    base = symbol[:-4]
    if base in STABLE_BASES:
        return False
    if base.endswith(("UP", "DOWN", "BULL", "BEAR")):
        return False
    return True


def fetch_top_symbols(limit, mode):
    last_err = None
    for proxy_base in BINANCE_PROXIES:
        try:
            url = f"{proxy_base}/api/v3/ticker/24hr"
            r = requests.get(url, timeout=15)
            r.raise_for_status()
            data = r.json()
            break
        except Exception as e:
            last_err = e
            continue
    else:
        raise Exception(f"All Binance API proxies failed. Last error: {last_err}")

    rows = [{
        "symbol": t["symbol"],
        "quoteVolume": float(t["quoteVolume"]),
        "changePct": float(t["priceChangePercent"]),
    } for t in data if is_eligible(t["symbol"])]

    if mode == "trending":
        rows.sort(key=lambda x: x["quoteVolume"], reverse=True)
        pool = rows[:max(limit * 2, 300)]
        pool.sort(key=lambda x: x["changePct"], reverse=True)
        return pool[:limit]

    rows.sort(key=lambda x: x["quoteVolume"], reverse=True)
    return rows[:limit]


def fetch_klines(symbol, interval, limit):
    last_err = None
    for proxy_base in BINANCE_PROXIES:
        try:
            url = f"{proxy_base}/api/v3/klines"
            r = requests.get(url,
                              params={"symbol": symbol, "interval": interval, "limit": limit},
                              timeout=15)
            r.raise_for_status()
            raw = r.json()
            break
        except Exception as e:
            last_err = e
            continue
    else:
        raise Exception(f"fetch_klines failed for {symbol}/{interval}. Last error: {last_err}")

    now_ms = time.time() * 1000
    closed = [k for k in raw if k[6] < now_ms]
    return {
        "times": [k[0] for k in closed],
        "highs": [float(k[2]) for k in closed],
        "lows": [float(k[3]) for k in closed],
        "closes": [float(k[4]) for k in closed],
    }


# ============================= STRATEGY CHECK =============================
def check_signal(k):
    closes, highs, lows = k["closes"], k["highs"], k["lows"]
    n = len(closes)
    if n < 210:
        return None
    e200 = ema(closes, 200)
    macd_line, signal_line = macd(closes, 12, 26, 9)
    sar = psar(highs, lows, 0.02, 0.02, 0.2)

    i = n - 1
    needed = (e200[i], e200[i - 1], macd_line[i], macd_line[i - 1],
              signal_line[i], signal_line[i - 1], sar[i], sar[i - 1])
    if any(v is None for v in needed):
        return None

    uptrend = lows[i] > e200[i]
    macd_bull_cross = macd_line[i - 1] <= signal_line[i - 1] and macd_line[i] > signal_line[i]
    psar_now_bull = sar[i] < lows[i]
    psar_prev_bull = sar[i - 1] < lows[i - 1]
    psar_flip_bull = psar_now_bull and not psar_prev_bull

    if uptrend and macd_bull_cross and psar_flip_bull:
        entry, stop = closes[i], sar[i]
        return {
            "entry": entry,
            "stop": stop,
            "target": entry + (entry - stop),
            "candle_open_time": k["times"][i],
        }
    return None


# ============================= STATE / DEDUP =============================
def load_state():
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE) as f:
                return json.load(f)
        except Exception:
            return {}
    return {}


def save_state(state):
    cutoff = time.time() * 1000 - STATE_MAX_AGE_DAYS * 86400 * 1000
    pruned = {k: v for k, v in state.items() if v > cutoff}
    with open(STATE_FILE, "w") as f:
        json.dump(pruned, f)


# ============================= TELEGRAM =============================
def send_telegram(text):
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    r = requests.post(url, json={
        "chat_id": TELEGRAM_CHAT_ID,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
    }, timeout=15)
    if not r.ok:
        print("Telegram send failed:", r.status_code, r.text)


def fmt_price(v):
    if v >= 100:
        return f"{v:.2f}"
    if v >= 1:
        return f"{v:.4f}"
    return f"{v:.6f}"


# ============================= MAIN =============================
def main():
    state = load_state()
    symbols = fetch_top_symbols(TOP_N, UNIVERSE_MODE)
    print(f"Scanning {len(symbols)} symbols x {TIMEFRAMES} ...")

    jobs = [(s["symbol"], tf) for s in symbols for tf in TIMEFRAMES]
    found = []

    for symbol, tf in jobs:
        try:
            k = fetch_klines(symbol, tf, KLINES_LIMIT)
            sig = check_signal(k)
            if sig:
                key = f"{symbol}:{tf}:{sig['candle_open_time']}"
                if key not in state:
                    state[key] = time.time() * 1000
                    found.append((symbol, tf, sig))
        except Exception as e:
            print(f"Error scanning {symbol}/{tf}: {e}")
            continue

    for symbol, tf, sig in found:
        risk_pct = (sig["entry"] - sig["stop"]) / sig["entry"] * 100
        link = f"https://www.tradingview.com/chart/?symbol=BINANCE:{symbol}"
        text = (
            f"\U0001F7E2 <b>Entry signal — {symbol}</b> ({tf})\n"
            f"Entry: <code>{fmt_price(sig['entry'])}</code>\n"
            f"Stop-loss: <code>{fmt_price(sig['stop'])}</code>\n"
            f"Take-profit (1:1): <code>{fmt_price(sig['target'])}</code>\n"
            f"Risk: {risk_pct:.2f}%\n"
            f'<a href="{link}">View chart</a>'
        )
        send_telegram(text)
        print(f"Notified: {symbol} {tf}")

    save_state(state)
    print(f"Done. {len(found)} new signal(s). {datetime.now(timezone.utc).isoformat()}")


if __name__ == "__main__":
    main()
