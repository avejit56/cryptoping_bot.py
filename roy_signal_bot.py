#!/usr/bin/env python3
"""
ROY Signal Bot — High Priority Only
Strategy: 4H OB Zone + Retest Confirm + ATR(14) based TP/SL + UT Bot EMA filter
Signals only after retest confirm — NOT on first OB touch
"""

import requests
import time
import json
import os
from datetime import datetime
from threading import Thread
from concurrent.futures import ThreadPoolExecutor, as_completed

TELEGRAM_TOKEN = "8720676004:AAEHtoPX7GumTTToWCIYM7pLNH62vWdcOw4"
ADMIN_CHAT_ID  = "6589114679"
CHECK_INTERVAL = 5 * 60

# ─── WATCHLIST ─────────────────────────────────────────────
WATCHLIST = [
    "STRAXUSDT","SOLUSDT","BTCUSDT","BNBUSDT","ALLOUSDT","EDENUSDT","ENAUSDT","OGUSDT",
    "SUSDT","GIGGLEUSDT","BROCCOLI714USDT","XNOUSDT","DYMUSDT","RONINUSDT","ENJUSDT",
    "HYPERUSDT","ORDIUSDT","OGNUSDT","BIOUSDT","NILUSDT","EDUUSDT","LUNAUSDT","SAHARAUSDT",
    "SENTUSDT","DOGEUSDT","CTSIUSDT","DOGSUSDT","STXUSDT","ADAUSDT","DOTUSDT","FETUSDT",
    "ZBTUSDT","LTCUSDT","PHAUSDT","MEGAUSDT","OSMOUSDT","ZKUSDT","ZKPUSDT","WALUSDT",
    "XLMUSDT","GENIUSUSDT","BABYUSDT","CHZUSDT","CHIPUSDT","STGUSDT","KATUSDT","IOUSDT",
    "MORPHOUSDT","CUSDT","ALTUSDT","AVAXUSDT","ONDOUSDT","APTUSDT","SPKUSDT","FORMUSDT",
    "HIVEUSDT","MANTRAUSDT","AVNTUSDT","RUNEUSDT","ASRUSDT","SANTOSUSDT","DUSKUSDT",
    "STEEMUSDT","AWEUSDT","CYBERUSDT","QTUMUSDT","SSVUSDT","COOKIEUSDT","CTKUSDT",
    "ACXUSDT","ZAMAUSDT","ZRXUSDT","AIGENSYNUSDT","WLFIUSDT","CRVUSDT","GPSUSDT",
    "OPNUSDT","HEMIUSDT","ASTRUSDT","ONTUSDT","SUIUSDT","XRPUSDT","FILUSDT","ICPUSDT",
    "APEUSDT","LINKUSDT","TONUSDT","TSTUSDT","NEOUSDT","NOTUSDT","ESPUSDT","UNIUSDT",
    "STOUSDT","OPGUSDT","PUNDIXUSDT","SCUSDT","ATOMUSDT","STORJUSDT","BMTUSDT","PSGUSDT",
    "LAZIOUSDT","CITYUSDT","BARUSDT","JUVUSDT","ACMUSDT","SHIBUSDT","PEPEUSDT","FLOKIUSDT",
    "BONKUSDT","WIFUSDT","ATMUSDT","PORTOUSDT","ALPINEUSDT","GALUSDT","GNSUSDT","WLDUSDT",
    "TRXUSDT","DEXEUSDT","EPICUSDT","FIDAUSDT","ENSUSDT","LUMIAUSDT","HOLOUSDT","ZROUSDT",
    "BANANAS31USDT","DUSDT","OPENUSDT","KITEUSDT","ROBOUSDT","VICUSDT","CATIUSDT",
    "SAGAUSDT","AUSDT","SOMIUSDT","MBOXUSDT","USUALUSDT","KMNOUSDT","SUNUSDT","FOGOUSDT",
    "XTZUSDT","ARBUSDT","HIGHUSDT","COSUSDT","UUSDT","NIGHTUSDT","ETCUSDT","AMPUSDT",
    "KSMUSDT","SCRUSDT","MMTUSDT","TUTUSDT","DODOUSDT","DOLOUSDT","LPTUSDT","SYNUSDT",
    "TLMUSDT","INITUSDT","AIXBTUSDT","ORCAUSDT","BARDUSDT","MIRAUSDT","MOVRUSDT","RIFUSDT",
    "TRUMPUSDT","ATUSDT","HEIUSDT","UTKUSDT","SPCXBUSDT","ELFUSDT","GNOUSDT",
    "HAEDALUSDT","PARTIUSDT","XPLUSDT","AXLUSDT","HIFIUSDT","SEIUSDT","ERNUSDT",
    "PLAUSDT","LUNCUSDT","JUPUSDT","LITUSDT","PHBUSDT","ATAUSDT","A2ZUSDT","DEGOUSDT",
    "SXPUSDT","DENTUSDT","FIOUSDT","HMSTRUSDT","OOKIUSDT","FUNUSDT","BIFIUSDT","TRUUSDT",
    "SYSUSDT","RDNTUSDT","GFTUSDT","NEBLUSDT","IDUSDT","AIONUSDT","BSWUSDT","NBSUSDT",
    "VIRTUALUSDT","ICXUSDT","WUSDT","SOPHUSDT",
]

# State tracking
ob_zones      = {}   # {symbol: {ob_low, ob_high, ob_time, state, peak, went_up, ...}}
signal_results = {}  # {key: {symbol, entry, highest, signal_time, result_sent}}
alerted       = {}   # {key: last_alert_time}

# ─── TELEGRAM ──────────────────────────────────────────────
def send(chat_id, message):
    try:
        requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
            json={"chat_id": chat_id, "text": message, "parse_mode": "HTML"},
            timeout=10
        )
    except Exception as e:
        print(f"Telegram error: {e}")

def broadcast(message, symbol=None):
    send(ADMIN_CHAT_ID, message)

# ─── BINANCE ───────────────────────────────────────────────
def get_klines(symbol, interval="4h", limit=50):
    try:
        r = requests.get(
            "https://api.binance.com/api/v3/klines",
            params={"symbol": symbol, "interval": interval, "limit": limit},
            timeout=10
        )
        if r.status_code == 200:
            return r.json()
    except:
        pass
    return None

def get_ticker(symbol):
    try:
        r = requests.get(
            "https://api.binance.com/api/v3/ticker/24hr",
            params={"symbol": symbol},
            timeout=10
        )
        if r.status_code == 200:
            return r.json()
    except:
        pass
    return None

def format_price(price):
    if price < 0.0001:  return f"${price:.8f}"
    elif price < 0.01:  return f"${price:.6f}"
    elif price < 1:     return f"${price:.4f}"
    else:               return f"${price:.3f}"

# ─── INDICATORS ────────────────────────────────────────────
def calc_atr(klines, period=14):
    """ATR(14) calculate"""
    if len(klines) < period + 1:
        return None
    trs = []
    for i in range(1, len(klines)):
        high  = float(klines[i][2])
        low   = float(klines[i][3])
        prev_close = float(klines[i-1][4])
        tr = max(high - low, abs(high - prev_close), abs(low - prev_close))
        trs.append(tr)
    if len(trs) < period:
        return None
    atr = sum(trs[-period:]) / period
    return atr

def calc_ema(closes, period):
    if len(closes) < period:
        return None
    k = 2 / (period + 1)
    ema = sum(closes[:period]) / period
    for price in closes[period:]:
        ema = price * k + ema * (1 - k)
    return ema

def ut_bot_filter(klines):
    """
    UT Bot inspired filter:
    - EMA(21) bullish trend
    - Price above EMA(21)
    - ATR multiplier: price not too extended
    """
    if len(klines) < 25:
        return False
    closes = [float(k[4]) for k in klines[:-1]]
    current = float(klines[-2][4])
    ema21 = calc_ema(closes, 21)
    if not ema21:
        return False
    # Price must be above EMA21
    if current < ema21:
        return False
    # EMA trending up (last 3 EMAs)
    ema_prev1 = calc_ema(closes[:-1], 21)
    ema_prev2 = calc_ema(closes[:-2], 21)
    if ema_prev1 and ema_prev2:
        if not (ema21 > ema_prev1 > ema_prev2):
            return False
    return True

def calc_tp_sl(entry, atr):
    """
    SL = entry - 1.5x ATR (UT Bot uses ATR multiplier)
    TP based on Risk-Reward:
    TP1 = 1.5R, TP2 = 2.5R, TP3 = 4R
    """
    sl = entry - (atr * 1.5)
    risk = entry - sl
    tp1 = entry + risk * 1.5
    tp2 = entry + risk * 2.5
    tp3 = entry + risk * 4.0
    return sl, tp1, tp2, tp3

def is_daily_downtrend(symbol, current_price):
    klines_d = get_klines(symbol, interval="1d", limit=15)
    if not klines_d or len(klines_d) < 7:
        return False
    closes = [float(k[4]) for k in klines_d[-8:-1]]
    ema7 = calc_ema(closes, min(7, len(closes)))
    if ema7 and current_price < ema7:
        return True
    last = klines_d[-2]
    if float(last[1]) > 0 and (float(last[4]) - float(last[1])) / float(last[1]) < -0.10:
        return True
    d_closes = [float(k[4]) for k in klines_d[-5:-1]]
    if len(d_closes) >= 4 and d_closes[-1] < d_closes[-2] < d_closes[-3] < d_closes[-4]:
        return True
    return False

# ─── OB ZONE DETECTION ─────────────────────────────────────
def find_ob_zone(klines, min_pump=0.12):
    """Find most recent significant pump candle = OB zone"""
    for i in range(len(klines) - 3, max(len(klines) - 20, 0), -1):
        o = float(klines[i][1])
        c = float(klines[i][4])
        if o > 0 and (c - o) / o >= min_pump:
            ob_low  = o * 0.98
            ob_high = o + (c - o) * 0.35
            return ob_low, ob_high
    return None, None

# ─── MAIN CHECK ────────────────────────────────────────────
def check_symbol(symbol):
    now = time.time()

    ticker = get_ticker(symbol)
    if not ticker:
        return
    current_price = float(ticker["lastPrice"])
    change_24h    = float(ticker["priceChangePercent"])

    # Daily downtrend filter
    if is_daily_downtrend(symbol, current_price):
        return

    klines_4h = get_klines(symbol, interval="4h", limit=50)
    if not klines_4h or len(klines_4h) < 20:
        return

    ob_low, ob_high = find_ob_zone(klines_4h)
    if not ob_low:
        return

    key = f"{symbol}_4h"
    state = ob_zones.get(key, {}).get("state", "waiting")

    # ── waiting → in_zone ──
    if state == "waiting":
        if ob_low * 0.99 <= current_price <= ob_high * 1.02:
            ob_zones[key] = {
                "symbol": symbol, "ob_low": ob_low, "ob_high": ob_high,
                "state": "in_zone", "enter_time": now,
                "lowest": current_price, "confirmed": False,
                "went_up": False, "peak": current_price,
                "retest_entered": False,
            }
        return

    zone = ob_zones[key]

    # Track lowest
    if current_price < zone.get("lowest", ob_low):
        ob_zones[key]["lowest"] = current_price

    # Bearish close → dip_wait
    last_c = klines_4h[-2]
    l_open  = float(last_c[1])
    l_close = float(last_c[4])

    if state == "in_zone":
        if l_close < ob_low and l_close < l_open:
            ob_zones[key]["state"] = "dip_wait"
            return

        # First confirm: 4H green body close + volume
        if not zone.get("confirmed"):
            l_vol  = float(last_c[5])
            l_buy  = float(last_c[9])
            prev_vols = [float(k[5]) for k in klines_4h[-8:-2]]
            avg_vol = sum(prev_vols)/len(prev_vols) if prev_vols else 1
            vol_ratio = l_vol / avg_vol
            buy_ratio = l_buy / l_vol if l_vol > 0 else 0

            if (l_close > l_open and l_close > ob_low and
                    current_price > ob_high * 0.99 and
                    vol_ratio >= 1.8 and buy_ratio >= 0.55 and
                    ut_bot_filter(klines_4h)):
                ob_zones[key]["confirmed"] = True
                ob_zones[key]["state"] = "post_confirm"
                ob_zones[key]["peak"] = current_price
                ob_zones[key]["went_up"] = False
                print(f"✅ First confirm: {symbol} — watching for retest")
        return

    if state == "dip_wait":
        if ob_low * 0.99 <= current_price <= ob_high * 1.02:
            ob_zones[key]["state"] = "in_zone"
            ob_zones[key]["confirmed"] = False
        return

    if state == "post_confirm":
        # Track peak
        if current_price > zone.get("peak", 0):
            ob_zones[key]["peak"] = current_price
            if current_price > ob_high * 1.02:
                ob_zones[key]["went_up"] = True

        # Retest: price returns to zone after going up
        in_zone_now = ob_low * 0.99 <= current_price <= ob_high * 1.02
        if in_zone_now and zone.get("went_up"):
            ob_zones[key]["retest_entered"] = True

        # Retest confirmed: price exits zone upward again
        if zone.get("retest_entered") and current_price > ob_high * 1.01:
            l_vol = float(last_c[5])
            l_buy = float(last_c[9])
            prev_vols = [float(k[5]) for k in klines_4h[-8:-2]]
            avg_vol = sum(prev_vols)/len(prev_vols) if prev_vols else 1
            vol_ratio = l_vol / avg_vol
            buy_ratio = l_buy / l_vol if l_vol > 0 else 0

            alert_key = f"{symbol}_retest"
            if (l_close > l_open and vol_ratio >= 1.8 and buy_ratio >= 0.55 and
                    ut_bot_filter(klines_4h) and
                    now - alerted.get(alert_key, 0) > 6 * 3600):

                # Calculate ATR + TP/SL
                atr = calc_atr(klines_4h)
                if not atr:
                    return

                entry = current_price
                sl, tp1, tp2, tp3 = calc_tp_sl(entry, atr)

                sl_pct   = (sl - entry) / entry * 100
                tp1_pct  = (tp1 - entry) / entry * 100
                tp2_pct  = (tp2 - entry) / entry * 100
                tp3_pct  = (tp3 - entry) / entry * 100
                risk_pct = abs(sl_pct)

                alerted[alert_key] = now
                ob_zones[key]["retest_entered"] = False
                ob_zones[key]["went_up"] = False
                ob_zones[key]["confirmed"] = False
                ob_zones[key]["state"] = "waiting"

                msg = (
                    f"🎯 <b>RETEST ENTRY SIGNAL! [4H OB]</b>\n\n"
                    f"🪙 <b>{symbol}</b>\n"
                    f"📊 24h: {change_24h:+.2f}%\n"
                    f"🔲 OB Zone: {format_price(ob_low)} — {format_price(ob_high)}\n\n"
                    f"💰 <b>Entry: {format_price(entry)}</b>\n"
                    f"🛑 SL: {format_price(sl)} ({sl_pct:.1f}%) [1.5x ATR]\n\n"
                    f"🎯 TP1: {format_price(tp1)} (+{tp1_pct:.1f}%) [1.5R]\n"
                    f"🎯 TP2: {format_price(tp2)} (+{tp2_pct:.1f}%) [2.5R]\n"
                    f"🎯 TP3: {format_price(tp3)} (+{tp3_pct:.1f}%) [4R]\n\n"
                    f"📐 ATR(14): {format_price(atr)} | Risk: {risk_pct:.1f}%\n"
                    f"⚡ Volume: {vol_ratio:.1f}x | Buy: {buy_ratio*100:.0f}%\n"
                    f"🕐 {datetime.now().strftime('%H:%M:%S')}\n\n"
                    f"⚠️ <i>Retest confirmed। SL tight রাখো।</i>"
                )
                broadcast(msg, symbol=symbol)
                print(f"🎯 Retest signal: {symbol}")

                # Track result
                signal_results[f"{symbol}_{int(now)}"] = {
                    "symbol": symbol, "entry": entry,
                    "signal_time": now, "highest_close": entry,
                    "peak_time": now, "result_sent": False,
                    "sl": sl, "tp1": tp1, "tp2": tp2, "tp3": tp3,
                }

        # Zone invalidation after going up
        if zone.get("went_up"):
            if l_close < ob_low and l_close < l_open:
                if not zone.get("post_invalid_sent"):
                    ob_zones[key]["post_invalid_sent"] = True
                    ob_zones[key]["state"] = "waiting"
                    send(ADMIN_CHAT_ID,
                        f"❌ <b>POST-CONFIRM INVALIDATED</b>\n\n"
                        f"🪙 {symbol} | 4H OB\n"
                        f"📉 Zone break হয়েছে।"
                    )

# ─── RESULT TRACKER ────────────────────────────────────────
def track_results():
    while True:
        now = time.time()
        remove = []

        for key, data in list(signal_results.items()):
            symbol  = data["symbol"]
            elapsed = now - data["signal_time"]

            if elapsed > 15 * 24 * 3600:
                remove.append(key)
                continue

            ticker = get_ticker(symbol)
            if not ticker:
                continue
            current_price = float(ticker["lastPrice"])

            # Track highest close via 1H
            klines_1h = get_klines(symbol, interval="1h", limit=3)
            if klines_1h and len(klines_1h) >= 2:
                last_close = float(klines_1h[-2][4])
                if last_close > data.get("highest_close", data["entry"]):
                    signal_results[key]["highest_close"] = last_close
                    signal_results[key]["peak_time"] = now

            highest = data.get("highest_close", data["entry"])
            peak_pct = (highest - data["entry"]) / data["entry"] * 100

            # Dump detect: peak থেকে 5%+ নামলে result দাও
            dumped = current_price < highest * 0.95

            if dumped and peak_pct >= 2.0 and not data.get("result_sent"):
                peak_hrs = (data.get("peak_time", now) - data["signal_time"]) / 3600
                signal_results[key]["result_sent"] = True

                emoji = "🚀" if peak_pct >= 20 else ("🟠" if peak_pct >= 10 else "🟡")

                # TP hit check
                tp_hit = ""
                if highest >= data["tp3"]:
                    tp_hit = "🎯 TP3 hit!"
                elif highest >= data["tp2"]:
                    tp_hit = "🎯 TP2 hit!"
                elif highest >= data["tp1"]:
                    tp_hit = "🎯 TP1 hit!"

                broadcast(
                    f"{emoji} <b>SIGNAL RESULT</b>\n\n"
                    f"🪙 <b>{symbol}</b>\n"
                    f"💰 {format_price(data['entry'])} → {format_price(highest)}\n"
                    f"📈 <b>+{peak_pct:.1f}%</b> | ⏱ {peak_hrs:.1f}hr\n"
                    + (f"{tp_hit}\n" if tp_hit else "")
                )

        for k in remove:
            signal_results.pop(k, None)

        time.sleep(60)

# ─── MAIN ──────────────────────────────────────────────────
def main():
    print("=" * 50)
    print("🎯 ROY Signal Bot — High Priority Only")
    print(f"📋 {len(WATCHLIST)} coins | 4H OB Retest Strategy")
    print("=" * 50)

    send(ADMIN_CHAT_ID,
        f"🎯 <b>ROY Signal Bot চালু!</b>\n\n"
        f"📋 Coins: {len(WATCHLIST)}\n"
        f"Strategy: 4H OB Retest + ATR TP/SL\n\n"
        f"Only retest confirm হলেই signal আসবে।"
    )

    Thread(target=track_results, daemon=True).start()

    while True:
        print(f"\n[{datetime.now().strftime('%H:%M:%S')}] Checking {len(WATCHLIST)} coins...")
        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = {executor.submit(check_symbol, s): s for s in WATCHLIST}
            for f in as_completed(futures):
                pass
        print(f"Done. Next check in 5 min...")
        time.sleep(CHECK_INTERVAL)

if __name__ == "__main__":
    main()
