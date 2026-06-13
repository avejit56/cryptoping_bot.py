#!/usr/bin/env python3
"""
ROY Signal Bot — High Priority Only v2
- Syncs zones + watchlist from main bot via Telegram
- Manual zones: retest confirm → TP/SL signal
- Auto OB: detects own zones when no manual zone
- Only admin receives signals (no subscribers)
"""

import requests
import time
import json
from datetime import datetime
from threading import Thread
from concurrent.futures import ThreadPoolExecutor, as_completed

# ─── CONFIG ────────────────────────────────────────────────
SIGNAL_TOKEN   = "8973668144:AAFwvLoZhV1WDC5i0OIs8IpCylbkcx279Z8"
MAIN_TOKEN     = "8720676004:AAEHtoPX7GumTTToWCIYM7pLNH62vWdcOw4"
ADMIN_CHAT_ID  = "6589114679"
CHECK_INTERVAL = 5 * 60
SYNC_INTERVAL  = 15 * 60  # sync zones every 15 min

# ─── DEFAULT WATCHLIST ─────────────────────────────────────
DEFAULT_WATCHLIST = [
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

# ─── STATE ─────────────────────────────────────────────────
watchlist      = DEFAULT_WATCHLIST.copy()
manual_zones   = {}   # synced from main bot
auto_ob_zones  = {}   # auto detected OB zones
signal_results = {}   # result tracking
alerted        = {}   # cooldown
last_update_id = 0
last_sync_time = 0

# ─── TELEGRAM ──────────────────────────────────────────────
def send(message):
    try:
        requests.post(
            f"https://api.telegram.org/bot{SIGNAL_TOKEN}/sendMessage",
            json={"chat_id": ADMIN_CHAT_ID, "text": message, "parse_mode": "HTML"},
            timeout=10
        )
    except Exception as e:
        print(f"Telegram error: {e}")

def get_updates():
    global last_update_id
    try:
        r = requests.get(
            f"https://api.telegram.org/bot{SIGNAL_TOKEN}/getUpdates",
            params={"offset": last_update_id + 1, "timeout": 5},
            timeout=10
        )
        if r.status_code == 200:
            return r.json().get("result", [])
    except:
        pass
    return []

# ─── SYNC FROM MAIN BOT ────────────────────────────────────
def sync_from_main_bot():
    global manual_zones, watchlist, last_sync_time
    now = time.time()
    if now - last_sync_time < SYNC_INTERVAL:
        return
    last_sync_time = now

    try:
        # Read signal bot's own messages (main bot sends export here)
        r = requests.get(
            f"https://api.telegram.org/bot{SIGNAL_TOKEN}/getUpdates",
            params={"limit": 100},
            timeout=10
        )
        if r.status_code != 200:
            return

        updates = r.json().get("result", [])
        found_zones = False
        found_watchlist = False

        for update in reversed(updates):
            msg = update.get("message", {})
            text = msg.get("text", "")
            chat_id = str(msg.get("chat", {}).get("id", ""))

            if chat_id != ADMIN_CHAT_ID:
                continue

            # Sync zones
            if text.startswith("ZONES_EXPORT:") and not found_zones:
                try:
                    data = json.loads(text[13:])
                    new_zones = {}
                    for zid, z in data.items():
                        new_zones[zid] = {
                            "symbol":     z["symbol"],
                            "tf":         z["tf"],
                            "low":        z["low"],
                            "high":       z["high"],
                            "added_time": z["added_time"],
                            "state":      "waiting",
                            "confirmed":  False,
                            "went_up":    False,
                            "retest_entered": False,
                        }
                    manual_zones = new_zones
                    found_zones = True
                    print(f"✅ Synced {len(manual_zones)} zones from main bot")
                except Exception as e:
                    print(f"Zone sync error: {e}")

            # Sync watchlist
            if text.startswith("WATCHLIST_EXPORT:") and not found_watchlist:
                try:
                    wl = json.loads(text[17:])
                    watchlist = wl
                    found_watchlist = True
                    print(f"✅ Synced {len(watchlist)} coins from main bot")
                except Exception as e:
                    print(f"Watchlist sync error: {e}")

            if found_zones and found_watchlist:
                break

    except Exception as e:
        print(f"Sync error: {e}")

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
    if len(klines) < period + 1:
        return None
    trs = []
    for i in range(1, len(klines)):
        high = float(klines[i][2])
        low  = float(klines[i][3])
        prev_close = float(klines[i-1][4])
        tr = max(high - low, abs(high - prev_close), abs(low - prev_close))
        trs.append(tr)
    if len(trs) < period:
        return None
    return sum(trs[-period:]) / period

def calc_ema(closes, period):
    if len(closes) < period:
        return None
    k = 2 / (period + 1)
    ema = sum(closes[:period]) / period
    for p in closes[period:]:
        ema = p * k + ema * (1 - k)
    return ema

def ut_bot_filter(klines):
    if len(klines) < 25:
        return False
    closes  = [float(k[4]) for k in klines[:-1]]
    current = float(klines[-2][4])
    ema21   = calc_ema(closes, 21)
    if not ema21 or current < ema21:
        return False
    ema_p1 = calc_ema(closes[:-1], 21)
    ema_p2 = calc_ema(closes[:-2], 21)
    if ema_p1 and ema_p2:
        if not (ema21 > ema_p1 > ema_p2):
            return False
    return True

def calc_tp_sl(entry, atr):
    sl   = entry - atr * 1.5
    risk = entry - sl
    return sl, entry + risk*1.5, entry + risk*2.5, entry + risk*4.0

def is_daily_downtrend(symbol, price):
    klines = get_klines(symbol, interval="1d", limit=15)
    if not klines or len(klines) < 7:
        return False
    closes = [float(k[4]) for k in klines[-8:-1]]
    ema7   = calc_ema(closes, min(7, len(closes)))
    if ema7 and price < ema7:
        return True
    last = klines[-2]
    if float(last[1]) > 0 and (float(last[4]) - float(last[1])) / float(last[1]) < -0.10:
        return True
    d = [float(k[4]) for k in klines[-5:-1]]
    if len(d) >= 4 and d[-1] < d[-2] < d[-3] < d[-4]:
        return True
    return False

# ─── SIGNAL SENDER ─────────────────────────────────────────
def send_entry_signal(symbol, entry, atr, ob_low, ob_high, vol_ratio, buy_ratio, change_24h, zone_label):
    sl, tp1, tp2, tp3 = calc_tp_sl(entry, atr)
    now = time.time()

    key = f"{symbol}_signal"
    if now - alerted.get(key, 0) < 6 * 3600:
        return
    alerted[key] = now

    sl_pct  = (sl   - entry) / entry * 100
    tp1_pct = (tp1  - entry) / entry * 100
    tp2_pct = (tp2  - entry) / entry * 100
    tp3_pct = (tp3  - entry) / entry * 100

    send(
        f"🎯 <b>RETEST ENTRY SIGNAL! [{zone_label}]</b>\n\n"
        f"🪙 <b>{symbol}</b>\n"
        f"📊 24h: {change_24h:+.2f}%\n"
        f"🔲 OB Zone: {format_price(ob_low)} — {format_price(ob_high)}\n\n"
        f"💰 <b>Entry: {format_price(entry)}</b>\n"
        f"🛑 SL: {format_price(sl)} ({sl_pct:.1f}%) [1.5x ATR]\n\n"
        f"🎯 TP1: {format_price(tp1)} (+{tp1_pct:.1f}%) [1.5R]\n"
        f"🎯 TP2: {format_price(tp2)} (+{tp2_pct:.1f}%) [2.5R]\n"
        f"🎯 TP3: {format_price(tp3)} (+{tp3_pct:.1f}%) [4R]\n\n"
        f"📐 ATR(14): {format_price(atr)} | Risk: {abs(sl_pct):.1f}%\n"
        f"⚡ Volume: {vol_ratio:.1f}x | Buy: {buy_ratio*100:.0f}%\n"
        f"🕐 {datetime.now().strftime('%H:%M:%S')}\n\n"
        f"⚠️ <i>Retest confirmed। SL tight রাখো।</i>"
    )
    print(f"🎯 Signal sent: {symbol}")

    signal_results[f"{symbol}_{int(now)}"] = {
        "symbol": symbol, "entry": entry,
        "signal_time": now, "highest_close": entry,
        "peak_time": now, "result_sent": False,
        "sl": sl, "tp1": tp1, "tp2": tp2, "tp3": tp3,
    }

# ─── MANUAL ZONE CHECK ─────────────────────────────────────
def check_manual_zone(zone_id, zone, klines_4h, current_price, change_24h, ticker):
    now     = time.time()
    z_low   = zone["low"]
    z_high  = zone["high"]
    tf      = zone.get("tf", "4h")
    state   = zone.get("state", "waiting")

    klines = get_klines(zone["symbol"], interval=tf, limit=50) if tf != "4h" else klines_4h
    if not klines or len(klines) < 20:
        return

    last    = klines[-2]
    l_open  = float(last[1])
    l_close = float(last[4])
    l_vol   = float(last[5])
    l_buy   = float(last[9])
    prev_vols = [float(k[5]) for k in klines[-8:-2]]
    avg_vol = sum(prev_vols)/len(prev_vols) if prev_vols else 1
    vol_ratio = l_vol / avg_vol
    buy_ratio = l_buy / l_vol if l_vol > 0 else 0

    if state == "waiting":
        if z_low * 0.99 <= current_price <= z_high * 1.02:
            manual_zones[zone_id]["state"] = "in_zone"
            manual_zones[zone_id]["lowest"] = current_price
            manual_zones[zone_id]["confirmed"] = False
            manual_zones[zone_id]["went_up"] = False
        return

    if state == "in_zone":
        if current_price < zone.get("lowest", z_low):
            manual_zones[zone_id]["lowest"] = current_price
        if l_close < z_low and l_close < l_open:
            manual_zones[zone_id]["state"] = "dip_wait"
            return
        if not zone.get("confirmed"):
            if (l_close > l_open and l_close > z_low and
                    current_price > z_high * 0.99 and
                    vol_ratio >= 1.8 and buy_ratio >= 0.55 and
                    ut_bot_filter(klines)):
                manual_zones[zone_id]["confirmed"] = True
                manual_zones[zone_id]["state"] = "post_confirm"
                manual_zones[zone_id]["peak"] = current_price
                manual_zones[zone_id]["went_up"] = False
                print(f"✅ Manual zone first confirm: {zone_id}")
        return

    if state == "dip_wait":
        if z_low * 0.99 <= current_price <= z_high * 1.02:
            manual_zones[zone_id]["state"] = "in_zone"
            manual_zones[zone_id]["confirmed"] = False
        return

    if state == "post_confirm":
        if current_price > zone.get("peak", 0):
            manual_zones[zone_id]["peak"] = current_price
            if current_price > z_high * 1.02:
                manual_zones[zone_id]["went_up"] = True

        in_zone_now = z_low * 0.99 <= current_price <= z_high * 1.02
        if in_zone_now and zone.get("went_up"):
            manual_zones[zone_id]["retest_entered"] = True

        if zone.get("retest_entered") and current_price > z_high * 1.01:
            if (l_close > l_open and vol_ratio >= 1.8 and buy_ratio >= 0.55 and
                    ut_bot_filter(klines) and
                    not zone.get("retest_sent")):
                atr = calc_atr(klines)
                if atr:
                    manual_zones[zone_id]["retest_sent"] = True
                    manual_zones[zone_id]["retest_entered"] = False
                    manual_zones[zone_id]["state"] = "waiting"
                    manual_zones[zone_id]["confirmed"] = False
                    manual_zones[zone_id]["went_up"] = False
                    send_entry_signal(
                        zone["symbol"], current_price, atr,
                        z_low, z_high, vol_ratio, buy_ratio,
                        change_24h, f"Manual {tf.upper()} OB"
                    )

# ─── AUTO OB ZONE CHECK ────────────────────────────────────
def check_auto_ob(symbol, klines_4h, current_price, change_24h):
    now = time.time()

    # Find OB zone from pump candle
    ob_low, ob_high = None, None
    for i in range(len(klines_4h) - 3, max(len(klines_4h) - 20, 0), -1):
        o = float(klines_4h[i][1])
        c = float(klines_4h[i][4])
        if o > 0 and (c - o) / o >= 0.12:
            ob_low  = o * 0.98
            ob_high = o + (c - o) * 0.35
            break

    if not ob_low:
        return

    key = f"{symbol}_auto_4h"
    state = auto_ob_zones.get(key, {}).get("state", "waiting")

    last    = klines_4h[-2]
    l_open  = float(last[1])
    l_close = float(last[4])
    l_vol   = float(last[5])
    l_buy   = float(last[9])
    prev_vols = [float(k[5]) for k in klines_4h[-8:-2]]
    avg_vol = sum(prev_vols)/len(prev_vols) if prev_vols else 1
    vol_ratio = l_vol / avg_vol
    buy_ratio = l_buy / l_vol if l_vol > 0 else 0

    if state == "waiting":
        if ob_low * 0.99 <= current_price <= ob_high * 1.02:
            auto_ob_zones[key] = {
                "ob_low": ob_low, "ob_high": ob_high,
                "state": "in_zone", "confirmed": False,
                "went_up": False, "peak": current_price,
                "retest_entered": False,
            }
        return

    zone = auto_ob_zones[key]

    if state == "in_zone":
        if l_close < ob_low and l_close < l_open:
            auto_ob_zones[key]["state"] = "dip_wait"
            return
        if not zone.get("confirmed"):
            if (l_close > l_open and l_close > ob_low and
                    current_price > ob_high * 0.99 and
                    vol_ratio >= 1.8 and buy_ratio >= 0.55 and
                    ut_bot_filter(klines_4h)):
                auto_ob_zones[key]["confirmed"] = True
                auto_ob_zones[key]["state"] = "post_confirm"
                auto_ob_zones[key]["peak"] = current_price
                auto_ob_zones[key]["went_up"] = False
        return

    if state == "dip_wait":
        if ob_low * 0.99 <= current_price <= ob_high * 1.02:
            auto_ob_zones[key]["state"] = "in_zone"
            auto_ob_zones[key]["confirmed"] = False
        return

    if state == "post_confirm":
        if current_price > zone.get("peak", 0):
            auto_ob_zones[key]["peak"] = current_price
            if current_price > ob_high * 1.02:
                auto_ob_zones[key]["went_up"] = True

        in_zone_now = ob_low * 0.99 <= current_price <= ob_high * 1.02
        if in_zone_now and zone.get("went_up"):
            auto_ob_zones[key]["retest_entered"] = True

        if zone.get("retest_entered") and current_price > ob_high * 1.01:
            if (l_close > l_open and vol_ratio >= 1.8 and buy_ratio >= 0.55 and
                    ut_bot_filter(klines_4h) and
                    not zone.get("retest_sent")):
                atr = calc_atr(klines_4h)
                if atr:
                    auto_ob_zones[key]["retest_sent"] = True
                    auto_ob_zones[key]["retest_entered"] = False
                    auto_ob_zones[key]["state"] = "waiting"
                    auto_ob_zones[key]["confirmed"] = False
                    auto_ob_zones[key]["went_up"] = False
                    send_entry_signal(
                        symbol, current_price, atr,
                        ob_low, ob_high, vol_ratio, buy_ratio,
                        change_24h, "Auto 4H OB"
                    )

# ─── MAIN CHECK ────────────────────────────────────────────
def check_symbol(symbol):
    ticker = get_ticker(symbol)
    if not ticker:
        return
    current_price = float(ticker["lastPrice"])
    change_24h    = float(ticker["priceChangePercent"])

    if is_daily_downtrend(symbol, current_price):
        return

    klines_4h = get_klines(symbol, interval="4h", limit=50)
    if not klines_4h or len(klines_4h) < 20:
        return

    # Check manual zones for this symbol
    has_manual = False
    for zone_id, zone in list(manual_zones.items()):
        if zone.get("symbol") == symbol:
            has_manual = True
            check_manual_zone(zone_id, zone, klines_4h, current_price, change_24h, ticker)

    # Auto OB only if no manual zone for this symbol
    if not has_manual:
        check_auto_ob(symbol, klines_4h, current_price, change_24h)

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

            klines_1h = get_klines(symbol, interval="1h", limit=3)
            if klines_1h and len(klines_1h) >= 2:
                last_close = float(klines_1h[-2][4])
                if last_close > data.get("highest_close", data["entry"]):
                    signal_results[key]["highest_close"] = last_close
                    signal_results[key]["peak_time"] = now

            highest  = data.get("highest_close", data["entry"])
            peak_pct = (highest - data["entry"]) / data["entry"] * 100
            dumped   = current_price < highest * 0.95

            if dumped and peak_pct >= 2.0 and not data.get("result_sent"):
                peak_hrs = (data.get("peak_time", now) - data["signal_time"]) / 3600
                signal_results[key]["result_sent"] = True

                emoji = "🚀" if peak_pct >= 20 else ("🟠" if peak_pct >= 10 else "🟡")
                tp_hit = ""
                if highest >= data["tp3"]:   tp_hit = "🎯 TP3 hit!"
                elif highest >= data["tp2"]: tp_hit = "🎯 TP2 hit!"
                elif highest >= data["tp1"]: tp_hit = "🎯 TP1 hit!"

                send(
                    f"{emoji} <b>SIGNAL RESULT</b>\n\n"
                    f"🪙 <b>{symbol}</b>\n"
                    f"💰 {format_price(data['entry'])} → {format_price(highest)}\n"
                    f"📈 <b>+{peak_pct:.1f}%</b> | ⏱ {peak_hrs:.1f}hr\n"
                    + (f"{tp_hit}" if tp_hit else "")
                )

        for k in remove:
            signal_results.pop(k, None)

        time.sleep(60)

# ─── COMMAND HANDLER ───────────────────────────────────────
def handle_commands():
    global last_update_id
    while True:
        try:
            updates = get_updates()
            for update in updates:
                last_update_id = update["update_id"]
                msg  = update.get("message", {})
                text = msg.get("text", "").strip().upper()
                chat_id = str(msg.get("chat", {}).get("id", ""))

                if chat_id != ADMIN_CHAT_ID:
                    continue

                if text == "/STATUS":
                    send(
                        f"✅ <b>ROY Signal Bot</b>\n\n"
                        f"📋 Coins: {len(watchlist)}\n"
                        f"📐 Manual zones: {len(manual_zones)}\n"
                        f"🔍 Auto OB zones: {len(auto_ob_zones)}\n"
                        f"📊 Tracking signals: {len(signal_results)}\n"
                        f"🕐 {datetime.now().strftime('%H:%M:%S')}\n\n"
                        f"Strategy: 4H OB Retest + ATR TP/SL"
                    )

                elif text == "/SYNC":
                    global last_sync_time
                    last_sync_time = 0  # force sync
                    sync_from_main_bot()
                    send(f"✅ Synced!\n📐 Zones: {len(manual_zones)}\n📋 Coins: {len(watchlist)}")

                elif text == "/ZONES":
                    if not manual_zones:
                        send("📐 কোনো manual zone নেই।\nMain bot এ /exportzones দাও।")
                    else:
                        lines = [f"📐 <b>Manual Zones ({len(manual_zones)}):</b>\n"]
                        for zid, z in manual_zones.items():
                            lines.append(
                                f"• {z['symbol']} | {z.get('tf','4h').upper()} | "
                                f"{z['low']:.6g}—{z['high']:.6g} | {z.get('state','waiting')}"
                            )
                        send("\n".join(lines))

                elif text == "/HELP":
                    send(
                        "🤖 <b>Commands:</b>\n\n"
                        "/status — bot status\n"
                        "/sync — main bot থেকে zones+watchlist sync\n"
                        "/zones — active manual zones\n"
                    )

        except Exception as e:
            print(f"Command error: {e}")
        time.sleep(2)

# ─── MAIN ──────────────────────────────────────────────────
def main():
    global last_update_id
    print("=" * 50)
    print("🎯 ROY Signal Bot — High Priority Only")
    print("=" * 50)

    # Skip old messages
    try:
        r = requests.get(
            f"https://api.telegram.org/bot{SIGNAL_TOKEN}/getUpdates",
            params={"limit": 1, "offset": -1}, timeout=10
        )
        if r.status_code == 200:
            results = r.json().get("result", [])
            if results:
                last_update_id = results[-1]["update_id"]
    except:
        pass

    # Initial sync
    sync_from_main_bot()

    send(
        f"🎯 <b>ROY Signal Bot চালু!</b>\n\n"
        f"📋 Coins: {len(watchlist)}\n"
        f"📐 Manual zones: {len(manual_zones)}\n"
        f"Strategy: 4H OB Retest + ATR TP/SL\n\n"
        f"Sync করতে: /sync\n"
        f"Main bot এ /exportzones দাও আগে।"
    )

    Thread(target=handle_commands, daemon=True).start()
    Thread(target=track_results, daemon=True).start()

    while True:
        sync_from_main_bot()
        print(f"\n[{datetime.now().strftime('%H:%M:%S')}] Checking {len(watchlist)} coins...")
        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = {executor.submit(check_symbol, s): s for s in list(watchlist)}
            for f in as_completed(futures):
                pass
        print("Done. Next in 5 min...")
        time.sleep(CHECK_INTERVAL)

if __name__ == "__main__":
    main()
