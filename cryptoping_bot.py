#!/usr/bin/env python3
"""
CryptoPing Bot v1 (rebuilt from Volume Alert Bot v71)

A spot-trading crypto signal bot for Binance: zone/OB detection, breakout/retest alerts,
volume spike & buildup detection, pre-pump staged detection, and a personal trade monitor
(both for the admin and for premium subscribers).

This is the foundation pass — rename/rebrand, English-only messaging, new topic structure,
and full state persistence. Subsequent passes add: bug fixes (explosive pump filter, zone
confidence score no longer silently blocking notifications), 1D timeframe support, channel/
trendline breakout detection, the resistance-flip auto-detector, the subscriber trade
monitor module, and the /entry on-demand analysis command.
"""

import os
import requests
from requests.adapters import HTTPAdapter
import time
from datetime import datetime
from threading import Thread
from concurrent.futures import ThreadPoolExecutor, as_completed

# ─── SHARED HTTP SESSION (connection-pool fix) ─────────────
"""
BUGFIX: scanning 422 coins across multiple timeframes in parallel threads, each
using a bare http_session.get(...) call, was opening a brand new TCP+TLS connection
per request instead of reusing any. Under that load this exhausts the OS's
ephemeral port range within minutes ("High ephemeral port usage detected... Max
retries exceeded... Cannot assign requested address"), and the bot effectively
stops being able to reach Binance at all.

A single shared requests.Session() with an HTTPAdapter sized for our concurrency
keeps connections alive and reused across calls instead of leaking a new socket
every time. pool_maxsize is set well above our thread pool size so threads don't
contend for pool slots (which would otherwise just move the bottleneck rather
than fix it).
"""
http_session = requests.Session()
_adapter = HTTPAdapter(pool_connections=50, pool_maxsize=50, max_retries=2)
http_session.mount("https://", _adapter)
http_session.mount("http://", _adapter)

TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN", "")  # set in Railway → Variables
ADMIN_CHAT_ID = "6589114679"
CHECK_INTERVAL = 5 * 60

# CryptoPing Alerts Group — categorized topics
ALERTS_GROUP_ID = "-1003765700295"
TOPIC_HIGH        = 2     # 🎯 High Priority Signals
TOPIC_SPIKES      = 3     # ⚡ Quick Spikes
TOPIC_BUILDUPS    = 4     # 📈 Building Momentum
TOPIC_RESULTS     = 5     # 📊 Signal Results
TOPIC_TRADES      = 1463  # 💼 Trade Monitor (admin-only — Avejit's own trades)
TOPIC_USER_TRADES = 3042  # 👥 User Trades (admin-only — subscriber trade activity log)
TOPIC_USER_RESULTS = 3045 # 🏆 User Trade Results (admin-only — subscriber win/loss log)
TOPIC_TOP_PICKS   = 3046  # 🔥 Top Picks (highest-confidence prospects only)

# ─── TRADE MONITOR SETTINGS ──────────────────────────
TRADE_EMA_PERIOD       = 20     # EMA period for base trend filter
TRADE_VOL_MULTIPLIER   = 1.5    # red candle volume must exceed this x avg to count as "aggressive"
TRADE_CHECK_TF_DEFAULT = "1h"   # default timeframe to monitor a trade on
TRADE_SCORE_CAUTION    = 30     # 30-49 -> early caution
TRADE_SCORE_WEAKENING  = 50     # 50-69 -> tighten SL
TRADE_SCORE_HIGH       = 70     # 70+   -> high priority exit warning
TRADE_RETRACE_HEAVY    = 0.50   # red candles eating 50%+ of last impulse = heavy retracement
TRADE_TRAIL_TRIGGER_R  = 1.0    # once price reaches 1R profit, suggest moving SL to breakeven
TRADE_TRAIL_TRIGGER_R2 = 2.0    # at 2R, suggest trailing further (to 1R locked)

TIMEFRAMES = {
    "5m": {"multiplier": 60.0, "emoji": "⚡", "label": "5min", "cooldown": 900,    "retest_window": 90*60,   "direct_window": 10*60},
    "1h": {"multiplier": 15.0, "emoji": "📊", "label": "1H",   "cooldown": 10800,  "retest_window": 6*3600,  "direct_window": 60*60},
    "4h": {"multiplier": 8.0,  "emoji": "🔭", "label": "4H",   "cooldown": 21600,  "retest_window": 24*3600, "direct_window": 4*3600},
    "1d": {"multiplier": 5.0,  "emoji": "📅", "label": "1D",   "cooldown": 43200,  "retest_window": 72*3600, "direct_window": 12*3600},
}

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
    "TRUMPUSDT","ATUSDT","HEIUSDT","UTKUSDT","GNOUSDT","HAEDALUSDT","PARTIUSDT",
    "XPLUSDT","AXLUSDT","HIFIUSDT","SEIUSDT","ERNUSDT","PLAUSDT","LUNCUSDT","JUPUSDT",
    "LITUSDT","PHBUSDT","ATAUSDT","A2ZUSDT","DEGOUSDT","SXPUSDT","DENTUSDT","FIOUSDT",
    "HMSTRUSDT","OOKIUSDT","FUNUSDT","BIFIUSDT","TRUUSDT","SYSUSDT","RDNTUSDT","GFTUSDT",
    "NEBLUSDT","IDUSDT","AIONUSDT","BSWUSDT","NBSUSDT","VIRTUALUSDT","ICXUSDT","WUSDT",
    "SOPHUSDT","MITOUSDT","HUMAUSDT","METISUSDT","ZKCUSDT","MEMEUSDT","JASMYUSDT",
    "LAUSDT","EULUSDT","KAITOUSDT","XMRUSDT","BONDUSDT","LOKAUSDT","FFUSDT","PONDUSDT",
    "GTOUSDT","ANCUSDT","AUTOUSDT","ZENUSDT","KNCUSDT","DASHUSDT","ETHUSDT","SKLUSDT",
    "JTOUSDT","ADXUSDT","ZECUSDT","NEARUSDT","EIGENUSDT","XVGUSDT","METUSDT","ETHFIUSDT",
    "AAVEUSDT","TIAUSDT","BBUSDT","HOMEUSDT","MIRUSDT","TCTUSDT","OMNIUSDT","LRCUSDT",
    "GUNUSDT","BREVUSDT","ALICEUSDT","ADADOWNUSDT","SPCXBUSDT","VELODROMEUSDT","INJUSDT",
    "ELFUSDT","ENSOUSDT","IOTAUSDT","BCHUSDT","ROSEUSDT","PLUMEUSDT","VETUSDT","DNTUSDT",
    "PDAUSDT","1000CHEEMSUSDT","BTCSTUSDT","OMUSDT","RAREUSDT","UMAUSDT","MOVEUSDT",
    "BANKUSDT","BEAMXUSDT","BNSOLUSDT","MUBUSDT","LAYERUSDT","TWTUSDT","QNTUSDT",
    "JSTUSDT","YBUSDT","OXTUSDT","TAOUSDT","GRTUSDT","OPUSDT","PORTALUSDT","ASTERUSDT",
    "PENGUUSDT","CVXUSDT","TRBUSDT","HBARUSDT","FLOWUSDT","COCOSUSDT","TOMOUSDT","AVAUSDT",
    "FRAXUSDT","0GUSDT","NOMUSDT","TOWNSUSDT","KERNELUSDT","BEAMUSDT","SNDKBUSDT",
    "BNXUSDT","RNDRUSDT","YFIIUSDT","BAKEUSDT","DARUSDT","FRONTUSDT","HNTUSDT","MEUSDT",
    "NXPCUSDT","CRCLBUSDT","2ZUSDT","CFGUSDT","MCUSDT","VGXUSDT","KLAYUSDT","SPELLUSDT",
    "POLYXUSDT","POLYUSDT","BLZUSDT","DYDXUSDT","PYTHUSDT","PUMPUSDT","RENDERUSDT",
    "SXTUSDT","SUSHIUSDT","ARKMUSDT","PENDLEUSDT","1INCHUSDT","CFXUSDT","AXSUSDT",
    "LDOUSDT","CAKEUSDT","LINEAUSDT","IOTXUSDT","SYRUPUSDT","RAYUSDT","ARUSDT","LOOMUSDT",
]
DEFAULT_SUBSCRIBERS = [
    "6589114679","6113756284","1964213565","5881085618","6576647088","1575222335",
    "6261519935","1269617875","5978818610","6155938355","8151713222","6078380972",
    "8109438929","6519337248","7019792210","5401378684","1205496159","5278421971",
    "1792697433","6827377696","6841158775","8358119698","5486158393",
    "6209964312",   # Bayzid
    "5818496416",   # Nabil_tradeR
    "8450717853",   # MD Shawon
    "7228117196",   # $IFAT
]

DEFAULT_SUBSCRIBERS_INFO = {
    "6209964312": {"name": "Bayzid",       "joined": "2026-06-12 18:45"},
    "5818496416": {"name": "Nabil_tradeR", "joined": "2026-06-12 19:59"},
    "8450717853": {"name": "MD Shawon",    "joined": "2026-06-13 00:00"},
}

watchlist = DEFAULT_WATCHLIST.copy()
subscribers = DEFAULT_SUBSCRIBERS.copy()

alerted_coins = {}
momentum_tracking = {}
accumulation_tracking = {}
buildup_alerted = {}
trendline_alerted = {}
postpump_alerted = {}
ob_fvg_zone_tracking = {}
trendline_retest_tracking = {}
signal_performance = {}
buy_pressure_alerted = {}
last_coin_alert = {}
subscribers_info = {}
last_update_id = 0
volume_surge_alerted = {}   # {symbol: last_alert_time} — 6hr cooldown
manual_zones = {}           # {zone_id: {symbol, tf, low, high, added_time, state}}
zone_high_alerted = {}     # {zone_id_touch: last_notify_time}
last_scan_results = []      # cache for /addall

# v68: Active Trade Monitor
active_trades = {}          # {trade_id: {symbol, entry, sl, tp1, tp2, tp3, tf, opened_time, ...}}
trade_alert_cooldown = {}   # {trade_id: last_alert_time} — avoid spam per trade

# v68: Zone bounce history (remembers invalidated zones for future confluence)
zone_bounce_history = {}    # {symbol_low_high: {bounce_count, last_bounce_time, outcomes:[...]}}

# 5M spike tracking → waiting for 15M confirm
spike_pending_confirm = {}   # {symbol: {spike_price, spike_vol_ratio, spike_time, price_at_spike}}

# ─── FILE PATHS ───────────────────────────────────────────
import json as _json
_BOT_DIR = os.path.dirname(os.path.abspath(__file__))
ZONES_FILE     = os.path.join(_BOT_DIR, "zones.json")
WATCHLIST_FILE = os.path.join(_BOT_DIR, "watchlist.json")
SUBS_FILE      = os.path.join(_BOT_DIR, "subscribers.json")
SIGNAL_QUEUE_FILE = os.path.join(_BOT_DIR, "signal_queue.json")
TRADES_FILE    = os.path.join(_BOT_DIR, "active_trades.json")
ZONE_HISTORY_FILE = os.path.join(_BOT_DIR, "zone_history.json")
SIGNAL_PERFORMANCE_FILE = os.path.join(_BOT_DIR, "signal_performance.json")
PREPUMP_PHASES_FILE = os.path.join(_BOT_DIR, "prepump_phases.json")
TRENDLINE_TRACKING_FILE = os.path.join(_BOT_DIR, "trendline_retest_tracking.json")
COOLDOWN_TRACKERS_FILE = os.path.join(_BOT_DIR, "cooldown_trackers.json")

# ─── PERSISTENT STORAGE ───────────────────────────────────
WATCHLIST_MSG_ID = None
SUBSCRIBERS_MSG_ID = None

def save_zones():
    try:
        with open(ZONES_FILE, "w") as f:
            _json.dump(manual_zones, f, indent=2)
    except Exception as e:
        print(f"Zone save error: {e}")

def load_zones():
    global manual_zones
    try:
        if os.path.exists(ZONES_FILE):
            with open(ZONES_FILE) as f:
                manual_zones = _json.load(f)
            print(f"✅ Zones loaded: {len(manual_zones)}")
        else:
            print("📐 No zones file, starting fresh")
    except Exception as e:
        print(f"Zone load error: {e}")

def push_signal_to_queue(signal):
    """
    Appends a signal dict to signal_queue.json for the separate trader_bot.py to consume.
    This bot NEVER reads this file back — it's write-only from here, one-way handoff.
    signal example:
      {symbol, zone_low, zone_high, score, tf, signal_label, signal_type, price, time}
    """
    try:
        queue = []
        if os.path.exists(SIGNAL_QUEUE_FILE):
            try:
                with open(SIGNAL_QUEUE_FILE) as f:
                    queue = _json.load(f)
            except Exception:
                queue = []
        signal["queued_time"] = time.time()
        signal["signal_id"] = f"{signal.get('symbol','?')}_{int(signal['queued_time']*1000)}"
        queue.append(signal)
        # Keep file small — trim anything older than 24hr that's still sitting unread
        cutoff = time.time() - 24 * 3600
        queue = [s for s in queue if s.get("queued_time", 0) > cutoff]
        tmp_path = SIGNAL_QUEUE_FILE + ".tmp"
        with open(tmp_path, "w") as f:
            _json.dump(queue, f, indent=2)
        os.replace(tmp_path, SIGNAL_QUEUE_FILE)  # atomic write, avoids partial-read races
        print(f"📤 Signal queued for trader_bot: {signal.get('symbol')} score={signal.get('score')}")
    except Exception as e:
        print(f"Signal queue write error: {e}")

def save_active_trades():
    try:
        with open(TRADES_FILE, "w") as f:
            _json.dump(active_trades, f, indent=2)
    except Exception as e:
        print(f"Active trades save error: {e}")

def load_active_trades():
    global active_trades
    try:
        if os.path.exists(TRADES_FILE):
            with open(TRADES_FILE) as f:
                active_trades = _json.load(f)
            print(f"✅ Active trades loaded: {len(active_trades)}")
        else:
            print("💼 No active trades file, starting fresh")
    except Exception as e:
        print(f"Active trades load error: {e}")

def save_zone_history():
    try:
        with open(ZONE_HISTORY_FILE, "w") as f:
            _json.dump(zone_bounce_history, f, indent=2)
    except Exception as e:
        print(f"Zone history save error: {e}")

def load_zone_history():
    global zone_bounce_history
    try:
        if os.path.exists(ZONE_HISTORY_FILE):
            with open(ZONE_HISTORY_FILE) as f:
                zone_bounce_history = _json.load(f)
            print(f"✅ Zone history loaded: {len(zone_bounce_history)}")
        else:
            print("📍 No zone history file, starting fresh")
    except Exception as e:
        print(f"Zone history load error: {e}")

def save_signal_performance():
    try:
        with open(SIGNAL_PERFORMANCE_FILE, "w") as f:
            _json.dump(signal_performance, f, indent=2)
    except Exception as e:
        print(f"Signal performance save error: {e}")

def load_signal_performance():
    global signal_performance
    try:
        if os.path.exists(SIGNAL_PERFORMANCE_FILE):
            with open(SIGNAL_PERFORMANCE_FILE) as f:
                signal_performance = _json.load(f)
            print(f"✅ Signal performance loaded: {len(signal_performance)}")
        else:
            print("📊 No signal performance file, starting fresh")
    except Exception as e:
        print(f"Signal performance load error: {e}")

def save_prepump_phases():
    try:
        with open(PREPUMP_PHASES_FILE, "w") as f:
            _json.dump(prepump_phases, f, indent=2)
    except Exception as e:
        print(f"Pre-pump phases save error: {e}")

def load_prepump_phases():
    global prepump_phases
    try:
        if os.path.exists(PREPUMP_PHASES_FILE):
            with open(PREPUMP_PHASES_FILE) as f:
                prepump_phases = _json.load(f)
            print(f"✅ Pre-pump phases loaded: {len(prepump_phases)}")
        else:
            print("🔍 No pre-pump phases file, starting fresh")
    except Exception as e:
        print(f"Pre-pump phases load error: {e}")

def save_trendline_tracking():
    try:
        with open(TRENDLINE_TRACKING_FILE, "w") as f:
            _json.dump(trendline_retest_tracking, f, indent=2)
    except Exception as e:
        print(f"Trendline tracking save error: {e}")

def load_trendline_tracking():
    global trendline_retest_tracking
    try:
        if os.path.exists(TRENDLINE_TRACKING_FILE):
            with open(TRENDLINE_TRACKING_FILE) as f:
                trendline_retest_tracking = _json.load(f)
            print(f"✅ Trendline tracking loaded: {len(trendline_retest_tracking)}")
        else:
            print("📐 No trendline tracking file, starting fresh")
    except Exception as e:
        print(f"Trendline tracking load error: {e}")

def save_cooldown_trackers():
    """
    Bundles all 9 cooldown/dedup dicts into one file. These were originally left
    unpersisted (item #29 only covered the 3 analytically-relevant dicts), on the
    assumption they only prevent duplicate alerts within a single running process
    and have no historical value. In practice, with frequent redeploys (each one
    wiping in-memory state), this caused the bot to "forget" it had just sent an
    alert moments before a deploy and re-send the same signal right after — exactly
    the duplicate-message-after-update symptom. Persisting these closes that gap.
    """
    try:
        bundle = {
            "alerted_coins": alerted_coins,
            "buildup_alerted": buildup_alerted,
            "trendline_alerted": trendline_alerted,
            "postpump_alerted": postpump_alerted,
            "buy_pressure_alerted": buy_pressure_alerted,
            "volume_surge_alerted": volume_surge_alerted,
            "zone_high_alerted": zone_high_alerted,
            "big_pump_alerted": big_pump_alerted,
            "breakout_alerted": breakout_alerted,
        }
        with open(COOLDOWN_TRACKERS_FILE, "w") as f:
            _json.dump(bundle, f, indent=2)
    except Exception as e:
        print(f"Cooldown trackers save error: {e}")

def load_cooldown_trackers():
    global alerted_coins, buildup_alerted, trendline_alerted, postpump_alerted
    global buy_pressure_alerted, volume_surge_alerted, zone_high_alerted
    global big_pump_alerted, breakout_alerted
    try:
        if os.path.exists(COOLDOWN_TRACKERS_FILE):
            with open(COOLDOWN_TRACKERS_FILE) as f:
                bundle = _json.load(f)
            alerted_coins.update(bundle.get("alerted_coins", {}))
            buildup_alerted.update(bundle.get("buildup_alerted", {}))
            trendline_alerted.update(bundle.get("trendline_alerted", {}))
            postpump_alerted.update(bundle.get("postpump_alerted", {}))
            buy_pressure_alerted.update(bundle.get("buy_pressure_alerted", {}))
            volume_surge_alerted.update(bundle.get("volume_surge_alerted", {}))
            zone_high_alerted.update(bundle.get("zone_high_alerted", {}))
            big_pump_alerted.update(bundle.get("big_pump_alerted", {}))
            breakout_alerted.update(bundle.get("breakout_alerted", {}))
            total = sum(len(v) for v in bundle.values())
            print(f"✅ Cooldown trackers loaded: {total} entries across 9 dicts")
        else:
            print("⏱ No cooldown trackers file, starting fresh")
    except Exception as e:
        print(f"Cooldown trackers load error: {e}")

def save_watchlist_file():
    try:
        extra = [c for c in watchlist if c not in DEFAULT_WATCHLIST]
        with open(WATCHLIST_FILE, "w") as f:
            _json.dump(extra, f, indent=2)
    except Exception as e:
        print(f"Watchlist save error: {e}")

def load_watchlist_file():
    global watchlist
    watchlist = DEFAULT_WATCHLIST.copy()
    try:
        if os.path.exists(WATCHLIST_FILE):
            with open(WATCHLIST_FILE) as f:
                extra = _json.load(f)
            for c in extra:
                if c not in watchlist:
                    watchlist.append(c)
            print(f"✅ Watchlist: {len(DEFAULT_WATCHLIST)} default + {len(extra)} extra = {len(watchlist)}")
        else:
            print(f"📋 Using default: {len(watchlist)} coins")
    except Exception as e:
        print(f"Watchlist load error: {e}")

def save_subscribers_file():
    try:
        data = {"ids": subscribers, "info": subscribers_info}
        with open(SUBS_FILE, "w") as f:
            _json.dump(data, f, indent=2)
    except Exception as e:
        print(f"Subscribers save error: {e}")

def load_subscribers_file():
    global subscribers, subscribers_info
    subscribers = DEFAULT_SUBSCRIBERS.copy()
    subscribers_info.update(DEFAULT_SUBSCRIBERS_INFO)
    try:
        if os.path.exists(SUBS_FILE):
            with open(SUBS_FILE) as f:
                data = _json.load(f)
            for sid in data.get("ids", []):
                if sid not in subscribers:
                    subscribers.append(sid)
            subscribers_info.update(data.get("info", {}))
            print(f"✅ Subscribers: {len(subscribers)} loaded")
        else:
            print(f"👥 Using default: {len(subscribers)} subs")
    except Exception as e:
        print(f"Subscribers load error: {e}")

def save_watchlist():
    """Legacy Telegram save — now uses file"""
    save_watchlist_file()

def save_subscribers():
    """Legacy Telegram save — now uses file"""
    save_subscribers_file()

def load_from_telegram():
    """Load all persistent data from files"""
    load_watchlist_file()
    load_subscribers_file()
    load_zones()
    load_active_trades()
    load_zone_history()
    load_signal_performance()
    load_prepump_phases()
    load_trendline_tracking()
    load_cooldown_trackers()

# ─── TELEGRAM ─────────────────────────────────────────────
def send_to(chat_id, message):
    try:
        http_session.post(f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
            json={"chat_id": chat_id, "text": message, "parse_mode": "HTML"}, timeout=10)
    except:
        pass

def send_to_topic(topic_id, message):
    """Send a message to a specific topic in the CryptoPing Alerts group"""
    try:
        http_session.post(f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
            json={
                "chat_id": ALERTS_GROUP_ID,
                "message_thread_id": topic_id,
                "text": message,
                "parse_mode": "HTML"
            }, timeout=10)
    except:
        pass

def send_chunked(chat_id, lines, header=""):
    """
    Telegram sendMessage has a 4096 char hard limit — sending anything longer
    fails SILENTLY (send_to's bare except swallows the error). This splits a
    list of lines into safe ~3000 char chunks so large lists (watchlist, zones,
    subscribers) always actually reach the user instead of vanishing.
    """
    chunk_text = header
    for line in lines:
        if len(chunk_text) + len(line) > 3000:
            send_to(chat_id, chunk_text)
            chunk_text = line + "\n"
        else:
            chunk_text += line + "\n"
    if chunk_text:
        send_to(chat_id, chunk_text)

def get_topic_for_message(message):
    """Decide which topic to route this message to, based on its content"""
    if any(x in message for x in ["ZONE CONFIRMED", "RETEST CONFIRMED", "EXPLOSIVE PUMP", "MANUAL ZONE", "OB BOUNCE", "BUY PRESSURE", "TRENDLINE RETEST", "PRE-PUMP", "PHASE 3", "Breakout!", "BREAKOUT!"]):
        return TOPIC_HIGH
    elif any(x in message for x in ["VOLUME SPIKE", "VOLUME SURGE", "EARLY SIGNAL CONFIRMED"]):
        return TOPIC_SPIKES
    elif any(x in message for x in ["BUILD-UP", "ACCUMULATION", "HIGHER LOW", "DIRECT MOMENTUM", "PHASE 1", "PHASE 2"]):
        return TOPIC_BUILDUPS
    elif any(x in message for x in ["SIGNAL RESULT", "+5%", "+10%", "+20%"]):
        return TOPIC_RESULTS
    else:
        return TOPIC_SPIKES  # default

def get_category_for_signal_type(signal_type):
    """
    Maps a signal_performance entry's signal_type string to the same category
    buckets used for topic routing (High Priority / Quick Spikes / Building
    Momentum), so the /report command's breakdown matches what subscribers
    actually see in each topic.
    """
    st = signal_type.upper()
    if any(x in st for x in ["ZONE", "RETEST", "EXPLOSIVE", "OB BOUNCE", "TRENDLINE", "PRE-PUMP", "BREAKOUT"]):
        return "High Priority"
    elif any(x in st for x in ["VOLUME SPIKE", "VOLUME SURGE", "EARLY SIGNAL", "ABNORMAL"]):
        return "Quick Spikes"
    elif any(x in st for x in ["BUILD-UP", "ACCUMULATION", "HIGHER LOW", "DIRECT MOMENTUM"]):
        return "Building Momentum"
    else:
        return "Other"

def build_report(window_seconds, window_label):
    """
    Builds a category breakdown report from signal_performance: how many signals
    fired in the window, and what fraction of those reached +10% or more (using
    the actual highest price seen, not just the price at signal time — same
    "highest_after" tracking the SIGNAL RESULT messages use).
    """
    now = time.time()
    cutoff = now - window_seconds
    by_category = {}

    for perf_key, data in signal_performance.items():
        if data.get("signal_time", 0) < cutoff:
            continue
        category = get_category_for_signal_type(data.get("signal_type", ""))
        bucket = by_category.setdefault(category, {"total": 0, "hit_10pct": 0})
        bucket["total"] += 1
        signal_price = data.get("signal_price", 0)
        highest = data.get("highest_after", signal_price)
        if signal_price > 0:
            gain_pct = (highest - signal_price) / signal_price * 100
            if gain_pct >= 10.0:
                bucket["hit_10pct"] += 1

    if not by_category:
        return f"📊 <b>Report — {window_label}</b>\n\nNo signals recorded in this window yet."

    lines = [f"📊 <b>Report — {window_label}</b>\n"]
    total_all = 0
    hit_all = 0
    for category in ["High Priority", "Quick Spikes", "Building Momentum", "Other"]:
        if category not in by_category:
            continue
        b = by_category[category]
        total_all += b["total"]
        hit_all += b["hit_10pct"]
        pct = (b["hit_10pct"] / b["total"] * 100) if b["total"] else 0
        lines.append(f"<b>{category}:</b> {b['total']} signals | {b['hit_10pct']} hit +10%+ ({pct:.0f}%)")

    overall_pct = (hit_all / total_all * 100) if total_all else 0
    lines.append(f"\n<b>Overall:</b> {total_all} signals | {hit_all} hit +10%+ ({overall_pct:.0f}%)")
    return "\n".join(lines)

def is_important_signal(message):
    """OB, zone, retest — these important signals have no cooldown"""
    return any(x in message for x in [
        "ZONE CONFIRMED", "RETEST CONFIRMED", "EXPLOSIVE PUMP",
        "OB BOUNCE", "TRENDLINE RETEST",
        "BUY PRESSURE", "ACCUMULATION", "MANUAL ZONE", "Zone entered"
    ])

def send_all(message, symbol=None):
    if symbol and not is_important_signal(message):
        now = time.time()
        if now - last_coin_alert.get(symbol, 0) < 3600:
            print(f"⏸ Gap skip: {symbol}")
            return False
        last_coin_alert[symbol] = now
    # Send to all subscribers
    for chat_id in subscribers:
        send_to(chat_id, message)
    # Also send to the CryptoPing Alerts group topic
    topic = get_topic_for_message(message)
    send_to_topic(topic, message)
    return True

def get_updates():
    global last_update_id
    try:
        r = http_session.get(f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/getUpdates",
            params={"offset": last_update_id + 1, "timeout": 5}, timeout=10)
        if r.status_code == 200:
            return r.json().get("result", [])
    except:
        pass
    return []

# ─── BINANCE ──────────────────────────────────────────────
def get_klines(symbol, interval="5m", limit=50):
    try:
        r = http_session.get("https://api.binance.com/api/v3/klines",
            params={"symbol": symbol, "interval": interval, "limit": limit}, timeout=10)
        if r.status_code == 200:
            return r.json()
        else:
            # DIAGNOSTIC (temporary): log non-200 responses instead of silently
            # swallowing them. 418/429 specifically indicate a Binance rate-limit
            # ban on this IP — if that's what's happening, every single call will
            # fail this way and explain "no errors, no activity" symptom exactly.
            print(f"⚠️ get_klines {symbol} {interval}: HTTP {r.status_code} — {r.text[:200]}")
    except Exception as e:
        print(f"⚠️ get_klines {symbol} {interval} exception: {e}")
    return None

def get_ticker(symbol):
    try:
        r = http_session.get("https://api.binance.com/api/v3/ticker/24hr",
            params={"symbol": symbol}, timeout=10)
        if r.status_code == 200:
            return r.json()
        else:
            print(f"⚠️ get_ticker {symbol}: HTTP {r.status_code} — {r.text[:200]}")
    except Exception as e:
        print(f"⚠️ get_ticker {symbol} exception: {e}")
    return None

def calculate_ema(closes, period=20):
    if len(closes) < period:
        return None
    k = 2 / (period + 1)
    ema = sum(closes[:period]) / period
    for price in closes[period:]:
        ema = price * k + ema * (1 - k)
    return ema

def format_price(price):
    if price < 0.0001:
        return f"${price:.8f}"
    elif price < 0.01:
        return f"${price:.6f}"
    elif price < 1:
        return f"${price:.4f}"
    else:
        return f"${price:.3f}"

# ─── HELPER: Higher High / Higher Low check ───────────────
def check_hh_hl(klines, lookback=10):
    """
    Check whether recent candles show Higher High and Higher Low.
    True means bullish structure.
    """
    if len(klines) < lookback:
        return False
    recent = klines[-lookback:]
    highs = [float(k[2]) for k in recent]
    lows = [float(k[3]) for k in recent]
    # Compare HH and HL across the last 3 candles
    hh = highs[-1] > highs[-3]
    hl = lows[-1] > lows[-3]
    return hh and hl

def check_hl_only(klines, lookback=6):
    """Just check for a Higher Low (early bounce signal)"""
    if len(klines) < lookback:
        return False
    lows = [float(k[3]) for k in klines[-lookback:]]
    return lows[-1] > lows[-3]

# ─── HELPER: OB body close confirm ────────────────────────
def ob_body_close_confirmed(candle, ob_low, ob_high):
    """
    Check whether the candle body (open-close) closed above the OB zone.
    A wick touch alone returns false.
    """
    c_open = float(candle[1])
    c_close = float(candle[4])
    bullish = c_close > c_open
    body_close_above = c_close > ob_low
    return bullish and body_close_above

# ─── EARLY DETECTION: 5M spike → 15M confirm ─────────────
def check_5m_spike_early(symbol):
    """
    Detect a volume spike on the 5M timeframe.
    Track it as soon as the pump starts.
    Send the alert once 15M confirms it.
    """
    now = time.time()
    key = f"{symbol}_5m_early"

    # Already pending confirm? Skip new detection
    if symbol in spike_pending_confirm:
        return

    klines_5m = get_klines(symbol, interval="5m", limit=20)
    if not klines_5m or len(klines_5m) < 10:
        return

    candle = klines_5m[-2]  # last closed candle
    c_open = float(candle[1])
    c_close = float(candle[4])
    c_vol = float(candle[5])
    c_buy_vol = float(candle[9])

    # Must be bullish
    if c_close <= c_open:
        return

    # Volume spike: 20x avg
    prev_vols = [float(k[5]) for k in klines_5m[-9:-2]]
    avg_vol = sum(prev_vols) / len(prev_vols) if prev_vols else 1
    vol_ratio = c_vol / avg_vol
    if vol_ratio < 20.0:
        return

    # Buy volume > 60%
    buy_ratio = c_buy_vol / c_vol if c_vol > 0 else 0
    if buy_ratio < 0.60:
        return

    # Cooldown
    if now - alerted_coins.get(key, 0) < 900:
        return

    # Track for 15M confirmation
    spike_pending_confirm[symbol] = {
        "spike_time": now,
        "spike_price": c_close,
        "spike_vol_ratio": vol_ratio,
        "spike_buy_ratio": buy_ratio,
        "price_3candles_ago": float(klines_5m[-5][4]),  # price 15min ago
    }
    print(f"⚡ 5M spike detected: {symbol} | vol={vol_ratio:.1f}x | Waiting 15M confirm...")

def check_15m_confirm(symbol):
    """
    After a 5M spike is detected, check the 15M timeframe.
    If confirmed, send the alert.
    """
    now = time.time()
    if symbol not in spike_pending_confirm:
        return

    data = spike_pending_confirm[symbol]
    elapsed = now - data["spike_time"]

    # Expire after 15 minutes
    if elapsed > 20 * 60:
        spike_pending_confirm.pop(symbol, None)
        return

    # Need at least 1 new 15M candle to have closed
    if elapsed < 3 * 60:
        return

    klines_15m = get_klines(symbol, interval="15m", limit=10)
    if not klines_15m or len(klines_15m) < 4:
        return

    # Last closed 15M candle
    candle_15m = klines_15m[-2]
    c_open = float(candle_15m[1])
    c_close = float(candle_15m[4])
    c_high = float(candle_15m[2])
    c_vol = float(candle_15m[5])
    c_buy_vol = float(candle_15m[9])

    prev_vols_15m = [float(k[5]) for k in klines_15m[-6:-2]]
    avg_vol_15m = sum(prev_vols_15m) / len(prev_vols_15m) if prev_vols_15m else 1
    vol_ratio_15m = c_vol / avg_vol_15m

    # 15M confirm conditions:
    # 1. Bullish candle
    bullish = c_close > c_open
    # 2. Volume sustained (2x avg)
    vol_sustained = vol_ratio_15m >= 2.0
    # 3. Buy volume > 55%
    buy_ratio_15m = c_buy_vol / c_vol if c_vol > 0 else 0
    buy_dominant = buy_ratio_15m >= 0.55
    # 4. Price still above spike price (not dumped)
    price_holding = c_close >= data["spike_price"] * 0.99

    if not (bullish and vol_sustained and buy_dominant and price_holding):
        return

    # Calculate % changes during observation
    price_change_pct = (c_close - data["price_3candles_ago"]) / data["price_3candles_ago"] * 100
    vol_change_pct = (vol_ratio_15m - 1) * 100

    ticker = get_ticker(symbol)
    if not ticker:
        return
    current_price = float(ticker["lastPrice"])
    change_24h = float(ticker["priceChangePercent"])

    # 24h minimum +2%
    if change_24h < 2.0:
        spike_pending_confirm.pop(symbol, None)
        return

    # Price minimum +1.5% in 15M
    if price_change_pct < 1.5:
        spike_pending_confirm.pop(symbol, None)
        return

    # 4H trend filter
    klines_4h_check = get_klines(symbol, interval="4h", limit=15)
    if klines_4h_check and len(klines_4h_check) >= 5:
        closes_4h = [float(k[4]) for k in klines_4h_check[-6:-1]]
        ema_4h = calculate_ema(closes_4h, min(5, len(closes_4h)))
        if ema_4h and current_price < ema_4h:
            spike_pending_confirm.pop(symbol, None)
            return

    spike_pending_confirm.pop(symbol, None)
    alerted_coins[f"{symbol}_5m_early"] = now

    sent = send_all(
        f"⚡ <b>EARLY SIGNAL CONFIRMED!</b>\n\n"
        f"🪙 <b>{symbol}</b>\n"
        f"💰 Price: {format_price(current_price)}\n"
        f"📊 24h: {change_24h:+.2f}%\n\n"
        f"🔍 <b>5M Spike:</b> {data['spike_vol_ratio']:.1f}x volume\n"
        f"✅ <b>15M Confirmed:</b>\n"
        f"   📈 Price: +{price_change_pct:.1f}% (15min)\n"
        f"   ⚡ Volume: +{vol_change_pct:.0f}% vs avg\n"
        f"   🟢 Buy pressure: {buy_ratio_15m*100:.0f}%\n"
        f"🕐 {datetime.now().strftime('%H:%M:%S')}\n\n"
        f"⚠️ <i>Move is starting! Check the chart before entry.</i>",
        symbol=symbol
    )
    if sent:
        print(f"✅ Early signal confirmed: {symbol}")
        signal_performance[f"{symbol}_early_signal_{int(now)}"] = {
            "symbol": symbol, "signal_price": current_price,
            "signal_time": now, "signal_type": "Early Signal [5M→15M]",
            "highest_after": current_price,
        }

# ─── MTF OB BOUNCE: Improved with body close + HH/HL ─────
def check_postpump_retracement(symbol):
    now = time.time()

    def find_ob_zone(klines, min_pump):
        """OB zone: from the pump candle's open to 30% of its body"""
        for i in range(len(klines) - 10, len(klines) - 1):
            if i < 0:
                continue
            o = float(klines[i][1])
            c = float(klines[i][4])
            if o > 0 and (c - o) / o >= min_pump:
                return o * 0.97, o + (c - o) * 0.3, float(klines[i][2])
        return None, None, None

    ticker = get_ticker(symbol)
    if not ticker:
        return
    current_price = float(ticker["lastPrice"])
    change_24h = float(ticker["priceChangePercent"])

    # Daily downtrend filter
    if is_daily_downtrend(symbol, current_price):
        return
    klines_4h = get_klines(symbol, interval="4h", limit=20)
    ob4_bottom, ob4_top, pump4_high = (None, None, None)
    if klines_4h:
        ob4_bottom, ob4_top, pump4_high = find_ob_zone(klines_4h, 0.15)

    # 1H OB (8%+ pump)
    klines_1h_ob = get_klines(symbol, interval="1h", limit=20)
    ob1_bottom, ob1_top, pump1_high = (None, None, None)
    if klines_1h_ob:
        ob1_bottom, ob1_top, pump1_high = find_ob_zone(klines_1h_ob, 0.08)

    in_4h = ob4_bottom and ob4_bottom <= current_price <= ob4_top * 1.05
    in_1h = ob1_bottom and ob1_bottom <= current_price <= ob1_top * 1.05
    in_zone = in_4h or in_1h

    ob_bottom = ob4_bottom if in_4h else ob1_bottom
    ob_top = ob4_top if in_4h else ob1_top
    pump_high = pump4_high if in_4h else pump1_high
    ob_label = "4H OB" if in_4h else "1H OB"

    key = f"{symbol}_postpump"

    if in_zone and key not in ob_fvg_zone_tracking:
        if pump_high and current_price >= pump_high * 0.90:
            return
        ob_fvg_zone_tracking[key] = {
            "symbol": symbol, "zone_type": ob_label,
            "zone_top": ob_top, "zone_bottom": ob_bottom,
            "pump_high": pump_high, "enter_time": now,
            "lowest_in_zone": current_price, "alert_sent": False,
        }
        print(f"🎯 {ob_label} zone entered: {symbol}")
        return

    if key not in ob_fvg_zone_tracking:
        return

    data = ob_fvg_zone_tracking[key]
    if now - data["enter_time"] > 7 * 24 * 3600 or data["alert_sent"]:
        ob_fvg_zone_tracking.pop(key, None)
        return

    if current_price < data["lowest_in_zone"]:
        ob_fvg_zone_tracking[key]["lowest_in_zone"] = current_price

    lowest = ob_fvg_zone_tracking[key]["lowest_in_zone"]

    # Price must exit zone upward
    exited_up = current_price > data["zone_top"] * 1.01
    if not exited_up:
        return

    # ── NEW: MTF Confirmation based on OB type ──
    if ob_label == "4H OB":
        # 4H OB → 1H retest+hold → 1H HH/HL
        klines_1h = get_klines(symbol, interval="1h", limit=15)
        if not klines_1h or len(klines_1h) < 8:
            return
        last_1h = klines_1h[-2]
        # 1H body close above OB zone
        if not ob_body_close_confirmed(last_1h, data["zone_bottom"], data["zone_top"]):
            return
        # 1H Higher Low forming
        if not check_hl_only(klines_1h, lookback=6):
            return
        # Volume check on 1H
        cv_1h = float(last_1h[5])
        pv_1h = [float(k[5]) for k in klines_1h[-8:-2]]
        vol_ratio = cv_1h / (sum(pv_1h) / len(pv_1h)) if pv_1h else 1
        if vol_ratio < 1.5:
            return

    elif ob_label == "1H OB":
        # 1H OB → 15M retest+hold → 15M HH/HL
        klines_15m = get_klines(symbol, interval="15m", limit=20)
        if not klines_15m or len(klines_15m) < 10:
            return
        last_15m = klines_15m[-2]
        # 15M body close above OB zone
        if not ob_body_close_confirmed(last_15m, data["zone_bottom"], data["zone_top"]):
            return
        # 15M HH and HL
        if not check_hh_hl(klines_15m, lookback=8):
            return
        # Volume check on 15M
        cv_15m = float(last_15m[5])
        pv_15m = [float(k[5]) for k in klines_15m[-8:-2]]
        vol_ratio = cv_15m / (sum(pv_15m) / len(pv_15m)) if pv_15m else 1
        if vol_ratio < 2.0:
            return

    if now - postpump_alerted.get(key, 0) < 12 * 3600:
        return

    retrace_pct = (data["pump_high"] - lowest) / data["pump_high"] * 100 if data["pump_high"] else 0
    recovery_pct = (current_price - lowest) / lowest * 100

    # MTF label for alert
    mtf_confirm = "4H OB → 1H retest ✅" if ob_label == "4H OB" else "1H OB → 15M retest ✅"

    postpump_alerted[key] = now
    ob_fvg_zone_tracking[key]["alert_sent"] = True

    sent = send_all(
        f"🎯 <b>{data['zone_type']} BOUNCE CONFIRMED!</b>\n\n"
        f"🪙 <b>{symbol}</b>\n"
        f"💰 Price: {format_price(current_price)}\n"
        f"📊 24h: {change_24h:+.2f}%\n"
        f"📉 Retraced: -{retrace_pct:.1f}% from pump high\n"
        f"📈 Recovery: +{recovery_pct:.1f}% from zone low\n"
        f"🔲 Zone: {data['zone_type']} ({format_price(data['zone_bottom'])} — {format_price(data['zone_top'])})\n"
        f"⚡ Volume: {vol_ratio:.1f}x\n"
        f"📐 MTF: {mtf_confirm}\n"
        f"🕐 {datetime.now().strftime('%H:%M:%S')}\n\n"
        f"⚠️ <i>OB bounce! Check the chart before entry.</i>",
        symbol=symbol
    )
    if sent:
        print(f"🎯 {ob_label} MTF bounce confirmed: {symbol}")
        signal_performance[f"{symbol}_ob_bounce_{int(now)}"] = {
            "symbol": symbol, "signal_price": current_price,
            "signal_time": now, "signal_type": f"OB Bounce [{data['zone_type']}]",
            "highest_after": current_price,
        }

# ─── TRENDLINE: Best swing highs ──────────────────────────
def find_best_trendline(klines):
    highs = []
    for i in range(3, len(klines) - 3):
        h = float(klines[i][2])
        if (h > float(klines[i-1][2]) and h > float(klines[i-2][2]) and h > float(klines[i-3][2]) and
            h > float(klines[i+1][2]) and h > float(klines[i+2][2]) and h > float(klines[i+3][2])):
            highs.append((i, h))
    if len(highs) < 2:
        return None, None, None
    recent_highs = highs[-5:] if len(highs) >= 5 else highs
    if len(recent_highs) < 2:
        return None, None, None
    best_pair = None
    for i in range(len(recent_highs) - 1):
        for j in range(i + 1, len(recent_highs)):
            idx1, val1 = recent_highs[i]
            idx2, val2 = recent_highs[j]
            if val1 > val2:
                if best_pair is None or (val1 - val2) > (best_pair[1] - best_pair[3]):
                    best_pair = (idx1, val1, idx2, val2)
    if not best_pair:
        return None, None, None
    h1_idx, h1_val, h2_idx, h2_val = best_pair
    slope = (h2_val - h1_val) / (h2_idx - h1_idx)
    current_idx = len(klines) - 2
    trendline_value = h2_val + slope * (current_idx - h2_idx)
    return trendline_value, slope, (h1_val, h2_val)

def find_best_lower_trendline(klines):
    """
    Item #14: builds the LOWER (support) line of a descending channel from swing
    lows — the mirror of find_best_trendline's upper/resistance line from swing
    highs. A descending channel has two roughly-parallel lines: price bouncing
    off this lower line and breaking back above it (after approaching from inside
    the channel) is a separate, valid breakout signal from the upper-line case —
    e.g. MASKUSDT approaching its lower channel line near $0.393-0.394.
    """
    lows = []
    for i in range(3, len(klines) - 3):
        l = float(klines[i][3])
        if (l < float(klines[i-1][3]) and l < float(klines[i-2][3]) and l < float(klines[i-3][3]) and
            l < float(klines[i+1][3]) and l < float(klines[i+2][3]) and l < float(klines[i+3][3])):
            lows.append((i, l))
    if len(lows) < 2:
        return None, None, None
    recent_lows = lows[-5:] if len(lows) >= 5 else lows
    if len(recent_lows) < 2:
        return None, None, None
    best_pair = None
    for i in range(len(recent_lows) - 1):
        for j in range(i + 1, len(recent_lows)):
            idx1, val1 = recent_lows[i]
            idx2, val2 = recent_lows[j]
            if val1 > val2:  # descending: earlier low higher than later low
                if best_pair is None or (val1 - val2) > (best_pair[1] - best_pair[3]):
                    best_pair = (idx1, val1, idx2, val2)
    if not best_pair:
        return None, None, None
    l1_idx, l1_val, l2_idx, l2_val = best_pair
    slope = (l2_val - l1_val) / (l2_idx - l1_idx)
    current_idx = len(klines) - 2
    trendline_value = l2_val + slope * (current_idx - l2_idx)
    return trendline_value, slope, (l1_val, l2_val)

# ─── TRENDLINE BREAKOUT ───────────────────────────────────
def check_trendline_breakout(symbol, tf, klines):
    if len(klines) < 25:
        return
    now = time.time()
    key = f"{symbol}_{tf}_trendline"
    if now - trendline_alerted.get(key, 0) < 6 * 3600:
        return

    candle = klines[-2]
    current_close = float(candle[4])
    current_open = float(candle[1])
    current_high = float(candle[2])
    current_low = float(candle[3])
    current_vol = float(candle[5])
    buy_vol = float(candle[9])
    prev_vols = [float(k[5]) for k in klines[-9:-2]]
    avg_vol = sum(prev_vols) / len(prev_vols) if prev_vols else 1
    vol_ratio = current_vol / avg_vol
    buy_ratio = buy_vol / current_vol if current_vol > 0 else 0
    total_range = current_high - current_low
    body = abs(current_close - current_open)

    # Shared fake-breakout filters, checked once, used by both line types below
    vol_ok = vol_ratio >= 2.0
    buy_ok = buy_ratio >= 0.65
    body_ok = total_range > 0 and body / total_range >= 0.60

    # ── Upper (resistance) line breakout — original logic ──
    trendline_value, slope, _ = find_best_trendline(klines)
    upper_break = (
        trendline_value is not None and
        current_close > trendline_value * 1.002 and
        vol_ok and buy_ok and body_ok
    )

    # ── Lower (support/channel) line breakout — item #14 ──
    # Only meaningful if price was approaching this line from inside the channel
    # (i.e. price recently below or near it) and now breaks back above it with
    # the same fake-breakout filters. This catches descending-channel setups like
    # MASKUSDT, distinct from the upper-line resistance breakout case above.
    lower_value, lower_slope, _ = find_best_lower_trendline(klines)
    lower_break = False
    if lower_value is not None:
        prior_close = float(klines[-3][4])
        was_near_or_below = prior_close <= lower_value * 1.02
        lower_break = (
            was_near_or_below and
            current_close > lower_value * 1.002 and
            vol_ok and buy_ok and body_ok
        )

    if not (upper_break or lower_break):
        return

    if tf == "4h":
        klines_daily = get_klines(symbol, interval="1d", limit=30)
        if klines_daily and len(klines_daily) >= 5:
            daily_closes = [float(k[4]) for k in klines_daily[:-1]]
            daily_ema20 = calculate_ema(daily_closes, min(20, len(daily_closes)))
            if daily_ema20 and current_close < daily_ema20:
                return
            recent_highs = [float(k[2]) for k in klines_daily[-4:-1]]
            if len(recent_highs) >= 3 and not (recent_highs[-1] > recent_highs[-3]):
                return
    if tf == "1h":
        klines_4h = get_klines(symbol, interval="4h", limit=20)
        if klines_4h and len(klines_4h) >= 5:
            closes_4h = [float(k[4]) for k in klines_4h[:-1]]
            ema20_4h = calculate_ema(closes_4h, min(20, len(closes_4h)))
            if ema20_4h and current_close < ema20_4h:
                return

    trendline_alerted[key] = now
    cfg = TIMEFRAMES[tf]
    line_label = "lower channel" if (lower_break and not upper_break) else "resistance"
    used_value = lower_value if (lower_break and not upper_break) else trendline_value
    retest_key = f"{symbol}_tl_retest_{tf}"
    if retest_key not in trendline_retest_tracking:
        trendline_retest_tracking[retest_key] = {
            "symbol": symbol, "tf": tf, "start_time": now,
            "breakout_price": current_close, "trendline_value": used_value,
            "highest_since": current_close, "has_retested": False, "alert_sent": False,
        }
        print(f"📐 [{cfg['label']}] Real breakout tracked ({line_label}): {symbol} | buy={buy_ratio*100:.0f}%")

# ─── VOLUME BUILD-UP ──────────────────────────────────────
def check_volume_buildup(symbol, tf, klines):
    if len(klines) < 15:
        return
    now = time.time()
    key = f"{symbol}_{tf}_buildup"
    if now - buildup_alerted.get(key, 0) < 8 * 3600:
        return
    spike_key = f"{symbol}_{tf}"
    if now - alerted_coins.get(spike_key, 0) < 8 * 3600:
        return
    ticker = get_ticker(symbol)
    if not ticker:
        return
    change_24h = float(ticker["priceChangePercent"])
    if change_24h < 5.0:
        return
    # Daily downtrend filter
    price = float(ticker["lastPrice"])
    if is_daily_downtrend(symbol, price):
        return
    avg_vol = sum(float(k[5]) for k in klines[-22:-2]) / 20
    recent = klines[-4:-1]
    # Only count bullish candles with high volume
    consecutive = sum(1 for c in recent if float(c[5]) >= avg_vol * 2.5 and float(c[4]) > float(c[1]))
    if consecutive >= 3:
        price = float(ticker["lastPrice"])
        # HL filter: 1H→5M HL, 4H→15M HL
        if tf == "1h":
            klines_5m = get_klines(symbol, interval="5m", limit=10)
            if klines_5m and not check_hl_only(klines_5m, lookback=6):
                return
        elif tf == "4h":
            klines_15m = get_klines(symbol, interval="15m", limit=10)
            if klines_15m and not check_hl_only(klines_15m, lookback=6):
                return
        buildup_alerted[key] = now
        cfg = TIMEFRAMES[tf]
        sent = send_all(
            f"📈 <b>VOLUME BUILD-UP! [{cfg['label']}]</b>\n\n"
            f"🪙 <b>{symbol}</b>\n"
            f"💰 Price: {format_price(price)}\n"
            f"📊 24h: {change_24h:+.2f}%\n"
            f"⚡ {consecutive} consecutive candles with 2.5x+ avg volume\n"
            f"🕐 {datetime.now().strftime('%H:%M:%S')}\n\n"
            f"⚠️ <i>Pump may be coming, check the chart!</i>",
            symbol=symbol
        )
        if sent:
            print(f"📈 [{cfg['label']}] Build-up: {symbol}")
            signal_performance[f"{symbol}_buildup_{tf}_{int(now)}"] = {
                "symbol": symbol, "signal_price": price,
                "signal_time": now, "signal_type": f"Volume Build-up [{cfg['label']}]",
                "highest_after": price,
            }

# ─── HIGHER LOWS ──────────────────────────────────────────
def check_higher_lows(symbol, tf, klines):
    if tf not in ["1h", "4h"] or len(klines) < 20:
        return
    now = time.time()
    key = f"{symbol}_{tf}_higher_lows"
    if now - buildup_alerted.get(key, 0) < 3 * 3600:
        return
    ticker = get_ticker(symbol)
    if not ticker:
        return
    change_24h = float(ticker["priceChangePercent"])
    if change_24h < 5.0:
        return
    avg_vol = sum(float(k[5]) for k in klines[-12:-3]) / 9
    recent_vols = [float(k[5]) for k in klines[-4:-1]]
    recent_candles = klines[-4:-1]
    if not all(v >= avg_vol * 2.0 for v in recent_vols):
        return
    # Recent candles must be bullish
    if not all(float(c[4]) > float(c[1]) for c in recent_candles):
        return
    lows = []
    highs = []
    for i in range(2, len(klines) - 2):
        l = float(klines[i][3])
        h = float(klines[i][2])
        if l < float(klines[i-1][3]) and l < float(klines[i-2][3]) and \
           l < float(klines[i+1][3]) and l < float(klines[i+2][3]):
            lows.append(l)
        if h > float(klines[i-1][2]) and h > float(klines[i-2][2]) and \
           h > float(klines[i+1][2]) and h > float(klines[i+2][2]):
            highs.append(h)
    if len(lows) < 3:
        return
    last3_lows = lows[-3:]
    higher_lows_ok = last3_lows[0] < last3_lows[1] < last3_lows[2]

    # Item #18: also require Higher Highs, not just Higher Lows — a real uptrend
    # structure needs both. HL alone can happen during a choppy decline too (each
    # bounce slightly higher than the last dip, but the highs are still falling).
    higher_highs_ok = True
    hh_note = ""
    if len(highs) >= 2:
        last2_highs = highs[-2:]
        higher_highs_ok = last2_highs[0] < last2_highs[1]
        hh_note = " + Higher High" if higher_highs_ok else ""

    if higher_lows_ok and higher_highs_ok:
        price = float(ticker["lastPrice"])
        buildup_alerted[key] = now
        cfg = TIMEFRAMES[tf]
        sent = send_all(
            f"🔬 <b>ACCUMULATION! [{cfg['label']}]</b>\n\n"
            f"🪙 <b>{symbol}</b>\n"
            f"💰 Price: {format_price(price)}\n"
            f"📊 24h: {change_24h:+.2f}%\n"
            f"📈 3 Higher Lows{hh_note} + Volume build-up\n"
            f"🕐 {datetime.now().strftime('%H:%M:%S')}\n\n"
            f"⚠️ <i>Big pump may be coming. Check the chart!</i>",
            symbol=symbol
        )
        if sent:
            print(f"🔬 [{cfg['label']}] Accumulation: {symbol}")
            signal_performance[f"{symbol}_accum_{tf}_{int(now)}"] = {
                "symbol": symbol, "signal_price": price,
                "signal_time": now, "signal_type": f"Accumulation [{cfg['label']}]",
                "highest_after": price,
            }

# ─── BUY PRESSURE ─────────────────────────────────────────
def check_buy_pressure(symbol):
    now = time.time()
    key = f"{symbol}_buypressure"
    if now - buy_pressure_alerted.get(key, 0) < 4 * 3600:
        return
    ticker = get_ticker(symbol)
    if not ticker:
        return
    current_price = float(ticker["lastPrice"])
    change_24h = float(ticker["priceChangePercent"])
    if change_24h < 1.0:   # must be net positive on 24h
        return

    def find_ob(klines, min_pump):
        for i in range(len(klines) - 8, len(klines) - 1):
            if i < 0:
                continue
            o = float(klines[i][1])
            c = float(klines[i][4])
            if o > 0 and (c - o) / o >= min_pump:
                return o * 0.97, o + (c - o) * 0.3
        return None, None

    klines_4h = get_klines(symbol, interval="4h", limit=20)
    ob4_b, ob4_t = find_ob(klines_4h, 0.15) if klines_4h else (None, None)
    klines_1h_ob = get_klines(symbol, interval="1h", limit=20)
    ob1_b, ob1_t = find_ob(klines_1h_ob, 0.08) if klines_1h_ob else (None, None)

    in_4h = ob4_b and ob4_b <= current_price <= ob4_t * 1.03
    in_1h = ob1_b and ob1_b <= current_price <= ob1_t * 1.03
    if not (in_4h or in_1h):
        return

    ob_b = ob4_b if in_4h else ob1_b
    ob_t = ob4_t if in_4h else ob1_t
    ob_label = "4H OB" if in_4h else "1H OB"

    # ── Daily trend filter ──
    if is_daily_downtrend(symbol, current_price):
        return

    klines_15m = get_klines(symbol, interval="15m", limit=6)
    if not klines_15m or len(klines_15m) < 4:
        return
    recent = klines_15m[-4:-1]
    buy_ratios = []
    for c in recent:
        total_vol = float(c[5])
        buy_vol = float(c[9])
        if total_vol > 0:
            buy_ratios.append(buy_vol / total_vol)
    if not buy_ratios or sum(buy_ratios)/len(buy_ratios) < 0.70:
        return
    vols = [float(c[5]) for c in recent]
    if vols[-1] <= vols[0]:
        return

    avg_buy_ratio = sum(buy_ratios) / len(buy_ratios)
    buy_pressure_alerted[key] = now
    sent = send_all(
        f"💚 <b>STRONG BUY PRESSURE!</b>\n\n"
        f"🪙 <b>{symbol}</b>\n"
        f"💰 Price: {format_price(current_price)}\n"
        f"📊 24h: {change_24h:+.2f}%\n"
        f"🟢 Buy volume: <b>{avg_buy_ratio*100:.0f}%</b> of total\n"
        f"🔲 {ob_label}: {format_price(ob_b)} — {format_price(ob_t)}\n"
        f"📈 Volume increasing\n"
        f"🕐 {datetime.now().strftime('%H:%M:%S')}\n\n"
        f"⚠️ <i>Buy pressure at OB zone! Confirm on the chart.</i>",
        symbol=symbol
    )
    if sent:
        print(f"💚 Buy Pressure: {symbol} ({avg_buy_ratio*100:.0f}% buy, {ob_label})")
        signal_performance[f"{symbol}_buypressure_{int(now)}"] = {
            "symbol": symbol, "signal_price": current_price,
            "signal_time": now, "signal_type": "Buy Pressure [OB]",
            "highest_after": current_price,
        }

# ─── 200%+ VOLUME SURGE ───────────────────────────────────
def check_volume_surge(symbol):
    """
    Alert when any 1H candle's volume is 3x (200%+) the average.
    Only notifies once per 6-hour window.
    No spike-type check — any direction of surge triggers the alert.
    """
    now = time.time()
    key = f"{symbol}_volsurge"
    if now - volume_surge_alerted.get(key, 0) < 6 * 3600:
        return

    klines = get_klines(symbol, interval="1h", limit=20)
    if not klines or len(klines) < 10:
        return

    last_candle = klines[-2]
    current_vol = float(last_candle[5])
    prev_vols = [float(k[5]) for k in klines[-9:-2]]
    avg_vol = sum(prev_vols) / len(prev_vols) if prev_vols else 1
    if avg_vol == 0:
        return

    vol_ratio = current_vol / avg_vol
    if vol_ratio < 3.0:
        return

    # Only bullish candle with meaningful body (min +2%)
    c_open = float(last_candle[1])
    c_close = float(last_candle[4])
    if c_close <= c_open:
        return
    price_change = (c_close - c_open) / c_open * 100
    if price_change < 3.0:   # min +3% body
        return

    ticker = get_ticker(symbol)
    if not ticker:
        return
    current_price = float(ticker["lastPrice"])
    change_24h = float(ticker["priceChangePercent"])

    # 24h minimum +5%
    if change_24h < 5.0:
        return
    # Daily downtrend filter
    if is_daily_downtrend(symbol, current_price):
        return

    surge_pct = (vol_ratio - 1) * 100
    volume_surge_alerted[key] = now
    # Suppress pre-pump for same symbol
    alerted_coins[f"{symbol}_prepump"] = now

    sent = send_all(
        f"🌊 <b>VOLUME SURGE 200%+! [1H]</b>\n\n"
        f"🪙 <b>{symbol}</b>\n"
        f"💰 Price: {format_price(current_price)}\n"
        f"📊 24h: {change_24h:+.2f}%\n"
        f"⚡ Volume: <b>{vol_ratio:.1f}x avg (+{surge_pct:.0f}%)</b>\n"
        f"🕯 Candle: 🟢 +{price_change:.1f}%\n"
        f"🕐 {datetime.now().strftime('%H:%M:%S')}\n\n"
        f"⚠️ <i>Bullish surge! Check the chart before entry.</i>",
        symbol=symbol
    )
    if sent:
        print(f"🌊 Volume Surge: {symbol} ({vol_ratio:.1f}x)")
        signal_performance[f"{symbol}_surge_{int(now)}"] = {
            "symbol": symbol, "signal_price": current_price,
            "signal_time": now, "signal_type": "Volume Surge [1H]",
            "highest_after": current_price,
        }

# ─── MANUAL ZONE MONITOR ──────────────────────────────────
def zone_history_key(symbol, z_low, z_high):
    """Stable key for a price zone, rounded so near-identical zones match"""
    return f"{symbol}_{z_low:.8f}_{z_high:.8f}"

def record_zone_outcome(symbol, z_low, z_high, outcome):
    """outcome: 'confirmed', 'invalidated', 'retest_confirmed'"""
    key = zone_history_key(symbol, z_low, z_high)
    hist = zone_bounce_history.setdefault(key, {
        "symbol": symbol, "low": z_low, "high": z_high,
        "bounce_count": 0, "invalid_count": 0,
        "last_time": 0, "outcomes": []
    })
    hist["last_time"] = time.time()
    hist["outcomes"].append({"outcome": outcome, "time": time.time()})
    hist["outcomes"] = hist["outcomes"][-10:]  # keep last 10 only
    if outcome in ("confirmed", "retest_confirmed"):
        hist["bounce_count"] += 1
    elif outcome == "invalidated":
        hist["invalid_count"] += 1
    save_zone_history()

def get_zone_bounce_info(symbol, z_low, z_high):
    """Check if this zone (or a very close price level) has bounced before"""
    key = zone_history_key(symbol, z_low, z_high)
    if key in zone_bounce_history:
        return zone_bounce_history[key]
    # Also check nearby zones for same symbol (within 2% of either edge)
    for hist in zone_bounce_history.values():
        if hist["symbol"] != symbol:
            continue
        if abs(hist["low"] - z_low) / z_low < 0.02 and abs(hist["high"] - z_high) / z_high < 0.02:
            return hist
    return None

def check_daily_confluence(symbol, z_low, z_high):
    """
    Check if a 4H (or lower TF) zone lines up with Daily support/resistance.
    Returns (is_confluent: bool, note: str)
    """
    klines_d = get_klines(symbol, interval="1d", limit=30)
    if not klines_d or len(klines_d) < 10:
        return False, ""
    highs_d = [float(k[2]) for k in klines_d[-25:-1]]
    lows_d  = [float(k[3]) for k in klines_d[-25:-1]]
    zone_mid = (z_low + z_high) / 2

    # Look for any daily high/low within 2.5% of the zone
    for h in highs_d:
        if abs(h - zone_mid) / zone_mid < 0.025:
            return True, f"Daily resistance ~{format_price(h)}"
    for l in lows_d:
        if abs(l - zone_mid) / zone_mid < 0.025:
            return True, f"Daily support ~{format_price(l)}"
    return False, ""

def calc_confidence(symbol, tf, current_price, z_low, z_high):
    score = 0
    details = []
    early_tf = {"4h": "1h", "1h": "15m", "15m": "5m"}.get(tf, "15m")

    klines_check = get_klines(symbol, interval=early_tf, limit=6)
    if klines_check and len(klines_check) >= 3:
        recent = klines_check[-3:-1]
        total_buy = sum(float(c[9]) for c in recent)
        total_vol = sum(float(c[5]) for c in recent)
        buy_pct = total_buy / total_vol * 100 if total_vol > 0 else 50
        if buy_pct >= 60:
            score += 25; details.append(f"✅ Buy {buy_pct:.0f}%")
        elif buy_pct <= 40:
            score -= 10; details.append(f"⚠️ Sell {100-buy_pct:.0f}%")
        else:
            details.append(f"⚖️ Mixed {buy_pct:.0f}%")

    if tf == "4h":
        klines_d = get_klines(symbol, interval="1d", limit=10)
        if klines_d and len(klines_d) >= 5:
            closes_d = [float(k[4]) for k in klines_d[-6:-1]]
            ema_d = calculate_ema(closes_d, min(5, len(closes_d)))
            if ema_d and current_price > ema_d:
                score += 20; details.append("✅ Daily trend bullish")
            else:
                score -= 10; details.append("⚠️ Daily trend bearish")
    elif tf == "1h":
        klines_4h = get_klines(symbol, interval="4h", limit=10)
        if klines_4h and len(klines_4h) >= 5:
            closes_4h = [float(k[4]) for k in klines_4h[-6:-1]]
            ema_4h = calculate_ema(closes_4h, min(5, len(closes_4h)))
            if ema_4h and current_price > ema_4h:
                score += 20; details.append("✅ 4H trend bullish")
            else:
                score -= 10; details.append("⚠️ 4H trend bearish")

    klines_main = get_klines(symbol, interval=tf, limit=20)
    if klines_main and len(klines_main) >= 10:
        recent_high = max(float(k[2]) for k in klines_main[-15:-2])
        drop_pct = (recent_high - current_price) / recent_high * 100
        if drop_pct >= 20:
            score += 25; details.append(f"✅ Big retracement -{drop_pct:.0f}%")
        elif drop_pct >= 10:
            score += 10; details.append(f"✅ Retracement -{drop_pct:.0f}%")
        else:
            details.append(f"⚠️ Small retracement -{drop_pct:.0f}%")

    if klines_check and len(klines_check) >= 4:
        cur_vol = float(klines_check[-2][5])
        prev_vols = [float(k[5]) for k in klines_check[-5:-2]]
        avg_v = sum(prev_vols)/len(prev_vols) if prev_vols else 1
        vol_r = cur_vol / avg_v
        if vol_r >= 2.0:
            score += 15; details.append(f"✅ Volume {vol_r:.1f}x")
        elif vol_r >= 1.3:
            score += 5; details.append(f"✅ Volume {vol_r:.1f}x")
        else:
            details.append(f"⚠️ Low volume {vol_r:.1f}x")

    if klines_main and check_hl_only(klines_main, lookback=8):
        score += 15; details.append("✅ Higher lows forming")
    else:
        details.append("⚠️ No higher lows")

    # v68: Multi-timeframe confluence (4H zone vs Daily S/R)
    if tf == "4h":
        is_confluent, conf_note = check_daily_confluence(symbol, z_low, z_high)
        if is_confluent:
            score += 15; details.append(f"🎯 Daily confluence ({conf_note})")

    # v68: Zone bounce history
    bounce_info = get_zone_bounce_info(symbol, z_low, z_high)
    if bounce_info and bounce_info.get("bounce_count", 0) > 0:
        bc = bounce_info["bounce_count"]
        score += min(15, bc * 8)
        details.append(f"📍 Previous bounce zone (x{bc})")

    label = "🟢 HIGH" if score >= 70 else ("🟡 MEDIUM" if score >= 40 else "🔴 LOW (risky)")
    return score, label, details


def check_manual_zones():
    now = time.time()
    to_remove = []

    for zone_id, zone in list(manual_zones.items()):
        symbol = zone["symbol"]
        tf     = zone["tf"]
        z_low  = zone["low"]
        z_high = zone["high"]
        state  = zone.get("state", "waiting")

        if now - zone["added_time"] > 30 * 24 * 3600:
            to_remove.append(zone_id)
            continue

        ticker = get_ticker(symbol)
        if not ticker:
            continue
        current_price = float(ticker["lastPrice"])
        change_24h    = float(ticker["priceChangePercent"])

        # ── waiting → in_zone ──
        if state == "waiting":
            if z_low * 0.99 <= current_price <= z_high * 1.02:
                manual_zones[zone_id]["state"] = "in_zone"
                manual_zones[zone_id]["lowest_in_zone"] = current_price
                manual_zones[zone_id]["confirmed"] = False
                manual_zones[zone_id]["zone_high_below"] = False
                save_zones()
            continue

        # ── in_zone ──
        if state == "in_zone":
            if current_price < zone.get("lowest_in_zone", z_low):
                manual_zones[zone_id]["lowest_in_zone"] = current_price

            klines_tf = get_klines(symbol, interval=tf, limit=10)
            if not klines_tf or len(klines_tf) < 6:
                continue

            last   = klines_tf[-2]
            l_open = float(last[1])
            l_close= float(last[4])
            l_high = float(last[2])
            l_low  = float(last[3])

            # Item #19: wick-rejection early warning. A candle at/near the zone
            # with a long lower wick and a small body signals sellers got rejected
            # fast (buyers stepped in before the close) — this is visible BEFORE
            # the full body-close+volume confirmation fires, so it gives an early
            # heads-up to watch the next candle closely instead of being caught
            # off guard by a sharp bounce (e.g. the ALICEUSDT case).
            candle_range = l_high - l_low
            if candle_range > 0:
                lower_wick = min(l_open, l_close) - l_low
                body = abs(l_close - l_open)
                wick_dominant = lower_wick / candle_range >= 0.55
                small_body = body / candle_range <= 0.35
                near_zone = l_low <= z_high and l_low >= z_low * 0.97
                wick_key = f"{zone_id}_wick_{int(last[0])}"  # candle open-time, so each candle only triggers once
                if wick_dominant and small_body and near_zone and not zone.get("last_wick_alert_candle") == last[0]:
                    manual_zones[zone_id]["last_wick_alert_candle"] = last[0]
                    save_zones()
                    send_to_topic(TOPIC_HIGH,
                        f"👀 <b>WATCH CLOSELY — Wick Rejection at Zone</b>\n\n"
                        f"🪙 <b>{symbol}</b> | {tf.upper()} OB\n"
                        f"🔲 Zone: {format_price(z_low)} — {format_price(z_high)}\n"
                        f"💰 Current: {format_price(current_price)}\n"
                        f"📉 Long lower wick + small body — possible early reversal\n\n"
                        f"⚠️ <i>Not a full confirmation yet — watch the next candle.</i>"
                    )
                    print(f"👀 Wick rejection watch: {zone_id}")

            if l_close < z_low and l_close < l_open:
                manual_zones[zone_id]["state"] = "dip_wait"
                manual_zones[zone_id]["zone_high_below"] = False
                save_zones()
                continue

            # Zone high touch notify — 4H body above z_high, once per touch cycle
            if l_close > z_high and l_open < z_high:
                # Body crossed zone high this candle
                if not zone.get("zone_high_touched"):
                    manual_zones[zone_id]["zone_high_touched"] = True
                    manual_zones[zone_id]["zone_high_below"] = False
                    touch_key = f"{zone_id}_touch"
                    if now - zone_high_alerted.get(touch_key, 0) > 4 * 3600:
                        zone_high_alerted[touch_key] = now
                        send_all(
                            f"📍 <b>Zone High Touch!</b>\n\n"
                            f"🪙 <b>{symbol}</b> | {tf.upper()} OB\n"
                            f"🔲 Zone: {format_price(z_low)} — {format_price(z_high)}\n"
                            f"💰 Price: {format_price(current_price)}\n"
                            f"🕐 {datetime.now().strftime('%H:%M:%S')}\n\n"
                            f"⚠️ <i>Check the chart before entry.</i>",
                            symbol=symbol
                        )
                        # Reset for next touch cycle after dip
            elif l_close < z_high:
                # Price came back below zone high — reset for next touch
                if zone.get("zone_high_touched"):
                    manual_zones[zone_id]["zone_high_touched"] = False
                    manual_zones[zone_id]["zone_high_below"] = True

            if zone.get("confirmed"):
                continue

            m_vol  = float(last[5])
            m_buy  = float(last[9])
            prev_vols = [float(k[5]) for k in klines_tf[-8:-2]]
            avg_vol = sum(prev_vols)/len(prev_vols) if prev_vols else 1
            vol_ratio = m_vol / avg_vol
            buy_ratio = m_buy / m_vol if m_vol > 0 else 0

            confirm_ok = (
                l_close > l_open and
                l_close > z_low and
                l_close > z_high * 0.95 and
                vol_ratio >= 1.5 and
                buy_ratio >= 0.52
            )

            if confirm_ok and now - zone.get("confirmed_time", 0) > 6 * 3600:
                lowest = zone.get("lowest_in_zone", z_low)
                recovery_pct = (current_price - lowest) / lowest * 100
                score, conf_label, details = calc_confidence(symbol, tf, current_price, z_low, z_high)

                # NOTE (item #9 fix): manual zones are YOUR own hand-picked levels —
                # a genuine body-close + volume + buy-pressure confirmation must ALWAYS
                # notify. The confidence score is informational context only (shown in
                # the message as HIGH/MEDIUM/LOW), never a silent gate that drops the
                # alert entirely. The old `if score < 40: continue` here was exactly
                # what caused SYNUSDT's vertical breakout to never notify — a fast,
                # no-retracement move scored low on a metric designed for pullback
                # setups, and the whole confirmation got swallowed with zero visibility.
                details_str = "\n   ".join(details)

                manual_zones[zone_id]["confirmed"] = True
                manual_zones[zone_id]["confirmed_time"] = now
                manual_zones[zone_id]["state"] = "post_confirm"
                manual_zones[zone_id]["peak_after_confirm"] = current_price
                manual_zones[zone_id]["went_up"] = False
                save_zones()
                record_zone_outcome(symbol, z_low, z_high, "confirmed")

                confluence_tag = ""
                if tf == "4h":
                    is_confluent, conf_note = check_daily_confluence(symbol, z_low, z_high)
                    if is_confluent:
                        confluence_tag = f"🎯 <b>HIGH CONFLUENCE ZONE</b> — {conf_note}\n"

                # Item #15: coiling duration context. A zone that's been actively
                # watched for a long time (many touches, no breakout yet) has been
                # "coiling" — accumulated pressure that tends to release in a bigger
                # move once it finally breaks. This doesn't predict WHEN a zone will
                # break, but flags the breakout, once it happens, as more significant
                # than a fresh/short-lived zone's bounce.
                coiling_days = (now - zone["added_time"]) / 86400
                coiling_tag = ""
                if coiling_days >= 30:
                    coiling_tag = f"⏳ <b>LONG-COILED ZONE</b> — active {coiling_days:.0f} days, this breakout may be significant\n"
                elif coiling_days >= 14:
                    coiling_tag = f"⏳ Coiling — active {coiling_days:.0f} days\n"

                msg = (
                    f"🎯 <b>ZONE CONFIRMED! [{tf.upper()} OB]</b>\n\n"
                    f"🪙 <b>{symbol}</b>\n"
                    f"💰 Price: {format_price(current_price)}\n"
                    f"📊 24h: {change_24h:+.2f}%\n"
                    f"🔲 Zone: {format_price(z_low)} — {format_price(z_high)}\n"
                    f"{confluence_tag}"
                    f"{coiling_tag}"
                    f"✅ {tf.upper()} green body close\n"
                    f"⚡ Volume: {vol_ratio:.1f}x | Buy: {buy_ratio*100:.0f}%\n"
                    f"📈 From zone low: +{recovery_pct:.1f}%\n\n"
                    f"📊 <b>Confidence: {conf_label}</b>\n"
                    f"   {details_str}\n\n"
                    f"🕐 {datetime.now().strftime('%H:%M:%S')}\n\n"
                    f"⚠️ <i>Check the chart before entry.</i>"
                )
                # Items #20/#21: Top Picks — the highest-confidence prospects get
                # their own topic, separated from the High Priority noise. Since
                # most manual zones are already 4H, requiring timeframe alone would
                # make almost everything a "Top Pick" and defeat the purpose. Instead
                # require at least 2 of these strong signals together: long coiling,
                # daily confluence, 1D timeframe specifically, or exceptional volume.
                # When this fires, the message goes ONLY to Top Picks, not also to
                # High Priority — no duplication, per explicit preference.
                top_pick_signals = sum([
                    coiling_days >= 30,
                    bool(confluence_tag),
                    tf == "1d",
                    vol_ratio >= 3.0,
                ])
                is_top_pick = top_pick_signals >= 2

                # Zone alerts → routed automatically by send_all via get_topic_for_message,
                # UNLESS this qualifies as a Top Pick, in which case it goes only there.
                if is_top_pick:
                    send_to_topic(TOPIC_TOP_PICKS, msg)
                    # Still broadcast to subscribers' personal DMs, just not to High Priority
                    for sub_chat_id in subscribers:
                        send_to(sub_chat_id, msg)
                else:
                    send_all(msg, symbol=symbol)
                print(f"🎯 Zone confirmed: {zone_id}{' [TOP PICK]' if is_top_pick else ''}")
                signal_performance[f"{symbol}_zone_{int(now)}"] = {
                    "symbol": symbol, "signal_price": current_price,
                    "signal_time": now, "signal_type": f"Zone OB [{tf.upper()}]",
                    "highest_after": current_price,
                }
                # v70: hand off to the separate trader_bot.py via shared queue file.
                # This bot does NOT execute trades itself — write-only, fire and forget.
                push_signal_to_queue({
                    "symbol": symbol, "zone_low": z_low, "zone_high": z_high,
                    "score": score, "tf": tf, "signal_label": f"Zone OB [{tf.upper()}]",
                    "signal_type": "zone_confirmed", "price": current_price,
                })
            continue

        # ── post_confirm ──
        if state == "post_confirm":
            confirmed_time = zone.get("confirmed_time", now)
            elapsed = now - confirmed_time

            if current_price > zone.get("peak_after_confirm", 0):
                manual_zones[zone_id]["peak_after_confirm"] = current_price
                if current_price > z_high * 1.02:
                    manual_zones[zone_id]["went_up"] = True

            peak = zone.get("peak_after_confirm", current_price)

            # Post-confirm invalidation
            if zone.get("went_up") and elapsed <= 3 * 3600:
                klines_tf = get_klines(symbol, interval=tf, limit=4)
                if klines_tf and len(klines_tf) >= 2:
                    last   = klines_tf[-2]
                    l_open = float(last[1])
                    l_close= float(last[4])
                    if l_close < z_low and l_close < l_open and not zone.get("post_invalid_sent"):
                        manual_zones[zone_id]["post_invalid_sent"] = True
                        manual_zones[zone_id]["state"] = "waiting"
                        save_zones()
                        record_zone_outcome(symbol, z_low, z_high, "invalidated")
                        inv_msg = (
                            f"❌ <b>POST-CONFIRM INVALIDATED!</b>\n\n"
                            f"🪙 {symbol} | {tf.upper()} OB\n"
                            f"📈 Peak: {format_price(peak)}\n"
                            f"📉 Close: {format_price(l_close)} (below zone)\n\n"
                            f"⚠️ Went up, then broke back below the zone."
                        )
                        send_to_topic(TOPIC_HIGH, inv_msg)
                        send_to(ADMIN_CHAT_ID, inv_msg)
                        continue

            # Retest: zone high touch → dip below z_high → touch again
            klines_tf = get_klines(symbol, interval=tf, limit=6)
            if not klines_tf or len(klines_tf) < 4:
                continue
            last   = klines_tf[-2]
            l_open = float(last[1])
            l_close= float(last[4])

            # Track retest cycle: went_up → dipped below z_high → above z_high again
            if current_price > z_high * 1.01 and zone.get("went_up"):
                if not zone.get("retest_dipped"):
                    # Not yet dipped — just tracking peak
                    pass
                elif not zone.get("retest_sent"):
                    # Dipped below z_high and now back above — RETEST!
                    l_vol  = float(last[5])
                    l_buy  = float(last[9])
                    prev_vols = [float(k[5]) for k in klines_tf[-6:-2]]
                    avg_vol = sum(prev_vols)/len(prev_vols) if prev_vols else 1
                    vol_r = l_vol / avg_vol
                    buy_r = l_buy / l_vol if l_vol > 0 else 0

                    if l_close > l_open and l_close > z_high and vol_r >= 1.5 and buy_r >= 0.52 and tf == "4h":
                        score, conf_label, details = calc_confidence(symbol, tf, current_price, z_low, z_high)
                        # Block LOW confidence retest
                        if score < 40:
                            manual_zones[zone_id]["retest_sent"] = True
                            manual_zones[zone_id]["state"] = "waiting"
                            save_zones()
                            continue
                        score, conf_label, details = calc_confidence(symbol, tf, current_price, z_low, z_high)
                        details_str = "\n   ".join(details)
                        manual_zones[zone_id]["retest_sent"] = True
                        manual_zones[zone_id]["retest_dipped"] = False
                        manual_zones[zone_id]["state"] = "waiting"
                        manual_zones[zone_id]["repump_sent"] = False
                        manual_zones[zone_id].pop("sideways_track", None)
                        save_zones()
                        record_zone_outcome(symbol, z_low, z_high, "retest_confirmed")
                        ret_msg = (
                            f"🔄 <b>RETEST CONFIRMED! [{tf.upper()} OB]</b>\n\n"
                            f"🪙 <b>{symbol}</b>\n"
                            f"💰 Price: {format_price(current_price)}\n"
                            f"📊 24h: {change_24h:+.2f}%\n"
                            f"🔲 Zone: {format_price(z_low)} — {format_price(z_high)}\n"
                            f"✅ {tf.upper()} body close above zone\n"
                            f"⚡ Volume: {vol_r:.1f}x | Buy: {buy_r*100:.0f}%\n\n"
                            f"📊 <b>Confidence: {conf_label}</b>\n"
                            f"   {details_str}\n\n"
                            f"🕐 {datetime.now().strftime('%H:%M:%S')}\n\n"
                            f"⚠️ <i>Strong retest! Take the entry.</i>"
                        )
                        send_all(ret_msg, symbol=symbol)
                        print(f"🔄 Retest confirmed: {zone_id}")
                        if score >= 70:
                            push_signal_to_queue({
                                "symbol": symbol, "zone_low": z_low, "zone_high": z_high,
                                "score": score, "tf": tf, "signal_label": f"Retest OB [{tf.upper()}]",
                                "signal_type": "retest_confirmed", "price": current_price,
                            })

            # Mark dipped when price goes below z_high after going up
            elif current_price < z_high and zone.get("went_up"):
                manual_zones[zone_id]["retest_dipped"] = True

            # ── v70: Sideways-then-repump detection ──
            # Handles the case where price goes up, then sits SIDEWAYS above the zone
            # (never dips back to retest), then breaks out again. Without this, the bot
            # only catches classic retests (dip back into zone, then reclaim) and misses
            # moves that consolidate higher up and continue — exactly what happened with
            # SYNUSDT (+71% move where price never came back down to retest the zone).
            if zone.get("went_up") and not zone.get("retest_dipped") and not zone.get("repump_sent"):
                track = manual_zones[zone_id].setdefault("sideways_track", {
                    "range_high": current_price, "range_low": current_price, "start_time": now
                })
                # Update the observed sideways range
                if current_price > track["range_high"]:
                    track["range_high"] = current_price
                if current_price < track["range_low"]:
                    track["range_low"] = current_price
                manual_zones[zone_id]["sideways_track"] = track
                save_zones()

                sideways_duration = now - track["start_time"]
                range_width_pct = (track["range_high"] - track["range_low"]) / track["range_low"] * 100 if track["range_low"] else 100

                # Need at least 45min of observed sideways behavior in a tight band (<6%)
                # before treating a new push as a genuine breakout, not just normal drift
                if sideways_duration >= 45 * 60 and range_width_pct < 6:
                    klines_tf2 = get_klines(symbol, interval=tf, limit=6)
                    if klines_tf2 and len(klines_tf2) >= 3:
                        last2 = klines_tf2[-2]
                        o2, c2, v2 = float(last2[1]), float(last2[4]), float(last2[5])
                        buy2 = float(last2[9])
                        prev_vols2 = [float(k[5]) for k in klines_tf2[-6:-2]]
                        avg_vol2 = sum(prev_vols2) / len(prev_vols2) if prev_vols2 else 1
                        vol_ratio2 = v2 / avg_vol2 if avg_vol2 else 0
                        buy_ratio2 = buy2 / v2 if v2 > 0 else 0
                        broke_range_high = c2 > track["range_high"] * 1.005

                        if c2 > o2 and broke_range_high and vol_ratio2 >= 1.5 and buy_ratio2 >= 0.52:
                            manual_zones[zone_id]["repump_sent"] = True
                            save_zones()
                            repump_msg = (
                                f"⚡ <b>RE-PUMP STARTING! [{tf.upper()} OB]</b>\n\n"
                                f"🪙 <b>{symbol}</b>\n"
                                f"💰 Price: {format_price(current_price)}\n"
                                f"📊 24h: {change_24h:+.2f}%\n"
                                f"🔲 Original zone: {format_price(z_low)} — {format_price(z_high)}\n"
                                f"↔️ Sideways range: {format_price(track['range_low'])} — {format_price(track['range_high'])} ({sideways_duration/60:.0f}min)\n"
                                f"✅ Breaking sideways range with strong candle\n"
                                f"⚡ Volume: {vol_ratio2:.1f}x | Buy: {buy_ratio2*100:.0f}%\n\n"
                                f"⚠️ <i>No retest — pumping directly from sideways. Look now.</i>"
                            )
                            send_all(repump_msg, symbol=symbol)
                            print(f"⚡ Re-pump (no retest): {zone_id}")

            if elapsed > 3 * 3600 and not zone.get("went_up"):
                manual_zones[zone_id]["state"] = "waiting"
                for k in ["confirmed","went_up","retest_dipped","retest_sent","post_invalid_sent","repump_sent"]:
                    manual_zones[zone_id][k] = False
                manual_zones[zone_id].pop("sideways_track", None)
                save_zones()
            continue

        # ── dip_wait → in_zone ──
        if state == "dip_wait":
            if z_low * 0.99 <= current_price <= z_high * 1.02:
                manual_zones[zone_id]["state"] = "in_zone"
                manual_zones[zone_id]["confirmed"] = False
                manual_zones[zone_id]["zone_high_touched"] = False
                save_zones()
            continue

    for zid in to_remove:
        manual_zones.pop(zid, None)
    if to_remove:
        save_zones()

# ─── ACTIVE TRADE MONITOR ──────────────────────────
"""
You report a trade entry manually with the /trade command (symbol, entry, sl, tp1/2/3).
On every candle close (default 1H), the bot checks three layers to build a trend health score:

  Layer A — EMA filter (base trend):     price closing below 20EMA adds +score
  Layer B — Candle/Volume pattern:       consecutive red candles + volume > avg*1.5 adds +score
  Layer C — Structure break:             a break below entry or recent swing low adds +score

Based on the score threshold, an alert goes to the admin-only Trade Monitor topic:
  30-49  → ⚠️ Caution (info only)
  50-69  → 🔶 Trend weakening, tighten SL
  70+    → 🔴 High Priority, consider exit

It also gives a dynamic SL trail suggestion based on profit (1R → breakeven, 2R → lock 1R).
This is entirely admin-only — it never goes to the subscriber broadcast.
"""

def calc_trade_trend_score(symbol, tf, entry_price, direction="long"):
    """
    Returns (score, label, details, red_streak_info)
    direction: only "long" is supported right now (spot trading)
    """
    score = 0
    details = []

    klines = get_klines(symbol, interval=tf, limit=30)
    if not klines or len(klines) < 15:
        return 0, "🟢 OK (no data)", ["⚠️ Klines unavailable"], {}

    closed = klines[:-1]  # exclude unclosed candle
    closes = [float(k[4]) for k in closed]
    current_price = closes[-1]

    # ── Layer A: EMA filter ──
    ema = calculate_ema(closes, TRADE_EMA_PERIOD)
    ema_break = False
    if ema:
        if current_price < ema:
            ema_break = True
            score += 30
            details.append(f"⚠️ Price below {TRADE_EMA_PERIOD}EMA ({format_price(ema)})")
        else:
            details.append(f"✅ Price above {TRADE_EMA_PERIOD}EMA ({format_price(ema)})")

    # ── Layer B: Candle/Volume pattern ──
    recent = closed[-8:]
    vols = [float(k[5]) for k in recent]
    avg_vol = sum(vols[:-1]) / len(vols[:-1]) if len(vols) > 1 else (vols[0] if vols else 1)

    red_streak = 0
    aggressive_red = 0
    streak_drop_start = None
    for k in reversed(recent):
        o, c, v = float(k[1]), float(k[4]), float(k[5])
        if c < o:
            red_streak += 1
            if streak_drop_start is None:
                streak_drop_start = o
            if v > avg_vol * TRADE_VOL_MULTIPLIER:
                aggressive_red += 1
        else:
            break

    if red_streak >= 2:
        score += 20
        details.append(f"⚠️ {red_streak} consecutive red candles")
    if aggressive_red >= 1:
        score += 20
        details.append(f"⚠️ {aggressive_red} red candle(s) with volume >{TRADE_VOL_MULTIPLIER}x avg (aggressive selling)")

    # Heavy retracement check — how much of the recent swing (low to high) has been given back
    recent_high = max(float(k[2]) for k in closed[-12:])
    recent_low_for_swing = min(float(k[3]) for k in closed[-12:])
    if recent_high > recent_low_for_swing:
        swing_size = recent_high - recent_low_for_swing
        given_back = recent_high - current_price
        retrace_pct = min(given_back / swing_size, 1.0)  # capped at 100%
        if retrace_pct >= TRADE_RETRACE_HEAVY and recent_high > entry_price:
            score += 20
            details.append(f"⚠️ Heavy retracement — {retrace_pct*100:.0f}% of recent swing given back")

    # ── Layer C: Structure break ──
    swing_low = min(float(k[3]) for k in closed[-10:-1])
    structure_break = False
    if current_price < swing_low and current_price < entry_price:
        structure_break = True
        score += 30
        details.append(f"⚠️ Structure break — closed below recent swing low ({format_price(swing_low)})")
    elif current_price < entry_price:
        details.append(f"⚠️ Price below entry but structure holding")
    else:
        details.append(f"✅ Structure intact, above entry")

    label = "🔴 HIGH RISK" if score >= TRADE_SCORE_HIGH else ("🔶 WEAKENING" if score >= TRADE_SCORE_WEAKENING else ("⚠️ CAUTION" if score >= TRADE_SCORE_CAUTION else "🟢 HEALTHY"))
    return score, label, details, {"red_streak": red_streak, "aggressive_red": aggressive_red, "ema_break": ema_break, "structure_break": structure_break}


def check_active_trades():
    now = time.time()
    to_close = []

    for trade_id, trade in list(active_trades.items()):
        symbol  = trade["symbol"]
        entry   = trade["entry"]
        sl      = trade["sl"]
        tps     = trade.get("tps", [])
        tf      = trade.get("tf", TRADE_CHECK_TF_DEFAULT)

        ticker = get_ticker(symbol)
        if not ticker:
            continue
        current_price = float(ticker["lastPrice"])

        # ── Hard SL/TP hit check ──
        if current_price <= sl:
            send_to_topic(TOPIC_TRADES,
                f"🛑 <b>SL HIT</b>\n\n🪙 {symbol}\n💰 Entry: {format_price(entry)} → SL: {format_price(sl)}\n📉 Current: {format_price(current_price)}\n\n<i>Trade auto-closed from monitor.</i>"
            )
            to_close.append(trade_id)
            continue

        hit_tps = trade.get("hit_tps", [])
        for i, tp in enumerate(tps):
            tp_label = f"tp{i+1}"
            if current_price >= tp and tp_label not in hit_tps:
                hit_tps.append(tp_label)
                active_trades[trade_id]["hit_tps"] = hit_tps
                save_active_trades()
                send_to_topic(TOPIC_TRADES,
                    f"✅ <b>TP{i+1} HIT!</b>\n\n🪙 {symbol}\n🎯 Target: {format_price(tp)}\n💰 Current: {format_price(current_price)}\n\n<i>Consider partial close / trail SL.</i>"
                )

        # ── Dynamic SL trail suggestion (R-multiple based) ──
        risk = entry - sl
        if risk > 0:
            r_multiple = (current_price - entry) / risk
            trail_stage = trade.get("trail_stage", 0)
            if r_multiple >= TRADE_TRAIL_TRIGGER_R2 and trail_stage < 2:
                active_trades[trade_id]["trail_stage"] = 2
                save_active_trades()
                suggested_sl = entry + risk * TRADE_TRAIL_TRIGGER_R
                send_to_topic(TOPIC_TRADES,
                    f"📈 <b>TRAIL SL — Lock 1R Profit</b>\n\n🪙 {symbol}\n💰 Current: {format_price(current_price)} ({r_multiple:.1f}R)\n🔧 Suggest moving SL to: {format_price(suggested_sl)}"
                )
            elif r_multiple >= TRADE_TRAIL_TRIGGER_R and trail_stage < 1:
                active_trades[trade_id]["trail_stage"] = 1
                save_active_trades()
                send_to_topic(TOPIC_TRADES,
                    f"📈 <b>TRAIL SL — Breakeven</b>\n\n🪙 {symbol}\n💰 Current: {format_price(current_price)} ({r_multiple:.1f}R)\n🔧 Suggest moving SL to entry: {format_price(entry)}"
                )

        # ── Trend health score (combination logic) ──
        score, label, details, info = calc_trade_trend_score(symbol, tf, entry)
        last_score = trade.get("last_score", 0)
        active_trades[trade_id]["last_score"] = score
        save_active_trades()

        cooldown_ok = now - trade_alert_cooldown.get(trade_id, 0) > 3 * 3600
        crossed_up_threshold = score >= TRADE_SCORE_CAUTION and (last_score < TRADE_SCORE_CAUTION or cooldown_ok)

        if score >= TRADE_SCORE_CAUTION and crossed_up_threshold:
            trade_alert_cooldown[trade_id] = now
            details_str = "\n   ".join(details)
            if score >= TRADE_SCORE_HIGH:
                header = "🔴 <b>HIGH PRIORITY — Trend Reversal Risk</b>"
                footer = "<i>⚠️ Strongly consider exiting or tightening SL now.</i>"
            elif score >= TRADE_SCORE_WEAKENING:
                header = "🔶 <b>Trend Weakening</b>"
                footer = "<i>Consider tightening SL to reduce risk.</i>"
            else:
                header = "⚠️ <b>Early Caution</b>"
                footer = "<i>Just monitor — no action needed yet.</i>"

            send_to_topic(TOPIC_TRADES,
                f"{header}\n\n"
                f"🪙 <b>{symbol}</b> | {tf.upper()}\n"
                f"💰 Entry: {format_price(entry)} | Current: {format_price(current_price)}\n"
                f"📊 Trend Score: {score} ({label})\n"
                f"   {details_str}\n\n"
                f"🕐 {datetime.now().strftime('%H:%M:%S')}\n\n"
                f"{footer}"
            )

    for tid in to_close:
        active_trades.pop(tid, None)
        trade_alert_cooldown.pop(tid, None)
    if to_close:
        save_active_trades()

# ─── EXPLOSIVE PUMP DETECTOR (RIF type) ──────────────────
def check_explosive_pump(symbol):
    """
    Detect a sudden explosive move across 1-3 candles.
    Liquidity grab + breakout pattern.
    Monitored on the 5M timeframe to catch fast pumps.
    """
    now = time.time()
    key = f"{symbol}_explosive"
    if now - alerted_coins.get(key, 0) < 3600:
        return

    klines = get_klines(symbol, interval="5m", limit=15)
    if not klines or len(klines) < 10:
        return

    # Last 3 closed candles
    c3 = klines[-4]  # 3 candles ago
    c2 = klines[-3]
    c1 = klines[-2]  # last closed

    c1_open  = float(c1[1])
    c1_close = float(c1[4])
    c1_high  = float(c1[2])
    c1_vol   = float(c1[5])
    c1_buy   = float(c1[9])

    # Must be bullish
    if c1_close <= c1_open:
        return

    # Price gain in last 3 candles
    base_price = float(c3[3])  # low of 3 candles ago
    gain_pct = (c1_close - base_price) / base_price * 100
    if gain_pct < 5.0:  # minimum 5% in 3 candles
        return

    # Volume explosion
    prev_vols = [float(k[5]) for k in klines[-10:-4]]
    avg_vol = sum(prev_vols) / len(prev_vols) if prev_vols else 1
    vol_ratio = c1_vol / avg_vol
    if vol_ratio < 10.0:  # minimum 10x spike
        return

    # Buy dominant
    buy_ratio = c1_buy / c1_vol if c1_vol > 0 else 0
    if buy_ratio < 0.60:
        return

    # 24h positive
    ticker = get_ticker(symbol)
    if not ticker:
        return
    change_24h = float(ticker["priceChangePercent"])
    if change_24h < 0:
        return

    current_price = float(ticker["lastPrice"])

    # Price still reasonably near the explosive candle (not already dumped).
    # FIX (item #7): this was 0.97 (only allowing a 3% pullback before silently
    # dropping the alert), which is what caused RARE and TNSR to be missed —
    # by the time the scan loop reached them, a completely normal pullback from
    # the peak had already exceeded that tiny buffer. Relaxed to 0.90 (10%
    # buffer) so a real, still-valid explosive move isn't thrown away just
    # because of scan timing.
    if current_price < c1_close * 0.90:
        return

    alerted_coins[key] = now

    ft_score, ft_details = calc_followthrough_score(symbol, "5m", klines, vol_ratio, buy_ratio, current_price)
    high_potential = ft_score >= 60
    ft_tag = ""
    if high_potential:
        ft_details_str = "\n   ".join(ft_details)
        ft_tag = f"\n\n🔥 <b>HIGH FOLLOW-THROUGH POTENTIAL ({ft_score})</b>\n   {ft_details_str}"

    msg = (
        f"💥 <b>EXPLOSIVE PUMP DETECTED! [5M]</b>\n\n"
        f"🪙 <b>{symbol}</b>\n"
        f"💰 Price: {format_price(current_price)}\n"
        f"📊 24h: {change_24h:+.2f}%\n"
        f"🚀 Across 3 candles: <b>+{gain_pct:.1f}%</b>\n"
        f"⚡ Volume: <b>{vol_ratio:.1f}x</b> normal\n"
        f"🟢 Buy pressure: {buy_ratio*100:.0f}%\n"
        f"🕐 {datetime.now().strftime('%H:%M:%S')}\n"
        f"{ft_tag}\n\n"
        f"⚠️ <i>Fast move! Look for an entry on a retracement.</i>"
    )
    sent = send_all(msg, symbol=symbol)
    if sent and high_potential:
        send_to_topic(TOPIC_HIGH, msg)
    if sent:
        print(f"💥 Explosive pump: {symbol} (+{gain_pct:.1f}% in 3 candles) | FT score: {ft_score}")
        signal_performance[f"{symbol}_explosive_{int(now)}"] = {
            "symbol": symbol, "signal_price": current_price,
            "signal_time": now, "signal_type": "Explosive Pump [5M]",
            "highest_after": current_price,
        }

# ─── DAILY TREND FILTER ───────────────────────────────────
def is_daily_downtrend(symbol, current_price):
    """
    Returns True if the symbol is in a daily downtrend.
    True = skip the signal.
    """
    klines_daily = get_klines(symbol, interval="1d", limit=15)
    if not klines_daily or len(klines_daily) < 7:
        return False  # no data, don't skip

    daily_closes = [float(k[4]) for k in klines_daily[-8:-1]]
    daily_ema = calculate_ema(daily_closes, min(7, len(daily_closes)))

    # 1. Price below Daily EMA
    if daily_ema and current_price < daily_ema:
        return True

    # 2. Recent Daily candle -10%+ dump
    last_daily = klines_daily[-2]
    d_open  = float(last_daily[1])
    d_close = float(last_daily[4])
    if d_open > 0 and (d_close - d_open) / d_open < -0.10:
        return True

    # 3. 4 consecutive lower daily closes
    d_closes = [float(k[4]) for k in klines_daily[-5:-1]]
    if len(d_closes) >= 4:
        if d_closes[-1] < d_closes[-2] < d_closes[-3] < d_closes[-4]:
            return True

    return False

# ─── ABNORMAL VOLUME DETECTOR (100x+) ───────────────────
def check_abnormal_volume(symbol):
    now = time.time()
    key = f"{symbol}_abnormal"
    if now - alerted_coins.get(key, 0) < 6 * 3600:
        return

    klines = get_klines(symbol, interval="1h", limit=20)
    if not klines or len(klines) < 10:
        return

    last = klines[-2]
    c_open  = float(last[1])
    c_close = float(last[4])
    c_vol   = float(last[5])

    if c_close <= c_open:
        return

    prev_vols = [float(k[5]) for k in klines[-10:-2]]
    avg_vol = sum(prev_vols) / len(prev_vols) if prev_vols else 1
    vol_ratio = c_vol / avg_vol

    if vol_ratio < 100.0:
        return

    ticker = get_ticker(symbol)
    if not ticker:
        return
    current_price = float(ticker["lastPrice"])
    change_24h = float(ticker["priceChangePercent"])

    if change_24h < 0:
        return
    # Note: daily downtrend filter NOT applied for abnormal volume
    # 100x+ volume is significant regardless of trend

    alerted_coins[key] = now
    sent = send_all(
        f"🚨 <b>ABNORMAL VOLUME! [1H]</b>\n\n"
        f"🪙 <b>{symbol}</b>\n"
        f"💰 Price: {format_price(current_price)}\n"
        f"📊 24h: {change_24h:+.2f}%\n"
        f"⚡ Volume: <b>{vol_ratio:.0f}x normal</b>\n"
        f"🔥 Big pump expected!\n"
        f"🕐 {datetime.now().strftime('%H:%M:%S')}",
        symbol=symbol
    )
    if sent:
        print(f"🚨 Abnormal volume: {symbol} ({vol_ratio:.0f}x)")
        signal_performance[f"{symbol}_abnormal_{int(now)}"] = {
            "symbol": symbol, "signal_price": current_price,
            "signal_time": now, "signal_type": "Abnormal Volume [1H]",
            "highest_after": current_price,
        }

# ─── PRE-PUMP DETECTOR ────────────────────────────────────
def calc_atr_main(klines, period=14):
    """ATR calculate for entry/exit"""
    if len(klines) < period + 1:
        return None
    trs = []
    for i in range(1, len(klines)):
        h = float(klines[i][2])
        l = float(klines[i][3])
        pc = float(klines[i-1][4])
        trs.append(max(h-l, abs(h-pc), abs(l-pc)))
    return sum(trs[-period:]) / period if len(trs) >= period else None

def send_entry_exit(symbol, entry_price, klines_4h):
    """Entry/Exit details → admin only, High Priority"""
    atr = calc_atr_main(klines_4h)
    if not atr:
        return
    sl   = entry_price - atr * 1.5
    risk = entry_price - sl
    tp1  = entry_price + risk * 1.5
    tp2  = entry_price + risk * 2.5
    tp3  = entry_price + risk * 4.0

    sl_pct  = (sl  - entry_price) / entry_price * 100
    tp1_pct = (tp1 - entry_price) / entry_price * 100
    tp2_pct = (tp2 - entry_price) / entry_price * 100
    tp3_pct = (tp3 - entry_price) / entry_price * 100

    msg = (
        f"📍 <b>ENTRY DETAILS — {symbol}</b>\n\n"
        f"💰 Entry: {format_price(entry_price)}\n"
        f"🛑 SL: {format_price(sl)} ({sl_pct:.1f}%) [1.5x ATR]\n\n"
        f"🎯 TP1: {format_price(tp1)} (+{tp1_pct:.1f}%) [1.5R]\n"
        f"🎯 TP2: {format_price(tp2)} (+{tp2_pct:.1f}%) [2.5R]\n"
        f"🎯 TP3: {format_price(tp3)} (+{tp3_pct:.1f}%) [4R]\n\n"
        f"📐 ATR(14): {format_price(atr)}"
    )
    send_to_topic(TOPIC_HIGH, msg)
    send_to(ADMIN_CHAT_ID, msg)

# Pre-pump phase tracking
prepump_phases = {}  # {symbol: {phase, phase1_time, phase2_time}}

def check_prepump(symbol):
    now = time.time()
    key = f"{symbol}_prepump"
    if now - alerted_coins.get(key, 0) < 4 * 3600:
        return

    klines_1h = get_klines(symbol, interval="1h", limit=48)
    klines_4h = get_klines(symbol, interval="4h", limit=30)
    klines_daily = get_klines(symbol, interval="1d", limit=14)

    if not klines_1h or len(klines_1h) < 12:
        return

    ticker = get_ticker(symbol)
    if not ticker:
        return
    current_price = float(ticker["lastPrice"])
    change_24h    = float(ticker["priceChangePercent"])

    if change_24h < 1.0:
        return
    if is_daily_downtrend(symbol, current_price):
        return

    # ── Common signals ──

    # Volume building (1H)
    vols_1h = [float(k[5]) for k in klines_1h[-12:-1]]
    avg_early = sum(vols_1h[:6]) / 6 if len(vols_1h) >= 6 else 1
    avg_late  = sum(vols_1h[6:]) / len(vols_1h[6:]) if len(vols_1h[6:]) > 0 else 1
    vol_building = avg_late > avg_early * 1.3

    # Higher lows
    hl_1h = check_hl_only(klines_1h, lookback=8)

    # Tight range
    recent_highs = [float(k[2]) for k in klines_1h[-8:-1]]
    recent_lows  = [float(k[3]) for k in klines_1h[-8:-1]]
    price_range  = (max(recent_highs) - min(recent_lows)) / min(recent_lows) * 100 if recent_lows else 100
    tight_range  = price_range < 10.0

    # Buy pressure
    recent_3 = klines_1h[-4:-1]
    t_buy = sum(float(c[9]) for c in recent_3)
    t_vol = sum(float(c[5]) for c in recent_3)
    buy_pct = t_buy / t_vol * 100 if t_vol > 0 else 50
    buy_pressure = buy_pct >= 58

    # 4H volume consistency
    vol_4h_ok = False
    if klines_4h and len(klines_4h) >= 8:
        vols_4h = [float(k[5]) for k in klines_4h[-6:-1]]
        avg_4h_early = sum(vols_4h[:3]) / 3
        avg_4h_late  = sum(vols_4h[3:]) / 3 if len(vols_4h) >= 6 else avg_4h_early
        vol_4h_ok = avg_4h_late > avg_4h_early * 1.2

    # Weekly uptrend
    weekly_ok = False
    if klines_daily and len(klines_daily) >= 7:
        closes_w = [float(k[4]) for k in klines_daily[-8:-1]]
        ema7 = calculate_ema(closes_w, 7)
        weekly_ok = ema7 and current_price > ema7 * 1.02

    # RSI oversold recovery (simplified)
    rsi_ok = False
    if klines_1h and len(klines_1h) >= 15:
        closes_rsi = [float(k[4]) for k in klines_1h[-15:-1]]
        gains = [max(0, closes_rsi[i]-closes_rsi[i-1]) for i in range(1,len(closes_rsi))]
        losses = [max(0, closes_rsi[i-1]-closes_rsi[i]) for i in range(1,len(closes_rsi))]
        avg_g = sum(gains)/len(gains) if gains else 0
        avg_l = sum(losses)/len(losses) if losses else 1
        rsi = 100 - (100/(1 + avg_g/avg_l)) if avg_l > 0 else 50
        rsi_ok = 35 <= rsi <= 55  # recovering from oversold

    score_phase1 = sum([vol_building, hl_1h, tight_range])
    score_phase2 = sum([vol_building, hl_1h, tight_range, buy_pressure, vol_4h_ok])
    score_phase3 = sum([vol_building, hl_1h, tight_range, buy_pressure, vol_4h_ok, weekly_ok, rsi_ok])

    phase_data = prepump_phases.get(symbol, {"phase": 0, "phase1_time": 0, "phase2_time": 0})
    current_phase = phase_data.get("phase", 0)

    signs_1 = []
    if vol_building:  signs_1.append("📈 Volume building")
    if hl_1h:         signs_1.append("✅ Higher lows [1H]")
    if tight_range:   signs_1.append(f"🔲 Tight range ({price_range:.0f}%)")

    signs_2 = signs_1.copy()
    if buy_pressure:  signs_2.append(f"🟢 Buy pressure {buy_pct:.0f}%")
    if vol_4h_ok:     signs_2.append("📊 4H volume building")

    signs_3 = signs_2.copy()
    if weekly_ok:     signs_3.append("✅ Weekly uptrend")
    if rsi_ok:        signs_3.append(f"📉 RSI recovering ({rsi:.0f})")

    # ── PHASE 1: Early Watch — tracked internally only, no notification.
    # Phase 1 used to send a message here, but it's not yet an actionable signal
    # (just early accumulation forming) and was generating too much noise. The
    # bot still records phase1_time below so Phase 2's 30-min cooldown-after-
    # phase-1 logic keeps working exactly as before — only the notification
    # itself is removed. ──
    if current_phase == 0 and score_phase1 >= 3:
        alerted_coins[key] = now
        prepump_phases[symbol] = {"phase": 1, "phase1_time": now, "phase2_time": 0}
        return

    # ── PHASE 2: Setup Forming — subscribers + buildups topic ──
    if current_phase == 1 and score_phase2 >= 4:
        elapsed_p1 = now - phase_data.get("phase1_time", now)
        if elapsed_p1 < 30 * 60:  # min 30min after phase 1
            return
        alerted_coins[key] = now
        prepump_phases[symbol]["phase"] = 2
        prepump_phases[symbol]["phase2_time"] = now

        msg = (
            f"🔔 <b>PRE-PUMP SETUP! [Phase 2]</b>\n\n"
            f"🪙 <b>{symbol}</b>\n"
            f"💰 Price: {format_price(current_price)}\n"
            f"📊 24h: {change_24h:+.2f}%\n\n"
            f"{'\\n'.join(signs_2)}\n\n"
            f"🕐 {datetime.now().strftime('%H:%M:%S')}\n\n"
            f"⚠️ <i>Pump may be coming! Check the chart.</i>"
        )
        send_all(msg, symbol=symbol)
        signal_performance[f"{symbol}_prepump_{int(now)}"] = {
            "symbol": symbol, "signal_price": current_price,
            "signal_time": now, "signal_type": "Pre-pump Setup [1H]",
            "highest_after": current_price,
        }
        print(f"🔔 Phase 2: {symbol}")
        return

    # ── PHASE 3: Breakout — High Priority ──
    if current_phase >= 2 and score_phase3 >= 5:
        elapsed_p2 = now - phase_data.get("phase2_time", now)
        if elapsed_p2 < 30 * 60:
            return
        alerted_coins[key] = now
        prepump_phases[symbol]["phase"] = 3

        msg = (
            f"🚀 <b>PRE-PUMP BREAKOUT! [Phase 3]</b>\n\n"
            f"🪙 <b>{symbol}</b>\n"
            f"💰 Price: {format_price(current_price)}\n"
            f"📊 24h: {change_24h:+.2f}%\n\n"
            f"{'\\n'.join(signs_3)}\n\n"
            f"🕐 {datetime.now().strftime('%H:%M:%S')}\n\n"
            f"⚠️ <i>Strong setup! Take the entry.</i>"
        )
        send_all(msg, symbol=symbol)

        # ENTRY DETAILS (ATR-based auto SL/TP suggestion) — DISABLED per request.
        # Was firing automatically on every Phase 3 breakout; turned off, function kept
        # below in case it's wanted again later (e.g. wired into /entry on-demand instead).
        # if klines_4h:
        #     send_entry_exit(symbol, current_price, klines_4h)

        signal_performance[f"{symbol}_prepump_{int(now)}"] = {
            "symbol": symbol, "signal_price": current_price,
            "signal_time": now, "signal_type": "Pre-pump Setup [1H]",
            "highest_after": current_price,
        }
        print(f"🚀 Phase 3: {symbol}")



# ─── BIG PUMP SETUP DETECTOR (30%+ potential) ────────────
big_pump_alerted = {}

def check_big_pump_setup(symbol):
    now = time.time()
    key = f"{symbol}_bigpump"
    if now - big_pump_alerted.get(key, 0) < 24 * 3600:
        return

    klines_daily = get_klines(symbol, interval="1d", limit=20)
    if not klines_daily or len(klines_daily) < 10:
        return

    ticker = get_ticker(symbol)
    if not ticker:
        return
    current_price = float(ticker["lastPrice"])
    change_24h = float(ticker["priceChangePercent"])

    if change_24h < 3.0:
        return
    if is_daily_downtrend(symbol, current_price):
        return

    # 1. Long consolidation: 7+ days tight range
    recent = klines_daily[-8:-1]
    highs = [float(k[2]) for k in recent]
    lows  = [float(k[3]) for k in recent]
    if not highs or not lows:
        return
    price_range = (max(highs) - min(lows)) / min(lows) * 100
    consolidation = price_range < 15.0  # tight range

    # 2. Volume building
    vols = [float(k[5]) for k in klines_daily[-8:-1]]
    avg_early = sum(vols[:4]) / 4 if len(vols) >= 4 else 1
    avg_late  = sum(vols[4:]) / len(vols[4:]) if len(vols) > 4 else 1
    vol_building = avg_late > avg_early * 1.8

    # 3. Higher lows on daily
    klines_4h = get_klines(symbol, interval="4h", limit=20)
    hl = check_hl_only(klines_4h, lookback=10) if klines_4h else False

    # 4. Weekly support — price near recent low
    recent_low = min(lows)
    near_support = current_price < recent_low * 1.15

    # 5. Buy pressure
    klines_1h = get_klines(symbol, interval="1h", limit=6)
    buy_pressure = False
    if klines_1h and len(klines_1h) >= 4:
        recent_1h = klines_1h[-4:-1]
        t_buy = sum(float(c[9]) for c in recent_1h)
        t_vol = sum(float(c[5]) for c in recent_1h)
        bp = t_buy / t_vol * 100 if t_vol > 0 else 0
        buy_pressure = bp >= 60

    score = sum([consolidation, vol_building, hl, near_support, buy_pressure])
    if score < 4:
        return

    signs = []
    if consolidation:   signs.append(f"✅ Tight consolidation ({price_range:.0f}% range)")
    if vol_building:    signs.append("✅ Volume building")
    if hl:              signs.append("✅ Higher lows")
    if near_support:    signs.append("✅ Near support level")
    if buy_pressure:    signs.append(f"✅ Buy pressure {bp:.0f}%")

    big_pump_alerted[key] = now
    sent = send_all(
        f"🔥 <b>BIG PUMP SETUP! ({score}/5)</b>\n\n"
        f"🪙 <b>{symbol}</b>\n"
        f"💰 Price: {format_price(current_price)}\n"
        f"📊 24h: {change_24h:+.2f}%\n\n"
        f"{'\\n'.join(signs)}\n\n"
        f"🕐 {datetime.now().strftime('%H:%M:%S')}\n\n"
        f"⚠️ <i>30%+ pump possible! Check the chart.</i>",
        symbol=symbol
    )
    if sent:
        print(f"🔥 Big pump setup: {symbol} ({score}/5)")

# ─── AUTO WATCHLIST UPDATE (Daily) ───────────────────────
last_auto_update = 0

def auto_update_watchlist():
    global last_auto_update
    now = time.time()
    # Run once per day
    if now - last_auto_update < 6 * 3600:
        return
    last_auto_update = now

    try:
        # Get all Binance Spot USDT pairs
        r = http_session.get(
            "https://api.binance.com/api/v3/ticker/24hr",
            timeout=15
        )
        if r.status_code != 200:
            return

        all_tickers = r.json()
        skip_keywords = ['USDC','BUSD','TUSD','USDP','USDS','RLUS','FDUSD','WBTC','WBETH','WETH']
        new_coins = []

        for t in all_tickers:
            sym = t.get("symbol","")
            if not sym.endswith("USDT"):
                continue
            base = sym.replace("USDT","")
            if any(k in base for k in skip_keywords):
                continue
            try:
                vol_usd = float(t.get("quoteVolume", 0))
                chg     = float(t.get("priceChangePercent", 0))
            except:
                continue
            if vol_usd < 500_000:
                continue
            if sym in watchlist:
                continue
            new_coins.append((sym, chg, vol_usd))

        if not new_coins:
            return

        # Sort by volume, take top gainers + losers
        gainers = sorted([c for c in new_coins if c[1] > 0], key=lambda x: x[1], reverse=True)[:10]
        losers  = sorted([c for c in new_coins if c[1] < 0], key=lambda x: x[1])[:5]
        to_add  = gainers + losers

        added = []
        for sym, chg, vol in to_add:
            r_check = http_session.get(
                f"https://api.binance.com/api/v3/ticker/price?symbol={sym}",
                timeout=5
            )
            if r_check.status_code == 200:
                watchlist.append(sym)
                added.append(f"{sym} ({chg:+.1f}%)")

        if added:
            save_watchlist_file()
            vol_str = '\n'.join(f"• {c}" for c in added)
            send_to(ADMIN_CHAT_ID,
                f"📋 <b>Auto Watchlist Update</b>\n\n"
                f"✅ {len(added)} new coins added:\n{vol_str}\n\n"
                f"Total: {len(watchlist)} coins"
            )
            print(f"📋 Auto added {len(added)} coins")

    except Exception as e:
        print(f"Auto update error: {e}")

# ─── BREAKOUT DETECTORS ───────────────────────────────────
breakout_alerted = {}

def check_breakouts(symbol):
    """
    Detect: Descending Triangle, Ascending Triangle,
    Falling Wedge, Bull Flag, Key Level Breakout
    All → High Priority, once per breakout, with retest warning
    """
    now = time.time()
    key = f"{symbol}_breakout"
    if now - breakout_alerted.get(key, 0) < 12 * 3600:
        return

    ticker = get_ticker(symbol)
    if not ticker:
        return
    current_price = float(ticker["lastPrice"])
    change_24h    = float(ticker["priceChangePercent"])

    if change_24h < 2.0:
        return
    if is_daily_downtrend(symbol, current_price):
        return

    klines = get_klines(symbol, interval="4h", limit=30)
    if not klines or len(klines) < 15:
        return

    # Use last 2 confirmed candles for volume
    last_c  = klines[-2]
    prev_c  = klines[-3]
    l_open  = float(last_c[1])
    l_high  = float(last_c[2])
    l_close = float(last_c[4])
    l_vol   = float(last_c[5])
    l_buy   = float(last_c[9])

    if l_close <= l_open:
        return  # Must be bullish candle

    prev_vols = [float(k[5]) for k in klines[-10:-2]]
    avg_vol = sum(prev_vols)/len(prev_vols) if prev_vols else 1
    vol_ratio = l_vol / avg_vol
    buy_ratio = l_buy / l_vol if l_vol > 0 else 0

    if vol_ratio < 2.0 or buy_ratio < 0.55:
        return

    highs  = [float(k[2]) for k in klines[-15:-1]]
    lows   = [float(k[3]) for k in klines[-15:-1]]
    closes = [float(k[4]) for k in klines[-15:-1]]

    pattern = None
    pattern_desc = ""

    # 1. Descending Triangle — lower highs + flat support
    recent_highs = highs[-8:]
    recent_lows  = lows[-8:]
    lower_highs = all(recent_highs[i] <= recent_highs[i-1] for i in range(1, len(recent_highs)))
    support_lvl = min(recent_lows)
    flat_support = (max(recent_lows) - min(recent_lows)) / min(recent_lows) < 0.04
    if lower_highs and flat_support and l_close > max(recent_highs[-3:]) * 1.01:
        pattern = "Descending Triangle Breakout"
        pattern_desc = f"Lower highs + flat support ${format_price(support_lvl)} break"

    # 2. Ascending Triangle — higher lows + flat resistance
    if not pattern:
        higher_lows = all(recent_lows[i] >= recent_lows[i-1] for i in range(1, len(recent_lows)))
        resistance_lvl = max(recent_highs)
        flat_resist = (max(recent_highs) - sorted(recent_highs)[-3]) / max(recent_highs) < 0.03
        if higher_lows and flat_resist and l_close > resistance_lvl * 1.01:
            pattern = "Ascending Triangle Breakout"
            pattern_desc = f"Higher lows + resistance {format_price(resistance_lvl)} break"

    # 3. Falling Wedge — lower highs AND lower lows (converging)
    if not pattern:
        lower_highs2 = all(recent_highs[i] < recent_highs[i-1] for i in range(1, min(6, len(recent_highs))))
        lower_lows2  = all(recent_lows[i] < recent_lows[i-1] for i in range(1, min(6, len(recent_lows))))
        # Converging: range shrinking
        early_range = recent_highs[0] - recent_lows[0] if recent_lows[0] > 0 else 1
        late_range  = recent_highs[-1] - recent_lows[-1] if recent_lows[-1] > 0 else 1
        converging  = late_range < early_range * 0.7
        if lower_highs2 and lower_lows2 and converging and l_close > recent_highs[-2] * 1.01:
            pattern = "Falling Wedge Breakout"
            pattern_desc = "Converging lower highs + lower lows break"

    # 4. Bull Flag — strong pump then tight consolidation → break
    if not pattern:
        # Look for pump in last 5-10 candles
        pump_candles = klines[-12:-5]
        if pump_candles:
            pump_start = float(pump_candles[0][3])
            pump_peak  = max(float(k[2]) for k in pump_candles)
            pump_pct   = (pump_peak - pump_start) / pump_start * 100 if pump_start > 0 else 0
            # Consolidation: last 4 candles tight range
            consol = klines[-5:-1]
            consol_range = (max(float(k[2]) for k in consol) - min(float(k[3]) for k in consol))
            consol_pct = consol_range / pump_peak * 100 if pump_peak > 0 else 100
            if pump_pct >= 15 and consol_pct < 8 and l_close > max(float(k[2]) for k in consol):
                pattern = "Bull Flag Breakout"
                pattern_desc = f"Pump +{pump_pct:.0f}% → consolidation → break"

    # 5. Key Level Breakout — price touched level 3+ times then broke
    if not pattern:
        # Find key resistance: price touched a level 3+ times in last 20 candles
        all_highs = [float(k[2]) for k in klines[-20:-2]]
        for resistance in all_highs:
            touches = sum(1 for h in all_highs if abs(h - resistance) / resistance < 0.02)
            if touches >= 3 and l_close > resistance * 1.015:
                pattern = "Key Level Breakout"
                pattern_desc = f"Level {format_price(resistance)} touched {touches}x, now breaking"
                break

    if not pattern:
        return

    # Wait 1 candle confirm (price should be above breakout for 2 consecutive candles)
    prev_close = float(prev_c[4])
    prev_open  = float(prev_c[1])
    # At least current candle is strongly bullish
    body_pct = (l_close - l_open) / l_open * 100
    if body_pct < 1.5:
        return

    breakout_alerted[key] = now

    msg = (
        f"📐 <b>{pattern}! [4H]</b>\n\n"
        f"🪙 <b>{symbol}</b>\n"
        f"💰 Price: {format_price(current_price)}\n"
        f"📊 24h: {change_24h:+.2f}%\n"
        f"📋 {pattern_desc}\n"
        f"⚡ Volume: {vol_ratio:.1f}x | Buy: {buy_ratio*100:.0f}%\n"
        f"🕐 {datetime.now().strftime('%H:%M:%S')}\n\n"
        f"⚠️ <i>Wait for a retest — could be a fake breakout!</i>"
    )
    send_all(msg, symbol=symbol)
    print(f"📐 Breakout: {symbol} — {pattern}")
    signal_performance[f"{symbol}_breakout_{int(now)}"] = {
        "symbol": symbol, "signal_price": current_price,
        "signal_time": now, "signal_type": f"{pattern} [4H]",
        "highest_after": current_price,
    }

# ─── FOLLOW-THROUGH SCORE ───────────────────────────
"""
A lot of Volume Spike / Explosive Pump messages come in, but most stop at a small
move or dump. A small subset (e.g. MITOUSDT +34.8%, HIGHUSDT +40.4%) gave a big pump.

This function checks, at the exact moment a signal fires (without knowing the future,
using only data available at that moment) a handful of structural strength markers that
correlate with bigger moves:
  • Daily trend bullish (higher timeframe alignment)
  • Higher lows already forming (not a one-off spike, but a building pattern)
  • Volume ratio much more extreme (15x+ instead of just 10x+)
  • Buy pressure strong and consistent (across several candles, not just one)
  • Comfortably above EMA with good margin (not just barely above)

If the score is 60+, that Volume Spike/Explosive Pump signal also goes to High
Priority alongside its normal topic — flagged as "a good follow-through candidate".
This isn't a guarantee, but it's a filter to separate high-potential signals from noise.
"""
def calc_followthrough_score(symbol, tf, klines, vol_ratio, buy_ratio, current_price):
    score = 0
    details = []

    # Daily trend check
    if not is_daily_downtrend(symbol, current_price):
        score += 20
        details.append("✅ Daily trend bullish/neutral")
    else:
        details.append("⚠️ Daily downtrend")

    # Higher lows already forming before this spike (not a one-off spike from nowhere)
    if check_hl_only(klines[:-1], lookback=6):
        score += 20
        details.append("✅ Higher lows already forming")

    # Extreme volume (well beyond minimum threshold)
    if vol_ratio >= 15:
        score += 20
        details.append(f"✅ Extreme volume ({vol_ratio:.1f}x)")
    elif vol_ratio >= 10:
        score += 10
        details.append(f"⚠️ High volume ({vol_ratio:.1f}x)")

    # Sustained buy pressure across last 2-3 candles, not just the trigger candle
    recent = klines[-4:-1]
    buy_ratios = []
    for k in recent:
        v = float(k[5])
        b = float(k[9])
        if v > 0:
            buy_ratios.append(b / v)
    if buy_ratios and sum(buy_ratios) / len(buy_ratios) >= 0.55:
        score += 20
        details.append(f"✅ Sustained buy pressure ({sum(buy_ratios)/len(buy_ratios)*100:.0f}% avg)")
    elif buy_ratio >= 0.65:
        score += 10
        details.append(f"⚠️ Strong single-candle buy pressure ({buy_ratio*100:.0f}%)")

    # EMA margin — price comfortably above EMA20, not just barely
    closes = [float(k[4]) for k in klines[:-1]]
    ema20 = calculate_ema(closes, 20)
    if ema20 and current_price > ema20 * 1.02:
        score += 20
        details.append("✅ Comfortably above 20EMA")
    elif ema20 and current_price > ema20:
        score += 10
        details.append("⚠️ Just above 20EMA")

    return score, details

# ─── VOLUME SPIKE ─────────────────────────────────────────
def check_timeframe(symbol, tf):
    cfg = TIMEFRAMES[tf]
    klines = get_klines(symbol, interval=tf, limit=50)
    if not klines or len(klines) < 10:
        return

    if tf != "5m":
        check_volume_buildup(symbol, tf, klines)
        # Item #16: trendline breakout/retest restricted to 4H and 1D only.
        # 1H trendline breakout was disabled because its timing-sensitive single-
        # candle check was a major source of missed signals (same root cause class
        # as the RARE/TNSR explosive-pump misses) — fast 1H moves often blew past
        # the breakout level before the scan loop caught up. Volume Spike/Buildup
        # and the manual zone OB BOUNCE/CONFIRMED path stay active on 1H, since
        # those are still proven sources of good signals (e.g. BICO, SYN).
        if tf in ["4h", "1d"]:
            check_trendline_breakout(symbol, tf, klines)
    if tf in ["1h", "4h", "1d"]:
        check_higher_lows(symbol, tf, klines)

    # 5M early detection
    if tf == "5m":
        check_5m_spike_early(symbol)
        check_15m_confirm(symbol)

    candle = klines[-2]
    current_vol = float(candle[5])
    open_price = float(candle[1])
    close_price = float(candle[4])
    spike_high = float(candle[2])

    prev_vols = [float(k[5]) for k in klines[-9:-2]]
    avg_vol = sum(prev_vols) / len(prev_vols) if prev_vols else 0
    if avg_vol == 0:
        return

    ratio = current_vol / avg_vol
    if ratio < cfg["multiplier"]:
        return
    if close_price <= open_price:
        return

    closes = [float(k[4]) for k in klines[:-1]]
    ema20 = calculate_ema(closes, 20)
    if ema20 and close_price < ema20:
        return

    if tf in ["1h", "4h"]:
        ticker_check = get_ticker(symbol)
        if ticker_check and float(ticker_check["priceChangePercent"]) < 2.0:
            return
        # Daily downtrend filter
        if is_daily_downtrend(symbol, close_price):
            return

    if tf == "5m":
        ticker_check = get_ticker(symbol)
        if ticker_check and float(ticker_check["priceChangePercent"]) < 0:
            return
        # Daily downtrend filter
        if is_daily_downtrend(symbol, close_price):
            return

    key = f"{symbol}_{tf}"
    now = time.time()
    if now - alerted_coins.get(key, 0) < cfg["cooldown"]:
        return

    alerted_coins[key] = now
    ticker = get_ticker(symbol)
    price = float(ticker["lastPrice"]) if ticker else close_price
    change_24h = float(ticker["priceChangePercent"]) if ticker else 0

    ft_score, ft_details = calc_followthrough_score(symbol, tf, klines, ratio, 0, price)
    high_potential = ft_score >= 60
    ft_tag = ""
    if high_potential:
        ft_details_str = "\n   ".join(ft_details)
        ft_tag = f"\n\n🔥 <b>HIGH FOLLOW-THROUGH POTENTIAL ({ft_score})</b>\n   {ft_details_str}"

    msg = (
        f"{cfg['emoji']} <b>VOLUME SPIKE! [{cfg['label']}]</b>\n\n"
        f"🪙 <b>{symbol}</b>\n"
        f"💰 Price: {format_price(price)}\n"
        f"📊 24h: {change_24h:+.2f}%\n"
        f"⚡ Spike: <b>{ratio:.1f}x</b> normal\n"
        f"🕐 {datetime.now().strftime('%H:%M:%S')}\n"
        f"{ft_tag}\n\n"
        f"⚠️ <i>Check the chart before entry!</i>"
    )
    sent = send_all(msg, symbol=symbol)
    if sent and high_potential:
        send_to_topic(TOPIC_HIGH, msg)  # escalate — separate from normal Spikes topic
    if sent:
        print(f"✅ [{cfg['label']}] Spike: {symbol} ({ratio:.1f}x) | FT score: {ft_score}")
        signal_performance[f"{symbol}_spike_{tf}_{int(now)}"] = {
            "symbol": symbol, "signal_price": price,
            "signal_time": now, "signal_type": f"Volume Spike [{cfg['label']}]",
            "highest_after": price,
        }

    track_key = f"{symbol}_{tf}"
    momentum_tracking[track_key] = {
        "symbol": symbol, "tf": tf, "start_time": now,
        "spike_close": close_price, "spike_high": spike_high,
        "lowest_since": close_price, "has_dipped": False,
        "type1_sent": False, "type2_sent": False,
    }

    if tf == "5m":
        accumulation_tracking[symbol] = {
            "start_time": now, "spike_price": close_price, "alert_sent": False
        }

# ─── MOMENTUM MONITOR ─────────────────────────────────────
def monitor_momentum():
    while True:
        now = time.time()
        to_remove = []

        for track_key, data in list(momentum_tracking.items()):
            symbol = data["symbol"]
            tf = data["tf"]
            cfg = TIMEFRAMES[tf]
            elapsed = now - data["start_time"]

            if elapsed > cfg["retest_window"]:
                to_remove.append(track_key)
                continue
            if data["type1_sent"] and data["type2_sent"]:
                to_remove.append(track_key)
                continue

            ticker = get_ticker(symbol)
            if not ticker:
                continue

            current_price = float(ticker["lastPrice"])
            change_24h = float(ticker["priceChangePercent"])
            spike_close = data["spike_close"]
            spike_high = data["spike_high"]

            klines = get_klines(symbol, interval=tf, limit=15)
            if not klines:
                continue

            current_vol = float(klines[-2][5])
            prev_vols = [float(k[5]) for k in klines[-9:-2]]
            avg_vol = sum(prev_vols) / len(prev_vols) if prev_vols else 1
            vol_ratio = current_vol / avg_vol

            if current_price < data["lowest_since"]:
                momentum_tracking[track_key]["lowest_since"] = current_price
                if current_price < spike_close * 0.99:
                    momentum_tracking[track_key]["has_dipped"] = True

            lowest = momentum_tracking[track_key]["lowest_since"]
            has_dipped = momentum_tracking[track_key]["has_dipped"]
            price_gain = (current_price - spike_close) / spike_close

            # Direct momentum (1H/4H only)
            if (not data["type1_sent"] and tf != "5m" and
                    elapsed <= cfg["direct_window"] and
                    not has_dipped and price_gain >= 0.02 and vol_ratio >= 2.0
                    and change_24h > 0):   # bullish: 24h positive
                sent = send_all(
                    f"🔥 <b>DIRECT MOMENTUM! [{cfg['label']}]</b>\n\n"
                    f"🪙 <b>{symbol}</b>\n"
                    f"💰 Price: {format_price(current_price)}\n"
                    f"📊 24h: {change_24h:+.2f}%\n"
                    f"📈 From spike: <b>+{price_gain*100:.1f}%</b>\n"
                    f"⚡ Volume: {vol_ratio:.1f}x\n"
                    f"🕐 {datetime.now().strftime('%H:%M:%S')}\n\n"
                    f"⚠️ <i>Confirm on the chart before entry.</i>",
                    symbol=symbol
                )
                if sent:
                    print(f"🔥 [{cfg['label']}] Direct: {symbol}")
                momentum_tracking[track_key]["type1_sent"] = True

            # Retest — 4H only, with strong confirmation
            if (not data["type2_sent"] and tf == "4h" and
                    has_dipped and
                    current_price >= spike_close * 1.005 and vol_ratio >= 2.0
                    and change_24h > 0):
                dip_pct = (spike_close - lowest) / spike_close * 100
                if dip_pct >= 5.0:  # 4H minimum -5% dip

                    # ── Strong confirmation checks ──
                    confirmed = True

                    # 1. HH/HL on 1H
                    klines_1h_rt = get_klines(symbol, interval="1h", limit=12)
                    if klines_1h_rt:
                        if not check_hh_hl(klines_1h_rt, lookback=8):
                            confirmed = False

                    # 2. Bullish engulfing or strong green candle on 1H
                    if klines_1h_rt and len(klines_1h_rt) >= 3:
                        last_1h = klines_1h_rt[-2]
                        h_open  = float(last_1h[1])
                        h_close = float(last_1h[4])
                        h_vol   = float(last_1h[5])
                        prev_1h_vols = [float(k[5]) for k in klines_1h_rt[-6:-2]]
                        avg_1h_vol = sum(prev_1h_vols)/len(prev_1h_vols) if prev_1h_vols else 1
                        if h_close <= h_open:  # must be bullish
                            confirmed = False
                        if h_vol < avg_1h_vol * 1.5:  # 1H volume spike
                            confirmed = False

                    # 3. 3 consecutive green candles on 15M
                    klines_15m_rt = get_klines(symbol, interval="15m", limit=10)
                    if klines_15m_rt and len(klines_15m_rt) >= 5:
                        last_3 = klines_15m_rt[-4:-1]
                        green_count = sum(1 for c in last_3 if float(c[4]) > float(c[1]))
                        if green_count < 2:
                            confirmed = False

                    # 4. Higher Lows on 15M
                    if klines_15m_rt:
                        if not check_hl_only(klines_15m_rt, lookback=6):
                            confirmed = False

                    # 5. 15M buy pressure 60%+
                    if klines_15m_rt and len(klines_15m_rt) >= 4:
                        recent_15m = klines_15m_rt[-4:-1]
                        t_buy = sum(float(c[9]) for c in recent_15m)
                        t_vol = sum(float(c[5]) for c in recent_15m)
                        bp_15m = t_buy / t_vol * 100 if t_vol > 0 else 0
                        if bp_15m < 60:
                            confirmed = False

                    # 6. OB zone holding — 4H candle closed above zone low
                    if float(klines[-2][4]) < spike_close * 0.98:
                        confirmed = False

                    if confirmed:
                        # Buy pressure % for display
                        bp_display = bp_15m if klines_15m_rt else 0
                        sent = send_all(
                            f"💎 <b>RETEST CONFIRMED! [4H]</b>\n\n"
                            f"🪙 <b>{symbol}</b>\n"
                            f"💰 Price: {format_price(current_price)}\n"
                            f"📊 24h: {change_24h:+.2f}%\n"
                            f"📉 Retest: -{dip_pct:.1f}% → <b>+{price_gain*100:.1f}%</b>\n"
                            f"⚡ Volume: {vol_ratio:.1f}x\n\n"
                            f"✅ 1H HH/HL\n"
                            f"✅ 1H bullish + volume\n"
                            f"✅ 15M green candles\n"
                            f"✅ 15M HL forming\n"
                            f"✅ Buy pressure: {bp_display:.0f}%\n"
                            f"🕐 {datetime.now().strftime('%H:%M:%S')}\n\n"
                            f"⚠️ <i>Strong retest! Take the entry.</i>",
                            symbol=symbol
                        )
                        if sent:
                            print(f"💎 [4H] Strong Retest: {symbol}")
                            signal_performance[f"{symbol}_retest_4h_{int(now)}"] = {
                                "symbol": symbol, "signal_price": current_price,
                                "signal_time": now, "signal_type": "Retest Confirmed [4H]",
                                "highest_after": current_price,
                            }
                momentum_tracking[track_key]["type2_sent"] = True

        for key in to_remove:
            momentum_tracking.pop(key, None)

        # Accumulation (72hr)
        acc_remove = []
        for symbol, data in list(accumulation_tracking.items()):
            elapsed = now - data["start_time"]
            if elapsed > 72 * 3600 or data["alert_sent"]:
                acc_remove.append(symbol)
                continue
            klines_1h = get_klines(symbol, interval="1h", limit=30)
            if not klines_1h or len(klines_1h) < 10:
                continue
            avg_vol = sum(float(k[5]) for k in klines_1h[-12:-3]) / 9
            recent_vols = [float(k[5]) for k in klines_1h[-4:-1]]
            recent_candles_1h = klines_1h[-4:-1]
            buildup = (all(v >= avg_vol * 1.3 for v in recent_vols) and
                       all(float(c[4]) > float(c[1]) for c in recent_candles_1h))
            lows = [float(k[3]) for k in klines_1h[-6:-1]]
            higher_lows = lows[-1] > lows[-3] > lows[-5] if len(lows) >= 5 else False
            if buildup and higher_lows:
                ticker = get_ticker(symbol)
                price = float(ticker["lastPrice"]) if ticker else 0
                change_24h = float(ticker["priceChangePercent"]) if ticker else 0
                hrs_since = elapsed / 3600
                sent = send_all(
                    f"🔬 <b>ACCUMULATION SIGNAL!</b>\n\n"
                    f"🪙 <b>{symbol}</b>\n"
                    f"💰 Price: {format_price(price)}\n"
                    f"📊 24h: {change_24h:+.2f}%\n"
                    f"📈 Higher lows + Volume build-up\n"
                    f"⏱ {hrs_since:.1f}hr after first spike\n"
                    f"🕐 {datetime.now().strftime('%H:%M:%S')}\n\n"
                    f"⚠️ <i>Big pump may be coming! Check the chart.</i>",
                    symbol=symbol
                )
                if sent:
                    accumulation_tracking[symbol]["alert_sent"] = True
                    signal_performance[f"{symbol}_accum_72hr_{int(now)}"] = {
                        "symbol": symbol, "signal_price": price,
                        "signal_time": now, "signal_type": "Accumulation [72hr]",
                        "highest_after": price,
                    }
        for symbol in acc_remove:
            accumulation_tracking.pop(symbol, None)

        # Trendline retest
        tl_remove = []
        for retest_key, data in list(trendline_retest_tracking.items()):
            symbol = data["symbol"]
            tf_r = data["tf"]
            elapsed = now - data["start_time"]
            if elapsed > 7 * 24 * 3600 or data["alert_sent"]:
                tl_remove.append(retest_key)
                continue
            ticker = get_ticker(symbol)
            if not ticker:
                continue
            current_price = float(ticker["lastPrice"])
            change_24h = float(ticker["priceChangePercent"])
            breakout_price = data["breakout_price"]
            trendline_val = data["trendline_value"]
            if current_price > data["highest_since"]:
                trendline_retest_tracking[retest_key]["highest_since"] = current_price
            highest = trendline_retest_tracking[retest_key]["highest_since"]
            near_trendline = current_price <= breakout_price * 1.03
            if near_trendline and highest > breakout_price * 1.02:
                trendline_retest_tracking[retest_key]["has_retested"] = True
            has_retested = trendline_retest_tracking[retest_key]["has_retested"]
            if (has_retested and current_price > breakout_price * 1.015 and
                    current_price > trendline_val * 1.01):
                klines_tf = get_klines(symbol, interval=tf_r, limit=10)
                vol_ratio = 1.0
                if klines_tf:
                    cv = float(klines_tf[-2][5])
                    pv = [float(k[5]) for k in klines_tf[-9:-2]]
                    vol_ratio = cv / (sum(pv)/len(pv)) if pv else 1
                gain_pct = (current_price - breakout_price) / breakout_price * 100
                sent = send_all(
                    f"🏆 <b>TRENDLINE RETEST CONFIRMED! [{tf_r.upper()}]</b>\n\n"
                    f"🪙 <b>{symbol}</b>\n"
                    f"💰 Price: {format_price(current_price)}\n"
                    f"📊 24h: {change_24h:+.2f}%\n"
                    f"📐 Break → retest → continuation\n"
                    f"📈 From breakout: <b>+{gain_pct:.1f}%</b>\n"
                    f"⚡ Volume: {vol_ratio:.1f}x\n"
                    f"🕐 {datetime.now().strftime('%H:%M:%S')}\n\n"
                    f"⚠️ <i>Strong setup! Check OB/FVG before entry.</i>",
                    symbol=symbol
                )
                if sent:
                    print(f"🏆 [{tf_r.upper()}] Trendline Retest: {symbol}")
                    signal_performance[f"{symbol}_tl_retest"] = {
                        "symbol": symbol, "signal_price": current_price,
                        "signal_time": now, "signal_type": f"Trendline Retest [{tf_r.upper()}]",
                        "highest_after": current_price,
                    }
                trendline_retest_tracking[retest_key]["alert_sent"] = True
        for key in tl_remove:
            trendline_retest_tracking.pop(key, None)

        # Signal performance — minimum tracking window before finalizing result (v71 fix).
        # OLD BUG: result was finalized the moment price dipped 8% from whatever peak had
        # been seen so far — so a signal that peaked +20% after just 1hr and then had a
        # normal pullback got reported as "final" immediately, even though the real move
        # (e.g. continuing to +50%+ over the next 2-3 days) hadn't happened yet. Now the
        # bot keeps tracking the actual highest price for at least RESULT_MIN_HOURS before
        # it's allowed to lock in any result — short-term noise no longer ends tracking early.
        RESULT_MIN_HOURS = 60  # ~2.5 days minimum before a result can be finalized
        perf_remove = []
        symbol_best = {}  # {symbol: best_data_key} — highest peak per symbol

        for perf_key, data in list(signal_performance.items()):
            symbol = data["symbol"]
            elapsed = now - data["signal_time"]

            if elapsed > 15 * 24 * 3600:
                perf_remove.append(perf_key)
                continue

            if data.get("result_sent"):
                continue

            ticker = get_ticker(symbol)
            if not ticker:
                continue
            current_price = float(ticker["lastPrice"])

            # Track highest using 1H HIGH
            klines_1h = get_klines(symbol, interval="1h", limit=3)
            if klines_1h and len(klines_1h) >= 2:
                last_high = float(klines_1h[-2][2])
                if last_high > data.get("highest_close", data["signal_price"]):
                    signal_performance[perf_key]["highest_close"] = last_high
                    signal_performance[perf_key]["peak_time"] = now

            highest = data.get("highest_close", data["signal_price"])
            peak_pct = (highest - data["signal_price"]) / data["signal_price"] * 100

            # Track best signal per symbol
            if symbol not in symbol_best:
                symbol_best[symbol] = {"key": perf_key, "peak_pct": peak_pct, "highest": highest, "data": data}
            elif peak_pct > symbol_best[symbol]["peak_pct"]:
                symbol_best[symbol] = {"key": perf_key, "peak_pct": peak_pct, "highest": highest, "data": data}

        # Check dump and send result — one per symbol, 10%+ only
        for symbol, best in symbol_best.items():
            perf_key = best["key"]
            data = best["data"]
            highest = best["highest"]
            peak_pct = best["peak_pct"]

            if peak_pct < 10.0:
                continue

            ticker = get_ticker(symbol)
            if not ticker:
                continue
            current_price = float(ticker["lastPrice"])
            dumped = current_price < highest * 0.92  # 8%+ dump from peak
            window_passed = (now - data["signal_time"]) >= RESULT_MIN_HOURS * 3600

            if dumped and window_passed and not data.get("result_sent"):
                peak_time = data.get("peak_time", data["signal_time"])
                peak_hrs = (peak_time - data["signal_time"]) / 3600
                emoji = "🚀" if peak_pct >= 20 else "🟠"

                # Send to subscribers
                send_all(
                    f"{emoji} <b>SIGNAL RESULT</b>\n\n"
                    f"🪙 <b>{symbol}</b>\n"
                    f"📊 {data['signal_type']}\n"
                    f"💰 {format_price(data['signal_price'])} → {format_price(highest)}\n"
                    f"📈 <b>+{peak_pct:.1f}%</b> | ⏱ {peak_hrs:.1f}hr",
                    symbol=None
                )
                # Mark all signals for this symbol as sent
                for pk, pd in signal_performance.items():
                    if pd["symbol"] == symbol:
                        signal_performance[pk]["result_sent"] = True

            # Dump tracker — admin only, fires when a signal dumps
            if current_price < data["signal_price"] * 0.95 and not data.get("dump_notified"):
                dump_pct = (data["signal_price"] - current_price) / data["signal_price"] * 100
                signal_performance[perf_key]["dump_notified"] = True
                send_to(ADMIN_CHAT_ID,
                    f"📉 <b>SIGNAL DUMPED</b>\n\n"
                    f"🪙 <b>{symbol}</b>\n"
                    f"📊 {data['signal_type']}\n"
                    f"💰 {format_price(data['signal_price'])} → {format_price(current_price)}\n"
                    f"📉 -{dump_pct:.1f}% dump"
                )
        for key in perf_remove:
            signal_performance.pop(key, None)

        # Periodic persistence — signal_performance, prepump_phases,
        # trendline_retest_tracking, and the cooldown trackers were previously pure
        # in-memory and lost on every restart. Saving once per pass here (not on
        # every single mutation) keeps disk I/O reasonable while still persisting
        # within ~60s of any change.
        save_signal_performance()
        save_prepump_phases()
        save_trendline_tracking()
        save_cooldown_trackers()

        time.sleep(60)

# ─── COMMAND HANDLER ──────────────────────────────────────
def handle_commands():
    global last_update_id, watchlist, subscribers
    while True:
        try:
            updates = get_updates()
            for update in updates:
                last_update_id = update["update_id"]
                msg = update.get("message", {})
                raw_text = msg.get("text", "").strip()
                text = raw_text.upper()
                chat_id = str(msg.get("chat", {}).get("id", ""))
                first_name = msg.get("chat", {}).get("first_name", "Friend")
                is_admin = (chat_id == ADMIN_CHAT_ID)

                if raw_text.startswith("WATCHLIST_SAVE:"):
                    continue
                if not text:
                    continue

                if text == "/START":
                    if chat_id not in subscribers:
                        subscribers.append(chat_id)
                        subscribers_info[chat_id] = {
                            "name": first_name,
                            "joined": datetime.now().strftime('%Y-%m-%d %H:%M')
                        }
                        welcome_msg = (
                            f"👋 <b>Welcome, {first_name}!</b>\n\n"
                            f"You've joined <b>CryptoPing</b>.\n\n"
                            f"This bot monitors coins on Binance. When unusual volume hits a coin, it sends a notification right away.\n\n"
                            f"📌 <b>What the alerts mean:</b>\n"
                            f"⚡ Early Signal — 5M spike + 15M confirm (right at the start of a move)\n"
                            f"⚡ Volume Spike — a sudden large volume\n"
                            f"📈 Build-up — volume gradually increasing\n"
                            f"💎 Retest — dipped, then back up\n"
                            f"🏆 Trendline Retest — breakout confirmation\n"
                            f"🎯 OB Bounce — MTF-confirmed bounce from a key zone\n"
                            f"💚 Buy Pressure — large buyers stepping in\n"
                            f"🎉 Pump Result — lets everyone know when a signal pumps 20%+\n\n"
                            f"⚠️ <b>Keep in mind:</b>\n"
                            f"This is a volume alert, not a trading signal.\n"
                            f"When a notification comes in, analyze the chart yourself,\n"
                            f"and only take the entry once you've confirmed it.\n\n"
                            f"Good luck! 🚀\n— CryptoPing"
                        )
                        send_to(chat_id, welcome_msg)
                        send_to(ADMIN_CHAT_ID, f"👤 New subscriber: <b>{first_name}</b> (ID: {chat_id})")
                        save_subscribers()
                    else:
                        send_to(chat_id, "✅ You're already subscribed!")

                elif text == "/STOP":
                    if chat_id in subscribers and chat_id != ADMIN_CHAT_ID:
                        subscribers.remove(chat_id)
                        save_subscribers()
                        send_to(chat_id, "❌ Unsubscribed.")

                elif text == "/LIST":
                    coin_lines = [f"• {c}" for c in watchlist]
                    send_chunked(chat_id, coin_lines, header=f"📋 <b>Watchlist ({len(watchlist)} coins):</b>\n\n")

                elif text.startswith("/ADD ") and is_admin:
                    symbol = text.replace("/ADD ", "").strip()
                    if not symbol.endswith("USDT"):
                        symbol += "USDT"
                    if symbol in watchlist:
                        send_to(chat_id, f"⚠️ {symbol} is already on the list!")
                    else:
                        r = http_session.get(f"https://api.binance.com/api/v3/ticker/price?symbol={symbol}", timeout=5)
                        if r.status_code == 200:
                            watchlist.append(symbol)
                            save_watchlist()
                            send_to(chat_id, f"✅ {symbol} added! Total: {len(watchlist)}")
                        else:
                            send_to(chat_id, f"❌ {symbol} not found on Binance.")

                elif text.startswith("/REMOVE ") and is_admin:
                    symbol = text.replace("/REMOVE ", "").strip()
                    if not symbol.endswith("USDT"):
                        symbol += "USDT"
                    if symbol in watchlist:
                        watchlist.remove(symbol)
                        save_watchlist()
                        send_to(chat_id, f"🗑 {symbol} removed. Total: {len(watchlist)}")
                    else:
                        send_to(chat_id, f"⚠️ {symbol} isn't on the watchlist.")

                elif text == "/STATUS" and is_admin:
                    send_to(chat_id,
                        f"✅ <b>CryptoPing is running!</b>\n\n"
                        f"📋 Coins: {len(watchlist)}\n"
                        f"👥 Subscribers: {len(subscribers)}\n"
                        f"🔍 Momentum: {len(momentum_tracking)}\n"
                        f"⚡ Pending 5M→15M: {len(spike_pending_confirm)}\n"
                        f"🎯 OB/FVG: {len(ob_fvg_zone_tracking)}\n"
                        f"📐 Manual Zones: {len(manual_zones)}\n"
                        f"🏆 TL Retest: {len(trendline_retest_tracking)}\n"
                        f"🕐 {datetime.now().strftime('%H:%M:%S')}"
                    )

                elif (text == "/REPORT" or raw_text.upper().startswith("/REPORT ")) and is_admin:
                    # /report           -> defaults to last 24h
                    # /report 24h       -> last 24 hours
                    # /report 7d        -> last 7 days
                    # /report 12h, 48h, 3d, etc. — any number + h or d
                    arg = raw_text.strip().split(None, 1)
                    window_str = arg[1].strip().lower() if len(arg) > 1 else "24h"
                    import re as _re
                    m = _re.match(r"^(\d+)([hd])$", window_str)
                    if not m:
                        send_to(chat_id, "⚠️ Format: /report 24h  or  /report 7d")
                    else:
                        amount, unit = int(m.group(1)), m.group(2)
                        window_seconds = amount * 3600 if unit == "h" else amount * 86400
                        window_label = f"Last {amount}{'hr' if unit == 'h' else ' day(s)'}"
                        send_to(chat_id, build_report(window_seconds, window_label))

                elif raw_text.upper().startswith("/BROADCAST ") and is_admin:
                    broadcast_text = raw_text[11:].strip()
                    if broadcast_text:
                        for chat_id_sub in subscribers:
                            send_to(chat_id_sub, f"📢 <b>Message from CryptoPing:</b>\n\n{broadcast_text}")
                        send_to(ADMIN_CHAT_ID, f"✅ Sent to {len(subscribers)} people!")
                    else:
                        send_to(chat_id, "⚠️ Example: /broadcast stay alert today")

                elif text == "/SUBSCRIBERS" and is_admin:
                    if not subscribers:
                        send_to(chat_id, "👥 No subscribers yet.")
                    else:
                        lines = []
                        for i, sid in enumerate(subscribers, 1):
                            info = subscribers_info.get(sid, {})
                            name = info.get("name", "Unknown")
                            joined = info.get("joined", "—")
                            lines.append(f"{i}. {name} | ID: {sid} | {joined}")
                        send_chunked(chat_id, lines, header=f"\U0001f465 <b>Subscribers ({len(subscribers)}):</b>\n\n")

                elif raw_text.upper().startswith("/MSG ") and is_admin:
                    parts = raw_text[5:].strip().split(" ", 1)
                    if len(parts) == 2:
                        target_id, personal_msg = parts
                        if target_id in subscribers:
                            msg_text = "\U0001f4e9 Message from CryptoPing:\n\n" + personal_msg
                            send_to(target_id, msg_text)
                            name = subscribers_info.get(target_id, {}).get("name", target_id)
                            send_to(ADMIN_CHAT_ID, f"✅ Message sent to <b>{name}</b>")
                        else:
                            send_to(chat_id, f"⚠️ ID {target_id} isn't on the subscriber list.")
                    else:
                        send_to(chat_id, "Format: /msg [ID] [message]")

                elif text == "/HELP" and is_admin:
                    send_to(chat_id,
                        "🤖 <b>Commands:</b>\n\n"
                        "/add STRAX — add a coin\n"
                        "/remove STRAX — remove a coin\n"
                        "/list — view the watchlist\n"
                        "/status — bot status\n"
                        "/report [24h|7d|etc] — category breakdown of signals + win rate\n"
                        "/subscribers — subscriber list\n"
                        "/msg [ID] [msg] — personal message\n"
                        "/broadcast [msg] — message everyone\n\n"
                        "<b>📐 Manual Zone Commands:</b>\n"
                        "/addzone BTC 95000 98000 4H — add a zone\n"
                        "/removezone BTC_4H_1 — remove a zone\n"
                        "/resetzone BTC_4H_1 — reactivate an invalidated zone\n"
                        "/zones — view all active zones\n\n"
                        "<b>📊 Market Scan:</b>\n"
                        "/scanmarket — view USDT coins with 500K+ volume\n"
                        "/scanmarket 1000000 — custom volume threshold\n"
                        "/addall — add all coins from the last scan at once\n"
                    )

                elif text == "/EXPORTZONES" and is_admin:
                    import json as _j
                    if not manual_zones:
                        send_to(chat_id, "📐 No active zones.")
                    else:
                        export = {}
                        for zid, z in manual_zones.items():
                            export[zid] = {
                                "symbol": z["symbol"], "tf": z["tf"],
                                "low": z["low"], "high": z["high"],
                                "added_time": z["added_time"],
                            }
                        text_out = f"ZONES_EXPORT:{_j.dumps(export)}"
                        # Split into chunks if too long
                        chunk_size = 3000
                        if len(text_out) <= chunk_size:
                            send_to(chat_id, f"<code>{text_out}</code>")
                        else:
                            parts = [text_out[i:i+chunk_size] for i in range(0, len(text_out), chunk_size)]
                            for i, part in enumerate(parts):
                                send_to(chat_id, f"Part {i+1}/{len(parts)}:\n<code>{part}</code>")
                        send_to(chat_id, f"✅ {len(export)} zones. Run /sync on the signal bot.")

                elif text == "/EXPORTWATCHLIST" and is_admin:
                    import json as _j
                    SIGNAL_TOKEN = "8973668144:AAFwvLoZhV1WDC5i0OIs8IpCylbkcx279Z8"
                    text_out = f"WATCHLIST_EXPORT:{_j.dumps(watchlist)}"
                    try:
                        http_session.post(
                            f"https://api.telegram.org/bot{SIGNAL_TOKEN}/sendMessage",
                            json={"chat_id": ADMIN_CHAT_ID, "text": text_out},
                            timeout=10
                        )
                    except:
                        pass
                    send_to(chat_id, f"📤 Watchlist exported ({len(watchlist)} coins)!")


                    # Parse optional volume threshold
                    parts = text.split()
                    min_vol = 500_000
                    if len(parts) == 2:
                        try:
                            min_vol = float(parts[1])
                        except:
                            pass

                    send_to(chat_id, f"🔍 Scanning Binance (min ${min_vol:,.0f} volume)...")

                    try:
                        r = http_session.get(
                            "https://api.binance.com/api/v3/ticker/24hr",
                            timeout=15
                        )
                        if r.status_code != 200:
                            send_to(chat_id, "❌ Binance API error")
                        else:
                            all_tickers = r.json()

                            # Stables/wrapped to skip
                            skip_keywords = ['USDC','BUSD','TUSD','USDP','USDS','RLUS','FDUSD','WBTC','WBETH','WETH']

                            gainers = []
                            losers  = []

                            for t in all_tickers:
                                sym = t.get("symbol","")
                                if not sym.endswith("USDT"):
                                    continue
                                # Skip stables/wrapped
                                base = sym.replace("USDT","")
                                if any(k in base for k in skip_keywords):
                                    continue
                                try:
                                    vol_usd  = float(t.get("quoteVolume", 0))
                                    chg      = float(t.get("priceChangePercent", 0))
                                    price    = float(t.get("lastPrice", 0))
                                except:
                                    continue

                                if vol_usd < min_vol:
                                    continue
                                if sym in watchlist:
                                    continue

                                if chg > 0:
                                    gainers.append((sym, chg, vol_usd, price))
                                else:
                                    losers.append((sym, chg, vol_usd, price))

                            gainers.sort(key=lambda x: x[1], reverse=True)
                            losers.sort(key=lambda x: x[1])

                            # Top 20 gainers
                            if gainers:
                                lines = [f"🚀 <b>Top Gainers (not in watchlist)</b> | min ${min_vol/1e6:.1f}M vol\n"]
                                for sym, chg, vol, price in gainers[:20]:
                                    vol_str = f"${vol/1e6:.1f}M" if vol >= 1e6 else f"${vol/1e3:.0f}K"
                                    lines.append(f"• <b>{sym}</b> +{chg:.1f}% | {vol_str}")
                                send_to(chat_id, "\n".join(lines))
                            else:
                                send_to(chat_id, "🚀 No gainers found.")

                            # Top 20 losers
                            if losers:
                                lines = [f"\n📉 <b>Top Losers (not in watchlist)</b>\n"]
                                for sym, chg, vol, price in losers[:20]:
                                    vol_str = f"${vol/1e6:.1f}M" if vol >= 1e6 else f"${vol/1e3:.0f}K"
                                    lines.append(f"• <b>{sym}</b> {chg:.1f}% | {vol_str}")
                                send_to(chat_id, "\n".join(lines))

                            # Add format
                            all_new = gainers[:20] + losers[:20]
                            if all_new:
                                # Cache for /addall
                                last_scan_results.clear()
                                last_scan_results.extend([s for s,_,_,_ in all_new])

                                add_lines = ["📋 <b>Add format:</b> (or use /addall to add everything at once)\n"]
                                for sym, _, _, _ in all_new:
                                    add_lines.append(f"/add {sym.replace('USDT','')}")
                                chunk = []
                                for line in add_lines:
                                    chunk.append(line)
                                    if len("\n".join(chunk)) > 3500:
                                        send_to(chat_id, "\n".join(chunk))
                                        chunk = []
                                if chunk:
                                    send_to(chat_id, "\n".join(chunk))

                    except Exception as e:
                        send_to(chat_id, f"❌ Scan error: {e}")

                elif text == "/ADDALL" and is_admin:
                    if not last_scan_results:
                        send_to(chat_id, "⚠️ Run /scanmarket first, then /addall")
                    else:
                        added = []
                        skipped = []
                        failed = []
                        for sym in last_scan_results:
                            if sym in watchlist:
                                skipped.append(sym)
                                continue
                            try:
                                r = http_session.get(
                                    f"https://api.binance.com/api/v3/ticker/price?symbol={sym}",
                                    timeout=5
                                )
                                if r.status_code == 200:
                                    watchlist.append(sym)
                                    added.append(sym)
                                else:
                                    failed.append(sym)
                            except:
                                failed.append(sym)

                        save_watchlist()
                        msg = f"✅ <b>Bulk add done!</b>\n\n"
                        msg += f"✅ Added: {len(added)}\n"
                        if added:
                            msg += "\n".join(f"  • {s}" for s in added) + "\n"
                        if skipped:
                            msg += f"\n⏭ Already in list: {len(skipped)}\n"
                        if failed:
                            msg += f"\n❌ Not found: {len(failed)}\n"
                            msg += "\n".join(f"  • {s}" for s in failed)
                        msg += f"\n\n📋 Total watchlist: {len(watchlist)}"
                        send_to(chat_id, msg)

                elif raw_text.upper().startswith("/ADDZONE "):
                    if not is_admin:
                        send_to(chat_id, "⚠️ Admin only command.")
                    else:
                        parts = raw_text.strip().split()
                        if len(parts) == 5:
                            _, sym, low_s, high_s, ztf = parts
                            sym = sym.upper()
                            if not sym.endswith("USDT"):
                                sym += "USDT"
                            ztf = ztf.lower()
                            if ztf not in ["5m","15m","1h","4h","1d"]:
                                send_to(chat_id, "⚠️ TF must be: 5m / 15m / 1h / 4h / 1d")
                            else:
                                try:
                                    z_low  = float(low_s)
                                    z_high = float(high_s)
                                    if z_low >= z_high:
                                        send_to(chat_id, "⚠️ Low must be less than High")
                                    else:
                                        zone_count = sum(1 for k in manual_zones if k.startswith(f"{sym}_{ztf}"))
                                        zone_id = f"{sym}_{ztf}_{zone_count+1}"
                                        manual_zones[zone_id] = {
                                            "symbol": sym, "tf": ztf,
                                            "low": z_low, "high": z_high,
                                            "added_time": time.time(),
                                            "state": "waiting",
                                            "alert_sent_time": 0,
                                        }

                                        extra_lines = []
                                        if ztf == "4h":
                                            is_confluent, conf_note = check_daily_confluence(sym, z_low, z_high)
                                            if is_confluent:
                                                extra_lines.append(f"🎯 <b>HIGH CONFLUENCE</b> — {conf_note}")
                                        bounce_info = get_zone_bounce_info(sym, z_low, z_high)
                                        if bounce_info and bounce_info.get("bounce_count", 0) > 0:
                                            extra_lines.append(f"📍 Previous bounce zone (x{bounce_info['bounce_count']}) — higher probability")
                                        extra_str = ("\n".join(extra_lines) + "\n\n") if extra_lines else ""

                                        send_to(chat_id,
                                            f"✅ <b>Zone added!</b>\n\n"
                                            f"🪙 {sym} | {ztf.upper()} OB\n"
                                            f"🔲 {format_price(z_low)} — {format_price(z_high)}\n"
                                            f"🆔 ID: <code>{zone_id}</code>\n\n"
                                            f"{extra_str}"
                                            f"Bot is monitoring. You'll be notified when price reaches the zone."
                                        )
                                        save_zones()
                                        print(f"📐 Zone added: {zone_id}")
                                except ValueError:
                                    send_to(chat_id, "⚠️ Format: /addzone RIF 0.0665 0.0703 4H")
                        else:
                            send_to(chat_id, f"⚠️ Format: /addzone RIF 0.0665 0.0703 4H\nParts received: {len(parts)}")

                elif raw_text.upper().startswith("/REMOVEZONE "):
                    if not is_admin:
                        send_to(chat_id, "⚠️ Admin only.")
                    else:
                        zone_id = raw_text.strip().split(None, 1)[1].strip()
                        if zone_id in manual_zones:
                            manual_zones.pop(zone_id)
                            save_zones()
                            send_to(chat_id, f"🗑 Zone removed: <code>{zone_id}</code>")
                        else:
                            send_to(chat_id, f"⚠️ Zone not found: {zone_id}\nUse /zones to see the list")

                elif raw_text.upper().startswith("/RESETZONE "):
                    if not is_admin:
                        send_to(chat_id, "⚠️ Admin only.")
                    else:
                        zone_id = raw_text.strip().split(None, 1)[1].strip()
                        if zone_id in manual_zones:
                            manual_zones[zone_id]["state"] = "waiting"
                            manual_zones[zone_id]["invalidated"] = False
                            manual_zones[zone_id]["layer1_sent"] = False
                            manual_zones[zone_id]["layer2_sent"] = False
                            manual_zones[zone_id]["entered_notified_time"] = 0
                            save_zones()
                            send_to(chat_id, f"♻️ Zone reset: <code>{zone_id}</code>\nMonitoring has restarted.")
                        else:
                            send_to(chat_id, f"⚠️ Zone not found: {zone_id}")

                elif text == "/ZONES":
                    if not is_admin:
                        send_to(chat_id, "⚠️ Admin only.")
                    elif not manual_zones:
                        send_to(chat_id, "📐 No active zones.\n/addzone RIF 0.0665 0.0703 4H")
                    else:
                        lines = []
                        for zid, z in manual_zones.items():
                            age_hr = (time.time() - z["added_time"]) / 3600
                            lines.append(
                                f"• <code>{zid}</code> {z['symbol']} | {z['tf'].upper()} | "
                                f"{format_price(z['low'])}—{format_price(z['high'])} | "
                                f"{z.get('state','waiting')} | {age_hr:.0f}hr"
                            )
                        send_chunked(chat_id, lines, header=f"📐 <b>Active Zones ({len(manual_zones)}):</b>\n\n")

                # ─── ACTIVE TRADE MONITOR COMMANDS (v68) ──────────
                elif raw_text.upper().startswith("/TRADE "):
                    if not is_admin:
                        send_to(chat_id, "⚠️ Admin only.")
                    else:
                        # /trade GPS entry=0.00762 sl=0.00700 tp1=0.00850 tp2=0.00925 tp3=0.01000 [tf=1h]
                        parts = raw_text.strip().split()
                        if len(parts) < 4:
                            send_to(chat_id,
                                "⚠️ Format:\n<code>/trade GPS entry=0.00762 sl=0.00700 tp1=0.00850 tp2=0.00925 tp3=0.01000</code>\n\n"
                                "tf=1h is the default; use tf=4h if you want."
                            )
                        else:
                            try:
                                sym_raw = parts[1].upper()
                                sym = sym_raw if sym_raw.endswith("USDT") else sym_raw + "USDT"
                                kv = {}
                                for p in parts[2:]:
                                    if "=" in p:
                                        k, v = p.split("=", 1)
                                        kv[k.lower()] = v

                                entry = float(kv["entry"])
                                sl    = float(kv["sl"])
                                tps   = []
                                for tpk in ["tp1", "tp2", "tp3"]:
                                    if tpk in kv:
                                        tps.append(float(kv[tpk]))
                                tf = kv.get("tf", TRADE_CHECK_TF_DEFAULT)

                                if sl >= entry:
                                    send_to(chat_id, "⚠️ SL must be below entry (assuming a long trade)")
                                elif not tps:
                                    send_to(chat_id, "⚠️ At least one tp1 is required")
                                else:
                                    trade_id = f"{sym}_{int(time.time())}"
                                    active_trades[trade_id] = {
                                        "symbol": sym, "entry": entry, "sl": sl,
                                        "tps": tps, "tf": tf,
                                        "opened_time": time.time(),
                                        "hit_tps": [], "trail_stage": 0, "last_score": 0,
                                    }
                                    save_active_trades()
                                    tp_str = " | ".join(f"TP{i+1}: {format_price(t)}" for i, t in enumerate(tps))
                                    send_to(chat_id,
                                        f"✅ <b>Trade added to monitor!</b>\n\n"
                                        f"🪙 {sym} | {tf.upper()}\n"
                                        f"💰 Entry: {format_price(entry)} | SL: {format_price(sl)}\n"
                                        f"🎯 {tp_str}\n"
                                        f"🆔 <code>{trade_id}</code>\n\n"
                                        f"Bot will now monitor trend health (EMA + candle/volume + structure). "
                                        f"Alerts will go to the Trade Monitor topic."
                                    )
                                    print(f"💼 Trade added: {trade_id}")
                            except (KeyError, ValueError) as e:
                                send_to(chat_id, f"⚠️ Format is wrong. Example:\n<code>/trade GPS entry=0.00762 sl=0.00700 tp1=0.00850</code>")

                elif text == "/TRADES":
                    if not is_admin:
                        send_to(chat_id, "⚠️ Admin only.")
                    elif not active_trades:
                        send_to(chat_id, "💼 No active trades.\n/trade SYMBOL entry=.. sl=.. tp1=..")
                    else:
                        lines = [f"💼 <b>Active Trades ({len(active_trades)}):</b>\n"]
                        for tid, t in active_trades.items():
                            age_hr = (time.time() - t["opened_time"]) / 3600
                            tp_done = len(t.get("hit_tps", []))
                            lines.append(
                                f"• <code>{tid}</code>\n"
                                f"  {t['symbol']} | Entry: {format_price(t['entry'])} | SL: {format_price(t['sl'])}\n"
                                f"  TP hit: {tp_done}/{len(t.get('tps', []))} | Score: {t.get('last_score', 0)} | {age_hr:.0f}hr"
                            )
                        send_to(chat_id, "\n".join(lines))

                elif raw_text.upper().startswith("/CLOSETRADE "):
                    if not is_admin:
                        send_to(chat_id, "⚠️ Admin only.")
                    else:
                        trade_id = raw_text.strip().split(None, 1)[1].strip()
                        if trade_id in active_trades:
                            active_trades.pop(trade_id)
                            trade_alert_cooldown.pop(trade_id, None)
                            save_active_trades()
                            send_to(chat_id, f"🗑 Trade closed/removed: <code>{trade_id}</code>")
                        else:
                            send_to(chat_id, f"⚠️ Trade not found: {trade_id}\nUse /trades to see the list")

        except Exception as e:
            print(f"Command error: {e}")
        time.sleep(2)

# ─── MAIN ─────────────────────────────────────────────────
def main():
    global last_update_id
    print("=" * 50)
    print("🤖 CryptoPing")
    load_from_telegram()
    print(f"📋 {len(watchlist)} coins | 👥 {len(subscribers)} subs")

    # Skip all old pending messages on startup
    try:
        r = http_session.get(f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/getUpdates",
            params={"limit": 1, "offset": -1}, timeout=10)
        if r.status_code == 200:
            results = r.json().get("result", [])
            if results:
                last_update_id = results[-1]["update_id"]
                print(f"⏭ Skipping old messages, starting from update_id: {last_update_id}")
    except:
        pass

    print("=" * 50)

    send_to(ADMIN_CHAT_ID,
        f"✅ <b>CryptoPing is running!</b>\n\n"
        f"📋 Coins: {len(watchlist)}\n"
        f"👥 Subscribers: {len(subscribers)}\n"
        f"💼 Active trades: {len(active_trades)}\n\n"
        f"This build:\n"
        f"• 🐛 Fixed connection-pool leak that exhausted ephemeral ports under\n"
        f"  parallel scanning — now uses a shared, pooled HTTP session\n"
        f"• 🐛 get_klines/get_ticker now log real errors instead of silently\n"
        f"  swallowing them (this is what surfaced the HTTP 451 geo-block)\n"
        f"• 🔇 Phase 1 (Early Watch) no longer sends a notification — tracked\n"
        f"  internally only; Phase 2/3 still notify as before\n"
        f"• 📅 1D timeframe added across Volume Spike, Buildup, Higher Lows,\n"
        f"  and Trendline Breakout/Retest\n"
        f"• 📈 Higher Lows check now also requires Higher Highs (real uptrend\n"
        f"  structure, not just one bounce)\n"
        f"• 🚫 1H trendline breakout/retest disabled (was timing-sensitive and\n"
        f"  a source of missed signals) — 4H/1D trendline stays active, as\n"
        f"  does 1H manual-zone OB bounce/confirm (proven signal source)\n"
        f"• 📐 Lower channel-line breakout detection added (descending channel\n"
        f"  support breaks, not just upper resistance breaks)\n"
        f"• 📅 /addzone now accepts 1D as a timeframe\n"
        f"• 👀 Wick-rejection early warning at manual zones (before full\n"
        f"  confirmation, flags long-lower-wick rejection candles)\n"
        f"• ⏳ Zone-confirmed messages now show coiling duration (zones\n"
        f"  active 14+/30+ days get flagged — bigger breakouts tend to\n"
        f"  follow long consolidation)\n"
        f"• 🔥 New Top Picks topic — zone confirmations with 2+ strong signals\n"
        f"  (long coiling, daily confluence, 1D timeframe, 3x+ volume) route\n"
        f"  there instead of High Priority, no duplication\n\n"
        f"Carried over from before:\n"
        f"• 🌐 CryptoPing rebrand, fully English, persistent state files\n"
        f"• 🐛 Zone confirmation no longer silently blocked by confidence score\n"
        f"• 🐛 Explosive Pump's \"already dumped\" filter relaxed (0.97 → 0.90)\n"
        f"• 🐛 Duplicate-message and topic-routing fixes\n"
        f"• ⚡ Re-pump detection for sideways→breakout moves with no retest\n"
        f"• 🔥 Follow-through score on Volume Spike/Explosive Pump\n"
        f"• 💼 Active Trade Monitor (/trade, /trades, /closetrade)\n"
        f"• 📊 EMA + candle/volume + structure combo trend score\n"
        f"• 📈 Dynamic SL trail suggestion (1R/2R)\n"
        f"• 🎯 4H↔Daily zone confluence tagging\n"
        f"• 📍 Zone bounce history (repeat zones flagged)"
    )

    Thread(target=handle_commands, daemon=True).start()
    Thread(target=monitor_momentum, daemon=True).start()

    def check_symbol(symbol):
        try:
            check_timeframe(symbol, "5m")
            check_timeframe(symbol, "1h")
            check_timeframe(symbol, "4h")
            check_timeframe(symbol, "1d")
            check_postpump_retracement(symbol)
            check_buy_pressure(symbol)
            check_volume_surge(symbol)
            check_explosive_pump(symbol)
            check_abnormal_volume(symbol)
            check_prepump(symbol)
            check_big_pump_setup(symbol)
            check_breakouts(symbol)
        except Exception as e:
            print(f"Error checking {symbol}: {e}")

    # Manual zones run separately (not per-symbol)
    def run_manual_zones():
        while True:
            try:
                check_manual_zones()
            except Exception as e:
                print(f"Manual zone error: {e}")
            time.sleep(60)

    Thread(target=run_manual_zones, daemon=True).start()

    def run_active_trades():
        while True:
            try:
                check_active_trades()
            except Exception as e:
                print(f"Active trade monitor error: {e}")
            time.sleep(60)

    Thread(target=run_active_trades, daemon=True).start()

    while True:
        wl = list(watchlist)
        print(f"\n[{datetime.now().strftime('%H:%M:%S')}] Checking {len(wl)} coins (parallel)...")
        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = {executor.submit(check_symbol, symbol): symbol for symbol in wl}
            for future in as_completed(futures):
                pass
        print(f"Next check in 5 min...")
        auto_update_watchlist()
        time.sleep(CHECK_INTERVAL)

if __name__ == "__main__":
    main()
