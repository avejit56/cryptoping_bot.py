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
from threading import Thread, Lock
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

# ─── GLOBAL BINANCE RATE LIMITER (fixes repeated 418 bans) ──
"""
BUGFIX: even after caching klines/ticker, a cold start (every restart/redeploy
wipes the in-memory cache) means ALL 429 coins x 4 timeframes try to fetch
fresh data within the same few seconds, across 10 parallel threads. That burst
alone is enough to exceed Binance's actual documented limit of 1200 request
weight per minute and trigger an HTTP 418 IP ban — this has now happened
multiple times specifically right after a deploy.

This is a simple token-bucket limiter: every outbound call to Binance must
acquire a slot before firing, and slots refill at a fixed safe rate. This caps
the bot's total request rate globally, across every thread, every endpoint,
at all times — not just at startup. The target rate is intentionally well
under Binance's 1200/min ceiling to leave headroom for weight differences
between endpoints (the no-symbol bulk ticker call costs much more than a
single klines call) and for any other usage on the same IP.
"""
_binance_rate_lock = Lock()
_binance_call_times = []  # sliding window of recent call timestamps
BINANCE_MAX_CALLS_PER_WINDOW = 8    # ~8 calls/sec sustained -> ~960 weight/min (klines weight=2),
                                     # leaving headroom under Binance's 1200/min ceiling for the
                                     # heavier no-symbol bulk ticker call too
BINANCE_RATE_WINDOW = 1.0           # seconds

def _binance_rate_limit_wait():
    """Blocks the calling thread until a request slot is available."""
    while True:
        with _binance_rate_lock:
            now = time.time()
            # Drop timestamps older than the window
            while _binance_call_times and _binance_call_times[0] < now - BINANCE_RATE_WINDOW:
                _binance_call_times.pop(0)
            if len(_binance_call_times) < BINANCE_MAX_CALLS_PER_WINDOW:
                _binance_call_times.append(now)
                return
        time.sleep(0.05)

_original_session_get = http_session.get

def _rate_limited_get(url, *args, **kwargs):
    if "binance.com" in url:
        _binance_rate_limit_wait()
    return _original_session_get(url, *args, **kwargs)

http_session.get = _rate_limited_get

TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN", "")  # set in Railway → Variables
ADMIN_CHAT_ID = os.environ.get("ADMIN_CHAT_ID", "")  # set in Railway → Variables
BOT_VERSION = "v2.1"  # increment this with every update


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
TOPIC_BIG_PUMP    = 22047  # 🚀 Big Pump — dedicated last-line-of-defense channel (note #7)
TOPIC_MY_SETUPS   = 6386  # 📍 My Setups (manual zones, lines, watches — Avejit's own marked levels)

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
    "ENSOUSDT","IOTAUSDT","BCHUSDT","ROSEUSDT","PLUMEUSDT","VETUSDT","DNTUSDT",
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
removed_coins = set()  # coins explicitly /remove'd — persisted so they don't reappear
                        # even if they're in DEFAULT_WATCHLIST and the bot restarts

alerted_coins = {}
momentum_tracking = {}
accumulation_tracking = {}
buildup_alerted = {}
gradual_buildup_alerted = {}   # {f"{symbol}_{tf}_gradual": last_alert_time}
range_breakout_alerted = {}    # {f"{symbol}_breakout": last_alert_time} — 4H confirmed alerts only
range_breakout_tracking = {}   # {symbol: {range_high, range_low, breakout_close, breakout_vol_ratio,
                               #           breakout_time, near_top_touches, range_width_pct}}
                               # 1H breakouts go here silently; alert fires when 4H confirms the hold
building_signal_tracker = {}   # {symbol: {"signals": [...], "last_combined_alert": time, "window_start": time}}
trendline_alerted = {}
postpump_alerted = {}
ob_fvg_zone_tracking = {}
trendline_retest_tracking = {}
retest_watch_list = {}  # {f"{symbol}_{chat_id}": {symbol, chat_id, requested_time, last_seen_state}}
_last_cleanup_check = 0  # timestamp of last weekly auto-cleanup run
signal_performance = {}
buy_pressure_alerted = {}
last_coin_alert = {}
subscribers_info = {}
last_update_id = 0
volume_surge_alerted = {}   # {symbol: last_alert_time} — 6hr cooldown
manual_zones = {}           # {zone_id: {symbol, tf, low, high, added_time, state}}
zone_high_alerted = {}     # {zone_id_touch: last_notify_time}
manual_lines = {}          # {line_id: {symbol, tf, price, added_time, state, chat_id, name}}
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
_BOT_DIR = os.environ.get("BOT_DATA_DIR", "/data")
# All persistent data (zones, watchlist, cooldowns, etc.) goes to /data which
# must be a Railway Volume mount — without that, everything resets on redeploy.
# Set BOT_DATA_DIR env variable to override (e.g. for local testing).
os.makedirs(_BOT_DIR, exist_ok=True)
ZONES_FILE     = os.path.join(_BOT_DIR, "zones.json")
WATCHLIST_FILE = os.path.join(_BOT_DIR, "watchlist.json")
REMOVED_COINS_FILE = os.path.join(_BOT_DIR, "removed_coins.json")
SUBS_FILE      = os.path.join(_BOT_DIR, "subscribers.json")
SIGNAL_QUEUE_FILE = os.path.join(_BOT_DIR, "signal_queue.json")
TRADES_FILE    = os.path.join(_BOT_DIR, "active_trades.json")
ZONE_HISTORY_FILE = os.path.join(_BOT_DIR, "zone_history.json")
SIGNAL_PERFORMANCE_FILE = os.path.join(_BOT_DIR, "signal_performance.json")
PREPUMP_PHASES_FILE = os.path.join(_BOT_DIR, "prepump_phases.json")
TRENDLINE_TRACKING_FILE = os.path.join(_BOT_DIR, "trendline_retest_tracking.json")
COOLDOWN_TRACKERS_FILE = os.path.join(_BOT_DIR, "cooldown_trackers.json")
RETEST_WATCH_FILE = os.path.join(_BOT_DIR, "retest_watch.json")
MANUAL_LINES_FILE = os.path.join(_BOT_DIR, "manual_lines.json")

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

def auto_add_zone(sym, tf, low, high, source="auto_high_priority"):
    """
    Auto-adds a zone (used by the High Priority auto-zone feature). Reuses
    the same duplicate/overlap check as the manual /addzone command so
    auto-added zones never collide with existing ones (manual or auto).
    Returns the new zone_id on success, or None if skipped (duplicate or
    invalid range).
    """
    try:
        if low is None or high is None or low >= high:
            return None
        for eid, ez in manual_zones.items():
            try:
                if ez.get("symbol") != sym or ez.get("tf", "4h") != tf:
                    continue
                e_low, e_high = ez["low"], ez["high"]
                overlaps = low <= e_high and high >= e_low
                nearly_same = (
                    abs(low - e_low) <= e_low * 0.005
                    and abs(high - e_high) <= e_high * 0.005
                )
                if overlaps or nearly_same:
                    return None  # duplicate/overlap — skip silently
            except Exception:
                continue
        zone_count = sum(1 for k in manual_zones if k.startswith(f"{sym}_{tf}"))
        zone_id = f"{sym}_{tf}_{zone_count+1}"
        manual_zones[zone_id] = {
            "symbol": sym, "tf": tf,
            "low": low, "high": high,
            "added_time": time.time(),
            "state": "waiting",
            "alert_sent_time": 0,
            "source": source,
        }
        save_zones()
        return zone_id
    except Exception as e:
        print(f"Auto zone add error {sym}: {e}")
        return None

def save_manual_lines():
    try:
        with open(MANUAL_LINES_FILE, "w") as f:
            _json.dump(manual_lines, f, indent=2)
    except Exception as e:
        print(f"Manual line save error: {e}")

def load_manual_lines():
    global manual_lines
    try:
        if os.path.exists(MANUAL_LINES_FILE):
            with open(MANUAL_LINES_FILE) as f:
                manual_lines = _json.load(f)
            print(f"✅ Manual lines loaded: {len(manual_lines)}")
        else:
            print("📏 No manual lines file, starting fresh")
    except Exception as e:
        print(f"Manual line load error: {e}")

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
    try:
        bundle = {
            "alerted_coins": alerted_coins,
            "buildup_alerted": buildup_alerted,
            "gradual_buildup_alerted": gradual_buildup_alerted,
            "range_breakout_alerted": range_breakout_alerted,
            "range_breakout_tracking": range_breakout_tracking,
            "trendline_alerted": trendline_alerted,
            "postpump_alerted": postpump_alerted,
            "buy_pressure_alerted": buy_pressure_alerted,
            "volume_surge_alerted": volume_surge_alerted,
            "zone_high_alerted": zone_high_alerted,
            "big_pump_alerted": big_pump_alerted,
            "breakout_alerted": breakout_alerted,
            "_global_liq_alerted": _global_liq_alerted,
            "_high_confidence_alerted": _high_confidence_alerted,
            "_extreme_pump_alerted": _extreme_pump_alerted,
            "_dormant_coil_alerted": _dormant_coil_alerted,
            "_whale_trade_alerted": _whale_trade_alerted,
            "_btc_divergence_alerted": _btc_divergence_alerted,
            "_known_bad_chat_ids": _known_bad_chat_ids,
        }
        with open(COOLDOWN_TRACKERS_FILE, "w") as f:
            _json.dump(bundle, f, indent=2)
    except Exception as e:
        print(f"Cooldown trackers save error: {e}")

def load_cooldown_trackers():
    global alerted_coins, buildup_alerted, gradual_buildup_alerted, range_breakout_alerted
    global range_breakout_tracking, trendline_alerted, postpump_alerted
    global buy_pressure_alerted, volume_surge_alerted, zone_high_alerted
    global big_pump_alerted, breakout_alerted
    try:
        if os.path.exists(COOLDOWN_TRACKERS_FILE):
            with open(COOLDOWN_TRACKERS_FILE) as f:
                bundle = _json.load(f)
            alerted_coins.update(bundle.get("alerted_coins", {}))
            buildup_alerted.update(bundle.get("buildup_alerted", {}))
            gradual_buildup_alerted.update(bundle.get("gradual_buildup_alerted", {}))
            range_breakout_alerted.update(bundle.get("range_breakout_alerted", {}))
            range_breakout_tracking.update(bundle.get("range_breakout_tracking", {}))
            trendline_alerted.update(bundle.get("trendline_alerted", {}))
            postpump_alerted.update(bundle.get("postpump_alerted", {}))
            buy_pressure_alerted.update(bundle.get("buy_pressure_alerted", {}))
            volume_surge_alerted.update(bundle.get("volume_surge_alerted", {}))
            zone_high_alerted.update(bundle.get("zone_high_alerted", {}))
            big_pump_alerted.update(bundle.get("big_pump_alerted", {}))
            breakout_alerted.update(bundle.get("breakout_alerted", {}))
            _global_liq_alerted.update(bundle.get("_global_liq_alerted", {}))
            _high_confidence_alerted.update(bundle.get("_high_confidence_alerted", {}))
            _extreme_pump_alerted.update(bundle.get("_extreme_pump_alerted", {}))
            _dormant_coil_alerted.update(bundle.get("_dormant_coil_alerted", {}))
            _whale_trade_alerted.update(bundle.get("_whale_trade_alerted", {}))
            _btc_divergence_alerted.update(bundle.get("_btc_divergence_alerted", {}))
            _known_bad_chat_ids.update(bundle.get("_known_bad_chat_ids", {}))
            total = sum(len(v) if isinstance(v, dict) else 0 for v in bundle.values())
            print(f"✅ Cooldown trackers loaded: {total} entries across {len(bundle)} dicts")
        else:
            print("⏱ No cooldown trackers file, starting fresh")
    except Exception as e:
        print(f"Cooldown trackers load error: {e}")

def save_retest_watch():
    try:
        with open(RETEST_WATCH_FILE, "w") as f:
            _json.dump(retest_watch_list, f, indent=2)
    except Exception as e:
        print(f"Retest watch save error: {e}")

def load_retest_watch():
    global retest_watch_list
    try:
        if os.path.exists(RETEST_WATCH_FILE):
            with open(RETEST_WATCH_FILE) as f:
                retest_watch_list = _json.load(f)
            print(f"✅ Retest watch list loaded: {len(retest_watch_list)}")
        else:
            print("👁 No retest watch file, starting fresh")
    except Exception as e:
        print(f"Retest watch load error: {e}")

def save_watchlist_file():
    try:
        extra = [c for c in watchlist if c not in DEFAULT_WATCHLIST]
        with open(WATCHLIST_FILE, "w") as f:
            _json.dump(extra, f, indent=2)
    except Exception as e:
        print(f"Watchlist save error: {e}")

def save_removed_coins():
    try:
        with open(REMOVED_COINS_FILE, "w") as f:
            _json.dump(sorted(removed_coins), f, indent=2)
    except Exception as e:
        print(f"Removed-coins save error: {e}")

def load_removed_coins():
    global removed_coins
    try:
        if os.path.exists(REMOVED_COINS_FILE):
            with open(REMOVED_COINS_FILE) as f:
                removed_coins = set(_json.load(f))
            print(f"🗑 Removed-coins list loaded: {len(removed_coins)}")
        else:
            removed_coins = set()
    except Exception as e:
        print(f"Removed-coins load error: {e}")
        removed_coins = set()

def load_watchlist_file():
    global watchlist
    # FIX: previously, /remove only removed a coin from the in-memory list for
    # the current run — DEFAULT_WATCHLIST coins always came back on the next
    # restart/redeploy because this function rebuilt watchlist from
    # DEFAULT_WATCHLIST.copy() with no memory of what had been explicitly
    # removed (the ELFUSDT case: removed via /remove, reappeared after the
    # next redeploy, generating "new" signals on a coin already rejected).
    # removed_coins is loaded first and applied here so a removal sticks
    # permanently, even for hardcoded default coins.
    watchlist = [c for c in DEFAULT_WATCHLIST if c not in removed_coins]
    try:
        if os.path.exists(WATCHLIST_FILE):
            with open(WATCHLIST_FILE) as f:
                extra = _json.load(f)
            for c in extra:
                if c not in watchlist and c not in removed_coins:
                    watchlist.append(c)
            print(f"✅ Watchlist: {len(watchlist) - len([c for c in extra if c not in removed_coins])} default + {len([c for c in extra if c not in removed_coins])} extra = {len(watchlist)} (removed: {len(removed_coins)})")
        else:
            print(f"📋 Using default: {len(watchlist)} coins (removed: {len(removed_coins)})")
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
    load_removed_coins()   # must load before load_watchlist_file (which filters using it)
    load_watchlist_file()
    load_subscribers_file()
    load_zones()
    load_active_trades()
    load_zone_history()
    load_signal_performance()
    load_prepump_phases()
    load_trendline_tracking()
    load_cooldown_trackers()
    load_retest_watch()
    load_manual_lines()

# ─── TELEGRAM ─────────────────────────────────────────────
_known_bad_chat_ids = {}  # {chat_id: reason} — any chat_id that's permanently failed, regardless of source

def send_to(chat_id, message, thread_id=None):
    """Send a message. If thread_id is provided, sends to that topic thread."""
    chat_id_key = str(chat_id)  # normalize — different call sites pass int/str inconsistently
    if chat_id_key in _known_bad_chat_ids:
        return  # already confirmed dead — skip silently, don't keep retrying forever
    try:
        payload = {"chat_id": chat_id, "text": message, "parse_mode": "HTML"}
        if thread_id:
            payload["message_thread_id"] = thread_id
        r = http_session.post(f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
            json=payload, timeout=10)
        if r.status_code != 200:
            print(f"⚠️ send_to failed: HTTP {r.status_code} — {r.text[:300]} (len={len(message)})")
            body_lower = r.text.lower()
            if r.status_code == 400 and "chat not found" in body_lower:
                # Permanently invalid chat_id (blocked the bot, deleted
                # account, never started a DM) — blacklist it everywhere,
                # not just the subscribers list, so it stops failing on
                # EVERY send from any feature (watches, trades, etc), not
                # just send_all()'s subscriber loop.
                _known_bad_chat_ids[chat_id_key] = "chat not found"
                if chat_id in subscribers:
                    subscribers.remove(chat_id)
                    subscribers_info.pop(str(chat_id), None)
                    save_subscribers_file()
                elif str(chat_id) in [str(s) for s in subscribers]:
                    subscribers[:] = [s for s in subscribers if str(s) != str(chat_id)]
                    subscribers_info.pop(str(chat_id), None)
                    save_subscribers_file()
                print(f"🧹 Blacklisted invalid chat_id {chat_id} (chat not found) — won't retry")
            elif r.status_code == 400 and "can't parse entities" in body_lower:
                # Malformed HTML in the message (stray '<'/'>' somewhere) —
                # retry once as plain text so the message still gets
                # delivered instead of silently vanishing.
                try:
                    plain_payload = {"chat_id": chat_id, "text": message}
                    if thread_id:
                        plain_payload["message_thread_id"] = thread_id
                    r2 = http_session.post(f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
                        json=plain_payload, timeout=10)
                    if r2.status_code == 200:
                        print(f"✅ send_to: resent as plain text after HTML parse error (len={len(message)})")
                    else:
                        print(f"⚠️ send_to plain-text retry also failed: HTTP {r2.status_code} — {r2.text[:200]}")
                except Exception as e2:
                    print(f"⚠️ send_to plain-text retry exception: {e2}")
    except Exception as e:
        print(f"⚠️ send_to exception: {e}")

def send_to_topic(topic_id, message):
    """Send a message to a specific topic in the CryptoPing Alerts group"""
    try:
        r = http_session.post(f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
            json={
                "chat_id": ALERTS_GROUP_ID,
                "message_thread_id": topic_id,
                "text": message,
                "parse_mode": "HTML"
            }, timeout=10)
        if r.status_code != 200:
            print(f"⚠️ send_to_topic failed: HTTP {r.status_code} — {r.text[:300]} (topic={topic_id}, len={len(message)})")
            if r.status_code == 400 and "can't parse entities" in r.text.lower():
                # Malformed HTML — retry once as plain text so it still gets delivered
                try:
                    r2 = http_session.post(f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
                        json={
                            "chat_id": ALERTS_GROUP_ID,
                            "message_thread_id": topic_id,
                            "text": message,
                        }, timeout=10)
                    if r2.status_code == 200:
                        print(f"✅ send_to_topic: resent as plain text after HTML parse error (topic={topic_id})")
                    else:
                        print(f"⚠️ send_to_topic plain-text retry also failed: HTTP {r2.status_code} — {r2.text[:200]}")
                except Exception as e2:
                    print(f"⚠️ send_to_topic plain-text retry exception: {e2}")
    except Exception as e:
        print(f"⚠️ send_to_topic exception: {e}")

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
    if any(x in message for x in ["LIQUIDITY RECLAIM", "POWER SIGNAL", "RETEST RECLAIM", "HIGH CONFIDENCE"]):
        return TOPIC_BUILDUPS
    elif any(x in message for x in ["ZONE CONFIRMED", "RETEST CONFIRMED", "OB BOUNCE", "TRENDLINE RETEST", "Line Retest Complete", "Retest Complete"]):
        return TOPIC_MY_SETUPS
    elif any(x in message for x in ["EXPLOSIVE PUMP", "BUY PRESSURE", "BREAKOUT!"]):
        return TOPIC_BUILDUPS
    elif any(x in message for x in ["VOLUME SPIKE", "VOLUME SURGE", "EARLY SIGNAL CONFIRMED"]):
        return TOPIC_SPIKES
    elif any(x in message for x in ["BUILD-UP", "ACCUMULATION", "HIGHER LOW", "DIRECT MOMENTUM", "PHASE 1", "PHASE 2", "PHASE 3", "PRE-PUMP", "Breakout!"]):
        return TOPIC_BUILDUPS
    elif any(x in message for x in ["SIGNAL RESULT", "+5%", "+10%", "+20%"]):
        return TOPIC_RESULTS
    else:
        return TOPIC_SPIKES

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

_coil_pattern_tracked = {}  # {symbol: last_tracked_time} — 24h cooldown for signal_performance recording

def get_pattern_history_stats(pattern_name, min_samples=3):
    """
    Looks up historical performance for a specific pattern/signal type (e.g.
    "Coil After Pump") from signal_performance, using the same "highest_after"
    peak-gain tracking the SIGNAL RESULT messages use. Returns a formatted
    stats line, or None if there isn't enough historical data yet (note #10:
    helps size MEDIUM-score setups on data instead of gut feel).
    """
    samples = [
        d for d in signal_performance.values()
        if d.get("signal_type") == pattern_name and d.get("signal_price", 0) > 0
    ]
    if len(samples) < min_samples:
        return None
    gains = [(d["highest_after"] - d["signal_price"]) / d["signal_price"] * 100 for d in samples]
    avg_gain = sum(gains) / len(gains)
    hit_15 = sum(1 for g in gains if g >= 15.0)
    hit_pct = hit_15 / len(gains) * 100
    return f"📊 Historical ({len(gains)} past signals): avg +{avg_gain:.0f}% peak, {hit_pct:.0f}% reached +15%+"

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
        else:
            print(f"⚠️ getUpdates failed: HTTP {r.status_code} — {r.text[:300]}")
    except Exception as e:
        print(f"⚠️ getUpdates exception: {e}")
    return []

# ─── BINANCE ──────────────────────────────────────────────
# ─── KLINES CACHE (rate-limit fix, part 2) ─────────────────
"""
BUGFIX continued: get_ticker was already fixed with a bulk cache, but
get_klines still made one API call per (symbol, interval) — and within a
single scan pass, MANY different check functions (check_timeframe,
check_postpump_retracement, check_buy_pressure, calc_entry_score, etc.) all
call get_klines for the SAME symbol+interval independently. With 426 coins
across up to 4 timeframes, that's potentially 1700+ klines calls per cycle,
even after the ticker fix — enough to trigger another 418 ban.

A short TTL cache, keyed by (symbol, interval, limit), means repeated calls
for the same symbol+interval within the same scan pass reuse one fetch
instead of re-requesting. TTL is scaled to how often each timeframe's candle
actually changes — no point re-fetching 4H klines every 30 seconds when the
candle won't close for hours.
"""
_klines_cache = {}  # {(symbol, interval, limit): {"data": [...], "fetched_at": ts}}
_klines_cache_lock = Lock()
KLINES_CACHE_TTL = {
    "5m": 30, "15m": 60, "1h": 120, "4h": 300, "1d": 900,
}

# ─── BINANCE CONNECTIVITY MONITOR ──────────────────────────
"""
FIX (after a real incident: Binance became unreachable for several minutes —
connect timeouts across every symbol/timeframe, not a rate-limit ban — with
no notification, the only way to know was manually checking Railway logs).

This tracks consecutive failures across get_klines + the ticker cache
refresh (both hit api.binance.com). Past a threshold, it sends ONE admin DM
saying Binance looks unreachable, with how long it's been failing. It then
stays quiet (no spam on every subsequent failure) until a call succeeds
again, at which point it sends a single "reachable again" DM. This doesn't
fix connectivity issues (those are infrastructure/region-routing, not
something code can resolve) — it just makes sure you find out promptly
instead of discovering it later in the logs.
"""
_binance_failure_state = {
    "consecutive_failures": 0,
    "first_failure_time": None,
    "alert_sent": False,
}
_binance_failure_lock = Lock()
BINANCE_FAILURE_ALERT_THRESHOLD = 15  # consecutive failures before alerting
BINANCE_FAILURE_RESET_TIME = 6 * 3600  # forget old failure streaks after 6h of inactivity, just in case

def _record_binance_call_result(success):
    with _binance_failure_lock:
        now = time.time()
        if success:
            was_down = _binance_failure_state["alert_sent"]
            _binance_failure_state["consecutive_failures"] = 0
            _binance_failure_state["first_failure_time"] = None
            _binance_failure_state["alert_sent"] = False
            if was_down:
                send_to(ADMIN_CHAT_ID,
                    "✅ <b>Binance reachable again</b>\n\n"
                    "Connection to api.binance.com has recovered — scanning is back to normal."
                )
                print("✅ Binance connectivity recovered")
            return

        if _binance_failure_state["first_failure_time"] is None:
            _binance_failure_state["first_failure_time"] = now
        _binance_failure_state["consecutive_failures"] += 1

        if (not _binance_failure_state["alert_sent"]
                and _binance_failure_state["consecutive_failures"] >= BINANCE_FAILURE_ALERT_THRESHOLD):
            duration_min = (now - _binance_failure_state["first_failure_time"]) / 60
            _binance_failure_state["alert_sent"] = True
            send_to(ADMIN_CHAT_ID,
                f"🔴 <b>Binance unreachable</b>\n\n"
                f"{_binance_failure_state['consecutive_failures']} consecutive failed requests "
                f"over the last {duration_min:.0f} minute(s) — connection timeouts, not a rate-limit "
                f"ban (that shows a different error). Scanning is effectively paused until this clears.\n\n"
                f"This is usually a Railway region-routing issue on Binance's end, not something fixable "
                f"in the bot itself. If it doesn't recover on its own in a while, a redeploy or switching "
                f"Railway region (Settings → Source) has fixed this before.\n\n"
                f"You'll get another message here once it's reachable again."
            )
            print(f"🔴 Binance connectivity alert sent ({_binance_failure_state['consecutive_failures']} failures, {duration_min:.0f}m)")

def is_plausible_symbol(sym_raw):
    """
    Rejects obviously-invalid symbol input at the command-parsing stage —
    catches cases like a price ("0.0874") getting typed/passed where a
    ticker ("BTC") was expected, which would otherwise silently create a
    persistent watch/entry that retries forever with HTTP 400 'Invalid
    symbol' on every check cycle.
    """
    if not sym_raw:
        return False
    s = sym_raw.strip().upper()
    if not s:
        return False
    if s[0].isdigit() or s[0] == ".":
        return False  # looks like a price/number, not a ticker
    if "." in s:
        return False  # tickers don't contain decimal points
    return True


def get_klines(symbol, interval="5m", limit=50):
    cache_key = (symbol, interval, limit)
    ttl = KLINES_CACHE_TTL.get(interval, 60)
    now = time.time()
    cached = _klines_cache.get(cache_key)
    if cached and now - cached["fetched_at"] < ttl:
        return cached["data"]

    with _klines_cache_lock:
        # Re-check inside the lock in case another thread just refreshed this
        # exact key while we were waiting.
        cached = _klines_cache.get(cache_key)
        if cached and time.time() - cached["fetched_at"] < ttl:
            return cached["data"]
        try:
            r = http_session.get("https://api.binance.com/api/v3/klines",
                params={"symbol": symbol, "interval": interval, "limit": limit}, timeout=10)
            if r.status_code == 200:
                data = r.json()
                _klines_cache[cache_key] = {"data": data, "fetched_at": time.time()}
                _record_binance_call_result(success=True)
                return data
            else:
                print(f"⚠️ get_klines {symbol} {interval}: HTTP {r.status_code} — {r.text[:200]}")
                # 418/429 are rate-limit responses, not connectivity failures —
                # they resolve on their own timer and the rate limiter already
                # exists to prevent them; don't conflate them with Binance
                # being unreachable.
                if r.status_code not in (418, 429):
                    _record_binance_call_result(success=False)
        except Exception as e:
            print(f"⚠️ get_klines {symbol} {interval} exception: {e}")
            _record_binance_call_result(success=False)
    return None

def get_agg_trades(symbol, limit=30):
    """Fetch recent aggregated trades (for whale/block trade detection, note #14)."""
    try:
        _binance_rate_limit_wait()
        r = http_session.get("https://api.binance.com/api/v3/aggTrades",
            params={"symbol": symbol, "limit": limit}, timeout=10)
        if r.status_code == 200:
            _record_binance_call_result(success=True)
            return r.json()
        else:
            print(f"⚠️ get_agg_trades {symbol}: HTTP {r.status_code} — {r.text[:200]}")
            if r.status_code not in (418, 429):
                _record_binance_call_result(success=False)
    except Exception as e:
        print(f"⚠️ get_agg_trades {symbol} exception: {e}")
        _record_binance_call_result(success=False)
    return None

# ─── BULK TICKER CACHE (rate-limit fix) ────────────────────
"""
BUGFIX: get_ticker(symbol) used to make one individual API call per symbol,
every time any check function needed 24h change / last price. With 422 coins
checked on a 5-min loop, calling this dozens of times per coin per pass added
up to thousands of requests per cycle — enough to exceed Binance's request-
weight limit and get the IP banned (HTTP 418, "Way too much request weight
used"). Binance's own error message suggests the fix: batch requests instead
of one-per-symbol.

Binance's /api/v3/ticker/24hr endpoint, called WITHOUT a symbol parameter,
returns ALL symbols' ticker data in a single response. We fetch that once
every TICKER_CACHE_TTL seconds and serve every get_ticker() call from this
in-memory cache — turning thousands of requests into one.
"""
_ticker_cache = {"data": {}, "fetched_at": 0}
_ticker_cache_lock = Lock()
TICKER_CACHE_TTL = 30  # seconds — refresh at most every 30s, regardless of how many checks happen

def _refresh_ticker_cache():
    try:
        r = http_session.get("https://api.binance.com/api/v3/ticker/24hr", timeout=15)
        if r.status_code == 200:
            all_tickers = r.json()
            _ticker_cache["data"] = {t["symbol"]: t for t in all_tickers}
            _ticker_cache["fetched_at"] = time.time()
            _record_binance_call_result(success=True)
        else:
            print(f"⚠️ ticker cache refresh: HTTP {r.status_code} — {r.text[:200]}")
            if r.status_code not in (418, 429):
                _record_binance_call_result(success=False)
    except Exception as e:
        print(f"⚠️ ticker cache refresh exception: {e}")
        _record_binance_call_result(success=False)

def get_ticker(symbol):
    now = time.time()
    if now - _ticker_cache["fetched_at"] > TICKER_CACHE_TTL:
        with _ticker_cache_lock:
            # Re-check inside the lock — another thread may have just refreshed
            # while we were waiting for the lock, avoiding a redundant call.
            if time.time() - _ticker_cache["fetched_at"] > TICKER_CACHE_TTL:
                _refresh_ticker_cache()
    return _ticker_cache["data"].get(symbol)

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

    # Fibonacci retracement level detection
    fib_label = ""
    if data["pump_high"] and lowest and data["pump_high"] > lowest:
        swing_high = data["pump_high"]
        swing_low = lowest
        fib_range = swing_high - swing_low
        fib_levels = {
            "0.236": swing_high - 0.236 * fib_range,
            "0.382": swing_high - 0.382 * fib_range,
            "0.500": swing_high - 0.500 * fib_range,
            "0.618": swing_high - 0.618 * fib_range,
            "0.786": swing_high - 0.786 * fib_range,
        }
        closest_fib = None
        closest_dist = float("inf")
        for level_name, level_price in fib_levels.items():
            dist = abs(current_price - level_price) / level_price
            if dist < closest_dist and dist <= 0.03:  # within 3% of level
                closest_dist = dist
                closest_fib = (level_name, level_price)
        if closest_fib:
            fname, fprice = closest_fib
            if fname == "0.618":
                fib_label = f"📐 Fib: <b>0.618 🎯 GOLDEN POCKET</b> (~{format_price(fprice)}) — OB + Fibonacci confluence\n"
            elif fname == "0.500":
                fib_label = f"📐 Fib: 0.500 ({format_price(fprice)}) — mid-range bounce, institutional level\n"
            elif fname == "0.382":
                fib_label = f"📐 Fib: 0.382 ({format_price(fprice)}) — shallow pullback, trend still strong\n"
            elif fname == "0.786":
                fib_label = f"📐 Fib: 0.786 ({format_price(fprice)}) — deep pullback, last support before trend break\n"
            else:
                fib_label = f"📐 Fib: {fname} (~{format_price(fprice)})\n"

    # MTF label for alert
    mtf_confirm = "4H OB → 1H retest ✅" if ob_label == "4H OB" else "1H OB → 15M retest ✅"
    full_confluence_ob = build_entry_decision_block(symbol, current_price, tf="4h")

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
        f"{fib_label}"
        f"⚡ Volume: {vol_ratio:.1f}x\n"
        f"📐 MTF: {mtf_confirm}\n"
        + (f"\n{full_confluence_ob}\n" if full_confluence_ob else "") +
        f"\n🕐 {datetime.now().strftime('%H:%M:%S')}\n\n"
        f"⚠️ <i>OB bounce! Check the chart before entry.</i>",
        symbol=symbol
    )
    if sent:
        print(f"🎯 {ob_label} MTF bounce confirmed: {symbol}{' [FIB ' + closest_fib[0] + ']' if closest_fib else ''}")
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
            track_building_signal(symbol, f"Volume Build-up [{cfg['label']}]", price)
        check_high_confidence_signal(symbol, f"Volume Build-up [{cfg['label']}]", price)
"""
The existing check_volume_buildup() catches 3 consecutive candles each with
2.5x+ volume — a short burst pattern. But some coins pump after a multi-day
gradual accumulation: volume slowly rises across 5-8 candles (no single candle
meets 2.5x threshold alone), then a breakout candle fires on the elevated base.
This detector looks for that pattern on 4H and 1D specifically — the slope of
the last N candles' volume being consistently upward, plus a strong final candle.
Fires to Top Picks since it's a high-confidence setup when it triggers.
"""
def check_gradual_buildup(symbol, tf, klines):
    if tf not in ("4h", "1d") or len(klines) < 15:
        return
    now = time.time()
    key = f"{symbol}_{tf}_gradual"
    cooldown = 48 * 3600 if tf == "4h" else 72 * 3600
    if now - gradual_buildup_alerted.get(key, 0) < cooldown:
        return

    ticker = get_ticker(symbol)
    if not ticker:
        return
    change_24h = float(ticker["priceChangePercent"])
    if change_24h < 2.0:
        return
    if is_daily_downtrend(symbol, float(ticker["lastPrice"])):
        return

    # Look at the last 7 closed candles (excluding the forming one)
    window = klines[-8:-1]
    if len(window) < 6:
        return
    vols = [float(k[5]) for k in window]

    # Baseline: average of the first half of the window
    half = len(vols) // 2
    baseline_avg = sum(vols[:half]) / half if half else 1

    # Check: is there a consistent upward slope in volume?
    # Use a simple linear regression slope — positive slope = ascending volume
    n = len(vols)
    mean_x = (n - 1) / 2
    mean_y = sum(vols) / n
    slope_num = sum((i - mean_x) * (vols[i] - mean_y) for i in range(n))
    slope_den = sum((i - mean_x) ** 2 for i in range(n))
    vol_slope = slope_num / slope_den if slope_den else 0

    # Slope must be clearly positive (volume growing over time)
    if vol_slope <= 0:
        return

    # Most recent volume must be meaningfully above the baseline
    recent_vol_ratio = vols[-1] / baseline_avg if baseline_avg > 0 else 0
    if recent_vol_ratio < 1.8:
        return

    # Last closed candle must be bullish with a decent body
    last = klines[-2]
    l_open, l_close = float(last[1]), float(last[4])
    l_high, l_low = float(last[2]), float(last[3])
    if l_close <= l_open:
        return
    candle_range = l_high - l_low
    body_ratio = (l_close - l_open) / candle_range if candle_range > 0 else 0
    if body_ratio < 0.35:
        return

    # Price above 20EMA
    closes = [float(k[4]) for k in klines[:-1]]
    ema20 = calculate_ema(closes, 20)
    if ema20 and l_close < ema20:
        return

    gradual_buildup_alerted[key] = now
    price = float(ticker["lastPrice"])

    # Check if the forming candle (klines[-1]) is already showing strong movement
    # so we can label this as "live" vs "closed candle" confirmed
    forming = klines[-1]
    f_open  = float(forming[1])
    f_vol   = float(forming[5])
    live_moving = price > f_open * 1.01 and f_vol > baseline_avg * 2.0
    live_tag = "⚡ <b>LIVE — forming candle already moving!</b>\n" if live_moving else ""

    send_to_topic(TOPIC_BUILDUPS,
        f"🌊 <b>GRADUAL BUILDUP DETECTED [{tf.upper()}]</b>\n\n"
        f"🪙 <b>{symbol}</b>\n"
        f"💰 Price: {format_price(price)}\n"
        f"📊 24h: {change_24h:+.2f}%\n"
        f"📈 Volume rising across last {n} candles "
        f"(recent: {recent_vol_ratio:.1f}x baseline)\n"
        f"✅ Strong green close on the latest candle\n"
        f"{live_tag}"
        f"🕐 {datetime.now().strftime('%H:%M:%S')}\n\n"
        f"💡 <i>Volume has been building up steadily — "
        f"this type of slow accumulation before a breakout can produce "
        f"larger moves than sudden spikes. Check the chart.</i>"
    )
    signal_performance[f"{symbol}_gradual_{tf}_{int(now)}"] = {
        "symbol": symbol, "signal_price": price,
        "signal_time": now, "signal_type": f"Gradual Buildup [{tf.upper()}]",
        "highest_after": price,
    }
    print(f"🌊 Gradual buildup: {symbol} [{tf}] vol_slope={vol_slope:.1f} ratio={recent_vol_ratio:.1f}x{' [LIVE]' if live_moving else ''}")
    track_building_signal(symbol, f"Gradual Buildup [{tf.upper()}]", price)
    check_high_confidence_signal(symbol, f"Gradual Buildup [{tf.upper()}]", price)
    start_prospect_watch(symbol, f"Gradual Buildup [{tf.upper()}]")


# ─── RANGE BREAKOUT DETECTOR (ARPA case) ──────────────────
"""
ARPA pumped +69% from a ranging base in 1-2 candles with no prior signal.
The existing volume spike detector (check_timeframe) requires 8x (4H) or
15x (1H) volume — a bar that fast vertical moves can miss if the preceding
range had very low volume (making the average low, so the spike ratio is
technically high, but the candle was already well past its range by the time
the scan ran, triggering the current_price < c1_close*0.90 guard).

This detector takes a different approach: instead of volume ratio alone,
it looks for price CLOSING above the top of a tight preceding range (equal
highs that formed a ceiling) with at least 3x volume confirmation — a
"coiled spring" breakout. Fires to Top Picks.
"""
def check_range_breakout(symbol, tf, klines):
    """
    Two-stage range-breakout detector to reduce noise (after seeing too many
    1H breakout alerts that turned out to be short-lived spikes):

    Stage 1 — 1H: if a coin breaks above a tight range (coiling ≤15%, 2+
    touches at the top, 3x+ volume), record it silently in
    range_breakout_tracking. No alert fires yet.

    Stage 2 — 4H: on the next 4H candle close, check if the 1H-detected
    breakout is still holding (price above the original range_high). If yes,
    fire the Top Picks alert. If not, discard silently.

    4H breakouts still fire an immediate alert (they're already slower/more
    reliable by nature and don't need the extra confirmation step).
    """
    if tf not in ("1h", "4h") or len(klines) < 20:
        return
    now = time.time()

    ticker = get_ticker(symbol)
    if not ticker:
        return
    change_24h = float(ticker["priceChangePercent"])
    current_price = float(ticker["lastPrice"])

    # ── Stage 2: check pending 1H tracking entries on every 4H scan ──
    if tf == "4h" and symbol in range_breakout_tracking:
        tracked = range_breakout_tracking[symbol]
        age_hours = (now - tracked["breakout_time"]) / 3600
        range_high = tracked["range_high"]
        alerted_key = f"{symbol}_breakout"

        # Expire after 24h if no 4H confirmation came
        if age_hours > 24:
            range_breakout_tracking.pop(symbol, None)
        elif (current_price >= range_high * 1.002
              and now - range_breakout_alerted.get(alerted_key, 0) > 24 * 3600):
            # 4H scan sees price still holding above range_high — fire the alert
            range_breakout_alerted[alerted_key] = now
            range_breakout_tracking.pop(symbol, None)
            breakout_pct = (current_price - range_high) / range_high * 100
            send_to_topic(TOPIC_BUILDUPS,
                f"🚀 <b>RANGE BREAKOUT CONFIRMED [1H→4H]</b>\n\n"
                f"🪙 <b>{symbol}</b>\n"
                f"💰 Price: {format_price(current_price)}\n"
                f"📊 24h: {change_24h:+.2f}%\n"
                f"📐 Range: {format_price(tracked['range_low'])} — {format_price(range_high)} "
                f"({tracked['range_width_pct']:.1f}% wide, {tracked['near_top_touches']} touches at top)\n"
                f"⚡ Holding +{breakout_pct:.1f}% above {format_price(range_high)} after a 4H close\n"
                f"💥 Breakout volume was {tracked['vol_ratio']:.1f}x range average\n"
                f"🕐 {datetime.now().strftime('%H:%M:%S')}\n\n"
                f"⚠️ <i>Check the chart before entry.</i>"
            )
            signal_performance[f"{symbol}_range_breakout_{int(now)}"] = {
                "symbol": symbol, "signal_price": current_price,
                "signal_time": now, "signal_type": "Range Breakout [1H→4H]",
                "highest_after": current_price,
            }
            print(f"🚀 Range breakout CONFIRMED (4H hold): {symbol} +{breakout_pct:.1f}% above range")
            track_building_signal(symbol, "Range Breakout [1H→4H]", current_price)
            check_high_confidence_signal(symbol, "Range Breakout [1H→4H]", current_price)
            start_prospect_watch(symbol, "Range Breakout [1H→4H]")
        elif current_price < range_high * 0.97:
            # Price dropped back below the range — false breakout, discard
            range_breakout_tracking.pop(symbol, None)
            print(f"❌ Range breakout FAILED (price dropped back): {symbol}")
        return  # whether we alerted or not, stage-2 check is done for this coin

    # ── Stage 1 (1H) and direct 4H detection ──
    if change_24h < 0:
        return

    lookback = klines[-12:-2]
    if len(lookback) < 8:
        return

    highs = [float(k[2]) for k in lookback]
    lows = [float(k[3]) for k in lookback]
    range_high = max(highs)
    range_low = min(lows)
    range_width_pct = (range_high - range_low) / range_low * 100 if range_low > 0 else 100

    if range_width_pct > 10:  # tighter range = more coiled = more reliable
        return

    near_top_touches = sum(1 for h in highs if h >= range_high * 0.99)
    if near_top_touches < 3:  # raised from 2 to 3 — needs real resistance level
        return

    range_vols = [float(k[5]) for k in lookback]
    avg_range_vol = sum(range_vols) / len(range_vols) if range_vols else 1

    # ── LIVE detection (forming candle) ──
    live_key = f"{symbol}_{tf}_breakout_live"
    forming = klines[-1]
    f_open = float(forming[1])
    f_vol  = float(forming[5])
    f_vol_ratio = f_vol / avg_range_vol if avg_range_vol > 0 else 0
    live_crossed = (
        current_price > range_high * 1.001 and
        current_price > f_open and
        f_vol_ratio >= 1.5 and
        now - range_breakout_alerted.get(live_key, 0) > 4 * 3600
    )
    if live_crossed:
        range_breakout_alerted[live_key] = now
        breakout_pct_live = (current_price - range_high) / range_high * 100
        send_to_topic(TOPIC_BUILDUPS,
            f"⚡ <b>RANGE BREAKOUT — LIVE [{tf.upper()}]</b>\n\n"
            f"🪙 <b>{symbol}</b>\n"
            f"💰 Price: {format_price(current_price)} (+{breakout_pct_live:.1f}% above range)\n"
            f"📊 24h: {change_24h:+.2f}%\n"
            f"📐 Range: {format_price(range_low)} — {format_price(range_high)} "
            f"({range_width_pct:.1f}% wide, {near_top_touches} touches at top)\n"
            f"💥 Volume: {f_vol_ratio:.1f}x range avg (forming candle)\n\n"
            f"⏳ <i>Candle still forming — early heads-up. "
            f"A confirmed alert follows when the candle closes.</i>\n\n"
            f"⚠️ <i>Check the chart before entry.</i>"
        )
        print(f"⚡ Range breakout LIVE: {symbol} [{tf}] +{breakout_pct_live:.1f}%")

    last = klines[-2]
    l_open, l_close = float(last[1]), float(last[4])
    if l_close <= l_open:
        return
    if l_close <= range_high * 1.003:  # must close at least 0.3% above range
        return

    breakout_vol = float(last[5])
    vol_ratio = breakout_vol / avg_range_vol if avg_range_vol > 0 else 0
    if vol_ratio < 4.0:  # raised from 3x to 4x — stronger confirmation needed
        return

    if current_price < l_close * 0.93:  # allow slightly more pullback
        return

    if tf == "1h":
        # Silent tracking — no alert yet, wait for 4H confirmation
        if symbol not in range_breakout_tracking:
            range_breakout_tracking[symbol] = {
                "range_high": range_high, "range_low": range_low,
                "range_width_pct": range_width_pct,
                "near_top_touches": near_top_touches,
                "breakout_close": l_close, "vol_ratio": vol_ratio,
                "breakout_time": now,
            }
            print(f"👁 Range breakout TRACKED (1H, waiting 4H confirm): {symbol} "
                  f"vol={vol_ratio:.1f}x range_width={range_width_pct:.1f}%")
    else:
        # 4H direct: reliable enough to alert immediately
        alerted_key = f"{symbol}_breakout"
        if now - range_breakout_alerted.get(alerted_key, 0) < 24 * 3600:
            return
        range_breakout_alerted[alerted_key] = now
        breakout_pct = (l_close - range_high) / range_high * 100
        send_to_topic(TOPIC_BUILDUPS,
            f"🚀 <b>RANGE BREAKOUT [4H]</b>\n\n"
            f"🪙 <b>{symbol}</b>\n"
            f"💰 Price: {format_price(current_price)}\n"
            f"📊 24h: {change_24h:+.2f}%\n"
            f"📐 Range: {format_price(range_low)} — {format_price(range_high)} "
            f"({range_width_pct:.1f}% wide, {near_top_touches} touches at top)\n"
            f"⚡ Broke above {format_price(range_high)} by +{breakout_pct:.1f}%\n"
            f"💥 Volume: {vol_ratio:.1f}x range average\n"
            f"🕐 {datetime.now().strftime('%H:%M:%S')}\n\n"
            f"⚠️ <i>Check the chart before entry.</i>"
        )
        signal_performance[f"{symbol}_range_breakout_4h_{int(now)}"] = {
            "symbol": symbol, "signal_price": current_price,
            "signal_time": now, "signal_type": "Range Breakout [4H]",
            "highest_after": current_price,
        }
        print(f"🚀 Range breakout (4H direct): {symbol} vol={vol_ratio:.1f}x")
        track_building_signal(symbol, "Range Breakout [4H]", current_price)
        check_high_confidence_signal(symbol, "Range Breakout [4H]", current_price)
        start_prospect_watch(symbol, "Range Breakout [4H]")


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
            track_building_signal(symbol, f"Accumulation [{cfg['label']}]", price)
        check_high_confidence_signal(symbol, f"Accumulation [{cfg['label']}]", price)
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
        track_building_signal(symbol, "Volume Surge [1H]", current_price)
        check_high_confidence_signal(symbol, "Volume Surge [1H]", current_price)
def zone_history_key(symbol, z_low, z_high):
    """Stable key for a price zone, rounded so near-identical zones match"""
    return f"{symbol}_{z_low:.8f}_{z_high:.8f}"

def record_zone_outcome(symbol, z_low, z_high, outcome, vol_ratio=None):
    """outcome: 'confirmed', 'invalidated', 'retest_confirmed'.
    vol_ratio (optional): volume ratio at the time of this attempt, stored
    so future attempts can be compared against past ones (note #3)."""
    key = zone_history_key(symbol, z_low, z_high)
    hist = zone_bounce_history.setdefault(key, {
        "symbol": symbol, "low": z_low, "high": z_high,
        "bounce_count": 0, "invalid_count": 0,
        "last_time": 0, "outcomes": []
    })
    hist["last_time"] = time.time()
    hist["outcomes"].append({"outcome": outcome, "time": time.time(), "vol_ratio": vol_ratio})
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

def check_resistance_rejection_history(symbol, current_price, current_vol_ratio=None):
    """
    Note #3 (part 2): finds a resistance zone slightly ABOVE current price
    that has a history of repeated rejections (reuses zone_bounce_history),
    and — if current_vol_ratio is given — compares this attempt's volume
    against the average of past rejection attempts, flagging if this one
    looks meaningfully stronger. Returns a formatted string or None.
    """
    best = None
    for hist in zone_bounce_history.values():
        if hist["symbol"] != symbol:
            continue
        if hist["low"] <= current_price:
            continue  # not above current price
        if (hist["low"] - current_price) / current_price > 0.15:
            continue  # too far away to be relevant right now
        if hist["invalid_count"] < 2:
            continue  # need at least 2 rejections to call this "repeated"
        if best is None or hist["low"] < best["low"]:
            best = hist

    if not best:
        return None

    lines = [f"🔴 Resistance above: {format_price(best['low'])}–{format_price(best['high'])} — rejected {best['invalid_count']}x before"]

    if current_vol_ratio is not None:
        past_vols = [o["vol_ratio"] for o in best.get("outcomes", [])
                     if o.get("outcome") == "invalidated" and o.get("vol_ratio") is not None]
        if past_vols:
            avg_past_vol = sum(past_vols) / len(past_vols)
            if current_vol_ratio > avg_past_vol * 1.3:
                lines.append(f"   💪 This attempt's volume ({current_vol_ratio:.1f}x) is meaningfully stronger "
                             f"than past rejection attempts (avg {avg_past_vol:.1f}x) — may break through this time")
            else:
                lines.append(f"   ⚠️ Volume ({current_vol_ratio:.1f}x) similar to/weaker than past rejection "
                             f"attempts (avg {avg_past_vol:.1f}x) — may reject again")

    return "\n".join(lines)

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

                # Note #6: on zone entry, do NOT message immediately —
                # start a multi-timeframe retest watch (5M/15M/30M/1H) and
                # only message My Setups once the retest completes/holds.
                # Include the resistance-rejection-history comparison
                # (note #3) if a repeatedly-rejected zone sits above.
                rejection_note = check_resistance_rejection_history(symbol, current_price)
                context_lines = [
                    f"🔲 Zone: {format_price(z_low)}–{format_price(z_high)} [{tf.upper()}]",
                    f"💰 Entered at: {format_price(current_price)}",
                ]
                if rejection_note:
                    context_lines.append(rejection_note)
                start_shared_retest_watch(
                    key=f"zone_{zone_id}_retest",
                    symbol=symbol, level=z_low, topic=TOPIC_MY_SETUPS,
                    header=f"🎯 <b>Zone Retest — {symbol}</b>",
                    context="\n".join(context_lines),
                )
            continue

        # ── in_zone ──
        if state == "in_zone":
            # If price has moved far above the zone since we entered it,
            # this zone is no longer the relevant level — move to post_confirm
            # silently so we stop generating misleading alerts from a stale zone.
            if current_price > z_high * 1.40:
                manual_zones[zone_id]["state"] = "post_confirm"
                manual_zones[zone_id]["confirmed"] = True
                manual_zones[zone_id]["confirmed_time"] = now
                manual_zones[zone_id]["went_up"] = True
                save_zones()
                print(f"⚠️ Zone {zone_id} auto-advanced to post_confirm: price already "
                      f"{((current_price/z_high)-1)*100:.0f}% above zone high")
                continue
            if current_price < zone.get("lowest_in_zone", z_low):
                manual_zones[zone_id]["lowest_in_zone"] = current_price

            # FIX (AWE case): the full confirm below only fires once the zone's
            # OWN timeframe (e.g. 4H) candle closes — for a fast vertical spike,
            # that can mean a multi-hour wait while price is already far above
            # the zone by the time confirm fires. This fast-spike check runs
            # independently of `tf`'s own candle cycle: if price has already
            # moved well past the zone with real volume behind it (checked on
            # 1H, falling back to 15M), it sends an early heads-up immediately —
            # the normal tf confirm still follows later as usual.
            if not zone.get("fast_spike_alerted"):
                pct_above_zone = (current_price - z_high) / z_high * 100 if z_high > 0 else 0
                if pct_above_zone >= 5:
                    fast_klines = get_klines(symbol, interval="1h", limit=8)
                    fast_tf_label = "1H"
                    if not fast_klines or len(fast_klines) < 6:
                        fast_klines = get_klines(symbol, interval="15m", limit=8)
                        fast_tf_label = "15M"
                    if fast_klines and len(fast_klines) >= 6:
                        fk_last = fast_klines[-2]
                        fk_open, fk_close = float(fk_last[1]), float(fk_last[4])
                        fk_vol = float(fk_last[5])
                        fk_prev_vols = [float(k[5]) for k in fast_klines[-6:-2]]
                        fk_avg_vol = sum(fk_prev_vols) / len(fk_prev_vols) if fk_prev_vols else 1
                        fk_vol_ratio = fk_vol / fk_avg_vol if fk_avg_vol > 0 else 0
                        if fk_close > fk_open and fk_vol_ratio >= 2.0:
                            manual_zones[zone_id]["fast_spike_alerted"] = True
                            save_zones()
                            send_to_topic(TOPIC_MY_SETUPS,
                                f"⚡ <b>FAST SPIKE — Zone Already Cleared [{fast_tf_label}]</b>\n\n"
                                f"🪙 <b>{symbol}</b> | {tf.upper()} OB\n"
                                f"🔲 Zone: {format_price(z_low)} — {format_price(z_high)}\n"
                                f"💰 Price: {format_price(current_price)} (+{pct_above_zone:.1f}% above zone high)\n"
                                f"⚡ Volume: {fk_vol_ratio:.1f}x on {fast_tf_label}\n\n"
                                f"⏳ Moving fast — the full {tf.upper()} confirmation may still take "
                                f"a while to close. This is an early heads-up so you're not caught off "
                                f"guard waiting for it.\n\n"
                                f"⚠️ <i>Check the chart before entry.</i>"
                            )
                            print(f"⚡ Fast spike alert: {zone_id}")

            klines_tf = get_klines(symbol, interval=tf, limit=15)
            if not klines_tf or len(klines_tf) < 10:
                continue

            last   = klines_tf[-2]
            l_open = float(last[1])
            l_close= float(last[4])
            l_high = float(last[2])
            l_low  = float(last[3])

            # UPGRADE (liquidity sweep, was: wick-rejection): the old check only
            # looked at the current candle's wick/body shape near the zone — any
            # long-lower-wick candle near the zone would fire, regardless of
            # whether there was an actual established low underneath it. A real
            # liquidity sweep needs a genuine sell-side liquidity pool: a swing
            # low that's been TESTED MULTIPLE TIMES (so stop-losses/limit orders
            # have realistically clustered there), then a sweep below it and a
            # reclaim back above — exactly the "down e liquidity thakle pump
            # ney" pattern. This runs on whichever tf the zone itself uses (1H
            # or 4H, both supported via /addzone).
            candle_range = l_high - l_low
            if candle_range > 0 and len(klines_tf) >= 10:
                lower_wick = min(l_open, l_close) - l_low
                body = abs(l_close - l_open)
                wick_dominant = lower_wick / candle_range >= 0.55
                small_body = body / candle_range <= 0.35
                near_zone = l_low <= z_high and l_low >= z_low * 0.97

                # Find a prior swing low in the lookback window (before this
                # candle) that's been touched 2+ times within a tight band —
                # that clustering is what makes it a real liquidity pool, not
                # just a random dip.
                lookback = klines_tf[-10:-2]  # 8 candles strictly before `last` (klines_tf[-2])
                swing_low = min(float(k[3]) for k in lookback) if lookback else None
                touches = sum(
                    1 for k in lookback
                    if swing_low and abs(float(k[3]) - swing_low) / swing_low <= 0.015
                ) if swing_low else 0
                established_liquidity = touches >= 2

                swept_below = swing_low is not None and l_low < swing_low * 0.998
                reclaimed = l_close > swing_low if swing_low else False

                # Volume on the reclaim candle vs recent average — a genuine
                # sweep+absorb should show real volume, not a thin wick on no volume.
                m_vol = float(last[5])
                prev_vols_sweep = [float(k[5]) for k in klines_tf[-8:-2]]
                avg_vol_sweep = sum(prev_vols_sweep) / len(prev_vols_sweep) if prev_vols_sweep else 1
                vol_ratio_sweep = m_vol / avg_vol_sweep if avg_vol_sweep > 0 else 0

                is_liquidity_sweep = (
                    established_liquidity and swept_below and reclaimed and
                    wick_dominant and vol_ratio_sweep >= 1.3
                )
                is_plain_wick_rejection = (
                    wick_dominant and small_body and near_zone and not is_liquidity_sweep
                )

                wick_key_candle = int(last[0])
                if is_liquidity_sweep and zone.get("last_wick_alert_candle") != wick_key_candle:
                    manual_zones[zone_id]["last_wick_alert_candle"] = wick_key_candle
                    save_zones()
                    full_confluence_liq = build_entry_decision_block(symbol, current_price, tf=tf if tf in ("1h","4h") else "4h")
                    send_to_topic(TOPIC_MY_SETUPS,
                        f"🩸 <b>LIQUIDITY SWEEP — {symbol} [{tf.upper()} OB]</b>\n\n"
                        f"🔲 Zone: {format_price(z_low)} — {format_price(z_high)}\n"
                        f"💰 Current: {format_price(current_price)}\n"
                        f"📍 Swept below {format_price(swing_low)} (tested {touches}x prior) "
                        f"and reclaimed it on the close\n"
                        f"⚡ Volume: {vol_ratio_sweep:.1f}x on the reclaim candle\n"
                        + (f"\n{full_confluence_liq}\n" if full_confluence_liq else "") +
                        f"\n💡 <i>Sell-side stops below that low likely got triggered and absorbed — "
                        f"this is the classic setup before a move up. Not a full zone confirmation "
                        f"yet, but a strong early signal.</i>\n\n"
                        f"⚠️ <i>Check the chart before entry.</i>"
                    )
                    print(f"🩸 Liquidity sweep: {zone_id}")
                elif is_plain_wick_rejection and zone.get("last_wick_alert_candle") != wick_key_candle:
                    manual_zones[zone_id]["last_wick_alert_candle"] = wick_key_candle
                    save_zones()
                    send_to(ADMIN_CHAT_ID,
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

                # Guard: if price is already >40% above the zone high, this zone
                # is stale — it was added when price was near the zone, but price
                # has since moved far away. A confirm at this distance is not
                # actionable and would just confuse (e.g. SYN zone at $0.17 when
                # price is already at $0.34 — "+98.9% from zone low").
                if current_price > z_high * 1.40:
                    print(f"⚠️ Zone {zone_id} skipped: price {format_price(current_price)} is "
                          f"+{recovery_pct:.0f}% above zone high {format_price(z_high)} — too far, zone is stale")
                    continue
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

                # Check for trendline liquidity sweep confluence
                full_confluence = build_entry_decision_block(symbol, current_price, tf=tf if tf in ("1h","4h") else "4h")

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
                    f"   {details_str}\n"
                    + (f"\n{full_confluence}\n" if full_confluence else "") +
                    f"\n🕐 {datetime.now().strftime('%H:%M:%S')}\n\n"
                    f"⚠️ <i>Check the chart before entry.</i>"
                )
                top_pick_signals = sum([
                    coiling_days >= 30,
                    bool(confluence_tag),
                    tf == "1d",
                    vol_ratio >= 3.0,
                ])
                is_top_pick = top_pick_signals >= 2

                # Route based on zone origin: auto-added (from High Priority)
                # zones' results go back to High Priority; user's manually
                # /addzone-added zones keep going to My Setups as before.
                zone_dest = TOPIC_BUILDUPS if zone.get("source") == "auto_high_priority" else TOPIC_MY_SETUPS
                send_to_topic(zone_dest, msg)
                if is_top_pick and zone_dest != TOPIC_BUILDUPS:
                    send_to_topic(TOPIC_BUILDUPS, msg)
                # Subscriber DMs
                for sub_chat_id in subscribers:
                    send_to(sub_chat_id, msg)
                print(f"🎯 Zone confirmed: {zone_id}{' [TOP PICK]' if is_top_pick else ''}")
                start_confirm_watch(symbol, current_price, zone_dest, "Zone Confirmed")
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
                klines_tf = get_klines(symbol, interval=tf, limit=10)
                if klines_tf and len(klines_tf) >= 4:
                    last   = klines_tf[-2]
                    l_open = float(last[1])
                    l_close= float(last[4])
                    l_vol  = float(last[5])
                    prior_vols = [float(k[5]) for k in klines_tf[-8:-2]]
                    avg_vol = sum(prior_vols) / len(prior_vols) if prior_vols else 1
                    inval_vol_ratio = l_vol / avg_vol if avg_vol > 0 else None
                    if l_close < z_low and l_close < l_open and not zone.get("post_invalid_sent"):
                        manual_zones[zone_id]["post_invalid_sent"] = True
                        manual_zones[zone_id]["state"] = "waiting"
                        save_zones()
                        record_zone_outcome(symbol, z_low, z_high, "invalidated", vol_ratio=inval_vol_ratio)
                        inv_msg = (
                            f"❌ <b>POST-CONFIRM INVALIDATED!</b>\n\n"
                            f"🪙 {symbol} | {tf.upper()} OB\n"
                            f"📈 Peak: {format_price(peak)}\n"
                            f"📉 Close: {format_price(l_close)} (below zone)\n\n"
                            f"⚠️ Went up, then broke back below the zone."
                        )
                        send_to_topic(TOPIC_BUILDUPS, inv_msg)
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
                        zone_dest = TOPIC_BUILDUPS if zone.get("source") == "auto_high_priority" else TOPIC_MY_SETUPS
                        for sub_chat_id in subscribers:
                            send_to(sub_chat_id, ret_msg)
                        send_to_topic(zone_dest, ret_msg)
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
                manual_zones[zone_id]["fast_spike_alerted"] = False
                save_zones()
            continue

    for zid in to_remove:
        manual_zones.pop(zid, None)
    if to_remove:
        save_zones()

# ─── MANUAL PRICE LINES (single-level break + retest) ──────
"""
/addline SYMBOL PRICE TIMEFRAME — for a single horizontal level you've drawn
on the chart yourself (resistance or support, not a zone range). The bot
tracks two stages on the given timeframe:

  waiting → break: a candle closes with a strong body above the line,
            with volume/buy-pressure confirmation (same criteria as
            manual zone confirm: vol_ratio >= 1.5, buy_ratio >= 0.52).
  break → retest: price comes back near the line and a green body candle
            closes back above it — this is the "retest confirmed" alert,
            sent to Top Picks + the requester's personal DM (same routing
            as /watch).

If price closes back below the line after a break without ever retesting
cleanly (i.e. drops well under it), the line is marked failed and removed
on the next check, so a single bad level doesn't sit around forever.
"""

def check_manual_lines():
    now = time.time()
    to_remove = []

    for line_id, line in list(manual_lines.items()):
        symbol  = line["symbol"]
        tf      = line["tf"]
        level   = line["price"]
        state   = line.get("state", "waiting")
        chat_id = line.get("chat_id")

        if now - line["added_time"] > 30 * 24 * 3600:
            to_remove.append(line_id)
            continue

        klines_tf = get_klines(symbol, interval=tf, limit=10)
        ticker = get_ticker(symbol)
        if not klines_tf or len(klines_tf) < 8 or not ticker:
            continue
        current_price = float(ticker["lastPrice"])

        last    = klines_tf[-2]
        l_open  = float(last[1])
        l_close = float(last[4])
        m_vol   = float(last[5])
        m_buy   = float(last[9])
        prev_vols = [float(k[5]) for k in klines_tf[-8:-2]]
        avg_vol = sum(prev_vols) / len(prev_vols) if prev_vols else 1
        vol_ratio = m_vol / avg_vol if avg_vol > 0 else 0
        buy_ratio = m_buy / m_vol if m_vol > 0 else 0
        candle_key = int(last[0])  # candle open-time, so each candle only triggers once per stage

        strong_break = (
            l_close > l_open and
            l_close > level and
            vol_ratio >= 1.5 and
            buy_ratio >= 0.52
        )

        # ── LIVE cross early alert (forming candle, before close) ──
        # FIX (BEL case): waiting for the candle to close before alerting meant
        # price was already far above the level by the time the message arrived.
        # This checks the FORMING candle (klines_tf[-1]) via current_price —
        # if price has already moved above the level with the forming candle
        # showing real body momentum, fire an immediate early heads-up so
        # monitoring can start right now, not at candle close.
        if state == "waiting" and not line.get("live_cross_alerted"):
            forming = klines_tf[-1]
            f_open = float(forming[1])
            f_vol  = float(forming[5])
            f_buy  = float(forming[9]) if len(forming) > 9 else f_vol * 0.5
            f_vol_ratio = f_vol / avg_vol if avg_vol > 0 else 0
            f_buy_ratio = f_buy / f_vol if f_vol > 0 else 0
            # FIX (note #5 backlog): volume/buy-pressure condition was still
            # gated behind the 1h klines cache (120s TTL), so by the time
            # f_vol_ratio caught up the price had often already moved well
            # past the level (e.g. VANAUSDT: level $1.250, alert at $1.265).
            # User wants this to fire on price alone — they'll check volume
            # themselves via /entry right after getting the alert.
            live_body_above = current_price > level * 1.001  # just 0.1% above level
            if live_body_above:
                manual_lines[line_id]["live_cross_alerted"] = True
                save_manual_lines()
                live_msg = (
                    f"⚡ <b>LINE CROSSED (Live) — {symbol} [{tf.upper()}]</b>\n\n"
                    f"💰 Price: {format_price(current_price)}\n"
                    f"📍 Level: {format_price(level)}\n"
                    f"⚡ Volume: {f_vol_ratio:.1f}x | Buy: {f_buy_ratio*100:.0f}% "
                    f"<i>(may lag live price slightly — check /entry for fresh volume)</i>\n\n"
                    f"🕐 Forming candle just crossed {format_price(level)} — "
                    f"check the chart NOW if you want to enter early. "
                    f"A confirmed break alert follows when the candle closes.\n\n"
                    f"⚠️ <i>Candle not closed yet — use this to get ready, not as full confirmation.</i>"
                )
                send_to_topic(TOPIC_MY_SETUPS, live_msg)
                if chat_id:
                    send_to(chat_id, live_msg)
                print(f"⚡ Line live cross: {line_id} @ {format_price(current_price)}")

        # ── waiting → break (closed candle confirm) ──
        if state == "waiting":
            if strong_break and line.get("last_break_candle") != candle_key:
                manual_lines[line_id]["state"] = "broken"
                manual_lines[line_id]["break_price"] = l_close
                manual_lines[line_id]["break_time"] = now
                manual_lines[line_id]["last_break_candle"] = candle_key
                manual_lines[line_id]["lowest_since_break"] = l_close
                manual_lines[line_id]["live_cross_alerted"] = False  # reset for next level
                save_manual_lines()
                full_confluence = build_entry_decision_block(symbol, current_price, tf=tf if tf in ("1h","4h") else "1h")
                msg = (
                    f"📏 <b>Line Break Confirmed — {symbol} [{tf.upper()}]</b>\n\n"
                    f"💰 Price: {format_price(current_price)}\n"
                    f"📍 Level: {format_price(level)}\n"
                    f"⚡ Volume: {vol_ratio:.1f}x | Buy: {buy_ratio*100:.0f}%\n\n"
                    f"✅ Candle closed above {format_price(level)} with body confirmation.\n"
                    f"⏳ Watching for a retest now — you'll get another alert "
                    f"if/when it confirms.\n"
                    + (f"\n{full_confluence}\n" if full_confluence else "") +
                    f"\n⚠️ <i>Confirm on the chart before treating this as actionable.</i>"
                )
                send_to_topic(TOPIC_MY_SETUPS, msg)
                if chat_id:
                    send_to(chat_id, msg)
                print(f"📏 Line break confirmed: {line_id}")
                start_confirm_watch(symbol, current_price, TOPIC_MY_SETUPS, "Line Break Confirmed")

                # User's specific scenario: after a market-wide down move, a
                # coin repeatedly touches/rejects at a resistance, then goes
                # sideways there — user manually marks that as a 1H line.
                # A green-body break of THAT line gets elevated importance:
                # skip the normal score>=4 confluence gate and feed straight
                # into the Big Pump pipeline (with an approximate % target),
                # so this specific setup can't be missed.
                if tf == "1h":
                    send_big_pump_alert(symbol, current_price, "Manual Line Break [1H]")
            continue

        # ── broken → retest confirmed / failed ──
        if state == "broken":
            if current_price < line.get("lowest_since_break", level):
                manual_lines[line_id]["lowest_since_break"] = current_price

            # Failure: closes meaningfully back below the line after breaking it
            if l_close < level * 0.97 and l_close < l_open:
                to_remove.append(line_id)
                if chat_id:
                    send_to(chat_id,
                        f"⚠️ <b>Line Invalidated — {symbol} [{tf.upper()}]</b>\n\n"
                        f"Price broke {format_price(level)} but has now closed back "
                        f"below it — treating this level as invalidated and removing "
                        f"the watch.\n\n"
                        f"Use /addline again if you want to re-mark it."
                    )
                print(f"📏 Line failed: {line_id}")
                continue

            near_level = abs(current_price - level) / level <= 0.05
            # FIX (note #8, same issue as LINE CROSSED Live): vol_ratio/buy_ratio
            # here come from the 1h-cached klines (up to 120s stale), causing
            # this to fire late relative to live price. Fire on price action
            # alone — user checks volume themselves via /entry afterward.
            retest_confirmed = (
                near_level and
                l_close > l_open and
                l_close > level and
                line.get("last_retest_candle") != candle_key
            )

            if retest_confirmed:
                manual_lines[line_id]["last_retest_candle"] = candle_key
                manual_lines[line_id]["state"] = "followup"
                manual_lines[line_id]["followup_candles_checked"] = 0
                manual_lines[line_id]["last_checked_candle"] = candle_key
                save_manual_lines()

                suggestion, strength_details = analyze_move_strength(symbol, current_price)
                is_distribution_flagged = any("Distribution risk" in d for d in strength_details)
                full_confluence_line = build_entry_decision_block(symbol, current_price, tf=tf if tf in ("1h","4h") else "1h")

                intro_line = (
                    f"⚠️ Price broke {format_price(level)} — distribution risk detected, see analysis below."
                    if is_distribution_flagged else
                    f"✅ Price broke {format_price(level)}, retested, and just closed "
                    f"back above it with a strong green candle. Continuation looks favorable."
                )

                if is_distribution_flagged:
                    # Distribution risk — send to admin DM only, not My Setups
                    # Bot will continue monitoring — if volume confirms pump, alert will follow
                    send_to(chat_id if chat_id else ADMIN_CHAT_ID,
                        f"⚠️ <b>Distribution Risk — {symbol} [{tf.upper()}]</b>\n\n"
                        f"💰 Price: {format_price(current_price)} | Level: {format_price(level)}\n\n"
                        f"🚨 Possible fake breakout — price broke level but volume pattern suggests distribution.\n"
                        f"Monitoring for volume confirmation before alerting.\n\n"
                        f"💡 Wait for strong green candle with 3x+ volume before entry."
                    )
                    print(f"📏 Line retest DISTRIBUTION RISK (admin only): {line_id}")
                else:
                    msg = (
                        f"🔥 <b>Line Retest Complete — {symbol} [{tf.upper()}]</b>\n\n"
                        f"💰 Price: {format_price(current_price)}\n"
                        f"📍 Level: {format_price(level)}\n\n"
                        f"✅ Price broke {format_price(level)}, retested, and just closed "
                        f"back above it with a strong green candle. Continuation looks favorable.\n\n"
                        + (f"{full_confluence_line}\n\n" if full_confluence_line else "") +
                        f"{suggestion}\n\n"
                        f"⏳ <i>Tracking the next 3 candles to confirm this holds — you'll get a follow-up.</i>"
                    )
                    send_to_topic(TOPIC_MY_SETUPS, msg)
                    if chat_id:
                        send_to(chat_id, msg)
                    print(f"📏 Line retest confirmed: {line_id}")
            continue

        # ── followup: confirm already fired, check whether it actually holds ──
        if state == "followup":
            last_candle_key = candle_key
            if line.get("last_checked_candle") == last_candle_key:
                continue  # already evaluated this candle close

            manual_lines[line_id]["last_checked_candle"] = last_candle_key
            candles_checked = line.get("followup_candles_checked", 0) + 1
            manual_lines[line_id]["followup_candles_checked"] = candles_checked

            if l_close < level * 0.98:
                to_remove.append(line_id)
                if chat_id:
                    send_to(chat_id,
                        f"⚠️ <b>{symbol} retest gave back the breakout [{tf.upper()}]</b>\n\n"
                        f"The level held for the confirming candle, but price has now closed "
                        f"back below {format_price(level)} — the continuation didn't hold. "
                        f"Treat the earlier confirmation as invalidated."
                    )
                print(f"📏 Line followup FAILED: {line_id}")
            elif candles_checked >= 3:
                to_remove.append(line_id)
                if chat_id:
                    send_to(chat_id,
                        f"✅ <b>{symbol} retest held [{tf.upper()}]</b>\n\n"
                        f"3 candles since the confirmation and price is still holding above "
                        f"{format_price(level)} (currently {format_price(current_price)}). "
                        f"The breakout looks genuine so far — still confirm on the chart and "
                        f"manage your own risk."
                    )
                print(f"📏 Line followup HELD: {line_id}")
            else:
                save_manual_lines()

    for lid in to_remove:
        manual_lines.pop(lid, None)
    if to_remove:
        save_manual_lines()


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
            sl_pnl_pct = (current_price - entry) / entry * 100
            send_to_topic(TOPIC_TRADES,
                f"🛑 <b>SL HIT</b>\n\n🪙 {symbol}\n💰 Entry: {format_price(entry)} → SL: {format_price(sl)}\n"
                f"📉 Current: {format_price(current_price)} ({sl_pnl_pct:+.1f}%)\n\n<i>Trade auto-closed from monitor.</i>"
            )
            to_close.append(trade_id)
            continue

        hit_tps = trade.get("hit_tps", [])
        final_tp_hit = False
        for i, tp in enumerate(tps):
            tp_label = f"tp{i+1}"
            if current_price >= tp and tp_label not in hit_tps:
                hit_tps.append(tp_label)
                active_trades[trade_id]["hit_tps"] = hit_tps
                save_active_trades()
                tp_pnl_pct = (current_price - entry) / entry * 100
                send_to_topic(TOPIC_TRADES,
                    f"✅ <b>TP{i+1} HIT!</b>\n\n🪙 {symbol}\n🎯 Target: {format_price(tp)}\n"
                    f"💰 Current: {format_price(current_price)} ({tp_pnl_pct:+.1f}%)\n\n<i>Consider partial close / trail SL.</i>"
                )
                # Note #5 part 3: auto-remove once the FINAL TP is hit
                if len(hit_tps) >= len(tps) and tps:
                    send_to_topic(TOPIC_TRADES,
                        f"🏁 <b>Final TP reached — {symbol}</b>\n\n"
                        f"All targets hit ({tp_pnl_pct:+.1f}% from entry). Removing from Trade Monitor."
                    )
                    to_close.append(trade_id)
                    final_tp_hit = True
        if final_tp_hit:
            continue

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

        # Note #5 part 1: only actually message the user at real conviction
        # (WEAKENING or HIGH). The lowest "Early Caution — just monitor, no
        # action needed" tier used to fire too, which is exactly the kind of
        # low-value repeat notification the user doesn't want — score/trend
        # is still tracked internally every cycle (last_score above), it's
        # just silent until it's actually worth telling the user about.
        if score >= TRADE_SCORE_WEAKENING and crossed_up_threshold:
            trade_alert_cooldown[trade_id] = now
            pnl_pct = (current_price - entry) / entry * 100
            details_str = "\n   ".join(details)
            if score >= TRADE_SCORE_HIGH:
                header = "🔴 <b>HIGH PRIORITY — Trend Reversal Risk</b>"
                footer = "<i>⚠️ Strongly consider exiting or tightening SL now.</i>"
            else:
                header = "🔶 <b>Trend Weakening</b>"
                footer = "<i>Consider tightening SL to reduce risk.</i>"

            send_to_topic(TOPIC_TRADES,
                f"{header}\n\n"
                f"🪙 <b>{symbol}</b> | {tf.upper()}\n"
                f"💰 Entry: {format_price(entry)} | Current: {format_price(current_price)} ({pnl_pct:+.1f}%)\n"
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


def check_scalp_trades():
    """
    Separate, fast, tight-SL monitor for scalp trades (_scalp_trades —
    distinct from active_trades, checked more frequently for quick updates).

    Also detects when a scalp setup looks like it could go much BIGGER than
    the scalp TP1 (ALLOUSDT-style 20-50% moves in a few hours): if volume/
    momentum stays strong after entry, removes the fixed TP (tells the user
    to watch the chart manually for a higher exit) and switches to actively
    monitoring for downside risk instead of a rigid scalp target — user is
    fine waiting 4-12h for a bigger opportunity like this.
    """
    now = time.time()
    to_remove = []
    for trade_id, trade in list(_scalp_trades.items()):
        if trade.get("closed"):
            to_remove.append(trade_id)
            continue
        if now - trade["started"] > 16 * 3600:
            to_remove.append(trade_id)
            continue
        symbol = trade["symbol"]
        try:
            ticker = get_ticker(symbol)
            if not ticker:
                continue
            current_price = float(ticker["lastPrice"])
            entry, sl, tp1 = trade["entry"], trade["sl"], trade["tp1"]
            pnl_pct = (current_price - entry) / entry * 100 if entry > 0 else 0

            if current_price <= sl:
                send_to_topic(TOPIC_BIG_PUMP,
                    f"🛑 <b>SCALP SL HIT — {symbol}</b>\n\n"
                    f"💰 Entry: {format_price(entry)} → SL: {format_price(sl)} ({pnl_pct:+.1f}%)\n"
                    f"<i>Removed from scalp monitor.</i>"
                )
                trade["closed"] = True
                to_remove.append(trade_id)
                continue

            if not trade["tp_removed"] and not trade.get("tp1_hit") and current_price >= tp1:
                trade["tp1_hit"] = True
                send_to_topic(TOPIC_BIG_PUMP,
                    f"✅ <b>SCALP TP1 HIT — {symbol}</b>\n\n"
                    f"💰 Entry: {format_price(entry)} → Now: {format_price(current_price)} ({pnl_pct:+.1f}%)\n"
                    f"<i>Target reached — consider taking profit.</i>"
                )
                trade["closed"] = True
                to_remove.append(trade_id)
                continue

            # "Bigger opportunity" override — only checked before TP1 fills,
            # only fires once per trade.
            if not trade["tp_removed"] and pnl_pct >= 3.0:
                klines_5m_bp = get_klines(symbol, interval="5m", limit=10)
                if klines_5m_bp and len(klines_5m_bp) >= 8:
                    closed_bp = klines_5m_bp[:-1]
                    last_bp = closed_bp[-1]
                    lv = float(last_bp[5])
                    lb = float(last_bp[9]) if len(last_bp) > 9 else lv * 0.5
                    buy_ratio_bp = lb / lv if lv > 0 else 0.5
                    prior_vols_bp = [float(k[5]) for k in closed_bp[-7:-1]]
                    avg_vol_bp = sum(prior_vols_bp) / len(prior_vols_bp) if prior_vols_bp else 1
                    vol_ratio_bp = lv / avg_vol_bp if avg_vol_bp > 0 else 0

                    if vol_ratio_bp >= 3.0 and buy_ratio_bp >= 0.60:
                        trade["tp_removed"] = True
                        send_to_topic(TOPIC_BIG_PUMP,
                            f"🚀 <b>BIGGER OPPORTUNITY — {symbol}</b>\n\n"
                            f"💰 Entry: {format_price(entry)} → Now: {format_price(current_price)} ({pnl_pct:+.1f}%)\n"
                            f"⚡ Volume still strong: {vol_ratio_bp:.1f}x | Buy: {buy_ratio_bp*100:.0f}%\n\n"
                            f"💡 This may run well past the scalp TP1 ({format_price(tp1)}) — "
                            f"removed the fixed target. Watch the chart yourself for a higher exit.\n"
                            f"🔍 Bot will keep monitoring and notify if this starts turning down.\n"
                            f"🕐 {datetime.now().strftime('%H:%M:%S')}"
                        )
                        print(f"🚀 Scalp → bigger opportunity: {symbol}")

            # After the TP is removed, actively watch for downside risk
            if trade["tp_removed"]:
                warning = check_2m_reversal_structure(symbol)
                if warning and now - trade.get("last_downside_alert", 0) > 1800:
                    trade["last_downside_alert"] = now
                    send_to_topic(TOPIC_BIG_PUMP,
                        f"⚠️ <b>Downside Risk — {symbol}</b>\n\n"
                        f"💰 Now: {format_price(current_price)} ({pnl_pct:+.1f}% from entry)\n"
                        f"   {warning}\n\n"
                        f"💡 Consider securing profit — the bigger move may be losing steam.\n"
                        f"🕐 {datetime.now().strftime('%H:%M:%S')}"
                    )
        except Exception as e:
            print(f"Scalp trade check error {symbol}: {e}")
    for t in to_remove:
        _scalp_trades.pop(t, None)


def check_2m_reversal_structure(symbol):
    """
    Note #8 (Trade Monitor 2M enhancement): watches SEVERAL 2M candles (not
    a single one) for early reversal signs — volume in/out flow shifting
    from buying to selling, plus SMC structure: CHoCH (Change of Character)
    and LH (Lower High) formation. Requires at least 2 of these 3 signals
    together to reduce noise. Returns a warning string or None.
    """
    klines_2m = get_klines(symbol, interval="2m", limit=20)
    if not klines_2m or len(klines_2m) < 15:
        return None
    closed = klines_2m[:-1]
    recent = closed[-12:]
    if len(recent) < 10:
        return None

    highs = [float(k[2]) for k in recent]
    lows  = [float(k[3]) for k in recent]
    closes = [float(k[4]) for k in recent]
    vols  = [float(k[5]) for k in recent]
    buys  = [float(k[9]) if len(k) > 9 else float(k[5]) * 0.5 for k in recent]

    # Volume in/out flow: buy-dominant first half vs sell-dominant second half
    half = len(recent) // 2
    first_half_net  = sum(buys[:half]) - sum(vols[i] - buys[i] for i in range(half))
    second_half_net = sum(buys[half:]) - sum(vols[i] - buys[i] for i in range(half, len(recent)))
    volume_flow_reversing = first_half_net > 0 and second_half_net < first_half_net * -0.5

    # Swing highs/lows for CHoCH / LH
    swing_highs = [(i, highs[i]) for i in range(1, len(highs) - 1) if highs[i] > highs[i-1] and highs[i] > highs[i+1]]
    swing_lows  = [(i, lows[i])  for i in range(1, len(lows) - 1)  if lows[i]  < lows[i-1]  and lows[i]  < lows[i+1]]

    lh_detected = len(swing_highs) >= 2 and swing_highs[-1][1] < swing_highs[-2][1]

    choch_detected = False
    if len(swing_lows) >= 2:
        last_low, prev_low = swing_lows[-1][1], swing_lows[-2][1]
        # Was making higher lows (uptrend structure), then closed below the
        # most recent one — a break of structure (CHoCH)
        if last_low > prev_low and closes[-1] < last_low:
            choch_detected = True

    warnings = []
    if volume_flow_reversing:
        warnings.append("📉 2M volume flow reversing: buy pressure fading, sell volume increasing")
    if lh_detected:
        warnings.append("📉 2M Lower High (LH) forming — momentum weakening")
    if choch_detected:
        warnings.append("🔴 2M CHoCH (Change of Character) — structure just broke bearish")

    if len(warnings) >= 2:  # require 2+ confirming signals to keep this from being noisy
        return "\n   ".join(warnings)
    return None


def check_active_trades_fast():
    """
    Fast trade monitor — runs every 60s on 5M/15M/1H.
    - 4h cooldown for routine warnings (no more spam)
    - Urgent alert if price near SL (within 2%)
    - TP1 hit → suggest moving SL to breakeven
    - Clear HOLD / EXIT verdict
    """
    now = time.time()
    for trade_id, trade in list(active_trades.items()):
        symbol  = trade["symbol"]
        entry   = trade["entry"]
        sl      = trade["sl"]
        tp1     = trade.get("tp1", entry * 1.05)

        ticker = get_ticker(symbol)
        if not ticker:
            continue
        current_price = float(ticker["lastPrice"])
        pnl_pct = (current_price - entry) / entry * 100
        sl_dist = (current_price - sl) / current_price * 100

        alerts = []
        is_urgent = False

        # ── Bigger pump potential: strong volume still coming in after a
        # meaningful gain — heads-up that this could run well past the
        # planned TP, so the user can watch the chart for a higher exit
        # instead of just taking the fixed target. Independent of the other
        # checks below (own 2h re-notify cooldown, since the move may keep extending).
        if pnl_pct >= 10.0 and now - trade.get("big_pump_watch_alerted", 0) > 2 * 3600:
            klines_1h_bp = get_klines(symbol, interval="1h", limit=10)
            if klines_1h_bp and len(klines_1h_bp) >= 5:
                closed_1h_bp = klines_1h_bp[:-1]
                vols_1h_bp = [float(k[5]) for k in closed_1h_bp[-5:]]
                avg_vol_1h_bp = sum(vols_1h_bp[:-1]) / max(1, len(vols_1h_bp) - 1)
                last_vol_1h_bp = vols_1h_bp[-1]
                vol_ratio_bp = last_vol_1h_bp / avg_vol_1h_bp if avg_vol_1h_bp > 0 else 0
                last_candle_bp = closed_1h_bp[-1]
                buy_vol_bp = float(last_candle_bp[9]) if len(last_candle_bp) > 9 else last_vol_1h_bp * 0.5
                buy_ratio_bp = buy_vol_bp / last_vol_1h_bp if last_vol_1h_bp > 0 else 0.5
                if vol_ratio_bp >= 2.0 and buy_ratio_bp >= 0.55:
                    active_trades[trade_id]["big_pump_watch_alerted"] = now
                    send_to_topic(TOPIC_TRADES,
                        f"🚀 <b>Bigger Pump Potential — {symbol}</b>\n\n"
                        f"💰 Entry: {format_price(entry)} → Now: {format_price(current_price)} ({pnl_pct:+.1f}%)\n"
                        f"⚡ Volume still elevated: {vol_ratio_bp:.1f}x | Buy: {buy_ratio_bp*100:.0f}%\n\n"
                        f"💡 This may extend well past your planned TP — keep an eye on the chart, "
                        f"consider holding part of the position for a higher exit instead of taking full profit here.\n"
                        f"🕐 {datetime.now().strftime('%H:%M:%S')}"
                    )
                    print(f"🚀 Big pump potential (trade): {symbol} +{pnl_pct:.1f}% vol={vol_ratio_bp:.1f}x")

                    # Note #11: also judge whether ADDING to the position
                    # (re-entry) makes sense here, not just holding — daily
                    # trend, sustained higher lows, and genuine room to a
                    # further resistance before calling it favorable.
                    try:
                        klines_4h_re = get_klines(symbol, interval="4h", limit=40)
                        re_ok = True
                        re_reasons = []
                        if is_daily_downtrend(symbol, current_price):
                            re_ok = False
                            re_reasons.append("❌ Daily trend still bearish")
                        else:
                            re_reasons.append("✅ Daily trend bullish/neutral")

                        lows_1h_re = [float(k[3]) for k in closed_1h_bp[-6:]]
                        hl_re = sum(1 for j in range(1, len(lows_1h_re)) if lows_1h_re[j] > lows_1h_re[j-1]) >= 3
                        if hl_re:
                            re_reasons.append("✅ Higher lows sustained (1H)")
                        else:
                            re_ok = False
                            re_reasons.append("⚠️ Higher lows not clearly sustained")

                        room_pct = None
                        if klines_4h_re and len(klines_4h_re) >= 10:
                            closed_4h_re = klines_4h_re[:-1]
                            highs_above = [float(k[2]) for k in closed_4h_re[-30:] if float(k[2]) > current_price * 1.03]
                            if highs_above:
                                nearest_res_re = min(highs_above)
                                room_pct = (nearest_res_re - current_price) / current_price * 100
                                if room_pct >= 8.0:
                                    re_reasons.append(f"✅ Room to next resistance: +{room_pct:.1f}%")
                                else:
                                    re_ok = False
                                    re_reasons.append(f"⚠️ Only +{room_pct:.1f}% room before next resistance")

                        if re_ok:
                            send_to_topic(TOPIC_MY_SETUPS,
                                f"💰 <b>Re-Entry Worth Considering — {symbol}</b>\n\n"
                                f"Following up on the Bigger Pump Potential alert — conditions still "
                                f"look favorable to add to the position:\n"
                                + "\n".join(f"   {r}" for r in re_reasons) +
                                f"\n\n💰 Current: {format_price(current_price)} ({pnl_pct:+.1f}% from original entry)\n"
                                f"⚠️ <i>Confirm on chart before adding — this is not guaranteed.</i>"
                            )
                            print(f"💰 Re-entry suggested: {symbol}")
                    except Exception as e:
                        print(f"Re-entry analysis error {symbol}: {e}")

        # ── URGENT: price near SL (within 2%) ──
        if sl_dist <= 2.0 and sl_dist > 0:
            if now - trade.get("last_sl_alert", 0) > 30 * 60:  # 30min cooldown for SL alerts
                active_trades[trade_id]["last_sl_alert"] = now
                alerts.append(f"🚨 URGENT: Price {format_price(current_price)} only {sl_dist:.1f}% above SL {format_price(sl)}")
                alerts.append(f"   → Consider exiting now to protect capital")
                is_urgent = True

        # ── TP1 hit → auto-move SL to breakeven (note #9) ──
        elif current_price >= tp1 and not trade.get("tp1_hit_alerted"):
            active_trades[trade_id]["tp1_hit_alerted"] = True
            old_sl = sl
            active_trades[trade_id]["sl"] = entry  # risk-free on the remaining position
            sl = entry
            sl_dist = (current_price - sl) / current_price * 100  # recalc for the updated SL
            save_active_trades()
            alerts.append(f"🎯 TP1 hit! (+{pnl_pct:.1f}%)")
            alerts.append(f"   → SL auto-moved to breakeven: {format_price(old_sl)} → {format_price(entry)} (risk-free on the rest)")
            alerts.append(f"   → Remaining position rides toward TP2")

        # ── Routine checks (4h cooldown) ──
        elif now - trade.get("last_fast_alert", 0) > 4 * 3600:
            klines_5m = get_klines(symbol, interval="5m", limit=20)
            if klines_5m and len(klines_5m) >= 10:
                closed_5m = klines_5m[:-1]
                highs_5m = [float(k[2]) for k in closed_5m[-6:]]
                lows_5m  = [float(k[3]) for k in closed_5m[-6:]]
                lower_highs = all(highs_5m[i] < highs_5m[i-1] for i in range(1, len(highs_5m)))
                lower_lows  = all(lows_5m[i] < lows_5m[i-1] for i in range(1, len(lows_5m)))
                vols_5m = [float(k[5]) for k in closed_5m[-6:]]
                avg_vol_5m = sum(vols_5m[:3]) / 3 if vols_5m else 1
                sell_spike_5m = vols_5m[-1] > avg_vol_5m * 3 and float(closed_5m[-1][4]) < float(closed_5m[-1][1])
                if lower_highs and lower_lows:
                    alerts.append("📉 5M lower highs + lower lows — short-term momentum shifting")
                if sell_spike_5m:
                    alerts.append(f"⚡ 5M sell spike ({vols_5m[-1]/avg_vol_5m:.1f}x) — watch closely")

            klines_1h = get_klines(symbol, interval="1h", limit=10)
            if klines_1h and len(klines_1h) >= 5:
                closed_1h = klines_1h[:-1]
                closes_1h = [float(k[4]) for k in closed_1h]
                ema20_1h = calculate_ema(closes_1h, min(20, len(closes_1h)))
                below_ema = ema20_1h and current_price < ema20_1h
                vols_1h = [float(k[5]) for k in closed_1h[-5:]]
                avg_vol_1h = sum(vols_1h[:3]) / 3 if vols_1h else 1
                sell_spike_1h = vols_1h[-1] > avg_vol_1h * 2.5 and float(closed_1h[-1][4]) < float(closed_1h[-1][1])
                if below_ema:
                    alerts.append(f"🔴 1H below 20EMA ({format_price(ema20_1h)}) — trend weakening")
                if sell_spike_1h:
                    alerts.append(f"⚡ 1H sell spike ({vols_1h[-1]/avg_vol_1h:.1f}x) — distribution risk")

            # Note #8: 2M multi-candle volume flow + SMC structure (CHoCH/LH)
            two_m_warning = check_2m_reversal_structure(symbol)
            if two_m_warning:
                alerts.append(two_m_warning)

        if not alerts:
            continue

        # Build verdict
        # FIX: a single 5M sell spike alone is too short-term/noisy to drive
        # an exit recommendation on a multi-day hold (KATUSDT case: exited on
        # a lone 4.3x 5M spike, then price recovered). It still shows in the
        # message as an FYI, but only the more sustained signals (lower
        # highs+lows over 6 candles, 1H below EMA, 1H sell spike) count
        # toward the "negative/consider exiting" classification.
        negative = any(x in " ".join(alerts) for x in ["📉","🔴","⚡ 1H sell","URGENT"])
        if is_urgent:
            verdict = "🚨 EXIT or tighten SL immediately"
        elif negative and pnl_pct < 0:
            verdict = "⚠️ Consider exiting — negative PnL + weakening structure"
        elif negative and pnl_pct > 3:
            verdict = "💡 Still in profit — consider partial exit or tighten SL"
        elif negative:
            verdict = "⚠️ Monitor closely — structure weakening"
        else:
            verdict = "✅ Hold — structure intact"

        if not is_urgent:
            active_trades[trade_id]["last_fast_alert"] = now
        save_active_trades()

        alert_str = "\n".join(f"  {a}" for a in alerts)
        emoji = "🚨" if is_urgent else "⚠️" if negative else "ℹ️"
        send_to_topic(TOPIC_TRADES,
            f"{emoji} <b>Trade Update — {symbol}</b>\n\n"
            f"💰 Entry: {format_price(entry)} → Now: {format_price(current_price)} ({pnl_pct:+.1f}%)\n"
            f"🛑 SL: {format_price(sl)} ({sl_dist:.1f}% away)\n\n"
            f"{alert_str}\n\n"
            f"📊 Verdict: {verdict}\n"
            f"🕐 {datetime.now().strftime('%H:%M:%S')}"
        )
        print(f"{emoji} Trade monitor: {symbol} — {verdict}")


def check_no_retest_pump_risk(symbol, tf="1h"):
    """
    Early-warning detector for coins likely to pump 10-60%+ WITHOUT ever
    giving a retest (unlike check_explosive_pump, this doesn't require the
    move to already be 3 candles deep — it looks at the breakout candle
    itself: body strength, abnormal volume vs baseline, and buy pressure).
    Fires to Top Picks, then feeds the same instant-analysis promotion
    pipeline (check_high_confidence_signal) that can escalate it to High
    Priority as ⭐ HIGH CONFIDENCE if the broader confluence also checks out.
    Also alerts My Setups directly if the user has manually /addzone-tracked
    this coin (their own watched setups shouldn't get buried in Top Picks).

    Note #10: runs on BOTH 1H and 4H (tf param) — a move concentrated across
    a single 4H candle (like ONDOUSDT) could slip through 1H-only checking.
    """
    now = time.time()
    key = f"{symbol}_noretest_pump_{tf}"
    cooldown = 4 * 3600 if tf == "1h" else 8 * 3600
    if now - alerted_coins.get(key, 0) < cooldown:
        return

    klines = get_klines(symbol, interval=tf, limit=15)
    if not klines or len(klines) < 10:
        return

    closed = klines[:-1]
    last = closed[-1]
    l_open, l_close = float(last[1]), float(last[4])
    l_vol = float(last[5])
    l_buy = float(last[9]) if len(last) > 9 else l_vol * 0.5

    if l_close <= l_open:
        return  # must be a bullish breakout candle

    if l_open <= 0:
        return  # malformed candle data — avoid division by zero
    body_pct = (l_close - l_open) / l_open * 100
    body_min = 3.0 if tf == "1h" else 5.0  # 4H candles naturally have bigger bodies
    if body_pct < body_min:
        return  # not a strong enough breakout candle

    prev_vols = [float(k[5]) for k in closed[-8:-1]]
    avg_vol = sum(prev_vols) / len(prev_vols) if prev_vols else 1
    vol_ratio = l_vol / avg_vol if avg_vol > 0 else 0
    if vol_ratio < 5.0:
        return  # lower bar than check_explosive_pump's 10x — this fires earlier/faster

    buy_ratio = l_buy / l_vol if l_vol > 0 else 0
    if buy_ratio < 0.62:
        return  # must be strongly buy-dominant

    ticker = get_ticker(symbol)
    if not ticker:
        return
    current_price = float(ticker["lastPrice"])
    change_24h = float(ticker["priceChangePercent"])
    if change_24h < 0:
        return

    # Still reasonably near the breakout candle's close (not already dumped)
    if current_price < l_close * 0.95:
        return

    alerted_coins[key] = now

    msg = (
        f"⚠️ <b>PUMP RISK — NO RETEST YET [{tf.upper()}]</b>\n\n"
        f"🪙 <b>{symbol}</b>\n"
        f"💰 Price: {format_price(current_price)}\n"
        f"📊 24h: {change_24h:+.2f}%\n"
        f"🕯 Breakout candle: +{body_pct:.1f}% body, {vol_ratio:.1f}x volume\n"
        f"🟢 Buy pressure: {buy_ratio*100:.0f}%\n"
        f"🕐 {datetime.now().strftime('%H:%M:%S')}\n\n"
        f"🚀 High probability of a 10-60%+ extended pump WITHOUT a retest.\n"
        f"💡 Consider entry now based on volume/BS strength — waiting for a retest may mean missing this one.\n\n"
        f"⚠️ <i>Confirm on chart before entry. Use stop-loss.</i>"
    )
    send_to_topic(TOPIC_BUILDUPS, msg)

    # If the user has manually /addzone-tracked this coin, they're already
    # watching it personally — also alert My Setups so it doesn't get lost
    # among Building Momentum's broader coin coverage.
    has_manual_zone = any(
        z.get("symbol") == symbol and z.get("source", "manual") == "manual"
        for z in manual_zones.values()
    )
    if has_manual_zone:
        send_to_topic(TOPIC_MY_SETUPS, msg)

    sig_label = f"No-Retest Pump Risk [{tf.upper()}]"
    track_building_signal(symbol, sig_label, current_price)
    check_high_confidence_signal(symbol, sig_label, current_price)
    print(f"⚠️ No-retest pump risk [{tf.upper()}]: {symbol} body={body_pct:.1f}% vol={vol_ratio:.1f}x buy={buy_ratio*100:.0f}%"
          + (" [manual zone → My Setups]" if has_manual_zone else ""))

    # Guaranteed High Priority bypass for extreme moves (note #7 from prior session)
    if vol_ratio >= 20.0 or body_pct >= 15.0:
        send_extreme_pump_alert(symbol, current_price, body_pct, vol_ratio, buy_ratio, sig_label)


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

    # Note #7: faster path — check the STILL-FORMING candle (klines[-1],
    # not yet closed) for extreme volume already accumulating. GUSDT-style
    # 974x-volume events shouldn't need to wait for check_explosive_pump's
    # normal 3-fully-closed-candle requirement — this catches them mid-
    # candle instead. Deliberately a MUCH higher bar (15x+) than the normal
    # path since it's acting on incomplete data.
    fast_key = f"{symbol}_explosive_fast"
    if now - alerted_coins.get(fast_key, 0) >= 1800:
        forming = klines[-1]
        f_open, f_close = float(forming[1]), float(forming[4])
        f_vol = float(forming[5])
        f_buy = float(forming[9]) if len(forming) > 9 else f_vol * 0.5
        f_buy_ratio = f_buy / f_vol if f_vol > 0 else 0.5
        closed_for_baseline = klines[-9:-1]
        baseline_vols = [float(k[5]) for k in closed_for_baseline]
        avg_baseline = sum(baseline_vols) / len(baseline_vols) if baseline_vols else 1
        f_vol_ratio = f_vol / avg_baseline if avg_baseline > 0 else 0
        f_body_pct = (f_close - f_open) / f_open * 100 if f_open > 0 else 0

        if f_close > f_open and f_vol_ratio >= 15.0 and f_buy_ratio >= 0.60 and f_body_pct >= 2.0:
            ticker_fast = get_ticker(symbol)
            if ticker_fast:
                current_price_fast = float(ticker_fast["lastPrice"])
                change_24h_fast = float(ticker_fast["priceChangePercent"])
                if change_24h_fast >= 0:
                    alerted_coins[fast_key] = now
                    send_to_topic(TOPIC_BUILDUPS,
                        f"🚨 <b>EXTREME VOLUME FORMING [5M, live]</b>\n\n"
                        f"🪙 <b>{symbol}</b>\n"
                        f"💰 Price: {format_price(current_price_fast)}\n"
                        f"⚡ Volume (mid-candle): {f_vol_ratio:.1f}x normal\n"
                        f"🟢 Buy pressure: {f_buy_ratio*100:.0f}%\n"
                        f"🕐 {datetime.now().strftime('%H:%M:%S')}\n\n"
                        f"⚠️ Detected on the STILL-FORMING candle — earlier than the normal "
                        f"3-closed-candle check. Extreme magnitude, moving fast.\n"
                        f"⚠️ <i>Confirm on chart before entry.</i>"
                    )
                    print(f"🚨 Fast explosive detection: {symbol} vol={f_vol_ratio:.1f}x (forming candle)")
                    track_building_signal(symbol, "Explosive Pump [5M]", current_price_fast)
                    check_high_confidence_signal(symbol, "Explosive Pump [5M]", current_price_fast)
                    if f_vol_ratio >= 20.0:
                        send_extreme_pump_alert(symbol, current_price_fast, f_body_pct, f_vol_ratio, f_buy_ratio, "Explosive Pump [5M] (forming)")

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

    ft_score, ft_details = calc_followthrough_score(symbol, "5m", klines, vol_ratio, buy_ratio, current_price, change_24h)
    high_potential = ft_score >= 60
    is_distribution_warning = any("DISTRIBUTION WARNING" in d or "bearish (red)" in d for d in ft_details)
    ft_tag = ""
    if high_potential:
        ft_details_str = "\n   ".join(ft_details)
        ft_tag = f"\n\n🔥 <b>HIGH FOLLOW-THROUGH POTENTIAL ({ft_score})</b>\n   {ft_details_str}"
    elif is_distribution_warning:
        ft_details_str = "\n   ".join(ft_details)
        ft_tag = f"\n\n   {ft_details_str}"

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
        send_to_topic(TOPIC_BUILDUPS, msg)
    if sent:
        print(f"💥 Explosive pump: {symbol} (+{gain_pct:.1f}% in 3 candles) | FT score: {ft_score}")
        signal_performance[f"{symbol}_explosive_{int(now)}"] = {
            "symbol": symbol, "signal_price": current_price,
            "signal_time": now, "signal_type": "Explosive Pump [5M]",
            "highest_after": current_price,
        }
        track_building_signal(symbol, "Explosive Pump [5M]", current_price)
        check_high_confidence_signal(symbol, "Explosive Pump [5M]", current_price)

        # ── Guaranteed High Priority path for EXTREME moves (note #7) ──
        # GNOUSDT-style vertical wick spikes are extreme enough that no big
        # pump should be missable via the normal score>=2/3 promotion gate —
        # bypass it entirely and fire straight to High Priority.
        if vol_ratio >= 20.0 or gain_pct >= 15.0:
            send_extreme_pump_alert(symbol, current_price, gain_pct, vol_ratio, buy_ratio, "Explosive Pump [5M]")

# ─── EXTREME PUMP: GUARANTEED HIGH PRIORITY BYPASS (note #7) ──────────
_extreme_pump_alerted = {}

def send_extreme_pump_alert(symbol, current_price, gain_pct, vol_ratio, buy_ratio, source_signal):
    """
    For sufficiently extreme single-move pumps (very high vol_ratio / body
    size), bypasses check_high_confidence_signal's score>=2/3 gate entirely
    — sends to Building Momentum and the Big Pump topic, so no big pump is
    missable just because the broader confluence checklist didn't line up.
    """
    now = time.time()
    key = f"{symbol}_extreme"
    if now - _extreme_pump_alerted.get(key, 0) < 6 * 3600:
        return
    _extreme_pump_alerted[key] = now
    send_to_topic(TOPIC_BUILDUPS,
        f"🚨 <b>EXTREME PUMP — GUARANTEED ALERT</b>\n\n"
        f"🪙 <b>{symbol}</b>\n"
        f"💰 Price: {format_price(current_price)}\n"
        f"🚀 Move: <b>+{gain_pct:.1f}%</b> | ⚡ Volume: <b>{vol_ratio:.1f}x</b> | 🟢 Buy: {buy_ratio*100:.0f}%\n"
        f"📡 Source: {source_signal}\n"
        f"🕐 {datetime.now().strftime('%H:%M:%S')}\n\n"
        f"⚠️ Extreme magnitude — bypasses the normal confluence gate entirely "
        f"(also sent to Big Pump topic), so this can't be missed. Confirm on chart before entry."
    )
    print(f"🚨 Extreme pump guaranteed alert: {symbol} +{gain_pct:.1f}% vol={vol_ratio:.1f}x")
    send_big_pump_alert(symbol, current_price, f"Extreme Pump ({source_signal})")


_big_pump_alerted = {}  # {symbol: last_alert_time}

def send_big_pump_alert(symbol, current_price, source_signal, klines_4h=None):
    """
    Note #7: dedicated "last line of defense" alert for the user's Big Pump
    topic (TOPIC_BIG_PUMP). For coins showing strong signs of an actual big
    pump developing (fed from check_high_confidence_signal at score>=4,
    fast-pumper hits, and extreme-pump guaranteed alerts), sends directly
    here with a trade plan — separate from every other topic, specifically
    so a real big pump can't be missed.

    TP2 is resistance/liquidity/volume-based (nearest real resistance
    cluster from EQH + 4H/1D swing highs), NOT a fixed percentage — it
    reports whatever % that actually works out to (could be 17%, could be
    35%), rather than forcing a round 20% every time.
    """
    now = time.time()
    if now - _big_pump_alerted.get(symbol, 0) < 6 * 3600:
        return
    if not klines_4h:
        klines_4h = get_klines(symbol, interval="4h", limit=100)
    if not klines_4h:
        return

    eql_data = detect_equal_highs_lows(klines_4h, current_price)
    nearest_eql = eql_data["eq_lows"][0] if eql_data.get("eq_lows") else None
    sl = nearest_eql["price"] * 0.985 if nearest_eql else current_price * 0.90
    risk = (current_price - sl) / current_price * 100

    # Gather resistance candidates from multiple real sources — EQH levels,
    # 4H swing highs, 1D swing highs — instead of a fixed percentage.
    closed_4h = klines_4h[:-1]
    res_candidates = [(h["price"], h["touches"]) for h in eql_data.get("eq_highs", [])]
    res_candidates += [(float(k[2]), 1) for k in closed_4h[-40:]]
    klines_1d = get_klines(symbol, interval="1d", limit=60)
    if klines_1d:
        res_candidates += [(float(k[2]), 1) for k in klines_1d[:-1]]

    res_above = sorted(set(p for p, _ in res_candidates if p > current_price * 1.03))
    # Cluster levels within 3% of each other, keep the lower of each cluster
    res_clean = []
    for p in res_above:
        if not res_clean or (p - res_clean[-1]) / res_clean[-1] > 0.03:
            res_clean.append(p)

    tp1 = next((p for p in res_clean if p >= current_price * 1.05), current_price * 1.08)
    tp2 = next((p for p in res_clean if p > tp1 * 1.03), None)
    if tp2 is None:
        # No further real resistance found — use historical pattern context
        # (peak_hrs/peak_pct track record) as a reference instead of a flat %
        hist = get_pattern_history_stats(source_signal, min_samples=3)
        tp2 = tp1 * 1.15 if hist else tp1 * 1.10

    tp1_pct = (tp1 - current_price) / current_price * 100
    tp2_pct = (tp2 - current_price) / current_price * 100
    hist_note = get_pattern_history_stats(source_signal, min_samples=3)

    _big_pump_alerted[symbol] = now
    send_to_topic(TOPIC_BIG_PUMP,
        f"🚀 <b>BIG PUMP ALERT — {symbol}</b>\n\n"
        f"💰 Price: {format_price(current_price)}\n"
        f"📡 Source: {source_signal}\n"
        f"🕐 {datetime.now().strftime('%H:%M:%S')}\n\n"
        f"📐 Entry: {format_price(current_price)}\n"
        f"🔴 SL: {format_price(sl)} (-{risk:.1f}%)\n"
        f"🟢 TP1: {format_price(tp1)} (+{tp1_pct:.1f}%)\n"
        f"🟢 TP2: {format_price(tp2)} (+{tp2_pct:.1f}%) — nearest real resistance\n"
        + (f"   {hist_note}\n" if hist_note else "") +
        f"\n⚠️ <i>Confirm on chart before entry. Use stop-loss.</i>"
    )
    print(f"🚀 Big Pump alert: {symbol} @ {format_price(current_price)} (TP2 +{tp2_pct:.1f}%)")
    start_big_pump_watch(symbol, current_price, source_signal)


_big_pump_watch = {}  # {symbol: {"alert_price": float, "started": time, "source": str, "confirmed": bool}}

def start_big_pump_watch(symbol, alert_price, source_signal):
    """
    Note (build session): after a Big Pump alert fires, some coins retest
    before actually pumping, others pump immediately — user wants a distinct
    follow-up once the REAL pump starts either way. Registers the coin for
    continued monitoring (up to 72h).
    """
    if symbol not in _big_pump_watch:
        _big_pump_watch[symbol] = {
            "alert_price": alert_price,
            "started": time.time(),
            "source": source_signal,
            "confirmed": False,
        }

def _check_tf_pump_signal(symbol, tf, limit=12):
    """
    Helper for check_big_pump_watches: checks ONE timeframe for a
    developing pump. Requires at least 2 of the last 3 CLOSED candles to be
    bullish (not just a single candle/wick), plus elevated volume and buy
    pressure on the most recent one — sustained pattern, not one-candle noise.
    Returns (developing: bool, vol_ratio: float, buy_ratio: float).
    """
    klines = get_klines(symbol, interval=tf, limit=limit)
    if not klines or len(klines) < 8:
        return False, 0, 0.5
    closed = klines[:-1]
    last3 = closed[-3:]
    bullish_count = sum(1 for k in last3 if float(k[4]) > float(k[1]))

    prior_vols = [float(k[5]) for k in closed[-9:-3]]
    avg_vol = sum(prior_vols) / len(prior_vols) if prior_vols else 1
    last_vol = float(last3[-1][5])
    vol_ratio = last_vol / avg_vol if avg_vol > 0 else 0

    last_buy = float(last3[-1][9]) if len(last3[-1]) > 9 else last_vol * 0.5
    buy_ratio = last_buy / last_vol if last_vol > 0 else 0.5

    developing = bullish_count >= 2 and vol_ratio >= 3.0 and buy_ratio >= 0.58
    return developing, vol_ratio, buy_ratio


_shared_retest_watch = {}  # {key: {...}} — shared multi-timeframe retest state machine

def start_shared_retest_watch(key, symbol, level, topic, context, header, tf_sequence=None, chat_id=None):
    """
    Shared multi-timeframe retest-monitoring watch (notes #1 and #6).
    Escalates 5M → 15M → 30M → 1H(→4H): watches for price to dip back to
    `level` and either hold (reclaim with a green candle → notify "retest
    held") or fail (drop meaningfully below → move to the NEXT timeframe in
    the sequence). If retest fails on 2 different timeframes, that's treated
    as a real warning sign and reported directly instead of silently retrying
    forever.
    """
    if key in _shared_retest_watch:
        return
    _shared_retest_watch[key] = {
        "symbol": symbol, "level": level, "topic": topic, "chat_id": chat_id,
        "context": context, "header": header,
        "tf_sequence": tf_sequence or ["5m", "15m", "30m", "1h"],
        "tf_index": 0, "fail_count": 0, "state": "waiting_dip",
        "started": time.time(),
    }

def check_shared_retest_watches():
    """Runs periodically for up to 24h per watch."""
    now = time.time()
    to_remove = []
    for key, w in list(_shared_retest_watch.items()):
        if now - w["started"] > 24 * 3600:
            to_remove.append(key)
            continue
        symbol, level = w["symbol"], w["level"]
        tf = w["tf_sequence"][w["tf_index"]]
        try:
            ticker = get_ticker(symbol)
            klines = get_klines(symbol, interval=tf, limit=10)
            if not ticker or not klines or len(klines) < 5:
                continue
            current_price = float(ticker["lastPrice"])
            closed = klines[:-1]
            last = closed[-1]
            l_low, l_open, l_close = float(last[3]), float(last[1]), float(last[4])

            if w["state"] == "waiting_dip":
                if l_low <= level * 1.005:
                    w["state"] = "dipped"
            elif w["state"] == "dipped":
                if l_close > level and l_close > l_open:
                    # Retest held — notify and finish
                    send_to_topic(w["topic"],
                        f"{w['header']}\n\n{w['context']}\n\n"
                        f"✅ Retest held on {tf.upper()} — reclaimed with a green candle.\n"
                        f"💰 Now: {format_price(current_price)}"
                    )
                    if w.get("chat_id"):
                        send_to(w["chat_id"], f"{w['header']} — retest held on {tf.upper()}, now {format_price(current_price)}")
                    to_remove.append(key)
                    print(f"✅ Retest watch held: {symbol} [{tf.upper()}]")
                    continue
                elif current_price < level * 0.97:
                    # Failed on this timeframe — escalate to the next one
                    w["fail_count"] += 1
                    w["tf_index"] += 1
                    w["state"] = "waiting_dip"
                    if w["fail_count"] >= 2:
                        send_to_topic(w["topic"],
                            f"⚠️ <b>Retest Failed Twice — {symbol}</b>\n\n"
                            f"{w['context']}\n\n"
                            f"Retest failed on 2 different timeframes — something's off with this move, "
                            f"be cautious here.\n"
                            f"💰 Now: {format_price(current_price)}"
                        )
                        to_remove.append(key)
                        print(f"⚠️ Retest watch failed twice: {symbol}")
                        continue
                    if w["tf_index"] >= len(w["tf_sequence"]):
                        to_remove.append(key)  # exhausted the sequence quietly
        except Exception as e:
            print(f"Retest watch error {key}: {e}")
    for k in to_remove:
        _shared_retest_watch.pop(k, None)


def check_big_pump_watches():
    """
    Runs periodically. For each watched symbol, checks whether the actual
    pump has started, using 5M/15M/1H together (SMC-style multi-timeframe
    confirmation) rather than waiting on a single 1H candle:
    - If at least 2 of these 3 timeframes show a "developing" pattern
      (2 of last 3 candles bullish + volume 3x+ + buy pressure 58%+) at the
      same time, that's cross-timeframe agreement → confirmed. This catches
      fast moves within minutes instead of waiting up to an hour, while
      still requiring sustained (not single-candle) confirmation to keep
      false positives down.
    - Fallback: price already up 10%+ from the alert price with volume
      still elevated (covers a move that's already well underway).
    Fires ONE confirmed-pump follow-up to TOPIC_HIGH, then keeps tracking
    (won't re-fire) until it expires after 72h.
    """
    now = time.time()
    to_remove = []
    for symbol, watch in list(_big_pump_watch.items()):
        if now - watch["started"] > 72 * 3600:
            to_remove.append(symbol)
            continue
        if watch["confirmed"]:
            continue
        try:
            ticker = get_ticker(symbol)
            if not ticker:
                continue
            current_price = float(ticker["lastPrice"])

            tf_results = {}
            for tf in ("5m", "15m", "1h"):
                tf_results[tf] = _check_tf_pump_signal(symbol, tf)

            developing_count = sum(1 for d, _, _ in tf_results.values() if d)
            # Use the fastest timeframe that's developing for the display numbers
            display_vol_ratio, display_buy_ratio = 0, 0.5
            for tf in ("5m", "15m", "1h"):
                d, vr, br = tf_results[tf]
                if d:
                    display_vol_ratio, display_buy_ratio = vr, br
                    break
            if display_vol_ratio == 0:
                display_vol_ratio, display_buy_ratio = tf_results["1h"][1], tf_results["1h"][2]

            gain_from_alert = (current_price - watch["alert_price"]) / watch["alert_price"] * 100

            multi_tf_confirmed = developing_count >= 2
            already_pumping = gain_from_alert >= 10.0 and display_vol_ratio >= 2.0

            if multi_tf_confirmed or already_pumping:
                watch["confirmed"] = True
                vol_ratio, buy_ratio = display_vol_ratio, display_buy_ratio

                # Lightweight confluence summary (NOT the full /entry-style
                # analysis — that needs multi-timeframe retest checks + chart
                # pattern detection, which risks delaying this alert right
                # when speed matters most). Reuses cheap, usually-cached
                # calls only: daily trend, BS pressure (already fetched
                # above), OB zone, and nearest EQL — same cost as
                # get_quick_confluence_score.
                extra_lines = []
                try:
                    klines_4h = get_klines(symbol, interval="4h", limit=30)
                    if klines_4h:
                        closed_4h = klines_4h[:-1]
                        if not is_daily_downtrend(symbol, current_price):
                            extra_lines.append("✅ Daily trend bullish/neutral")
                        else:
                            extra_lines.append("❌ Daily trend bearish")

                        extra_lines.append(f"{'✅' if buy_ratio >= 0.55 else '🔴'} BS Pressure: {'Positive' if buy_ratio >= 0.55 else 'Negative'} ({buy_ratio*100:.0f}% buy)")

                        avg_v4h = sum(float(k[5]) for k in closed_4h[-10:]) / 10 or 1
                        for k in reversed(closed_4h[-15:]):
                            ko, kc, kh, kl, kv = float(k[1]), float(k[4]), float(k[2]), float(k[3]), float(k[5])
                            if kc > ko and kv >= avg_v4h * 1.3 and kl <= current_price <= kh * 1.05:
                                extra_lines.append(f"🔲 OB zone: {format_price(kl)}–{format_price(kh)}")
                                break

                        eql_data = detect_equal_highs_lows(klines_4h, current_price)
                        if eql_data.get("eq_lows"):
                            eql = eql_data["eq_lows"][0]
                            extra_lines.append(f"💧 Liq sweep zone: EQL {format_price(eql['price'])} ({eql['touches']}x tested)")
                except Exception as e:
                    print(f"Pump confirmed quick-analysis error {symbol}: {e}")

                extra_str = ("\n" + "\n".join(f"   {l}" for l in extra_lines) + "\n") if extra_lines else ""

                confirmed_tfs = [tf.upper() for tf, (d, _, _) in tf_results.items() if d]
                tf_note = f" [{'+'.join(confirmed_tfs)}]" if confirmed_tfs else ""
                send_to_topic(TOPIC_HIGH,
                    f"🚀 <b>PUMP CONFIRMED — {symbol}</b>{tf_note}\n\n"
                    f"💰 Alert price: {format_price(watch['alert_price'])} → Now: {format_price(current_price)} "
                    f"({gain_from_alert:+.1f}%)\n"
                    f"⚡ Volume: {vol_ratio:.1f}x | Buy: {buy_ratio*100:.0f}%\n"
                    f"📡 Original signal: {watch['source']}\n"
                    f"🕐 {datetime.now().strftime('%H:%M:%S')}\n"
                    + extra_str +
                    f"\n✅ The pump flagged earlier is actually moving now.\n"
                    f"⚠️ <i>Confirm on chart before entry.</i>"
                )
                print(f"🚀 Big Pump CONFIRMED: {symbol} {gain_from_alert:+.1f}% vol={vol_ratio:.1f}x")

                # Note #1: start the post-confirmation retest watch —
                # if this coin retests instead of continuing straight up,
                # monitor 5M→15M→30M→1H for the retest to hold or fail.
                start_shared_retest_watch(
                    key=f"{symbol}_pumpconfirm_retest",
                    symbol=symbol, level=current_price, topic=TOPIC_HIGH,
                    header=f"🔄 <b>Post-Pump Retest — {symbol}</b>",
                    context=f"Following up on the PUMP CONFIRMED alert at {format_price(current_price)}.",
                )
        except Exception as e:
            print(f"Big pump watch error {symbol}: {e}")
    for s in to_remove:
        _big_pump_watch.pop(s, None)

# ─── DAILY TREND FILTER ───────────────────────────────────
def is_daily_downtrend(symbol, current_price, klines_1h=None, klines_daily=None):
    """
    Returns True if the symbol is in a daily downtrend.
    Accepts optional cached klines to avoid redundant API calls.
    """
    # Check for extreme volume reversal before applying downtrend filter
    if klines_1h is None:
        ticker = get_ticker(symbol)
        if ticker:
            klines_1h = get_klines(symbol, interval="1h", limit=12)
    if klines_1h and len(klines_1h) >= 8:
        closed_1h = klines_1h[:-1]
        last_1h = closed_1h[-1]
        l_open  = float(last_1h[1])
        l_close = float(last_1h[4])
        l_vol   = float(last_1h[5])
        avg_vol = sum(float(k[5]) for k in closed_1h[-8:-1]) / 7
        vol_ratio = l_vol / avg_vol if avg_vol > 0 else 0
        if (vol_ratio >= 8.0 and l_close > l_open and l_open > 0 and
                (l_close - l_open) / l_open >= 0.03):
            return False  # bypass — reversal pump

    if klines_daily is None:
        klines_daily = get_klines(symbol, interval="1d", limit=15)
    if not klines_daily or len(klines_daily) < 7:
        return False

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
    send_to_topic(TOPIC_BUILDUPS, msg)
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
        track_building_signal(symbol, "Pre-pump Phase 2", current_price)
        check_high_confidence_signal(symbol, "Pre-pump Setup [1H]", current_price)
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
        send_to_topic(TOPIC_BUILDUPS, msg)  # Phase 3 → Building Momentum, not High Priority

        signal_performance[f"{symbol}_prepump_{int(now)}"] = {
            "symbol": symbol, "signal_price": current_price,
            "signal_time": now, "signal_type": "Pre-pump Setup [1H]",
            "highest_after": current_price,
        }
        print(f"🚀 Phase 3: {symbol}")
        track_building_signal(symbol, "Pre-pump Phase 3 🚀", current_price)
        check_high_confidence_signal(symbol, "Pre-pump Setup [1H]", current_price)
        start_prospect_watch(symbol, "Pre-pump Breakout [Phase 3]")



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
        start_prospect_watch(symbol, "Big Pump Setup")

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
            if sym in removed_coins:  # never re-add manually removed coins
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
    if l_open <= 0:
        return
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
def calc_followthrough_score(symbol, tf, klines, vol_ratio, buy_ratio, current_price, change_24h=None):
    score = 0
    details = []

    # ── Distribution pattern check (added after TNSRUSDT case) ──
    # A huge volume spike during a price that's DOWN heavily on the day is a
    # classic distribution signature: large holders unload into retail buying
    # interest right as the move attracts attention, then the price dumps
    # further once that supply is absorbed. The old logic only checked daily
    # EMA position for "trend", which can still say "bullish/neutral" while the
    # coin is actively crashing — it never looked at the spike candle's own
    # direction or the actual 24h change. This check looks at both directly and
    # can veto the whole score to flag a likely distribution event instead of
    # following through.
    last_candle = klines[-2]
    spike_candle_bearish = float(last_candle[4]) < float(last_candle[1])  # close < open
    heavy_24h_down = change_24h is not None and change_24h <= -10.0

    if heavy_24h_down and vol_ratio >= 10:
        details.append(f"🚨 DISTRIBUTION WARNING — {change_24h:+.1f}% on 24h with {vol_ratio:.1f}x volume")
        details.append("⚠️ Large volume during a heavy daily decline can mean big holders are selling into this spike, not buying — be cautious about treating this as a bullish signal")
        return 0, details
    if spike_candle_bearish and vol_ratio >= 10:
        details.append(f"⚠️ Spike candle itself is bearish (red) despite {vol_ratio:.1f}x volume — this may be selling pressure, not buying")
        score -= 10

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

# ─── /ENTRY ON-DEMAND TECHNICAL CHECK ─────────────────────
"""
Public command: any subscriber can run /entry SYMBOL to get a real-time
technical snapshot before deciding whether to take an entry — without waiting
for the bot to fire a signal on its own. This reuses the same building blocks
the bot's own signals are scored with (EMA position, structure/swing-low,
volume, daily trend, distribution-risk check), but framed as a single combined
"entry health" score where higher = more favorable conditions right now.

This is explicitly NOT a win-probability number (see the team's earlier
decision against fabricated percentages) — it's a transparent breakdown of
observable technical conditions so the person can make their own informed
call, consistent with the bot's "honest, no exaggerated claims" positioning.
"""
# ─── VOLUME SPIKE ─────────────────────────────────────────
def check_timeframe(symbol, tf):
    cfg = TIMEFRAMES[tf]
    klines = get_klines(symbol, interval=tf, limit=50)
    if not klines or len(klines) < 10:
        return

    if tf != "5m":
        check_volume_buildup(symbol, tf, klines)
        check_gradual_buildup(symbol, tf, klines)   # HMSTR-style multi-day accumulation
        check_range_breakout(symbol, tf, klines)    # ARPA-style tight-range breakout
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

    # ── Fake breakout filter (note #1 backlog) ──
    # (1) Consolidation before spike: prior candles should show a tight range,
    #     not already trending/volatile — a real breakout emerges from a base.
    candle_low = float(candle[3])
    candle_range = spike_high - candle_low
    prior_ranges_pct = [
        (float(k[2]) - float(k[3])) / float(k[3]) * 100
        for k in klines[-9:-2] if float(k[3]) > 0
    ]
    if prior_ranges_pct and (sum(prior_ranges_pct) / len(prior_ranges_pct)) > 3.0:
        return  # prior candles too volatile to call this a real consolidation base

    # (2) Breakout candle must have a real body, not just a long wick.
    if candle_range > 0 and ((close_price - open_price) / candle_range) < 0.4:
        return  # mostly wick — likely a fake/failed breakout attempt

    # (3) Volume spike on the breakout candle itself is already required above.

    closes = [float(k[4]) for k in klines[:-1]]
    ema20 = calculate_ema(closes, 20)
    if ema20 and close_price < ema20:
        return

    if tf in ["1h", "4h", "1d"]:
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

    ft_score, ft_details = calc_followthrough_score(symbol, tf, klines, ratio, 0, price, change_24h)
    high_potential = ft_score >= 60
    is_distribution_warning = any("DISTRIBUTION WARNING" in d or "bearish (red)" in d for d in ft_details)
    ft_tag = ""
    if high_potential:
        ft_details_str = "\n   ".join(ft_details)
        ft_tag = f"\n\n🔥 <b>HIGH FOLLOW-THROUGH POTENTIAL ({ft_score})</b>\n   {ft_details_str}"
    elif is_distribution_warning:
        # Always surface this even though it's not a "high potential" tag —
        # the warning itself is the important information here, not a bonus.
        ft_details_str = "\n   ".join(ft_details)
        ft_tag = f"\n\n   {ft_details_str}"

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
        send_to_topic(TOPIC_BUILDUPS, msg)  # escalate to Building Momentum
    if sent:
        print(f"✅ [{cfg['label']}] Spike: {symbol} ({ratio:.1f}x) | FT score: {ft_score}{' [DISTRIBUTION WARNING]' if is_distribution_warning else ''}")
        signal_performance[f"{symbol}_spike_{tf}_{int(now)}"] = {
            "symbol": symbol, "signal_price": price,
            "signal_time": now, "signal_type": f"Volume Spike [{cfg['label']}]",
            "highest_after": price,
        }
        track_building_signal(symbol, f"Volume Spike [{cfg['label']}]", price)
        check_high_confidence_signal(symbol, f"Volume Spike [{cfg['label']}]", price)

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

def detect_patterns(symbol, klines_4h, klines_1d, current_price):
    """
    Detects common chart patterns on 4H and 1D timeframes for use in /entry
    output. Returns a list of pattern description strings (plain text, ready
    to embed in the message). Patterns are context/information only — they
    don't affect the numeric entry score, since seeing a pattern and deciding
    to enter are separate judgments that should always include chart confirmation.

    Patterns detected:
      Ascending Triangle   — flat resistance + higher lows (bullish coiling)
      Symmetrical Triangle — converging highs + lows (neutral coiling, bias=breakout direction)
      Descending Triangle  — flat support + lower highs (warning: bearish pressure)
      Double Bottom (W)    — two equal lows + neckline break (reversal)
      Bull Flag            — strong rally + tight sideways consolidation
      Cup & Handle         — rounded bottom + small handle pullback
      Higher Lows Trend    — simple ascending low structure
      Range Breakout       — flat top + bottom range cleared with volume
    """
    results = []

    def _scan(klines, tf_label):
        if not klines or len(klines) < 20:
            return
        closed = klines[:-1]
        highs  = [float(k[2]) for k in closed[-20:]]
        lows   = [float(k[3]) for k in closed[-20:]]
        closes = [float(k[4]) for k in closed[-20:]]
        vols   = [float(k[5]) for k in closed[-20:]]
        n = len(highs)

        # ── Ascending Triangle ──────────────────────────────────────────
        # Flat resistance (equal highs within 1.5%) + higher lows
        recent_highs = highs[-10:]
        max_h = max(recent_highs)
        near_top = [h for h in recent_highs if h >= max_h * 0.985]
        if len(near_top) >= 3:
            recent_lows = lows[-10:]
            hl_count = sum(1 for i in range(1, len(recent_lows))
                           if recent_lows[i] > recent_lows[i-1] * 1.005)
            if hl_count >= 3:
                touch_count = len(near_top)
                pct_below = (max_h - current_price) / max_h * 100
                if pct_below < 5:
                    results.append(
                        f"🔺 [{tf_label}] <b>Ascending Triangle</b> — flat resistance "
                        f"~{format_price(max_h)} ({touch_count} touches), higher lows forming. "
                        f"Breakout above {format_price(max_h)} with volume = strong entry signal."
                    )

        # ── Symmetrical Triangle / Wedge ────────────────────────────────
        # Converging highs and lows over last 12 candles
        h12 = highs[-12:]
        l12 = lows[-12:]
        h_slope = (h12[-1] - h12[0]) / len(h12)
        l_slope = (l12[-1] - l12[0]) / len(l12)
        range_now  = h12[-1] - l12[-1]
        range_then = h12[0]  - l12[0]
        if (h_slope < -0.0001 and l_slope > 0.0001 and
                range_now < range_then * 0.65 and range_then > 0):
            compression = (1 - range_now / range_then) * 100
            results.append(
                f"🔻 [{tf_label}] <b>Symmetrical Triangle (Coiling)</b> — price "
                f"compressing {compression:.0f}% from {format_price(range_then)} to "
                f"{format_price(range_now)} range. Breakout direction = strong move. "
                f"Current bias: {'bullish 📈' if closes[-1] > closes[-6] else 'bearish 📉'}."
            )

        # ── Double Bottom (W Pattern) ────────────────────────────────────
        # Two lows within 2% of each other with a peak in between
        if n >= 15:
            region = lows[-15:]
            min_idx = region.index(min(region))
            if 3 <= min_idx <= len(region) - 4:
                left_lows  = region[:min_idx]
                right_lows = region[min_idx+1:]
                left_min   = min(left_lows)  if left_lows  else 999
                right_min  = min(right_lows) if right_lows else 999
                neckline   = max(closes[max(0, -15+min_idx-3):-15+min_idx+3] or [0])
                if (abs(left_min - right_min) / right_min < 0.025 and
                        current_price > neckline * 0.99):
                    results.append(
                        f"🅆 [{tf_label}] <b>Double Bottom (W Pattern)</b> — two lows "
                        f"at ~{format_price(min(left_min, right_min))}, neckline "
                        f"~{format_price(neckline)}. "
                        f"{'✅ Breaking above neckline — reversal signal.' if current_price > neckline else '⏳ Watch for neckline break above ' + format_price(neckline) + '.'}"
                    )

        # ── Bull Flag ────────────────────────────────────────────────────
        # Strong rally (5+ candles up) followed by tight sideways consolidation
        if n >= 12:
            rally = closes[-12:-6]
            flag  = closes[-6:]
            rally_gain = (rally[-1] - rally[0]) / rally[0] * 100 if rally[0] > 0 else 0
            flag_range = (max(flag) - min(flag)) / min(flag) * 100 if min(flag) > 0 else 100
            if rally_gain >= 8 and flag_range <= 5:
                results.append(
                    f"🚩 [{tf_label}] <b>Bull Flag</b> — +{rally_gain:.1f}% rally followed by "
                    f"tight {flag_range:.1f}% consolidation (flag). "
                    f"Breakout above {format_price(max(flag))} with volume = continuation entry."
                )

        # ── Cup & Handle ─────────────────────────────────────────────────
        # Rounded bottom over 10+ candles + small handle pullback (3-5 candles)
        if n >= 18:
            cup   = lows[-18:-5]
            handle= closes[-5:]
            cup_low  = min(cup)
            cup_high = max(closes[-18:-5])
            handle_low = min(handle)
            handle_pullback = (cup_high - handle_low) / cup_high * 100 if cup_high > 0 else 100
            # Cup: lows form a U shape (middle lower than edges)
            mid_cup = cup[len(cup)//2]
            if (mid_cup <= cup_low * 1.03 and
                    handle_pullback <= 15 and handle_pullback >= 3 and
                    current_price >= handle_low * 1.01):
                results.append(
                    f"☕ [{tf_label}] <b>Cup & Handle</b> — rounded bottom with handle "
                    f"pullback of {handle_pullback:.1f}%. "
                    f"Breakout above {format_price(cup_high)} = high-probability continuation."
                )

        # ── Higher Lows Trend ─────────────────────────────────────────────
        # Simple: last 3 swing lows each higher than the previous
        swing_lows = []
        for i in range(2, n - 2):
            if lows[i] < lows[i-1] and lows[i] < lows[i-2] and lows[i] < lows[i+1] and lows[i] < lows[i+2]:
                swing_lows.append(lows[i])
        if len(swing_lows) >= 3 and all(swing_lows[i] > swing_lows[i-1] for i in range(1, len(swing_lows[-3:]))):
            trend_strength = (swing_lows[-1] - swing_lows[-3]) / swing_lows[-3] * 100
            results.append(
                f"📈 [{tf_label}] <b>Higher Lows Trend</b> — 3 consecutive higher swing lows "
                f"(+{trend_strength:.1f}% rise in lows). Uptrend structure intact — "
                f"dips are buying opportunities while this holds."
            )

    _scan(klines_4h, "4H")
    _scan(klines_1d, "1D")
    return results


def detect_break_retest_pattern(klines_4h, current_price):
    """
    Item: /entry Pattern Context. Looks for a "break → retest → continuation"
    setup on 4H — a recent resistance level broken with a strong-body candle,
    with price now back near that level checking whether it holds as new
    support. This is one of the more reliable setups (the ENA case that
    prompted this), and it's exactly the kind of context a static EMA/volume
    score alone can't see — it needs to look at price structure over several
    candles, not just the latest one.

    APPROACH (rewritten after the first version missed a real ENA retest):
    The original version required a strict local-maximum (lower highs on BOTH
    sides) to call something a "resistance swing high". Real charts are
    messier than that — a resistance zone often has several candles near the
    same high, and the breakout candle itself can sit right at the edge of the
    lookback window where there's no room for "candles after" to confirm a
    local max. Instead: find the strongest, most recent breakout candle in the
    window first (the candle search recognizes as a genuine break), then use
    the highest high in the candles BEFORE it as the resistance level being
    broken. This matches how a person actually reads the chart — "what was
    price struggling to get through before this candle blew past it" — rather
    than requiring an isolated single-candle peak.

    Returns a single guidance string, or None if no clean pattern is found.
    """
    if not klines_4h or len(klines_4h) < 12:
        return None

    closed = klines_4h[:-1]
    lookback = closed[-20:] if len(closed) >= 20 else closed
    n = len(lookback)
    if n < 8:
        return None

    # Find the most recent bullish candle that closed above the highest price
    # seen in the candles before it — that's our breakout candle. Qualifies via
    # EITHER strong body ratio OR strong volume+buy-pressure (matching how the
    # bot's own zone-confirmation logic already validates breakouts). This is
    # the second relaxation: the IQUSDT case showed a real breakout candle with
    # a large upper wick (price pushed even higher within the candle before
    # settling back) that failed the body-ratio test outright, even though the
    # bot's own zone logic had already confirmed it as a genuine breakout using
    # volume (1.5x+) and buy pressure (52%+) instead of candle shape.
    break_i = None
    level_price = None
    debug_lines = []
    for i in range(n - 2, 3, -1):  # leave room for at least 1 candle after it (a retest candle)
        k = lookback[i]
        o, c, h, l, v = float(k[1]), float(k[4]), float(k[2]), float(k[3]), float(k[5])
        buy_v = float(k[9]) if len(k) > 9 else 0
        candle_range = h - l
        body = abs(c - o)
        if candle_range <= 0 or c <= o:
            debug_lines.append(f"  i={i}: bearish/zero-range (o={o:.6g} c={c:.6g}) — skip")
            continue

        body_ratio_ok = body / candle_range >= 0.40
        prev_vols = [float(x[5]) for x in lookback[max(0, i - 8):i]]
        avg_vol = sum(prev_vols) / len(prev_vols) if prev_vols else 0
        vol_ratio = v / avg_vol if avg_vol else 0
        buy_ratio = buy_v / v if v > 0 else 0
        volume_confirmed_ok = vol_ratio >= 1.5 and buy_ratio >= 0.52

        prior_high = max(float(x[2]) for x in lookback[max(0, i - 8):i])
        # FIX (after the JST case): a 0.5% margin missed a genuine breakout that
        # closed only ~0.36% above prior_high — on lower-priced/lower-volatility
        # coins, real breakouts often clear resistance by a small margin. Relaxed
        # to 0.2%, still enough buffer to avoid noise-level "barely poking above".
        closed_above = c > prior_high * 1.002
        debug_lines.append(
            f"  i={i}: body_ratio={body/candle_range:.2f}({body_ratio_ok}) "
            f"vol_ratio={vol_ratio:.1f}x({volume_confirmed_ok}, buy={buy_ratio:.2f}) "
            f"close={c:.6g} prior_high={prior_high:.6g} closed_above={closed_above}"
        )

        if not (body_ratio_ok or volume_confirmed_ok):
            continue
        if closed_above:
            break_i = i
            level_price = prior_high
            break

    if break_i is None or level_price is None:
        print(f"📐 /entry pattern: no qualifying breakout candle found in last {n} 4H candles for this symbol")
        print("📐 /entry pattern debug:\n" + "\n".join(debug_lines))
        return None
    print(f"📐 /entry pattern: breakout found at candle -{n - break_i}, level={level_price:.6g}")

    candles_since_break = lookback[break_i + 1:]
    if not candles_since_break:
        return None

    return _build_retest_guidance(level_price, current_price, candles_since_break)

def _build_retest_guidance(level_price, current_price, candles_since_break):
    near_level = abs(current_price - level_price) / level_price <= 0.05
    still_above = current_price >= level_price * 0.98  # hasn't broken back below it
    pct_from_level = (current_price - level_price) / level_price * 100

    # Note #2 fix: the old check only looked at current price's distance
    # from the level + whether the LAST candle is green — it never verified
    # that price actually DIPPED back down to test the level at all. A coin
    # that broke out and just kept climbing on green candles (never pulling
    # back) could satisfy "near_level" simply because it hadn't traveled far
    # from the breakout point yet, producing a false "retest confirmed"
    # (JASMYUSDT case — no retest pattern existed at all on the chart).
    # Fix: require at least one candle SINCE the breakout (before the final
    # one) to have actually dipped down near/into the level — a genuine
    # touch-back, not just proximity.
    genuine_dip_occurred = any(
        float(c[3]) <= level_price * 1.02  # candle's LOW came within 2% of the level
        for c in candles_since_break[:-1]
    )

    print(f"📐 /entry pattern: current={current_price:.6g} level={level_price:.6g} "
          f"({pct_from_level:+.1f}% from level) near={near_level} still_above={still_above} "
          f"genuine_dip={genuine_dip_occurred}")

    last_candle = candles_since_break[-1]
    last_o, last_c = float(last_candle[1]), float(last_candle[4])
    last_h, last_l = float(last_candle[2]), float(last_candle[3])
    last_range = last_h - last_l
    last_is_strong_green = (
        last_c > last_o and last_range > 0 and
        (last_c - last_o) / last_range >= 0.55
    )

    if near_level and still_above and last_is_strong_green and last_c > level_price and genuine_dip_occurred:
        return (f"✅ <b>Retest confirmed</b> — price broke {format_price(level_price)} resistance, "
                f"retested, and just closed back above it with a strong green candle. Continuation looks favorable.")
    elif near_level and still_above:
        return (f"⏳ <b>Retest in progress</b> — price broke {format_price(level_price)} resistance and is now "
                f"retesting that level. Consider waiting for a green body candle to close back above "
                f"{format_price(level_price)} before entering, with a stop-loss below the retest low.")
    elif current_price < level_price * 0.98:
        return (f"⚠️ <b>Retest failed</b> — price broke {format_price(level_price)} but has since closed back "
                f"below it. This weakens the breakout; treat it with caution.")
    elif still_above and not near_level and pct_from_level > 0:
        # FIX (after the SOPH case): price broke out and kept moving well past
        # the level (+6-8% or more) without ever pulling back to retest it.
        # The old code silently returned None here, which made the bot look
        # broken ("no qualifying breakout" was misleading — a breakout WAS
        # found, there's just no retest opportunity left). This is an honest,
        # informative case: the move has already extended past the point
        # where a retest entry makes sense.
        return (f"✅ <b>Breakout extended</b> — price broke {format_price(level_price)} and has moved "
                f"+{pct_from_level:.1f}% past it without retesting. No retest opportunity right now — "
                f"this is already an extended move, not an entry-on-pullback setup.")
    return None

def calc_entry_score(symbol):
    klines_1h = get_klines(symbol, interval="1h", limit=30)
    klines_4h = get_klines(symbol, interval="4h", limit=30)
    klines_1d = get_klines(symbol, interval="1d", limit=30)
    klines_15m = get_klines(symbol, interval="15m", limit=30)
    klines_30m = get_klines(symbol, interval="30m", limit=30)
    ticker = get_ticker(symbol)
    if not klines_1h or len(klines_1h) < 15 or not ticker:
        return None  # not enough data — caller should report "unavailable"

    closed_1h = klines_1h[:-1]
    closes_1h = [float(k[4]) for k in closed_1h]
    current_price = closes_1h[-1]
    change_24h = float(ticker["priceChangePercent"])

    score = 0
    max_score = 0
    details = []

    # ── Distribution-risk veto (same logic as calc_followthrough_score) ──
    last_candle = klines_1h[-2]
    spike_vol = float(last_candle[5])
    prev_vols_1h = [float(k[5]) for k in closed_1h[-8:-1]]
    avg_vol_1h = sum(prev_vols_1h) / len(prev_vols_1h) if prev_vols_1h else 1
    vol_ratio_1h = spike_vol / avg_vol_1h if avg_vol_1h else 1

    # FIX (after the MBL case): the old -10% threshold let -9.71% slip through —
    # an arbitrary boundary missed a textbook distribution pattern by a fraction
    # of a percentage point. Relaxed to -7% for real margin.
    heavy_24h_down = change_24h <= -7.0 and vol_ratio_1h >= 10

    # FIX: also check directly whether price has given back most of a recent
    # spike, regardless of where 24h change lands — this is the actual visible
    # signature in the MBL chart (huge volume spike candle, then hard dump right
    # after) and is a more direct signal than the 24h-change proxy alone.
    recent_high_1h = max(float(k[2]) for k in closed_1h[-10:])
    recent_low_1h = min(float(k[3]) for k in closed_1h[-10:])
    spike_retraced = False
    if recent_high_1h > recent_low_1h:
        retrace_pct = (recent_high_1h - current_price) / (recent_high_1h - recent_low_1h)
        spike_retraced = retrace_pct >= 0.70 and vol_ratio_1h >= 5

    if heavy_24h_down or spike_retraced:
        reason = (f"{change_24h:+.1f}% on 24h with {vol_ratio_1h:.1f}x recent volume" if heavy_24h_down
                   else f"price has given back most of a recent spike ({vol_ratio_1h:.1f}x volume high)")
        return {
            "score": 0, "max_score": 100, "label": "🔴 AVOID",
            "details": [
                f"🚨 DISTRIBUTION WARNING — {reason}",
                "⚠️ This pattern often means large holders selling into retail interest, not a genuine bullish move",
            ],
            "price": current_price, "pattern_note": None, "pattern_notes": {},
        }

    # ── 1H EMA position ──
    ema20_1h = calculate_ema(closes_1h, 20)
    max_score += 20
    if ema20_1h and current_price > ema20_1h * 1.01:
        score += 20
        details.append(f"✅ Above 1H 20EMA with margin ({format_price(ema20_1h)})")
    elif ema20_1h and current_price > ema20_1h:
        score += 10
        details.append(f"⚠️ Just above 1H 20EMA ({format_price(ema20_1h)})")
    else:
        details.append(f"❌ Below 1H 20EMA" + (f" ({format_price(ema20_1h)})" if ema20_1h else ""))

    # ── Daily trend ──
    max_score += 20
    if not is_daily_downtrend(symbol, current_price):
        score += 20
        details.append("✅ Daily trend bullish/neutral")
    else:
        details.append("❌ Daily downtrend")

    # ── Structure: higher lows on 1H ──
    max_score += 20
    if check_hl_only(closed_1h, lookback=8):
        score += 20
        details.append("✅ Higher lows forming (1H)")
    else:
        details.append("⚠️ No clear higher-low structure (1H)")

    # ── Recent volume / momentum ──
    max_score += 20
    if vol_ratio_1h >= 2.0:
        score += 20
        details.append(f"✅ Elevated recent volume ({vol_ratio_1h:.1f}x avg)")
    elif vol_ratio_1h >= 1.2:
        score += 10
        details.append(f"⚠️ Slightly elevated volume ({vol_ratio_1h:.1f}x avg)")
    else:
        details.append(f"⚠️ Normal/low volume ({vol_ratio_1h:.1f}x avg)")

    # ── Extended-move warning (informational, doesn't add/subtract score) ──
    if change_24h >= 50:
        details.append(f"⚠️ Already +{change_24h:.0f}% in 24h — an extended move, consider waiting for a pullback")

    # ── 4H confluence with a daily level, if available ──
    if klines_4h and len(klines_4h) >= 10:
        recent_low_4h = min(float(k[3]) for k in klines_4h[-10:-1])
        recent_high_4h = max(float(k[2]) for k in klines_4h[-10:-1])
        is_confluent, conf_note = check_daily_confluence(symbol, recent_low_4h, recent_high_4h)
        if is_confluent:
            max_score += 10
            score += 10
            details.append(f"🎯 Daily confluence — {conf_note}")

        # Note #2/#9 part 2: a zone with a history of repeated rejections
        # shouldn't score the same as a fresh clean retest — apply a
        # confidence penalty so it doesn't mislead into "HIGH" territory.
        bounce_info = get_zone_bounce_info(symbol, recent_low_4h, recent_high_4h)
        if bounce_info and bounce_info.get("invalid_count", 0) >= 2:
            penalty = min(20, bounce_info["invalid_count"] * 7)
            score = max(0, score - penalty)
            details.append(
                f"⚠️ This zone has been rejected {bounce_info['invalid_count']}x before — "
                f"treat with extra caution, confidence reduced"
            )

    # ── Pattern context: break-retest-continuation, checked across 15m/30m/1H/4H ──
    # Previously only 4H and 1H were checked. The SOPH and BROCCOLI714 cases
    # showed real retest setups visible on 15m/30m that the 1H/4H-only check
    # missed entirely (the move was too fast/small to show up as a "resistance
    # break" on the larger timeframes yet). Checking all four and reporting
    # each independently lets the person see exactly which timeframes agree.
    pattern_notes = {}
    klines_by_tf_for_pattern = {"15m": klines_15m, "30m": klines_30m, "1h": klines_1h, "4h": klines_4h}
    for tf_label, klines_tf in klines_by_tf_for_pattern.items():
        if klines_tf:
            note = detect_break_retest_pattern(klines_tf, current_price)
            if note:
                pattern_notes[tf_label] = note

    # FIX (after the SOPH live case): if a retest is actively "in progress" on
    # any timeframe, check whether that timeframe's most recent candle is red.
    # The old score had no awareness of this — it showed "90/90 HIGH" at the
    # exact moment a live retest candle was bearish, which is precisely the
    # situation where entry should be paused and the next candle's close
    # watched, not treated as a green light. This doesn't override the
    # technical score (which reflects broader trend/structure, still valid),
    # but it adds a direct, visible warning for the live moment.
    active_retest_red_candle_tf = None
    for tf_label, note in pattern_notes.items():
        if "in progress" in note:
            klines_tf = klines_by_tf_for_pattern.get(tf_label)
            if klines_tf and len(klines_tf) >= 2:
                last_closed = klines_tf[-2]
                if float(last_closed[4]) < float(last_closed[1]):  # close < open = red
                    active_retest_red_candle_tf = tf_label
                    break
    if active_retest_red_candle_tf:
        details.append(
            f"🔴 Live retest candle on {active_retest_red_candle_tf.upper()} is currently RED — "
            f"this is a normal part of a retest, but it's not yet confirmed. Wait for a green "
            f"candle to close above the level before treating this as a green light."
        )

    label = "🟢 HIGH" if score / max_score >= 0.75 else ("🟡 MEDIUM" if score / max_score >= 0.45 else "🔴 LOW")
    # ── Chart Pattern Recognition (4H + 1D) ──
    chart_patterns = detect_patterns(symbol, klines_4h, klines_1d, current_price)

    return {
        "score": score, "max_score": max_score, "label": label,
        "details": details, "price": current_price,
        "pattern_note": pattern_notes.get("4h"),
        "pattern_note_4h": pattern_notes.get("4h"),
        "pattern_note_1h": pattern_notes.get("1h"),
        "pattern_notes": pattern_notes,
        "chart_patterns": chart_patterns,
        # Pass klines so build_powerful_entry can reuse — avoids duplicate API calls
        "klines_4h": klines_4h,
        "klines_1h": klines_1h,
        "klines_1d": klines_1d,
        "ticker": ticker,
    }


    cfg = TIMEFRAMES[tf]
    klines = get_klines(symbol, interval=tf, limit=50)
    if not klines or len(klines) < 10:
        return

    if tf != "5m":
        check_volume_buildup(symbol, tf, klines)
        # Trendline breakout: 4H/1D fire to existing High Priority flow.
        # 1H re-enabled — retests route to My Setups (not High Priority),
        # so the old noise concern no longer applies.
        if tf in ["4h", "1d", "1h"]:
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

    ft_score, ft_details = calc_followthrough_score(symbol, tf, klines, ratio, 0, price, change_24h)
    high_potential = ft_score >= 60
    is_distribution_warning = any("DISTRIBUTION WARNING" in d or "bearish (red)" in d for d in ft_details)
    ft_tag = ""
    if high_potential:
        ft_details_str = "\n   ".join(ft_details)
        ft_tag = f"\n\n🔥 <b>HIGH FOLLOW-THROUGH POTENTIAL ({ft_score})</b>\n   {ft_details_str}"
    elif is_distribution_warning:
        # Always surface this even though it's not a "high potential" tag —
        # the warning itself is the important information here, not a bonus.
        ft_details_str = "\n   ".join(ft_details)
        ft_tag = f"\n\n   {ft_details_str}"

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
        send_to_topic(TOPIC_BUILDUPS, msg)  # escalate to Building Momentum
    if sent:
        print(f"✅ [{cfg['label']}] Spike: {symbol} ({ratio:.1f}x) | FT score: {ft_score}{' [DISTRIBUTION WARNING]' if is_distribution_warning else ''}")
        signal_performance[f"{symbol}_spike_{tf}_{int(now)}"] = {
            "symbol": symbol, "signal_price": price,
            "signal_time": now, "signal_type": f"Volume Spike [{cfg['label']}]",
            "highest_after": price,
        }
        track_building_signal(symbol, f"Volume Spike [{cfg['label']}]", price)
        check_high_confidence_signal(symbol, f"Volume Spike [{cfg['label']}]", price)

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
def detect_fvg(klines, min_gap_pct=0.3):
    """
    Detects Fair Value Gaps (FVG) — a 3-candle pattern where candle 1's high
    and candle 3's low don't overlap, leaving an unfilled gap. These act as
    magnets for price to return and fill.

    Bullish FVG: candle3 low > candle1 high (gap above)
    Bearish FVG: candle3 high < candle1 low (gap below)

    min_gap_pct: minimum gap size as % of price (filters out tiny gaps)
    Returns list of FVGs, most recent first.
    """
    fvgs = []
    closed = klines[:-1]
    if len(closed) < 3:
        return fvgs

    for i in range(len(closed) - 2):
        c1 = closed[i]
        c3 = closed[i + 2]
        c1_high = float(c1[2])
        c1_low  = float(c1[3])
        c3_high = float(c3[2])
        c3_low  = float(c3[3])
        mid_price = (c1_high + c3_low) / 2

        # Bullish FVG
        if c3_low > c1_high:
            gap_pct = (c3_low - c1_high) / mid_price * 100
            if gap_pct >= min_gap_pct:
                fvgs.append({
                    "type": "bullish", "top": c3_low, "bottom": c1_high,
                    "mid": (c1_high + c3_low) / 2,
                    "gap_pct": gap_pct, "candle_idx": i,
                    "age": len(closed) - i - 2,
                })
        # Bearish FVG
        elif c3_high < c1_low:
            gap_pct = (c1_low - c3_high) / mid_price * 100
            if gap_pct >= min_gap_pct:
                fvgs.append({
                    "type": "bearish", "top": c1_low, "bottom": c3_high,
                    "mid": (c1_low + c3_high) / 2,
                    "gap_pct": gap_pct, "candle_idx": i,
                    "age": len(closed) - i - 2,
                })

    return list(reversed(fvgs))  # most recent first


def detect_ifvg(klines, current_price):
    """
    iFVG (Inverse FVG) — when price enters a bearish FVG and bounces back up
    (or enters a bullish FVG and drops), the FVG flips into a support/resistance
    level. This is a high-probability setup from the iFVG framework.

    Returns list of active iFVGs near current price.
    """
    fvgs = detect_fvg(klines)
    closed = klines[:-1]
    ifvgs = []

    for fvg in fvgs[:10]:  # check last 10 FVGs
        gap_top    = fvg["top"]
        gap_bottom = fvg["bottom"]
        gap_mid    = fvg["mid"]
        fvg_type   = fvg["type"]
        candle_idx = fvg["candle_idx"]

        # Check if price has entered and exited the FVG (making it an iFVG)
        entered = False
        exited_bullishly = False
        exited_bearishly = False

        subsequent = closed[candle_idx + 2:]
        for k in subsequent:
            low  = float(k[3])
            high = float(k[2])
            close = float(k[4])
            open_ = float(k[1])

            if not entered:
                if fvg_type == "bearish" and high >= gap_bottom:
                    entered = True
                elif fvg_type == "bullish" and low <= gap_top:
                    entered = True
            else:
                if fvg_type == "bearish" and close > gap_top and close > open_:
                    exited_bullishly = True
                    break
                elif fvg_type == "bullish" and close < gap_bottom and close < open_:
                    exited_bearishly = True
                    break

        if entered and (exited_bullishly or exited_bearishly):
            near = abs(current_price - gap_mid) / gap_mid <= 0.08  # within 8%
            if near:
                ifvgs.append({
                    "type": "bullish_ifvg" if exited_bullishly else "bearish_ifvg",
                    "top": gap_top, "bottom": gap_bottom, "mid": gap_mid,
                    "gap_pct": fvg["gap_pct"],
                    "original_fvg": fvg_type,
                })

    return ifvgs


def detect_equal_highs_lows(klines, current_price, tolerance=0.015):
    """
    Equal Highs (EQH) and Equal Lows (EQL) — price levels that have been tested
    multiple times at nearly the same price. These are liquidity pools:
    - EQH above price: sell-side stops clustered there (sweep → drop or pump)
    - EQL below price: buy-side stops clustered there (sweep → pump)

    Returns dict with eq_highs and eq_lows lists.
    """
    closed = klines[:-1]
    if len(closed) < 5:
        return {"eq_highs": [], "eq_lows": []}

    highs  = [float(k[2]) for k in closed[-30:]]
    lows   = [float(k[3]) for k in closed[-30:]]

    def find_equal_levels(prices, min_touches=2):
        levels = []
        used = set()
        for i, p1 in enumerate(prices):
            if i in used:
                continue
            group = [p1]
            indices = [i]
            for j, p2 in enumerate(prices):
                if j != i and j not in used:
                    if abs(p1 - p2) / p1 <= tolerance:
                        group.append(p2)
                        indices.append(j)
            if len(group) >= min_touches:
                for idx in indices:
                    used.add(idx)
                avg_price = sum(group) / len(group)
                levels.append({
                    "price": avg_price,
                    "touches": len(group),
                })
        return sorted(levels, key=lambda x: x["touches"], reverse=True)

    eq_highs_raw = find_equal_levels(highs)
    eq_lows_raw  = find_equal_levels(lows)

    # Filter: EQH above current price, EQL below
    eq_highs = [l for l in eq_highs_raw if l["price"] > current_price * 1.005][:3]
    eq_lows  = [l for l in eq_lows_raw  if l["price"] < current_price * 0.995][:3]

    return {"eq_highs": eq_highs, "eq_lows": eq_lows}


def analyze_ifvg_framework(symbol, current_price, tf="4h"):
    """
    Full iFVG framework analysis for /entry output:
    1. FVG — nearby unfilled gaps (price magnets)
    2. iFVG — flipped FVGs acting as S/R
    3. Equal Highs/Lows — liquidity pools (sweep targets)
    4. Delivery — is price delivering from an FVG or iFVG right now?

    Returns formatted string to embed in /entry message.
    """
    klines = get_klines(symbol, interval=tf, limit=50)
    klines_1h = get_klines(symbol, interval="1h", limit=50)
    if not klines or len(klines) < 10:
        return ""

    parts = []

    # ── FVGs ──
    fvgs = detect_fvg(klines, min_gap_pct=0.3)
    nearby_fvgs = [f for f in fvgs[:8]
                   if abs(f["mid"] - current_price) / current_price <= 0.10]

    if nearby_fvgs:
        fvg_lines = ["📊 <b>Fair Value Gaps (FVG):</b>"]
        for f in nearby_fvgs[:3]:
            direction = "above" if f["mid"] > current_price else "below"
            emoji = "🟢" if f["type"] == "bullish" else "🔴"
            fvg_lines.append(
                f"  {emoji} {f['type'].capitalize()} FVG {direction}: "
                f"{format_price(f['bottom'])} — {format_price(f['top'])} "
                f"({f['gap_pct']:.1f}% gap, {f['age']} candles ago)"
            )
        parts.append("\n".join(fvg_lines))

    # ── iFVGs ──
    ifvgs = detect_ifvg(klines, current_price)
    if not ifvgs and klines_1h:
        ifvgs = detect_ifvg(klines_1h, current_price)

    if ifvgs:
        ifvg_lines = ["🔄 <b>Inverse FVG (iFVG) — High Probability Setup:</b>"]
        for iv in ifvgs[:2]:
            direction = "support" if "bullish" in iv["type"] else "resistance"
            ifvg_lines.append(
                f"  ⭐ iFVG {direction}: {format_price(iv['bottom'])} — "
                f"{format_price(iv['top'])} ({iv['gap_pct']:.1f}% zone)\n"
                f"     Price previously swept this FVG and flipped it — "
                f"high-confidence bounce zone."
            )
        parts.append("\n".join(ifvg_lines))

    # ── Equal Highs/Lows ──
    eql = detect_equal_highs_lows(klines, current_price)
    eql_lines = []
    if eql["eq_highs"]:
        for h in eql["eq_highs"][:2]:
            pct = (h["price"] - current_price) / current_price * 100
            eql_lines.append(
                f"  🎯 EQH: {format_price(h['price'])} "
                f"(+{pct:.1f}%, {h['touches']}x touched) — "
                f"sell-side liquidity, sweep target / TP level"
            )
    if eql["eq_lows"]:
        for l in eql["eq_lows"][:2]:
            pct = (current_price - l["price"]) / current_price * 100
            eql_lines.append(
                f"  🎯 EQL: {format_price(l['price'])} "
                f"(-{pct:.1f}%, {l['touches']}x touched) — "
                f"buy-side liquidity, sweep target / SL zone"
            )
    if eql_lines:
        parts.append("📍 <b>Equal Highs/Lows (Liquidity Pools):</b>\n" + "\n".join(eql_lines))

    # ── Delivery check ──
    # Is price currently delivering from a nearby FVG/iFVG?
    delivery_note = ""
    if ifvgs:
        iv = ifvgs[0]
        if iv["bottom"] <= current_price <= iv["top"] * 1.02:
            delivery_note = (
                f"🚀 <b>Delivery Active</b> — price is currently inside/near "
                f"the iFVG zone ({format_price(iv['bottom'])}–{format_price(iv['top'])}). "
                f"This is a high-conviction entry zone per the iFVG framework."
            )
        elif current_price > iv["top"] and "bullish" in iv["type"]:
            delivery_note = (
                f"📦 <b>Post-delivery</b> — price has delivered out of the iFVG "
                f"({format_price(iv['bottom'])}–{format_price(iv['top'])}). "
                f"Wait for a pullback to the iFVG zone for re-entry."
            )
    if delivery_note:
        parts.append(delivery_note)

    if not parts:
        return ""

    # Plain language summary — "What this means for you"
    summary_lines = ["💡 <b>What this means for you:</b>"]
    nearest_sup = None
    nearest_res = None

    # Find nearest iFVG support/resistance
    for iv in ifvgs:
        if "bullish" in iv["type"] and iv["mid"] < current_price:
            if not nearest_sup or abs(iv["mid"] - current_price) < abs(nearest_sup["mid"] - current_price):
                nearest_sup = iv
        elif "bearish" in iv["type"] and iv["mid"] > current_price:
            if not nearest_res or abs(iv["mid"] - current_price) < abs(nearest_res["mid"] - current_price):
                nearest_res = iv

    if nearest_sup:
        pct = (current_price - nearest_sup["mid"]) / current_price * 100
        summary_lines.append(f"   → Strong support zone at {format_price(nearest_sup['bottom'])}–{format_price(nearest_sup['top'])} ({pct:.1f}% below) — price has bounced here before")
    if nearest_res:
        pct = (nearest_res["mid"] - current_price) / current_price * 100
        summary_lines.append(f"   → Resistance at {format_price(nearest_res['top'])} ({pct:.1f}% above) — sellers likely waiting there")

    # EQH/EQL targets
    if eql["eq_highs"]:
        best_h = eql["eq_highs"][0]
        pct = (best_h["price"] - current_price) / current_price * 100
        summary_lines.append(f"   → TP target: {format_price(best_h['price'])} (+{pct:.1f}%) — {best_h['touches']}x tested, big liquidity there")
    if eql["eq_lows"]:
        best_l = eql["eq_lows"][0]
        pct = (current_price - best_l["price"]) / current_price * 100
        summary_lines.append(f"   → SL zone: below {format_price(best_l['price'])} (-{pct:.1f}%) — if this breaks, trend changes")

    # Verdict
    if ifvgs and nearest_sup and current_price <= nearest_sup["top"] * 1.02:
        verdict = "✅ Price is at/near a high-probability bounce zone — wait for a green candle confirmation then enter"
    elif nearest_res and (nearest_res["mid"] - current_price) / current_price < 0.03:
        verdict = "⚠️ Price is near resistance — wait for a clear break above before entering"
    else:
        verdict = "⏳ No immediate high-probability setup — monitor these levels and wait for price to reach them"

    summary_lines.append(f"   → Verdict: {verdict}")
    parts.append("\n".join(summary_lines))

    return "🔬 <b>iFVG Framework Analysis:</b>\n\n" + "\n\n".join(parts)


def get_order_book_clusters(symbol, depth=100):
    """
    Fetches Binance order book and finds significant bid/ask clusters —
    price levels where large orders are stacked. These are liquidation
    targets: price sweeping a big bid cluster = stop losses triggered =
    fuel for a pump.
    Returns dict with buy_walls, sell_walls, and the nearest liquidation zone.
    """
    try:
        r = http_session.get(
            f"https://api.binance.com/api/v3/depth",
            params={"symbol": symbol, "limit": depth},
            timeout=8
        )
        if r.status_code != 200:
            return None
        data = r.json()
        bids = [(float(p), float(q)) for p, q in data.get("bids", [])]
        asks = [(float(p), float(q)) for p, q in data.get("asks", [])]
        if not bids or not asks:
            return None

        current_price = bids[0][0]  # best bid ≈ current

        # Find significant walls (clusters with large USDT value)
        def find_walls(orders, min_usdt=10000):  # lowered from 50K — catches small/mid cap walls
            walls = []
            for price, qty in orders:
                usdt_val = price * qty
                if usdt_val >= min_usdt:
                    walls.append({"price": price, "usdt": usdt_val})
            return sorted(walls, key=lambda x: x["usdt"], reverse=True)[:3]

        buy_walls  = find_walls(bids)   # support / liquidation targets below
        sell_walls = find_walls(asks)   # resistance / stop clusters above

        # Nearest liquidation zone = biggest buy wall just below current price
        liq_zone = None
        all_liq_zones = sorted(
            [w for w in buy_walls if w["price"] < current_price],
            key=lambda x: (-(x["price"] < current_price), -x["usdt"])
        )
        if all_liq_zones:
            liq_zone = all_liq_zones[0]

        return {
            "buy_walls": buy_walls,
            "sell_walls": sell_walls,
            "liq_zone": liq_zone,
            "all_liq_zones": all_liq_zones,
            "current_price": current_price,
        }
    except Exception as e:
        print(f"Order book error {symbol}: {e}")
        return None


def format_order_flow_block(ob_data, current_price):
    """Formats order flow data. Returns empty string if nothing meaningful."""
    if not ob_data:
        return ""

    lines = []

    if ob_data.get("sell_walls"):
        for w in ob_data["sell_walls"][:2]:
            lines.append(f"  🔴 Sell wall: {format_price(w['price'])} ({w['usdt']/1000:.0f}K USDT)")

    if ob_data.get("buy_walls"):
        for w in ob_data["buy_walls"][:2]:
            lines.append(f"  🟢 Buy wall: {format_price(w['price'])} ({w['usdt']/1000:.0f}K USDT)")

    # Show all significant liquidation zones, not just nearest
    liq_zones = ob_data.get("all_liq_zones", [])
    if not liq_zones and ob_data.get("liq_zone"):
        liq_zones = [ob_data["liq_zone"]]

    if liq_zones:
        for i, lz in enumerate(liq_zones[:3]):
            pct_away = (current_price - lz["price"]) / current_price * 100
            size_label = "🔥 Large" if lz["usdt"] >= 50000 else "⚡"
            lines.append(
                f"\n{size_label} Liquidation zone {i+1}: {format_price(lz['price'])} "
                f"({pct_away:.1f}% below) — {lz['usdt']/1000:.0f}K USDT\n"
                f"   💡 Watch for: price dipping briefly below this level (triggering stops), "
                f"then quickly reclaiming it with a green candle — that's your entry signal. "
                f"Don't enter on the dip — wait for the reclaim."
            )
        if len(liq_zones) > 1:
            biggest = max(liq_zones, key=lambda x: x["usdt"])
            if biggest != liq_zones[0]:
                lines.append(f"   ℹ️ Bigger pool at {format_price(biggest['price'])} ({biggest['usdt']/1000:.0f}K) — stronger pump potential if swept")

    if not lines:
        return ""

    return "💧 <b>Order Flow (Live Order Book):</b>\n" + "\n".join(lines)


# ── Liquidation sweep tracker ─────────────────────────────
# Tracks symbols being monitored after /entry for liquidation sweeps.
# {symbol: {liq_price, liq_usdt, entry_price, chat_id, started, swept, sweep_time}}
_liq_watch = {}

# Entry watch — tracks weak setups after /entry, monitors for improvement
# {symbol: {chat_id, entry_price, started, weak_reasons, entry_analysis,
#           vol_alerted, retest_alerted, trend_alerted, expires}}
_entry_watch = {}

def start_entry_watch(symbol, chat_id, entry_price, weak_reasons, entry_analysis):
    """
    Starts background monitoring after /entry when setup is weak.
    Monitors for: volume spike, retest confirm, daily trend shift, liq sweep.
    Fires to My Setups + personal DM when setup improves.
    """
    _entry_watch[symbol] = {
        "chat_id":        chat_id,
        "entry_price":    entry_price,
        "started":        time.time(),
        "weak_reasons":   weak_reasons,
        "entry_analysis": entry_analysis,
        "vol_alerted":    False,
        "retest_alerted": False,
        "trend_alerted":  False,
        "bs_alerted":     False,
        "expires":        time.time() + 48 * 3600,
    }
    print(f"👁 Entry watch started: {symbol} weak={weak_reasons}")


def check_entry_watches():
    """
    Runs every 5 minutes. For each symbol in _entry_watch:
    - Volume spike: if volume jumps to 3x+ → alert
    - Retest confirm: if any TF shows confirmed retest → alert
    - Daily trend shift: if was bearish, now bullish → alert
    - Expire after 48h
    """
    now = time.time()
    to_remove = []

    for symbol, watch in list(_entry_watch.items()):
        if now > watch["expires"]:
            to_remove.append(symbol)
            continue

        try:
            ticker = get_ticker(symbol)
            if not ticker:
                continue
            current_price = float(ticker["lastPrice"])
            change_24h    = float(ticker["priceChangePercent"])

            improvements = []

            # ── Volume spike check ──
            if not watch["vol_alerted"]:
                klines_1h = get_klines(symbol, interval="1h", limit=15)
                if klines_1h and len(klines_1h) >= 8:
                    closed = klines_1h[:-1]
                    vols = [float(k[5]) for k in closed]
                    baseline = sum(vols[:6]) / 6 if len(vols) >= 6 else 1
                    recent   = vols[-1] if vols else 0
                    vol_ratio = recent / baseline if baseline > 0 else 0
                    if vol_ratio >= 3.0:
                        improvements.append(f"⚡ Volume spike: {vol_ratio:.1f}x (was low before)")
                        _entry_watch[symbol]["vol_alerted"] = True

            # ── BS (Buy/Sell Pressure) turned positive ──
            if not watch.get("bs_alerted"):
                klines_1h_bs = get_klines(symbol, interval="1h", limit=6)
                if klines_1h_bs and len(klines_1h_bs) >= 3:
                    closed_bs = klines_1h_bs[:-1]
                    # Taker buy volume in field [9], total in [5]
                    bs_values = []
                    for k in closed_bs[-3:]:
                        total_vol = float(k[5])
                        buy_vol   = float(k[9]) if len(k) > 9 else total_vol * 0.5
                        sell_vol  = total_vol - buy_vol
                        bs_values.append(buy_vol - sell_vol)
                    net_bs = sum(bs_values)
                    was_negative = "bs_was_negative" in watch
                    if not was_negative and net_bs < 0:
                        _entry_watch[symbol]["bs_was_negative"] = True
                    elif was_negative and net_bs > 0:
                        bs_usdt = net_bs * float(ticker["lastPrice"])
                        improvements.append(
                            f"📈 Buy/Sell pressure turned POSITIVE (+{bs_usdt/1000:.1f}K USDT)\n"
                            f"   → Sellers backing off — entry window opening"
                        )
                        _entry_watch[symbol]["bs_alerted"] = True

            # ── Retest confirm check ──
            if not watch["retest_alerted"]:
                klines_4h = get_klines(symbol, interval="4h", limit=10)
                klines_1h = get_klines(symbol, interval="1h", limit=10)
                for tf, klines_tf in [("4h", klines_4h), ("1h", klines_1h)]:
                    if not klines_tf or len(klines_tf) < 5:
                        continue
                    closed = klines_tf[:-1]
                    last = closed[-1]
                    prev = closed[-2]
                    l_close = float(last[4])
                    l_open  = float(last[1])
                    l_vol   = float(last[5])
                    p_high  = float(prev[2])
                    avg_vol = sum(float(k[5]) for k in closed[-6:]) / 6 or 1
                    vol_r   = l_vol / avg_vol

                    # Green candle breaking above previous high with volume
                    if l_close > l_open and l_close > p_high and vol_r >= 1.5:
                        improvements.append(f"✅ Retest confirmed [{tf.upper()}] — green candle + volume ({vol_r:.1f}x)")
                        _entry_watch[symbol]["retest_alerted"] = True
                        _entry_watch[symbol]["confirmed_tf"] = tf
                        break

            # ── Daily trend shift ──
            daily_down_fresh = None
            if not watch["trend_alerted"] and "daily_down" in watch["weak_reasons"]:
                daily_down_fresh = is_daily_downtrend(symbol, current_price)
                if not daily_down_fresh:
                    improvements.append("✅ Daily trend shifted bullish — bearish filter cleared")
                    _entry_watch[symbol]["trend_alerted"] = True

            # ── Fire alert if improvements found ──
            # Note #4 redesign: accumulate improvements SILENTLY instead of
            # messaging on every single one (was disturbing/too frequent).
            # Only fire ONE combined message right when the pump actually
            # starts — and critically, don't wait for ALL improvements to
            # align first (good pumps happen without every criterion
            # improving), so this must not become over-conservative.
            if improvements:
                _entry_watch[symbol].setdefault("accumulated_improvements", []).extend(improvements)
                print(f"🔕 Entry watch improvement (silent): {symbol} — {improvements}")

            # ── Pump-starting detection (reuses the same pattern as
            # check_no_retest_pump_risk: strong body + volume + buy pressure)
            # — fires the ONE combined alert regardless of how many
            # improvements have accumulated, even zero.
            if not watch.get("pump_start_alerted"):
                klines_1h_ps = get_klines(symbol, interval="1h", limit=10)
                if klines_1h_ps and len(klines_1h_ps) >= 8:
                    closed_ps = klines_1h_ps[:-1]
                    last_ps = closed_ps[-1]
                    ps_open, ps_close = float(last_ps[1]), float(last_ps[4])
                    ps_vol = float(last_ps[5])
                    ps_buy = float(last_ps[9]) if len(last_ps) > 9 else ps_vol * 0.5
                    ps_buy_ratio = ps_buy / ps_vol if ps_vol > 0 else 0.5
                    prior_vols_ps = [float(k[5]) for k in closed_ps[-7:-1]]
                    avg_vol_ps = sum(prior_vols_ps) / len(prior_vols_ps) if prior_vols_ps else 1
                    ps_vol_ratio = ps_vol / avg_vol_ps if avg_vol_ps > 0 else 0
                    body_pct_ps = (ps_close - ps_open) / ps_open * 100 if ps_open > 0 else 0

                    pump_starting = (
                        ps_close > ps_open and body_pct_ps >= 2.0 and
                        ps_vol_ratio >= 3.0 and ps_buy_ratio >= 0.58
                    )
                    if pump_starting:
                        watch["pump_start_alerted"] = True
                        result = calc_entry_score(symbol)
                        if result:
                            ob_data = get_order_book_clusters(symbol)
                            confirmed_tf = watch.get("confirmed_tf")
                            if confirmed_tf and result.get("pattern_notes") is not None:
                                result["pattern_notes"][confirmed_tf] = "Retest confirmed"
                            entry_msg, _entry_meta = build_powerful_entry(
                                symbol, result, ob_data, daily_down_override=daily_down_fresh
                            )
                            all_improvements = _entry_watch[symbol].get("accumulated_improvements", [])
                            improvement_str = ("\n".join(f"  {i}" for i in all_improvements)
                                                if all_improvements else "  (no notable changes flagged, but pump signs detected)")
                            pct_from_entry = (current_price - watch["entry_price"]) / watch["entry_price"] * 100
                            alert = (
                                f"🚀 <b>PUMP STARTING — {symbol}</b>\n\n"
                                f"📈 <b>Accumulated changes since /entry:</b>\n{improvement_str}\n\n"
                                f"⚡ Volume: {ps_vol_ratio:.1f}x | Buy: {ps_buy_ratio*100:.0f}%\n"
                                f"💰 Now: {format_price(current_price)} ({pct_from_entry:+.1f}% from /entry price)\n\n"
                                f"━━━━━━━━━━━━━━\n"
                                f"{entry_msg}"
                            )
                            send_to_topic(TOPIC_MY_SETUPS, alert)
                            send_to(watch["chat_id"], alert)
                            print(f"🚀 Entry watch — pump starting: {symbol}")
                        to_remove.append(symbol)
                        continue

                # If all improvements found or 24h passed, remove from watch
                all_done = (
                    watch["vol_alerted"] and
                    watch["retest_alerted"] and
                    watch["trend_alerted"]
                )
                if all_done or now > watch["started"] + 24 * 3600:
                    to_remove.append(symbol)

        except Exception as e:
            print(f"Entry watch error {symbol}: {e}")

    for s in to_remove:
        _entry_watch.pop(s, None)


def start_liq_watch(symbol, ob_data, entry_price, chat_id, eql_price=None, eql_touches=None):
    """Called after /entry — starts background liquidation monitoring.
    Works even if no liq zone exists yet — will detect one as it develops.
    Prefers the EQL level (real historical liquidity pool flagged in the /entry
    analysis, e.g. 'Liquidity Hunt Risk') over the live order-book cluster,
    since EQL is the specific 'wait for sweep+reclaim' level shown to the user."""
    lz = ob_data.get("liq_zone") if ob_data else None
    if eql_price:
        watch_price, watch_source, watch_usdt = eql_price, "EQL", 0
    elif lz:
        watch_price, watch_source, watch_usdt = lz["price"], "OB", lz["usdt"]
    else:
        watch_price, watch_source, watch_usdt = None, None, 0

    _liq_watch[symbol] = {
        "liq_price":   watch_price,
        "liq_usdt":    watch_usdt,
        "liq_source":  watch_source,
        "liq_touches": eql_touches,
        "entry_price": entry_price,
        "chat_id":    chat_id,
        "started":    time.time(),
        "swept":      False,
        "sweep_time": None,
        "alerted":    False,
    }
    src_note = f" ({watch_source}{f', {eql_touches}x tested' if watch_source=='EQL' and eql_touches else ''})" if watch_source else ""
    print(f"💧 Liq watch started: {symbol} @ {format_price(watch_price) if watch_price else 'no zone yet'}{src_note}")


def check_liq_watches():
    """
    Runs every 30s. For each symbol in _liq_watch:
    - If price swept below liq_price with volume → mark as swept
    - If price reclaimed above liq_price after sweep → PUMP ALERT
    - Expire after 24h
    """
    now = time.time()
    to_remove = []

    for symbol, watch in list(_liq_watch.items()):
        if now - watch["started"] > 24 * 3600:
            to_remove.append(symbol)
            continue

        ticker = get_ticker(symbol)
        if not ticker:
            continue

        current_price = float(ticker["lastPrice"])
        liq_price = watch.get("liq_price")

        # If no liq zone yet, check if one has developed
        if not liq_price:
            ob_refresh = get_order_book_clusters(symbol)
            if ob_refresh and ob_refresh.get("liq_zone"):
                liq_price = ob_refresh["liq_zone"]["price"]
                _liq_watch[symbol]["liq_price"] = liq_price
                _liq_watch[symbol]["liq_usdt"]  = ob_refresh["liq_zone"]["usdt"]
                send_to(watch["chat_id"],
                    f"💧 <b>Liquidation zone found — {symbol}</b>\n"
                    f"New zone at {format_price(liq_price)} — now actively monitoring."
                )
            else:
                continue  # still no zone, keep waiting

        # Stage 1: detect sweep below liq zone
        if not watch["swept"]:
            klines_5m = get_klines(symbol, interval="5m", limit=8)
            if klines_5m and len(klines_5m) >= 4:
                last = klines_5m[-2]
                l_low   = float(last[3])
                l_close = float(last[4])
                l_vol   = float(last[5])
                avg_vol = sum(float(k[5]) for k in klines_5m[-6:-2]) / 4
                vol_ratio = l_vol / avg_vol if avg_vol > 0 else 0

                swept = l_low < liq_price * 0.999 and vol_ratio >= 2.0

                if swept:
                    _liq_watch[symbol]["swept"]      = True
                    _liq_watch[symbol]["sweep_time"] = now
                    _liq_watch[symbol]["sweep_low"]  = l_low
                    _liq_watch[symbol]["sweep_vol"]  = vol_ratio
                    send_to(watch["chat_id"],
                        f"⚡ <b>Liquidation zone swept — {symbol}</b>\n\n"
                        f"Price wicked to {format_price(l_low)} "
                        f"(below cluster at {format_price(liq_price)}) "
                        f"on {vol_ratio:.1f}x volume.\n\n"
                        f"⏳ Watching for reclaim — if price closes back above "
                        f"{format_price(liq_price)}, pump likely incoming."
                    )
                    print(f"⚡ Liq swept: {symbol} low={format_price(l_low)}")

        # Stage 2: after sweep, detect reclaim → pump alert
        elif not watch["alerted"]:
            time_since_sweep = now - watch["sweep_time"]
            if time_since_sweep > 4 * 3600:  # too long, no reclaim
                to_remove.append(symbol)
                continue

            klines_5m = get_klines(symbol, interval="5m", limit=6)
            if klines_5m and len(klines_5m) >= 3:
                last = klines_5m[-2]
                l_close = float(last[4])
                l_open  = float(last[1])
                l_vol   = float(last[5])
                avg_vol = sum(float(k[5]) for k in klines_5m[-6:-2]) / 4
                vol_ratio = l_vol / avg_vol if avg_vol > 0 else 0

                reclaimed = (
                    l_close > liq_price and
                    l_close > l_open and
                    vol_ratio >= 1.5
                )

                if reclaimed:
                    _liq_watch[symbol]["alerted"] = True
                    ob_new = get_order_book_clusters(symbol)
                    of_block = format_order_flow_block(ob_new, current_price) if ob_new else ""
                    pct_from_entry = (current_price - watch["entry_price"]) / watch["entry_price"] * 100

                    # Check if price is also inside a manual zone or near a manual line
                    combined_setup = None
                    for zone_id, zone in manual_zones.items():
                        if (zone["symbol"] == symbol and
                                zone["low"] <= current_price <= zone["high"] * 1.02):
                            combined_setup = f"📐 Zone: {format_price(zone['low'])}–{format_price(zone['high'])} [{zone['tf'].upper()} OB]"
                            break
                    if not combined_setup:
                        for line_id, line in manual_lines.items():
                            if (line["symbol"] == symbol and
                                    abs(current_price - line["price"]) / line["price"] <= 0.02):
                                combined_setup = f"📏 Line: {format_price(line['price'])} [{line['tf'].upper()}]"
                                break

                    # Build SL/TP for combined alert
                    klines_tp = get_klines(symbol, interval="4h", limit=50)
                    sl_price = watch.get("sweep_low", liq_price) * 0.98
                    tp1 = current_price * 1.05
                    tp2 = current_price * 1.10
                    rr = (tp1 - current_price) / (current_price - sl_price) if current_price > sl_price else 0

                    if combined_setup:
                        msg = (
                            f"🔥 <b>COMBINED SETUP — {symbol}</b>\n\n"
                            f"{combined_setup}\n"
                            f"💧 Liquidation swept: {format_price(watch.get('sweep_low', liq_price))} "
                            f"on {watch.get('sweep_vol', 0):.1f}x volume → reclaimed ✅\n\n"
                            f"📐 <b>Trade Plan:</b>\n"
                            f"   💰 Entry: {format_price(current_price)}\n"
                            f"   🔴 SL: {format_price(sl_price)} ({(current_price-sl_price)/current_price*100:.1f}%)\n"
                            f"   🟢 TP1: {format_price(tp1)} (+5%)\n"
                            f"   🟢 TP2: {format_price(tp2)} (+10%)\n"
                            f"   ⚖️ R/R: {rr:.1f}x\n\n"
                            f"💡 <i>Zone + Liquidation confluence = strongest setup. "
                            f"Confirm on chart and use stop-loss.</i>"
                        )
                    else:
                        msg = (
                            f"🔥 <b>LIQUIDATION SWEEP COMPLETE — {symbol}</b>\n\n"
                            f"💰 Current: {format_price(current_price)}\n"
                            f"📍 Swept {format_price(watch.get('sweep_low', liq_price))} "
                            f"on {watch.get('sweep_vol', 0):.1f}x volume\n"
                            f"✅ Reclaimed {format_price(liq_price)} — buyers absorbed sell stops\n"
                            f"📊 From your /entry price: {pct_from_entry:+.1f}%\n\n"
                            f"📐 <b>Quick plan:</b>\n"
                            f"   🔴 SL: {format_price(sl_price)}\n"
                            f"   🟢 TP1: {format_price(tp1)} (+5%) | TP2: {format_price(tp2)} (+10%)\n\n"
                            + (f"{of_block}\n\n" if of_block else "") +
                            f"💡 <i>Stop losses triggered = fuel consumed. "
                            f"High-probability entry window.</i>\n\n"
                            f"⚠️ <i>Confirm on chart and use a stop-loss.</i>"
                        )
                    send_to_topic(TOPIC_MY_SETUPS, msg)
                    send_to(watch["chat_id"], msg)
                    to_remove.append(symbol)
                    print(f"🔥 Liq reclaim alert: {symbol}{' [COMBINED]' if combined_setup else ''}")

                    # User request: weighted multi-factor "low retest risk"
                    # score for COMBINED SETUP → also-send-to-High-Priority
                    # decision. Volume is the heaviest-weighted factor (daily
                    # trend alone was too strict — a pump can start before
                    # daily trend has flipped, e.g. ALLOUSDT). Kept fast/
                    # cheap on purpose (reuses already-fetched data + one
                    # cheap 1H call) since timely delivery matters more than
                    # exhaustive analysis — don't want to miss the pump.
                    if combined_setup:
                        rr_score = 0
                        sweep_vol = watch.get("sweep_vol", 0)
                        # Volume — heaviest weight (up to 3 of ~6 points)
                        if sweep_vol >= 5.0 or vol_ratio >= 5.0:
                            rr_score += 3
                        elif sweep_vol >= 3.0 or vol_ratio >= 3.0:
                            rr_score += 2
                        elif sweep_vol >= 2.0 or vol_ratio >= 2.0:
                            rr_score += 1

                        # BS pressure conviction on the reclaim candle
                        l_buy = float(last[9]) if len(last) > 9 else l_vol * 0.5
                        buy_ratio_rc = l_buy / l_vol if l_vol > 0 else 0.5
                        if buy_ratio_rc >= 0.60:
                            rr_score += 1

                        # Daily trend — soft factor now, not a hard gate
                        if not is_daily_downtrend(symbol, current_price):
                            rr_score += 1

                        # Multi-timeframe SMC/HL check (1H higher lows)
                        klines_1h_rc = get_klines(symbol, interval="1h", limit=8)
                        if klines_1h_rc and len(klines_1h_rc) >= 6:
                            lows_1h_rc = [float(k[3]) for k in klines_1h_rc[:-1][-6:]]
                            if sum(1 for j in range(1, len(lows_1h_rc)) if lows_1h_rc[j] > lows_1h_rc[j-1]) >= 3:
                                rr_score += 1

                        # Liquidity sweep+reclaim and zone confluence are
                        # already guaranteed true here (that's what a
                        # COMBINED SETUP is) — no extra points needed, they're
                        # the baseline requirement, not differentiators.

                        low_retest_risk = rr_score >= 4
                        if low_retest_risk:
                            send_to_topic(TOPIC_HIGH,
                                f"🔥 <b>COMBINED SETUP (Low Retest Risk, score {rr_score}/6) — {symbol}</b>\n\n" + msg.split("\n\n", 1)[1]
                            )
                            print(f"🔥 Combined setup → High Priority (score {rr_score}/6): {symbol}")

                    # User clarification: LIQUIDATION SWEEP COMPLETE itself
                    # isn't the High Priority message — it feeds into the
                    # same confluence-scored Big Pump pipeline as everything
                    # else. Only once the pump ACTUALLY confirms (via
                    # check_big_pump_watches) does "Pump Confirmed" land in
                    # High Priority.
                    check_high_confidence_signal(symbol, "Liquidity Sweep+Reclaim", current_price)

    for s in to_remove:
        _liq_watch.pop(s, None)


def build_powerful_entry(sym, result, ob_data, daily_down_override=None):
    """
    Builds the new comprehensive /entry message with:
    - Daily trend + volume + SMC all-in-one
    - Long base / coil after pump detection
    - Liquidity hunt probability from EQL touches
    - Historical TP from actual resistance levels (not fixed %)
    - Smart bot opinion
    """
    current_price = result["price"]
    details_str   = "\n".join(result["details"])
    pattern_notes = result.get("pattern_notes", {})
    chart_patterns = result.get("chart_patterns", [])
    now = time.time()

    # ── Timeframe retest row ──
    tf_map = {"15m": "15M", "30m": "30M", "1h": "1H", "4h": "4H"}
    retest_parts = []
    confirmed_tfs   = []
    inprogress_tfs  = []
    for tf_key, tf_label in tf_map.items():
        note = pattern_notes.get(tf_key, "")
        if "confirmed" in note:
            retest_parts.append(f"{tf_label} ✅")
            confirmed_tfs.append(tf_key)
        elif "in progress" in note:
            retest_parts.append(f"{tf_label} ⏳")
            inprogress_tfs.append(tf_key)
        elif "extended" in note:
            retest_parts.append(f"{tf_label} 📈")
        else:
            retest_parts.append(f"{tf_label} ❌")
    retest_row = " | ".join(retest_parts)

    # ── Chart pattern one-line ──
    import re as _re
    pattern_line = ""
    if chart_patterns:
        m = _re.search(r'\[.+?\] <b>(.+?)</b>', chart_patterns[0])
        if m:
            pattern_line = m.group(1)

    # ── Fetch klines — use cached from calc_entry_score if available ──
    klines_4h = result.get("klines_4h") or get_klines(sym, interval="4h", limit=100)
    klines_1h = result.get("klines_1h") or get_klines(sym, interval="1h", limit=50)
    klines_1d = result.get("klines_1d") or get_klines(sym, interval="1d", limit=60)
    ticker    = result.get("ticker")    or get_ticker(sym)
    if not ticker or not klines_4h:
        return f"📊 {sym} — could not fetch data"

    change_24h = float(ticker["priceChangePercent"]) if ticker else 0
    vol_24h    = float(ticker.get("quoteVolume", 0)) if ticker else 0

    analysis_lines = []

    # ── Daily trend — use cached klines (or a caller-provided override so
    # this can't disagree with an earlier trend-shift check on the same call) ──
    daily_down = (
        daily_down_override if daily_down_override is not None
        else is_daily_downtrend(sym, current_price, klines_1h=klines_1h, klines_daily=klines_1d)
    )
    if not daily_down:
        analysis_lines.append("✅ Daily trend bullish/neutral")
    else:
        analysis_lines.append("❌ Daily trend bearish")

    # ── Volume analysis ──
    vol_label = ""
    avg_vol_ratio = 0
    if klines_4h and len(klines_4h) >= 10:
        closed_4h = klines_4h[:-1]
        vols = [float(k[5]) for k in closed_4h[-20:]]
        baseline = sum(vols[:10]) / 10 if len(vols) >= 10 else 1
        recent   = vols[-1] if vols else 0
        avg_vol_ratio = recent / baseline if baseline > 0 else 0
        if avg_vol_ratio >= 5:
            vol_label = f"⚡ Volume: {avg_vol_ratio:.1f}x — strong accumulation"
            analysis_lines.append(vol_label)
        elif avg_vol_ratio >= 2:
            vol_label = f"📊 Volume: {avg_vol_ratio:.1f}x — elevated"
            analysis_lines.append(vol_label)
        else:
            vol_label = f"⚠️ Volume: {avg_vol_ratio:.1f}x — low"
            analysis_lines.append(vol_label)

    # ── Higher lows ──
    if klines_1h and len(klines_1h) >= 8:
        lows_1h = [float(k[3]) for k in klines_1h[-8:-1]]
        hl_count = sum(1 for i in range(1, len(lows_1h)) if lows_1h[i] > lows_1h[i-1])
        if hl_count >= 4:
            analysis_lines.append("✅ Strong higher lows (1H)")
        elif hl_count >= 2:
            analysis_lines.append("✅ Higher lows forming (1H)")

    # ── OB zone ──
    ob_zone = None
    if klines_4h and len(klines_4h) >= 5:
        closed_4h = klines_4h[:-1]
        avg_v4h = sum(float(k[5]) for k in closed_4h[-10:]) / 10 or 1
        for k in reversed(closed_4h[-15:]):
            ko, kc, kh, kl, kv = float(k[1]), float(k[4]), float(k[2]), float(k[3]), float(k[5])
            if kc > ko and kv >= avg_v4h * 1.3 and kl <= current_price <= kh * 1.05:
                ob_zone = (kl, kh)
                # Note #9: label whether this OB is "valid" (swept + FVG +
                # unmitigated) using the same classifier as the background scanner.
                valid_ob = find_valid_order_block(sym, tf="4h")
                is_valid_ob = bool(valid_ob and abs(valid_ob["low"] - kl) < kl * 0.01 and abs(valid_ob["high"] - kh) < kh * 0.01)
                ob_label = " ✅ Valid OB (swept+FVG+unmitigated)" if is_valid_ob else ""
                analysis_lines.append(f"🔲 OB zone: {format_price(kl)}–{format_price(kh)}{ob_label}")
                break

    # ── iFVG ──
    ifvg_support = None
    if klines_4h:
        ifvgs = detect_ifvg(klines_4h, current_price)
        if ifvgs:
            iv = ifvgs[0]
            ifvg_support = iv
            analysis_lines.append(f"🔄 iFVG support: {format_price(iv['bottom'])}–{format_price(iv['top'])}")

    # ── Equal Highs/Lows (liquidity pools) ──
    eql_data = None
    nearest_eql = None
    nearest_eqh = None
    all_eqh = []
    if klines_4h:
        eql_data = detect_equal_highs_lows(klines_4h, current_price)
        if eql_data["eq_lows"]:
            nearest_eql = eql_data["eq_lows"][0]
        if eql_data["eq_highs"]:
            all_eqh = eql_data["eq_highs"]
            nearest_eqh = eql_data["eq_highs"][0]

    # ── Liquidity hunt probability ──
    liq_hunt_warning = ""
    if nearest_eql:
        eql_pct = (current_price - nearest_eql["price"]) / current_price * 100
        touches = nearest_eql["touches"]

        # Note #10 fix: check whether price has ALREADY dipped below this
        # EQL and reclaimed back above it recently, instead of always
        # framing it as a pending future risk.
        already_swept = False
        if klines_1h and len(klines_1h) >= 8 and current_price > nearest_eql["price"]:
            recent_1h = klines_1h[:-1][-8:]  # last 8 closed 1H candles
            for k in recent_1h:
                k_low = float(k[3])
                if k_low < nearest_eql["price"]:
                    already_swept = True
                    break

        if already_swept and touches >= 5 and eql_pct <= 20:
            liq_hunt_warning = (
                f"✅ Liquidity already swept & reclaimed: EQL {format_price(nearest_eql['price'])} "
                f"({touches}x tested) — price already reclaimed above this level\n"
                f"   → Stops already cleared, this is a completed positive signal, not a pending risk"
            )
        elif touches >= 8 and eql_pct <= 15:
            liq_hunt_warning = (
                f"⚠️ Liquidity Hunt Risk: EQL {format_price(nearest_eql['price'])} "
                f"({touches}x tested, {eql_pct:.1f}% below)\n"
                f"   → Smart money may sweep this before pump\n"
                f"   → If sweep + reclaim → strongest entry signal"
            )
        elif touches >= 5 and eql_pct <= 10:
            liq_hunt_warning = (
                f"💧 Liq zone: {format_price(nearest_eql['price'])} "
                f"({touches}x tested, {eql_pct:.1f}% below)"
            )

    # ── BS (Buy/Sell Pressure) ──
    bs_note = ""
    bs_positive = False
    if klines_1h and len(klines_1h) >= 4:
        closed_1h_bs = klines_1h[:-1]
        bs_sum = 0
        for k in closed_1h_bs[-3:]:
            total_v = float(k[5])
            buy_v   = float(k[9]) if len(k) > 9 else total_v * 0.5
            bs_sum += (buy_v - (total_v - buy_v))
        bs_usdt = bs_sum * current_price
        if bs_sum > 0:
            bs_note = f"✅ BS Pressure: Positive (+{bs_usdt/1000:.1f}K) — buyers in control"
            bs_positive = True
            analysis_lines.append(bs_note)
        else:
            bs_note = f"🔴 BS Pressure: Negative ({bs_usdt/1000:.1f}K) — sellers active, wait for flip"
            analysis_lines.append(bs_note)
            analysis_lines.append("   → Bot will alert when BS turns positive")
    tp_levels = []  # initialize before coil so pump high can be appended
    base_note = ""
    coil_note = ""
    if klines_1d and len(klines_1d) >= 10:
        closed_1d = klines_1d[:-1]

        # Long base: tight range for 14+ days
        recent_30d = closed_1d[-30:]
        if len(recent_30d) >= 14:
            highs_30d = [float(k[2]) for k in recent_30d]
            lows_30d  = [float(k[3]) for k in recent_30d]
            range_high = max(highs_30d)
            range_low  = min(lows_30d)
            range_pct  = (range_high - range_low) / range_low * 100 if range_low > 0 else 100

            # Count days in tight range (within 15% of current)
            days_in_range = sum(
                1 for k in recent_30d
                if abs(float(k[4]) - current_price) / current_price <= 0.15
            )

            if range_pct <= 20 and days_in_range >= 14:
                base_note = (
                    f"📦 Long Base: {days_in_range} days in "
                    f"{range_pct:.0f}% range ({format_price(range_low)}–{format_price(range_high)})\n"
                    f"   Volume declining during base → breakout energy building"
                )

        # Coil after pump: pumped 30%+ in last 60d then retraced
        if len(closed_1d) >= 20:
            past_high = max(float(k[2]) for k in closed_1d[-60:])
            past_low  = min(float(k[3]) for k in closed_1d[-60:])
            pump_pct  = (past_high - past_low) / past_low * 100 if past_low > 0 else 0
            retrace   = (past_high - current_price) / past_high * 100 if past_high > 0 else 0

            if pump_pct >= 40 and 20 <= retrace <= 70 and not base_note:
                # 2nd pump estimate: historically 50-100% of first pump
                est_low  = pump_pct * 0.5
                est_high = pump_pct * 1.0
                prev_high_pct = (past_high - current_price) / current_price * 100
                coil_note = (
                    f"🌀 Coil After Pump: +{pump_pct:.0f}% | Retracement: -{retrace:.0f}%\n"
                    f"   📈 2nd pump potential: +{est_low:.0f}–{est_high:.0f}% (historical)\n"
                    f"   🌙 Longer-term stretch target: previous high {format_price(past_high)} (+{prev_high_pct:.0f}%) "
                    f"— informational only, separate from TP1/TP2 below"
                )
                # Record this occurrence + show the pattern's own historical
                # track record (note #10) so a MEDIUM-score coil setup can be
                # sized on data instead of gut feel.
                if now - _coil_pattern_tracked.get(sym, 0) > 24 * 3600:
                    _coil_pattern_tracked[sym] = now
                    signal_performance[f"{sym}_coilpattern_{int(now)}"] = {
                        "symbol": sym, "signal_price": current_price,
                        "signal_time": now, "signal_type": "Coil After Pump",
                        "highest_after": current_price,
                    }
                pattern_stats = get_pattern_history_stats("Coil After Pump")
                if pattern_stats:
                    coil_note += f"\n   {pattern_stats}"
                # NOTE (note #8): previous pump high is intentionally NOT
                # added to tp_levels — user wants TP1/TP2 to always stay
                # near-term/nearest-resistance based. The coil target above
                # is shown as an informational stretch target only.

    # ── Historical TP from actual resistance levels ──
    if klines_4h and len(klines_4h) >= 10:
        closed_4h = klines_4h[:-1]
        seen = set()
        for i in range(1, len(closed_4h) - 1):
            h = float(closed_4h[i][2])
            if h <= current_price * 1.01:
                continue
            if all(h >= float(closed_4h[i+j][2]) for j in [-1, 1]):
                # Cluster: bucket by 2% of current price
                bucket = int(h / (current_price * 0.02))
                if bucket not in seen:
                    seen.add(bucket)
                    pct = (h - current_price) / current_price * 100
                    tp_levels.append((h, pct))

        # EQH levels
        for eqh in all_eqh[:3]:
            pct = (eqh["price"] - current_price) / current_price * 100
            if pct > 2:
                tp_levels.append((eqh["price"], pct))

        # 1D swing highs
        if klines_1d:
            for k in klines_1d[-30:]:
                h = float(k[2])
                if h > current_price * 1.02:
                    pct = (h - current_price) / current_price * 100
                    tp_levels.append((h, pct))

    # Deduplicate — cluster levels within 2% of each other, keep highest
    # Also enforce minimum distances: TP1 >= 3%, TP2 >= 8% from current
    tp_levels_clean = []
    tp_levels_sorted = sorted(set(tp_levels), key=lambda x: x[0])
    prev_p = 0
    for p, pct in tp_levels_sorted:
        if pct < 3.0:  # skip levels too close to current price
            continue
        if prev_p == 0 or (p - prev_p) / prev_p > 0.05:  # at least 5% apart
            tp_levels_clean.append((p, pct))
            prev_p = p
    # If we have < 2 TPs, add percentage-based fallbacks
    if len(tp_levels_clean) == 0:
        tp_levels_clean.append((current_price * 1.05, 5.0))
        tp_levels_clean.append((current_price * 1.15, 15.0))
    elif len(tp_levels_clean) == 1:
        tp2_p = tp_levels_clean[0][0] * 1.10
        tp_levels_clean.append((tp2_p, (tp2_p - current_price) / current_price * 100))
    tp_levels = tp_levels_clean[:3]

    # ── SL — below EQL or OB or recent swing low ──
    sl_price = None
    sl_reason = ""
    if nearest_eql and (current_price - nearest_eql["price"]) / current_price <= 0.20:
        sl_price  = nearest_eql["price"] * 0.985
        sl_reason = f"below EQL {format_price(nearest_eql['price'])} ({nearest_eql['touches']}x)"
    elif ob_zone:
        sl_price  = ob_zone[0] * 0.985
        sl_reason = f"below OB zone"
    elif klines_4h:
        recent_lows = [float(k[3]) for k in klines_4h[-10:-1]]
        if recent_lows:
            sl_price  = min(recent_lows) * 0.985
            sl_reason = "below recent swing low"

    if not sl_price:
        sl_price = current_price * 0.92

    risk_pct = (current_price - sl_price) / current_price * 100

    # ── R/R calculation ──
    tp_rr = []
    for tp_p, tp_pct in tp_levels[:2]:
        rr = tp_pct / risk_pct if risk_pct > 0 else 0
        tp_rr.append((tp_p, tp_pct, rr))

    # Note #1/#5 fix: TP2 minimum floor of 20%. Nearest-resistance-based TP2
    # was often much smaller than the coil "2nd pump potential" — user
    # targets 20-30%+ coins, so TP2 shouldn't undersell that. Search WIDER
    # before falling back to a flat percentage: (1) check the full
    # tp_levels_clean list (not just the top-3 truncated tp_levels), (2) if
    # still nothing, fetch a longer 1D history (120 days) and search again.
    # Only use the flat +20% synthetic target as an absolute last resort —
    # user wants a real % (17%, 23%, whatever it actually is) reported
    # whenever a genuine resistance level exists.
    if len(tp_rr) >= 2 and tp_rr[1][1] < 20.0:
        better_tp2 = next((lvl for lvl in tp_levels_clean if lvl[1] >= 20.0), None)
        if not better_tp2:
            klines_1d_wide = get_klines(sym, interval="1d", limit=120)
            if klines_1d_wide:
                wide_highs = sorted(set(
                    (float(k[2]), (float(k[2]) - current_price) / current_price * 100)
                    for k in klines_1d_wide[-120:]
                    if float(k[2]) > current_price * 1.20
                ), key=lambda x: x[1])
                if wide_highs:
                    better_tp2 = wide_highs[0]
        if better_tp2:
            tp2_p, tp2_pct = better_tp2
        else:
            tp2_p, tp2_pct = current_price * 1.20, 20.0
        tp2_rr = tp2_pct / risk_pct if risk_pct > 0 else 0
        tp_rr[1] = (tp2_p, tp2_pct, tp2_rr)

    # ── Smart Bot Opinion ──
    opinion_lines = []

    # Entry timing
    if confirmed_tfs and not daily_down:
        if avg_vol_ratio >= 10:
            # Extreme volume can mean strong accumulation OR a blow-off-top —
            # ZBTUSDT reversed hard right after a 10.1x-volume "entry valid
            # now" alert. Soften the language instead of a flat green light.
            opinion_lines.append(
                f"⚠️ {'/'.join(t.upper() for t in confirmed_tfs)} confirmed, "
                f"but volume is extreme ({avg_vol_ratio:.1f}x) — could be strong "
                f"accumulation OR a blow-off-top. Consider a smaller starter position."
            )
        else:
            opinion_lines.append(f"✅ {'/'.join(t.upper() for t in confirmed_tfs)} confirmed — entry valid now")
    elif inprogress_tfs:
        opinion_lines.append(f"⏳ Wait for {'/'.join(t.upper() for t in inprogress_tfs).upper()} green candle to confirm")
    elif base_note or coil_note:
        opinion_lines.append("📦 No immediate retest but base/coil structure present — consider small position")
    else:
        opinion_lines.append("⏳ No clear setup yet — wait for price to reach key levels")

    # Liq hunt advice
    if liq_hunt_warning and nearest_eql:
        eql_pct = (current_price - nearest_eql["price"]) / current_price * 100
        opinion_lines.append(
            f"💡 Best entry: wait for sweep of {format_price(nearest_eql['price'])} "
            f"({eql_pct:.1f}% below) then reclaim = high conviction entry"
        )

    # Volume pump probability
    if avg_vol_ratio >= 5:
        opinion_lines.append("🔥 High volume = strong pump probability if breakout holds")
    elif avg_vol_ratio >= 2 and (base_note or coil_note):
        opinion_lines.append("📈 Volume + base = good risk/reward for patient hold")
    elif avg_vol_ratio < 1.5 and not confirmed_tfs:
        opinion_lines.append("⚠️ Low volume — entry premature, wait for volume confirmation")

    # Daily downtrend warning
    if daily_down:
        opinion_lines.append("⚠️ Daily bearish — use smaller position, tighter SL")

    opinion = "\n   ".join(opinion_lines)

    # ── Build final message ──
    lines = [
        f"📊 <b>{sym}</b> | {format_price(current_price)} | {result['label']} ({result['score']}/{result['max_score']})\n",
        f"📐 Retest: {retest_row}",
    ]
    if pattern_line:
        lines.append(f"📐 {pattern_line}")
    if base_note:
        lines.append(f"\n{base_note}")
    if coil_note:
        lines.append(f"\n{coil_note}")

    lines.append(f"\n📊 <b>Analysis:</b>")
    for al in analysis_lines:
        lines.append(f"   {al}")
    if liq_hunt_warning:
        lines.append(f"   {liq_hunt_warning}")

    # Note #3 part 2: flag a resistance zone above with repeated rejection
    # history, and compare this attempt's volume against past attempts.
    rejection_note = check_resistance_rejection_history(sym, current_price, current_vol_ratio=avg_vol_ratio)
    if rejection_note:
        lines.append(f"   {rejection_note}")

    # Trade plan
    tp_str = ""
    for i, (tp_p, tp_pct, rr) in enumerate(tp_rr, 1):
        tp_str += f"\n   🟢 TP{i}: {format_price(tp_p)} (+{tp_pct:.1f}%) | R/R: {rr:.1f}x"
    if not tp_rr and tp_levels:
        tp_p, tp_pct = tp_levels[0]
        tp_str = f"\n   🟢 TP1: {format_price(tp_p)} (+{tp_pct:.1f}%)"

    lines.append(
        f"\n📐 <b>Trade Plan:</b>\n"
        f"   💰 Entry: {format_price(current_price)}\n"
        f"   🔴 SL: {format_price(sl_price)} (-{risk_pct:.1f}%) ← {sl_reason}"
        + tp_str
    )

    lines.append(f"\n🤖 <b>Opinion:</b>\n   {opinion}")

    # ── Zone suggestions ──
    zone_suggestions = []
    sym_short = sym.replace("USDT", "")

    # Current bounce zone (OB or support)
    if ob_zone:
        margin = ob_zone[0] * 0.01
        zone_suggestions.append(
            f"🟡 <code>/addzone {sym_short} {format_price(ob_zone[0]-margin)} {format_price(ob_zone[1]+margin)} 4h</code>"
            f" ← current bounce zone"
        )
    # EQL sweep zone
    if nearest_eql:
        margin = nearest_eql["price"] * 0.015
        pct = (current_price - nearest_eql["price"]) / current_price * 100
        zone_suggestions.append(
            f"🔴 <code>/addzone {sym_short} {format_price(nearest_eql['price']-margin)} {format_price(nearest_eql['price']+margin)} 4h</code>"
            f" ← EQL sweep zone ({pct:.1f}% below, {nearest_eql['touches']}x tested)"
        )
    if zone_suggestions:
        lines.append(f"\n📏 <b>Zones to add:</b>\n" + "\n".join(zone_suggestions))
        lines.append("💡 Add both — whichever fires first = your entry signal")

    entry_meta = {
        "eql_price":   nearest_eql["price"]   if nearest_eql else None,
        "eql_touches": nearest_eql["touches"] if nearest_eql else None,
    }
    return "\n".join(lines), entry_meta


def get_quick_confluence_score(symbol, current_price):
    """
    Lightweight version of the extra-confluence scoring inside
    build_entry_decision_block (daily trend, BS pressure, OB zone, liq
    sweep presence) without building the full text block — used by the
    confirmation-message follow-up watcher (note #6) to detect improvement.
    Returns an int 0-4.
    """
    try:
        klines_4h = get_klines(symbol, interval="4h", limit=30)
        klines_1h = get_klines(symbol, interval="1h", limit=20)
        if not klines_4h:
            return 0
        closed_4h = klines_4h[:-1]
        score = 0

        if not is_daily_downtrend(symbol, current_price):
            score += 1

        eql_data = detect_equal_highs_lows(klines_4h, current_price)
        if eql_data.get("eq_lows"):
            score += 1

        if klines_1h and len(klines_1h) >= 3:
            last_1h = klines_1h[:-1][-1]
            vol_1h = float(last_1h[5])
            buy_1h = float(last_1h[9]) if len(last_1h) > 9 else vol_1h * 0.5
            buy_ratio = buy_1h / vol_1h if vol_1h > 0 else 0.5
            if buy_ratio >= 0.55:
                score += 1

        if closed_4h and len(closed_4h) >= 10:
            avg_v4h = sum(float(k[5]) for k in closed_4h[-10:]) / 10 or 1
            for k in reversed(closed_4h[-15:]):
                ko, kc, kh, kl, kv = float(k[1]), float(k[4]), float(k[2]), float(k[3]), float(k[5])
                if kc > ko and kv >= avg_v4h * 1.3 and kl <= current_price <= kh * 1.05:
                    score += 1
                    break

        return score
    except Exception:
        return 0


_confirm_watch = {}  # {symbol: {"topic": id, "score": int, "started": time, "label": str}}

def start_confirm_watch(symbol, current_price, topic, label):
    """Registers a confirmation-style alert (Zone Confirmed, Line Break
    Confirmed, etc) for follow-up monitoring (note #6) — if confluence
    improves afterward, a follow-up goes back to the SAME topic."""
    _confirm_watch[symbol] = {
        "topic": topic,
        "score": get_quick_confluence_score(symbol, current_price),
        "started": time.time(),
        "label": label,
    }

def check_confirm_watches():
    """Runs periodically — if a watched symbol's confluence score has
    improved since its confirmation alert fired, sends a follow-up to the
    same topic. Expires after 48h."""
    now = time.time()
    to_remove = []
    for symbol, watch in list(_confirm_watch.items()):
        if now - watch["started"] > 48 * 3600:
            to_remove.append(symbol)
            continue
        ticker = get_ticker(symbol)
        if not ticker:
            continue
        current_price = float(ticker["lastPrice"])
        new_score = get_quick_confluence_score(symbol, current_price)
        if new_score > watch["score"]:
            send_to_topic(watch["topic"],
                f"📈 <b>Confluence Improved — {symbol}</b>\n\n"
                f"Since the earlier {watch['label']} alert, confluence went "
                f"{watch['score']}/4 → <b>{new_score}/4</b>.\n"
                f"💰 Now: {format_price(current_price)}\n\n"
                f"⚠️ <i>Confirm on chart before entry.</i>"
            )
            watch["score"] = new_score
            print(f"📈 Confirm watch improved: {symbol} {watch['score']}/4")
    for s in to_remove:
        _confirm_watch.pop(s, None)


_prospect_watch = {}  # {symbol: {"signal_type": str, "started": time}}

def start_prospect_watch(symbol, signal_type):
    """
    Note #2: registers a "prospect" signal (Large Trade Detected, Big Pump
    Setup, Pre-pump Breakout Phase 3, Range Breakout, Gradual Buildup, High
    Confidence) for continued monitoring — these often fire before real
    price impact/OB/FVG exists yet. check_prospect_watches() re-runs the
    same instant-analysis (check_high_confidence_signal) on the symbol going
    forward; if/when confluence clears the bar (pump actually developing),
    it fires normally through that pipeline (including the Top Picks copy
    for perfect 6/6 scores).
    """
    if symbol not in _prospect_watch:
        _prospect_watch[symbol] = {"signal_type": signal_type, "started": time.time()}

def check_prospect_watches():
    """Runs periodically for up to 24h per prospect signal."""
    now = time.time()
    to_remove = []
    for symbol, watch in list(_prospect_watch.items()):
        if now - watch["started"] > 24 * 3600:
            to_remove.append(symbol)
            continue
        try:
            ticker = get_ticker(symbol)
            if not ticker:
                continue
            current_price = float(ticker["lastPrice"])
            check_high_confidence_signal(symbol, watch["signal_type"], current_price)
        except Exception as e:
            print(f"Prospect watch error {symbol}: {e}")
    for s in to_remove:
        _prospect_watch.pop(s, None)


_hc_followup_watch = {}  # {symbol: {"score": int, "started": time}}

def start_hc_followup_watch(symbol, current_price):
    """
    Note #4: distinct from note #2's prospect watch — this tracks a symbol
    AFTER a High Priority (⭐ HIGH CONFIDENCE) alert already fired. If
    confluence merely improves afterward (e.g. BS Negative → Positive), a
    follow-up goes to Top Picks (not re-sent to High Priority).
    """
    _hc_followup_watch[symbol] = {
        "score": get_quick_confluence_score(symbol, current_price),
        "started": time.time(),
    }

def check_hc_followup_watches():
    """Runs periodically for up to 48h per High Priority alert."""
    now = time.time()
    to_remove = []
    for symbol, watch in list(_hc_followup_watch.items()):
        if now - watch["started"] > 48 * 3600:
            to_remove.append(symbol)
            continue
        try:
            ticker = get_ticker(symbol)
            if not ticker:
                continue
            current_price = float(ticker["lastPrice"])
            new_score = get_quick_confluence_score(symbol, current_price)
            if new_score > watch["score"]:
                send_to_topic(TOPIC_TOP_PICKS,
                    f"📈 <b>High Priority Setup Improved — {symbol}</b>\n\n"
                    f"Confluence improved since the earlier High Priority alert: "
                    f"{watch['score']}/4 → <b>{new_score}/4</b>\n"
                    f"💰 Now: {format_price(current_price)}\n\n"
                    f"⚠️ <i>Confirm on chart before entry.</i>"
                )
                watch["score"] = new_score
                print(f"📈 HC follow-up improved: {symbol} {new_score}/4")
        except Exception as e:
            print(f"HC follow-up watch error {symbol}: {e}")
    for s in to_remove:
        _hc_followup_watch.pop(s, None)


def build_entry_decision_block(symbol, current_price, tf="4h"):
    """
    Generates a complete entry decision block for retest completion messages
    (/watch, /addline, zone confirm, line break confirm). Combines confluence
    analysis with a ready-to-use trade plan: entry confirmation, SL, TP
    targets, and a decision summary so the user can act immediately without
    having to run /entry separately.

    Note #6: now also checks liquidity sweep (EQL), BS pressure, and OB zone
    confluence — matching /entry's build_powerful_entry depth — and computes
    an overall score that the decision text factors in (not just raw R/R).
    """
    # Full confluence (trendline sweep + fib + volume/HL + key levels)
    confluence = build_full_confluence_block(symbol, current_price, tf=tf)

    # Get key levels for trade plan
    klines_4h = get_klines(symbol, interval="4h", limit=100)
    klines_1h = get_klines(symbol, interval="1h", limit=20)
    ticker    = get_ticker(symbol)
    if not klines_4h or not ticker:
        return confluence  # fallback to just confluence if data unavailable

    change_24h = float(ticker["priceChangePercent"])
    closed_4h  = klines_4h[:-1]
    highs = [float(k[2]) for k in closed_4h]
    lows  = [float(k[3]) for k in closed_4h]

    # Wider lookback — include 1D for meaningful targets
    klines_1d_tp = get_klines(symbol, interval="1d", limit=60)
    if klines_1d_tp:
        highs += [float(k[2]) for k in klines_1d_tp[:-1]]

    res_above = sorted([h for h in highs if h > current_price * 1.005])
    sup_below = sorted([l for l in lows  if l < current_price * 0.995], reverse=True)

    nearest_sup = sup_below[0] if sup_below else current_price * 0.95
    sl          = nearest_sup * 0.985

    def find_res_min(res_list, min_pct):
        min_price = current_price * (1 + min_pct / 100)
        candidates = [r for r in res_list if r >= min_price]
        return candidates[0] if candidates else current_price * (1 + min_pct / 100)

    tp1 = find_res_min(res_above, 5)
    tp2 = find_res_min(res_above, 10)
    tp2 = max(tp2, tp1 * 1.05)

    risk    = (current_price - sl) / current_price * 100
    reward1 = (tp1 - current_price) / current_price * 100
    reward2 = (tp2 - current_price) / current_price * 100
    rr      = reward1 / risk if risk > 0 else 0

    # ── Note #6: additional confluence — liq sweep, BS pressure, OB zone ──
    extra_lines = []
    extra_score = 0

    daily_down = is_daily_downtrend(symbol, current_price)

    eql_data = detect_equal_highs_lows(klines_4h, current_price)
    nearest_eql = eql_data["eq_lows"][0] if eql_data.get("eq_lows") else None
    if nearest_eql:
        extra_score += 1
        pct_below = (current_price - nearest_eql["price"]) / current_price * 100
        extra_lines.append(f"💧 Liq sweep zone: EQL {format_price(nearest_eql['price'])} ({nearest_eql['touches']}x tested, {pct_below:.1f}% below)")

    buy_ratio = None
    if klines_1h and len(klines_1h) >= 3:
        last_1h = klines_1h[:-1][-1]
        vol_1h = float(last_1h[5])
        buy_1h = float(last_1h[9]) if len(last_1h) > 9 else vol_1h * 0.5
        buy_ratio = buy_1h / vol_1h if vol_1h > 0 else 0.5
        if buy_ratio >= 0.55:
            extra_score += 1
            extra_lines.append(f"✅ BS Pressure: Positive (+{buy_ratio*100:.0f}% buy) — buyers in control")
        else:
            extra_lines.append(f"🔴 BS Pressure: Negative ({buy_ratio*100:.0f}% buy) — sellers active")

    ob_zone_here = None
    avg_v4h = sum(float(k[5]) for k in closed_4h[-10:]) / 10 or 1
    for k in reversed(closed_4h[-15:]):
        ko, kc, kh, kl, kv = float(k[1]), float(k[4]), float(k[2]), float(k[3]), float(k[5])
        if kc > ko and kv >= avg_v4h * 1.3 and kl <= current_price <= kh * 1.05:
            ob_zone_here = (kl, kh)
            extra_score += 1
            extra_lines.append(f"🔲 OB zone confluence: {format_price(kl)}–{format_price(kh)}")
            break

    if not daily_down:
        extra_score += 1

    # Decision quality — now factors in the extra confluence, not just R/R
    is_bearish = change_24h < -5 or daily_down
    rr_ok      = rr >= 2.0
    strong_confluence = extra_score >= 3  # e.g. liq sweep + BS positive + OB zone all present

    if strong_confluence and not is_bearish:
        decision_emoji = "✅"
        decision_text  = f"Strong confluence ({extra_score}/4) backs this even with R/R at {rr:.1f}x — setup looks solid, confirm on chart then enter"
    elif is_bearish and not rr_ok and extra_score < 2:
        decision_emoji = "⚠️"
        decision_text  = "Daily trend bearish + R/R weak + limited confluence — consider skipping or very small size"
    elif is_bearish and extra_score >= 2:
        decision_emoji = "🟡"
        decision_text  = f"Daily bearish but confluence ({extra_score}/4) is decent — small position, tight SL"
    elif is_bearish:
        decision_emoji = "🟡"
        decision_text  = "Daily trend bearish but R/R acceptable — small position, tight SL"
    elif not rr_ok and extra_score < 2:
        decision_emoji = "🟡"
        decision_text  = "R/R below 2:1 and confluence limited — consider waiting for a deeper retest or skip"
    elif not rr_ok:
        decision_emoji = "🟡"
        decision_text  = f"R/R below 2:1, but confluence ({extra_score}/4) is meaningful — smaller position could work"
    else:
        decision_emoji = "✅"
        decision_text  = "Setup looks clean — R/R favorable, confirm on chart then enter"

    trade_block = (
        f"\n🎯 <b>Entry Decision (2-10 day hold):</b>\n"
        + ("   " + "\n   ".join(extra_lines) + "\n" if extra_lines else "")
        + f"   💰 Entry: {format_price(current_price)}\n"
        f"   🔴 SL: {format_price(sl)} (-{risk:.1f}%)\n"
        f"   🟢 TP1: {format_price(tp1)} (+{reward1:.1f}%)\n"
        f"   🟢 TP2: {format_price(tp2)} (+{reward2:.1f}%)\n"
        f"   ⚖️ R/R: {rr:.1f}x\n\n"
        f"{decision_emoji} {decision_text}"
    )

    parts = []
    if confluence:
        parts.append(confluence)
    parts.append(trade_block)
    return "\n\n".join(parts)


def suggest_entry_action(symbol, current_price, score, label, pattern_notes, chart_patterns):
    """
    Generates ready-to-use /addline commands for 1H, 4H, and 1D timeframes
    based on the nearest resistance levels, pattern context, and trend.
    Also calculates SL (below nearest support) and TP targets (next resistances).
    Returns a formatted string ready to embed in /entry output.
    """
    # Get key levels
    klines_4h = get_klines(symbol, interval="4h", limit=100)
    klines_1h = get_klines(symbol, interval="1h", limit=50)
    ticker    = get_ticker(symbol)
    if not klines_4h or not ticker:
        return ""

    change_24h = float(ticker["priceChangePercent"])

    # Find supports and resistances from 4H
    closed_4h = klines_4h[:-1]
    highs = [float(k[2]) for k in closed_4h]
    lows  = [float(k[3]) for k in closed_4h]

    # Swing highs above current price (resistances) — use wider lookback
    klines_1d_tp = get_klines(symbol, interval="1d", limit=60)
    all_highs = highs[:]
    if klines_1d_tp:
        all_highs += [float(k[2]) for k in klines_1d_tp[:-1]]

    res_above = sorted(set(
        round(h, 8) for h in all_highs
        if h > current_price * 1.005
    ))
    # Swing lows below current price (supports)
    sup_below = sorted(set(
        round(l, 8) for l in lows
        if l < current_price * 0.995
    ), reverse=True)

    nearest_sup = sup_below[0] if sup_below else current_price * 0.95
    sl = nearest_sup * 0.985

    # TP targets — enforce minimum distances for 2-10 day holds
    # TP1: at least 5% away, TP2: at least 10%, TP3: at least 20%
    def find_res_above_min(res_list, min_pct):
        min_price = current_price * (1 + min_pct / 100)
        candidates = [r for r in res_list if r >= min_price]
        return candidates[0] if candidates else current_price * (1 + min_pct / 100)

    tp1 = find_res_above_min(res_above, 5)
    tp2 = find_res_above_min(res_above, 10)
    tp3 = find_res_above_min(res_above, 20)

    # Ensure they're strictly increasing
    tp2 = max(tp2, tp1 * 1.05)
    tp3 = max(tp3, tp2 * 1.08)

    # Use nearest res for entry line (not TP)
    nearest_res = res_above[0] if res_above else current_price * 1.02
    second_res  = res_above[1] if len(res_above) > 1 else nearest_res * 1.05

    # Risk/reward
    risk    = (current_price - sl) / current_price * 100
    reward1 = (tp1 - current_price) / current_price * 100
    rr1     = reward1 / risk if risk > 0 else 0

    # Determine best entry strategy per timeframe
    line_1h = nearest_res * 1.002
    line_4h = nearest_res * 1.005
    line_1d = second_res  * 1.002

    # Context flags
    is_bearish_daily = change_24h < -5 or "bearish" in " ".join(pattern_notes.values()).lower()
    has_retest = any("in progress" in v or "confirmed" in v for v in pattern_notes.values())
    is_high_score = score >= 70

    lines = ["\n💡 <b>Suggested Entry Levels:</b>"]

    # 1H — fastest entry
    lines.append(
        f"⚡ <b>1H (Aggressive)</b> — catch the move early:\n"
        f"   <code>/addline {symbol.replace('USDT','')} {format_price(line_1h)} 1h</code>\n"
        f"   Alert fires as soon as {format_price(nearest_res)} breaks with volume"
    )

    # 4H — balanced
    lines.append(
        f"📊 <b>4H (Balanced)</b> — confirmed breakout:\n"
        f"   <code>/addline {symbol.replace('USDT','')} {format_price(line_4h)} 4h</code>\n"
        f"   Waits for 4H candle close above resistance"
    )

    # 1D — conservative (especially if bearish daily)
    conservative_note = "recommended given daily bearish trend" if is_bearish_daily else "safest entry, least noise"
    lines.append(
        f"🛡 <b>1D (Conservative)</b> — {conservative_note}:\n"
        f"   <code>/addline {symbol.replace('USDT','')} {format_price(line_1d)} 1d</code>\n"
        f"   Only triggers after strong daily momentum confirms"
    )

    # SL + TP
    lines.append(
        f"\n📐 <b>Trade Plan (2-10 day hold):</b>\n"
        f"   🔴 SL: {format_price(sl)} (-{risk:.1f}%)\n"
        f"   🟢 TP1: {format_price(tp1)} (+{(tp1-current_price)/current_price*100:.1f}%)\n"
        f"   🟢 TP2: {format_price(tp2)} (+{(tp2-current_price)/current_price*100:.1f}%)\n"
        f"   🟢 TP3: {format_price(tp3)} (+{(tp3-current_price)/current_price*100:.1f}%)\n"
        f"   ⚖️ R/R (TP1): {rr1:.1f}x"
    )

    # Quick decision summary
    if not is_high_score:
        decision = "⚠️ Score is moderate — wait for retest confirmation before entry"
    elif is_bearish_daily:
        decision = "⚠️ Daily trend bearish — use 1D option, smaller position size"
    elif has_retest:
        decision = "✅ Retest in progress — 1H entry is valid if next candle closes green"
    else:
        decision = "✅ Setup looks clean — pick your timeframe based on risk tolerance"

    lines.append(f"\n{decision}")

    return "\n".join(lines)


def analyze_key_levels(symbol, current_price):
    """
    Automatically finds key support and resistance levels from 4H klines,
    then for each resistance level above current price: counts how many
    times it's been tested and compares the volume of each test to see
    if buying pressure is increasing (sellers getting absorbed = breakout
    more likely) or decreasing (buyers not committed).

    Returns a formatted string ready to embed in any confirmation message.
    """
    klines = get_klines(symbol, interval="4h", limit=100)
    if not klines or len(klines) < 20:
        return None

    closed = klines[:-1]
    highs  = [float(k[2]) for k in closed]
    lows   = [float(k[3]) for k in closed]
    closes = [float(k[4]) for k in closed]
    vols   = [float(k[5]) for k in closed]

    # ── Find swing highs (resistance) and swing lows (support) ──
    swing_highs = []
    swing_lows  = []
    for i in range(3, len(closed) - 3):
        h = highs[i]
        if (h > highs[i-1] and h > highs[i-2] and h > highs[i-3] and
                h > highs[i+1] and h > highs[i+2] and h > highs[i+3]):
            swing_highs.append((i, h, vols[i]))
        l = lows[i]
        if (l < lows[i-1] and l < lows[i-2] and l < lows[i-3] and
                l < lows[i+1] and l < lows[i+2] and l < lows[i+3]):
            swing_lows.append((i, l, vols[i]))

    # ── Cluster nearby levels (within 1.5%) ──
    def cluster_levels(raw_levels):
        if not raw_levels:
            return []
        clustered = []
        used = set()
        for i, (idx_i, price_i, vol_i) in enumerate(raw_levels):
            if i in used:
                continue
            group = [(idx_i, price_i, vol_i)]
            for j, (idx_j, price_j, vol_j) in enumerate(raw_levels):
                if j != i and j not in used:
                    if abs(price_i - price_j) / price_i <= 0.015:
                        group.append((idx_j, price_j, vol_j))
                        used.add(j)
            used.add(i)
            avg_price = sum(p for _, p, _ in group) / len(group)
            touch_vols = [v for _, _, v in group]
            clustered.append({
                "price": avg_price,
                "touches": len(group),
                "touch_vols": sorted(touch_vols),  # chronological order
                "indices": [idx for idx, _, _ in group],
            })
        return sorted(clustered, key=lambda x: x["price"])

    res_levels = cluster_levels([(i, h, v) for i, h, v in swing_highs if h > current_price * 1.005])
    sup_levels = cluster_levels([(i, l, v) for i, l, v in swing_lows  if l < current_price * 0.995])

    avg_vol_overall = sum(vols[-20:]) / 20

    def vol_trend_label(touch_vols):
        if len(touch_vols) < 2:
            return None
        last = touch_vols[-1]
        prev_avg = sum(touch_vols[:-1]) / len(touch_vols[:-1])
        ratio = last / prev_avg if prev_avg > 0 else 1
        last_x = last / avg_vol_overall if avg_vol_overall > 0 else 1
        if ratio >= 1.3:
            return (f"⚡ This touch volume: {last_x:.1f}x avg — "
                    f"🔥 INCREASING vs prior tests ({ratio:.1f}x more) — "
                    f"sellers getting absorbed, breakout pressure building")
        elif ratio <= 0.7:
            return (f"⚡ This touch volume: {last_x:.1f}x avg — "
                    f"⬇️ DECREASING vs prior tests — "
                    f"buyers not committed yet, may need more tests")
        else:
            return (f"⚡ This touch volume: {last_x:.1f}x avg — "
                    f"↔️ Similar to prior tests — no clear absorption signal yet")

    lines = ["📍 <b>Key Levels (4H):</b>"]

    # Nearest support (just below current price)
    near_supports = [s for s in sup_levels if s["price"] < current_price]
    if near_supports:
        nearest_sup = near_supports[-1]  # closest below
        lines.append(
            f"🟢 Support: {format_price(nearest_sup['price'])} "
            f"({nearest_sup['touches']} touch{'es' if nearest_sup['touches'] > 1 else ''}) "
            f"— {'holds as floor' if current_price > nearest_sup['price'] * 1.01 else 'being tested now'}"
        )

    # Resistance levels above (nearest + major)
    near_resistances = [r for r in res_levels if r["price"] > current_price][:3]
    for i, res in enumerate(near_resistances):
        label = "Next resistance" if i == 0 else "Major resistance" if i == 1 else "Extended target"
        vt = vol_trend_label(res["touch_vols"])
        touches_str = f"{res['touches']} touch{'es' if res['touches'] > 1 else ''}"
        lines.append(f"🔴 {label}: {format_price(res['price'])} ({touches_str})")
        if vt and res["touches"] >= 2:
            lines.append(f"   {vt}")

    if len(lines) == 1:
        return None  # no useful levels found

    # Clear sky detection — if nearest resistance is far away, highlight it
    if near_resistances:
        nearest_res_price = near_resistances[0]["price"]
        gap_pct = (nearest_res_price - current_price) / current_price * 100
        if gap_pct >= 15:
            lines.insert(1,
                f"🚀 <b>CLEAR SKY</b> — no significant resistance until "
                f"{format_price(nearest_res_price)} (+{gap_pct:.1f}% away). "
                f"Price has cleared all nearby levels — momentum moves tend "
                f"to be faster and larger here with less friction."
            )
    elif not near_resistances:
        lines.insert(1,
            f"🚀 <b>CLEAR SKY</b> — no resistance detected above current price "
            f"in recent 4H history. Price is in uncharted territory — "
            f"momentum moves can be very fast here."
        )

    return "\n".join(lines)


def track_building_signal(symbol, signal_type, current_price):
    """
    Tracks signals on the same coin across a 12h window. When 2+ different
    signal types fire on the same coin within that window, sends a combined
    'Building Signal' alert to Top Picks — so the progressive confirmation
    pattern (Phase 2 → Volume Spike 1H → Volume Spike 4H → Explosive) is
    visible as a single coherent alert rather than scattered messages across
    different topics that get lost in noise.
    """
    now = time.time()
    window = 12 * 3600  # 12h window

    if symbol not in building_signal_tracker:
        building_signal_tracker[symbol] = {
            "signals": [],
            "last_combined_alert": 0,
            "window_start": now,
        }

    tracker = building_signal_tracker[symbol]

    # Reset window if it's been more than 12h since first signal
    if now - tracker["window_start"] > window:
        tracker["signals"] = []
        tracker["window_start"] = now
        tracker["last_combined_alert"] = 0

    # Add this signal if it's a new type
    existing_types = {s["type"] for s in tracker["signals"]}
    if signal_type not in existing_types:
        tracker["signals"].append({
            "type": signal_type,
            "price": current_price,
            "time": now,
        })

    # Fire combined alert if 2+ signals and not alerted in last 6h
    signals = tracker["signals"]
    if (len(signals) >= 2 and
            now - tracker["last_combined_alert"] > 6 * 3600):
        tracker["last_combined_alert"] = now

        # Format progression
        signal_lines = []
        for s in sorted(signals, key=lambda x: x["time"]):
            from datetime import datetime as _dt
            t = _dt.fromtimestamp(s["time"]).strftime("%H:%M")
            signal_lines.append(f"  {t} — {s['type']}")

        ticker = get_ticker(symbol)
        change_24h = float(ticker["priceChangePercent"]) if ticker else 0

        urgency = "🚨" if len(signals) >= 3 else "🔥"
        send_to_topic(TOPIC_BUILDUPS,
            f"{urgency} <b>BUILDING SIGNAL — {symbol}</b>\n\n"
            f"💰 Price: {format_price(current_price)}\n"
            f"📊 24h: {change_24h:+.2f}%\n\n"
            f"⏱ <b>Signal progression (last 12h):</b>\n"
            + "\n".join(signal_lines) +
            f"\n\n💡 <i>{len(signals)} timeframes/patterns confirming — "
            f"{'high conviction setup, watch closely for entry' if len(signals) >= 3 else 'building confirmation, watch for next signal'}.</i>\n\n"
            f"⚠️ <i>Check the chart before entry.</i>"
        )
        print(f"🔥 Building signal alert: {symbol} ({len(signals)} signals)")


def build_full_confluence_block(symbol, current_price, tf="4h", swing_high=None, swing_low=None):
    """
    Builds the complete confluence analysis block used in all confirmation
    messages. Combines:
      - Trendline sweep confluence
      - Fibonacci retracement level (if swing_high/low provided or detectable)
      - Volume + HL structure (from analyze_move_strength)
      - Key levels with clear-sky breakout detection
    Returns a formatted string or empty string if nothing notable found.
    """
    parts = []

    # Trendline sweep confluence
    tl_sweep = check_trendline_sweep_confluence(symbol, current_price, tf=tf if tf in ("1h","4h") else "4h")
    if tl_sweep:
        parts.append(tl_sweep)

    # Fibonacci retracement (auto-detect swing high/low from 4H if not provided)
    if not swing_high or not swing_low:
        klines_fib = get_klines(symbol, interval="4h", limit=50)
        if klines_fib and len(klines_fib) >= 10:
            closed_fib = klines_fib[:-1]
            swing_high = max(float(k[2]) for k in closed_fib[-30:])
            swing_low  = min(float(k[3]) for k in closed_fib[-30:])
    if swing_high and swing_low and swing_high > swing_low:
        fib_range = swing_high - swing_low
        fib_levels = {
            "0.236": swing_high - 0.236 * fib_range,
            "0.382": swing_high - 0.382 * fib_range,
            "0.500": swing_high - 0.500 * fib_range,
            "0.618": swing_high - 0.618 * fib_range,
            "0.786": swing_high - 0.786 * fib_range,
        }
        closest_fib = None
        closest_dist = float("inf")
        for fname, fprice in fib_levels.items():
            dist = abs(current_price - fprice) / fprice
            if dist < closest_dist and dist <= 0.03:
                closest_dist = dist
                closest_fib = (fname, fprice)
        if closest_fib:
            fname, fprice = closest_fib
            if fname == "0.618":
                parts.append(f"📐 <b>Fibonacci: 0.618 🎯 GOLDEN POCKET</b> (~{format_price(fprice)}) — strongest institutional confluence level")
            elif fname == "0.500":
                parts.append(f"📐 Fibonacci: 0.500 ({format_price(fprice)}) — mid-range, institutional level")
            elif fname == "0.382":
                parts.append(f"📐 Fibonacci: 0.382 ({format_price(fprice)}) — shallow pullback, trend still strong")
            elif fname == "0.786":
                parts.append(f"📐 Fibonacci: 0.786 ({format_price(fprice)}) — deep pullback, last support before trend break")
            else:
                parts.append(f"📐 Fibonacci: {fname} (~{format_price(fprice)})")

    # Volume + HL structure
    suggestion, strength_details = analyze_move_strength(symbol, current_price)
    if strength_details:
        struct_lines = [d for d in strength_details if "Distribution" not in d]
        dist_lines   = [d for d in strength_details if "Distribution" in d]
        if struct_lines:
            parts.append("📊 <b>Move Strength:</b>\n" + "\n".join(struct_lines))
        if dist_lines:
            parts.append("\n".join(dist_lines))

    # Key levels (support/resistance + clear sky)
    key_levels = analyze_key_levels(symbol, current_price)
    if key_levels:
        parts.append(key_levels)

    return "\n\n".join(parts) if parts else ""


def check_trendline_sweep_confluence(symbol, confirm_price, tf="1h"):
    """
    Checks whether the current retest/bounce happened at or after a trendline
    liquidity sweep — price dipped below a descending trendline (drawn from
    recent swing highs on the given TF), triggering sell stops, then reclaimed
    back above it with volume. When this is present alongside a zone/line/watch
    retest, it's a significantly stronger setup (institutional absorption of
    sell-side liquidity before the move up). Returns a string to embed in the
    retest message, or None if no sweep detected.
    """
    klines = get_klines(symbol, interval=tf, limit=30)
    if not klines or len(klines) < 15:
        return None
    closed = klines[:-1]

    # Find the two most recent swing highs (local maxima) to define the trendline
    swing_highs = []
    for i in range(2, len(closed) - 2):
        h = float(closed[i][2])
        if (h > float(closed[i-1][2]) and h > float(closed[i-2][2]) and
                h > float(closed[i+1][2]) and h > float(closed[i+2][2])):
            swing_highs.append((i, h))
    if len(swing_highs) < 2:
        return None

    # Use the two most recent swing highs to build the descending trendline
    sh1_idx, sh1_h = swing_highs[-2]
    sh2_idx, sh2_h = swing_highs[-1]
    if sh2_h >= sh1_h:
        return None  # not descending

    # Project trendline value at the last closed candle
    slope = (sh2_h - sh1_h) / (sh2_idx - sh1_idx) if sh2_idx != sh1_idx else 0
    last_idx = len(closed) - 1
    trendline_at_last = sh2_h + slope * (last_idx - sh2_idx)

    # Check the last 3 candles for a sweep below and reclaim
    recent = closed[-3:]
    sweep_candle = None
    for k in recent:
        k_low = float(k[3])
        k_close = float(k[4])
        if k_low < trendline_at_last * 0.999 and k_close > trendline_at_last:
            sweep_candle = k
            break

    if not sweep_candle:
        return None

    # Volume on the sweep/reclaim candle vs average
    sw_vol = float(sweep_candle[5])
    avg_vol = sum(float(k[5]) for k in closed[-10:-3]) / 7 if len(closed) >= 10 else sw_vol
    vol_ratio = sw_vol / avg_vol if avg_vol > 0 else 0
    if vol_ratio < 1.5:
        return None

    # Current price must still be above the trendline (reclaim held)
    if confirm_price < trendline_at_last * 0.99:
        return None

    return (
        f"📉➡️📈 <b>Trendline Liquidity Sweep</b> — price swept below the descending "
        f"trendline ({format_price(trendline_at_last)}) on {vol_ratio:.1f}x volume then "
        f"reclaimed it. Sell-side stops likely absorbed before this move — "
        f"adds meaningful confluence to the retest."
    )


def analyze_move_strength(symbol, confirm_price):
    """
    After a /watch or /addline retest confirms on a smaller timeframe (15m/30m/1H),
    this looks at how strong the underlying move actually is — volume intensity
    plus 5m/15m higher-low structure — to suggest whether the move looks
    strong enough to consider an earlier entry, or whether it's safer to wait
    for 4H to confirm too. This is explicitly a suggestion based on observable
    technical conditions, not a probability or guarantee — consistent with
    how /entry's score is framed.

    It also runs the same distribution-risk check used in calc_entry_score and
    calc_followthrough_score: a "retest" can be a fake breakout if large holders
    are using the bounce to exit (price spikes, gets bought into briefly on the
    retest, then dumps again on real volume) — the retest candle alone looks
    bullish but the broader picture is distribution, not accumulation. When this
    fires, it overrides the strength suggestion entirely with an explicit warning,
    since a confirmed retest under a distribution pattern is the most dangerous
    combination — it looks like the "safe" signal but isn't.

    Separately, it checks (same logic as the manual-zone liquidity sweep
    detector) whether the retest's low actually swept below a previously
    established swing low (tested 2+ times — real sell-side liquidity) and
    reclaimed it. A retest backed by a genuine liquidity grab is a stronger
    setup than a plain bounce, since it suggests stop-losses/limit orders below
    the level were absorbed rather than the move just running out of sellers.
    """
    details = []
    strength_score = 0

    # ── Distribution-risk check ──
    # Unlike calc_entry_score (which checks this live, right at the spike candle),
    # this runs AFTER a retest has already confirmed — meaning several candles have
    # passed since the actual spike. Looking only at the most recent candle's volume
    # would miss the pattern almost every time, since volume normalizes quickly after
    # a spike. Instead, find the highest-volume candle in the recent window (the real
    # spike, wherever it sits) and measure retracement from ITS high.
    klines_1h_dist = get_klines(symbol, interval="1h", limit=15)
    ticker_dist = get_ticker(symbol)
    if klines_1h_dist and len(klines_1h_dist) >= 10 and ticker_dist:
        closed_1h_dist = klines_1h_dist[:-1]
        change_24h_dist = float(ticker_dist["priceChangePercent"])

        window = closed_1h_dist[-10:]
        baseline_vols = [float(k[5]) for k in closed_1h_dist[-15:-10]] or [float(k[5]) for k in window[:3]]
        avg_baseline = sum(baseline_vols) / len(baseline_vols) if baseline_vols else 1

        spike_idx = max(range(len(window)), key=lambda i: float(window[i][5]))
        spike_candle_dist = window[spike_idx]
        spike_vol_dist = float(spike_candle_dist[5])
        vol_ratio_dist = spike_vol_dist / avg_baseline if avg_baseline else 1
        spike_high_dist = float(spike_candle_dist[2])

        heavy_24h_down = change_24h_dist <= -7.0 and vol_ratio_dist >= 10

        low_since_spike = min(float(k[3]) for k in window[spike_idx:]) if spike_idx < len(window) else spike_high_dist
        spike_retraced = False
        if spike_high_dist > low_since_spike:
            retrace_pct_dist = (spike_high_dist - confirm_price) / (spike_high_dist - low_since_spike)
            spike_retraced = retrace_pct_dist >= 0.70 and vol_ratio_dist >= 5

        if heavy_24h_down or spike_retraced:
            reason = (f"{change_24h_dist:+.1f}% on 24h with {vol_ratio_dist:.1f}x recent volume" if heavy_24h_down
                       else f"price has given back most of a recent spike on real volume ({vol_ratio_dist:.1f}x)")
            return (
                f"🚨 <b>DISTRIBUTION WARNING — possible fake breakout</b>\n"
                f"⚠️ {reason}. A \"retest confirm\" candle can still appear here even "
                f"while large holders are using the bounce to exit — treat this as a "
                f"high-risk setup, not a green light. 4H confirmation is strongly "
                f"recommended before any entry, and a tight stop-loss is essential if "
                f"you do enter.",
                [f"🚨 Distribution risk — {reason}"],
            )

    # Volume intensity check (5m candles around the confirm moment)
    klines_5m = get_klines(symbol, interval="5m", limit=20)
    vol_ratio = 0
    if klines_5m and len(klines_5m) >= 10:
        closed_5m = klines_5m[:-1]
        recent_vols = [float(k[5]) for k in closed_5m[-3:]]
        prior_vols = [float(k[5]) for k in closed_5m[-10:-3]]
        avg_prior = sum(prior_vols) / len(prior_vols) if prior_vols else 0
        avg_recent = sum(recent_vols) / len(recent_vols) if recent_vols else 0
        vol_ratio = avg_recent / avg_prior if avg_prior else 0
        if vol_ratio >= 5:
            strength_score += 2
            details.append(f"✅ Strong volume ({vol_ratio:.1f}x recent vs prior)")
        elif vol_ratio >= 2:
            strength_score += 1
            details.append(f"⚠️ Moderate volume ({vol_ratio:.1f}x recent vs prior)")
        else:
            details.append(f"⚠️ Weak volume ({vol_ratio:.1f}x recent vs prior)")

    # 5m and 15m higher-low structure
    hl_5m = check_hl_only(klines_5m, lookback=6) if klines_5m else False
    klines_15m_hl = get_klines(symbol, interval="15m", limit=20)
    hl_15m = check_hl_only(klines_15m_hl, lookback=6) if klines_15m_hl else False
    if hl_5m:
        strength_score += 1
        details.append("✅ Higher lows forming (5M)")
    if hl_15m:
        strength_score += 1
        details.append("✅ Higher lows forming (15M)")
    if not hl_5m and not hl_15m:
        details.append("⚠️ No clear higher-low structure yet (5M/15M)")

    # ── Liquidity sweep check ──
    # Uses the same logic as the manual-zone liquidity sweep detector: did the
    # retest's low actually sweep below a previously-established swing low (one
    # that's been tested 2+ times — real sell-side liquidity, not a random dip)
    # and reclaim it? If so, this retest is backed by a genuine liquidity grab,
    # not just a bounce — a meaningfully stronger signal than volume/HL alone.
    liquidity_swept = False
    klines_1h_sweep = get_klines(symbol, interval="1h", limit=15)
    if klines_1h_sweep and len(klines_1h_sweep) >= 10:
        last_sweep = klines_1h_sweep[-2]
        ls_open, ls_high, ls_low, ls_close = (float(last_sweep[1]), float(last_sweep[2]),
                                               float(last_sweep[3]), float(last_sweep[4]))
        candle_range_sweep = ls_high - ls_low
        if candle_range_sweep > 0:
            lower_wick_sweep = min(ls_open, ls_close) - ls_low
            wick_dominant_sweep = lower_wick_sweep / candle_range_sweep >= 0.55

            lookback_sweep = klines_1h_sweep[-10:-2]
            swing_low_sweep = min(float(k[3]) for k in lookback_sweep)
            touches_sweep = sum(
                1 for k in lookback_sweep
                if abs(float(k[3]) - swing_low_sweep) / swing_low_sweep <= 0.015
            )
            established_sweep = touches_sweep >= 2
            swept_below_sweep = ls_low < swing_low_sweep * 0.998
            reclaimed_sweep = ls_close > swing_low_sweep

            m_vol_sweep = float(last_sweep[5])
            prev_vols_sweep = [float(k[5]) for k in klines_1h_sweep[-8:-2]]
            avg_vol_sweep = sum(prev_vols_sweep) / len(prev_vols_sweep) if prev_vols_sweep else 1
            vol_ratio_sweep = m_vol_sweep / avg_vol_sweep if avg_vol_sweep > 0 else 0

            liquidity_swept = (
                established_sweep and swept_below_sweep and reclaimed_sweep and
                wick_dominant_sweep and vol_ratio_sweep >= 1.3
            )
            if liquidity_swept:
                strength_score += 2
                details.append(
                    f"🩸 Liquidity sweep — swept below {format_price(swing_low_sweep)} "
                    f"(tested {touches_sweep}x prior) and reclaimed on {vol_ratio_sweep:.1f}x volume"
                )

    if strength_score >= 3:
        if liquidity_swept:
            suggestion = "🔥 <b>Strong move</b> — backed by a genuine liquidity sweep plus healthy volume/structure. An earlier entry can be considered, still with a stop-loss below the sweep low."
        else:
            suggestion = "🔥 <b>Strong move</b> — volume and structure both look healthy. An earlier entry can be considered, still with a stop-loss."
    elif strength_score >= 1:
        suggestion = "⏳ <b>Moderate signs</b> — some support, but consider waiting for 4H to confirm before a full-size entry."
    else:
        suggestion = "⚠️ <b>Weak follow-through signs so far</b> — waiting for a 4H retest confirmation is the more cautious path here."

    return suggestion, details

def check_retest_watches():
    """
    Checks every active /watch entry: has the retest completed (strong green
    candle closed back above the broken level)? If so, notify the requesting
    subscriber personally AND post to Top Picks (public visibility).

    FIX (after the JST case): "Retest confirmed" was a one-candle snapshot —
    it fired the moment ONE strong green candle closed above the level, then
    immediately removed the watch. But a single confirming candle doesn't
    guarantee the breakout holds; the very next candle can roll right back
    into the zone (exactly what happened with JST — confirmed, then the next
    15M candle was red and price dropped straight back into the broken range).
    Saying "Continuation looks favorable" and then dropping tracking made the
    bot look wrong when it wasn't really tracking anything after that point.

    Now, on confirm, the watch moves into a "followup" state instead of being
    removed: the bot checks the next up-to-3 candle closes (on the timeframe
    that confirmed) to see whether the level actually holds as support. Only
    after that does it send a final outcome message and remove the watch —
    or it sends an early failure alert immediately if a candle closes back
    below the level before the 3 candles are up.

    Checks whichever timeframe(s) were in progress when /watch was created
    (4H, 1H, or both) — older watches without a stored "timeframes" key
    default to 4H only for backward compatibility.
    """
    if not retest_watch_list:
        return
    to_remove = []
    now = time.time()
    for watch_key, watch in list(retest_watch_list.items()):
        symbol = watch["symbol"]
        stage = watch.get("stage", "watching")  # "watching" -> "followup" -> done

        # Expire stale watches after 5 days — if the retest hasn't resolved by
        # then, the setup has likely changed enough that the original /entry
        # read is stale anyway.
        if now - watch.get("requested_time", now) > 5 * 86400:
            to_remove.append(watch_key)
            continue

        ticker = get_ticker(symbol)
        if not ticker:
            continue
        current_price = float(ticker["lastPrice"])

        # ── Follow-up stage: confirm already fired, now checking if it holds ──
        if stage == "followup":
            tf = watch["followup_tf"]
            level_price = watch["followup_level"]
            klines_tf = get_klines(symbol, interval=tf, limit=10)
            if not klines_tf or len(klines_tf) < 3:
                continue
            last_closed = klines_tf[-2]
            last_candle_key = int(last_closed[0])
            if watch.get("last_checked_candle") == last_candle_key:
                continue  # already evaluated this candle close
            watch["last_checked_candle"] = last_candle_key

            l_close = float(last_closed[4])
            candles_checked = watch.get("followup_candles_checked", 0) + 1
            watch["followup_candles_checked"] = candles_checked

            if l_close < level_price * 0.98:
                send_to(watch["chat_id"],
                    f"⚠️ <b>{symbol} retest gave back the breakout [{tf.upper()}]</b>\n\n"
                    f"The level held for the confirming candle, but price has now closed "
                    f"back below {format_price(level_price)} — the continuation didn't hold. "
                    f"Treat the earlier confirmation as invalidated."
                )
                print(f"⚠️ Retest follow-up FAILED: {symbol} -> {watch['chat_id']}")
                to_remove.append(watch_key)
            elif candles_checked >= 3:
                send_to(watch["chat_id"],
                    f"✅ <b>{symbol} retest held [{tf.upper()}]</b>\n\n"
                    f"3 candles since the confirmation and price is still holding above "
                    f"{format_price(level_price)} (currently {format_price(current_price)}). "
                    f"The breakout looks genuine so far — still confirm on the chart and "
                    f"manage your own risk."
                )
                print(f"✅ Retest follow-up HELD: {symbol} -> {watch['chat_id']}")
                to_remove.append(watch_key)
            save_retest_watch()
            continue

        # ── Watching stage: waiting for the initial confirm/fail ──
        timeframes = watch.get("timeframes", ["4h"])
        confirmed_note = None
        failed_note = None
        for tf in timeframes:
            klines_tf = get_klines(symbol, interval=tf, limit=30)
            if not klines_tf:
                continue
            pattern_note = detect_break_retest_pattern(klines_tf, current_price)
            if pattern_note and "Retest confirmed" in pattern_note:
                confirmed_note = (tf, pattern_note, klines_tf)
                break  # one confirmed timeframe is enough to notify
            elif pattern_note and "Retest failed" in pattern_note:
                failed_note = (tf, pattern_note)

        if confirmed_note:
            tf, pattern_note, klines_tf = confirmed_note
            suggestion, strength_details = analyze_move_strength(symbol, current_price)
            is_distribution_flagged = any("Distribution risk" in d for d in strength_details)

            if is_distribution_flagged:
                # Admin DM only — distribution risk
                send_to(watch["chat_id"],
                    f"⚠️ <b>Distribution Risk — {symbol} [{tf.upper()}]</b>\n\n"
                    f"💰 Price: {format_price(current_price)}\n\n"
                    f"🚨 Retest pattern formed but volume suggests distribution.\n"
                    f"Monitoring for volume confirmation. Wait for 3x+ volume green candle."
                )
                print(f"🔥 Watch retest DISTRIBUTION (admin only): {symbol}")
            else:
                full_confluence_watch = build_entry_decision_block(symbol, current_price, tf=tf if tf in ("1h","4h") else "1h")
                ob_watch = get_order_book_clusters(symbol)
                of_watch = format_order_flow_block(ob_watch, current_price) if ob_watch else ""
                ifvg_watch = analyze_ifvg_framework(symbol, current_price, tf=tf if tf in ("4h","1d") else "4h")
                msg = (
                    f"🔥 <b>Retest Complete — {symbol} [{tf.upper()}]</b>\n\n"
                    f"💰 Price: {format_price(current_price)}\n\n"
                    f"{pattern_note}\n\n"
                    + (f"{full_confluence_watch}\n\n" if full_confluence_watch else "") +
                    (f"{ifvg_watch}\n\n" if ifvg_watch else "") +
                    (f"{of_watch}\n\n" if of_watch else "") +
                    f"{suggestion}\n\n"
                    f"⏳ <i>Tracking the next 3 candles to confirm this holds — "
                    f"you'll get a follow-up.</i>"
                )
                send_to(watch["chat_id"], msg)
                send_to_topic(TOPIC_MY_SETUPS, msg)
                print(f"🔥 Retest complete: {symbol}")
                start_liq_watch(symbol, ob_watch, current_price, watch["chat_id"])

            # Extract the level that was confirmed, from the pattern note text,
            # so follow-up can check against it without re-running detection.
            import re as _re
            level_match = _re.search(r"broke ([\d.]+)", pattern_note)
            followup_level = float(level_match.group(1)) if level_match else current_price
            watch["stage"] = "followup"
            watch["followup_tf"] = tf
            watch["followup_level"] = followup_level
            watch["followup_candles_checked"] = 0
            watch["last_checked_candle"] = int(klines_tf[-2][0])
            save_retest_watch()
        elif failed_note and not confirmed_note:
            tf, _ = failed_note
            send_to(watch["chat_id"],
                f"⚠️ <b>{symbol} retest failed [{tf.upper()}]</b> — price closed back below the broken level. "
                f"Removing this from your watch list."
            )
            to_remove.append(watch_key)

    for key in to_remove:
        retest_watch_list.pop(key, None)
    if to_remove:
        save_retest_watch()

def auto_cleanup_poor_performers():
    """
    Bi-weekly cleanup: removes extra watchlist coins that haven't performed.
    - Window: 15 days (was 7)
    - Threshold: 10% gain (was 20%)
    - Fundamental filter: if coin has good market cap (high 24h volume proxy),
      extends window to 30 days before removing
    - DEFAULT_WATCHLIST coins never touched
    - Coins with zero signals left alone
    """
    now = time.time()
    cutoff_15d = now - 15 * 86400
    cutoff_30d = now - 30 * 86400
    extra_coins = [c for c in watchlist if c not in DEFAULT_WATCHLIST]
    if not extra_coins:
        return

    # Build per-symbol best-gain map
    best_gain = {}
    has_recent_signal = set()
    for data in signal_performance.values():
        if data.get("signal_time", 0) < cutoff_30d:
            continue
        symbol = data.get("symbol")
        if not symbol:
            continue
        has_recent_signal.add(symbol)
        signal_price = data.get("signal_price", 0)
        highest = data.get("highest_after", signal_price)
        if signal_price > 0:
            gain_pct = (highest - signal_price) / signal_price * 100
            best_gain[symbol] = max(best_gain.get(symbol, 0), gain_pct)

    # Get volume data for fundamental filter
    high_vol_coins = set()
    try:
        ticker = get_ticker("BTCUSDT")  # just to warm up cache
        for sym in extra_coins:
            t = get_ticker(sym)
            if t and float(t.get("quoteVolume", 0)) >= 5_000_000:
                high_vol_coins.add(sym)
    except:
        pass

    to_remove = []
    for symbol in extra_coins:
        if symbol not in has_recent_signal:
            continue  # no signal = leave alone

        gain = best_gain.get(symbol, 0)
        if gain >= 10.0:
            continue  # performed well, keep

        # Fundamental filter: high volume coins get 30 days
        has_recent_15d = any(
            d.get("symbol") == symbol and d.get("signal_time", 0) >= cutoff_15d
            for d in signal_performance.values()
        )
        if symbol in high_vol_coins and not has_recent_15d:
            continue  # high volume coin, give it 30 days

        # Remove if: had signal in 15d window + under 10% gain
        if has_recent_15d and gain < 10.0:
            to_remove.append((symbol, gain))

    if not to_remove:
        return

    for symbol, gain in to_remove:
        if symbol in watchlist:
            watchlist.remove(symbol)
        removed_coins.add(symbol)
    save_watchlist_file()
    save_removed_coins()

    lines = [f"• {sym} (best: {gain:+.1f}%)" for sym, gain in to_remove]
    send_to(ADMIN_CHAT_ID,
        f"🧹 <b>15-Day Cleanup</b>\n\n"
        f"Removed {len(to_remove)} coin(s) — had signals but didn't reach 10% in 15 days:\n\n"
        + "\n".join(lines) +
        f"\n\nTotal watchlist: {len(watchlist)} coins"
    )
    print(f"🧹 Auto-cleanup removed {len(to_remove)} coins")

_btc_condition = {
    "state": "neutral",        # "neutral" | "warning" | "bearish"
    "alert_sent": False,
    "recovery_sent": False,
    "last_check": 0,
    "consecutive_red_4h": 0,
}

def check_btc_market_condition():
    """
    Monitors BTC's 4H and 1D structure for signs of a trend shift that would
    make altcoin entries significantly riskier. Fires a personal admin DM
    (not a subscriber broadcast) when BTC looks like it's turning bearish,
    and a recovery DM when it stabilises. Checks every 4H candle.
    """
    now = time.time()
    if now - _btc_condition["last_check"] < 3600:  # max once per hour
        return
    _btc_condition["last_check"] = now

    klines_4h = get_klines("BTCUSDT", interval="4h", limit=20)
    klines_1d  = get_klines("BTCUSDT", interval="1d", limit=10)
    ticker = get_ticker("BTCUSDT")
    if not klines_4h or len(klines_4h) < 10 or not ticker:
        return

    btc_price   = float(ticker["lastPrice"])
    change_24h  = float(ticker["priceChangePercent"])
    closed_4h   = klines_4h[:-1]
    closes_4h   = [float(k[4]) for k in closed_4h]
    ema20_4h    = calculate_ema(closes_4h, 20)

    # Consecutive red 4H candles
    consec_red = 0
    for k in reversed(closed_4h[-6:]):
        if float(k[4]) < float(k[1]):
            consec_red += 1
        else:
            break
    _btc_condition["consecutive_red_4h"] = consec_red

    # Single 4H dump ≥ 3%
    last_4h_change = (float(closed_4h[-1][4]) - float(closed_4h[-1][1])) / float(closed_4h[-1][1]) * 100

    # 4H EMA cross (price below EMA)
    below_ema_4h = ema20_4h and btc_price < ema20_4h

    # 1D EMA
    below_ema_1d = False
    if klines_1d and len(klines_1d) >= 5:
        closed_1d  = klines_1d[:-1]
        closes_1d  = [float(k[4]) for k in closed_1d]
        ema20_1d   = calculate_ema(closes_1d, 20)
        below_ema_1d = ema20_1d and btc_price < ema20_1d

    # Trigger conditions
    is_warning = consec_red >= 3 or last_4h_change <= -3.0 or below_ema_4h
    is_bearish = (consec_red >= 4 or below_ema_1d or
                  (below_ema_4h and consec_red >= 2))

    new_state = "bearish" if is_bearish else ("warning" if is_warning else "neutral")
    old_state = _btc_condition["state"]

    if new_state in ("warning", "bearish") and not _btc_condition["alert_sent"]:
        _btc_condition["state"] = new_state
        _btc_condition["alert_sent"] = True
        _btc_condition["recovery_sent"] = False
        emoji = "🔴" if new_state == "bearish" else "🟠"
        reasons = []
        if consec_red >= 3:
            reasons.append(f"{consec_red} consecutive red 4H candles")
        if last_4h_change <= -3.0:
            reasons.append(f"last 4H candle: {last_4h_change:.1f}%")
        if below_ema_4h:
            reasons.append(f"price below 4H 20EMA ({format_price(ema20_4h)})")
        if below_ema_1d:
            reasons.append("price below 1D 20EMA — serious")
        send_to(ADMIN_CHAT_ID,
            f"{emoji} <b>BTC TREND SHIFT — {'BEARISH' if new_state == 'bearish' else 'WARNING'}</b>\n\n"
            f"💰 BTC: {format_price(btc_price)} ({change_24h:+.2f}% 24h)\n"
            f"⚠️ Signals:\n" + "\n".join(f"  • {r}" for r in reasons) + "\n\n"
            f"Altcoin signals are significantly higher risk during BTC weakness.\n"
            f"Consider:\n"
            f"  • Tightening SL on open positions\n"
            f"  • Pausing new entries until BTC stabilises\n"
            f"  • Watching BTC support around {format_price(btc_price * 0.95)}\n\n"
            f"You'll get another message when BTC stabilises. 🟡"
        )
        print(f"🔴 BTC condition alert: {new_state} — {', '.join(reasons)}")

    elif new_state == "neutral" and old_state in ("warning", "bearish") and not _btc_condition["recovery_sent"]:
        _btc_condition["state"] = "neutral"
        _btc_condition["alert_sent"] = False
        _btc_condition["recovery_sent"] = True
        send_to(ADMIN_CHAT_ID,
            f"✅ <b>BTC Stabilising</b>\n\n"
            f"💰 BTC: {format_price(btc_price)} ({change_24h:+.2f}% 24h)\n"
            f"4H structure looks healthier — {consec_red} red candles, "
            f"{'above' if not below_ema_4h else 'near'} 4H EMA.\n\n"
            f"Altcoin conditions improving — normal scan resuming."
        )
        print("✅ BTC condition recovered to neutral")


_last_watchlist_validate = 0

_vol_accum_alerted  = {}  # {symbol: last_alert_time}
_postpump_retest_alerted = {}  # {symbol: last_alert_time}

_global_liq_alerted = {}  # {symbol: last_alert_time}

_high_confidence_alerted = {}  # {symbol: last_alert_time}

# Signal types with >20% win rate from /report performance
HIGH_WINRATE_SIGNALS = {
    "Explosive Pump [5M]": 50,
    "Accumulation [1H]": 33,
    "Volume Build-up [4H]": 25,
    "Volume Spike [4H]": 28,
    "Volume Surge [1H]": 30,
    "OB Bounce [4H OB]": 33,
    "Gradual Buildup [1D]": 20,
}

def find_valid_order_block(symbol, tf="4h"):
    """
    Finds a "valid Order Block" per the 3-part definition (note #9, from a
    shared trading reference): (1) a liquidity sweep occurred right before
    the OB candle, (2) an FVG sits right after/adjacent to it, (3) it's
    unmitigated — no candle since has traded back into its range. Returns
    the OB dict (symbol/tf/low/high/candle_idx) or None.
    """
    klines = get_klines(symbol, interval=tf, limit=60)
    if not klines or len(klines) < 20:
        return None
    closed = klines[:-1]
    fvgs = detect_fvg(klines)

    for i in range(len(closed) - 3, 5, -1):
        c = closed[i]
        ko, kc, kh, kl, kv = float(c[1]), float(c[4]), float(c[2]), float(c[3]), float(c[5])
        if kc <= ko:
            continue  # OB candle must be bullish (green)

        prior_vols = [float(k[5]) for k in closed[max(0, i - 10):i]]
        avg_v = sum(prior_vols) / len(prior_vols) if prior_vols else 1
        if avg_v <= 0 or kv < avg_v * 1.3:
            continue  # needs real volume behind it

        # (1) Liquidity sweep — this candle (or the one right before it)
        # undercut a recent swing low before reversing up
        lookback_lows = [float(k[3]) for k in closed[max(0, i - 10):i]]
        if not lookback_lows:
            continue
        swing_low = min(lookback_lows)
        swept = kl < swing_low
        if not swept and i > 0:
            swept = float(closed[i - 1][3]) < swing_low
        if not swept:
            continue

        # (2) FVG present right after/adjacent to the OB candle
        has_fvg = any(f["type"] == "bullish" and abs(f["candle_idx"] - i) <= 2 for f in fvgs)
        if not has_fvg:
            continue

        # (3) Unmitigated — no candle since has traded back into [kl, kh]
        mitigated = any(float(k[3]) <= kh and float(k[2]) >= kl for k in closed[i + 1:])
        if mitigated:
            continue

        return {"symbol": symbol, "tf": tf, "low": kl, "high": kh, "candle_idx": i}

    return None


_valid_ob_alerted = {}  # {symbol: last_alert_time}

def check_valid_order_block(symbol):
    """
    Note #9: scans a symbol for a valid Order Block (swept + FVG +
    unmitigated), then scores it with a lightweight 4-point confluence check
    (daily trend, BS/buy pressure, volume strength, higher lows). Routes:
    score>=2 → High Priority, score>=4 (all pass) → also Top Picks.
    """
    now = time.time()
    if now - _valid_ob_alerted.get(symbol, 0) < 8 * 3600:
        return

    ob = find_valid_order_block(symbol, tf="4h")
    if not ob:
        return

    ticker = get_ticker(symbol)
    if not ticker:
        return
    current_price = float(ticker["lastPrice"])
    if not (ob["low"] * 0.98 <= current_price <= ob["high"] * 1.05):
        return  # only relevant while price is still near the OB

    klines_1d = get_klines(symbol, interval="1d", limit=30)
    klines_1h = get_klines(symbol, interval="1h", limit=20)

    score = 0
    details = []

    daily_down = is_daily_downtrend(symbol, current_price)
    if not daily_down:
        score += 1
        details.append("✅ Daily trend bullish/neutral")
    else:
        details.append("❌ Daily trend bearish")

    buy_ratio = None
    if klines_1h and len(klines_1h) >= 3:
        last_1h = klines_1h[:-1][-1]
        vol_1h = float(last_1h[5])
        buy_1h = float(last_1h[9]) if len(last_1h) > 9 else vol_1h * 0.5
        buy_ratio = buy_1h / vol_1h if vol_1h > 0 else 0.5
    if buy_ratio is not None and buy_ratio >= 0.55:
        score += 1
        details.append(f"✅ BS Positive ({buy_ratio*100:.0f}% buy)")
    else:
        details.append("⚠️ BS Negative/neutral")

    vol_strong = False
    if klines_1d and len(klines_1d) >= 10:
        closed_1d = klines_1d[:-1]
        vols = [float(k[5]) for k in closed_1d[-10:]]
        baseline = sum(vols[:-1]) / max(1, len(vols) - 1)
        vol_strong = baseline > 0 and vols[-1] >= baseline * 1.3
    if vol_strong:
        score += 1
        details.append("✅ Volume elevated")
    else:
        details.append("⚠️ Volume unremarkable")

    hl_forming = False
    if klines_1h and len(klines_1h) >= 8:
        lows_1h = [float(k[3]) for k in klines_1h[:-1][-8:]]
        hl_forming = sum(1 for j in range(1, len(lows_1h)) if lows_1h[j] > lows_1h[j - 1]) >= 3
    if hl_forming:
        score += 1
        details.append("✅ Higher lows forming (1H)")
    else:
        details.append("⚠️ No clear higher lows")

    if score < 2:
        return

    _valid_ob_alerted[symbol] = now
    details_str = "\n   ".join(details)
    msg = (
        f"🔲 <b>VALID ORDER BLOCK — {symbol}</b>\n\n"
        f"📐 Definition: liquidity swept ✅ | FVG present ✅ | unmitigated ✅\n"
        f"🔲 OB Zone: {format_price(ob['low'])}–{format_price(ob['high'])} [{ob['tf'].upper()}]\n"
        f"💰 Price: {format_price(current_price)}\n"
        f"📊 Confluence Score: {score}/4\n\n"
        f"   {details_str}\n\n"
        f"⚠️ <i>Confirm on chart before entry.</i>"
    )
    send_to_topic(TOPIC_BUILDUPS, msg)
    if score >= 4:
        send_to_topic(TOPIC_TOP_PICKS, "🏆 <b>VALID OB — STRONG</b>\n\n" + msg)
    print(f"🔲 Valid OB: {symbol} score={score}/4 zone={format_price(ob['low'])}-{format_price(ob['high'])}")


def is_fast_pumper(symbol, min_samples=3, max_hours=48, min_gain=30.0):
    """
    Note #3: classifies a symbol as a "fast pumper" — one whose historical
    signals consistently reach big gains (>=30%) within a short window
    (<48h) rather than slow multi-day/week grinds. Uses the existing
    peak_time/peak_hrs tracking in signal_performance (same data the SIGNAL
    RESULT messages use). Needs at least min_samples historical signals to
    make a call; returns False (not fast-pumper) if there isn't enough data.
    """
    samples = [
        d for d in signal_performance.values()
        if d.get("symbol") == symbol and d.get("signal_price", 0) > 0 and "peak_time" in d
    ]
    if len(samples) < min_samples:
        return False
    fast_hits = 0
    for d in samples:
        gain = (d["highest_after"] - d["signal_price"]) / d["signal_price"] * 100
        hrs = (d["peak_time"] - d["signal_time"]) / 3600
        if gain >= min_gain and hrs <= max_hours:
            fast_hits += 1
    return fast_hits / len(samples) >= 0.5  # majority of historical signals were fast+big


def check_high_confidence_signal(symbol, signal_type, current_price):
    """
    Runs an instant confluence check after ANY signal fires (Top Picks,
    Building Momentum, Quick Spikes, Signal Results types, etc).
    Conditions checked: Volume 3x+, BS positive, Daily bullish,
                Higher lows, OB zone, Liq sweep bonus (score out of 6).
    Threshold: high win-rate signal types need score>=3 (original behavior,
    unchanged). All other signal types are now also evaluated (previously
    skipped entirely) using a broader score>=2 bar — this is the "catch every
    big pump" promotion pipeline: Top Picks / Building Momentum / Quick Spikes
    / Signal Results signals get promoted up to High Priority as
    ⭐ HIGH CONFIDENCE when they clear this bar.
    """
    win_rate = None
    for sig, wr in HIGH_WINRATE_SIGNALS.items():
        if sig in signal_type:
            win_rate = wr
            break
    required_score = 3 if win_rate else 2

    # Note #3: symbols classified as historical "fast pumpers" get a much
    # lower confluence bar — score>=1 is enough to reach High Priority (vs
    # the normal 2/3), since their track record already shows fast, sizable
    # moves. This is a per-symbol override, not a global change (would be
    # too noisy applied to every coin).
    fast_pumper = is_fast_pumper(symbol)
    if fast_pumper:
        required_score = 1

    now = time.time()
    key = f"{symbol}_hc"
    if now - _high_confidence_alerted.get(key, 0) < 8 * 3600:
        return

    try:
        ticker = get_ticker(symbol)
        if not ticker:
            return
        change_24h = float(ticker["priceChangePercent"])

        klines_1h = get_klines(symbol, interval="1h", limit=20)
        klines_4h = get_klines(symbol, interval="4h", limit=20)
        if not klines_1h or not klines_4h:
            return

        score = 0
        details = []

        # ── Volume 3x+ ──
        closed_1h = klines_1h[:-1]
        vols = [float(k[5]) for k in closed_1h[-10:]]
        baseline = sum(vols[:6]) / 6 if len(vols) >= 6 else 1
        vol_ratio = vols[-1] / baseline if baseline > 0 else 0
        if vol_ratio >= 3.0:
            score += 1
            details.append(f"✅ Volume: {vol_ratio:.1f}x")
        else:
            details.append(f"⚠️ Volume: {vol_ratio:.1f}x")

        # ── BS Positive ──
        bs_sum = 0
        for k in closed_1h[-3:]:
            total_v = float(k[5])
            buy_v = float(k[9]) if len(k) > 9 else total_v * 0.5
            bs_sum += buy_v - (total_v - buy_v)
        if bs_sum > 0:
            score += 1
            details.append("✅ BS Positive")
        else:
            details.append("⚠️ BS Negative")

        # ── Daily trend ──
        if not is_daily_downtrend(symbol, current_price):
            score += 1
            details.append("✅ Daily bullish/neutral")
        else:
            details.append("❌ Daily bearish")

        # ── Higher lows ──
        lows_1h = [float(k[3]) for k in closed_1h[-8:]]
        hl_count = sum(1 for i in range(1, len(lows_1h)) if lows_1h[i] > lows_1h[i-1])
        if hl_count >= 3:
            score += 1
            details.append("✅ Higher lows (1H)")
        else:
            details.append("⚠️ No clear higher lows")

        # ── OB zone confirmed/retest ──
        closed_4h = klines_4h[:-1]
        avg_v4h = sum(float(k[5]) for k in closed_4h[-10:]) / 10 or 1
        ob_found = False
        for k in reversed(closed_4h[-10:]):
            ko, kc, kh, kl, kv = float(k[1]), float(k[4]), float(k[2]), float(k[3]), float(k[5])
            if kc > ko and kv >= avg_v4h * 1.3 and kl <= current_price <= kh * 1.05:
                score += 1
                details.append(f"✅ OB zone: {format_price(kl)}–{format_price(kh)}")
                ob_found = True
                break
        if not ob_found:
            details.append("⚠️ No OB zone nearby")

        # ── Liq sweep + reclaim (bonus) ──
        sweep_found = False
        for k in reversed(closed_1h[-6:]):
            k_low = float(k[3])
            k_close = float(k[4])
            k_open = float(k[1])
            k_vol = float(k[5])
            avg_v = baseline
            if k_vol / avg_v >= 2.0 and k_close > k_open and (k_close - k_low) / k_close > 0.005:
                score += 1
                details.append("✅ Liq sweep + reclaim detected")
                sweep_found = True
                break
        if not sweep_found:
            details.append("— No liq sweep")

        # Fire if score >= required_score (3 for high-winrate types, 2 otherwise)
        if score < required_score:
            # Note #12: don't just drop low-confluence signals — if volume is
            # abnormally high on its own (well beyond the normal 3x that
            # already earns a confluence point), flag it distinctly instead
            # of requiring the full score bar. Lighter weight than promotion:
            # goes back to Building Momentum, not High Priority.
            if vol_ratio >= 8.0:
                vkey = f"{symbol}_volflag"
                if now - _high_confidence_alerted.get(vkey, 0) >= 4 * 3600:
                    _high_confidence_alerted[vkey] = now
                    send_to_topic(TOPIC_BUILDUPS,
                        f"⚠️ <b>HIGH VOLUME, LOW CONFLUENCE — {symbol}</b>\n\n"
                        f"📡 Signal: {signal_type}\n"
                        f"⚡ Volume: {vol_ratio:.1f}x (abnormal) but confluence only {score}/6\n"
                        f"💰 Price: {format_price(current_price)}\n\n"
                        f"💡 Weak structure/trend but unusually strong volume — "
                        f"watch closely, some of these still run.\n"
                        f"⚠️ <i>Not a full signal — confirm on chart before entry.</i>"
                    )
                    print(f"⚠️ High-volume/low-confluence flag: {symbol} vol={vol_ratio:.1f}x score={score}/6")
            return

        _high_confidence_alerted[key] = now

        # SL/TP
        eql_data = detect_equal_highs_lows(klines_4h, current_price)
        nearest_eql = eql_data["eq_lows"][0] if eql_data["eq_lows"] else None
        nearest_eqh = eql_data["eq_highs"][0] if eql_data["eq_highs"] else None
        sl = nearest_eql["price"] * 0.985 if nearest_eql else current_price * 0.92
        tp1 = nearest_eqh["price"] if nearest_eqh else current_price * 1.08
        tp2 = eql_data["eq_highs"][1]["price"] if len(eql_data["eq_highs"]) >= 2 else tp1 * 1.08
        risk = (current_price - sl) / current_price * 100
        tp1_pct = (tp1 - current_price) / current_price * 100
        tp2_pct = (tp2 - current_price) / current_price * 100
        rr = tp1_pct / risk if risk > 0 else 0

        details_str = "\n".join(f"   {d}" for d in details)
        winrate_line = f"🏆 Signal: {signal_type} ({win_rate}% win rate)\n" if win_rate else f"🏆 Signal: {signal_type}\n"
        hc_msg = (
            f"⭐ <b>HIGH CONFIDENCE — {symbol}</b>\n\n"
            f"{winrate_line}"
            f"📊 Confluence Score: {score}/6\n\n"
            f"{details_str}\n\n"
            f"📐 Entry: {format_price(current_price)}\n"
            f"🔴 SL: {format_price(sl)} (-{risk:.1f}%)\n"
            f"🟢 TP1: {format_price(tp1)} (+{tp1_pct:.1f}%) | TP2: {format_price(tp2)} (+{tp2_pct:.1f}%)\n"
            f"⚖️ R/R: {rr:.1f}x\n\n"
            f"⚠️ <i>Confirm on chart before entry.</i>"
        )
        send_to_topic(TOPIC_BUILDUPS, hc_msg)
        if score >= 6:
            # Top Picks (note #11) is reserved exclusively for perfect 6/6
            # confluence scores — the absolute best-scoring signals get a copy.
            send_to_topic(TOPIC_TOP_PICKS, "🏆 <b>PERFECT SCORE</b>\n\n" + hc_msg)
        elif fast_pumper and score >= 4:
            # Note #3: fast-pumper coins get a lower Top Picks bar too (4+, not 6/6)
            send_to_topic(TOPIC_TOP_PICKS, "⚡ <b>FAST PUMPER</b>\n\n" + hc_msg)
        print(f"⭐ High confidence: {symbol} score={score}/6 signal={signal_type}")
        start_prospect_watch(symbol, signal_type)
        start_hc_followup_watch(symbol, current_price)

        # Note #7: feed the Big Pump topic from strong High Priority hits too
        # — score>=4 (solid confluence) or any fast-pumper hit, since those
        # coins have a track record of fast, sizable moves.
        if score >= 4 or fast_pumper:
            send_big_pump_alert(symbol, current_price, signal_type, klines_4h=klines_4h)

        # ── Auto-add 4 high-confluence zones around this High Priority signal ──
        # 2 above current price (resistance/EQH), 2 below (support/EQL).
        # Tagged "auto_high_priority" so its confirm/retest results route back
        # to High Priority instead of My Setups. Duplicate/overlap-safe.
        try:
            sym_short = symbol.replace("USDT", "")
            eq_highs_above = [h for h in eql_data["eq_highs"] if h["price"] > current_price][:2]
            eq_lows_below  = [l for l in eql_data["eq_lows"]  if l["price"] < current_price][:2]
            for h in eq_highs_above:
                margin = h["price"] * 0.01
                auto_add_zone(sym_short, "4h", h["price"] - margin, h["price"] + margin)
            for l in eq_lows_below:
                margin = l["price"] * 0.01
                auto_add_zone(sym_short, "4h", l["price"] - margin, l["price"] + margin)
        except Exception as e:
            print(f"Auto-zone (high confidence) error {symbol}: {e}")

    except Exception as e:
        print(f"High confidence check error {symbol}: {e}")


_dormant_coil_alerted = {}  # {symbol: last_alert_time}

_btc_divergence_alerted = {}  # {symbol: last_alert_time}

_scalp_alerted = {}  # {symbol: last_alert_time}
_scalp_trades = {}   # {trade_id: {...}} — SEPARATE from active_trades, tight/fast monitoring

def check_scalp_opportunity(symbol):
    """
    Scalping Scanner: finds coins showing enough historical intraday
    VOLATILITY (like XECUSDT — multiple 10%+ swings per day) to realistically
    support repeat scalp entries, then watches 5M for a LOCAL liquidity
    sweep + reclaim with above-average volume. Prefers liquid/established
    coins (24h volume floor) over obscure micro-caps. Fires with SL + a
    single TP1 (next local resistance, % shown), routed to the Big Pump
    topic labeled "for scalping". Short cooldown — supports multiple
    entries on the same coin in one day.
    """
    now = time.time()
    if now - _scalp_alerted.get(symbol, 0) < 90 * 60:  # 1.5h — allows repeat entries per day
        return

    ticker = get_ticker(symbol)
    if not ticker:
        return
    quote_vol_24h = float(ticker.get("quoteVolume", 0))
    if quote_vol_24h < 500_000:
        return  # too illiquid for reliable scalp fills
    current_price = float(ticker["lastPrice"])

    # Volatility filter — does this coin already show big intraday swings?
    # Uses a cheaper ~8h window (100 x 5m candles) rather than a full 24h
    # fetch to keep this affordable to run across the whole watchlist often.
    klines_5m = get_klines(symbol, interval="5m", limit=100)
    if not klines_5m or len(klines_5m) < 80:
        return
    closed_5m = klines_5m[:-1]

    window_size = 24  # ~2 hours per window
    windows = [closed_5m[i:i+window_size] for i in range(0, len(closed_5m) - window_size, window_size)]
    big_swing_count = 0
    for w in windows:
        if not w:
            continue
        w_high = max(float(k[2]) for k in w)
        w_low = min(float(k[3]) for k in w)
        if w_low > 0 and (w_high - w_low) / w_low * 100 >= 8.0:
            big_swing_count += 1
    if big_swing_count < 2:
        return  # not volatile enough historically — core filter per user's request

    # Local liquidity sweep + reclaim on the most recent 5M candles
    recent = closed_5m[-20:]
    if len(recent) < 15:
        return
    local_lows = [float(k[3]) for k in recent[:-3]]
    local_swing_low = min(local_lows) if local_lows else 0
    if local_swing_low <= 0:
        return

    last = recent[-1]
    l_open, l_close = float(last[1]), float(last[4])
    l_vol = float(last[5])
    l_buy = float(last[9]) if len(last) > 9 else l_vol * 0.5
    buy_ratio = l_buy / l_vol if l_vol > 0 else 0.5

    swept = any(float(k[3]) <= local_swing_low * 1.005 for k in recent[-4:-1])
    reclaimed = l_close > l_open and l_close > local_swing_low

    prior_vols = [float(k[5]) for k in recent[-8:-1]]
    avg_vol = sum(prior_vols) / len(prior_vols) if prior_vols else 1
    vol_ratio = l_vol / avg_vol if avg_vol > 0 else 0

    if not (swept and reclaimed and vol_ratio >= 2.5 and buy_ratio >= 0.58):
        return

    # Bonus SMC confluence — OB zone nearby (preferred, not required)
    ob_note = ""
    avg_v_ob = sum(float(k[5]) for k in recent[-10:]) / 10 or 1
    for k in reversed(recent[-10:]):
        ko, kc, kh, kl, kv = float(k[1]), float(k[4]), float(k[2]), float(k[3]), float(k[5])
        if kc > ko and kv >= avg_v_ob * 1.3 and kl <= current_price <= kh * 1.05:
            ob_note = f"🔲 OB zone: {format_price(kl)}–{format_price(kh)}\n"
            break

    # TP1 = next local resistance from recent 5m highs
    highs_above = [float(k[2]) for k in closed_5m[-60:] if float(k[2]) > current_price * 1.01]
    tp1 = min(highs_above) if highs_above else current_price * 1.10
    tp1 = max(tp1, current_price * 1.05)
    tp1_pct = (tp1 - current_price) / current_price * 100
    if tp1_pct < 5.0:
        return  # not worth it — too little room to the next resistance

    sl = local_swing_low * 0.995
    sl_pct = (current_price - sl) / current_price * 100

    _scalp_alerted[symbol] = now
    send_to_topic(TOPIC_BIG_PUMP,
        f"⚡ <b>SCALP SETUP — {symbol}</b> [for scalping]\n\n"
        f"💰 Price: {format_price(current_price)}\n"
        f"💧 Local liquidity swept & reclaimed: {format_price(local_swing_low)}\n"
        f"⚡ Volume: {vol_ratio:.1f}x | Buy: {buy_ratio*100:.0f}%\n"
        f"{ob_note}"
        f"📊 24h volume: ${quote_vol_24h/1e6:.1f}M | Volatile: {big_swing_count}x 8%+ swings recently\n\n"
        f"📐 Entry: {format_price(current_price)}\n"
        f"🔴 SL: {format_price(sl)} (-{sl_pct:.1f}%)\n"
        f"🟢 TP1: {format_price(tp1)} (+{tp1_pct:.1f}%) — next local resistance\n\n"
        f"⚠️ <i>Fast/scalp setup — confirm on chart, use a tight stop-loss.</i>"
    )
    print(f"⚡ Scalp setup: {symbol} vol={vol_ratio:.1f}x tp1=+{tp1_pct:.1f}%")

    # Auto-register for the dedicated scalp trade monitor (tight SL, fast checks)
    trade_id = f"{symbol}_scalp_{int(now)}"
    _scalp_trades[trade_id] = {
        "symbol": symbol, "entry": current_price, "sl": sl, "tp1": tp1,
        "started": now, "tp_removed": False, "closed": False,
    }


def scan_btc_divergence():
    """
    Relative strength scanner: when BTC is meaningfully red, finds coins
    showing real relative strength (positive or much-less-negative than BTC)
    — these often have a coin-specific catalyst, are getting quietly
    accumulated regardless of the broader market, or become the leaders
    once the market recovers. Confirms with volume/BS pressure so it's not
    just noise, then feeds straight into the existing Big Pump pipeline
    (send_big_pump_alert → later "PUMP CONFIRMED" in High Priority once the
    move actually develops), same as every other Big Pump source.
    """
    btc_ticker = get_ticker("BTCUSDT")
    if not btc_ticker:
        return
    btc_change = float(btc_ticker["priceChangePercent"])
    if btc_change > -1.0:
        return  # only scan when BTC is meaningfully down

    now = time.time()
    for symbol in list(watchlist):
        if symbol in removed_coins or symbol == "BTCUSDT":
            continue
        if now - _btc_divergence_alerted.get(symbol, 0) < 6 * 3600:
            continue
        try:
            ticker = get_ticker(symbol)
            if not ticker:
                continue
            change = float(ticker["priceChangePercent"])
            divergence = change - btc_change
            # Require genuine relative strength: coin actually green, and
            # meaningfully stronger than BTC — not just "less red"
            if change < 0.5 or divergence < 5.0:
                continue

            current_price = float(ticker["lastPrice"])

            klines_1h = get_klines(symbol, interval="1h", limit=10)
            if not klines_1h or len(klines_1h) < 5:
                continue
            closed = klines_1h[:-1]
            last = closed[-1]
            l_vol = float(last[5])
            l_buy = float(last[9]) if len(last) > 9 else l_vol * 0.5
            buy_ratio = l_buy / l_vol if l_vol > 0 else 0.5
            prior_vols = [float(k[5]) for k in closed[-6:-1]]
            avg_vol = sum(prior_vols) / len(prior_vols) if prior_vols else 1
            vol_ratio = l_vol / avg_vol if avg_vol > 0 else 0

            if buy_ratio < 0.55 or vol_ratio < 1.5:
                continue  # not enough real volume/buy conviction behind the divergence

            _btc_divergence_alerted[symbol] = now
            send_to_topic(TOPIC_BUILDUPS,
                f"💪 <b>RELATIVE STRENGTH — {symbol}</b>\n\n"
                f"💰 Price: {format_price(current_price)}\n"
                f"📊 24h: {change:+.2f}% (BTC: {btc_change:+.2f}%) — {divergence:+.1f}% stronger than BTC\n"
                f"⚡ Volume: {vol_ratio:.1f}x | Buy: {buy_ratio*100:.0f}%\n\n"
                f"💡 Holding up/pumping while BTC and the broader market are red — "
                f"possible catalyst, quiet accumulation, or early-recovery leader.\n"
                f"⚠️ <i>Confirm on chart before entry.</i>"
            )
            print(f"💪 BTC divergence: {symbol} {change:+.1f}% vs BTC {btc_change:+.1f}%")
            track_building_signal(symbol, "BTC Divergence", current_price)
            send_big_pump_alert(symbol, current_price, "BTC Divergence (Relative Strength)")
        except Exception as e:
            print(f"BTC divergence check error {symbol}: {e}")


def scan_dormant_coil_candidates():
    """
    Detects coins that have been deeply down AND stayed down for an extended
    period (weeks) with declining volatility/volume — the hypothesis (note
    #13) being that long-suppressed, quietly-basing coins are more likely to
    give a sudden big pump (50-300%) later. This is distinct from every other
    detector in this file: those catch pumps already starting, this flags
    still-dormant coins that MIGHT pump later, so it's not confused with
    active signals (separate 🌙 DORMANT COIL WATCH format, no trade plan).
    Runs on 1D klines — no need to check often.
    """
    now = time.time()
    for symbol in list(watchlist):
        if symbol in removed_coins:
            continue
        try:
            key = symbol
            if now - _dormant_coil_alerted.get(key, 0) < 3 * 24 * 3600:  # re-check every 3 days
                continue

            klines_1d = get_klines(symbol, interval="1d", limit=60)
            if not klines_1d or len(klines_1d) < 30:
                continue

            closed = klines_1d[:-1]
            highs  = [float(k[2]) for k in closed]
            closes = [float(k[4]) for k in closed]
            vols   = [float(k[5]) for k in closed]

            recent_high = max(highs)
            recent_high_idx = highs.index(recent_high)
            current_price = closes[-1]
            if recent_high <= 0:
                continue

            # (2) Large % decline from recent high
            decline_pct = (recent_high - current_price) / recent_high * 100
            if decline_pct < 40:
                continue

            # (1) Extended time suppressed: the high must be well in the past,
            # and price has stayed in a tight range since
            days_since_high = len(closed) - 1 - recent_high_idx
            if days_since_high < 14:
                continue

            recent_window = closes[-14:]
            if min(recent_window) <= 0:
                continue
            window_range_pct = (max(recent_window) - min(recent_window)) / min(recent_window) * 100
            if window_range_pct > 25:
                continue  # still too volatile to call it "coiling"

            # (3) Decreasing volatility/volume exhaustion — classic pre-breakout coiling
            if len(vols) < 30:
                continue
            early_vol = sum(vols[-30:-14]) / 16
            recent_vol = sum(vols[-14:]) / 14
            if early_vol <= 0 or recent_vol >= early_vol * 0.7:
                continue  # volume hasn't meaningfully dried up yet

            _dormant_coil_alerted[key] = now
            send_to_topic(TOPIC_BUILDUPS,
                f"🌙 <b>DORMANT COIL WATCH — {symbol}</b>\n\n"
                f"💰 Price: {format_price(current_price)}\n"
                f"📉 Down {decline_pct:.0f}% from {format_price(recent_high)} ({days_since_high}d ago)\n"
                f"📊 14d range: {window_range_pct:.0f}% (tight — coiling)\n"
                f"📉 Volume exhausted: {recent_vol/early_vol*100:.0f}% of the earlier average\n\n"
                f"💡 Long-suppressed, quietly basing — historically coins like this can give "
                f"sudden 50-300% pumps. This is early monitoring only, not an active signal.\n"
                f"⚠️ <i>No trade plan yet — just a watchlist flag.</i>"
            )
            print(f"🌙 Dormant coil candidate: {symbol} down {decline_pct:.0f}% for {days_since_high}d, "
                  f"vol at {recent_vol/early_vol*100:.0f}% of earlier")
        except Exception as e:
            print(f"Dormant coil scan error {symbol}: {e}")


_whale_trade_alerted = {}  # {symbol: last_alert_time}

def check_whale_trades(symbol):
    """
    Whale/block trade detection (note #14): Binance's API can't identify
    wallet identities (no on-chain data), so this approximates "smart money
    entering" using the closest available proxy — an individual trade that's
    abnormally large vs this coin's typical recent trade size, from the
    aggTrades stream (single trade size, not aggregated candle volume like
    every other volume-ratio signal in this file).
    """
    now = time.time()
    if now - _whale_trade_alerted.get(symbol, 0) < 2 * 3600:
        return

    trades = get_agg_trades(symbol, limit=30)
    if not trades or len(trades) < 15:
        return

    sizes = [float(t["p"]) * float(t["q"]) for t in trades]
    baseline_sizes = sorted(sizes)[:-1]  # exclude the largest so it doesn't skew its own baseline
    avg_size = sum(baseline_sizes) / len(baseline_sizes) if baseline_sizes else 0
    if avg_size <= 0:
        return

    latest = trades[-1]
    latest_value = float(latest["p"]) * float(latest["q"])
    ratio = latest_value / avg_size

    # Ratio alone isn't enough on a near-zero baseline (illiquid coin) — require
    # a real dollar floor too.
    if ratio < 15.0 or latest_value < 5000:
        return

    # isBuyerMaker=True means the seller was the aggressor (a sell); False means
    # the buyer was the aggressor (a buy).
    is_buy = not latest.get("m", True)

    ticker = get_ticker(symbol)
    current_price = float(ticker["lastPrice"]) if ticker else float(latest["p"])

    _whale_trade_alerted[symbol] = now
    send_to_topic(TOPIC_SPIKES,
        f"🐋 <b>Large trade detected — {symbol}</b>\n\n"
        f"💰 Price: {format_price(current_price)}\n"
        f"💵 Trade size: ${latest_value:,.0f} ({ratio:.0f}x typical trade size)\n"
        f"{'🟢 Buy-side' if is_buy else '🔴 Sell-side'} aggressor\n\n"
        f"💡 Approximated \"smart money\" signal — one unusually large single trade, "
        f"not aggregated candle volume. Not confirmation on its own.\n"
        f"⚠️ <i>Confirm on chart before entry.</i>"
    )
    print(f"🐋 Whale trade: {symbol} ${latest_value:,.0f} ({ratio:.0f}x, {'buy' if is_buy else 'sell'})")
    start_prospect_watch(symbol, "Large Trade Detected")


def scan_global_liq_reclaim():
    """
    Scans all watchlist coins every 2 minutes for liquidity sweep + reclaim.
    Uses 5M klines (fast detection) + ticker for volume confirmation.
    When sweep + reclaim detected → fires to High Priority with full SMC
    confluence analysis, TP scaled to volume size, and zone/line suggestions.
    """
    now = time.time()
    for symbol in list(watchlist):
        if symbol in removed_coins:
            continue
        if now - _global_liq_alerted.get(symbol, 0) < 6 * 3600:
            continue
        try:
            ticker = get_ticker(symbol)
            if not ticker:
                continue
            current_price = float(ticker["lastPrice"])
            change_24h = float(ticker["priceChangePercent"])

            klines_5m = get_klines(symbol, interval="5m", limit=20)
            klines_1h = get_klines(symbol, interval="1h", limit=30)
            if not klines_5m or len(klines_5m) < 10:
                continue

            closed_5m = klines_5m[:-1]
            vols_5m = [float(k[5]) for k in closed_5m]
            avg_vol_5m = sum(vols_5m[-10:-3]) / 7 if len(vols_5m) >= 10 else 1

            # Find a recent sweep candle (wicked below then recovered)
            sweep_candle = None
            sweep_low = None
            sweep_vol_ratio = 0

            for k in reversed(closed_5m[-8:]):
                k_low   = float(k[3])
                k_close = float(k[4])
                k_open  = float(k[1])
                k_vol   = float(k[5])
                k_high  = float(k[2])
                candle_range = k_high - k_low
                lower_wick = min(k_close, k_open) - k_low
                if candle_range <= 0:
                    continue
                wick_ratio = lower_wick / candle_range
                vol_r = k_vol / avg_vol_5m if avg_vol_5m > 0 else 0

                # Sweep: long lower wick + reclaim above open + volume
                if wick_ratio >= 0.40 and k_close > k_open and vol_r >= 1.5:
                    sweep_candle = k
                    sweep_low = k_low
                    sweep_vol_ratio = vol_r
                    break

            if not sweep_candle:
                continue

            # Confirm: current price still above sweep low + positive momentum
            if current_price < sweep_low * 1.001:
                continue
            if change_24h < -3:
                continue

            # ── SMC Confluence ──
            confluence = []
            daily_down = is_daily_downtrend(symbol, current_price)
            if not daily_down:
                confluence.append("✅ Daily trend bullish/neutral")
            else:
                confluence.append("⚠️ Daily trend bearish — lower confidence")

            # Higher lows on 1H
            if klines_1h and len(klines_1h) >= 10:
                lows_1h = [float(k[3]) for k in klines_1h[-8:-1]]
                hl_count = sum(1 for i in range(1, len(lows_1h)) if lows_1h[i] > lows_1h[i-1] * 1.002)
                if hl_count >= 3:
                    confluence.append("✅ Higher lows forming (1H)")

            # OB zone nearby
            klines_4h = get_klines(symbol, interval="4h", limit=20)
            ob_zone = None
            if klines_4h and len(klines_4h) >= 5:
                closed_4h = klines_4h[:-1]
                for k in reversed(closed_4h[-10:]):
                    k_open  = float(k[1])
                    k_close = float(k[4])
                    k_low   = float(k[3])
                    k_high  = float(k[2])
                    k_vol   = float(k[5])
                    avg_v4h = sum(float(x[5]) for x in closed_4h[-8:]) / 8
                    if (k_close > k_open and k_vol >= avg_v4h * 1.5 and
                            k_low <= current_price <= k_high * 1.05):
                        ob_zone = (k_low, k_high)
                        confluence.append(f"✅ 4H OB zone: {format_price(k_low)}–{format_price(k_high)}")
                        break

            # FVG support
            if klines_1h and len(klines_1h) >= 5:
                fvgs = detect_fvg(klines_1h, min_gap_pct=0.2)
                nearby_bull = [f for f in fvgs if f["type"] == "bullish"
                               and f["bottom"] < current_price
                               and f["top"] > current_price * 0.97]
                if nearby_bull:
                    confluence.append(f"✅ FVG support: {format_price(nearby_bull[0]['bottom'])}–{format_price(nearby_bull[0]['top'])}")

            # Equal lows (liquidity pool that got swept)
            if klines_1h and len(klines_1h) >= 5:
                eql_data = detect_equal_highs_lows(klines_1h, current_price)
                swept_eql = [l for l in eql_data["eq_lows"]
                             if sweep_low and abs(l["price"] - sweep_low) / sweep_low <= 0.02]
                if swept_eql:
                    confluence.append(f"✅ EQL swept: {format_price(swept_eql[0]['price'])} ({swept_eql[0]['touches']}x tested) — stops cleared")

            # Skip if too weak
            pos_confluence = sum(1 for c in confluence if c.startswith("✅"))

            # ── Retest Reclaim vs Reversal Reclaim ──
            klines_1d_pump = get_klines(symbol, interval="1d", limit=10)
            is_retest_reclaim = False
            recent_pump_pct = 0
            if klines_1d_pump and len(klines_1d_pump) >= 5:
                closed_1d = klines_1d_pump[:-1]
                recent_high = max(float(k[2]) for k in closed_1d[-7:])
                recent_low  = min(float(k[3]) for k in closed_1d[-7:])
                if recent_low > 0:
                    recent_pump_pct = (recent_high - recent_low) / recent_low * 100
                retrace_from_high = (recent_high - current_price) / recent_high * 100 if recent_high > 0 else 0
                is_retest_reclaim = recent_pump_pct >= 15 and 20 <= retrace_from_high <= 65

            if daily_down and pos_confluence < 2 and not is_retest_reclaim:
                continue
            if pos_confluence < 1:
                continue

            # Redesign (note #11): LIQUIDITY RECLAIM / RETEST RECLAIM / POWER
            # SIGNAL go to Building Momentum. High Priority only gets ⭐ HIGH
            # CONFIDENCE (via the promotion check below), and Top Picks is now
            # reserved exclusively for perfect 6/6 confluence scores.
            target_topic = TOPIC_BUILDUPS
            signal_prefix = "🎯 RETEST RECLAIM" if is_retest_reclaim else "🔥 LIQUIDITY RECLAIM"
            promotion_signal_type = "Retest Reclaim" if is_retest_reclaim else "Liquidity Reclaim"
            is_power_signal = (
                is_retest_reclaim and
                pos_confluence >= 4 and
                ob_zone is not None and
                not daily_down and
                sweep_vol_ratio >= 5
            )
            if is_power_signal:
                signal_prefix = "🚀 POWER SIGNAL"
                promotion_signal_type = "Power Signal"

            # ── TP scaling based on volume ──
            if sweep_vol_ratio >= 15:
                tp1_pct, tp2_pct = 0.07, 0.25
                vol_label = "🔥 Extreme (institutional)"
                pump_est = "25-40%+ potential"
            elif sweep_vol_ratio >= 8:
                tp1_pct, tp2_pct = 0.05, 0.18
                vol_label = "⚡ Strong"
                pump_est = "15-25% potential"
            elif sweep_vol_ratio >= 4:
                tp1_pct, tp2_pct = 0.05, 0.12
                vol_label = "📊 Moderate"
                pump_est = "8-15% potential"
            else:
                tp1_pct, tp2_pct = 0.05, 0.08
                vol_label = "⚠️ Weak"
                pump_est = "5-10% potential — watch closely"

            tl_note = check_trendline_sweep_confluence(symbol, current_price, tf="1h")
            if tl_note:
                confluence.append("✅ Trendline liquidity sweep")

            entry = current_price
            sl = sweep_low * 0.985
            tp1 = entry * (1 + tp1_pct)
            tp2 = entry * (1 + tp2_pct)
            risk = (entry - sl) / entry * 100
            rr = tp1_pct * 100 / risk if risk > 0 else 0

            # ── Zone/Line suggestions ──
            suggest_lines = []
            if klines_4h:
                closed_4h_s = klines_4h[:-1]
                highs_4h = sorted(set(
                    float(k[2]) for k in closed_4h_s
                    if float(k[2]) > current_price * 1.01
                ), key=lambda x: abs(x - current_price))[:3]
                for h in highs_4h[:2]:
                    suggest_lines.append(f"<code>/addline {symbol.replace('USDT','')} {format_price(h)} 4h</code>")
                lows_4h = []
                for i in range(3, len(closed_4h_s) - 2):
                    l = float(closed_4h_s[i][3])
                    if all(l <= float(closed_4h_s[i+j][3]) for j in [-2,-1,1,2]) and l < current_price * 0.97:
                        lows_4h.append(l)
                if lows_4h:
                    best_low = min(lows_4h, key=lambda x: abs(x - current_price * 0.95))
                    margin = best_low * 0.01
                    suggest_lines.append(f"<code>/addzone {symbol.replace('USDT','')} {format_price(best_low-margin)} {format_price(best_low+margin)} 4h</code>")

            suggest_str = ("\n📏 " + " | ".join(suggest_lines)) if suggest_lines else ""

            # ── Previous signals summary ──
            cutoff_24h = now - 24 * 3600
            prev_sigs = []
            for data in signal_performance.values():
                if data.get("symbol") == symbol and data.get("signal_time", 0) >= cutoff_24h:
                    from datetime import datetime as _dt2
                    t = _dt2.fromtimestamp(data["signal_time"]).strftime("%H:%M")
                    prev_sigs.append(f"{t} {data.get('signal_type','')}")
            prev_sigs = sorted(set(prev_sigs))[-4:]
            prev_str = ("\n📋 Today: " + " → ".join(prev_sigs)) if prev_sigs else ""

            _global_liq_alerted[symbol] = now
            send_to_topic(target_topic,
                f"{'🚀' if is_power_signal else '🎯' if is_retest_reclaim else '🔥'} "
                f"<b>{signal_prefix} — {symbol}</b>\n\n"
                f"💧 Swept: {format_price(sweep_low)} on {sweep_vol_ratio:.1f}x → reclaimed ✅\n"
                f"💰 {format_price(current_price)} | 24h: {change_24h:+.1f}% | {vol_label}\n"
                f"📈 {pump_est}"
                + (f" | Pump: +{recent_pump_pct:.0f}% prior" if is_retest_reclaim else "") +
                f"\n\n📊 SMC ({pos_confluence}/{len(confluence)}): "
                + " | ".join(c.replace("✅ ", "").replace("⚠️ ", "⚠️") for c in confluence) +
                f"\n\n📐 Entry: {format_price(entry)} | SL: {format_price(sl)} (-{risk:.1f}%)\n"
                f"   TP1: {format_price(tp1)} (+{tp1_pct*100:.0f}%) | TP2: {format_price(tp2)} (+{tp2_pct*100:.0f}%) | R/R: {rr:.1f}x"
                + suggest_str + prev_str +
                f"\n\n⚠️ <i>Confirm on chart before entry. Use stop-loss.</i>"
            )
            check_high_confidence_signal(symbol, promotion_signal_type, current_price)
            track_building_signal(symbol, "Liquidity Reclaim", current_price)
            print(f"🔥 Global liq reclaim: {symbol} vol={sweep_vol_ratio:.1f}x")

        except Exception as e:
            print(f"Global liq scan error {symbol}: {e}")


_reversal_pump_alerted = {}  # {symbol: last_alert_time}

def scan_reversal_pumps():
    """
    Scans coins in daily downtrend for extreme volume reversal —
    the SKL pattern: long downtrend → sudden 8x+ volume vertical spike.
    These are often the biggest pumps precisely because sellers are exhausted
    and any buying pressure creates a vacuum move up.
    Fires to High Priority (not filtered by downtrend check).
    """
    now = time.time()
    for symbol in list(watchlist):
        if now - _reversal_pump_alerted.get(symbol, 0) < 6 * 3600:
            continue
        try:
            ticker = get_ticker(symbol)
            if not ticker:
                continue
            current_price = float(ticker["lastPrice"])
            change_24h = float(ticker["priceChangePercent"])

            # Must be in downtrend (that's the whole point)
            if not is_daily_downtrend(symbol, current_price):
                continue

            klines_1h = get_klines(symbol, interval="1h", limit=15)
            if not klines_1h or len(klines_1h) < 10:
                continue

            closed = klines_1h[:-1]
            last = closed[-1]
            l_open  = float(last[1])
            l_close = float(last[4])
            l_vol   = float(last[5])
            l_buy   = float(last[9]) if len(last) > 9 else l_vol * 0.5

            # Baseline from candles before the spike
            avg_vol = sum(float(k[5]) for k in closed[-10:-2]) / 8
            vol_ratio = l_vol / avg_vol if avg_vol > 0 else 0
            buy_ratio = l_buy / l_vol if l_vol > 0 else 0

            # Strong reversal: extreme volume + strong green + significant price move
            price_move = (l_close - l_open) / l_open * 100 if l_open > 0 else 0
            is_reversal = (
                vol_ratio >= 8.0 and
                l_close > l_open and
                price_move >= 3.0 and
                buy_ratio >= 0.55 and
                change_24h >= 0
            )

            if is_reversal:
                _reversal_pump_alerted[symbol] = now
                send_to_topic(TOPIC_BUILDUPS,
                    f"🔄 <b>REVERSAL PUMP — {symbol}</b>\n\n"
                    f"💰 Price: {format_price(current_price)} (+{change_24h:.1f}% 24h)\n"
                    f"📈 1H candle: +{price_move:.1f}%\n"
                    f"💥 Volume: {vol_ratio:.1f}x normal | Buy: {buy_ratio*100:.0f}%\n\n"
                    f"⚠️ <i>Was in daily downtrend — this extreme volume may signal "
                    f"a trend reversal. These moves can be fast and large. "
                    f"Check the chart before entry.</i>"
                )
                track_building_signal(symbol, f"Reversal Pump [1H]", current_price)
                print(f"🔄 Reversal pump: {symbol} vol={vol_ratio:.1f}x move={price_move:.1f}%")
        except Exception as e:
            print(f"Reversal scan error {symbol}: {e}")


def scan_volume_accumulation():
    """
    Scans all watchlist coins for quiet volume accumulation — price barely
    moved but volume is rising steadily. Classic pre-pump signal. Fires to
    Top Picks. Runs every 2 hours.
    """
    now = time.time()
    for symbol in list(watchlist):
        if now - _vol_accum_alerted.get(symbol, 0) < 8 * 3600:
            continue
        try:
            klines = get_klines(symbol, interval="1h", limit=25)
            ticker = get_ticker(symbol)
            if not klines or len(klines) < 15 or not ticker:
                continue
            closed = klines[:-1]
            price_change = abs(float(ticker["priceChangePercent"]))
            if price_change > 5:
                continue  # already pumping, not accumulation

            vols = [float(k[5]) for k in closed[-12:]]
            baseline = sum(vols[:5]) / 5 if len(vols) >= 5 else 1
            recent   = sum(vols[-4:]) / 4 if len(vols) >= 4 else 0
            vol_ratio = recent / baseline if baseline > 0 else 0
            if vol_ratio < 3.0:
                continue

            # Volume slope must be positive
            n = len(vols)
            mean_x = (n-1)/2
            mean_y = sum(vols)/n
            slope = sum((i-mean_x)*(vols[i]-mean_y) for i in range(n))
            slope /= sum((i-mean_x)**2 for i in range(n)) or 1
            if slope <= 0:
                continue

            current_price = float(ticker["lastPrice"])
            _vol_accum_alerted[symbol] = now
            send_to_topic(TOPIC_BUILDUPS,
                f"🔍 <b>VOLUME ACCUMULATION — {symbol}</b>\n\n"
                f"💰 Price: {format_price(current_price)} ({float(ticker['priceChangePercent']):+.1f}% 24h)\n"
                f"📈 Volume {vol_ratio:.1f}x above baseline — rising steadily\n"
                f"😴 Price barely moved — quiet accumulation pattern\n\n"
                f"💡 <i>This is how pumps start — volume before price. "
                f"Watch for a breakout candle.</i>"
            )
            print(f"🔍 Volume accumulation: {symbol} vol={vol_ratio:.1f}x")
        except Exception as e:
            print(f"Volume accum scan error {symbol}: {e}")


def scan_postpump_retest():
    """
    Scans watchlist coins for post-pump retest opportunities — coins that
    pumped significantly (>20%) in the last 7 days and are now retesting
    their breakout level / OB zone on 15M, 1H, or 4H. Fires to Top Picks.
    """
    now = time.time()
    for symbol in list(watchlist):
        if now - _postpump_retest_alerted.get(symbol, 0) < 12 * 3600:
            continue
        try:
            klines_4h = get_klines(symbol, interval="4h", limit=50)
            klines_1h = get_klines(symbol, interval="1h", limit=30)
            ticker = get_ticker(symbol)
            if not klines_4h or not ticker:
                continue

            current_price = float(ticker["lastPrice"])
            change_24h    = float(ticker["priceChangePercent"])

            # Find recent pump high (last 7 days = ~42 4H candles)
            closed_4h = klines_4h[:-1]
            recent_high = max(float(k[2]) for k in closed_4h[-42:])
            recent_low  = min(float(k[3]) for k in closed_4h[-42:])
            pump_pct = (recent_high - recent_low) / recent_low * 100 if recent_low > 0 else 0

            if pump_pct < 20:
                continue  # not a significant pump

            # Current price should be retesting (below pump high but above midpoint)
            mid = recent_low + (recent_high - recent_low) * 0.5
            retrace = (recent_high - current_price) / recent_high * 100
            if not (10 <= retrace <= 60):
                continue  # too far or not pulled back enough

            # Check for retest pattern on 1H
            retest_note = None
            if klines_1h and len(klines_1h) >= 10:
                pattern = detect_break_retest_pattern(klines_1h, current_price)
                if pattern and ("Retest in progress" in pattern or "confirmed" in pattern):
                    retest_note = pattern

            _postpump_retest_alerted[symbol] = now
            send_to_topic(TOPIC_BUILDUPS,
                f"🎯 <b>POST-PUMP RETEST — {symbol}</b>\n\n"
                f"💰 Price: {format_price(current_price)} ({change_24h:+.1f}% 24h)\n"
                f"📈 Recent pump: +{pump_pct:.0f}% "
                f"({format_price(recent_low)} → {format_price(recent_high)})\n"
                f"📉 Now retesting: -{retrace:.0f}% from pump high\n"
                + (f"📐 1H: {retest_note}\n" if retest_note else "") +
                f"\n💡 <i>Classic retest entry setup — if the breakout level holds, "
                f"next leg up can be significant.</i>\n\n"
                f"⚠️ <i>Confirm on the chart before entry.</i>"
            )
            print(f"🎯 Post-pump retest: {symbol} pump={pump_pct:.0f}% retrace={retrace:.0f}%")
        except Exception as e:
            print(f"Post-pump scan error {symbol}: {e}")


_last_watchlist_validate = 0  # 0 = run immediately on first check

def auto_validate_watchlist():
    """
    Runs at startup and then every 12h (reduced from 24h).
    Checks every coin against Binance API — removes invalid/delisted ones permanently.
    Also ensures removed_coins are filtered from watchlist on every run.
    """
    global _last_watchlist_validate
    now = time.time()
    if now - _last_watchlist_validate < 12 * 3600:
        return
    _last_watchlist_validate = now

    # First: re-apply removed_coins filter to watchlist in case any slipped back
    before = len(watchlist)
    for coin in list(removed_coins):
        if coin in watchlist:
            watchlist.remove(coin)
    if len(watchlist) < before:
        save_watchlist_file()
        print(f"🗑 Re-applied removed_coins filter: removed {before - len(watchlist)} coins")

    print("🔍 Auto-validating watchlist against Binance...")
    invalid = []
    for symbol in list(watchlist):
        try:
            # Note: checking exchangeInfo instead of just ticker/price — a
            # symbol can return valid price data even when it's not
            # actually SPOT-tradeable (halted, margin/futures-only, etc).
            # MBOXUSDT case: scalping scanner picked it up with real price/
            # volume data, but it's not actually on Binance Spot.
            r = http_session.get(
                f"https://api.binance.com/api/v3/exchangeInfo?symbol={symbol}",
                timeout=5
            )
            if r.status_code in (400, 404):
                invalid.append(symbol)
            elif r.status_code == 200:
                data = r.json()
                symbols_info = data.get("symbols", [])
                if not symbols_info:
                    invalid.append(symbol)
                else:
                    info = symbols_info[0]
                    is_trading = info.get("status") == "TRADING"
                    is_spot = "SPOT" in info.get("permissions", []) or info.get("isSpotTradingAllowed", False)
                    if not (is_trading and is_spot):
                        invalid.append(symbol)
            # 418/429 = rate limited, skip — don't remove on transient errors
        except Exception:
            pass  # network error, skip this coin

    if invalid:
        for symbol in invalid:
            if symbol in watchlist:
                watchlist.remove(symbol)
            removed_coins.add(symbol)
        save_removed_coins()
        save_watchlist_file()
        send_to(ADMIN_CHAT_ID,
            f"🗑 <b>Auto-removed {len(invalid)} invalid coin(s)</b>\n\n"
            f"These coins returned an error from Binance (not found / delisted):\n"
            + "\n".join(f"• {s}" for s in invalid) +
            f"\n\nWatchlist now: {len(watchlist)} coins.\n"
            f"Use /add SYMBOL to re-add if this was a mistake."
        )
        print(f"🗑 Auto-removed {len(invalid)} invalid coins: {invalid}")
    else:
        print(f"✅ Watchlist validation complete — all {len(watchlist)} coins valid")


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
                tl_sweep_retest = check_trendline_sweep_confluence(symbol, current_price, tf=tf_r if tf_r in ("1h","4h") else "4h")
                full_confluence_tl = build_entry_decision_block(symbol, current_price, tf=tf_r if tf_r in ("1h","4h") else "4h")
                retest_msg = (
                    f"🏆 <b>TRENDLINE RETEST CONFIRMED! [{tf_r.upper()}]</b>\n\n"
                    f"🪙 <b>{symbol}</b>\n"
                    f"💰 Price: {format_price(current_price)}\n"
                    f"📊 24h: {change_24h:+.2f}%\n"
                    f"📐 Break → retest → continuation\n"
                    f"📈 From breakout: <b>+{gain_pct:.1f}%</b>\n"
                    f"⚡ Volume: {vol_ratio:.1f}x\n"
                    + (f"\n{full_confluence_tl}\n" if full_confluence_tl else "") +
                    f"\n🕐 {datetime.now().strftime('%H:%M:%S')}\n\n"
                    f"⚠️ <i>Strong setup! Check OB/FVG before entry.</i>"
                )
                if tf_r == "1h":
                    # 1H trendline retests go to My Setups — informative but not
                    # High Priority noise for all subscribers
                    send_to_topic(TOPIC_MY_SETUPS, retest_msg)
                    sent = True
                else:
                    sent = send_all(retest_msg, symbol=symbol)
                if sent:
                    print(f"🏆 [{tf_r.upper()}] Trendline Retest: {symbol}{' [TL SWEEP]' if tl_sweep_retest else ''}")
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
        RESULT_MIN_HOURS = 24   # minimum 24h before a result can be finalized
        RESULT_AUTO_HOURS = 48  # after 48h, send result regardless of dump
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
                    signal_performance[perf_key]["highest_after"] = last_high  # keep in sync for /report
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

            if peak_pct < 5.0:
                continue

            ticker = get_ticker(symbol)
            if not ticker:
                continue
            current_price = float(ticker["lastPrice"])
            dumped = current_price < highest * 0.90  # 10%+ pullback from peak
            window_passed = (now - data["signal_time"]) >= RESULT_MIN_HOURS * 3600
            auto_window = (now - data["signal_time"]) >= RESULT_AUTO_HOURS * 3600

            # Fire result if: dumped after minimum window, OR 48h has passed
            should_send = not data.get("result_sent") and (
                (dumped and window_passed) or auto_window
            )

            if should_send:
                peak_time = data.get("peak_time", data["signal_time"])
                peak_hrs = (peak_time - data["signal_time"]) / 3600
                emoji = "🚀" if peak_pct >= 20 else "🟢" if peak_pct >= 10 else "🟡"
                result_type = "Auto (48h)" if auto_window and not dumped else "Peak reached"
                current_vs_signal = (current_price - data["signal_price"]) / data["signal_price"] * 100

                # Send to subscribers
                send_all(
                    f"{emoji} <b>SIGNAL RESULT</b>\n\n"
                    f"🪙 <b>{symbol}</b>\n"
                    f"📊 {data['signal_type']}\n"
                    f"💰 {format_price(data['signal_price'])} → {format_price(highest)}\n"
                    f"📈 <b>Peak: +{peak_pct:.1f}%</b> | ⏱ {peak_hrs:.1f}hr\n"
                    f"💰 Now: {format_price(current_price)} ({current_vs_signal:+.1f}% from signal)\n"
                    f"📋 {result_type}",
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

        check_retest_watches()
        check_btc_market_condition()
        auto_validate_watchlist()  # auto-remove delisted/invalid coins every 24h

        # Weekly auto-cleanup (cost control) — only runs once every 7 days
        global _last_cleanup_check
        if time.time() - _last_cleanup_check > 7 * 86400:
            try:
                auto_cleanup_poor_performers()
            except Exception as e:
                print(f"Auto-cleanup error: {e}")
            _last_cleanup_check = time.time()

        # Periodic persistence — signal_performance, prepump_phases,
        # trendline_retest_tracking, and the cooldown trackers were previously pure
        # in-memory and lost on every restart. Saving once per pass here (not on
        # every single mutation) keeps disk I/O reasonable while still persisting
        # within ~60s of any change.
        save_signal_performance()
        save_prepump_phases()
        save_trendline_tracking()
        save_cooldown_trackers()
        save_retest_watch()

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
                from_id = str(msg.get("from", {}).get("id", ""))
                first_name = msg.get("chat", {}).get("first_name", "Friend")
                thread_id = msg.get("message_thread_id")
                is_admin = (
                    chat_id == ADMIN_CHAT_ID or
                    from_id == ADMIN_CHAT_ID
                )

                # Commands from group topics: only admin can use them.
                if thread_id and not is_admin:
                    continue

                # reply_chat_id / reply_thread_id: where to send the response.
                # If from group topic → reply to ADMIN personal DM (simpler, avoids group clutter)
                # If from personal DM → reply there
                if thread_id:
                    reply_chat_id   = ADMIN_CHAT_ID
                    reply_thread_id = None
                else:
                    reply_chat_id   = chat_id
                    reply_thread_id = None

                # Helper: send response back to where command came from
                def reply(msg_text):
                    send_to(reply_chat_id, msg_text, thread_id=reply_thread_id)

                if raw_text.startswith("WATCHLIST_SAVE:"):
                    continue
                if not text:
                    continue

                # ── Bulletproof shortcuts — processed BEFORE any chain ──
                _shortcut_map = {
                    "/E": "/ENTRY", "/W": "/WATCH", "/Z": "/ZONES",
                    "/ML": "/MYLINES", "/MYLINE": "/MYLINES",
                    "/U": "/UNWATCH",
                }
                if text in _shortcut_map:
                    text = _shortcut_map[text]
                elif text.startswith("/E ") and not text.startswith("/ENTRY"):
                    raw_text = "/entry " + raw_text[3:]; text = raw_text.upper()
                elif text.startswith("/W ") and not text.startswith("/WATCH"):
                    raw_text = "/watch " + raw_text[3:]; text = raw_text.upper()
                elif text.startswith("/S ") and not text.startswith("/SUGGEST") and not text.startswith("/STATUS"):
                    raw_text = "/suggest " + raw_text[3:]; text = raw_text.upper()
                elif text.startswith("/U ") and not text.startswith("/UNWATCH"):
                    raw_text = "/unwatch " + raw_text[3:]; text = raw_text.upper()

                # ── Bulletproof direct handlers (respond immediately, skip chain) ──
                if text == "/MYLINES":
                    mine = {k: v for k, v in manual_lines.items()
                            if str(v.get("chat_id","")) in (str(chat_id), str(ADMIN_CHAT_ID))}
                    if not mine:
                        reply(f"📏 No active lines. Use /addline SYMBOL PRICE 1h to add one.")
                    else:
                        rows = []
                        for lid, v in mine.items():
                            try:
                                rows.append(
                                    f"• <code>{lid}</code> {v['symbol']} {v.get('tf','').upper()} "
                                    f"@ {format_price(v['price'])} "
                                    f"({'⏳' if v.get('state')=='waiting' else '📈' if v.get('state')=='broken' else '🔎'})"
                                )
                            except Exception as e:
                                print(f"⚠️ Skipping malformed line {lid}: {e}")
                        send_chunked(reply_chat_id, rows, header=f"📏 <b>Lines ({len(rows)}):</b>\n\n")
                    continue

                if text == "/ZONES":
                    if not manual_zones:
                        reply("📐 No active zones. Use /addzone SYMBOL LOW HIGH 4h to add one.")
                    else:
                        rows = []
                        for zid, z in manual_zones.items():
                            try:
                                rows.append(
                                    f"• <code>{zid}</code> {z['symbol']} {z.get('tf','4h').upper()} "
                                    f"{format_price(z['low'])}–{format_price(z['high'])} "
                                    f"({'⏳' if z.get('state')=='waiting' else '✅'})"
                                )
                            except Exception as e:
                                print(f"⚠️ Skipping malformed zone {zid}: {e}")
                        send_chunked(reply_chat_id, rows, header=f"📐 <b>Zones ({len(rows)}):</b>\n\n")
                    continue

                # ── Main command chain ──
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
                            f"🔍 <b>Check a coin yourself:</b>\n"
                            f"Type /entry BTC (or any coin) anytime for a real-time technical\n"
                            f"snapshot — trend, structure, volume, and risk flags — before you decide.\n\n"
                            f"👁 <b>Watch a retest:</b>\n"
                            f"If /entry shows \"Retest in progress\", you can type /watch BTC to get\n"
                            f"a personal alert (and a Top Picks post) the moment it completes.\n"
                            f"Use /unwatch BTC to stop, or /mywatches to see your list.\n\n"
                            f"📏 <b>Mark your own level:</b>\n"
                            f"Drawn a resistance/support line yourself? /addline BTC 95000 1h\n"
                            f"tracks it for a strong-body break, then alerts again when the\n"
                            f"retest confirms. Use /removeline or /mylines to manage.\n\n"
                            f"⚠️ <b>Keep in mind:</b>\n"
                            f"This is a volume alert, not a trading signal.\n"
                            f"When a notification comes in, analyze the chart yourself,\n"
                            f"and only take the entry once you've confirmed it.\n\n"
                            f"Good luck! 🚀\n— CryptoPing"
                        )
                        reply( welcome_msg)
                        send_to(ADMIN_CHAT_ID, f"👤 New subscriber: <b>{first_name}</b> (ID: {chat_id})")
                        save_subscribers()
                    else:
                        reply( "✅ You're already subscribed!")

                elif text == "/STOP":
                    if chat_id in subscribers and chat_id != ADMIN_CHAT_ID:
                        subscribers.remove(chat_id)
                        save_subscribers()
                        reply( "❌ Unsubscribed.")

                elif text == "/LIST":
                    coin_lines = [f"• {c}" for c in watchlist]
                    send_chunked(chat_id, coin_lines, header=f"📋 <b>Watchlist ({len(watchlist)} coins):</b>\n\n")

                # ── Command shortcuts — normalize before command handling ──
                if text.startswith("/E ") and not text.startswith("/ENTRY"):
                    raw_text = "/entry " + raw_text[3:]
                    text = raw_text.upper()
                elif text == "/E":
                    text = "/ENTRY"
                elif text.startswith("/W ") and not text.startswith("/WATCH"):
                    raw_text = "/watch " + raw_text[3:]
                    text = raw_text.upper()
                elif text == "/W":
                    text = "/WATCH"
                elif text.startswith("/S ") and not text.startswith("/SUGGEST") and not text.startswith("/STATUS") and not text.startswith("/SUBSCRIBE"):
                    raw_text = "/suggest " + raw_text[3:]
                    text = raw_text.upper()
                elif text.startswith("/U ") and not text.startswith("/UNWATCH"):
                    raw_text = "/unwatch " + raw_text[3:]
                    text = raw_text.upper()
                elif text == "/Z":
                    text = "/ZONES"
                elif text == "/LIST":
                    coin_lines = [f"• {c}" for c in watchlist]
                    send_chunked(chat_id, coin_lines, header=f"📋 <b>Watchlist ({len(watchlist)} coins):</b>\n\n")

                elif text == "/MYLINES":
                    all_lines = {k: v for k, v in manual_lines.items()}
                    mine = {k: v for k, v in all_lines.items()
                            if str(v.get("chat_id","")) == str(chat_id) or
                               str(v.get("chat_id","")) == str(ADMIN_CHAT_ID)}
                    if not mine:
                        reply(f"📏 No active lines. ({len(all_lines)} total in system)\nUse /addline SYMBOL PRICE 1h to add one.")
                    else:
                        lines_out = []
                        for lid, ln in mine.items():
                            state_label = {"waiting": "⏳", "broken": "📈", "followup": "🔎"}.get(ln.get("state",""), ln.get("state",""))
                            lines_out.append(f"• <code>{lid}</code> {ln['symbol']} {ln.get('tf','').upper()} @ {format_price(ln['price'])} {state_label}")
                        reply(f"📏 <b>Your lines ({len(mine)}):</b>\n\n" + "\n".join(lines_out))

                elif text == "/ZONES":
                    if not manual_zones:
                        reply(f"📐 No active zones. Use /addzone SYMBOL LOW HIGH 4h to add one.")
                    else:
                        lines_out = []
                        for zid, z in manual_zones.items():
                            state = z.get("state", "waiting")
                            state_emoji = "⏳" if state == "waiting" else "✅" if state == "confirmed" else "❌"
                            lines_out.append(
                                f"• <code>{zid}</code> {z['symbol']} {z.get('tf','4h').upper()} "
                                f"{format_price(z['low'])}–{format_price(z['high'])} {state_emoji}"
                            )
                        reply(f"📐 <b>Active zones ({len(manual_zones)}):</b>\n\n" + "\n".join(lines_out))

                elif text.startswith("/ENTRY "):
                    sym_raw = text.replace("/ENTRY ", "").strip().split()[0] if text.replace("/ENTRY ", "").strip() else ""
                    if not sym_raw:
                        reply( "⚠️ Format: /entry BTC  (or /entry BTCUSDT)")
                    elif not is_plausible_symbol(sym_raw):
                        reply(f"⚠️ '{sym_raw}' doesn't look like a valid symbol. Use a ticker like BTC or BTCUSDT, not a price.")
                    else:
                        sym = sym_raw if sym_raw.endswith("USDT") else sym_raw + "USDT"
                        result = calc_entry_score(sym)
                        if result is None:
                            reply( f"⚠️ Couldn't fetch enough data for {sym}. Check the symbol and try again.")
                        else:
                            details_str = "\n".join(result["details"])
                            pattern_notes = result.get("pattern_notes", {})
                            tf_order = ["15m", "30m", "1h", "4h"]
                            confirmed_tfs = [tf for tf in tf_order
                                             if tf in pattern_notes and "confirmed" in pattern_notes[tf]]
                            inprogress_tfs = [tf for tf in tf_order
                                              if tf in pattern_notes and "in progress" in pattern_notes[tf]]
                            retest_tfs = [tf for tf in tf_order
                                          if tf in pattern_notes and
                                          ("in progress" in pattern_notes[tf] or "confirmed" in pattern_notes[tf])]
                            # FIX (DOGS/MUBARAK case): when lower TF confirms but higher
                            # TF is still in progress, output looked contradictory. Add a
                            # clear summary so the situation is immediately understandable.
                            mixed_signal_note = ""
                            if confirmed_tfs and inprogress_tfs:
                                lower_confirmed = [tf for tf in confirmed_tfs if tf in ["15m", "30m"]]
                                higher_pending = [tf for tf in inprogress_tfs if tf in ["1h", "4h"]]
                                if lower_confirmed and higher_pending:
                                    mixed_signal_note = (
                                        f"⚠️ <b>Mixed timeframe signals</b> — "
                                        f"{'/'.join(t.upper() for t in lower_confirmed)} confirmed "
                                        f"but {'/'.join(t.upper() for t in higher_pending)} still in progress. "
                                        f"Wait for the higher TF to also close a strong green candle "
                                        f"before treating this as fully aligned.\n\n"
                                    )
                            lines = []
                            if mixed_signal_note:
                                lines.append(mixed_signal_note)
                            if len(retest_tfs) >= 2:
                                tf_label = " and ".join(t.upper() for t in retest_tfs)
                                lines.append(f"🎯 <b>Confluence — {tf_label} all show a retest setup:</b>\n")
                            for tf in tf_order:
                                if tf in pattern_notes:
                                    lines.append(f"📐 <b>Pattern Context ({tf.upper()}):</b>\n{pattern_notes[tf]}")
                                else:
                                    # FIX (after the AVNT case): timeframes with no detected
                                    # pattern were silently omitted entirely, which looked
                                    # identical to "this timeframe wasn't checked" — now every
                                    # timeframe always gets a line, even when it's just "nothing
                                    # notable here", so silence never gets mistaken for a gap.
                                    lines.append(f"📐 <b>Pattern Context ({tf.upper()}):</b>\nNo clear breakout/retest setup detected on this timeframe right now.")
                            pattern_section = "\n" + "\n\n".join(lines) + "\n"
                            chart_patterns = result.get("chart_patterns", [])
                            ob_data = get_order_book_clusters(sym)

                            # Build powerful entry message
                            entry_msg, entry_meta = build_powerful_entry(sym, result, ob_data)

                            # Extract confirmed TFs from pattern_notes for watch logic
                            pattern_notes = result.get("pattern_notes", {})
                            confirmed_tfs = [tf for tf in ["15m","30m","1h","4h"]
                                           if tf in pattern_notes and "confirmed" in pattern_notes[tf]]

                            try:
                                reply(entry_msg)
                                entry_sent = True
                            except Exception as e:
                                print(f"Entry reply error: {e}")
                                # Fallback — short version
                                reply(
                                    f"📊 {sym} | {format_price(result['price'])} | {result['label']}\n"
                                    f"⚠️ Confirm on chart before entry."
                                )
                                entry_sent = True

                            # Auto-start liquidation watch only if entry message sent
                            if entry_sent:
                                start_liq_watch(sym, ob_data, result["price"], reply_chat_id,
                                                 eql_price=entry_meta.get("eql_price"),
                                                 eql_touches=entry_meta.get("eql_touches"))
                                # Note: liq sweep alerts go via entry watch (My Setups)
                                # No separate "Liquidation watch started" message needed

                                # Auto-start entry watch for weak setups
                                details_str_lower = details_str.lower()
                                weak_reasons = []
                                if "daily downtrend" in details_str_lower or "daily trend bearish" in details_str_lower:
                                    weak_reasons.append("daily_down")
                                if "low volume" in details_str_lower or "normal/low" in details_str_lower:
                                    weak_reasons.append("low_volume")
                                if not confirmed_tfs:
                                    weak_reasons.append("no_retest")

                                if weak_reasons:
                                    start_entry_watch(sym, reply_chat_id, result["price"], weak_reasons, entry_msg)
                                    reply(
                                        f"👁 <b>Setup watch started — {sym}</b>\n"
                                        f"Monitoring for: volume spike, retest confirm"
                                        + (", daily trend shift" if "daily_down" in weak_reasons else "") +
                                        f"\n— You'll get an alert in My Setups when setup improves."
                                    )

                elif text.startswith("/WATCH "):
                    sym_raw = text.replace("/WATCH ", "").strip().split()[0] if text.replace("/WATCH ", "").strip() else ""
                    if not sym_raw:
                        reply( "⚠️ Format: /watch BTC  (or /watch BTCUSDT)")
                    elif not is_plausible_symbol(sym_raw):
                        reply(f"⚠️ '{sym_raw}' doesn't look like a valid symbol. Use a ticker like BTC or BTCUSDT, not a price.")
                    else:
                        sym = sym_raw if sym_raw.endswith("USDT") else sym_raw + "USDT"
                        watch_key = f"{sym}_{chat_id}"
                        if watch_key in retest_watch_list:
                            reply( f"👁 Already watching {sym} for a retest completion.")
                        else:
                            ticker_check = get_ticker(sym)
                            klines_by_tf = {
                                tf: get_klines(sym, interval=tf, limit=50)
                                for tf in ["15m", "30m", "1h", "4h"]
                            }
                            if not any(klines_by_tf.values()) or not ticker_check:
                                reply( f"⚠️ Couldn't fetch data for {sym}. Check the symbol and try again.")
                            else:
                                current_price_check = float(ticker_check["lastPrice"])
                                tfs_in_progress = []
                                for tf, klines_tf in klines_by_tf.items():
                                    if not klines_tf:
                                        continue
                                    pattern_note = detect_break_retest_pattern(klines_tf, current_price_check)
                                    if pattern_note and ("Retest in progress" in pattern_note or "Retest confirmed" in pattern_note):
                                        tfs_in_progress.append(tf)

                                if not tfs_in_progress:
                                    reply(
                                        f"⚠️ {sym} doesn't currently show an active retest on 15m/30m/1H/4H.\n\n"
                                        f"If /entry showed \"Retest in progress\" a moment ago, the candle may "
                                        f"have shifted since — try /entry again to get the latest state, then "
                                        f"/watch immediately after."
                                    )
                                else:
                                    retest_watch_list[watch_key] = {
                                        "symbol": sym, "chat_id": chat_id,
                                        "requested_time": time.time(), "name": first_name,
                                        "timeframes": tfs_in_progress,
                                    }
                                    save_retest_watch()
                                    tf_label = " and ".join(t.upper() for t in tfs_in_progress)
                                    reply(
                                        f"👁 <b>Watching {sym}</b> for retest completion ({tf_label}).\n\n"
                                        f"You'll get a personal alert here (and it'll also post to Top Picks) "
                                        f"once a strong green candle closes back above the broken level.\n\n"
                                        f"Use /unwatch {sym_raw} to stop."
                                    )

                elif text.startswith("/UNWATCH "):
                    sym_raw = text.replace("/UNWATCH ", "").strip().split()[0] if text.replace("/UNWATCH ", "").strip() else ""
                    sym = sym_raw if sym_raw.endswith("USDT") else sym_raw + "USDT"
                    watch_key = f"{sym}_{chat_id}"
                    if watch_key in retest_watch_list:
                        retest_watch_list.pop(watch_key, None)
                        save_retest_watch()
                        reply( f"👁 Stopped watching {sym}.")
                    else:
                        reply( f"⚠️ You weren't watching {sym}.")

                elif text == "/MYWATCHES":
                    mine = [v for v in retest_watch_list.values() if v["chat_id"] == chat_id]
                    if not mine:
                        reply( "👁 You're not watching any coins right now. Use /watch SYMBOL after /entry shows a retest in progress.")
                    else:
                        stage_label = {"watching": "⏳ watching for retest", "followup": "🔎 confirmed, tracking continuation"}
                        lines = [f"• {w['symbol']} ({stage_label.get(w.get('stage', 'watching'), 'watching')})" for w in mine]
                        reply( "👁 <b>Your watches:</b>\n\n" + "\n".join(lines))

                elif text.startswith("/ADD ") and is_admin:
                    symbol = text.replace("/ADD ", "").strip()
                    if not symbol.endswith("USDT"):
                        symbol += "USDT"
                    if symbol in watchlist:
                        reply( f"⚠️ {symbol} is already on the list!")
                    else:
                        r = http_session.get(f"https://api.binance.com/api/v3/ticker/price?symbol={symbol}", timeout=5)
                        if r.status_code == 200:
                            watchlist.append(symbol)
                            removed_coins.discard(symbol)  # re-adding overrides a past /remove
                            save_removed_coins()
                            save_watchlist()
                            reply( f"✅ {symbol} added! Total: {len(watchlist)}")
                        else:
                            reply( f"❌ {symbol} not found on Binance.")

                elif text.startswith("/REMOVE ") and is_admin:
                    symbol = text.replace("/REMOVE ", "").strip()
                    if not symbol.endswith("USDT"):
                        symbol += "USDT"
                    if symbol in watchlist:
                        watchlist.remove(symbol)
                        # FIX (ELFUSDT case): previously this only removed the coin from
                        # the in-memory list — DEFAULT_WATCHLIST coins always came back
                        # on the next restart/redeploy since load_watchlist_file() rebuilt
                        # from DEFAULT_WATCHLIST.copy() every time. Now any removal is
                        # tracked permanently in removed_coins, which load_watchlist_file()
                        # checks on every load — the removal sticks even across redeploys,
                        # for default coins too.
                        removed_coins.add(symbol)
                        save_removed_coins()
                        save_watchlist()
                        reply( f"🗑 {symbol} removed permanently. Total: {len(watchlist)}")
                    else:
                        reply( f"⚠️ {symbol} isn't on the watchlist.")

                elif text == "/STATUS" and is_admin:
                    reply(
                        f"✅ <b>CryptoPing {BOT_VERSION} is running!</b>\n\n"
                        f"📋 Coins: {len(watchlist)}\n"
                        f"👥 Subscribers: {len(subscribers)}\n"
                        f"🔍 Momentum: {len(momentum_tracking)}\n"
                        f"⚡ Pending 5M→15M: {len(spike_pending_confirm)}\n"
                        f"🎯 OB/FVG: {len(ob_fvg_zone_tracking)}\n"
                        f"📐 Manual Zones: {len(manual_zones)}\n"
                        f"📏 Manual Lines: {len(manual_lines)}\n"
                        f"🏆 TL Retest: {len(trendline_retest_tracking)}\n"
                        f"🕐 {datetime.now().strftime('%H:%M:%S')}"
                    )

                elif text == "/CLEANUP" and is_admin:
                    extra_count_before = len([c for c in watchlist if c not in DEFAULT_WATCHLIST])
                    if extra_count_before == 0:
                        reply( "🧹 No extra coins to evaluate — watchlist is just the defaults.")
                    else:
                        reply( f"🧹 Running cleanup check on {extra_count_before} extra coins...")
                        auto_cleanup_poor_performers()
                        reply( f"✅ Done. Total watchlist now: {len(watchlist)} coins.")

                elif (text == "/REPORT" or raw_text.upper().startswith("/REPORT ")) and is_admin:
                    arg = raw_text.strip().split(None, 1)
                    window_str = arg[1].strip().lower() if len(arg) > 1 else "24h"

                    # /report performance -> signal type breakdown
                    if window_str == "performance":
                        now_r = time.time()
                        cutoff = now_r - 30 * 86400  # last 30 days
                        from collections import defaultdict
                        type_stats = defaultdict(lambda: {"count": 0, "gains": [], "wins": 0})
                        for pk, data in signal_performance.items():
                            if data.get("signal_time", 0) < cutoff:
                                continue
                            sig_type = data.get("signal_type", "Unknown")
                            sig_price = data.get("signal_price", 0)
                            highest = data.get("highest_after", sig_price)
                            if sig_price > 0:
                                pct = (highest - sig_price) / sig_price * 100
                                type_stats[sig_type]["count"] += 1
                                type_stats[sig_type]["gains"].append(pct)
                                if pct >= 10:
                                    type_stats[sig_type]["wins"] += 1
                        if not type_stats:
                            reply( "📊 No signal performance data yet (need at least some results).")
                        else:
                            sorted_types = sorted(
                                type_stats.items(),
                                key=lambda x: (sum(x[1]["gains"]) / len(x[1]["gains"])) if x[1]["gains"] else 0,
                                reverse=True
                            )
                            lines_p = ["📊 <b>Signal Performance (last 30 days)</b>\n"]
                            for sig_type, stats in sorted_types:
                                count = stats["count"]
                                gains = stats["gains"]
                                avg_gain = sum(gains) / len(gains) if gains else 0
                                win_rate = stats["wins"] / count * 100 if count else 0
                                emoji = "🏆" if avg_gain >= 20 else "✅" if avg_gain >= 10 else "🟡" if avg_gain >= 5 else "⚠️"
                                lines_p.append(
                                    f"{emoji} <b>{sig_type}</b>\n"
                                    f"   Signals: {count} | Avg gain: +{avg_gain:.1f}% | Win rate (>10%): {win_rate:.0f}%"
                                )
                            reply( "\n\n".join(lines_p))
                    else:
                        import re as _re
                        m = _re.match(r"^(\d+)([hd])$", window_str)
                        if not m:
                            reply( "⚠️ Format: /report 24h  or  /report 7d  or  /report performance")
                        else:
                            amount, unit = int(m.group(1)), m.group(2)
                            window_seconds = amount * 3600 if unit == "h" else amount * 86400
                            window_label = f"Last {amount}{'hr' if unit == 'h' else ' day(s)'}"
                            reply( build_report(window_seconds, window_label))

                elif raw_text.upper().startswith("/BROADCAST ") and is_admin:
                    broadcast_text = raw_text[11:].strip()
                    if broadcast_text:
                        for chat_id_sub in subscribers:
                            send_to(chat_id_sub, f"📢 <b>Message from CryptoPing:</b>\n\n{broadcast_text}")
                        send_to(ADMIN_CHAT_ID, f"✅ Sent to {len(subscribers)} people!")
                    else:
                        reply( "⚠️ Example: /broadcast stay alert today")

                elif text == "/SUBSCRIBERS" and is_admin:
                    if not subscribers:
                        reply( "👥 No subscribers yet.")
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
                            reply( f"⚠️ ID {target_id} isn't on the subscriber list.")
                    else:
                        reply( "Format: /msg [ID] [message]")

                elif text == "/HELP" and is_admin:
                    reply(
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
                        "<b>📏 Manual Line Commands (public, anyone can use):</b>\n"
                        "/addline BTC 95000 4h — watch a single level for break+retest\n"
                        "/removeline BTC_4h_1 — stop watching a line\n"
                        "/mylines — view your active lines\n\n"
                        "<b>📊 Market Scan:</b>\n"
                        "/scanmarket — view USDT coins with 500K+ volume\n"
                        "/scanmarket 1000000 — custom volume threshold\n"
                        "/addall — add all coins from the last scan at once\n"
                    )

                elif text == "/EXPORTZONES" and is_admin:
                    import json as _j
                    if not manual_zones:
                        reply( "📐 No active zones.")
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
                            reply( f"<code>{text_out}</code>")
                        else:
                            parts = [text_out[i:i+chunk_size] for i in range(0, len(text_out), chunk_size)]
                            for i, part in enumerate(parts):
                                reply( f"Part {i+1}/{len(parts)}:\n<code>{part}</code>")
                        reply( f"✅ {len(export)} zones. Run /sync on the signal bot.")

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
                    reply( f"📤 Watchlist exported ({len(watchlist)} coins)!")


                    # Parse optional volume threshold
                    parts = text.split()
                    min_vol = 500_000
                    if len(parts) == 2:
                        try:
                            min_vol = float(parts[1])
                        except:
                            pass

                    reply( f"🔍 Scanning Binance (min ${min_vol:,.0f} volume)...")

                    try:
                        r = http_session.get(
                            "https://api.binance.com/api/v3/ticker/24hr",
                            timeout=15
                        )
                        if r.status_code != 200:
                            reply( "❌ Binance API error")
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
                                reply( "\n".join(lines))
                            else:
                                reply( "🚀 No gainers found.")

                            # Top 20 losers
                            if losers:
                                lines = [f"\n📉 <b>Top Losers (not in watchlist)</b>\n"]
                                for sym, chg, vol, price in losers[:20]:
                                    vol_str = f"${vol/1e6:.1f}M" if vol >= 1e6 else f"${vol/1e3:.0f}K"
                                    lines.append(f"• <b>{sym}</b> {chg:.1f}% | {vol_str}")
                                reply( "\n".join(lines))

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
                                        reply( "\n".join(chunk))
                                        chunk = []
                                if chunk:
                                    reply( "\n".join(chunk))

                    except Exception as e:
                        reply( f"❌ Scan error: {e}")

                elif text == "/ADDALL" and is_admin:
                    if not last_scan_results:
                        reply( "⚠️ Run /scanmarket first, then /addall")
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
                        reply( msg)

                elif raw_text.upper().startswith("/ADDZONE "):
                    if not is_admin:
                        reply( "⚠️ Admin only command.")
                    else:
                        parts = raw_text.strip().split()
                        if len(parts) == 5:
                            _, sym, low_s, high_s, ztf = parts
                            sym = sym.upper()
                            low_s  = low_s.replace("$", "").replace(",", "")
                            high_s = high_s.replace("$", "").replace(",", "")
                            if not sym.endswith("USDT"):
                                sym += "USDT"
                            ztf = ztf.lower()
                            if ztf not in ["5m","15m","1h","4h","1d"]:
                                reply( "⚠️ TF must be: 5m / 15m / 1h / 4h / 1d")
                            else:
                                try:
                                    z_low  = float(low_s)
                                    z_high = float(high_s)
                                    if z_low >= z_high:
                                        reply( "⚠️ Low must be less than High")
                                    else:
                                        # Duplicate/overlap check — same symbol+tf zone that
                                        # already covers this range (or is very close to it)
                                        # gets rejected instead of creating a near-identical copy.
                                        dup_id = None
                                        for eid, ez in manual_zones.items():
                                            try:
                                                if ez["symbol"] != sym or ez.get("tf", "4h") != ztf:
                                                    continue
                                                e_low, e_high = ez["low"], ez["high"]
                                                overlaps = z_low <= e_high and z_high >= e_low
                                                nearly_same = (
                                                    abs(z_low - e_low) <= e_low * 0.005
                                                    and abs(z_high - e_high) <= e_high * 0.005
                                                )
                                                if overlaps or nearly_same:
                                                    dup_id = eid
                                                    break
                                            except Exception:
                                                continue
                                        if dup_id:
                                            ez = manual_zones[dup_id]
                                            reply(
                                                f"⚠️ <b>Duplicate/overlapping zone</b>\n\n"
                                                f"🪙 {sym} {ztf.upper()} already has a zone here:\n"
                                                f"🔲 {format_price(ez['low'])} — {format_price(ez['high'])}\n"
                                                f"🆔 ID: <code>{dup_id}</code>\n\n"
                                                f"Use /removezone {dup_id} first if you want to replace it."
                                            )
                                            continue
                                        zone_count = sum(1 for k in manual_zones if k.startswith(f"{sym}_{ztf}"))
                                        zone_id = f"{sym}_{ztf}_{zone_count+1}"
                                        manual_zones[zone_id] = {
                                            "symbol": sym, "tf": ztf,
                                            "low": z_low, "high": z_high,
                                            "added_time": time.time(),
                                            "state": "waiting",
                                            "alert_sent_time": 0,
                                            "source": "manual",
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

                                        reply(
                                            f"✅ <b>Zone added!</b>\n\n"
                                            f"🪙 {sym} | {ztf.upper()} OB\n"
                                            f"🔲 {format_price(z_low)} — {format_price(z_high)}\n"
                                            f"🆔 ID: <code>{zone_id}</code>\n\n"
                                            f"{extra_str}"
                                            f"Bot is monitoring. You'll be notified when price reaches the zone."
                                        )
                                        save_zones()
                                        print(f"📐 Zone added: {zone_id}")

                                        # Suggest nearby zones above and below
                                        try:
                                            ticker_s = get_ticker(sym)
                                            klines_s = get_klines(sym, interval="4h", limit=80)
                                            klines_1d_s = get_klines(sym, interval="1d", limit=30)
                                            if ticker_s and klines_s:
                                                cp = float(ticker_s["lastPrice"])
                                                closed_s = klines_s[:-1]
                                                all_highs, all_lows = [], []
                                                for k in closed_s[-50:]:
                                                    all_highs.append(float(k[2]))
                                                    all_lows.append(float(k[3]))
                                                if klines_1d_s:
                                                    for k in klines_1d_s[-20:]:
                                                        all_highs.append(float(k[2]))
                                                        all_lows.append(float(k[3]))

                                                # Zones above current price
                                                above = sorted(set(
                                                    round(h, 8) for h in all_highs
                                                    if h > z_high * 1.01
                                                ))[:3]
                                                # Zones below current price
                                                below = sorted(set(
                                                    round(l, 8) for l in all_lows
                                                    if l < z_low * 0.99
                                                ), reverse=True)[:2]

                                                suggest_parts = ["📏 <b>Next zones to add:</b>"]
                                                seen_p = set()
                                                for h in above:
                                                    bucket = round(h / (cp * 0.025))
                                                    if bucket not in seen_p:
                                                        seen_p.add(bucket)
                                                        m = h * 0.012
                                                        pct = (h - cp) / cp * 100
                                                        suggest_parts.append(
                                                            f"🔴 <code>/addzone {sym.replace('USDT','')} {format_price(h-m)} {format_price(h+m)} 4h</code> (+{pct:.1f}% — resistance/TP)"
                                                        )
                                                for l in below:
                                                    bucket = round(l / (cp * 0.025))
                                                    if bucket not in seen_p:
                                                        seen_p.add(bucket)
                                                        m = l * 0.012
                                                        pct = (cp - l) / cp * 100
                                                        suggest_parts.append(
                                                            f"🟢 <code>/addzone {sym.replace('USDT','')} {format_price(l-m)} {format_price(l+m)} 4h</code> (-{pct:.1f}% — support/SL area)"
                                                        )
                                                if len(suggest_parts) > 1:
                                                    reply("\n".join(suggest_parts))
                                        except Exception as se:
                                            print(f"Zone suggest error: {se}")
                                except ValueError:
                                    reply( "⚠️ Format: /addzone RIF 0.0665 0.0703 4H")
                        else:
                            reply( f"⚠️ Format: /addzone RIF 0.0665 0.0703 4H\nParts received: {len(parts)}")

                elif raw_text.upper().startswith("/REMOVEZONE "):
                    if not is_admin:
                        reply( "⚠️ Admin only.")
                    else:
                        zone_id = raw_text.strip().split(None, 1)[1].strip()
                        if zone_id in manual_zones:
                            manual_zones.pop(zone_id)
                            save_zones()
                            reply( f"🗑 Zone removed: <code>{zone_id}</code>")
                        else:
                            reply( f"⚠️ Zone not found: {zone_id}\nUse /zones to see the list")

                elif raw_text.upper().startswith("/RESETZONE "):
                    if not is_admin:
                        reply( "⚠️ Admin only.")
                    else:
                        zone_id = raw_text.strip().split(None, 1)[1].strip()
                        if zone_id in manual_zones:
                            manual_zones[zone_id]["state"] = "waiting"
                            manual_zones[zone_id]["invalidated"] = False
                            manual_zones[zone_id]["layer1_sent"] = False
                            manual_zones[zone_id]["layer2_sent"] = False
                            manual_zones[zone_id]["entered_notified_time"] = 0
                            manual_zones[zone_id]["fast_spike_alerted"] = False
                            save_zones()
                            reply( f"♻️ Zone reset: <code>{zone_id}</code>\nMonitoring has restarted.")
                        else:
                            reply( f"⚠️ Zone not found: {zone_id}")

                elif raw_text.upper().startswith("/ADDLINE "):
                    parts = raw_text.strip().split()
                    if len(parts) != 4:
                        reply( "⚠️ Format: /addline RIF 0.0703 4h\n(timeframe must be 1h, 4h or 1d)")
                    else:
                        _, sym, price_s, ltf = parts
                        sym = sym.upper()
                        price_s = price_s.replace("$", "").replace(",", "")  # strip $ and commas
                        if not sym.endswith("USDT"):
                            sym += "USDT"
                        ltf = ltf.lower()  # normalize: 1D→1d, 4H→4h, 1H→1h
                        if ltf not in ["1h", "4h", "1d"]:
                            reply( "⚠️ Timeframe must be 1h, 4h or 1d for /addline")
                        else:
                            try:
                                level_price = float(price_s)
                                if level_price <= 0:
                                    reply( "⚠️ Price must be greater than 0")
                                else:
                                    line_count = sum(1 for k in manual_lines if k.startswith(f"{sym}_{ltf}"))
                                    line_id = f"{sym}_{ltf}_{line_count+1}"
                                    manual_lines[line_id] = {
                                        "symbol": sym, "tf": ltf,
                                        "price": level_price,
                                        "added_time": time.time(),
                                        "state": "waiting",
                                        "chat_id": chat_id,
                                        "name": first_name,
                                    }
                                    save_manual_lines()
                                    reply(
                                        f"📏 <b>Line added!</b>\n\n"
                                        f"🪙 {sym} | {ltf.upper()}\n"
                                        f"📍 Level: {format_price(level_price)}\n"
                                        f"🆔 ID: <code>{line_id}</code>\n\n"
                                        f"Watching for a strong-body break above this level, "
                                        f"then a confirmed retest. You'll get a personal alert "
                                        f"at each stage (it'll also post to My Setups on retest "
                                        f"confirmation).\n\n"
                                        f"Use /removeline {line_id} to stop."
                                    )
                                    print(f"📏 Line added: {line_id}")
                            except ValueError:
                                reply( "⚠️ Format: /addline RIF 0.0703 1h")

                elif raw_text.upper().startswith("/REMOVELINE "):
                    line_id = raw_text.strip().split(None, 1)[1].strip()
                    if line_id in manual_lines:
                        manual_lines.pop(line_id)
                        save_manual_lines()
                        reply( f"🗑 Line removed: <code>{line_id}</code>")
                    else:
                        reply( f"⚠️ Line not found: {line_id}\nUse /mylines to see your lines")

                elif text == "/MYLINES":
                    mine = {k: v for k, v in manual_lines.items() if v.get("chat_id") == chat_id}
                    if not mine:
                        reply( "📏 You have no active lines. Use /addline SYMBOL PRICE 1h to add one.")
                    else:
                        lines_out = []
                        for lid, ln in mine.items():
                            state_label = {"waiting": "⏳ waiting for break", "broken": "📈 broken, watching retest", "followup": "🔎 confirmed, tracking continuation"}.get(ln.get("state"), ln.get("state"))
                            lines_out.append(f"• <code>{lid}</code> — {ln['symbol']} {ln['tf'].upper()} @ {format_price(ln['price'])} ({state_label})")
                        reply( "📏 <b>Your lines:</b>\n\n" + "\n".join(lines_out))

                elif text.startswith("/SUGGEST ") or text == "/SUGGEST":
                    sym_raw = text.replace("/SUGGEST ", "").strip().split()[0] if " " in text else ""
                    if not sym_raw:
                        reply("⚠️ Format: /suggest BTC  (or /s BTC)")
                    elif not is_plausible_symbol(sym_raw):
                        reply(f"⚠️ '{sym_raw}' doesn't look like a valid symbol. Use a ticker like BTC or BTCUSDT, not a price.")
                    else:
                        sym = sym_raw if sym_raw.endswith("USDT") else sym_raw + "USDT"
                        ticker = get_ticker(sym)
                        klines_4h = get_klines(sym, interval="4h", limit=100)
                        klines_1d = get_klines(sym, interval="1d", limit=30)
                        if not ticker or not klines_4h:
                            reply(f"⚠️ Couldn't fetch data for {sym}")
                        else:
                            current_price = float(ticker["lastPrice"])
                            closed_4h = klines_4h[:-1]
                            highs = [float(k[2]) for k in closed_4h[-50:]]
                            lows  = [float(k[3]) for k in closed_4h[-50:]]
                            vols  = [float(k[5]) for k in closed_4h[-50:]]
                            avg_vol = sum(vols[-20:]) / 20 if len(vols) >= 20 else 1

                            # Also include 1D klines for major levels
                            if klines_1d and len(klines_1d) >= 5:
                                closed_1d = klines_1d[:-1]
                                avg_vol_1d = sum(float(k[5]) for k in closed_1d[-10:]) / 10 or 1
                                for k in closed_1d[-30:]:
                                    highs.append(float(k[2]))
                                    lows.append(float(k[3]))
                                    vols.append(float(k[5]))

                            # Find swing highs/lows for zone suggestions
                            swing_highs, swing_lows = [], []
                            for i in range(3, len(closed_4h) - 3):
                                h = float(closed_4h[i][2])
                                l = float(closed_4h[i][3])
                                v = float(closed_4h[i][5])
                                if all(h >= float(closed_4h[i+j][2]) for j in [-3,-2,-1,1,2,3]):
                                    swing_highs.append((h, v/avg_vol))
                                if all(l <= float(closed_4h[i+j][3]) for j in [-3,-2,-1,1,2,3]):
                                    swing_lows.append((l, v/avg_vol))
                            # Add 1D swing highs/lows
                            if klines_1d and len(klines_1d) >= 5:
                                for i in range(2, len(closed_1d) - 2):
                                    h = float(closed_1d[i][2])
                                    l = float(closed_1d[i][3])
                                    v = float(closed_1d[i][5])
                                    if all(h >= float(closed_1d[i+j][2]) for j in [-2,-1,1,2]):
                                        swing_highs.append((h, v/avg_vol_1d * 1.5))  # weight 1D higher
                                    if all(l <= float(closed_1d[i+j][3]) for j in [-2,-1,1,2]):
                                        swing_lows.append((l, v/avg_vol_1d * 1.5))

                            # Cluster nearby levels
                            def cluster(levels, tol=0.02):
                                result, used = [], set()
                                for i, (p, v) in enumerate(levels):
                                    if i in used: continue
                                    group = [(p, v)]
                                    for j, (p2, v2) in enumerate(levels):
                                        if j != i and j not in used and abs(p-p2)/p <= tol:
                                            group.append((p2, v2))
                                            used.add(j)
                                    used.add(i)
                                    avg_p = sum(x[0] for x in group)/len(group)
                                    avg_v = sum(x[1] for x in group)/len(group)
                                    result.append((avg_p, len(group), avg_v))
                                return sorted(result, key=lambda x: -x[1])

                            c_highs = cluster(swing_highs)
                            c_lows  = cluster(swing_lows)

                            # Support zones below price
                            supports = [(p, t, v) for p, t, v in c_lows  if p < current_price * 0.99][:3]
                            # Resistance lines above price
                            resistances = [(p, t, v) for p, t, v in c_highs if p > current_price * 1.01][:3]

                            lines_out = [f"📊 <b>Zone/Line Suggestions — {sym}</b>\n"]
                            lines_out.append(f"💰 Current price: {format_price(current_price)}\n")

                            if supports:
                                lines_out.append("🟢 <b>Support Zones below (addzone):</b>")
                                for p, touches, vol_r in supports[:2]:
                                    margin = p * 0.01
                                    pct = (current_price - p) / current_price * 100
                                    strength = "🔥 Strong" if touches >= 3 else "✅ Moderate" if touches == 2 else "⚠️ Weak"
                                    lines_out.append(
                                        f"{strength} ({touches}x tested, {vol_r:.1f}x vol)\n"
                                        f"<code>/addzone {sym.replace('USDT','')} "
                                        f"{format_price(p-margin)} {format_price(p+margin)} 4h</code>\n"
                                        f"   → {pct:.1f}% below — bounce/entry zone"
                                    )

                            # Resistance ZONES above — show as both zone and line
                            res_zones = [(p, t, v) for p, t, v in c_highs if p > current_price * 1.005][:4]
                            if res_zones:
                                lines_out.append("\n🔴 <b>Resistance Zones above:</b>")
                                for p, touches, vol_r in res_zones[:3]:
                                    margin = p * 0.012
                                    pct = (p - current_price) / current_price * 100
                                    strength = "🔥 Strong" if touches >= 3 else "✅ Moderate" if touches == 2 else "⚠️ Weak"
                                    sym_short = sym.replace('USDT','')
                                    lines_out.append(
                                        f"{strength} ({touches}x tested, {vol_r:.1f}x vol)\n"
                                        f"<code>/addzone {sym_short} {format_price(p-margin)} {format_price(p+margin)} 4h</code>\n"
                                        f"   → {pct:.1f}% above — resistance / TP target"
                                    )

                            # Situation summary
                            change_24h = float(ticker["priceChangePercent"])
                            trend = "📈 uptrend" if change_24h > 3 else "📉 downtrend" if change_24h < -3 else "↔️ ranging"
                            lines_out.append(
                                f"\n💡 <b>Current situation:</b>\n"
                                f"   24h: {change_24h:+.1f}% — {trend}\n"
                                f"   Nearest support: {format_price(supports[0][0]) if supports else 'N/A'}\n"
                                f"   Nearest resistance: {format_price(resistances[0][0]) if resistances else 'N/A'}"
                            )
                            reply("\n\n".join(lines_out))

                elif text == "/ZONES":
                    if not is_admin:
                        reply( "⚠️ Admin only.")
                    elif not manual_zones:
                        reply( "📐 No active zones.\n/addzone RIF 0.0665 0.0703 4H")
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
                        reply( "⚠️ Admin only.")
                    else:
                        # /trade GPS entry=0.00762 sl=0.00700 tp1=0.00850 tp2=0.00925 tp3=0.01000 [tf=1h]
                        parts = raw_text.strip().split()
                        if len(parts) < 4:
                            reply(
                                "⚠️ Format:\n<code>/trade GPS entry=0.00762 sl=0.00700 tp1=0.00850 tp2=0.00925 tp3=0.01000</code>\n\n"
                                "tf=1h is the default; use tf=4h if you want."
                            )
                        else:
                            try:
                                sym_raw = parts[1].upper()
                                if not is_plausible_symbol(sym_raw):
                                    reply(f"⚠️ '{sym_raw}' doesn't look like a valid symbol. Use a ticker like BTC or BTCUSDT, not a price.")
                                    continue
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
                                    reply( "⚠️ SL must be below entry (assuming a long trade)")
                                elif not tps:
                                    reply( "⚠️ At least one tp1 is required")
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
                                    reply(
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
                                reply( f"⚠️ Format is wrong. Example:\n<code>/trade GPS entry=0.00762 sl=0.00700 tp1=0.00850</code>")

                elif raw_text.upper().startswith("/ST "):
                    if not is_admin:
                        reply( "⚠️ Admin only.")
                    else:
                        # /st GPS entry=0.00762 sl=0.00700 tp1=0.00850
                        # Separate, faster, tighter-SL scalp trade monitor —
                        # distinct from /trade's active_trades.
                        parts = raw_text.strip().split()
                        if len(parts) < 4:
                            reply("⚠️ Format:\n<code>/st GPS entry=0.00762 sl=0.00700 tp1=0.00850</code>")
                        else:
                            try:
                                sym_raw = parts[1].upper()
                                if not is_plausible_symbol(sym_raw):
                                    reply(f"⚠️ '{sym_raw}' doesn't look like a valid symbol. Use a ticker like BTC or BTCUSDT, not a price.")
                                    continue
                                sym = sym_raw if sym_raw.endswith("USDT") else sym_raw + "USDT"
                                kv = {}
                                for p in parts[2:]:
                                    if "=" in p:
                                        k, v = p.split("=", 1)
                                        kv[k.lower()] = v
                                entry = float(kv["entry"])
                                sl    = float(kv["sl"])
                                tp1   = float(kv["tp1"])
                                if sl >= entry:
                                    reply("⚠️ SL must be below entry (assuming a long trade)")
                                else:
                                    trade_id = f"{sym}_scalp_{int(time.time())}"
                                    _scalp_trades[trade_id] = {
                                        "symbol": sym, "entry": entry, "sl": sl, "tp1": tp1,
                                        "started": time.time(), "tp_removed": False, "closed": False,
                                    }
                                    reply(
                                        f"⚡ <b>Scalp trade added!</b>\n\n"
                                        f"🪙 {sym}\n"
                                        f"💰 Entry: {format_price(entry)} | SL: {format_price(sl)} | TP1: {format_price(tp1)}\n"
                                        f"🆔 <code>{trade_id}</code>\n\n"
                                        f"Monitored separately — fast checks, tight SL, and will flag if this looks "
                                        f"like a bigger opportunity than the TP1."
                                    )
                                    print(f"⚡ Scalp trade added: {trade_id}")
                            except (KeyError, ValueError):
                                reply("⚠️ Format is wrong. Example:\n<code>/st GPS entry=0.00762 sl=0.00700 tp1=0.00850</code>")

                elif text == "/TRADES":
                    if not is_admin:
                        reply( "⚠️ Admin only.")
                    elif not active_trades:
                        reply( "💼 No active trades.\n/trade SYMBOL entry=.. sl=.. tp1=..")
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
                        reply( "\n".join(lines))

                elif raw_text.upper().startswith("/CLOSETRADE ") or raw_text.upper().startswith("/TR "):
                    print(f"🗑 /TR or /closetrade command received: '{raw_text}' from chat_id={chat_id} is_admin={is_admin}")
                    if not is_admin:
                        reply( "⚠️ Admin only.")
                    else:
                        arg = raw_text.strip().split(None, 1)[1].strip()
                        if arg in active_trades:
                            # Exact trade ID given (old /closetrade usage)
                            active_trades.pop(arg)
                            trade_alert_cooldown.pop(arg, None)
                            save_active_trades()
                            reply( f"🗑 Trade closed/removed: <code>{arg}</code>")
                        else:
                            # Simple symbol-based removal (/TR POL, /TR POLUSDT, case-insensitive)
                            sym_query = arg.upper()
                            if not sym_query.endswith("USDT"):
                                sym_query += "USDT"
                            matches = [tid for tid, t in active_trades.items() if t.get("symbol", "").upper() == sym_query]
                            if matches:
                                for tid in matches:
                                    active_trades.pop(tid, None)
                                    trade_alert_cooldown.pop(tid, None)
                                save_active_trades()
                                reply( f"🗑 Trade(s) closed/removed for {sym_query}: {len(matches)} removed")
                            else:
                                reply( f"⚠️ No active trade found for: {arg}\nUse /trades to see the list")

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
        f"✅ <b>CryptoPing {BOT_VERSION} is running!</b>\n\n"
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
        f"• 💼 Active Trade Monitor (/trade, /trades, /closetrade or /TR <symbol>)\n"
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
            check_no_retest_pump_risk(symbol, tf="1h")
            check_no_retest_pump_risk(symbol, tf="4h")
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

    # Manual price lines (break + retest) — same pattern as manual zones
    def run_manual_lines():
        while True:
            try:
                check_manual_lines()
            except Exception as e:
                print(f"Manual line error: {e}")
            time.sleep(15)  # check every 15s for faster live cross detection

    Thread(target=run_manual_lines, daemon=True).start()

    def run_vol_accum_scanner():
        while True:
            try:
                scan_volume_accumulation()
                scan_reversal_pumps()
                scan_global_liq_reclaim()  # all-coin liq sweep → High Priority
            except Exception as e:
                print(f"Vol accum scan error: {e}")
            time.sleep(120)  # every 2 minutes, but internal cooldown is 8h per coin

    Thread(target=run_vol_accum_scanner, daemon=True).start()

    def run_dormant_coil_scanner():
        while True:
            try:
                scan_dormant_coil_candidates()
            except Exception as e:
                print(f"Dormant coil scan error: {e}")
            time.sleep(3600)  # every hour — 1D-based data, internal cooldown is 3 days per coin

    Thread(target=run_dormant_coil_scanner, daemon=True).start()

    def run_btc_divergence_scanner():
        while True:
            try:
                scan_btc_divergence()
            except Exception as e:
                print(f"BTC divergence scan error: {e}")
            time.sleep(600)  # every 10 minutes, internal cooldown is 6h per coin

    Thread(target=run_btc_divergence_scanner, daemon=True).start()

    def run_scalp_scanner():
        while True:
            try:
                for _sym in list(watchlist):
                    if _sym in removed_coins:
                        continue
                    try:
                        check_scalp_opportunity(_sym)
                    except Exception as e:
                        print(f"Scalp opportunity check error {_sym}: {e}")
            except Exception as e:
                print(f"Scalp scanner error: {e}")
            time.sleep(180)  # every 3 minutes — fast timeframe (2M-5M) needs frequent checks

    Thread(target=run_scalp_scanner, daemon=True).start()

    def run_scalp_trade_monitor():
        while True:
            try:
                check_scalp_trades()
            except Exception as e:
                print(f"Scalp trade monitor error: {e}")
            time.sleep(60)  # tight SL needs quick updates

    Thread(target=run_scalp_trade_monitor, daemon=True).start()

    def run_valid_ob_scanner():
        while True:
            try:
                for _sym in list(watchlist):
                    if _sym in removed_coins:
                        continue
                    try:
                        check_valid_order_block(_sym)
                    except Exception as e:
                        print(f"Valid OB check error {_sym}: {e}")
            except Exception as e:
                print(f"Valid OB scan error: {e}")
            time.sleep(600)  # every 10 minutes, internal cooldown is 8h per coin

    Thread(target=run_valid_ob_scanner, daemon=True).start()

    def run_confirm_watch_checker():
        while True:
            try:
                check_confirm_watches()
            except Exception as e:
                print(f"Confirm watch check error: {e}")
            time.sleep(300)  # every 5 minutes

    Thread(target=run_confirm_watch_checker, daemon=True).start()

    def run_prospect_watch_checker():
        while True:
            try:
                check_prospect_watches()
            except Exception as e:
                print(f"Prospect watch check error: {e}")
            time.sleep(300)  # every 5 minutes

    Thread(target=run_prospect_watch_checker, daemon=True).start()

    def run_hc_followup_checker():
        while True:
            try:
                check_hc_followup_watches()
            except Exception as e:
                print(f"HC follow-up check error: {e}")
            time.sleep(300)  # every 5 minutes

    Thread(target=run_hc_followup_checker, daemon=True).start()

    def run_big_pump_watch_checker():
        while True:
            try:
                check_big_pump_watches()
            except Exception as e:
                print(f"Big pump watch check error: {e}")
            time.sleep(300)  # every 5 minutes

    Thread(target=run_big_pump_watch_checker, daemon=True).start()

    def run_retest_watch_checker():
        while True:
            try:
                check_shared_retest_watches()
            except Exception as e:
                print(f"Retest watch check error: {e}")
            time.sleep(180)  # every 3 minutes — fast enough for 5M timeframe checks

    Thread(target=run_retest_watch_checker, daemon=True).start()

    def run_whale_trade_scanner():
        while True:
            try:
                for _sym in list(watchlist):
                    if _sym in removed_coins:
                        continue
                    try:
                        check_whale_trades(_sym)
                    except Exception as e:
                        print(f"Whale trade check error {_sym}: {e}")
            except Exception as e:
                print(f"Whale trade scan error: {e}")
            time.sleep(300)  # every 5 minutes, internal cooldown is 2h per coin

    Thread(target=run_whale_trade_scanner, daemon=True).start()

    def run_postpump_retest_scanner():
        while True:
            try:
                scan_postpump_retest()
            except Exception as e:
                print(f"Post-pump scan error: {e}")
            time.sleep(300)  # every 5 minutes, internal cooldown is 12h per coin

    Thread(target=run_postpump_retest_scanner, daemon=True).start()

    # Fast range breakout live detector — checks range_breakout_tracking
    # candidates every 15s using cached ticker, no extra klines API calls.
    # Solves THE/OPG case where main scan loop was too slow.
    def run_fast_range_scanner():
        while True:
            try:
                now_f = time.time()
                for symbol, tracked in list(range_breakout_tracking.items()):
                    live_key = f"{symbol}_1h_breakout_live"
                    if now_f - range_breakout_alerted.get(live_key, 0) < 4 * 3600:
                        continue
                    ticker = get_ticker(symbol)
                    if not ticker:
                        continue
                    current_price = float(ticker["lastPrice"])
                    change_24h    = float(ticker["priceChangePercent"])
                    range_high    = tracked.get("range_high", 0)
                    range_low     = tracked.get("range_low", 0)
                    range_width   = tracked.get("range_width_pct", 0)
                    touches       = tracked.get("near_top_touches", 0)
                    if range_high and current_price > range_high * 1.001:
                        pct = (current_price - range_high) / range_high * 100
                        range_breakout_alerted[live_key] = now_f
                        send_to_topic(TOPIC_BUILDUPS,
                            f"⚡ <b>RANGE BREAKOUT — LIVE [1H]</b>\n\n"
                            f"🪙 <b>{symbol}</b>\n"
                            f"💰 Price: {format_price(current_price)} (+{pct:.1f}% above range)\n"
                            f"📊 24h: {change_24h:+.2f}%\n"
                            f"📐 Range: {format_price(range_low)} — {format_price(range_high)} "
                            f"({range_width:.1f}% wide, {touches} touches at top)\n\n"
                            f"⚡ <i>Fast detection — check the chart NOW.</i>\n\n"
                            f"⚠️ <i>Check the chart before entry.</i>"
                        )
                        track_building_signal(symbol, "Range Breakout LIVE [1H]", current_price)
                        check_high_confidence_signal(symbol, "Range Breakout LIVE [1H]", current_price)
                        print(f"⚡ Fast range scanner: {symbol} +{pct:.1f}%")
            except Exception as e:
                print(f"Fast range scanner error: {e}")
            time.sleep(15)

    Thread(target=run_fast_range_scanner, daemon=True).start()

    def run_liq_watches():
        while True:
            try:
                check_liq_watches()
            except Exception as e:
                print(f"Liq watch error: {e}")
            time.sleep(30)

    Thread(target=run_liq_watches, daemon=True).start()

    def run_entry_watches():
        while True:
            try:
                check_entry_watches()
            except Exception as e:
                print(f"Entry watch error: {e}")
            time.sleep(600)  # every 10 minutes

    Thread(target=run_entry_watches, daemon=True).start()

    def run_active_trades():
        while True:
            try:
                check_active_trades()
            except Exception as e:
                print(f"Active trade monitor error: {e}")
            time.sleep(60)

    Thread(target=run_active_trades, daemon=True).start()

    def run_active_trades_fast():
        while True:
            try:
                check_active_trades_fast()
            except Exception as e:
                print(f"Fast trade monitor error: {e}")
            time.sleep(60)

    Thread(target=run_active_trades_fast, daemon=True).start()

    # Pre-warm the ticker cache once before the first scan pass — without this,
    # the first parallel pass after a cold start would have many threads each
    # individually trigger a ticker refresh before any of them see a populated
    # cache, multiplying calls right at the worst possible moment (startup).
    print("🔥 Pre-warming ticker cache before first scan...")
    _refresh_ticker_cache()

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
