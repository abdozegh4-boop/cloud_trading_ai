import os
import json
import re
import asyncio
import threading
import time
import logging
import math
import httpx
import feedparser
from datetime import datetime, timedelta, timezone
from typing import List, Optional, Dict, Any, Set
from dotenv import load_dotenv
from fastapi import FastAPI, Request, Response, status, HTTPException
from contextlib import asynccontextmanager
from openai import OpenAI
from apscheduler.schedulers.asyncio import AsyncIOScheduler

import db  # طبقة التخزين الدائم على Firestore (+ BigQuery اختياري) + الذاكرة التاريخية
try:
    import bq as bq_sink
except Exception:
    bq_sink = None

# Redis اختياري لتسريع الكاش بين الطبقات (إن لم يتوفر → كاش داخل العملية)
try:
    import redis.asyncio as aioredis  # type: ignore
except Exception:  # pragma: no cover
    aioredis = None  # type: ignore

# مكتبات التلغرام
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.error import BadRequest
from telegram.ext import (
    Application,
    ApplicationBuilder,
    CommandHandler,
    CallbackQueryHandler,
    ContextTypes
)

# مكتبات cTrader Open API و Twisted Engine
from twisted.internet import reactor
from ctrader_open_api import Client, TcpProtocol
from ctrader_open_api.messages.OpenApiCommonMessages_pb2 import *
from ctrader_open_api.messages.OpenApiMessages_pb2 import *
from ctrader_open_api.messages.OpenApiModelMessages_pb2 import *  # تم إضافته: يحتوي على ProtoOATrendbarPeriod و ProtoOATradeSide

# إعداد السجلات (Logging)
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("TradingBot")

load_dotenv()

# ==================== المتغيرات البيئية والإعدادات ====================
# NVIDIA Build (build.nvidia.com) — واجهة متوافقة مع OpenAI API
AI_MODEL = os.getenv("NVIDIA_MODEL", "nvidia/nemotron-3-super-120b-a12b")
api_key = os.getenv("NVIDIA_API_KEY")
ai_client = OpenAI(base_url="https://integrate.api.nvidia.com/v1", api_key=api_key) if api_key else None

# --- Ensemble Multi-Agent models (نفس NVIDIA endpoint؛ يمكن تخصيص كل طبقة) ---
# ملاحظة: NeMo Guardrails إطار عمل وليس chat-model ID على integrate.api.nvidia.com.
# طبقة الدمج/الحراسة تُنفَّذ برمجياً (Data Fusion). الوكلاء الثلاثة تستدعي نماذج LLM.
AGENT_NEWS_MODEL = os.getenv("AGENT_NEWS_MODEL", AI_MODEL)           # News Context Agent
AGENT_LIQUIDITY_MODEL = os.getenv("AGENT_LIQUIDITY_MODEL", AI_MODEL) # Liquidity & Intermarket Agent
AGENT_EXECUTIVE_MODEL = os.getenv("AGENT_EXECUTIVE_MODEL", AI_MODEL) # Executive Decision Agent
USE_ENSEMBLE_AGENTS = os.getenv("USE_ENSEMBLE_AGENTS", "1").strip() not in ("0", "false", "False", "no")
# وضع توفير Cloud Run: min-instances=0 — الاعتماد على طلبات HTTP / Cloud Scheduler لإيقاظ الخدمة
# القيمة الابتدائية من البيئة؛ يمكن تبديلها وقت التشغيل من أزرار تلغرام (runtime_cheap_mode)
CHEAP_CLOUD_RUN = os.getenv("CHEAP_CLOUD_RUN", "1").strip() not in ("0", "false", "False", "no")
CRON_SECRET = os.getenv("CRON_SECRET", "").strip()

TELEGRAM_BOT_TOKEN = (os.getenv("TELEGRAM_BOT_TOKEN") or "").strip()
# strip() مهم: إزالة \r\n أو مسافات قد تدخل من Secret Manager / PowerShell
MY_TELEGRAM_CHAT_ID = (os.getenv("MY_TELEGRAM_CHAT_ID") or "").strip()
WEBHOOK_HOST = (os.getenv("SERVICE_URL") or os.getenv("RENDER_EXTERNAL_URL") or "").strip()
WEBHOOK_PATH = f"/telegram/webhook/{TELEGRAM_BOT_TOKEN}" if TELEGRAM_BOT_TOKEN else "/telegram/webhook"
WEBHOOK_URL = f"{WEBHOOK_HOST}{WEBHOOK_PATH}"

FINNHUB_API_KEY = os.getenv("FINNHUB_API_KEY", "")

# إعدادات cTrader
CTRADER_HOST = os.getenv("CTRADER_HOST", "demo.ctraderapi.com")
CTRADER_PORT = int(os.getenv("CTRADER_PORT", 5035))
CLIENT_ID = os.getenv("CTRADER_CLIENT_ID")
CLIENT_SECRET = os.getenv("CTRADER_CLIENT_SECRET")
ACCESS_TOKEN = os.getenv("CTRADER_ACCESS_TOKEN")
ACCOUNT_ID = int(os.getenv("CTRADER_ACCOUNT_ID", 0)) if os.getenv("CTRADER_ACCOUNT_ID") else 0

# ==================== إيقاعات تحديث المصادر (متعددة، وليست 10 دقائق لكل شيء) ====================
# cTrader = المصدر الرئيسي للحساب + الشموع + الحسابات الفنية
CTRADER_ACCOUNT_REFRESH_SECONDS = int(os.getenv("CTRADER_ACCOUNT_REFRESH_SECONDS", "20"))
# Finnhub أخبار: 30–60 ثانية لتجنب Rate Limit
FINNHUB_REFRESH_SECONDS = int(os.getenv("FINNHUB_REFRESH_SECONDS", "45"))
# TradingView RSS كفلتر/سياق — تحديث شبه لحظي نسبياً
TRADINGVIEW_REFRESH_SECONDS = int(os.getenv("TRADINGVIEW_REFRESH_SECONDS", "60"))
# Forex Factory: عادي 30–60 دقيقة، وأثناء الأخبار القوية 1–5 دقائق
FOREX_FACTORY_NORMAL_MINUTES = int(os.getenv("FOREX_FACTORY_NORMAL_MINUTES", "45"))
FOREX_FACTORY_HIGH_IMPACT_MINUTES = int(os.getenv("FOREX_FACTORY_HIGH_IMPACT_MINUTES", "3"))
# توليد تحليل AI للمتابعات النشطة (منفصل عن جلب البيانات الخام)
# القيمة الافتراضية بقيت قابلة للضبط؛ البيانات نفسها تُحدَّث بإيقاعاتها أعلاه
AUTO_ANALYSIS_INTERVAL_MINUTES = int(os.getenv("AUTO_ANALYSIS_INTERVAL_MINUTES", str(db.AUTO_ANALYSIS_INTERVAL_MINUTES)))
OHLC_REFRESH_MINUTES = int(os.getenv("OHLC_REFRESH_MINUTES", "15"))
# الوضع غير الاقتصادي: APScheduler 24/7 (يتطلب نشر always-on: min-instances=1 بدون CPU throttling).
# تحليل NVIDIA كل 5 دقائق افتراضياً (قابل للتغيير).
NON_CHEAP_OHLC_REFRESH_SECONDS = int(os.getenv("NON_CHEAP_OHLC_REFRESH_SECONDS", "60"))
NON_CHEAP_ANALYSIS_INTERVAL_MINUTES = int(os.getenv("NON_CHEAP_ANALYSIS_INTERVAL_MINUTES", "5"))
# حجم اللوت الافتراضي لتنفيذ الإشارات (وحدات لوت قياسية، مثال 0.01)
DEFAULT_TRADE_LOT = float(os.getenv("DEFAULT_TRADE_LOT", "0.01"))

telegram_app: Optional[Application] = None
scheduler: Optional[AsyncIOScheduler] = None
is_ctrader_connected = False
stop_ctrader_flag = False
# وضع التشغيل الحالي (قابل للتبديل من تلغرام): True = اقتصادي / Cloud Scheduler
runtime_cheap_mode: bool = CHEAP_CLOUD_RUN
# تفعيل التنفيذ الآلي للإشارات (زر «بدء التداول»)
auto_trading_enabled: bool = False
# chat_id للمستخدم الذي ضغط «بدء التداول» — التنفيذ التلقائي المستمر يخصّه فقط
auto_trading_chat_id: Optional[int] = None
# دمج إشارات الأطر مع آخر توصية شاملة لتجنب التعارض
signals_merge_full: bool = os.getenv("SIGNALS_MERGE_FULL", "1").strip() not in ("0", "false", "False")


# ==================== مزامنة ردود cTrader (انتظار فعلي قبل عرض البيانات) ====================
# request_account_details() ترسل الطلب فقط وتعود فوراً. كنا نقرأ الكاش القديم قبل وصول
# الرد، فكنا نعرض "لا توجد صفقات" رغم وجودها. الآن ننتظر الحدث حتى يصل ProtoOATraderRes
# أو ProtoOAReconcileRes فعلياً (أو ينتهي timeout).
_ctrader_account_event: Optional[asyncio.Event] = None
CTRADER_REPLY_TIMEOUT = float(os.getenv("CTRADER_REPLY_TIMEOUT", "5.0"))


def _notify_account_replied() -> None:
    """يُستدعى من خيط cTrader (Twisted) عند وصول رد الحساب/الصفقات."""
    global _ctrader_account_event
    ev = _ctrader_account_event
    if ev is None:
        return
    try:
        loop = asyncio.get_event_loop()
        if loop.is_running():
            loop.call_soon_threadsafe(ev.set)
    except Exception:
        pass


async def request_account_details_and_wait(timeout: Optional[float] = None) -> bool:
    """
    يطلب بيانات الحساب وينتظر وصول رد cTrader الفعلي (حتى timeout).
    يعيد True لو وصل رد خلال المدة، False عند انتهاء المهلة (ونعرض الكاش المتوفر).
    """
    global _ctrader_account_event
    if not (is_ctrader_connected and ACCOUNT_ID):
        return False
    if _ctrader_account_event is None:
        try:
            _ctrader_account_event = asyncio.Event()
        except RuntimeError:
            _ctrader_account_event = None
            request_account_details()
            return False
    _ctrader_account_event.clear()
    request_account_details()
    wait_s = timeout if timeout is not None else CTRADER_REPLY_TIMEOUT
    try:
        await asyncio.wait_for(_ctrader_account_event.wait(), timeout=wait_s)
        return True
    except asyncio.TimeoutError:
        return False
    except Exception:
        return False


def _account_snapshot_age() -> Optional[float]:
    """عمر آخر لقطة حساب/صفقات بالثواني (None = لم يصل أي رد بعد)."""
    upd = ctrader_account_info.get("updated_at")
    if not upd:
        return None
    try:
        now = datetime.now(timezone.utc)
        return max(0.0, (now - upd).total_seconds())
    except Exception:
        return None


def _format_snapshot_age(age_sec: Optional[float]) -> str:
    if age_sec is None:
        return "—"
    if age_sec < 60:
        return f"{int(age_sec)} ثانية"
    if age_sec < 3600:
        return f"{int(age_sec // 60)} دقيقة"
    return f"{age_sec / 3600:.1f} ساعة"


def _snapshot_source_label(snap: Dict[str, Any]) -> str:
    """
    ملصق مصدر البيانات لرسائل التيليجرام حتى يعرف المستخدم ما إذا كانت
    البيانات لحظية من cTrader أم لقطة قديمة (احتياطية).
    """
    if snap.get("error") == "missing_credentials":
        return "⚪ **المصدر:** غير مُهيّأ — متغيرات cTrader (CLIENT_ID/SECRET/ACCOUNT_ID) ناقصة"
    if not snap.get("connected"):
        return (
            "🔴 **المصدر:** cTrader غير متصل الآن\n"
            f"📍 تعرض آخر لقطة محفوظة (عمرها `{_format_snapshot_age(snap.get('age_sec'))}`)"
        )
    if snap.get("fresh"):
        return "🟢 **المصدر:** cTrader مباشر (رد لحظي مؤكَّد)"
    return (
        "🟡 **المصدر:** cTrader متصل لكن الرد تأخّر\n"
        f"📍 تعرض آخر لقطة محفوظة (عمرها `{_format_snapshot_age(snap.get('age_sec'))}`)"
    )


async def ensure_ctrader_live_snapshot(timeout: float = 12.0) -> Dict[str, Any]:
    """
    يضمن قراءة لقطة حديثة مباشرة من حساب cTrader (وليس من قاعدة البيانات/الكاش):
      1) إن كان الاتصال مقطوعاً → يحفّز إعادة الاتصال وينتظر.
      2) يرسل طلب الحساب/الصفقات وينتظر الرد الفعلي من cTrader.
    يعيد {'ok', 'connected', 'fresh', 'age_sec', 'error'}.
    """
    if not (CLIENT_ID and CLIENT_SECRET and ACCOUNT_ID):
        return {"ok": False, "connected": False, "fresh": False,
                "age_sec": _account_snapshot_age(), "error": "missing_credentials"}

    # 1) تأمين الاتصال قبل أي طلب
    if not is_ctrader_connected:
        try:
            reactor.callFromThread(ctrader_client.startService)
        except Exception as e:
            logger.error(f"ensure live snapshot reconnect error: {e}")
        deadline = time.time() + min(timeout, 8.0)
        while not is_ctrader_connected and time.time() < deadline:
            await asyncio.sleep(0.2)

    if not is_ctrader_connected:
        return {"ok": False, "connected": False, "fresh": False,
                "age_sec": _account_snapshot_age(), "error": "not_connected"}

    # 2) طلب لحظي + انتظار الرد الفعلي
    fresh = await request_account_details_and_wait(timeout=timeout)
    return {"ok": True, "connected": True, "fresh": fresh,
            "age_sec": _account_snapshot_age(), "error": None}


# بيانات الحساب والصفقات (المصدر: cTrader فقط)
ctrader_account_info: Dict[str, Any] = {
    "balance": 0.0,
    "equity": 0.0,
    "margin": 0.0,
    "free_margin": 0.0,
    "margin_level": 0.0,
    "leverage": None,
    "deposit_asset": None,
    "money_digits": 2,
    "updated_at": None,
}
active_positions: List[Dict[str, Any]] = []
_known_position_ids: Set[int] = set()
_last_notify_ts: Dict[str, float] = {}  # rate-limit إشعارات
closed_deals_cache: List[Dict[str, Any]] = []
symbol_specs_cache: Dict[str, Dict[str, Any]] = {}

# ==================== Runtime In-Memory / Redis Cache ====================
# هدف: جلب لقطة الحساب/الأخبار/الفلاتر للوكلاء بالملي ثانية بدل ضرب DB في كل إشارة.
REDIS_URL = os.getenv("REDIS_URL", "").strip()
TV_WEBHOOK_SECRET = os.getenv("TV_WEBHOOK_SECRET", "").strip()

class RuntimeCache:
    """كاش تشغيلي: ذاكرة العملية دائماً، وRedis اختيارياً للمشاركة عبر النسخ."""

    def __init__(self):
        self._mem: Dict[str, Any] = {}
        self._redis = None

    async def init_redis(self):
        if not REDIS_URL or aioredis is None:
            if REDIS_URL and aioredis is None:
                logger.warning("⚠️ REDIS_URL مضبوط لكن حزمة redis غير مثبتة — استخدام كاش الذاكرة فقط.")
            return
        try:
            self._redis = aioredis.from_url(REDIS_URL, decode_responses=True)
            await self._redis.ping()
            logger.info("✅ تم الاتصال بـ Redis للكاش التشغيلي.")
        except Exception as e:
            logger.error(f"❌ فشل اتصال Redis — الاستمرار بكاش الذاكرة فقط: {e}")
            self._redis = None

    async def close(self):
        if self._redis is not None:
            try:
                await self._redis.close()
            except Exception:
                pass
            self._redis = None

    def set_mem(self, key: str, value: Any, ttl_seconds: Optional[int] = None):
        exp = None
        if ttl_seconds is not None:
            exp = time.time() + ttl_seconds
        self._mem[key] = {"value": value, "expires_at": exp}

    def get_mem(self, key: str, default: Any = None) -> Any:
        item = self._mem.get(key)
        if item is None:
            return default
        exp = item.get("expires_at")
        if exp is not None and time.time() > exp:
            self._mem.pop(key, None)
            return default
        return item.get("value", default)

    async def set(self, key: str, value: Any, ttl_seconds: Optional[int] = None):
        self.set_mem(key, value, ttl_seconds)
        if self._redis is not None:
            try:
                payload = json.dumps(value, ensure_ascii=False, default=str)
                if ttl_seconds:
                    await self._redis.set(key, payload, ex=int(ttl_seconds))
                else:
                    await self._redis.set(key, payload)
            except Exception as e:
                logger.error(f"Redis set error: {e}")

    async def get(self, key: str, default: Any = None) -> Any:
        if self._redis is not None:
            try:
                raw = await self._redis.get(key)
                if raw is not None:
                    return json.loads(raw)
            except Exception as e:
                logger.error(f"Redis get error: {e}")
        return self.get_mem(key, default)

    def snapshot_for_agents(self) -> Dict[str, Any]:
        """لقطة سريعة من الذاكرة لخط الوكلاء (بدون انتظار شبكة)."""
        return {
            "account": self.get_mem("account"),
            "positions": self.get_mem("positions") or [],
            "forex_factory": (self.get_mem("forex_factory") or {}).get("data")
                if isinstance(self.get_mem("forex_factory"), dict) else self.get_mem("forex_factory"),
            "finnhub_news": (self.get_mem("finnhub_news") or {}).get("data")
                if isinstance(self.get_mem("finnhub_news"), dict) else self.get_mem("finnhub_news"),
            "tradingview_rss": (self.get_mem("tradingview_rss") or {}).get("data")
                if isinstance(self.get_mem("tradingview_rss"), dict) else self.get_mem("tradingview_rss"),
            "last_news_agent": self.get_mem("last_news_agent"),
            "last_liquidity_agent": self.get_mem("last_liquidity_agent"),
            "last_executive": self.get_mem("last_executive"),
            "trendbars_symbols": list(trendbars_cache.keys()),
        }


runtime_cache = RuntimeCache()

# كاش المصادر الخارجية مع ختم زمني (TTL) — متوافق مع الدوال الحالية
_source_cache: Dict[str, Any] = {
    "forex_factory": {"data": [], "fetched_at": None},
    "finnhub_news": {"data": [], "fetched_at": None},
    "tradingview_rss": {"data": [], "fetched_at": None},
}

symbol_id_map: Dict[str, int] = {}
symbol_id_to_name: Dict[int, str] = {}
trendbars_cache: Dict[str, Dict[str, Any]] = {}
pending_requests_tf: Dict[int, str] = {}  # لتتبع الإطار الزمني للطلبات المرسلة

# الأطر الزمنية والأزواج المتاحة (M1 = شموع الدقيقة)
AVAILABLE_TIMEFRAMES = ["M1", "M15", "H1", "H4", "D1"]


# تخزين اختيارات المستخدم الحالية (في الذاكرة)
user_selected_symbols: Dict[int, List[str]] = {}
user_selected_tfs: Dict[int, List[str]] = {}
user_trading_tf: Dict[int, str] = {}  # إطار التنفيذ الفعلي (واحد)
# آخر وقت شمعة عُولجت لإشارة كل (symbol, tf) — لتحديث تلقائي عند إغلاق شمعة جديدة
_last_tf_signal_bar: Dict[str, datetime] = {}
TF_SIGNALS_AUTO_ENABLED = os.getenv("TF_SIGNALS_AUTO", "1").strip() not in ("0", "false", "False")

ALL_AVAILABLE_SYMBOLS = {
    "forex": ["EURUSD", "GBPUSD", "USDJPY", "USDCHF", "AUDUSD", "USDCAD", "NZDUSD", "EURGBP", "GBPJPY"],
    "commodities": ["XAUUSD", "XAGUSD", "USOIL", "UKOIL", "NGAS"],
    "indices": ["US30", "NAS100", "SPX500", "GER40", "UK100"],
    "crypto": ["BTCUSD", "ETHUSD", "SOLUSD", "AAPL", "NVDA", "TSLA"]
}

# ==================== Technical Analysis Engine ====================

def calculate_technical_indicators(bars: List[Any], symbol: Optional[str] = None) -> Dict[str, Any]:
    """
    مؤشرات cTrader محسّنة لدقة أعلى:
      RSI(14), EMA(20/50), ATR(14), Pivot,
      MACD(12,26,9), زخم 10 شموع، ميل EMA، نسبة الحجم،
      ADX تقريبي، ونقاط confluence_score ∈ [0..100].
    symbol: لفك ترميز الأسعار بالمقياس الصحيح (10^digits) لكل رمز.
    """
    if len(bars) < 50:
        return {"error": "عدد الشموع غير كافٍ لحساب المؤشرات (يُشترط 50 شمعة على الأقل)"}

    scale = symbol_price_scale(symbol) if symbol else 100000.0
    closes = [(b.low + b.deltaClose) / scale for b in bars]
    highs = [(b.low + b.deltaHigh) / scale for b in bars]
    lows = [b.low / scale for b in bars]
    volumes = [float(getattr(b, "volume", 0) or 0) for b in bars]
    n = len(closes)
    last_volume = volumes[-1]

    def calc_ema_series(period: int, prices: List[float]) -> List[float]:
        k = 2 / (period + 1)
        out = [0.0] * len(prices)
        ema = sum(prices[:period]) / period
        for i in range(period):
            out[i] = ema if i == period - 1 else prices[i]
        out[period - 1] = ema
        for i in range(period, len(prices)):
            ema = (prices[i] * k) + (ema * (1 - k))
            out[i] = ema
        return out

    def calc_ema(period: int, prices: List[float]) -> float:
        return calc_ema_series(period, prices)[-1]

    ema_20_series = calc_ema_series(20, closes)
    ema_50_series = calc_ema_series(50, closes)
    ema_20 = ema_20_series[-1]
    ema_50 = ema_50_series[-1]
    # ميل EMA20 على آخر 5 شموع (اتجاه قصير)
    ema20_slope = 0.0
    if n >= 25:
        prev = ema_20_series[-6]
        if abs(prev) > 1e-12:
            ema20_slope = (ema_20 - prev) / abs(prev)

    # RSI Wilder
    gains, losses = [], []
    for i in range(1, n):
        diff = closes[i] - closes[i - 1]
        gains.append(diff if diff > 0 else 0.0)
        losses.append(abs(diff) if diff < 0 else 0.0)
    avg_gain = sum(gains[:14]) / 14
    avg_loss = sum(losses[:14]) / 14
    for i in range(14, len(gains)):
        avg_gain = (avg_gain * 13 + gains[i]) / 14
        avg_loss = (avg_loss * 13 + losses[i]) / 14
    if avg_loss == 0:
        rsi_14 = 100.0
    else:
        rs = avg_gain / avg_loss
        rsi_14 = 100 - (100 / (1 + rs))

    # ATR
    tr_list = []
    for i in range(1, n):
        tr = max(
            highs[i] - lows[i],
            abs(highs[i] - closes[i - 1]),
            abs(lows[i] - closes[i - 1]),
        )
        tr_list.append(tr)
    atr_14 = sum(tr_list[-14:]) / 14 if len(tr_list) >= 14 else (tr_list[-1] if tr_list else 0.0)

    # MACD 12/26/9
    ema12 = calc_ema_series(12, closes)
    ema26 = calc_ema_series(26, closes)
    macd_line = [ema12[i] - ema26[i] for i in range(n)]
    # signal = EMA9 of macd_line (start from index 25)
    macd_signal_series = calc_ema_series(9, macd_line[25:]) if n > 34 else macd_line[:]
    macd_val = macd_line[-1]
    macd_sig = macd_signal_series[-1] if macd_signal_series else 0.0
    macd_hist = macd_val - macd_sig

    # زخم 10 شموع
    momentum_10 = 0.0
    if n >= 11 and abs(closes[-11]) > 1e-12:
        momentum_10 = (closes[-1] - closes[-11]) / abs(closes[-11]) * 100.0

    # حجم نسبي
    vol_avg = sum(volumes[-20:]) / min(20, len(volumes)) if volumes else 0.0
    volume_ratio = (last_volume / vol_avg) if vol_avg > 0 else 1.0

    # ADX تقريبي (14) — قوة الاتجاه بدون اتجاه
    plus_dm, minus_dm = [], []
    for i in range(1, n):
        up = highs[i] - highs[i - 1]
        down = lows[i - 1] - lows[i]
        plus_dm.append(up if up > down and up > 0 else 0.0)
        minus_dm.append(down if down > up and down > 0 else 0.0)
    def _wilder_smooth(vals, period=14):
        if len(vals) < period:
            return 0.0
        s = sum(vals[:period])
        for i in range(period, len(vals)):
            s = s - (s / period) + vals[i]
        return s / period
    atr_s = _wilder_smooth(tr_list, 14) or 1e-12
    pdi = 100.0 * (_wilder_smooth(plus_dm, 14) / atr_s)
    mdi = 100.0 * (_wilder_smooth(minus_dm, 14) / atr_s)
    dx = 100.0 * abs(pdi - mdi) / (pdi + mdi + 1e-12)
    adx_14 = min(100.0, max(0.0, dx))  # تقريب لنقطة واحدة كافية للفلترة

    # Pivot
    prev_high, prev_low, prev_close = highs[-2], lows[-2], closes[-2]
    pivot = (prev_high + prev_low + prev_close) / 3
    r1 = 2 * pivot - prev_low
    s1 = 2 * pivot - prev_high
    r2 = pivot + (prev_high - prev_low)
    s2 = pivot - (prev_high - prev_low)

    # نقاط confluence للاتجاه (0..100)
    score_buy = 0
    score_sell = 0
    if closes[-1] > ema_20:
        score_buy += 15
    else:
        score_sell += 15
    if ema_20 > ema_50:
        score_buy += 20
    elif ema_20 < ema_50:
        score_sell += 20
    if rsi_14 < 35:
        score_buy += 20
    elif rsi_14 > 65:
        score_sell += 20
    elif rsi_14 >= 52:
        score_buy += 8
    elif rsi_14 <= 48:
        score_sell += 8
    if macd_hist > 0:
        score_buy += 15
    else:
        score_sell += 15
    if momentum_10 > 0.05:
        score_buy += 10
    elif momentum_10 < -0.05:
        score_sell += 10
    if ema20_slope > 0:
        score_buy += 10
    elif ema20_slope < 0:
        score_sell += 10
    if adx_14 >= 22:
        # اتجاه قوي: عزّز الجانب المهيمن
        if score_buy >= score_sell:
            score_buy += 10
        else:
            score_sell += 10
    confluence_score = max(score_buy, score_sell)
    bias = "bullish" if score_buy > score_sell + 5 else ("bearish" if score_sell > score_buy + 5 else "neutral")

    return {
        "last_price": round(closes[-1], 5),
        "volume": last_volume,
        "volume_ratio": round(volume_ratio, 2),
        "rsi_14": round(rsi_14, 2),
        "ema_20": round(ema_20, 5),
        "ema_50": round(ema_50, 5),
        "ema20_slope_pct": round(ema20_slope * 100, 4),
        "atr_14": round(atr_14, 5),
        "macd": round(macd_val, 6),
        "macd_signal": round(macd_sig, 6),
        "macd_hist": round(macd_hist, 6),
        "momentum_10_pct": round(momentum_10, 3),
        "adx_14": round(adx_14, 2),
        "plus_di": round(pdi, 2),
        "minus_di": round(mdi, 2),
        "pivot": round(pivot, 5),
        "r1": round(r1, 5),
        "s1": round(s1, 5),
        "r2": round(r2, 5),
        "s2": round(s2, 5),
        "bias": bias,
        "confluence_score": int(min(100, confluence_score)),
        "score_buy": int(score_buy),
        "score_sell": int(score_sell),
    }



def decode_ctrader_bars_to_ohlc(bars: List[Any], symbol: Optional[str] = None) -> List[Dict[str, Any]]:
    """
    يحوّل trendbars من cTrader إلى قائمة OHLC قياسية:
    open, high, low, close, volume, bar_time
    symbol: يحدد المقياس الصحيح (10^digits) لكل رمز بدل الثابت 100000.
    """
    out: List[Dict[str, Any]] = []
    if not bars:
        return out
    scale = symbol_price_scale(symbol) if symbol else 100000.0
    for b in bars:
        try:
            low = float(b.low) / scale
            high = (float(b.low) + float(getattr(b, "deltaHigh", 0) or 0)) / scale
            close = (float(b.low) + float(getattr(b, "deltaClose", 0) or 0)) / scale
            delta_open = getattr(b, "deltaOpen", None)
            if delta_open is not None:
                open_ = (float(b.low) + float(delta_open)) / scale
            else:
                # إن لم يتوفر deltaOpen نقارب الافتتاح بالإغلاق السابق لاحقاً؛ هنا نستخدم close كنقطة بداية
                open_ = close
            vol = int(getattr(b, "volume", 0) or 0)
            # وقت الشمعة: utcTimestampFrom بالميلي ثانية غالباً
            ts_ms = getattr(b, "utcTimestampFrom", None) or getattr(b, "utcTimestampTo", None)
            if ts_ms is None:
                continue
            if isinstance(ts_ms, (int, float)):
                # بعض الردود بالثواني
                if ts_ms > 1e12:
                    bar_time = datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc)
                else:
                    bar_time = datetime.fromtimestamp(float(ts_ms), tz=timezone.utc)
            else:
                continue
            out.append({
                "bar_time": bar_time,
                "open": round(open_, 8),
                "high": round(high, 8),
                "low": round(low, 8),
                "close": round(close, 8),
                "volume": vol,
            })
        except Exception:
            continue
    # إصلاح open إن كان deltaOpen غائباً: open[i] = close[i-1]
    for i in range(1, len(out)):
        # فقط إذا open == close لنفس الشمعة (تقريبنا السابق)
        if out[i]["open"] == out[i]["close"]:
            out[i]["open"] = out[i - 1]["close"]
    return out


async def persist_ohlc_from_ctrader_bars(symbol: str, timeframe: str, bars: List[Any]) -> int:
    """تحويل شموع cTrader وتخزينها في جدول ohlc_bars."""
    if not bars:
        return 0
    rows = decode_ctrader_bars_to_ohlc(bars, symbol)
    if not rows:
        return 0
    n = await db.save_ohlc_bars(symbol, timeframe, rows, source="ctrader")
    if n:
        logger.info(f"💾 OHLC stored [{symbol} {timeframe}]: {n} bars")
    return n


# ==================== Data Aggregation Layer ====================

def _cache_age_seconds(key: str) -> Optional[float]:
    entry = _source_cache.get(key) or {}
    ts = entry.get("fetched_at")
    if not ts:
        return None
    return (datetime.now(timezone.utc) - ts).total_seconds()


def _is_high_impact_window(events: List[Any]) -> bool:
    """يُقدّر إن كنا قرب خبر عالي التأثير خلال ±90 دقيقة (لتكثيف تحديث التقويم)."""
    if not events:
        return False
    now = datetime.now(timezone.utc)
    for ev in events:
        if not isinstance(ev, dict):
            continue
        impact = str(ev.get("impact") or ev.get("importance") or ev.get("volatility") or "").lower()
        title = str(ev.get("title") or ev.get("event") or "")
        high = any(x in impact for x in ("high", "3", "red", "قوي")) or any(
            x in title.upper() for x in ("NFP", "FOMC", "CPI", "INTEREST RATE", "GDP")
        )
        if not high:
            continue
        for k in ("date", "time", "datetime", "timestamp"):
            raw = ev.get(k)
            if not raw:
                continue
            try:
                if isinstance(raw, (int, float)):
                    dt = datetime.fromtimestamp(float(raw) / (1000 if raw > 1e12 else 1), tz=timezone.utc)
                else:
                    dt = datetime.fromisoformat(str(raw).replace("Z", "+00:00"))
                    if dt.tzinfo is None:
                        dt = dt.replace(tzinfo=timezone.utc)
                if abs((dt - now).total_seconds()) <= 90 * 60:
                    return True
            except Exception:
                continue
    return False


async def fetch_forex_factory_calendar(force: bool = False) -> List[Dict[str, Any]]:
    """
    تقويم اقتصادي: تحديث عادي كل FOREX_FACTORY_NORMAL_MINUTES،
    وأثناء نافذة الأخبار القوية كل FOREX_FACTORY_HIGH_IMPACT_MINUTES.
    """
    cached = _source_cache["forex_factory"]["data"]
    age = _cache_age_seconds("forex_factory")
    high = _is_high_impact_window(cached or [])
    ttl_sec = (FOREX_FACTORY_HIGH_IMPACT_MINUTES if high else FOREX_FACTORY_NORMAL_MINUTES) * 60
    if not force and age is not None and age < ttl_sec and cached is not None:
        return cached

    url = "https://nfp.ourforecast.com/api/v1/calendar"
    try:
        async with httpx.AsyncClient(timeout=6.0) as http_client:
            resp = await http_client.get(url)
            if resp.status_code == 200:
                data = resp.json()[:12]
                entry = {
                    "data": data,
                    "fetched_at": datetime.now(timezone.utc),
                }
                _source_cache["forex_factory"] = entry
                runtime_cache.set_mem("forex_factory", entry, FOREX_FACTORY_NORMAL_MINUTES * 60)
                mode = "HIGH-IMPACT" if _is_high_impact_window(data) else "normal"
                logger.info(f"📅 Forex Factory: {len(data)} أحداث (وضع={mode})")
                return data
    except Exception as e:
        logger.error(f"Forex Factory Fetch Error: {e}")
    return cached or []


async def fetch_finnhub_news(force: bool = False) -> List[str]:
    """أخبار Finnhub عبر REST كل FINNHUB_REFRESH_SECONDS (افتراضي 45ث) لتجنب Rate Limit."""
    cached = _source_cache["finnhub_news"]["data"]
    age = _cache_age_seconds("finnhub_news")
    if not force and age is not None and age < FINNHUB_REFRESH_SECONDS and cached is not None:
        return cached

    if not FINNHUB_API_KEY:
        logger.warning("⚠️ FINNHUB_API_KEY غير مضبوط — لن يتم جلب أي أخبار من Finnhub.")
        return cached or []

    url = f"https://finnhub.io/api/v1/news?category=forex&token={FINNHUB_API_KEY}"
    try:
        async with httpx.AsyncClient(timeout=6.0) as http_client:
            resp = await http_client.get(url)
            if resp.status_code == 200:
                articles = resp.json()[:8]
                lines = [
                    f"- {item.get('headline')}: {(item.get('summary') or '')[:120]}..."
                    for item in articles
                ]
                entry = {
                    "data": lines,
                    "fetched_at": datetime.now(timezone.utc),
                }
                _source_cache["finnhub_news"] = entry
                runtime_cache.set_mem("finnhub_news", entry, FINNHUB_REFRESH_SECONDS)
                logger.info(f"📰 Finnhub: تم جلب {len(articles)} خبر (TTL={FINNHUB_REFRESH_SECONDS}s)")
                return lines
            else:
                logger.error(f"❌ Finnhub رفض الطلب — HTTP {resp.status_code}: {resp.text[:200]}")
    except Exception as e:
        logger.error(f"Finnhub Fetch Error: {e}")
    return cached or []


async def fetch_tradingview_rss(force: bool = False) -> List[str]:
    """
    TradingView كـفلتر/سياق (RSS + تقييمات نصية إن توفرت) — ليس مصدراً رئيسياً للسعر.
    التحديث كل TRADINGVIEW_REFRESH_SECONDS.
    ملاحظة: Volume Profile / DOM / Pine Webhooks / Screener تتطلب تكاملات TradingView
    إضافية (Webhook/API مدفوعة) وليست جزءاً من هذا المسار العام.
    """
    cached = _source_cache["tradingview_rss"]["data"]
    age = _cache_age_seconds("tradingview_rss")
    if not force and age is not None and age < TRADINGVIEW_REFRESH_SECONDS and cached is not None:
        return cached

    url = "https://www.tradingview.com/feed/"
    try:
        loop = asyncio.get_running_loop()
        feed = await loop.run_in_executor(None, feedparser.parse, url)
        entries = feed.entries[:8]
        lines = [
            f"- {entry.title}: {entry.summary if hasattr(entry, 'summary') else ''}"
            for entry in entries
        ]
        entry = {
            "data": lines,
            "fetched_at": datetime.now(timezone.utc),
        }
        _source_cache["tradingview_rss"] = entry
        runtime_cache.set_mem("tradingview_rss", entry, TRADINGVIEW_REFRESH_SECONDS)
        logger.info(f"📉 TradingView RSS (فلتر): {len(entries)} عنصر (TTL={TRADINGVIEW_REFRESH_SECONDS}s)")
        return lines
    except Exception as e:
        logger.error(f"TradingView RSS Fetch Error: {e}")
    return cached or []


async def aggregate_multi_symbols_data(symbols: List[str], tfs: List[str]) -> Dict[str, Any]:
    """
    يجمع البيانات للتحليل:
    - cTrader = المصدر الرئيسي للأسعار والشموع والمؤشرات الفنية المحسوبة.
    - Finnhub / Forex Factory / TradingView = مصادر مساعدة بفترات تحديث مستقلة (كاش TTL).
    """
    ff_task = fetch_forex_factory_calendar()
    fh_task = fetch_finnhub_news()
    tv_task = fetch_tradingview_rss()
    ff_data, fh_data, tv_data = await asyncio.gather(ff_task, fh_task, tv_task)

    # تحديث حساب cTrader عند كل تجميع (لحظي نسبياً عبر reconcile الدوري أيضاً)
    request_account_details()

    symbols_technical: Dict[str, Dict[str, Any]] = {}
    technical_lines: List[str] = []
    for sym in symbols:
        symbols_technical[sym] = {}
        for tf in tfs:
            bars = await ensure_trendbars_ready(sym, tf)
            indicators = (
                calculate_technical_indicators(bars, sym)
                if bars
                else {"error": "لا تتوفر شموع كافية من cTrader لهذا الزوج/الإطار حالياً"}
            )
            symbols_technical[sym][tf] = indicators
            if "error" in indicators:
                technical_lines.append(f"- {sym} [{tf}]: {indicators['error']}")
            else:
                technical_lines.append(
                    f"- {sym} [{tf}] (cTrader): last_price={indicators['last_price']}, "
                    f"RSI(14)={indicators['rsi_14']}, EMA(20)={indicators['ema_20']}, "
                    f"EMA(50)={indicators['ema_50']}, ATR(14)={indicators['atr_14']}, "
                    f"volume={indicators['volume']}, "
                    f"Pivot={indicators['pivot']}, R1={indicators['r1']}, S1={indicators['s1']}, "
                    f"R2={indicators['r2']}, S2={indicators['s2']}"
                )

    technical_summary = "\n".join(technical_lines) if technical_lines else "No live cTrader technical data available."

    acct = ctrader_account_info
    account_snapshot = (
        f"Balance={acct.get('balance')}, Equity={acct.get('equity')}, "
        f"Margin={acct.get('margin')}, FreeMargin={acct.get('free_margin')}, "
        f"MarginLevel={acct.get('margin_level')}, Leverage={acct.get('leverage')}, "
        f"Positions={len(active_positions)}"
    )

    return {
        "symbols": symbols,
        "forex_factory": ff_data,
        "finnhub_news": fh_data,
        "tradingview_rss": tv_data,
        "symbols_technical": symbols_technical,
        "technical_summary": technical_summary,
        "account_snapshot": account_snapshot,
        "data_hierarchy": {
            "primary_prices_and_ohlc": "cTrader Open API",
            "filters_context": ["TradingView RSS", "Finnhub news", "Forex Factory calendar"],
        },
    }


# ==================== cTrader Open API Client ====================

ctrader_client = Client(CTRADER_HOST, CTRADER_PORT, TcpProtocol)

def on_connected(client):
    global is_ctrader_connected
    is_ctrader_connected = True
    logger.info("Connected to cTrader Open API Engine")
    if CLIENT_ID and CLIENT_SECRET:
        req = ProtoOAApplicationAuthReq()
        req.clientId = CLIENT_ID
        req.clientSecret = CLIENT_SECRET
        client.send(req)

def on_disconnected(client, reason):
    global is_ctrader_connected
    is_ctrader_connected = False
    logger.warning(f"Disconnected from cTrader Open API: {reason}")
    try:
        schedule_notify(
            f"🔴 **انقطع اتصال cTrader**\n`{reason}`",
            key="ctrader_disconnect",
            min_interval_sec=60,
        )
    except Exception:
        pass

def on_message_received(client, message):
    global symbol_id_map, symbol_id_to_name, trendbars_cache, ctrader_account_info, active_positions
    msg_type = message.payloadType
    
    if msg_type == ProtoOAApplicationAuthRes().payloadType:
        if ACCOUNT_ID and ACCESS_TOKEN:
            acc_req = ProtoOAAccountAuthReq()
            acc_req.ctidTraderAccountId = int(ACCOUNT_ID)
            acc_req.accessToken = str(ACCESS_TOKEN)
            client.send(acc_req)
            
    elif msg_type == ProtoOAAccountAuthRes().payloadType:
        logger.info(f"cTrader Account {ACCOUNT_ID} Authenticated!")
        sym_req = ProtoOASymbolsListReq()
        sym_req.ctidTraderAccountId = ACCOUNT_ID
        client.send(sym_req)
        # طلب بيانات الحساب (رصيد/رافعة) — ليست ضمن Reconcile
        try:
            trader_req = ProtoOATraderReq()
            trader_req.ctidTraderAccountId = ACCOUNT_ID
            client.send(trader_req)
        except Exception as e:
            logger.error(f"ProtoOATraderReq send error: {e}")
        try:
            rec_req = ProtoOAReconcileReq()
            rec_req.ctidTraderAccountId = ACCOUNT_ID
            client.send(rec_req)
        except Exception as e:
            logger.error(f"ProtoOAReconcileReq send error: {e}")

    elif msg_type == ProtoOATraderRes().payloadType:
        res = ProtoOATraderRes()
        res.ParseFromString(message.payload)
        trader = getattr(res, "trader", None)
        if trader is not None:
            _apply_trader_account_info(trader)
            logger.info(
                f"Trader info: Balance=${ctrader_account_info.get('balance')}, "
                f"Leverage={ctrader_account_info.get('leverage')}, "
                f"moneyDigits={ctrader_account_info.get('money_digits')}"
            )
            runtime_cache.set_mem("account", dict(ctrader_account_info), CTRADER_ACCOUNT_REFRESH_SECONDS * 3)
            _notify_account_replied()
        else:
            logger.warning("ProtoOATraderRes received but trader payload empty")

    elif msg_type == ProtoOASymbolsListRes().payloadType:
        res = ProtoOASymbolsListRes()
        res.ParseFromString(message.payload)
        for s in res.symbol:
            symbol_id_map[s.symbolName] = s.symbolId
            symbol_id_to_name[s.symbolId] = s.symbolName
        logger.info(f"Loaded {len(symbol_id_map)} Symbol IDs from Broker.")
        # طلب مواصفات كل رمز (digits / lotSize / pipPosition) — ضروري لفك ترميز
        # الأسعار وحساب الحجم بشكل صحيح لكل رمز بدل الافتراض الثابت 100000.
        _request_all_symbol_specs(client)

    elif msg_type == ProtoOASymbolByIdRes().payloadType:
        try:
            res = ProtoOASymbolByIdRes()
            res.ParseFromString(message.payload)
            sym = getattr(res, "symbol", None)
            if sym is not None and getattr(sym, "symbolId", None):
                _apply_symbol_spec(sym)
        except Exception as e:
            logger.error(f"SymbolByIdRes parse error: {e}")

    elif msg_type == ProtoOAGetTrendbarsRes().payloadType:
        res = ProtoOAGetTrendbarsRes()
        res.ParseFromString(message.payload)
        
        sym_name = symbol_id_to_name.get(res.symbolId, "UNKNOWN")
        tf = pending_requests_tf.get(res.symbolId, "H1")
        
        if len(res.trendbar) > 0:
            if sym_name not in trendbars_cache:
                trendbars_cache[sym_name] = {}
            trendbars_cache[sym_name][tf] = list(res.trendbar)
            last_bar = res.trendbar[-1]
            close_price = (last_bar.low + last_bar.deltaClose) / symbol_price_scale(sym_name)
            logger.info(f"Trendbar Received [{sym_name} - {tf}] - Bars: {len(res.trendbar)}, Latest Close: {close_price}")

    elif msg_type == ProtoOAReconcileRes().payloadType:
        res = ProtoOAReconcileRes()
        res.ParseFromString(message.payload)
        # ملاحظة: Reconcile لا يتضمن عادةً رصيد الحساب — الرصيد من ProtoOATraderRes.
        # إن وُجد حقل trader/account نادراً نقرأه كدعم إضافي.
        acc = getattr(res, "account", None) or getattr(res, "trader", None)
        if acc is not None:
            _apply_trader_account_info(acc)

        scale = _account_money_scale()
        active_positions.clear()
        total_upnl = 0.0
        total_used_margin = 0.0
        missing_prices = False
        for pos in res.position:
            sym_name = symbol_id_to_name.get(pos.tradeData.symbolId, f"ID_{pos.tradeData.symbolId}")
            # PnL غير متوفر في ReconcileRes — يُحسب من سعر الدخول مقابل أحدث سعر
            pnl_val = _compute_position_pnl(pos, sym_name)
            if pnl_val is None:
                pnl_val = 0.0
                missing_prices = True
            total_upnl += pnl_val
            # الهامش مستهلك لكل صفقة (usedMargin موجود في ProtoOAPosition)
            um_pos = getattr(pos, "usedMargin", None)
            try:
                total_used_margin += float(um_pos) / scale if um_pos is not None else 0.0
            except Exception:
                pass
            sl_v = getattr(pos, "stopLoss", None)
            tp_v = getattr(pos, "takeProfit", None)
            try:
                sl_f = float(sl_v) / symbol_price_scale(sym_name) if sl_v not in (None, 0, 0.0) else None
            except Exception:
                sl_f = None
            try:
                tp_f = float(tp_v) / symbol_price_scale(sym_name) if tp_v not in (None, 0, 0.0) else None
            except Exception:
                tp_f = None
            active_positions.append({
                "position_id": pos.positionId,
                "symbol": sym_name,
                "trade_type": "BUY" if pos.tradeData.tradeSide == ProtoOATradeSide.BUY else "SELL",
                # 1 lot = lotSize الرمز (FX رئيسية: 100,000 وحدة)
                "volume": _volume_units_to_lots(pos.tradeData.volume, sym_name),
                "entry_price": float(pos.price) / symbol_price_scale(sym_name),
                "pnl": pnl_val,
                "stop_loss": sl_f,
                "take_profit": tp_f,
            })
        # الهامش الإجمالي من الصفقات (ProtoOATrader لا يحتوي usedMargin)
        if total_used_margin > 0:
            ctrader_account_info["margin"] = round(total_used_margin, 2)
        # Equity تقريبي = Balance + Unrealized PnL
        bal = float(ctrader_account_info.get("balance") or 0.0)
        ctrader_account_info["equity"] = round(bal + total_upnl, 2)
        m = float(ctrader_account_info.get("margin") or 0)
        e = float(ctrader_account_info.get("equity") or 0)
        if m > 0:
            ctrader_account_info["free_margin"] = round(e - m, 2)
            ctrader_account_info["margin_level"] = round((e / m) * 100.0, 2)
        else:
            ctrader_account_info["free_margin"] = round(e, 2)
            ctrader_account_info["margin_level"] = 0.0
        ctrader_account_info["updated_at"] = datetime.now(timezone.utc)
        if missing_prices:
            _request_missing_position_prices()
        logger.info(
            f"Account Reconciled: Balance ${ctrader_account_info['balance']}, "
            f"Equity ${ctrader_account_info.get('equity')}, "
            f"Margin ${ctrader_account_info.get('margin')}, "
            f"Open Positions: {len(active_positions)}"
        )
        runtime_cache.set_mem("account", dict(ctrader_account_info), CTRADER_ACCOUNT_REFRESH_SECONDS * 3)
        runtime_cache.set_mem("positions", list(active_positions), CTRADER_ACCOUNT_REFRESH_SECONDS * 3)
        _notify_account_replied()
        try:
            schedule_position_journal_sync()
        except Exception as _je:
            logger.error(f"schedule_position_journal_sync: {_je}")

    elif msg_type == ProtoOAExecutionEvent().payloadType:
        try:
            ev = ProtoOAExecutionEvent()
            ev.ParseFromString(message.payload)
            et = getattr(ev, "executionType", None)
            logger.info(f"cTrader ExecutionEvent type={et} orderId={getattr(getattr(ev, 'order', None), 'orderId', None)}")
            # بعد أي تنفيذ نحدّث لقطة الحساب/الصفقات
            if ACCOUNT_ID:
                request_account_details()
        except Exception as e:
            logger.error(f"ExecutionEvent parse error: {e}")

    elif msg_type == ProtoOAOrderErrorEvent().payloadType:
        try:
            err = ProtoOAOrderErrorEvent()
            err.ParseFromString(message.payload)
            code = getattr(err, "errorCode", None)
            desc = getattr(err, "description", None)
            oid = getattr(err, "orderId", None)
            logger.error(f"cTrader OrderError: code={code} desc={desc} orderId={oid}")
            # إن وُجد سجل pending صدر للتو لنفس الأمر، ارفضه فوراً حتى لا يبقى
            # عالقاً فيُربط لاحقاً بشكل خاطئ بصفقة أخرى على نفس الزوج.
            try:
                schedule_reject_pending_journal(str(code or "broker_rejected"))
            except Exception:
                pass
            try:
                schedule_notify(
                    f"❌ **رفض الوسيط للأمر**\n"
                    f"code=`{code}`\n"
                    f"{desc or '—'}\n"
                    f"orderId=`{oid}`",
                    key=f"order_err_{oid or code}",
                    min_interval_sec=3,
                )
            except Exception:
                pass
        except Exception as e:
            logger.error(f"OrderErrorEvent parse error: {e}")

def _account_money_scale() -> float:

    digits = ctrader_account_info.get("money_digits")
    try:
        d = int(digits) if digits is not None else 2
        return float(10 ** d)
    except Exception:
        return 100.0


def _latest_price_for_symbol(symbol: str) -> Optional[float]:
    """أحدث سعر إغلاق معروف للرمز من شموع cTrader المخزنة (أحدث إطار زمني متوفر)."""
    try:
        tfs = trendbars_cache.get(symbol) or trendbars_cache.get(str(symbol).upper()) or {}
        best: Optional[float] = None
        best_ts: Optional[int] = None
        for tf, bars in tfs.items():
            if not bars:
                continue
            last_bar = bars[-1]
            try:
                close = (last_bar.low + last_bar.deltaClose) / symbol_price_scale(symbol)
            except Exception:
                continue
            ts = int(getattr(last_bar, "timestamp", 0) or 0)
            if best_ts is None or ts >= best_ts:
                best_ts = ts
                best = float(close)
        return best
    except Exception:
        return None


def _convert_pnl_to_account_currency(symbol: str, pnl_quote: float) -> float:
    """
    يحوّل ربح/خسارة عملة التسعير إلى عملة الحساب (يُفترض USD).
    - الأزواج المنتهية بـ USD (EURUSD, XAUUSD, BTCUSD): مباشر.
    - أزواج JPY (USDJPY): quote=JPY → نقسم على سعر USDJPY اللحظي.
    """
    try:
        sym = str(symbol or "").strip().upper()
        if not sym:
            return pnl_quote
        if sym.endswith("USD"):
            return pnl_quote
        if sym.endswith("JPY") and len(sym) >= 6:
            rate = _latest_price_for_symbol("USD" + sym[-3:])
            if rate and rate > 0:
                return pnl_quote / rate
        return pnl_quote
    except Exception:
        return pnl_quote


def _compute_position_pnl(pos, symbol: str) -> Optional[float]:
    """
    يحسب الربح/الخسارة العائمة لصفقة من سعر الدخول مقابل أحدث سعر للحساب.
    يعيد None إن لم يتوفر سعر لحظي للرمز بعد.
    """
    try:
        side = pos.tradeData.tradeSide
        direction = 1.0 if side == ProtoOATradeSide.BUY else -1.0
        scale = symbol_price_scale(symbol)
        entry = float(pos.price) / scale
        cur = _latest_price_for_symbol(symbol)
        if cur is None or cur <= 0:
            return None
        volume_units = float(pos.tradeData.volume)
        pnl_quote = (cur - entry) * direction * volume_units
        return _convert_pnl_to_account_currency(symbol, pnl_quote)
    except Exception:
        return None


def _request_missing_position_prices() -> None:
    """يطلب شموع لأي رمز ذي صفقة مفتوحة لم نحصل على سعره بعد (لحساب PnL)."""
    try:
        for pos in active_positions:
            sym = pos.get("symbol")
            if not sym or sym.startswith("ID_"):
                continue
            if _latest_price_for_symbol(sym) is None:
                request_symbol_trendbars(sym, "M1")
    except Exception as e:
        logger.error(f"_request_missing_position_prices: {e}")


def _apply_trader_account_info(trader) -> None:
    """يملأ ctrader_account_info من ProtoOATrader (أو كائن مشابه)."""
    digits = getattr(trader, "moneyDigits", None)
    if digits is not None:
        try:
            ctrader_account_info["money_digits"] = int(digits)
        except Exception:
            pass
    scale = _account_money_scale()
    bal = getattr(trader, "balance", None)
    if bal is not None:
        try:
            ctrader_account_info["balance"] = float(bal) / scale
        except Exception:
            pass
    # ملاحظة: ProtoOATrader في ctrader-open-api 0.9.2 لا يحتوي على usedMargin/freeMargin.
    # الهامش يُجمَّع من حقل usedMargin الخاص بكل صفقة في ProtoOAReconcileRes.
    lev = getattr(trader, "leverageInCents", None)
    if lev is None:
        lev = getattr(trader, "leverage", None)
    if lev is not None:
        try:
            li = int(lev)
            # leverageInCents: 50000 → 500
            ctrader_account_info["leverage"] = (li / 100) if li > 1000 else li
        except Exception:
            ctrader_account_info["leverage"] = lev
    # equity أولي = balance إلى أن يُحدَّث من Reconcile + PnL
    if ctrader_account_info.get("balance") is not None:
        if not ctrader_account_info.get("equity"):
            ctrader_account_info["equity"] = float(ctrader_account_info["balance"])
    ctrader_account_info["updated_at"] = datetime.now(timezone.utc)


def request_account_details():
    """يطلب بيانات الحساب الحقيقية (Trader) + الصفقات المفتوحة (Reconcile)."""
    if not (is_ctrader_connected and ACCOUNT_ID):
        return
    try:
        trader_req = ProtoOATraderReq()
        trader_req.ctidTraderAccountId = ACCOUNT_ID
        ctrader_client.send(trader_req)
    except Exception as e:
        logger.error(f"request_account_details TraderReq error: {e}")
    try:
        req = ProtoOAReconcileReq()
        req.ctidTraderAccountId = ACCOUNT_ID
        ctrader_client.send(req)
    except Exception as e:
        logger.error(f"request_account_details ReconcileReq error: {e}")

def request_symbol_trendbars(symbol_name: str, timeframe: str = "H1"):
    if not is_ctrader_connected or symbol_name not in symbol_id_map:
        return
    
    symbol_id = symbol_id_map[symbol_name]
    pending_requests_tf[symbol_id] = timeframe
    
    period_map = {
        "M1": ProtoOATrendbarPeriod.M1,
        "M15": ProtoOATrendbarPeriod.M15,
        "H1": ProtoOATrendbarPeriod.H1,
        "H4": ProtoOATrendbarPeriod.H4,
        "D1": ProtoOATrendbarPeriod.D1,
    }

    # نافذة زمنية أقصر لشموع الدقيقة لتجنّب حدود الوسيط
    lookback_days = 2 if timeframe == "M1" else 30

    req = ProtoOAGetTrendbarsReq()
    req.ctidTraderAccountId = ACCOUNT_ID
    req.symbolId = symbol_id
    req.period = period_map.get(timeframe, ProtoOATrendbarPeriod.H1)
    req.fromTimestamp = int((time.time() - 86400 * lookback_days) * 1000)
    req.toTimestamp = int(time.time() * 1000)

    ctrader_client.send(req)


async def ensure_trendbars_ready(symbol: str, tf: str, wait_seconds: float = 4.0) -> List[Any]:
    """
    يرجع الشموع المخزّنة محلياً لهذا الزوج/الإطار؛ وإن لم تكن متوفرة بعد،
    يطلبها من cTrader وينتظر قليلاً قبل إعادة المحاولة، ثم يخزّن OHLC في قاعدة البيانات.
    """
    bars = trendbars_cache.get(symbol, {}).get(tf, [])
    if not bars:
        request_symbol_trendbars(symbol, tf)
        await asyncio.sleep(wait_seconds)
        bars = trendbars_cache.get(symbol, {}).get(tf, [])
    if bars:
        try:
            await persist_ohlc_from_ctrader_bars(symbol, tf, bars)
        except Exception as e:
            logger.error(f"persist OHLC error [{symbol} {tf}]: {e}")
    return bars


# ==================== تنفيذ أوامر التداول عبر cTrader ====================

def _lots_to_volume_units(lots: float, symbol: Optional[str] = None) -> int:
    """
    تحويل اللوت إلى وحدات حجم cTrader Open API لكل رمز.
    1.0 lot = lotSize الرمز (FX رئيسية: 100,000؛ الرموز الأخرى قد تختلف).
    أقل وحدة مسموحة = minVolume الرمز (غالباً 1,000 = 0.01 لوت).
    """
    try:
        lot_size = symbol_lot_size(symbol) if symbol else 100_000
        min_vol = symbol_min_volume(symbol) if symbol else 1_000
        v = int(round(float(lots) * lot_size))
        return max(v, min_vol)
    except Exception:
        return max(int(round(float(lots) * 100_000)), 1_000)


def _volume_units_to_lots(volume_units: Any, symbol: Optional[str] = None) -> float:
    try:
        lot_size = symbol_lot_size(symbol) if symbol else 100_000
        return float(volume_units) / float(lot_size)
    except Exception:
        return 0.0


def _price_distance_to_relative(distance: float, symbol: Optional[str] = None) -> int:
    """
    relativeStopLoss / relativeTakeProfit في cTrader =
    المسافة السعرية × 10^digits للرمز (1/100000 لوحدة السعر للـ FX الرئيسية).
    """
    try:
        scale = symbol_price_scale(symbol) if symbol else 100000.0
        d = abs(float(distance))
        if d <= 0:
            return 0
        return max(1, int(round(d * scale)))
    except Exception:
        return 0


def place_market_order(
    symbol: str,
    side: str,
    lots: float = DEFAULT_TRADE_LOT,
    stop_loss: Optional[float] = None,
    take_profit: Optional[float] = None,
    entry_price: Optional[float] = None,
) -> Dict[str, Any]:
    """
    إرسال أمر سوق فوري (MARKET).
    ملاحظة cTrader: stopLoss/takeProfit المطلقة غير مدعومة لـ MARKET —
    نستخدم relativeStopLoss / relativeTakeProfit.
    side: BUY أو SELL
    """
    if not is_ctrader_connected:
        return {"ok": False, "error": "cTrader غير متصل"}
    if symbol not in symbol_id_map:
        return {"ok": False, "error": f"رمز غير معروف لدى الوسيط: {symbol}"}
    if not ACCOUNT_ID:
        return {"ok": False, "error": "ACCOUNT_ID غير مضبوط"}

    side_u = (side or "").upper().strip()
    if side_u not in ("BUY", "SELL"):
        return {"ok": False, "error": f"اتجاه غير صالح: {side}"}

    # سعر مرجعي لحساب المسافة النسبية إن وُجد SL/TP مطلق
    ref_price = entry_price
    if ref_price is None:
        try:
            scale = symbol_price_scale(symbol)
            for tf in ("M1", "M15", "H1"):
                bars = trendbars_cache.get(symbol, {}).get(tf) or []
                if bars:
                    ind = calculate_technical_indicators(bars, symbol) if len(bars) >= 50 else None
                    if ind and ind.get("last_price"):
                        ref_price = float(ind["last_price"])
                        break
                    b = bars[-1]
                    ref_price = (b.low + b.deltaClose) / scale
                    break
        except Exception:
            ref_price = None

    # حماية: تأكد أن مسافة SL/TP ليست أصغر من الحد الأدنى للوسيط (slDistance/tpDistance)
    # لتفادي رفض الأمر بأخطاء TRADING_BAD_STOPS / PROTECTION_IS_TOO_CLOSE_TO_MARKET.
    def _enforce_min_distance(price_delta: Optional[float]) -> Optional[float]:
        if price_delta is None or ref_price is None:
            return price_delta
        try:
            spec = symbol_specs_cache.get(symbol) or symbol_specs_cache.get(str(symbol).upper())
            if not spec:
                return price_delta
            scale = symbol_price_scale(symbol)
            min_rel = 0
            if stop_loss is not None and (spec.get("sl_distance") or 0) > 0:
                min_rel = int(spec["sl_distance"])
            if min_rel <= 0:
                return price_delta
            min_abs = min_rel / scale
            delta = float(price_delta)
            if abs(delta) < min_abs:
                # وسّع المسافة للحد الأدنى مع الحفاظ على الاتجاه (افتراضياً حماية)
                sign = -1.0 if stop_loss is not None and ref_price is not None and float(ref_price or 0) >= float(stop_loss or 0) else 1.0
                return math.copysign(min_abs, delta if delta != 0 else sign)
        except Exception:
            pass
        return price_delta

    try:
        req = ProtoOANewOrderReq()
        req.ctidTraderAccountId = ACCOUNT_ID
        req.symbolId = symbol_id_map[symbol]
        req.orderType = ProtoOAOrderType.MARKET
        req.tradeSide = ProtoOATradeSide.BUY if side_u == "BUY" else ProtoOATradeSide.SELL
        req.volume = _lots_to_volume_units(lots, symbol)
        # MARKET غالباً يعمل بدون TIF؛ IOC قد يرفض عند نقص السيولة
        try:
            req.timeInForce = ProtoOATimeInForce.GOOD_TILL_CANCEL
        except Exception:
            pass

        # SL/TP: نسبية فقط لأوامر MARKET (التوثيق الرسمي)
        if stop_loss is not None and ref_price is not None:
            sl_delta = _enforce_min_distance(float(ref_price) - float(stop_loss))
            rel_sl = _price_distance_to_relative(sl_delta, symbol) if sl_delta is not None else 0
            if rel_sl > 0:
                try:
                    req.relativeStopLoss = rel_sl
                except Exception:
                    pass
        if take_profit is not None and ref_price is not None:
            tp_delta = float(take_profit) - float(ref_price)
            rel_tp = _price_distance_to_relative(tp_delta, symbol)
            if rel_tp > 0:
                try:
                    req.relativeTakeProfit = rel_tp
                except Exception:
                    pass

        ctrader_client.send(req)
        logger.info(
            f"📤 NewOrder MARKET {side_u} {symbol} lots={lots} "
            f"SL={stop_loss} TP={take_profit} ref={ref_price} "
            f"volume_units={req.volume} "
            f"relSL={getattr(req, 'relativeStopLoss', None)} "
            f"relTP={getattr(req, 'relativeTakeProfit', None)}"
        )
        return {
            "ok": True,
            "pending_broker": True,  # الإرسال نجح — التنفيذ يعتمد على رد الوسيط
            "symbol": symbol,
            "side": side_u,
            "lots": lots,
            "stop_loss": stop_loss,
            "take_profit": take_profit,
            "volume_units": req.volume,
        }
    except Exception as e:
        logger.error(f"place_market_order error [{symbol}]: {e}")
        return {"ok": False, "error": str(e)}


def close_position_by_id(position_id: int, volume_lots: float, symbol: Optional[str] = None) -> Dict[str, Any]:
    """إغلاق صفقة مفتوحة بالكامل (أو بالحجم المحدد)."""
    if not is_ctrader_connected or not ACCOUNT_ID:
        return {"ok": False, "error": "cTrader غير متصل أو ACCOUNT_ID مفقود"}
    try:
        req = ProtoOAClosePositionReq()
        req.ctidTraderAccountId = ACCOUNT_ID
        req.positionId = int(position_id)
        req.volume = _lots_to_volume_units(volume_lots, symbol)
        ctrader_client.send(req)
        logger.info(f"📤 ClosePosition id={position_id} volume_lots={volume_lots}")
        return {"ok": True, "position_id": position_id}
    except Exception as e:
        logger.error(f"close_position_by_id error: {e}")
        return {"ok": False, "error": str(e)}


def close_all_open_positions() -> List[Dict[str, Any]]:
    """إغلاق كل الصفقات المفتوحة معاً."""
    results = []
    positions_snapshot = list(active_positions)
    for pos in positions_snapshot:
        pid = pos.get("position_id")
        vol = float(pos.get("volume") or DEFAULT_TRADE_LOT)
        if pid is None:
            results.append({"ok": False, "symbol": pos.get("symbol"), "error": "لا يوجد position_id"})
            continue
        r = close_position_by_id(int(pid), vol, symbol=pos.get("symbol"))
        r["symbol"] = pos.get("symbol")
        results.append(r)
    return results


def _parse_float_safe(val: Any) -> Optional[float]:
    if val is None:
        return None
    try:
        s = str(val).strip().replace(",", "")
        if not s or s.lower() in ("none", "null", "-", "n/a"):
            return None
        return float(s)
    except Exception:
        return None


def normalize_trade_direction(direction: Any, emoji: Any = None) -> Optional[str]:
    """
    يحوّل نص الاتجاه المخزَّن (عربي/إنجليزي/مختلط) إلى BUY أو SELL فقط.
    أمثلة مقبولة: BUY, SELL, شراء, بيع, شراء (BUY), بيع (SELL), LONG, SHORT,
    وأي نص يحتوي BUY/SELL أو 🟢/🔴.
    يعيد None للحياد أو القيم غير القابلة للتنفيذ.
    """
    raw = (str(direction) if direction is not None else "").strip()
    em = (str(emoji) if emoji is not None else "").strip()
    combined = f"{raw} {em}".upper()

    # حياد صريح
    neutral_markers = (
        "NEUTRAL", "HOLD", "NONE", "FLAT", "WAIT",
        "لا توجد إشارة", "محايد", "حياد", "انتظار",
    )
    # إذا كان النص حياداً واضحاً بدون BUY/SELL
    is_neutral = any(m.upper() in combined or m in raw for m in neutral_markers)
    has_buy = (
        "BUY" in combined
        or "LONG" in combined
        or "شراء" in raw
        or "🟢" in raw
        or em == "🟢"
    )
    has_sell = (
        "SELL" in combined
        or "SHORT" in combined
        or "بيع" in raw
        or "🔴" in raw
        or em == "🔴"
    )

    if has_buy and not has_sell:
        return "BUY"
    if has_sell and not has_buy:
        return "SELL"
    # إن وُجد الاثنان في نفس النص (نادر) نفضّل الرمز التعبيري
    if em == "🟢":
        return "BUY"
    if em == "🔴":
        return "SELL"
    if is_neutral or (not has_buy and not has_sell):
        return None
    return None


def compute_dynamic_lot(
    symbol: str,
    sig: Dict[str, Any],
    sl: Optional[float],
    entry_price: Optional[float],
) -> float:
    """
    حجم ديناميكي للصفقة:
      - أساس: DEFAULT_TRADE_LOT
      - size_hint=='reduce' → نصف الأساسي
      - size_hint=='skip' → 0 (لا تنفيذ)
      - إن توفّر SL + ATR: حجم بحيث لا تتجاوز الخسارة المحتملة
        نسبة TRADE_RISK_PERCENT من الحساب (افتراضي 1%)، مقرّب لخطوات الرمز.
    """
    try:
        hint = str(sig.get("size_hint") or "normal").strip().lower()
        if hint == "skip":
            return 0.0
        base = DEFAULT_TRADE_LOT
        if hint == "reduce":
            base = base / 2.0

        risk_pct = float(os.getenv("TRADE_RISK_PERCENT", "1.0"))
        if risk_pct <= 0 or sl is None or entry_price is None:
            return max(base, DEFAULT_TRADE_LOT / 2.0)
        risk_amount = abs(float(sl) - float(entry_price))
        if risk_amount <= 0:
            return max(base, DEFAULT_TRADE_LOT / 2.0)
        balance = float(ctrader_account_info.get("balance") or 0)
        if balance <= 0:
            return max(base, DEFAULT_TRADE_LOT / 2.0)
        budget = balance * (risk_pct / 100.0)
        # القيمة الاسمية لكل وحدة لوت تقريباً = lotSize × سعر الدخول
        notional_per_lot = symbol_lot_size(symbol) * float(entry_price)
        if notional_per_lot <= 0:
            return max(base, DEFAULT_TRADE_LOT / 2.0)
        # الخسارة لكل 1.0 لوت ≈ (risk_amount / entry_price) × notional_per_lot
        loss_per_lot = (risk_amount / float(entry_price)) * notional_per_lot
        if loss_per_lot <= 0:
            return max(base, DEFAULT_TRADE_LOT / 2.0)
        risk_lots = budget / loss_per_lot
        # لا نتعدى 2× الأساسي أبدًا (حماية)
        capped = min(risk_lots, base * 2.0, float(os.getenv("TRADE_MAX_LOT", "0.1")))
        final = max(DEFAULT_TRADE_LOT / 2.0, min(base, capped))
        # قرّب لخطوات minVolume
        try:
            step_lots = symbol_min_volume(symbol) / symbol_lot_size(symbol)
            if step_lots > 0:
                final = max(step_lots, round(final / step_lots) * step_lots)
        except Exception:
            pass
        return float(final)
    except Exception as e:
        logger.error(f"compute_dynamic_lot error [{symbol}]: {e}")
        return DEFAULT_TRADE_LOT


async def execute_signals_professionally(
    symbols: List[str],
    timeframe: Optional[str] = None,
    chat_id: Optional[int] = None,
) -> Dict[str, Any]:
    """
    يقرأ آخر إشارات مخزّنة وينفّذ أوامر سوق:
    - يفضّل إشارات إطار التنفيذ (trading_tf) عند توفرها
    - يتخطى NEUTRAL / غياب الإشارة
    - لا يفتح صفقة مكررة على نفس الزوج
    """
    if not auto_trading_enabled:
        return {"ok": False, "error": "التداول الآلي متوقف — اضغط «بدء التداول» أولاً"}
    if not is_ctrader_connected:
        return {"ok": False, "error": "cTrader غير متصل"}

    trade_tf = timeframe
    if not trade_tf and chat_id is not None:
        trade_tf = user_trading_tf.get(chat_id)
    if not trade_tf:
        trade_tf = next(iter(user_trading_tf.values()), None) if user_trading_tf else None
    trade_tf = (trade_tf or "H1").upper()
    signals = await db.get_symbol_signals(symbols, timeframe=trade_tf)

    open_syms = {p.get("symbol") for p in active_positions}
    executed, skipped, failed = [], [], []

    min_q = int(os.getenv("PRED_MIN_QUALITY", "52"))
    require_quality = os.getenv("TRADE_REQUIRE_QUALITY", "1").strip() not in ("0", "false", "False")

    for sym in symbols:
        sig = signals.get(sym) if signals else None
        if not sig:
            skipped.append({"symbol": sym, "reason": "لا توجد إشارة مخزّنة"})
            continue
        direction = normalize_trade_direction(sig.get("direction"), sig.get("emoji"))
        if direction not in ("BUY", "SELL"):
            skipped.append({
                "symbol": sym,
                "reason": f"اتجاه غير قابل للتنفيذ (حياد/غير معروف): {sig.get('direction')}",
            })
            continue
        if sym in open_syms:
            skipped.append({"symbol": sym, "reason": "صفقة مفتوحة مسبقاً على نفس الزوج"})
            continue

        # دمج مع الشاملة: رفض التنفيذ عند تعارض الاتجاه
        if signals_merge_full and direction in ("BUY", "SELL"):
            try:
                max_age = int(os.getenv("SIGNALS_MERGE_MAX_AGE_MINUTES", "90"))
                full_map = await db.get_latest_full_signals([sym], max_age_minutes=max_age)
                full = full_map.get(sym)
                if full:
                    fdir = _norm_dir_simple(full.get("direction"), full.get("emoji"))
                    if fdir in ("BUY", "SELL") and fdir != direction:
                        skipped.append({
                            "symbol": sym,
                            "reason": (
                                f"دمج: تعارض مع الشاملة ({fdir}) "
                                f"مقابل إشارة الإطار ({direction})"
                            ),
                        })
                        continue
            except Exception as me:
                logger.error(f"merge check exec [{sym}]: {me}")

        # فلتر جودة عند التنفيذ: إعادة تقييم من مؤشرات حية إن توفرت
        if require_quality:

            try:
                bars = trendbars_cache.get(sym, {}).get(trade_tf) or []
                live_ind = calculate_technical_indicators(bars, sym) if bars and len(bars) >= 50 else None
                if live_ind and "error" not in live_ind:
                    # multi-tf من الكاش
                    by_tf = trendbars_cache.get(sym) or {}
                    all_ok = {}
                    for tf_k, bars_k in by_tf.items():
                        if bars_k and len(bars_k) >= 50:
                            ind_k = calculate_technical_indicators(bars_k, sym)
                            if ind_k and "error" not in ind_k:
                                all_ok[tf_k] = ind_k
                    mtf = compute_multi_tf_confluence(all_ok) if all_ok else {}
                    gated = signal_from_indicators(live_ind, trade_tf, mtf=mtf, apply_quality=True, symbol=sym)
                    if not gated.get("quality_pass") or gated.get("direction") != direction:
                        skipped.append({
                            "symbol": sym,
                            "reason": (
                                f"فلتر جودة رفض التنفيذ "
                                f"(جودة={gated.get('quality_score')}, "
                                f"اتجاه حي={gated.get('direction')}, مخزن={direction})"
                            ),
                        })
                        continue
                    # حدّث SL/TP من الإشارة المفلترة الحية
                    if gated.get("sl") is not None:
                        sig = dict(sig)
                        sig["sl"] = gated.get("sl")
                        sig["tp1"] = gated.get("tp1")
                        sig["tp2"] = gated.get("tp2")
                        sig["entry"] = gated.get("entry")
                else:
                    # بدون مؤشرات حية: ارفض إذا confluence المخزن ضعيف جداً (إن وُجد في السبب/الحقول)
                    pass
            except Exception as qe:
                logger.error(f"quality gate error [{sym}]: {qe}")

        sl = _parse_float_safe(sig.get("sl"))
        tp = _parse_float_safe(sig.get("tp1")) or _parse_float_safe(sig.get("tp2"))
        entry_px = _parse_float_safe(sig.get("entry"))
        lot_size = compute_dynamic_lot(sym, sig, sl, entry_px)
        if lot_size <= 0:
            skipped.append({"symbol": sym, "reason": "size_hint=skip (لا تنفيذ)"})
            continue
        result = place_market_order(
            sym, direction, lot_size,
            stop_loss=sl, take_profit=tp, entry_price=entry_px,
        )
        result["lot"] = lot_size
        if result.get("ok"):
            executed.append(result)
            try:
                jid = await db.insert_trade_journal(
                    symbol=sym,
                    direction=direction,
                    timeframe=trade_tf,
                    lots=lot_size,
                    entry_price=entry_px,
                    sl=sl,
                    tp1=_parse_float_safe(sig.get("tp1")),
                    tp2=_parse_float_safe(sig.get("tp2")),
                    status="pending",
                    notes="opened_via_auto_trade",
                )
                result["journal_id"] = jid
                remember_order_journal(jid, sym, direction)
                await notify_user(
                    f"📤 **أمر سوق أُرسل للوسيط** `{sym}` {direction}\n"
                    f"لوت=`{lot_size}` vol=`{result.get('volume_units')}`\n"
                    f"SL=`{sl}` TP=`{tp}`\n"
                    f"إطار=`{trade_tf}` journal=`{jid}`\n"
                    f"_بانتظار تأكيد التنفيذ أو رفض الوسيط..._",
                    key=f"order_{sym}",
                    min_interval_sec=2,
                )
            except Exception as je:
                logger.error(f"journal insert after order: {je}")
        else:
            failed.append({"symbol": sym, **result})
        await asyncio.sleep(0.4)

    # تحديث لقطة الصفقات بعد إرسال الأوامر
    if executed:
        await request_account_details_and_wait(timeout=4.0)

    return {
        "ok": True,
        "executed": executed,
        "skipped": skipped,
        "failed": failed,
        "lot": DEFAULT_TRADE_LOT,
        "lots_used": [float(r.get("lot") or DEFAULT_TRADE_LOT) for r in executed],
    }




async def notify_user(text: str, key: Optional[str] = None, min_interval_sec: float = 30.0) -> None:
    """إشعار تيليجرام للمالك مع حدّ تكرار بسيط."""
    global _last_notify_ts
    if not MY_TELEGRAM_CHAT_ID or not telegram_app:
        return
    if key:
        now = time.time()
        last = float(_last_notify_ts.get(key) or 0)
        if now - last < min_interval_sec:
            return
        _last_notify_ts[key] = now
    try:
        await safe_send_message(telegram_app.bot, int(MY_TELEGRAM_CHAT_ID), text[:3900])
    except Exception as e:
        logger.error(f"notify_user error: {e}")


def schedule_notify(text: str, key: Optional[str] = None, min_interval_sec: float = 30.0) -> None:
    """جدولة إشعار من خيط cTrader (sync) إلى حلقة asyncio."""
    try:
        loop = asyncio.get_event_loop()
        if loop.is_running():
            asyncio.ensure_future(notify_user(text, key=key, min_interval_sec=min_interval_sec))
        else:
            loop.run_until_complete(notify_user(text, key=key, min_interval_sec=min_interval_sec))
    except Exception:
        try:
            asyncio.get_event_loop().create_task(
                notify_user(text, key=key, min_interval_sec=min_interval_sec)
            )
        except Exception as e:
            logger.error(f"schedule_notify error: {e}")


# تتبع سجلات pending صدرت للوسيط للتو (FIFO) لربط الرفض بالسجل الصحيح
_pending_journal_fifo: List[Dict[str, Any]] = []


def remember_order_journal(journal_id: Any, symbol: str, direction: str = "") -> None:
    """يسجّل سجل صفقة صدر للوسيط للتو (يُستدعى بعد نجاح إرسال الأمر)."""
    try:
        _pending_journal_fifo.append(
            {
                "journal_id": journal_id,
                "symbol": symbol,
                "direction": direction,
                "ts": time.time(),
            }
        )
        # حد أقصى لتفادي النمو غير المحدود
        if len(_pending_journal_fifo) > 40:
            del _pending_journal_fifo[: len(_pending_journal_fifo) - 20]
    except Exception:
        pass


def schedule_reject_pending_journal(reason: str, max_age_seconds: float = 60.0) -> None:
    """
    يرفض أقدم سجل pending صدر خلال آخر max_age_seconds (الوسيط رفض الأمر).
    الأوامر تُرسل تباعاً فيُفترض أن الأقدم هو المقصود. آلية احتياطية:
    التنظيف الدوري reject_stale_pending_journals يمسك أي شيء فات.
    """
    async def _do_reject():
        try:
            now = time.time()
            target = None
            while _pending_journal_fifo:
                cand = _pending_journal_fifo[0]
                if now - float(cand.get("ts") or 0) > max_age_seconds:
                    # قديم جداً — تخلّص منه دون رفض (من المفترض تأكد تنفيذه)
                    _pending_journal_fifo.pop(0)
                    continue
                target = _pending_journal_fifo.pop(0)
                break
            if target is None:
                return
            jid = target.get("journal_id")
            await db.mark_journal_rejected(jid, reason=reason)
            logger.info(
                f"🗑️ سجل {jid} [{target.get('symbol')}] مُعلّم كـ rejected (reason={reason})"
            )
        except Exception as e:
            logger.error(f"reject pending journal error: {e}")

    try:
        loop = asyncio.get_event_loop()
        if loop.is_running():
            asyncio.ensure_future(_do_reject())
        else:
            loop.run_until_complete(_do_reject())
    except Exception as e:
        logger.error(f"schedule_reject_pending_journal error: {e}")


def forget_order_journal(journal_id: Any) -> None:
    """يحذف سجلاً من FIFO بعد تأكد تنفيذه (match مع position فعلية)."""
    try:
        if journal_id is None:
            return
        _pending_journal_fifo[:] = [
            x for x in _pending_journal_fifo if x.get("journal_id") != journal_id
        ]
    except Exception:
        pass


def amend_position_sl_tp(
    position_id: int,
    stop_loss: Optional[float] = None,
    take_profit: Optional[float] = None,
) -> Dict[str, Any]:
    """تعديل SL/TP لصفقة مفتوحة (نقطة التعادل / trailing)."""
    if not is_ctrader_connected or not ACCOUNT_ID:
        return {"ok": False, "error": "cTrader غير متصل"}
    try:
        # الاسم القياسي في Open API
        ReqCls = globals().get("ProtoOAAmendPositionSLTPReq")
        if ReqCls is None:
            return {"ok": False, "error": "ProtoOAAmendPositionSLTPReq غير متاح في المكتبة"}
        req = ReqCls()
        req.ctidTraderAccountId = ACCOUNT_ID
        req.positionId = int(position_id)
        if stop_loss is not None:
            req.stopLoss = float(stop_loss)
        if take_profit is not None:
            req.takeProfit = float(take_profit)
        ctrader_client.send(req)
        logger.info(f"📤 AmendPositionSLTP id={position_id} SL={stop_loss} TP={take_profit}")
        return {"ok": True, "position_id": position_id, "stop_loss": stop_loss, "take_profit": take_profit}
    except Exception as e:
        logger.error(f"amend_position_sl_tp error: {e}")
        return {"ok": False, "error": str(e)}




def schedule_position_journal_sync() -> None:
    """من خيط cTrader: مزامنة السجل + إغلاق الصفقات المختفية."""
    try:
        loop = asyncio.get_event_loop()
        if loop.is_running():
            asyncio.ensure_future(sync_trade_journal_with_positions())
        else:
            loop.run_until_complete(sync_trade_journal_with_positions())
    except Exception as e:
        logger.error(f"schedule_position_journal_sync error: {e}")


async def sync_trade_journal_with_positions() -> None:
    """
    - يربط pending في السجل بالصفقات الحية
    - يغلق سجلات open اختفت من reconcile
    - يرسل إشعار إغلاق
    """
    global _known_position_ids
    positions = list(active_positions)
    current_ids = set()
    for pos in positions:
        try:
            pid = int(pos["position_id"])
        except Exception:
            continue
        current_ids.add(pid)
        # ربط سجل pending
        try:
            matched = await db.match_pending_journal_to_position(
                pos.get("symbol") or "",
                pos.get("trade_type") or "",
                pid,
                entry_price=float(pos["entry_price"]) if pos.get("entry_price") is not None else None,
            )
            if matched is not None:
                forget_order_journal(matched)
        except Exception as e:
            logger.error(f"match journal: {e}")

    # اكتشاف إغلاق
    closed_ids = _known_position_ids - current_ids
    for pid in closed_ids:
        try:
            # ابحث آخر PnL معروف من كاش سابق إن أمكن
            closed = await db.close_journal_trade(
                position_id=pid,
                status="closed",
                notes="detected_via_reconcile",
            )
            if closed:
                pnl = closed.get("realized_pnl")
                sym = closed.get("symbol")
                direction = closed.get("direction")
                pnl_s = f"{pnl:+.2f}" if pnl is not None else "—"
                await notify_user(
                    f"📕 **أُغلقت صفقة** `{sym}` {direction}\n"
                    f"position_id=`{pid}` · PnL≈`{pnl_s}`\n"
                    f"status=`{closed.get('status')}`",
                    key=f"close_{pid}",
                    min_interval_sec=5,
                )
        except Exception as e:
            logger.error(f"close journal {pid}: {e}")

    # اكتشاف فتح جديد
    new_ids = current_ids - _known_position_ids
    for pos in positions:
        try:
            pid = int(pos["position_id"])
        except Exception:
            continue
        if pid in new_ids:
            await notify_user(
                f"📗 **فُتحت صفقة** `{pos.get('symbol')}` {pos.get('trade_type')}\n"
                f"دخول=`{pos.get('entry_price')}` · حجم=`{pos.get('volume')}`\n"
                f"position_id=`{pid}`",
                key=f"open_{pid}",
                min_interval_sec=5,
            )

    _known_position_ids = current_ids

    # تنظيف سجلات pending عالقة (الوسيط لم ينفذها أو رفضها بصمت)
    try:
        await db.reject_stale_pending_journals(max_age_seconds=300)
    except Exception as e:
        logger.error(f"stale pending cleanup: {e}")


async def circuit_breaker_check() -> bool:
    """
    قاطع دائرة وقائي: يوقف التداول الآلي عند:
      - N خسارات متتالية (TRADE_CIRCUIT_MAX_CONSEC_LOSSES، افتراضي 5)
      - أو انخفاض مجمع اليوم عن نسبة (TRADE_CIRCUIT_MAX_DAILY_LOSS_PCT، افتراضي 8%)
    يُعين auto_trading_enabled=False ويُخطر المالك. يعيد True إن فُعّل القاطع.
    """
    global auto_trading_enabled
    if not auto_trading_enabled:
        return False
    max_losses = int(os.getenv("TRADE_CIRCUIT_MAX_CONSEC_LOSSES", "5"))
    max_daily_pct = float(os.getenv("TRADE_CIRCUIT_MAX_DAILY_LOSS_PCT", "8"))
    if max_losses <= 0 and max_daily_pct <= 0:
        return False
    try:
        closed = await db.get_recent_closed_trades(limit=30)
        if not closed:
            return False
        # أحدث النتائج أولاً (get_recent_closed ترجع مرتبة تنازلياً)
        reasons = []
        # نتجاهل السجلات المرفوضة (لم تُنفّذ أصلاً)
        executed_only = [t for t in closed if str(t.get("status", "")).startswith("closed")]
        # 1) خسارات متتالية
        if max_losses > 0:
            consec = 0
            for t in executed_only:
                pnl = float(t.get("realized_pnl") or 0)
                if pnl < 0:
                    consec += 1
                else:
                    break
            if consec >= max_losses:
                reasons.append(f"{consec} خسارة متتالية (الحد {max_losses})")
        # 2) خسارة اليوم المجّمة
        if max_daily_pct > 0:
            try:
                day_start = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
                day_pnl = sum(
                    float(t.get("realized_pnl") or 0)
                    for t in executed_only
                    if t.get("closed_at") and _parse_dt_utc(t["closed_at"]) and _parse_dt_utc(t["closed_at"]) >= day_start
                )
                balance = float(ctrader_account_info.get("balance") or 0)
                if balance > 0 and day_pnl < 0 and (abs(day_pnl) / balance * 100.0) >= max_daily_pct:
                    reasons.append(
                        f"خسارة يومية {abs(day_pnl) / balance * 100.0:.1f}% (الحد {max_daily_pct}%)"
                    )
            except Exception:
                pass
        if not reasons:
            return False
        auto_trading_enabled = False
        await persist_runtime_state_from_memory(auto_trading_chat_id)
        logger.warning(f"🛑 قاطع الدائرة فُعّل: {' | '.join(reasons)}")
        await notify_user(
            f"🛑 **قاطع الدائرة: التداول الآلي متوقف**\n"
            f"السبب: {' | '.join(reasons)}\n"
            f"أعد التفعيل يدوياً من زر «بدء التداول» بعد المراجعة.",
            key="circuit_breaker",
            min_interval_sec=900,
        )
        return True
    except Exception as e:
        logger.error(f"circuit_breaker_check error: {e}")
        return False


def _parse_dt_utc(val: Any) -> Optional[datetime]:
    """يحوّل قيمة تاريخ (ISO أو datetime) إلى datetime UTC أو None."""
    if val is None:
        return None
    try:
        if isinstance(val, datetime):
            dt = val
        else:
            dt = datetime.fromisoformat(str(val).replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


async def manage_breakeven_and_trailing() -> None:
    """
    بعد وصول السعر قرب TP1: نقل SL إلى نقطة التعادل (الدخول).
    يعمل على سجلات journal المفتوحة المرتبطة بـ position_id.
    """
    if not is_ctrader_connected:
        return
    enable_be = os.getenv("TRADE_BE_AFTER_TP1", "1").strip() not in ("0", "false", "False")
    if not enable_be:
        return
    be_buffer_atr = float(os.getenv("TRADE_BE_BUFFER_ATR", "0.05"))  # هامش صغير فوق/تحت الدخول
    journals = await db.get_open_journal_trades()
    pos_by_id = {}
    for p in active_positions:
        try:
            pos_by_id[int(p["position_id"])] = p
        except Exception:
            continue

    for j in journals:
        if j.get("be_moved"):
            continue
        pid = j.get("position_id")
        if not pid:
            continue
        pos = pos_by_id.get(int(pid))
        if not pos:
            continue
        entry = j.get("entry_price") or pos.get("entry_price")
        tp1 = j.get("tp1")
        direction = (j.get("direction") or pos.get("trade_type") or "").upper()
        if entry is None or tp1 is None:
            continue
        try:
            entry_f = float(entry)
            tp1_f = float(tp1)
        except Exception:
            continue

        # سعر حي تقريبي من آخر شمعة H1/M1
        sym = j.get("symbol") or pos.get("symbol")
        last_price = None
        atr = 0.0
        for tf in ("M1", "M15", "H1"):
            bars = trendbars_cache.get(sym, {}).get(tf) or []
            if bars:
                try:
                    ind = calculate_technical_indicators(bars, sym) if len(bars) >= 50 else None
                    if ind and ind.get("last_price"):
                        last_price = float(ind["last_price"])
                        atr = float(ind.get("atr_14") or 0)
                        break
                except Exception:
                    try:
                        b = bars[-1]
                        last_price = (b.low + b.deltaClose) / symbol_price_scale(sym)
                        atr = abs(last_price) * 0.0005
                    except Exception:
                        pass
        if last_price is None:
            continue

        hit_tp1 = False
        if "BUY" in direction and last_price >= tp1_f:
            hit_tp1 = True
            new_sl = entry_f + (atr * be_buffer_atr if atr else 0)
        elif "SELL" in direction and last_price <= tp1_f:
            hit_tp1 = True
            new_sl = entry_f - (atr * be_buffer_atr if atr else 0)
        else:
            continue

        if not hit_tp1:
            continue

        r = amend_position_sl_tp(int(pid), stop_loss=round(new_sl, 5))
        if r.get("ok"):
            await db.mark_journal_be_moved(int(j["id"]), float(new_sl))
            await notify_user(
                f"🛡️ **نقل إلى التعادل** `{sym}`\n"
                f"position=`{pid}` · SL الجديد=`{round(new_sl, 5)}` (بعد بلوغ TP1)",
                key=f"be_{pid}",
                min_interval_sec=10,
            )


async def persist_runtime_state_from_memory(chat_id: Optional[int] = None) -> None:
    """حفظ وضع التشغيل الحالي في DB لاستعادته بعد إعادة تشغيل Cloud Run."""
    try:
        tf = "H1"
        tfs = ["H1"]
        syms: List[str] = []
        if chat_id is not None:
            tf = user_trading_tf.get(chat_id) or (user_selected_tfs.get(chat_id) or ["H1"])[0]
            tfs = user_selected_tfs.get(chat_id) or ["H1"]
            syms = user_selected_symbols.get(chat_id) or []
        elif user_trading_tf:
            cid = next(iter(user_trading_tf.keys()))
            tf = user_trading_tf.get(cid) or "H1"
            tfs = user_selected_tfs.get(cid) or ["H1"]
            syms = user_selected_symbols.get(cid) or []
        await db.save_runtime_state(
            cheap_mode=runtime_cheap_mode,
            auto_trading=auto_trading_enabled,
            trading_tf=tf,
            analysis_tfs=tfs,
            selected_symbols=syms,
            merge_full=signals_merge_full,
            auto_trading_chat_id=auto_trading_chat_id,
        )
    except Exception as e:
        logger.error(f"persist_runtime_state_from_memory: {e}")


async def restore_runtime_state_on_startup() -> None:
    """استعادة الوضع / التداول / إطار التنفيذ من DB."""
    global runtime_cheap_mode, auto_trading_enabled, signals_merge_full, auto_trading_chat_id
    st = await db.load_runtime_state()
    if not st:
        return
    try:
        if st.get("cheap_mode") is not None:
            runtime_cheap_mode = bool(st["cheap_mode"])
        if st.get("auto_trading") is not None:
            auto_trading_enabled = bool(st["auto_trading"])
        if st.get("auto_trading_chat_id"):
            auto_trading_chat_id = int(st["auto_trading_chat_id"])
        elif auto_trading_enabled and MY_TELEGRAM_CHAT_ID:
            # توافق مع حالات محفوظة سابقاً بدون هذا العمود
            try:
                auto_trading_chat_id = int(MY_TELEGRAM_CHAT_ID)
            except Exception:
                pass
        trading_tf = (st.get("trading_tf") or "H1").upper()
        analysis_tfs = [x for x in (st.get("analysis_tfs") or "H1").split(",") if x]
        symbols = [x for x in (st.get("selected_symbols") or "").split(",") if x]
        # طبّق على chat المالك إن وُجد
        if MY_TELEGRAM_CHAT_ID:
            try:
                cid = int(MY_TELEGRAM_CHAT_ID)
                user_trading_tf[cid] = trading_tf
                if analysis_tfs:
                    user_selected_tfs[cid] = analysis_tfs
                if symbols:
                    user_selected_symbols[cid] = symbols
            except Exception:
                pass
        logger.info(
            f"♻️ استعادة حالة التشغيل: cheap={runtime_cheap_mode} trading={auto_trading_enabled} "
            f"owner={auto_trading_chat_id} tf={trading_tf} tfs={analysis_tfs} syms={len(symbols)}"
        )
    except Exception as e:
        logger.error(f"restore_runtime_state_on_startup: {e}")


def apply_runtime_mode_schedules() -> str:
    """
    يعيد ضبط مهام APScheduler حسب الوضع الحالي:
    - اقتصادي (runtime_cheap_mode=True): OHLC بالدقائق من OHLC_REFRESH_MINUTES،
      وتحليل AI بفاصل AUTO_ANALYSIS_INTERVAL_MINUTES (Cloud Scheduler يوقظ الخدمة).
    - غير اقتصادي: OHLC كل NON_CHEAP_OHLC_REFRESH_SECONDS ثانية،
      وتحليل AI كل NON_CHEAP_ANALYSIS_INTERVAL_MINUTES دقيقة (أقل فاصل عملي مع NVIDIA).
    """
    global scheduler
    if scheduler is None:
        return "لا يوجد مجدول نشط"

    # --- OHLC ---
    try:
        if scheduler.get_job("ohlc_refresh_job"):
            scheduler.remove_job("ohlc_refresh_job")
    except Exception:
        pass

    if runtime_cheap_mode:
        scheduler.add_job(
            scheduled_ohlc_refresh,
            "interval",
            minutes=max(5, OHLC_REFRESH_MINUTES),
            id="ohlc_refresh_job",
            next_run_time=datetime.now(timezone.utc) + timedelta(seconds=20),
            max_instances=1,
            coalesce=True,
            replace_existing=True,
        )
        ohlc_desc = f"كل {max(5, OHLC_REFRESH_MINUTES)} دقيقة"
    else:
        scheduler.add_job(
            scheduled_ohlc_refresh,
            "interval",
            seconds=max(30, NON_CHEAP_OHLC_REFRESH_SECONDS),
            id="ohlc_refresh_job",
            next_run_time=datetime.now(timezone.utc) + timedelta(seconds=10),
            max_instances=1,
            coalesce=True,
            replace_existing=True,
        )
        ohlc_desc = f"كل {max(30, NON_CHEAP_OHLC_REFRESH_SECONDS)} ثانية"

    # --- تحليل AI للمتابعات ---
    analysis_minutes = (
        AUTO_ANALYSIS_INTERVAL_MINUTES
        if runtime_cheap_mode
        else max(1, NON_CHEAP_ANALYSIS_INTERVAL_MINUTES)
    )
    try:
        if scheduler.get_job("auto_analysis_job"):
            scheduler.remove_job("auto_analysis_job")
    except Exception:
        pass
    if db.is_storage_ready():
        scheduler.add_job(
            scheduled_analysis_job,
            "interval",
            minutes=analysis_minutes,
            id="auto_analysis_job",
            next_run_time=datetime.now(timezone.utc) + timedelta(seconds=15),
            max_instances=1,
            coalesce=True,
            replace_existing=True,
        )


    # --- إشارات الأطر تلقائياً عند كل شمعة جديدة (بدون AI) ---
    try:
        if scheduler.get_job("tf_signals_auto_job"):
            scheduler.remove_job("tf_signals_auto_job")
    except Exception:
        pass
    if TF_SIGNALS_AUTO_ENABLED and db.is_storage_ready():
        # فحص متكرر؛ التحديث الفعلي يحدث فقط عند إغلاق شمعة جديدة لكل إطار
        check_sec = (
            max(30, int(os.getenv("TF_SIGNALS_CHECK_SECONDS", "30")))
            if not runtime_cheap_mode
            else max(60, int(os.getenv("TF_SIGNALS_CHECK_SECONDS_CHEAP", "60")))
        )
        scheduler.add_job(
            scheduled_tf_signals_refresh,
            "interval",
            seconds=check_sec,
            id="tf_signals_auto_job",
            next_run_time=datetime.now(timezone.utc) + timedelta(seconds=25),
            max_instances=1,
            coalesce=True,
            replace_existing=True,
        )
        tf_sig_desc = f"فحص كل {check_sec}ث (تحديث عند شمعة جديدة فقط)"
    else:
        tf_sig_desc = "معطّل"

    mode_ar = "اقتصادي (Cloud Scheduler)" if runtime_cheap_mode else "غير اقتصادي (APScheduler 24/7)"
    logger.info(
        f"⚙️ تبديل الوضع → {mode_ar} | OHLC={ohlc_desc} | AI كل {analysis_minutes} د | TF-signals={tf_sig_desc}"
    )
    return (
        f"الوضع: **{mode_ar}**\n"
        f"🕯️ OHLC: {ohlc_desc}\n"
        f"🧠 تحليل AI: كل {analysis_minutes} دقيقة\n"
        f"⏱️ إشارات الأطر تلقائياً: {tf_sig_desc}"
    )



def _request_all_symbol_specs(client) -> None:
    """
    يطلب مواصفات كل رمز متاح (ProtoOASymbolByIdReq) لملء symbol_specs_cache
    بـ digits / lotSize / pipPosition لكل رمز. يُستدعى مرة واحدة بعد استلام
    قائمة الرموز. الردود تصل بشكل غير متزامن على on_message_received.
    """
    try:
        ids = list(symbol_id_map.values())
        for sid in ids:
            try:
                req = ProtoOASymbolByIdReq()
                req.ctidTraderAccountId = ACCOUNT_ID
                req.symbolId = int(sid)
                client.send(req)
            except Exception as e:
                logger.debug(f"symbol spec request failed id={sid}: {e}")
        logger.info(f"📥 طلب مواصفات {len(ids)} رمز من الوسيط")
    except Exception as e:
        logger.error(f"_request_all_symbol_specs error: {e}")


def _apply_symbol_spec(sym) -> None:
    """يخزّن مواصفات رمز واحد في symbol_specs_cache (مفتاحان: الاسم والـ id)."""
    try:
        sid = int(getattr(sym, "symbolId", 0) or 0)
        digits = int(getattr(sym, "digits", 5) or 5)
        spec = {
            "symbol_id": sid,
            "digits": digits,
            "pip_position": int(getattr(sym, "pipPosition", digits - 1) or (digits - 1)),
            "lot_size": int(getattr(sym, "lotSize", 100_000) or 100_000),
            "min_volume": int(getattr(sym, "minVolume", 1_000) or 1_000),
            "max_volume": int(getattr(sym, "maxVolume", 10_000_000) or 10_000_000),
            "step_volume": int(getattr(sym, "stepVolume", 1_000) or 1_000),
            "sl_distance": int(getattr(sym, "slDistance", 0) or 0),
            "tp_distance": int(getattr(sym, "tpDistance", 0) or 0),
        }
        symbol_specs_cache[sid] = spec
        name = symbol_id_to_name.get(sid)
        if name:
            symbol_specs_cache[name] = spec
    except Exception as e:
        logger.error(f"_apply_symbol_spec error: {e}")


def symbol_digits(symbol: str) -> int:
    """عدد الخانات العشرية لسعر الرمز (افتراضي 5 للأزواج الرئيسية)."""
    try:
        spec = symbol_specs_cache.get(symbol) or symbol_specs_cache.get(symbol.upper())
        if spec and spec.get("digits"):
            return max(1, int(spec["digits"]))
    except Exception:
        pass
    return 5


def symbol_price_scale(symbol: str) -> float:
    """10^digits — المقياس الصحيح لفك ترميز أسعار cTrader لهذا الرمز."""
    return float(10 ** symbol_digits(symbol))


def symbol_lot_size(symbol: str) -> int:
    """حجم اللوت الكامل للرمز بالوحدات (FX: 100,000 — ذهب/كريبتو قد يختلف)."""
    try:
        spec = symbol_specs_cache.get(symbol) or symbol_specs_cache.get(symbol.upper())
        if spec and spec.get("lot_size"):
            return max(1, int(spec["lot_size"]))
    except Exception:
        pass
    return 100_000


def symbol_min_volume(symbol: str) -> int:
    """أقل حجم تداول مسموح بالوحدات."""
    try:
        spec = symbol_specs_cache.get(symbol) or symbol_specs_cache.get(symbol.upper())
        if spec and spec.get("min_volume"):
            return max(1, int(spec["min_volume"]))
    except Exception:
        pass
    return 1_000


ctrader_client.setConnectedCallback(on_connected)
ctrader_client.setDisconnectedCallback(on_disconnected)
ctrader_client.setMessageReceivedCallback(on_message_received)

def start_ctrader_reactor():
    try:
        ctrader_client.startService()
        if not reactor.running:
            reactor.run(installSignalHandlers=False)
    except Exception as e:
        logger.error(f"cTrader Reactor Exception: {e}")

def ctrader_auto_reconnect_loop():
    global stop_ctrader_flag
    threading.Thread(target=start_ctrader_reactor, daemon=True).start()
    
    while not stop_ctrader_flag:
        if not is_ctrader_connected and CLIENT_ID and CLIENT_SECRET:
            try:
                reactor.callFromThread(ctrader_client.startService)
            except Exception as e:
                logger.error(f"Reconnect Error: {e}")
        for _ in range(30):
            if stop_ctrader_flag:
                break
            time.sleep(1)


# ==================== Ensemble Multi-Agent Pipeline ====================
# طبقة 1: Data Fusion (برمجي) → طبقة 2: News Agent → طبقة 3: Liquidity Agent
# → طبقة 4: Executive Agent (JSON منظم) → تنسيق عربي للتيليجرام + حفظ الإشارة

def _extract_json_object(text: str) -> Optional[Dict[str, Any]]:
    """يستخرج أول كائن JSON صالح من رد النموذج (حتى لو لفّه بـ markdown)."""
    if not text:
        return None
    cleaned = text.strip()
    if cleaned.startswith("```"):
        cleaned = re.sub(r"^```(?:json)?\s*", "", cleaned)
        cleaned = re.sub(r"\s*```$", "", cleaned)
    # محاولة مباشرة
    try:
        obj = json.loads(cleaned)
        return obj if isinstance(obj, dict) else None
    except Exception:
        pass
    # بحث عن أول {...}
    start = cleaned.find("{")
    end = cleaned.rfind("}")
    if start >= 0 and end > start:
        try:
            obj = json.loads(cleaned[start:end + 1])
            return obj if isinstance(obj, dict) else None
        except Exception:
            return None
    return None


async def _nvidia_chat(model: str, system: str, user: str, temperature: float = 0.2,
                       max_tokens: int = 2048) -> str:
    """استدعاء موحّد لـ NVIDIA (OpenAI-compatible) مع إعادة محاولة بسيطة عند 429."""
    if not ai_client:
        raise RuntimeError("NVIDIA API client غير مهيأ")
    max_retries = 3
    base_delay = 6
    last_err = None
    for attempt in range(1, max_retries + 1):
        try:
            loop = asyncio.get_running_loop()
            response = await loop.run_in_executor(
                None,
                lambda: ai_client.chat.completions.create(
                    model=model,
                    messages=[
                        {"role": "system", "content": system},
                        {"role": "user", "content": user},
                    ],
                    temperature=temperature,
                    max_tokens=max_tokens,
                ),
            )
            return (response.choices[0].message.content or "").strip()
        except Exception as e:
            last_err = e
            if ("429" in str(e) or "Too Many Requests" in str(e)) and attempt < max_retries:
                await asyncio.sleep(base_delay * attempt)
                continue
            try:
                await notify_user(
                    f"⚠️ **فشل استدعاء NVIDIA**\n`{str(e)[:300]}`",
                    key="nvidia_fail",
                    min_interval_sec=120,
                )
            except Exception:
                pass
            raise
    try:
        await notify_user(
            f"⚠️ **فشل استدعاء NVIDIA**\n`{str(last_err)[:300]}`",
            key="nvidia_fail",
            min_interval_sec=120,
        )
    except Exception:
        pass
    raise RuntimeError(str(last_err))



def compute_multi_tf_confluence(all_tfs: Dict[str, Any]) -> Dict[str, Any]:
    """
    يدمج انحياز عدة أطر زمنية في إشارة واحدة أدق.
    أوزان تقريبية: D1/H4 أعلى من M15/M1 لتقليل الضوضاء.
    """
    weights = {"D1": 1.4, "H4": 1.2, "H1": 1.0, "M15": 0.7, "M1": 0.4}
    buy_w = sell_w = 0.0
    used = []
    for tf, ind in (all_tfs or {}).items():
        if not ind or "error" in ind:
            continue
        w = weights.get(tf, 0.8)
        bias = ind.get("bias")
        score = float(ind.get("confluence_score") or 0)
        if bias == "bullish" or (ind.get("score_buy") or 0) > (ind.get("score_sell") or 0):
            buy_w += w * (0.5 + score / 200.0)
        elif bias == "bearish" or (ind.get("score_sell") or 0) > (ind.get("score_buy") or 0):
            sell_w += w * (0.5 + score / 200.0)
        used.append(tf)
    if buy_w == sell_w == 0:
        return {"bias": "neutral", "strength": 0.0, "timeframes_used": used, "agree_ratio": 0.0}
    total = buy_w + sell_w
    if buy_w > sell_w:
        return {
            "bias": "bullish",
            "strength": round(buy_w / total, 3),
            "timeframes_used": used,
            "agree_ratio": round(buy_w / total, 3),
        }
    if sell_w > buy_w:
        return {
            "bias": "bearish",
            "strength": round(sell_w / total, 3),
            "timeframes_used": used,
            "agree_ratio": round(sell_w / total, 3),
        }
    return {"bias": "neutral", "strength": 0.0, "timeframes_used": used, "agree_ratio": 0.5}


def fuse_market_state(aggregated: Dict[str, Any], history_text: str,
                      signal_history_text: str) -> Dict[str, Any]:
    """
    الطبقة 1 — Data Fusion & Guardrails (برمجية، بدون LLM):
    تجميع لقطة موحّدة من cTrader + كاش الأخبار/التقويم/TV + ذاكرة DB.
    ترفض الحقول الفارغة وتضع علامات توفر البيانات.
    """
    symbols = aggregated.get("symbols") or []
    tech = aggregated.get("symbols_technical") or {}
    per_symbol = {}
    for sym in symbols:
        by_tf = tech.get(sym) or {}
        primary = None
        for pref in ("H1", "H4", "M15", "M1", "D1"):
            ind = by_tf.get(pref)
            if ind and "error" not in ind:
                primary = {**ind, "timeframe": pref}
                break

        if primary is None:
            for tf, ind in by_tf.items():
                if ind and "error" not in ind:
                    primary = {**ind, "timeframe": tf}
                    break
        all_ok = {k: v for k, v in by_tf.items() if v and "error" not in v}
        mtf = compute_multi_tf_confluence(all_ok)
        if primary is not None:
            primary = {**primary, "multi_tf_bias": mtf.get("bias"), "multi_tf_strength": mtf.get("strength")}
        per_symbol[sym] = {
            "primary_technicals": primary,
            "all_timeframes": all_ok,
            "multi_tf": mtf,
            "has_live_ctrader_data": primary is not None,
        }

    fusion = {
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "symbols": symbols,
        "account": {
            "balance": ctrader_account_info.get("balance"),
            "equity": ctrader_account_info.get("equity"),
            "margin": ctrader_account_info.get("margin"),
            "free_margin": ctrader_account_info.get("free_margin"),
            "margin_level": ctrader_account_info.get("margin_level"),
            "leverage": ctrader_account_info.get("leverage"),
            "open_positions_count": len(active_positions),
            "open_positions": active_positions[:20],
        },
        "calendar_forex_factory": aggregated.get("forex_factory") or [],
        "news_finnhub": aggregated.get("finnhub_news") or [],
        "tradingview_filter_rss": aggregated.get("tradingview_rss") or [],
        "technical_summary_text": aggregated.get("technical_summary") or "",
        "per_symbol": per_symbol,
        "report_history_snippet": (history_text or "")[:2500],
        "signal_learning_memory": (signal_history_text or "")[:3500],
        "data_availability": {
            "ctrader_connected": is_ctrader_connected,
            "has_calendar": bool(aggregated.get("forex_factory")),
            "has_news": bool(aggregated.get("finnhub_news")),
            "has_tradingview_rss": bool(aggregated.get("tradingview_rss")),
            "note": (
                "PRIMARY prices/OHLC/tech from cTrader only. "
                "TradingView RSS is filter/context only (no Volume Profile/DOM/DXY series in this pipeline unless stored). "
                "Finnhub/FF are news-risk filters."
            ),
        },
    }
    return fusion


async def agent_news_context(fusion: Dict[str, Any]) -> Dict[str, Any]:
    """
    الوكيل 1 — News Context Agent:
    يحوّل التقويم + الأخبار إلى News Sentiment Score ∈ [-1, +1] ومخاطر التقلب.
    """
    system = (
        "You are the News Context Agent in a trading ensemble. "
        "Return ONLY valid JSON (no markdown, no prose). "
        "Score overall news impact for the given FX/commodities context from -1 (very bearish risk) to +1 (very bullish risk). "
        "Flag high-volatility windows around high-impact events."
    )
    user = {
        "task": "news_sentiment_score",
        "symbols": fusion.get("symbols"),
        "calendar": fusion.get("calendar_forex_factory"),
        "news": fusion.get("news_finnhub"),
        "required_schema": {
            "news_sentiment_score": "float -1..+1",
            "volatility_risk": "low|medium|high",
            "high_impact_events_nearby": "bool",
            "per_symbol_bias": {"SYMBOL": "bullish|bearish|neutral"},
            "rationale_ar": "short Arabic string",
            "block_new_entries": "bool",
        },
    }
    raw = await _nvidia_chat(
        AGENT_NEWS_MODEL, system, json.dumps(user, ensure_ascii=False, default=str),
        temperature=0.15, max_tokens=1200,
    )
    parsed = _extract_json_object(raw) or {}
    # defaults
    score = parsed.get("news_sentiment_score", 0)
    try:
        score = max(-1.0, min(1.0, float(score)))
    except Exception:
        score = 0.0
    return {
        "news_sentiment_score": score,
        "volatility_risk": parsed.get("volatility_risk") or "medium",
        "high_impact_events_nearby": bool(parsed.get("high_impact_events_nearby")),
        "per_symbol_bias": parsed.get("per_symbol_bias") or {},
        "rationale_ar": parsed.get("rationale_ar") or "",
        "block_new_entries": bool(parsed.get("block_new_entries")),
        "raw_model": AGENT_NEWS_MODEL,
    }


async def agent_liquidity_intermarket(fusion: Dict[str, Any]) -> Dict[str, Any]:
    """
    الوكيل 2 — Liquidity & Intermarket Agent:
    يقيّم توافق الإشارة مع السياق العام (RSS كفلتر + علاقات الأزواج من الذاكرة الفنية).
    لا يخترع Volume Profile/DOM إن لم تكن في البيانات.
    """
    system = (
        "You are the Liquidity & Intermarket Agent. Return ONLY valid JSON. "
        "Use ONLY provided data. If Volume Profile / DOM / DXY series are absent, "
        "set available=false for those fields and rely on cTrader technicals + RSS headlines + cross-symbol memory. "
        "Do not invent institutional liquidity pools."
    )
    user = {
        "task": "liquidity_intermarket_filter",
        "symbols": fusion.get("symbols"),
        "per_symbol_technicals": {
            s: fusion["per_symbol"][s].get("primary_technicals")
            for s in fusion.get("symbols") or []
        },
        "tradingview_rss_filter": fusion.get("tradingview_filter_rss"),
        "signal_learning_memory": fusion.get("signal_learning_memory"),
        "data_availability": fusion.get("data_availability"),
        "required_schema": {
            "intermarket_alignment": "aligned|mixed|conflicting",
            "liquidity_filter": "pass|caution|fail",
            "per_symbol": {
                "SYMBOL": {
                    "tech_bias": "bullish|bearish|neutral",
                    "confidence": "0..1",
                    "notes_ar": "string",
                }
            },
            "rationale_ar": "string",
        },
    }
    raw = await _nvidia_chat(
        AGENT_LIQUIDITY_MODEL, system, json.dumps(user, ensure_ascii=False, default=str),
        temperature=0.15, max_tokens=1600,
    )
    parsed = _extract_json_object(raw) or {}
    return {
        "intermarket_alignment": parsed.get("intermarket_alignment") or "mixed",
        "liquidity_filter": parsed.get("liquidity_filter") or "caution",
        "per_symbol": parsed.get("per_symbol") or {},
        "rationale_ar": parsed.get("rationale_ar") or "",
        "raw_model": AGENT_LIQUIDITY_MODEL,
    }


def _technical_fallback_decision(fusion: Dict[str, Any], news: Dict[str, Any],
                                  liquidity: Dict[str, Any]) -> Dict[str, Any]:
    """
    احتياطي فني أدق: يشترط confluence_score + تأكيد multi-TF + ADX/MACD.
    يرفض الإشارات الضعيفة لرفع الدقة على حساب عدد الصفقات.
    """
    decisions = []
    blocked = bool(news.get("block_new_entries")) or liquidity.get("liquidity_filter") == "fail"
    for sym in fusion.get("symbols") or []:
        tech = (fusion.get("per_symbol") or {}).get(sym) or {}
        ind = tech.get("primary_technicals") or {}
        mtf = tech.get("multi_tf") or {}
        if not ind or not tech.get("has_live_ctrader_data"):
            decisions.append({
                "symbol": sym, "direction": "NEUTRAL", "confidence": 0.0,
                "entry": None, "sl": None, "tp1": None, "tp2": None, "rr": None,
                "size_hint": "skip",
                "reason_ar": "لا تتوفر مؤشرات cTrader حية لهذا الزوج",
            })
            continue
        price = float(ind.get("last_price") or 0)
        rsi = float(ind.get("rsi_14") or 50)
        atr = float(ind.get("atr_14") or 0) or abs(price) * 0.001
        ema20 = float(ind.get("ema_20") or price)
        ema50 = float(ind.get("ema_50") or price)
        conf_score = int(ind.get("confluence_score") or 0)
        bias = ind.get("bias") or "neutral"
        adx = float(ind.get("adx_14") or 0)
        macd_h = float(ind.get("macd_hist") or 0)
        mtf_bias = mtf.get("bias") or "neutral"
        mtf_str = float(mtf.get("strength") or 0)

        direction = "NEUTRAL"
        confidence = 0.3
        reason = "إشارة ضعيفة — لا تأكيد كافٍ"

        # قواعد صارمة: درجة confluence + توافق إطار أعلى
        bullish_ok = (
            conf_score >= 55
            and bias == "bullish"
            and (mtf_bias in ("bullish", "neutral") or mtf_str < 0.55)
            and (adx >= 18 or abs(macd_h) > 0)
        )
        bearish_ok = (
            conf_score >= 55
            and bias == "bearish"
            and (mtf_bias in ("bearish", "neutral") or mtf_str < 0.55)
            and (adx >= 18 or abs(macd_h) > 0)
        )
        # تأكيد multi-TF قوي يرفع الثقة
        if bullish_ok and mtf_bias == "bullish" and mtf_str >= 0.6:
            direction, confidence = "BUY", min(0.82, 0.55 + conf_score / 250.0 + 0.1)
            reason = f"تأكيد متعدد الأطر صاعد | score={conf_score} ADX={adx:.0f} RSI={rsi:.1f}"
        elif bearish_ok and mtf_bias == "bearish" and mtf_str >= 0.6:
            direction, confidence = "SELL", min(0.82, 0.55 + conf_score / 250.0 + 0.1)
            reason = f"تأكيد متعدد الأطر هابط | score={conf_score} ADX={adx:.0f} RSI={rsi:.1f}"
        elif bullish_ok and rsi <= 40 and price <= ema20:
            direction, confidence = "BUY", 0.58
            reason = f"تشبع بيعي + confluence={conf_score} RSI={rsi:.1f}"
        elif bearish_ok and rsi >= 60 and price >= ema20:
            direction, confidence = "SELL", 0.58
            reason = f"تشبع شرائي + confluence={conf_score} RSI={rsi:.1f}"
        elif conf_score >= 70 and bias == "bullish" and ema20 > ema50:
            direction, confidence = "BUY", 0.56
            reason = f"اتجاه صاعد قوي score={conf_score}"
        elif conf_score >= 70 and bias == "bearish" and ema20 < ema50:
            direction, confidence = "SELL", 0.56
            reason = f"اتجاه هابط قوي score={conf_score}"

        # تعارض الأطر → حياد
        if direction == "BUY" and mtf_bias == "bearish" and mtf_str >= 0.65:
            direction, confidence = "NEUTRAL", 0.25
            reason = "تعارض مع الإطار الأعلى — إلغاء الشراء"
        if direction == "SELL" and mtf_bias == "bullish" and mtf_str >= 0.65:
            direction, confidence = "NEUTRAL", 0.25
            reason = "تعارض مع الإطار الأعلى — إلغاء البيع"

        if blocked and direction != "NEUTRAL":
            direction, confidence = "NEUTRAL", 0.2
            reason += " | حُظر بسبب فلتر أخبار/سيولة"

        entry = sl = tp1 = tp2 = rr = None
        size = "skip"
        # SL أوسع قليلاً + TP1 بنسبة R:R ≥ 1.5 لرفع جودة الصفقات
        if direction == "BUY":
            entry = round(price, 5)
            sl = round(price - 1.4 * atr, 5)
            tp1 = round(price + 2.1 * atr, 5)
            tp2 = round(price + 3.2 * atr, 5)
            rr = "1.5:1"
            size = "reduce" if liquidity.get("liquidity_filter") == "caution" or confidence < 0.6 else "normal"
        elif direction == "SELL":
            entry = round(price, 5)
            sl = round(price + 1.4 * atr, 5)
            tp1 = round(price - 2.1 * atr, 5)
            tp2 = round(price - 3.2 * atr, 5)
            rr = "1.5:1"
            size = "reduce" if liquidity.get("liquidity_filter") == "caution" or confidence < 0.6 else "normal"

        decisions.append({
            "symbol": sym,
            "direction": direction,
            "confidence": round(confidence, 3),
            "entry": entry,
            "sl": sl,
            "tp1": tp1,
            "tp2": tp2,
            "rr": rr,
            "size_hint": size,
            "reason_ar": reason + " (احتياطي فني محسّن)",
        })
    return {
        "decisions": decisions,
        "overall_bias_ar": "قرار احتياطي محسّن (confluence + multi-TF) بعد ضعف مخرجات وكيل التنفيذ.",
        "execute_ready": False,
        "news_agent": news,
        "liquidity_agent": liquidity,
        "raw_model": "technical_fallback_v2",
        "used_fallback": True,
    }



def _compact_technicals_for_prompt(fusion: Dict[str, Any]) -> Dict[str, Any]:
    """جدول مدمج واضح للنموذج — يقلل ضياع الأرقام داخل JSON كبير."""
    out = {}
    for sym in fusion.get("symbols") or []:
        ind = ((fusion.get("per_symbol") or {}).get(sym) or {}).get("primary_technicals") or {}
        if not ind:
            out[sym] = {"error": "no_live_data"}
            continue
        tech = (fusion.get("per_symbol") or {}).get(sym) or {}
        mtf = tech.get("multi_tf") or {}
        out[sym] = {
            "timeframe": ind.get("timeframe"),
            "last_price": ind.get("last_price"),
            "rsi_14": ind.get("rsi_14"),
            "ema_20": ind.get("ema_20"),
            "ema_50": ind.get("ema_50"),
            "atr_14": ind.get("atr_14"),
            "macd_hist": ind.get("macd_hist"),
            "adx_14": ind.get("adx_14"),
            "momentum_10_pct": ind.get("momentum_10_pct"),
            "volume_ratio": ind.get("volume_ratio"),
            "bias": ind.get("bias"),
            "confluence_score": ind.get("confluence_score"),
            "multi_tf_bias": mtf.get("bias"),
            "multi_tf_strength": mtf.get("strength"),
            "pivot": ind.get("pivot"),
            "r1": ind.get("r1"),
            "s1": ind.get("s1"),
        }
    return out


async def agent_executive_decision(fusion: Dict[str, Any], news: Dict[str, Any],
                                   liquidity: Dict[str, Any]) -> Dict[str, Any]:
    """
    الوكيل 3 — Executive Decision Agent:
    يدمج مخرجات الوكيلين + حساب cTrader + المؤشرات، ويُخرج قراراً JSON منظماً لكل زوج.
    عند فشل JSON أو ثقة صفر للجميع → احتياطي فني من cTrader.
    """
    compact_tech = _compact_technicals_for_prompt(fusion)
    system = (
        "You are the Executive Decision Agent for institutional FX trading. "
        "Return ONLY a single valid JSON object. No markdown fences. No prose. "
        "You MUST output one decision object per symbol in the symbols list. "
        "PRIMARY numbers for entry/SL/TP MUST be derived from cTrader fields last_price and atr_14 "
        "(e.g. BUY: sl=price-1.2*atr, tp1=price+1.5*atr). "
        "Do NOT return NEUTRAL for all symbols if RSI is extreme (<30 or >70) unless news block or liquidity fail. "
        "Account balance may be 0 if reconcile lagged — IGNORE account zeros; use technicals. "
        "Direction values: BUY, SELL, or NEUTRAL only. confidence must be between 0.4 and 0.9 when BUY/SELL. "
        "Use signal_learning_memory actively: if similar past setups failed, lower confidence or choose NEUTRAL; "
        "if win-rates for a side are strong and technicals agree, you may raise confidence slightly. "
        "ACCURACY RULES: Prefer NEUTRAL unless confluence_score>=55 AND multi_tf_bias does not strongly oppose. "
        "Require ADX>=18 or clear MACD_hist sign for non-neutral. "
        "SL distance ~1.3-1.5*ATR, TP1 ~2.0-2.2*ATR (min R:R 1.5). "
        "If multi_tf_strength>=0.65 against your side -> NEUTRAL. "
        "Never ignore live cTrader RSI/ATR extremes. Never invent prices not derived from last_price/atr_14."
    )
    user = {
        "task": "final_trade_decision",
        "symbols": fusion.get("symbols"),
        "ctrader_technicals_primary": compact_tech,
        "news_agent": {
            "news_sentiment_score": news.get("news_sentiment_score"),
            "volatility_risk": news.get("volatility_risk"),
            "block_new_entries": news.get("block_new_entries"),
            "per_symbol_bias": news.get("per_symbol_bias"),
        },
        "liquidity_agent": {
            "intermarket_alignment": liquidity.get("intermarket_alignment"),
            "liquidity_filter": liquidity.get("liquidity_filter"),
            "per_symbol": liquidity.get("per_symbol"),
        },
        "signal_learning_memory": (fusion.get("signal_learning_memory") or "")[:4000],
        "required_schema": {
            "decisions": [
                {
                    "symbol": "EURUSD",
                    "direction": "BUY|SELL|NEUTRAL",
                    "confidence": 0.5,
                    "entry": 1.1,
                    "sl": 1.09,
                    "tp1": 1.11,
                    "tp2": 1.12,
                    "rr": "1.5:1",
                    "size_hint": "normal",
                    "reason_ar": "سبب قصير",
                }
            ],
            "overall_bias_ar": "string",
            "execute_ready": False,
        },
    }
    raw = await _nvidia_chat(
        AGENT_EXECUTIVE_MODEL, system, json.dumps(user, ensure_ascii=False, default=str),
        temperature=0.15, max_tokens=min(8000, 1200 + max(1, len(fusion.get("symbols") or [])) * 450),
    )
    parsed = _extract_json_object(raw) or {}
    if not parsed:
        logger.warning(f"Executive agent JSON parse failed. Raw head: {(raw or '')[:400]}")
    decisions = parsed.get("decisions")
    if not isinstance(decisions, list):
        decisions = []
    # أحياناً النموذج يلف القرارات داخل مفتاح آخر
    if not decisions and isinstance(parsed.get("symbols"), list):
        decisions = parsed.get("symbols")

    by_sym = {}
    for d in decisions:
        if not isinstance(d, dict):
            continue
        sym = str(d.get("symbol") or d.get("ticker") or "").upper().replace("!", "")
        if sym:
            by_sym[sym] = d

    normalized = []
    for sym in fusion.get("symbols") or []:
        d = by_sym.get(sym) or by_sym.get(sym.upper()) or {}
        direction = str(d.get("direction") or d.get("side") or "NEUTRAL").upper()
        if direction in ("LONG", "شراء"):
            direction = "BUY"
        if direction in ("SHORT", "بيع"):
            direction = "SELL"
        if direction not in ("BUY", "SELL", "NEUTRAL"):
            direction = "NEUTRAL"
        try:
            confidence = float(d.get("confidence") or 0)
        except Exception:
            confidence = 0.0
        if news.get("block_new_entries") or liquidity.get("liquidity_filter") == "fail":
            if direction != "NEUTRAL":
                direction = "NEUTRAL"
                confidence = min(confidence, 0.25)
                d = dict(d)
                d["size_hint"] = "skip"
                d["reason_ar"] = (d.get("reason_ar") or "") + " | حُظر بسبب فلتر أخبار/سيولة"
        normalized.append({
            "symbol": sym,
            "direction": direction,
            "confidence": confidence,
            "entry": d.get("entry"),
            "sl": d.get("sl"),
            "tp1": d.get("tp1"),
            "tp2": d.get("tp2"),
            "rr": d.get("rr"),
            "size_hint": d.get("size_hint") or ("skip" if direction == "NEUTRAL" else "normal"),
            "reason_ar": d.get("reason_ar") or d.get("reason") or "",
        })

    # احتياطي فني لكل زوج على حدة عند قرار ضعيف (NEUTRAL بلا دخول)
    fb = _technical_fallback_decision(fusion, news, liquidity)
    fb_by = {d["symbol"]: d for d in (fb.get("decisions") or [])}
    used_fb = False
    merged = []
    for x in normalized:
        weak = (
            x["direction"] == "NEUTRAL"
            and float(x.get("confidence") or 0) < 0.15
            and not x.get("entry")
        )
        alt = fb_by.get(x["symbol"])
        if weak and alt and alt.get("direction") in ("BUY", "SELL"):
            merged.append(alt)
            used_fb = True
        else:
            merged.append(x)
    if used_fb:
        logger.warning("Executive weak on some symbols — merged per-symbol cTrader technical fallback.")

    any_trade = any(d.get("direction") in ("BUY", "SELL") for d in merged)
    return {
        "decisions": merged,
        "overall_bias_ar": parsed.get("overall_bias_ar") or parsed.get("summary_ar") or (
            fb.get("overall_bias_ar") if used_fb else ""
        ),
        "execute_ready": bool(parsed.get("execute_ready")) and not news.get("block_new_entries") and any_trade,
        "news_agent": news,
        "liquidity_agent": liquidity,
        "raw_model": AGENT_EXECUTIVE_MODEL,
        "used_fallback": used_fb,
    }


def format_ensemble_report_ar(decision: Dict[str, Any], fusion: Dict[str, Any]) -> str:
    """تحويل JSON القرار إلى تقرير عربي + كتل إشارة متوافقة مع parse_signal_blocks."""
    lines = []
    lines.append("🧠 **توصية متعددة الوكلاء (Ensemble AI Agents)**")
    exec_label = decision.get("raw_model") or AGENT_EXECUTIVE_MODEL
    if decision.get("used_fallback"):
        exec_label = f"{exec_label} + technical_fallback"
    lines.append(
        f"_News=`{AGENT_NEWS_MODEL.split('/')[-1]}` · "
        f"Liquidity=`{AGENT_LIQUIDITY_MODEL.split('/')[-1]}` · "
        f"Executive=`{str(exec_label).split('/')[-1]}`_"
    )
    news = decision.get("news_agent") or {}
    liq = decision.get("liquidity_agent") or {}
    lines.append("")
    lines.append("### ملخص الوكلاء")
    lines.append(
        f"• **وكيل الأخبار:** درجة التأثير `{news.get('news_sentiment_score', 0):+.2f}` | "
        f"تقلب `{news.get('volatility_risk', '—')}` | "
        f"حظر دخول: `{'نعم' if news.get('block_new_entries') else 'لا'}`"
    )
    if news.get("rationale_ar"):
        lines.append(f"  _{news['rationale_ar']}_")
    lines.append(
        f"• **وكيل السيولة/التداخل:** محاذاة `{liq.get('intermarket_alignment', '—')}` | "
        f"فلتر `{liq.get('liquidity_filter', '—')}`"
    )
    if liq.get("rationale_ar"):
        lines.append(f"  _{liq['rationale_ar']}_")
    acct = fusion.get("account") or {}
    lines.append(
        f"• **الحساب (cTrader):** رصيد `{acct.get('balance')}` | "
        f"Equity `{acct.get('equity')}` | هامش حر `{acct.get('free_margin')}` | "
        f"MarginLevel `{acct.get('margin_level')}%`"
    )
    lines.append("")
    lines.append("### قرارات الأزواج")
    for d in decision.get("decisions") or []:
        sym = d["symbol"]
        direction = d["direction"]
        if direction == "BUY":
            emoji, dir_ar = "🟢", "شراء (BUY)"
        elif direction == "SELL":
            emoji, dir_ar = "🔴", "بيع (SELL)"
        else:
            emoji, dir_ar = "⚪", "لا توجد إشارة (NEUTRAL)"
        # كتلة متوافقة مع SIGNAL_BLOCK_RE
        block = f"{emoji} {sym} — {dir_ar}\n"
        if direction != "NEUTRAL" and d.get("entry") is not None:
            block += (
                f"الدخول: {d.get('entry')}\n"
                f"SL: {d.get('sl')}\n"
                f"TP1: {d.get('tp1')}\n"
                f"TP2: {d.get('tp2')}\n"
                f"R:R: {d.get('rr') or '—'}"
            )
        else:
            block += "الدخول: غير متوفر\nSL: غير متوفر\nTP1: غير متوفر\nTP2: غير متوفر\nR:R: —"
        lines.append(block)
        conf = d.get("confidence")
        size = d.get("size_hint")
        reason = d.get("reason_ar") or ""
        qscore = d.get("quality_score")
        qgrade = d.get("quality_grade")
        conf_s = f"{float(conf):.2f}" if conf is not None else "—"
        q_s = f" · جودة: {qscore}/100" if qscore is not None else ""
        if qgrade:
            q_s += f" ({qgrade})"
        lines.append(f"_ثقة: {conf_s}{q_s} · حجم مقترح: {size}_")
        qb = d.get("quality_breakdown") or {}
        if qb and d.get("direction") in ("BUY", "SELL"):
            parts = [f"{k}={v}" for k, v in qb.items()]
            lines.append(f"_تفاصيل الجودة: {' · '.join(parts)}_")
        if reason:
            lines.append(f"_{reason}_")
        lines.append("")

    if decision.get("overall_bias_ar"):
        lines.append("### الخلاصة")
        lines.append(decision["overall_bias_ar"])
    lines.append("")
    lines.append(
        f"_execute_ready={decision.get('execute_ready')} · "
        f"المصدر السعري الرئيسي: cTrader_"
    )
    return "\n".join(lines)


async def run_ensemble_pipeline(aggregated: Dict[str, Any], history_text: str,
                                signal_history_text: str) -> str:
    """
    خط أنابيب الوكلاء المتعددين للتحليل full.
    عند كثرة الأزواج: تقسيم دفعات (افتراضي 3) حتى لا يُقطع JSON ويُحفظ NEUTRAL للجميع.
    """
    if not ai_client:
        return "❌ **خطأ:** مفتاح NVIDIA Build API غير متوفر في متغيرات البيئة."

    fusion = fuse_market_state(aggregated, history_text, signal_history_text)
    if not any(fusion["per_symbol"][s]["has_live_ctrader_data"] for s in fusion["symbols"]):
        return "❌ **خطأ:** لا تتوفر شموع/مؤشرات حية من cTrader لأي من الأزواج المختارة."

    try:
        news = await agent_news_context(fusion)
        liquidity = await agent_liquidity_intermarket(fusion)

        symbols = list(fusion.get("symbols") or [])
        chunk_size = int(os.getenv("ENSEMBLE_SYMBOL_CHUNK", "3"))
        chunk_size = max(1, min(chunk_size, 5))
        all_decisions = []
        used_fallback = False
        overall_bits = []

        if len(symbols) <= chunk_size:
            decision_part = await agent_executive_decision(fusion, news, liquidity)
            all_decisions = decision_part.get("decisions") or []
            used_fallback = bool(decision_part.get("used_fallback"))
            if decision_part.get("overall_bias_ar"):
                overall_bits.append(decision_part["overall_bias_ar"])
        else:
            logger.info(f"Ensemble chunking {len(symbols)} symbols by {chunk_size}")
            for i in range(0, len(symbols), chunk_size):
                chunk_syms = symbols[i:i + chunk_size]
                chunk_fusion = dict(fusion)
                chunk_fusion["symbols"] = chunk_syms
                chunk_fusion["per_symbol"] = {
                    s: fusion["per_symbol"][s]
                    for s in chunk_syms if s in fusion.get("per_symbol", {})
                }
                part = await agent_executive_decision(chunk_fusion, news, liquidity)
                all_decisions.extend(part.get("decisions") or [])
                used_fallback = used_fallback or bool(part.get("used_fallback"))
                if part.get("overall_bias_ar"):
                    overall_bits.append(part["overall_bias_ar"])
                await asyncio.sleep(0.3)

        # ضبط الثقة بالسجل الكمي (إن وُجدت إحصاءات في الذاكرة)
        learning_stats = aggregated.get("learning_stats") or {}
        if learning_stats and all_decisions:
            try:
                all_decisions = apply_learning_confidence_adjustments(all_decisions, learning_stats)
            except Exception as le:
                logger.error(f"learning confidence adjust error: {le}")
        if all_decisions:
            try:
                all_decisions = apply_prediction_quality_filter(
                    all_decisions,
                    fusion,
                    news=news,
                    learning_stats=learning_stats or aggregated.get("learning_stats") or {},
                )
            except Exception as qe:
                logger.error(f"prediction quality filter error: {qe}")

        decision = {
            "decisions": all_decisions,
            "overall_bias_ar": " | ".join(overall_bits)[:800],
            "execute_ready": any(d.get("direction") in ("BUY", "SELL") for d in all_decisions)
                and not news.get("block_new_entries"),
            "news_agent": news,
            "liquidity_agent": liquidity,
            "raw_model": AGENT_EXECUTIVE_MODEL,
            "used_fallback": used_fallback,
            "learning_applied": bool(learning_stats),
        }

        report = format_ensemble_report_ar(decision, fusion)
        aggregated["ensemble_decision"] = decision
        aggregated["ensemble_fusion_meta"] = {
            "timestamp_utc": fusion["timestamp_utc"],
            "data_availability": fusion["data_availability"],
            "chunk_size": chunk_size,
            "symbols_count": len(symbols),
        }
        runtime_cache.set_mem("last_news_agent", news, 300)
        runtime_cache.set_mem("last_liquidity_agent", liquidity, 300)
        runtime_cache.set_mem("last_executive", {
            "decisions": all_decisions,
            "overall_bias_ar": decision.get("overall_bias_ar"),
            "execute_ready": decision.get("execute_ready"),
        }, 300)
        return report
    except Exception as e:
        logger.error(f"Ensemble pipeline error: {e}")
        return f"❌ **خطأ أثناء خط أنابيب الوكلاء المتعددين:**\n`{str(e)}`"






def _norm_dir_simple(direction: Any, emoji: Any = None) -> str:
    d = normalize_trade_direction(direction, emoji)
    return d if d in ("BUY", "SELL") else "NEUTRAL"


def apply_full_merge_to_signal(
    symbol: str,
    sig: Dict[str, Any],
    full_map: Dict[str, Dict[str, Any]],
) -> Dict[str, Any]:
    """
    إن كان الدمج مفعّلاً ووُجدت إشارة full حديثة:
      - نفس الاتجاه → تُقبل مع ملاحظة توافق
      - تعارض BUY/SELL → تُحوَّل إلى NEUTRAL (لا تنفيذ متعارض)
      - full محايد → تُترك إشارة الإطار مع ملاحظة
    """
    if not signals_merge_full:
        return sig
    full = full_map.get(symbol) or full_map.get((symbol or "").upper())
    if not full:
        return sig
    out = dict(sig)
    tf_dir = _norm_dir_simple(out.get("direction"), out.get("emoji"))
    full_dir = _norm_dir_simple(full.get("direction"), full.get("emoji"))
    if full_dir in ("BUY", "SELL") and tf_dir in ("BUY", "SELL") and full_dir != tf_dir:
        out["direction"] = "NEUTRAL"
        out["emoji"] = "⚪"
        out["quality_pass"] = False
        out["entry"] = out["sl"] = out["tp1"] = out["tp2"] = None
        out["rr"] = None
        note = f"دمج: تعارض مع الشاملة ({full_dir}) → إلغاء إطار ({tf_dir})"
        out["reason"] = (str(out.get("reason") or "") + " | " + note).strip(" |")
        out["merge_status"] = "conflict_neutralized"
        out["full_direction"] = full_dir
    elif full_dir in ("BUY", "SELL") and tf_dir == full_dir:
        out["merge_status"] = "aligned"
        out["full_direction"] = full_dir
        out["reason"] = (str(out.get("reason") or "") + " | دمج: متوافق مع الشاملة").strip(" |")
    elif full_dir == "NEUTRAL" and tf_dir in ("BUY", "SELL"):
        out["merge_status"] = "full_neutral"
        out["full_direction"] = "NEUTRAL"
        out["reason"] = (str(out.get("reason") or "") + " | دمج: الشاملة محايدة").strip(" |")
    else:
        out["merge_status"] = "no_full_side"
        out["full_direction"] = full_dir
    return out


def signal_from_indicators(
    ind: Dict[str, Any],
    timeframe: str,
    mtf: Optional[Dict[str, Any]] = None,
    apply_quality: bool = True,
    symbol: Optional[str] = None,
) -> Dict[str, Any]:
    """
    إشارة سريعة لإطار واحد من المؤشرات المحلية + فلتر جودة (بدون LLM).
    symbol: لقرّبة الأسعار بدقة الرمز (10^digits).
    """
    if not ind or "error" in ind:
        return {
            "timeframe": timeframe, "direction": "NEUTRAL", "emoji": "⚪",
            "confidence": 0.0, "entry": None, "sl": None, "tp1": None, "tp2": None, "rr": None,
            "reason": "لا بيانات", "quality_score": 0, "quality_pass": False,
        }
    price = float(ind.get("last_price") or 0)
    atr = float(ind.get("atr_14") or 0) or (abs(price) * 0.001 if price else 0.0)
    rsi = float(ind.get("rsi_14") or 50)
    bias = ind.get("bias") or "neutral"
    cscore = int(ind.get("confluence_score") or 0)
    adx = float(ind.get("adx_14") or 0)
    macd_h = float(ind.get("macd_hist") or 0)
    ema20 = float(ind.get("ema_20") or price)
    ema50 = float(ind.get("ema_50") or price)
    mtf = mtf or {}

    direction = "NEUTRAL"
    conf = 0.3
    reason = "عرضي"
    if bias == "bullish" and cscore >= 50 and (adx >= 16 or macd_h > 0):
        direction, conf = "BUY", min(0.78, 0.45 + cscore / 250.0)
        reason = f"صاعد score={cscore} ADX={adx:.0f} RSI={rsi:.0f}"
    elif bias == "bearish" and cscore >= 50 and (adx >= 16 or macd_h < 0):
        direction, conf = "SELL", min(0.78, 0.45 + cscore / 250.0)
        reason = f"هابط score={cscore} ADX={adx:.0f} RSI={rsi:.0f}"
    elif rsi <= 30 and price <= ema20 and cscore >= 40:
        direction, conf = "BUY", 0.52
        reason = f"تشبع بيعي RSI={rsi:.0f}"
    elif rsi >= 70 and price >= ema20 and cscore >= 40:
        direction, conf = "SELL", 0.52
        reason = f"تشبع شرائي RSI={rsi:.0f}"

    quality_score = 0
    quality_grade = None
    quality_pass = False
    quality_notes: List[str] = []
    if apply_quality and direction in ("BUY", "SELL"):
        qinfo = compute_signal_quality_score(direction, ind, mtf, news=None, learning_side=None, timeframe=timeframe)
        quality_score = int(qinfo.get("score") or 0)
        quality_grade = qinfo.get("grade") or "F"
        vetoes = list(qinfo.get("vetoes") or [])
        min_q = int(os.getenv("PRED_MIN_QUALITY", "52"))
        hard_c = int(os.getenv("PRED_HARD_CONFLUENCE", "38"))
        min_c = int(os.getenv("PRED_MIN_CONFLUENCE", "48"))
        mtf_conflict = float(os.getenv("PRED_MTF_CONFLICT", "0.62"))
        mtf_bias = (mtf.get("bias") or "neutral")
        mtf_str = float(mtf.get("strength") or 0)

        if cscore < hard_c:
            vetoes.append(f"confluence={cscore}<{hard_c}")
        if direction == "BUY" and mtf_bias == "bearish" and mtf_str >= mtf_conflict:
            vetoes.append("تعارض multi-TF")
        if direction == "SELL" and mtf_bias == "bullish" and mtf_str >= mtf_conflict:
            vetoes.append("تعارض multi-TF")

        hard = [v for v in vetoes if any(k in v for k in (
            "multi-TF", "confluence", "حظر أخبار", "نجاح تاريخي", "RSI مرتفع", "RSI منخفض", "مطاردة",
        ))]
        if hard or quality_score < min_q:
            quality_pass = False
            direction = "NEUTRAL"
            conf = min(conf, 0.3)
            quality_notes = hard or [f"جودة {quality_score}<{min_q}"]
            reason = reason + " | مرفوض: " + " ؛ ".join(quality_notes)
        else:
            quality_pass = True
            if cscore < min_c:
                conf = min(conf, 0.48)
                quality_notes.append(f"confluence منخفض {cscore}")
            conf_cap = 0.35 + (quality_score / 100.0) * 0.55
            conf = min(conf, conf_cap)
            if quality_notes:
                reason = reason + " | " + " ؛ ".join(quality_notes)
    elif direction == "NEUTRAL":
        quality_pass = False
        quality_score = int(cscore * 0.5) if cscore else 0
    else:
        quality_pass = True
        quality_score = int(cscore) if cscore else 50

    emoji = "🟢" if direction == "BUY" else ("🔴" if direction == "SELL" else "⚪")
    round_n = symbol_digits(symbol) if symbol else 5
    entry = sl = tp1 = tp2 = rr = None
    if direction == "BUY" and price and atr:
        entry, sl = round(price, round_n), round(price - 1.4 * atr, round_n)
        tp1, tp2 = round(price + 2.1 * atr, round_n), round(price + 3.2 * atr, round_n)
        rr = "1.5:1"
    elif direction == "SELL" and price and atr:
        entry, sl = round(price, round_n), round(price + 1.4 * atr, round_n)
        tp1, tp2 = round(price - 2.1 * atr, round_n), round(price - 3.2 * atr, round_n)
        rr = "1.5:1"

    return {
        "timeframe": timeframe,
        "direction": direction,
        "emoji": emoji,
        "confidence": round(conf, 3),
        "entry": entry, "sl": sl, "tp1": tp1, "tp2": tp2, "rr": rr,
        "reason": reason,
        "rsi_14": ind.get("rsi_14"),
        "ema_20": ind.get("ema_20"),
        "ema_50": ind.get("ema_50"),
        "atr_14": ind.get("atr_14"),
        "confluence_score": cscore,
        "adx_14": adx,
        "quality_score": quality_score,
        "quality_pass": quality_pass,
        "quality_grade": quality_grade if apply_quality and direction in ("BUY", "SELL") else None,
    }



def build_per_tf_signals_matrix(
    aggregated: Dict[str, Any],
    analysis_tfs: List[str],
    trading_tf: str,
    full_map: Optional[Dict[str, Dict[str, Any]]] = None,
) -> str:
    """
    مصفوفة إشارات: كل زوج × كل إطار تحليل (حسابي فوري).
    إطار التنفيذ يُبرز بوضوح دون إعادة استدعاء AI.
    """
    symbols = list(aggregated.get("symbols") or [])
    tech = aggregated.get("symbols_technical") or {}
    tfs = [x for x in (analysis_tfs or ["H1"]) if x]
    trade_tf = (trading_tf or "H1").upper()
    if trade_tf not in tfs:
        tfs = list(tfs) + [trade_tf]

    merge_on = signals_merge_full
    full_map = full_map or {}
    lines = [
        "⏱️ **إشارات حسب الإطار الزمني** _(مؤشرات محلية — بدون AI)_",
        f"إطار **التنفيذ** المختار: `{trade_tf}`",
        f"أطر **التحليل**: `{', '.join(tfs)}`",
        f"دمج مع الشاملة: {'✅ مفعّل' if merge_on else '⬜ ملغى'}",
        "",
        "_الفكرة: التحليل متعدد الأطر للتأكيد، والتنفيذ على إطار واحد فقط._",
        "",
    ]

    for sym in symbols:
        by_tf = tech.get(sym) or {}
        lines.append(f"### {sym}")
        trade_row = None
        all_ok = {k: v for k, v in by_tf.items() if v and "error" not in v}
        mtf = compute_multi_tf_confluence(all_ok) if all_ok else {}
        for tf in tfs:
            ind = by_tf.get(tf)
            if not ind or "error" in ind:
                mark = " ← تنفيذ" if tf == trade_tf else ""
                lines.append(f"• `{tf}`: ⚪ لا بيانات{mark}")
                continue
            sig = signal_from_indicators(ind, tf, mtf=mtf, apply_quality=True, symbol=sym)
            if merge_on:
                sig = apply_full_merge_to_signal(sym, sig, full_map)
            mark = " **← إطار التنفيذ**" if tf == trade_tf else ""
            q = sig.get("quality_score")
            q_tag = f", جودة {q}" if q is not None else ""
            gate = "✓" if sig.get("quality_pass") else "✗"
            levels = ""
            if sig["direction"] != "NEUTRAL" and sig.get("entry") is not None:
                levels = f" | دخول {sig['entry']} SL {sig['sl']} TP1 {sig['tp1']}"
            lines.append(
                f"• `{tf}`: {sig['emoji']} **{sig['direction']}** "
                f"(ثقة {sig['confidence']:.2f}, conf {sig.get('confluence_score', 0)}{q_tag}, فلتر {gate})"
                f"{levels}{mark}"
            )
            lines.append(f"  _{sig['reason']}_")
            if tf == trade_tf:
                trade_row = sig
        # توافق multi-TF مبسط
        dirs = []
        for tf in tfs:
            ind = by_tf.get(tf)
            if ind and "error" not in ind:
                dirs.append(signal_from_indicators(ind, tf, mtf=mtf, apply_quality=True, symbol=sym)["direction"])
        if dirs:
            buy_n = dirs.count("BUY")
            sell_n = dirs.count("SELL")
            if buy_n >= 2 and sell_n == 0:
                lines.append("  ✅ توافق نسبي صاعد بين الأطر")
            elif sell_n >= 2 and buy_n == 0:
                lines.append("  ✅ توافق نسبي هابط بين الأطر")
            elif buy_n and sell_n:
                lines.append("  ⚠️ تعارض بين الأطر — خفّض حجم التنفيذ أو انتظر")
        if trade_row and trade_row["direction"] == "NEUTRAL":
            lines.append(f"  ℹ️ لا إشارة تنفيذ على `{trade_tf}` حالياً")
        lines.append("")

    lines.append(
        "_للتنفيذ الآلي يُستخدم إطار التنفيذ فقط. أطر التحليل تُستخدم للتأكيد والجودة._"
    )
    return "\n".join(lines)


async def persist_per_tf_technical_signals(
    aggregated: Dict[str, Any],
    tfs: List[str],
) -> int:
    """
    يخزّن إشارة فنية خفيفة لكل (زوج × إطار) بدون LLM.
    إن كان الدمج مفعّلاً تُحاذى مع آخر توصية شاملة (source=full).
    """
    symbols = list(aggregated.get("symbols") or [])
    tech = aggregated.get("symbols_technical") or {}
    n = 0
    full_map: Dict[str, Dict[str, Any]] = {}
    if signals_merge_full and symbols:
        try:
            max_age = int(os.getenv("SIGNALS_MERGE_MAX_AGE_MINUTES", "90"))
            full_map = await db.get_latest_full_signals(symbols, max_age_minutes=max_age)
        except Exception as e:
            logger.error(f"get_latest_full_signals: {e}")
    for sym in symbols:
        by_tf = tech.get(sym) or {}
        all_ok = {k: v for k, v in by_tf.items() if v and "error" not in v}
        mtf = compute_multi_tf_confluence(all_ok) if all_ok else {}
        for tf in tfs:
            ind = by_tf.get(tf)
            if not ind or "error" in ind:
                continue
            sig = signal_from_indicators(ind, tf, mtf=mtf, apply_quality=True, symbol=sym)
            sig = apply_full_merge_to_signal(sym, sig, full_map)
            dir_ar = (
                "شراء (BUY)" if sig["direction"] == "BUY"
                else ("بيع (SELL)" if sig["direction"] == "SELL" else "لا توجد إشارة (NEUTRAL)")
            )
            await db.save_symbol_signal(
                sym, sig["emoji"], dir_ar,
                str(sig["entry"]) if sig.get("entry") is not None else None,
                str(sig["sl"]) if sig.get("sl") is not None else None,
                str(sig["tp1"]) if sig.get("tp1") is not None else None,
                str(sig["tp2"]) if sig.get("tp2") is not None else None,
                sig.get("rr"),
                rsi_14=sig.get("rsi_14"),
                ema_20=sig.get("ema_20"),
                ema_50=sig.get("ema_50"),
                atr_14=sig.get("atr_14"),
                primary_tf=tf,
                source="tf",
            )
            n += 1
    return n


def build_general_trend_report(aggregated: Dict[str, Any], selected_tfs: List[str]) -> str:
    """
    تحليل الاتجاه العام — حسابي بحت من مؤشرات cTrader (بدون NVIDIA):
      • اتجاه كل زوج (إطار أساسي + multi-TF)
      • قوة الاتجاه (ADX / confluence)
      • الانحياز الجماعي للأزواج المختارة
      • ملخص قطاعي (فوركس / معادن / كريبتو)
    """
    symbols = list(aggregated.get("symbols") or [])
    tech_all = aggregated.get("symbols_technical") or {}
    lines: List[str] = []
    lines.append("📈 **تحليل الاتجاه العام** _(مؤشرات cTrader — بدون AI)_")
    lines.append(f"الأزواج: `{', '.join(symbols) or '—'}`")
    lines.append(f"الأطر: `{', '.join(selected_tfs) or 'H1'}`")
    lines.append("")

    if not symbols:
        lines.append("⚠️ لم يُحدَّد أي زوج.")
        return "\n".join(lines)

    # أوزان أطر للاتجاه العام
    tf_weights = {"D1": 1.4, "H4": 1.2, "H1": 1.0, "M15": 0.7, "M1": 0.35}
    rows = []  # per symbol summary
    bull_w = bear_w = 0.0

    for sym in symbols:
        by_tf = tech_all.get(sym) or {}
        # primary: prefer selected order then defaults
        primary = None
        primary_tf = None
        for pref in list(selected_tfs) + ["H1", "H4", "M15", "D1", "M1"]:
            ind = by_tf.get(pref)
            if ind and "error" not in ind:
                primary = ind
                primary_tf = pref
                break
        if primary is None:
            rows.append({
                "symbol": sym, "bias": "unknown", "strength": 0,
                "label": "لا بيانات", "detail": "لا تتوفر شموع/مؤشرات حية",
                "adx": None, "rsi": None, "cscore": 0, "mtf": {},
            })
            continue

        # multi-tf from all available
        all_ok = {k: v for k, v in by_tf.items() if v and "error" not in v}
        mtf = compute_multi_tf_confluence(all_ok)
        bias = primary.get("bias") or mtf.get("bias") or "neutral"
        cscore = int(primary.get("confluence_score") or 0)
        adx = float(primary.get("adx_14") or 0)
        rsi = float(primary.get("rsi_14") or 50)
        ema20 = primary.get("ema_20")
        ema50 = primary.get("ema_50")
        price = primary.get("last_price")
        macd_h = float(primary.get("macd_hist") or 0)
        mtf_bias = mtf.get("bias") or "neutral"
        mtf_str = float(mtf.get("strength") or 0)

        # قوة اتجاه مركبة 0..100
        strength = 0.0
        strength += min(40.0, cscore * 0.4)
        strength += min(25.0, adx * 0.7)
        strength += min(20.0, mtf_str * 20.0)
        if (bias == "bullish" and mtf_bias == "bullish") or (bias == "bearish" and mtf_bias == "bearish"):
            strength += 10.0
        if abs(macd_h) > 0:
            strength += 5.0
        strength = max(0.0, min(100.0, strength))

        # تصنيف نصي
        if bias == "bullish":
            if strength >= 70:
                label = "صاعد قوي"
            elif strength >= 45:
                label = "صاعد"
            else:
                label = "صاعد ضعيف"
        elif bias == "bearish":
            if strength >= 70:
                label = "هابط قوي"
            elif strength >= 45:
                label = "هابط"
            else:
                label = "هابط ضعيف"
        else:
            label = "عرضي / غير حاسم"

        # وزن جماعي
        w = tf_weights.get(primary_tf or "H1", 1.0) * (0.5 + strength / 200.0)
        if bias == "bullish":
            bull_w += w
        elif bias == "bearish":
            bear_w += w

        ema_txt = ""
        try:
            if ema20 is not None and ema50 is not None:
                ema_txt = "EMA20>EMA50" if float(ema20) > float(ema50) else "EMA20<EMA50"
        except Exception:
            pass

        detail = (
            f"إطار `{primary_tf}` | سعر `{price}` | RSI `{rsi:.1f}` | ADX `{adx:.0f}` | "
            f"confluence `{cscore}` | multi-TF `{mtf_bias}` ({mtf_str:.0%})"
            + (f" | {ema_txt}" if ema_txt else "")
        )
        rows.append({
            "symbol": sym, "bias": bias, "strength": strength,
            "label": label, "detail": detail,
            "adx": adx, "rsi": rsi, "cscore": cscore, "mtf": mtf,
            "primary_tf": primary_tf,
        })

    # --- ملخص جماعي ---
    total = bull_w + bear_w
    if total <= 0:
        market_bias = "غير محدد"
        market_pct = 0
    elif bull_w > bear_w * 1.15:
        market_bias = "صاعد"
        market_pct = int(round(100 * bull_w / total))
    elif bear_w > bull_w * 1.15:
        market_bias = "هابط"
        market_pct = int(round(100 * bear_w / total))
    else:
        market_bias = "متذبذب / مختلط"
        market_pct = int(round(100 * max(bull_w, bear_w) / total)) if total else 0

    n_bull = sum(1 for r in rows if r["bias"] == "bullish")
    n_bear = sum(1 for r in rows if r["bias"] == "bearish")
    n_neu = sum(1 for r in rows if r["bias"] not in ("bullish", "bearish"))
    n_ok = sum(1 for r in rows if r["bias"] != "unknown")

    lines.append("### الانحياز العام للمجموعة")
    lines.append(
        f"• **الاتجاه الجماعي:** `{market_bias}`"
        + (f" (ترجيح ≈ {market_pct}%)" if total > 0 else "")
    )
    lines.append(
        f"• **توزيع الأزواج:** صاعد `{n_bull}` · هابط `{n_bear}` · عرضي/غير حاسم `{n_neu}` · بدون بيانات `{len(rows) - n_ok}`"
    )
    lines.append("")

    # ترتيب حسب القوة
    ranked = sorted(
        [r for r in rows if r["bias"] != "unknown"],
        key=lambda r: r["strength"],
        reverse=True,
    )
    lines.append("### اتجاه كل زوج (من الأقوى للأضعف)")
    if not ranked:
        lines.append("_لا تتوفر بيانات فنية كافية من cTrader._")
    for r in ranked:
        icon = "🟢" if r["bias"] == "bullish" else ("🔴" if r["bias"] == "bearish" else "⚪")
        lines.append(
            f"{icon} **{r['symbol']}** — {r['label']} · قوة `{r['strength']:.0f}/100`\n"
            f"  _{r['detail']}_"
        )
    lines.append("")

    # قطاعات مبسّطة
    sectors = {
        "فوركس رئيسي": {"EURUSD", "GBPUSD", "USDJPY", "USDCHF", "AUDUSD", "NZDUSD", "USDCAD", "EURGBP", "GBPJPY"},
        "معادن/طاقة": {"XAUUSD", "XAGUSD", "USOIL", "UKOIL"},
        "كريبتو": {"BTCUSD", "ETHUSD", "SOLUSD"},
    }
    lines.append("### ملخص قطاعي (من الأزواج المختارة)")
    any_sector = False
    for name, members in sectors.items():
        subset = [r for r in rows if r["symbol"] in members and r["bias"] != "unknown"]
        if not subset:
            continue
        any_sector = True
        sb = sum(1 for r in subset if r["bias"] == "bullish")
        ss = sum(1 for r in subset if r["bias"] == "bearish")
        avg_s = sum(r["strength"] for r in subset) / len(subset)
        if sb > ss:
            sec_bias = "صاعد"
        elif ss > sb:
            sec_bias = "هابط"
        else:
            sec_bias = "مختلط"
        lines.append(
            f"• **{name}:** {sec_bias} — صاعد {sb}/{len(subset)} · متوسط قوة `{avg_s:.0f}`"
        )
    if not any_sector:
        lines.append("_لا قطاعات قياسية ضمن الاختيار الحالي._")
    lines.append("")

    # إشارات عملية
    lines.append("### ملاحظات عملية")
    strong_bull = [r["symbol"] for r in ranked if r["bias"] == "bullish" and r["strength"] >= 65]
    strong_bear = [r["symbol"] for r in ranked if r["bias"] == "bearish" and r["strength"] >= 65]
    weak = [r["symbol"] for r in ranked if r["strength"] < 40]
    if strong_bull:
        lines.append(f"• اتجاهات صاعدة واضحة: `{', '.join(strong_bull)}`")
    if strong_bear:
        lines.append(f"• اتجاهات هابطة واضحة: `{', '.join(strong_bear)}`")
    if weak:
        lines.append(f"• إشارات ضعيفة/عرضية (تجنّب المطاردة): `{', '.join(weak)}`")
    if market_bias in ("متذبذب / مختلط", "غير محدد"):
        lines.append("• السوق ضمن المجموعة **غير موحّد** — فضّل انتظار تأكيد multi-TF أو التوصية الشاملة.")
    elif market_pct >= 70:
        lines.append(f"• انحياز جماعي **واضح ({market_bias})** — راقب الأزواج المتوافقة مع الاتجاه العام.")
    lines.append("")
    lines.append(
        "_مصدر البيانات: cTrader فقط · لا توصية دخول فردية هنا · "
        "للدخول استخدم «التوصية الموحدة الشاملة» + فلتر الجودة._"
    )
    return "\n".join(lines)


# ==================== AI Specific Analysis Prompts ====================

async def run_specific_analysis(analysis_type: str, aggregated_data: Dict[str, Any], selected_tfs: List[str],
                                 history_text: str = "", signal_history_text: str = "") -> str:
    symbols_list_str = ", ".join(aggregated_data['symbols'])
    tfs_list_str = ", ".join(selected_tfs)

    # --- التقويم الاقتصادي / الأخبار / الرؤية الفنية: بيانات خام مباشرة، بدون أي استدعاء
    # للذكاء الاصطناعي إطلاقاً. فقط تُجلب وتُنسَّق وتُخزَّن كما هي — التحليل بالذكاء الاصطناعي
    # محجوز حصرياً لـ "التوصية الموحدة الشاملة" (full) لتوفير استهلاك حصة NVIDIA API.
    if analysis_type == "forex_factory":
        events = aggregated_data.get("forex_factory") or []
        if not events:
            body = "لا تتوفر بيانات تقويم اقتصادي حالياً من المصدر."
        else:
            lines = []
            for ev in events:
                if isinstance(ev, dict):
                    parts = [f"{k}: {v}" for k, v in ev.items() if v not in (None, "")]
                    lines.append("- " + " | ".join(parts))
                else:
                    lines.append(f"- {ev}")
            body = "\n".join(lines)
        return f"📅 **التقويم الاقتصادي (Forex Factory) — بيانات خام (بدون تحليل AI)**\nالأزواج: {symbols_list_str}\n\n{body}"

    if analysis_type == "finnhub":
        news = aggregated_data.get("finnhub_news") or []
        body = "\n".join(news) if news else "لا تتوفر أخبار حالية من Finnhub."
        return f"📰 **الأخبار الفورية (Finnhub) — بيانات خام (بدون تحليل AI)**\nالأزواج: {symbols_list_str}\n\n{body}"

    if analysis_type == "tradingview":
        rss = aggregated_data.get("tradingview_rss") or []
        body = "\n".join(rss) if rss else "لا تتوفر بيانات RSS حالية من TradingView."
        tech = aggregated_data.get("technical_summary", "")
        return (
            f"📉 **الرؤية الفنية (TradingView) — بيانات خام (بدون تحليل AI)**\n"
            f"الأزواج: {symbols_list_str} | الأطر: {tfs_list_str}\n\n"
            f"{body}\n\n"
            f"**المؤشرات الفنية الحية (RSI/EMA/ATR/Pivot):**\n{tech}"
        )

    if analysis_type == "trend":
        return build_general_trend_report(aggregated_data, selected_tfs)

    # --- من هنا فصاعداً: فقط "التوصية الموحدة الشاملة" تستدعي الذكاء الاصطناعي فعلياً ---
    # مسار Ensemble Multi-Agent (افتراضي) أو البرومبت الأحادي القديم كاحتياطي
    if analysis_type == "full" and USE_ENSEMBLE_AGENTS:
        return await run_ensemble_pipeline(aggregated_data, history_text, signal_history_text)

    if not ai_client:
        return "❌ **خطأ:** مفتاح NVIDIA Build API غير متوفر في متغيرات البيئة."

    current_dt_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    grounding_notice = f"""
        CRITICAL GROUNDING RULES (must follow strictly):
        - The current real-world date and time is: {current_dt_str}.
        - You MUST treat this as "now". Do NOT reference or imply any news, events, prices,
          or "recent" developments from before this date as if they were current.
        - You MUST rely ONLY on the data explicitly provided below in this prompt.
          Do NOT use any market knowledge, news, or events from your training data,
          even if it feels relevant — your training data has a cutoff far in the past
          relative to {current_dt_str} and using it will produce outdated, incorrect analysis.
        - If a data section below says "No news items available", "No RSS items available",
          or is empty, you MUST explicitly say so in Arabic (e.g. "لا تتوفر بيانات حالية لهذا المصدر
          حاليًا") for that section instead of inventing headlines, events, or dates.
        - LANGUAGE (STRICT): Your ENTIRE response MUST be written in Arabic, with no exceptions.
          Do not switch to English at any point, even for headings, labels, or transitions.
          The only English allowed is: symbol tickers (e.g. XAUUSD), numeric values, and the
          literal words BUY / SELL / NEUTRAL when paired with their Arabic translation as
          instructed below. Any other English word is a failure to follow instructions.
    """

    memory_block = (
        f"""
        --- الذاكرة التاريخية (تحاليل سابقة لنفس الأزواج والأطر، الأقدم أولاً) ---
        {history_text}
        ملاحظة: قارن قراءتك الحالية بهذه التحاليل السابقة إن وُجدت، ونوّه صراحة إلى
        أي تغيّر في الاتجاه أو التوصية أو استمرارية السيناريو السابق.
        """
        if history_text else ""
    )

    signal_memory_block = (
        f"""
        --- ذاكرة التعلّم من التوصيات والنتائج والظروف المشابهة وتداخل الأزواج ---
        {signal_history_text}

        تعليمات تعلّم إلزامية (يجب تطبيقها في تقييمك):
        1) تعلّم من النتائج: عندما ترى "النتيجة:" بجانب توصية سابقة، اعتبرها درساً —
           إن نجحت ظروف مشابهة سابقاً فزِد ثقتك بحذر؛ إن فشلت فخفّض الثقة أو غيّر الاتجاه.
        2) الظروف المشابهة: إن وُجد قسم "ظروف فنية مشابهة"، قارن RSI/EMA الحالية بتلك
           الحالات واستنتج ما الذي نجح أو فشل تحت نفس النظام الفني تقريباً.
        3) تداخل الأزواج: إن وُجدت إشارات قوية حديثة على أزواج أخرى، راعِ الكوروليشن
           (مثلاً EURUSD↔GBPUSD، الذهب↔الدولار، المؤشرات معاً). إذا كان زوج مرتبط يحمل
           توصية قوية واضحة، اذكر ذلك صراحة وكيف يؤثر على انحيازك للزوج الحالي.
        4) لا تكرر توصيات متضاربة بلا مبرر لنفس الزوج خلال فترات قصيرة؛ نوّه بأي انعكاس
           أو استمرارية مقارنة بالسجل.
        """
        if signal_history_text else ""
    )

    # من هنا فصاعداً نصل فقط في حالة "full" (التوصية الموحدة الشاملة) — وهي الحالة
    # الوحيدة المتبقية التي تستدعي الذكاء الاصطناعي فعلياً.
    prompt = f"""
    {grounding_notice}
    You are an Institutional Master Trader. Generate a Full Trading Recommendation for: [{symbols_list_str}]
    Shared Timeframes: [{tfs_list_str}]

    DATA HIERARCHY (STRICT):
    - PRIMARY source for ALL prices, OHLC candles, RSI/EMA/ATR/Pivot, and account/risk context: cTrader Open API.
    - TradingView is ONLY a FILTER / secondary confirmation (headlines, broad bias) — never override cTrader prices or levels.
    - Finnhub = news filter. Forex Factory = economic calendar / news-risk filter.
    - If cTrader technicals conflict with TradingView headlines, prefer cTrader numbers for Entry/SL/TP and explain the conflict in Arabic.

    --- ACCOUNT SNAPSHOT (cTrader) ---
    {aggregated_data.get('account_snapshot', 'N/A')}

    --- AGGREGATED DATA ---
    Calendar (Forex Factory filter): {json.dumps(aggregated_data['forex_factory'], indent=2)}
    News (Finnhub filter): {chr(10).join(aggregated_data['finnhub_news']) if aggregated_data['finnhub_news'] else 'No news items available.'}
    TradingView (FILTER only — not primary price): {chr(10).join(aggregated_data['tradingview_rss']) if aggregated_data['tradingview_rss'] else 'No RSS items available.'}
    Live Technical Data PRIMARY from cTrader (RSI/EMA/ATR/Pivot/price per symbol/timeframe):
    {aggregated_data['technical_summary']}
    {memory_block}
    {signal_memory_block}
    For EVERY symbol in [{symbols_list_str}], output — in this exact order, one symbol fully before
    moving to the next — ALL of the following:

    1. Symbol name & timeframes analyzed.
    2. Integrated Fundamental & Technical Assessment (a few sentences in Arabic combining the
       Calendar/News/TradingView context together with the Live Technical Data).
    3. Immediately after that assessment (directly under it, not deferred to a summary at the end
       of the whole report), output this EXACT structured signal block in Arabic and nothing else
       inside the block itself:

       🟢 [SYMBOL] — شراء (BUY)
       (use 🔴 instead of 🟢 and بيع (SELL) instead of شراء if the combined fundamental+technical
       bias is bearish; use ⚪ and لا توجد إشارة (NEUTRAL) only if there is no usable Live Technical
       Data for this symbol, or fundamentals and technicals strongly conflict)
       الدخول: [رقم]
       SL: [رقم]
       TP1: [رقم]
       TP2: [رقم]
       R:R: [رقم]:1

    Base Entry/SL/TP1/TP2 strictly on the actual last_price, ATR, and Pivot/R1/S1/R2/S2 values
    given in Live Technical Data — never invent arbitrary numbers detached from them — but the
    BUY/SELL/NEUTRAL direction itself must reflect the full combined picture (fundamentals +
    technicals together), not technicals alone. If a symbol has no usable Live Technical Data,
    output the ⚪ neutral block for it with "غير متوفر" instead of numbers.

    After finishing ALL symbols this way, end with one short closing paragraph in Arabic
    summarizing the overall market bias across all symbols together, explicitly mentioning any
    cross-pair correlation or strong signal on a related pair that influenced your view
    (this final paragraph is the ONLY place a multi-symbol summary belongs — do not repeat
    individual signal blocks there).
    """

    max_retries = 3
    base_delay = 8  # ثوانٍ — يتضاعف مع كل محاولة (backoff أُسّي)
    for attempt in range(1, max_retries + 1):
        try:
            loop = asyncio.get_running_loop()
            response = await loop.run_in_executor(
                None,
                lambda: ai_client.chat.completions.create(
                    model=AI_MODEL,
                    messages=[{"role": "user", "content": prompt}],
                    temperature=0.3,
                    max_tokens=4096,
                )
            )
            return response.choices[0].message.content
        except Exception as e:
            is_rate_limit = "429" in str(e) or "Too Many Requests" in str(e)
            if is_rate_limit and attempt < max_retries:
                wait_s = base_delay * attempt
                logger.warning(f"⏳ NVIDIA Build: تجاوز حد الطلبات (429) — إعادة محاولة {attempt}/{max_retries} بعد {wait_s} ثانية...")
                await asyncio.sleep(wait_s)
                continue
            logger.error(f"NVIDIA Build Analysis Error: {e}")
            return f"❌ **خطأ أثناء توليد التحليل عبر الذكاء الاصطناعي:**\n`{str(e)}`"

async def build_history_text(analysis_type: str, symbols: List[str], tfs: List[str]) -> str:
    """يجلب آخر 3 تحاليل سابقة لنفس التركيبة من Firestore ليُمرَّرا كـ'ذاكرة' إلى الذكاء الاصطناعي."""
    history = await db.get_report_history(analysis_type, symbols, tfs, limit=3)
    if not history:
        return ""
    lines = []
    for h in reversed(history):  # الأقدم أولاً حتى يقرأها النموذج بترتيب زمني منطقي
        ts = h["created_at"].strftime("%Y-%m-%d %H:%M UTC")
        snippet = h["report_text"][:600]
        lines.append(f"[{ts}]\n{snippet}\n")
    return "\n".join(lines)



def _primary_tech_for_symbol(symbols_technical: Dict[str, Any], symbol: str, preferred_tfs: List[str]) -> Dict[str, Any]:
    """يستخرج أفضل لقطة مؤشرات فنية متاحة لزوج (يفضّل H1 ثم أول إطار صالح)."""
    by_tf = (symbols_technical or {}).get(symbol) or {}
    for pref in list(preferred_tfs) + ["H1", "H4", "M15", "M1", "D1"]:
        ind = by_tf.get(pref)
        if ind and "error" not in ind:
            return {**ind, "_tf": pref}
    for tf, ind in by_tf.items():
        if ind and "error" not in ind:
            return {**ind, "_tf": tf}
    return {}


async def build_signal_history_text(symbols: List[str], limit_per_symbol: int = 10,
                                      symbols_technical: Optional[Dict[str, Any]] = None) -> str:
    """
    ذاكرة تعلّم محسّنة للوكلاء:
      1) إحصاءات كمية (win-rate) لكل زوج من السجل.
      2) سجل توصيات + نتيجة لاحقة تقديرية.
      3) ظروف فنية مشابهة مرتبة بالتشابه (RSI/EMA/ATR).
      4) إشارات قوية من أزواج مرتبطة (كوروليشن).
    """
    blocks: List[str] = []
    # خريطة كوروليشن مبسّطة لتوجيه الانتباه
    related = {
        "EURUSD": ["GBPUSD", "USDCHF", "EURGBP"],
        "GBPUSD": ["EURUSD", "EURGBP", "GBPJPY"],
        "USDJPY": ["GBPJPY", "EURUSD"],
        "XAUUSD": ["USDJPY", "EURUSD", "BTCUSD"],
        "BTCUSD": ["ETHUSD", "SOLUSD", "XAUUSD"],
        "ETHUSD": ["BTCUSD", "SOLUSD"],
        "AUDUSD": ["NZDUSD", "XAUUSD"],
        "NZDUSD": ["AUDUSD"],
        "USDCAD": ["USDJPY", "EURUSD"],
        "USDCHF": ["EURUSD", "USDJPY"],
        "EURGBP": ["EURUSD", "GBPUSD"],
        "GBPJPY": ["GBPUSD", "USDJPY"],
    }

    # --- 0) إحصاءات كمية متوازية ---
    stats_lines = ["=== إحصاءات التعلّم الكمية (من نتائج الإشارات السابقة) ==="]
    learning_stats: Dict[str, Any] = {}
    try:
        stats_list = await asyncio.gather(
            *[db.get_symbol_learning_stats(sym, lookback=max(12, limit_per_symbol * 2)) for sym in symbols]
        )
        for st in stats_list:
            learning_stats[st["symbol"]] = st
            wr = st.get("overall_win_rate")
            wr_s = f"{wr:.0%}" if wr is not None else "—"
            bwr = st.get("buy_win_rate")
            swr = st.get("sell_win_rate")
            bwr_s = f"{bwr:.0%}" if bwr is not None else "—"
            swr_s = f"{swr:.0%}" if swr is not None else "—"
            stats_lines.append(
                f"  {st['symbol']}: عينات={st.get('sample_n', 0)} | "
                f"نجاح إجمالي={wr_s} | شراء={bwr_s} (n={st.get('buy_n', 0)}) | "
                f"بيع={swr_s} (n={st.get('sell_n', 0)}) | edge={st.get('edge_hint')}"
            )
        blocks.append("\n".join(stats_lines))
    except Exception as e:
        logger.error(f"learning stats error: {e}")

    # --- 1) سجل كل زوج + نتيجة (جلب متوازٍ للنتائج) ---
    async def _hist_block(sym: str) -> Optional[str]:
        hist = await db.get_symbol_signal_history(sym, limit=limit_per_symbol)
        if not hist:
            return None
        outcomes = await asyncio.gather(*[
            db.infer_signal_outcome(
                sym, h.get("entry"), h.get("sl"), h.get("tp1"),
                h.get("direction") or "", h["created_at"],
            )
            for h in hist
        ])
        lines = [f"سجل توصيات {sym} (الأقدم→الأحدث) + نتيجة لاحقة:"]
        for h, outcome in zip(hist, outcomes):
            ts = h["created_at"].strftime("%Y-%m-%d %H:%M UTC")
            entry_part = f", دخول={h['entry']}, SL={h['sl']}, TP1={h['tp1']}" if h.get("entry") else ""
            rsi_part = f", RSI={h['rsi_14']}" if h.get("rsi_14") is not None else ""
            outcome_part = f" → النتيجة: {outcome}" if outcome else ""
            lines.append(f"  [{ts}] {h['emoji']} {h['direction']}{entry_part}{rsi_part}{outcome_part}")
        st = learning_stats.get(sym) or {}
        if st.get("edge_hint") == "negative":
            lines.append("  ⚠️ تحذير تعلّم: معدل النجاح التاريخي ضعيف — خفّض الثقة أو فضّل NEUTRAL ما لم تكن الإشارة الفنية قوية جداً.")
        elif st.get("edge_hint") == "positive":
            lines.append("  ✅ تلميح تعلّم: هذا الزوج أظهر edge إيجابياً نسبياً في الفترة الأخيرة.")
        return "\n".join(lines)

    hist_blocks = await asyncio.gather(*[_hist_block(sym) for sym in symbols])
    for b in hist_blocks:
        if b:
            blocks.append(b)

    # --- 2) ظروف مشابهة مرتبة بالتشابه ---
    if symbols_technical:
        similar_blocks = []

        async def _similar_block(sym: str) -> Optional[str]:
            tech_by_tf = symbols_technical.get(sym) or {}
            indicators = None
            for pref in ("H1", "H4", "M15", "M1", "D1"):
                if pref in tech_by_tf and "error" not in (tech_by_tf.get(pref) or {}):
                    indicators = tech_by_tf[pref]
                    break
            if not indicators:
                for v in tech_by_tf.values():
                    if v and "error" not in v:
                        indicators = v
                        break
            if not indicators:
                return None
            similar = await db.get_similar_condition_signals(
                sym,
                indicators.get("rsi_14"),
                indicators.get("ema_20"),
                indicators.get("ema_50"),
                limit=6,
                atr=indicators.get("atr_14"),
            )
            if not similar:
                return None
            outcomes = await asyncio.gather(*[
                db.infer_signal_outcome(
                    sym, h.get("entry"), h.get("sl"), h.get("tp1"),
                    h.get("direction") or "", h["created_at"],
                )
                for h in similar
            ])
            wins = sum(1 for o in outcomes if o and ("TP1" in o or "✓" in o or "مع الاتجاه" in o))
            losses = sum(1 for o in outcomes if o and ("SL" in o or "✗" in o or "ضد" in o))
            lines = [
                f"ظروف مشابهة لـ {sym} (RSI≈{indicators.get('rsi_14')}, ATR≈{indicators.get('atr_14')}): "
                f"من العيّنة المشابهة ≈ نجاح {wins} / فشل {losses}"
            ]
            for h, outcome in zip(similar, outcomes):
                ts = h["created_at"].strftime("%Y-%m-%d %H:%M UTC")
                outcome_part = f" → {outcome}" if outcome else ""
                lines.append(
                    f"  [{ts}] {h['emoji']} {h['direction']}, RSI={h.get('rsi_14')}, "
                    f"دخول={h.get('entry')}{outcome_part}"
                )
            if losses > wins and (wins + losses) >= 3:
                lines.append("  ⚠️ الإعدادات المشابهة فشلت أكثر مما نجحت — تجنّب تكرار نفس الاتجاه بثقة عالية.")
            elif wins > losses and (wins + losses) >= 3:
                lines.append("  ✅ الإعدادات المشابهة كانت ناجحة نسبياً — يمكن رفع الثقة بحذر.")
            return "\n".join(lines)

        sim_results = await asyncio.gather(*[_similar_block(sym) for sym in symbols])
        for b in sim_results:
            if b:
                similar_blocks.append(b)
        if similar_blocks:
            blocks.append("=== ظروف فنية مشابهة (مرتبة بالتشابه) ===\n" + "\n\n".join(similar_blocks))

    # --- 3) تداخل أزواج مرتبطة ---
    related_set = set()
    for sym in symbols:
        for r in related.get(sym, []):
            if r not in symbols:
                related_set.add(r)
    strong = await db.get_recent_strong_signals(
        exclude_symbols=list(symbols),
        limit=12,
    )
    if strong:
        lines = ["إشارات قوية حديثة (أولوية للأزواج المرتبطة):"]
        # رتّب: المرتبطة أولاً
        def _prio(h):
            return 0 if h.get("symbol") in related_set else 1
        for h in sorted(strong, key=_prio):
            ts = h["created_at"].strftime("%Y-%m-%d %H:%M UTC")
            tag = " [مرتبط]" if h.get("symbol") in related_set else ""
            lines.append(
                f"  [{ts}] {h['emoji']} {h['symbol']}{tag} — {h['direction']}"
                + (f", دخول={h['entry']}" if h.get("entry") else "")
            )
        blocks.append("=== تداخل الأزواج / كوروليشن ===\n" + "\n".join(lines))

    return "\n\n".join(blocks)




def _clamp(x: float, lo: float = 0.0, hi: float = 1.0) -> float:
    return max(lo, min(hi, x))


def _smoothstep(edge0: float, edge1: float, x: float) -> float:
    """انتقال سلس 0→1 بين حدّين (أفضل من عتبات حادة)."""
    if edge1 <= edge0:
        return 1.0 if x >= edge1 else 0.0
    t = _clamp((x - edge0) / (edge1 - edge0))
    return t * t * (3.0 - 2.0 * t)


def compute_signal_quality_score(
    direction: str,
    ind: Dict[str, Any],
    mtf: Dict[str, Any],
    news: Optional[Dict[str, Any]] = None,
    learning_side: Optional[Dict[str, Any]] = None,
    timeframe: Optional[str] = None,
) -> Dict[str, Any]:
    """
    خوارزمية جودة v3 — أوزان قابلة للضبط + منحنيات ناعمة + مكافأة توافق:

      confluence  W1  توافق المؤشرات مع الاتجاه
      multi_tf    W2  اتفاق الأطر
      trend       W3  ADX + DI + بنية EMA
      momentum    W4  MACD + زخم + RSI
      structure   W5  حجم / امتداد / Pivot
      learning    W6  نجاح تاريخي للجانب

    تُرجع: score, grade (A..F), breakdown, vetoes, aligned, components_norm
    """
    news = news or {}
    learning_side = learning_side or {}
    direction = (direction or "NEUTRAL").upper()
    # أوزان افتراضية (مجموع 100) — قابلة للتعديل من البيئة
    w_conf = float(os.getenv("Q_W_CONFLUENCE", "24"))
    w_mtf = float(os.getenv("Q_W_MTF", "20"))
    w_trend = float(os.getenv("Q_W_TREND", "16"))
    w_mom = float(os.getenv("Q_W_MOMENTUM", "18"))
    w_struct = float(os.getenv("Q_W_STRUCTURE", "12"))
    w_learn = float(os.getenv("Q_W_LEARNING", "10"))
    w_sum = w_conf + w_mtf + w_trend + w_mom + w_struct + w_learn
    if w_sum <= 0:
        w_sum = 100.0
    # تطبيع الأوزان إلى 100
    scale = 100.0 / w_sum
    w_conf, w_mtf, w_trend, w_mom, w_struct, w_learn = (
        w_conf * scale, w_mtf * scale, w_trend * scale,
        w_mom * scale, w_struct * scale, w_learn * scale,
    )

    zeros = {
        "confluence": 0.0, "multi_tf": 0.0, "trend": 0.0,
        "momentum": 0.0, "structure": 0.0, "learning": 0.0,
    }
    if direction not in ("BUY", "SELL"):
        return {
            "score": 0, "grade": "F", "breakdown": zeros,
            "vetoes": ["لا اتجاه قابل للتقييم"], "aligned": False,
            "components_norm": zeros,
        }

    cscore = float(ind.get("confluence_score") or 0)
    bias = (ind.get("bias") or "neutral")
    score_buy = float(ind.get("score_buy") or 0)
    score_sell = float(ind.get("score_sell") or 0)
    adx = float(ind.get("adx_14") or 0)
    plus_di = float(ind.get("plus_di") or 0)
    minus_di = float(ind.get("minus_di") or 0)
    rsi = float(ind.get("rsi_14") or 50)
    macd_h = float(ind.get("macd_hist") or 0)
    mom = float(ind.get("momentum_10_pct") or 0)
    vol_r = float(ind.get("volume_ratio") or 1.0)
    ema_slope = float(ind.get("ema20_slope_pct") or 0)
    price = ind.get("last_price")
    atr = ind.get("atr_14")
    ema20 = ind.get("ema_20")
    ema50 = ind.get("ema_50")
    r1, s1 = ind.get("r1"), ind.get("s1")
    mtf_bias = (mtf.get("bias") or "neutral")
    mtf_str = float(mtf.get("strength") or 0)
    tf = (timeframe or ind.get("timeframe") or "").upper()

    want_bull = direction == "BUY"
    vetoes: List[str] = []
    norm: Dict[str, float] = {}  # 0..1 لكل مكوّن قبل الوزن

    # ===== 1) confluence (norm 0..1) =====
    align = 1.0 if (
        (want_bull and bias == "bullish") or ((not want_bull) and bias == "bearish")
    ) else (0.4 if bias == "neutral" else 0.05)
    total_ss = score_buy + score_sell
    edge = (score_buy / total_ss) if want_bull and total_ss > 0 else (
        (score_sell / total_ss) if (not want_bull) and total_ss > 0 else 0.5
    )
    conf_n = _clamp(0.50 * (cscore / 100.0) + 0.30 * align + 0.20 * edge)
    if cscore <= 0:
        conf_n = 0.25
    norm["confluence"] = conf_n

    # ===== 2) multi_tf =====
    if mtf_bias == "bullish":
        mtf_n = mtf_str if want_bull else (1.0 - mtf_str) * 0.2
    elif mtf_bias == "bearish":
        mtf_n = mtf_str if (not want_bull) else (1.0 - mtf_str) * 0.2
    else:
        mtf_n = 0.40  # محايد: لا دعم قوي
    # مكافأة إن عدد الأطر المستخدمة أكبر
    n_tfs = len(mtf.get("timeframes_used") or [])
    if n_tfs >= 3 and (
        (want_bull and mtf_bias == "bullish") or ((not want_bull) and mtf_bias == "bearish")
    ):
        mtf_n = _clamp(mtf_n + 0.08)
    norm["multi_tf"] = _clamp(mtf_n)

    conflict_thr = float(os.getenv("PRED_MTF_CONFLICT", "0.62"))
    if want_bull and mtf_bias == "bearish" and mtf_str >= conflict_thr:
        vetoes.append(f"multi-TF هابط قوي ({mtf_str:.0%})")
    if (not want_bull) and mtf_bias == "bullish" and mtf_str >= conflict_thr:
        vetoes.append(f"multi-TF صاعد قوي ({mtf_str:.0%})")

    # ===== 3) trend: ADX منحنى جرس + DI + EMA =====
    # ADX مثالي ~22–38
    if adx <= 0:
        adx_n = 0.25
    else:
        # قمة عند 28، ينخفض تحت 15 وفوق 50
        adx_n = _smoothstep(12, 22, adx) * (1.0 - 0.55 * _smoothstep(40, 55, adx))
        adx_n = _clamp(adx_n)
    # DI يوافق الاتجاه
    di_n = 0.5
    if plus_di or minus_di:
        if want_bull:
            di_n = _clamp(0.5 + (plus_di - minus_di) / 40.0)
        else:
            di_n = _clamp(0.5 + (minus_di - plus_di) / 40.0)
    # EMA structure
    ema_n = 0.5
    try:
        if ema20 is not None and ema50 is not None:
            e20, e50 = float(ema20), float(ema50)
            spread = abs(e20 - e50) / (abs(e50) + 1e-12)
            if want_bull and e20 > e50:
                ema_n = _clamp(0.65 + min(0.25, spread * 50))
            elif (not want_bull) and e20 < e50:
                ema_n = _clamp(0.65 + min(0.25, spread * 50))
            else:
                ema_n = 0.15
    except Exception:
        pass
    # ميل EMA20
    slope_n = 0.5
    if want_bull:
        slope_n = _clamp(0.5 + ema_slope * 8)  # slope_pct
    else:
        slope_n = _clamp(0.5 - ema_slope * 8)
    trend_n = _clamp(0.40 * adx_n + 0.25 * di_n + 0.25 * ema_n + 0.10 * slope_n)
    norm["trend"] = trend_n
    if adx and adx < 14:
        vetoes.append(f"سوق عرضي ADX={adx:.0f}")

    # ===== 4) momentum =====
    # MACD: إشارة مستمرة عبر tanh تقريبي
    macd_sign = 1.0 if want_bull else -1.0
    macd_raw = macd_h * macd_sign
    # تطبيع تقريبي: قيم FX صغيرة جداً — نستخدم إشارة + شدة محدودة
    macd_n = _clamp(0.55 + (0.35 if macd_raw > 0 else -0.35) + _clamp(abs(macd_h) * 80, 0, 0.1) * (1 if macd_raw > 0 else -1))

    mom_sign = mom if want_bull else -mom
    mom_n = _clamp(0.5 + _clamp(mom_sign / 0.4, -0.45, 0.45))

    # RSI: منحنى حسب الاتجاه (شراء يفضّل 30–58، بيع 42–70)
    if want_bull:
        # قمة حول 45، عقوبة فوق 72 وتحت 20
        rsi_n = _smoothstep(22, 35, rsi) * (1.0 - 0.85 * _smoothstep(68, 78, rsi))
        if rsi >= 72:
            vetoes.append(f"RSI مرتفع جداً للشراء ({rsi:.0f})")
    else:
        rsi_n = _smoothstep(22, 32, 100 - rsi) * (1.0 - 0.85 * _smoothstep(68, 78, 100 - rsi))
        # re-express: for sell, high RSI good mid-high
        rsi_n = _smoothstep(28, 40, rsi) * (1.0 - 0.85 * _smoothstep(28, 22, rsi)) if False else rsi_n
        # clearer sell RSI curve
        if rsi >= 55:
            rsi_n = _clamp(_smoothstep(50, 62, rsi) * (1.0 - 0.7 * _smoothstep(78, 88, rsi)))
        elif rsi <= 30:
            rsi_n = 0.1
            vetoes.append(f"RSI منخفض جداً للبيع ({rsi:.0f})")
        else:
            rsi_n = 0.35 + (rsi - 30) / 50.0 * 0.3
    momentum_n = _clamp(0.40 * macd_n + 0.30 * mom_n + 0.30 * _clamp(rsi_n))
    norm["momentum"] = momentum_n

    # ===== 5) structure =====
    vol_n = _clamp(0.35 + 0.40 * _smoothstep(0.7, 1.25, vol_r) - 0.25 * _smoothstep(1.8, 3.0, vol_r))
    # امتداد عن EMA50 بوحدة ATR
    ext_n = 0.55
    try:
        if price is not None and ema50 is not None and atr is not None and float(atr) > 0:
            dist = (float(price) - float(ema50)) / float(atr)
            if want_bull:
                # مثالي 0..1.6 ATR فوق المتوسط؛ عقوبة فوق 2.6
                ext_n = _smoothstep(-0.8, 0.2, dist) * (1.0 - 0.9 * _smoothstep(2.0, 3.2, dist))
                if dist > 2.8:
                    vetoes.append("مطاردة ممتدة فوق EMA50")
            else:
                ext_n = _smoothstep(-0.2, 0.8, -dist) * (1.0 - 0.9 * _smoothstep(2.0, 3.2, -dist))
                if dist < -2.8:
                    vetoes.append("مطاردة ممتدة تحت EMA50")
            ext_n = _clamp(ext_n)
    except Exception:
        pass
    pivot_n = 1.0
    try:
        if price is not None and atr is not None and float(atr) > 0:
            p, a = float(price), float(atr)
            if want_bull and r1 is not None:
                gap = (float(r1) - p) / a
                if 0 <= gap <= 0.45:
                    pivot_n = 0.35  # مقاومة قريبة
            if (not want_bull) and s1 is not None:
                gap = (p - float(s1)) / a
                if 0 <= gap <= 0.45:
                    pivot_n = 0.35
    except Exception:
        pass
    structure_n = _clamp(0.35 * vol_n + 0.45 * ext_n + 0.20 * pivot_n)
    norm["structure"] = structure_n

    # ===== 6) learning =====
    wr = learning_side.get("side_win_rate")
    n = int(learning_side.get("sample_n") or 0)
    if wr is None or n < 4:
        learn_n = 0.50
    else:
        # logistic-ish: 0.35→0.1 ، 0.5→0.5 ، 0.65→0.9
        learn_n = _clamp((float(wr) - 0.30) / 0.40)
        if n >= 6 and float(wr) < 0.35:
            vetoes.append(f"نجاح تاريخي ضعيف ({float(wr):.0%}, n={n})")
        # ثقة أعلى مع عينة أكبر
        if n >= 12 and float(wr) >= 0.55:
            learn_n = _clamp(learn_n + 0.08)
    norm["learning"] = learn_n

    # ===== أخبار =====
    if news.get("block_new_entries"):
        vetoes.append("حظر أخبار نشط")
    elif (news.get("volatility_risk") or "").lower() == "high":
        norm["structure"] = _clamp(norm["structure"] - 0.15)

    # ===== عقوبة إطار سريع (M1): يطلب جودة فنية أعلى =====
    tf_penalty = 0.0
    if tf == "M1":
        tf_penalty = 0.04
        if adx < 18:
            norm["trend"] = _clamp(norm["trend"] - 0.12)
    elif tf == "M15":
        tf_penalty = 0.02

    # ===== مكافأة توافق ثلاثي (confluence + mtf + trend) =====
    align_bonus = 0.0
    if (
        norm["confluence"] >= 0.55
        and norm["multi_tf"] >= 0.55
        and norm["trend"] >= 0.55
        and (
            (want_bull and bias == "bullish" and mtf_bias in ("bullish", "neutral"))
            or ((not want_bull) and bias == "bearish" and mtf_bias in ("bearish", "neutral"))
        )
    ):
        align_bonus = 4.0  # نقاط مطلقة على الـ100

    # تجميع موزون
    breakdown = {
        "confluence": round(norm["confluence"] * w_conf, 1),
        "multi_tf": round(norm["multi_tf"] * w_mtf, 1),
        "trend": round(norm["trend"] * w_trend, 1),
        "momentum": round(norm["momentum"] * w_mom, 1),
        "structure": round(norm["structure"] * w_struct, 1),
        "learning": round(norm["learning"] * w_learn, 1),
    }
    raw = sum(breakdown.values()) + align_bonus - (tf_penalty * 100.0 * 0.15)
    score = int(round(_clamp(raw, 0.0, 100.0)))

    # كبح إذا ≥4 مكوّنات ضعيفة (norm < 0.35)
    weak = sum(1 for v in norm.values() if v < 0.35)
    if weak >= 4:
        score = min(score, 42)
    elif weak >= 3:
        score = min(score, 55)

    # تباين المكوّنات: إشارة غير متوازنة
    vals = list(norm.values())
    mean_v = sum(vals) / len(vals)
    var_v = sum((v - mean_v) ** 2 for v in vals) / len(vals)
    if var_v > 0.12 and score > 60:
        score = max(50, score - 8)

    # Grade
    if score >= 80:
        grade = "A"
    elif score >= 68:
        grade = "B"
    elif score >= 55:
        grade = "C"
    elif score >= 42:
        grade = "D"
    else:
        grade = "F"

    aligned = (
        (want_bull and bias in ("bullish", "neutral") and mtf_bias != "bearish")
        or ((not want_bull) and bias in ("bearish", "neutral") and mtf_bias != "bullish")
    )

    return {
        "score": score,
        "grade": grade,
        "breakdown": breakdown,
        "components_norm": {k: round(v, 3) for k, v in norm.items()},
        "vetoes": vetoes,
        "aligned": aligned,
        "align_bonus": align_bonus,
        "timeframe": tf or None,
    }



def apply_prediction_quality_filter(
    decisions: List[Dict[str, Any]],
    fusion: Dict[str, Any],
    news: Optional[Dict[str, Any]] = None,
    learning_stats: Optional[Dict[str, Dict[str, Any]]] = None,
) -> List[Dict[str, Any]]:
    """
    فلتر دقة نهائي يستخدم compute_signal_quality_score ثم:
      - فيتو → NEUTRAL
      - quality دون العتبة → NEUTRAL / حجم مخفّض
      - إعادة بناء SL/TP من ATR
    """
    min_confluence = int(os.getenv("PRED_MIN_CONFLUENCE", "48"))
    hard_confluence = int(os.getenv("PRED_HARD_CONFLUENCE", "38"))
    mtf_conflict = float(os.getenv("PRED_MTF_CONFLICT", "0.62"))
    min_adx_trend = float(os.getenv("PRED_MIN_ADX", "16"))
    min_quality = int(os.getenv("PRED_MIN_QUALITY", "52"))
    sl_atr = float(os.getenv("PRED_SL_ATR", "1.4"))
    tp1_atr = float(os.getenv("PRED_TP1_ATR", "2.15"))
    tp2_atr = float(os.getenv("PRED_TP2_ATR", "3.3"))

    news = news or {}
    learning_stats = learning_stats or {}
    per = fusion.get("per_symbol") or {}
    out: List[Dict[str, Any]] = []

    for d in decisions:
        x = dict(d)
        sym = x.get("symbol")
        tech = per.get(sym) or {}
        ind = tech.get("primary_technicals") or {}
        mtf = tech.get("multi_tf") or {}
        direction = (x.get("direction") or "NEUTRAL").upper()
        if direction in ("LONG", "شراء"):
            direction = "BUY"
        if direction in ("SHORT", "بيع"):
            direction = "SELL"
        if direction not in ("BUY", "SELL", "NEUTRAL"):
            direction = "NEUTRAL"

        try:
            conf = float(x.get("confidence") or 0)
        except Exception:
            conf = 0.0

        notes: List[str] = []
        st = learning_stats.get(sym) or {}
        side_wr = st.get("buy_win_rate") if direction == "BUY" else st.get("sell_win_rate")
        learning_side = {
            "side_win_rate": side_wr,
            "sample_n": int(st.get("sample_n") or 0),
        }

        qinfo = compute_signal_quality_score(direction, ind, mtf, news=news, learning_side=learning_side, timeframe=ind.get("timeframe"))
        q = int(qinfo.get("score") or 0)
        breakdown = qinfo.get("breakdown") or {}
        vetoes = list(qinfo.get("vetoes") or [])

        cscore = int(ind.get("confluence_score") or 0)
        adx = float(ind.get("adx_14") or 0)
        mtf_bias = (mtf.get("bias") or "neutral")
        mtf_str = float(mtf.get("strength") or 0)
        price = ind.get("last_price")
        atr = ind.get("atr_14")

        # فيتوهات صريحة إضافية من العتبات البيئية
        if direction == "BUY" and mtf_bias == "bearish" and mtf_str >= mtf_conflict:
            if f"multi-TF" not in " ".join(vetoes):
                vetoes.append(f"تعارض multi-TF هابط ({mtf_str:.0%})")
        if direction == "SELL" and mtf_bias == "bullish" and mtf_str >= mtf_conflict:
            if f"multi-TF" not in " ".join(vetoes):
                vetoes.append(f"تعارض multi-TF صاعد ({mtf_str:.0%})")
        if direction in ("BUY", "SELL") and cscore and cscore < hard_confluence:
            vetoes.append(f"confluence ضعيف جداً ({cscore})")
        if news.get("block_new_entries") and direction != "NEUTRAL":
            if "حظر أخبار" not in " ".join(vetoes):
                vetoes.append("حظر أخبار نشط")

        # تطبيق الفيتو
        if direction in ("BUY", "SELL") and vetoes:
            # بعض الفيتوهات «ناعمة» لا تلغي وحدها إن كانت الجودة عالية جداً
            hard_keys = ("multi-TF", "confluence ضعيف", "حظر أخبار", "نجاح تاريخي ضعيف")
            hard = [v for v in vetoes if any(k in v for k in hard_keys)]
            if hard:
                direction, conf = "NEUTRAL", min(conf, 0.28)
                notes.extend(hard)
                q = min(q, 32)
            else:
                conf = min(conf, 0.5)
                notes.extend(vetoes)
                q = min(q, q)  # تبقى الدرجة المحسوبة

        if direction in ("BUY", "SELL") and cscore and cscore < min_confluence and cscore >= hard_confluence:
            conf = min(conf, 0.48)
            notes.append(f"confluence منخفض ({cscore})")

        if direction in ("BUY", "SELL") and adx and adx < min_adx_trend:
            conf = min(conf, 0.48)
            notes.append(f"ADX منخفض ({adx:.0f})")

        if (news.get("volatility_risk") or "").lower() == "high" and direction != "NEUTRAL":
            conf = min(conf, 0.5)
            x["size_hint"] = "reduce"
            notes.append("تقلب أخبار مرتفع")

        # عتبة الجودة
        if direction in ("BUY", "SELL") and q < min_quality:
            direction, conf = "NEUTRAL", min(conf, 0.3)
            notes.append(f"جودة {q} < {min_quality}")
        elif direction in ("BUY", "SELL") and q < min_quality + 10:
            conf = min(conf, 0.52)
            x["size_hint"] = "reduce"
            notes.append(f"جودة متوسطة {q} → حجم مخفّض")

        # مواءمة الثقة مع الجودة (تقليل ثقة وهمية من الـ LLM)
        if direction in ("BUY", "SELL"):
            conf_cap = 0.35 + (q / 100.0) * 0.55  # جودة 100 → سقف 0.90
            if conf > conf_cap:
                conf = conf_cap
                notes.append(f"سقف ثقة حسب الجودة ({conf_cap:.2f})")

        # SL/TP
        if direction in ("BUY", "SELL") and price and atr:
            try:
                p, a = float(price), float(atr)
                if a > 0:
                    digits = symbol_digits(sym)
                    round_n = digits
                    sl_m = sl_atr + (0.25 if adx >= 30 else 0.0)
                    tp1_m = tp1_atr
                    tp2_m = tp2_atr
                    if direction == "BUY":
                        x["entry"] = round(p, round_n)
                        x["sl"] = round(p - sl_m * a, round_n)
                        x["tp1"] = round(p + tp1_m * a, round_n)
                        x["tp2"] = round(p + tp2_m * a, round_n)
                    else:
                        x["entry"] = round(p, round_n)
                        x["sl"] = round(p + sl_m * a, round_n)
                        x["tp1"] = round(p - tp1_m * a, round_n)
                        x["tp2"] = round(p - tp2_m * a, round_n)
                    x["rr"] = f"{(tp1_m / sl_m):.2f}:1"
            except Exception:
                pass

        x["direction"] = direction
        x["confidence"] = round(max(0.0, min(0.92, conf)), 3)
        x["quality_score"] = q
        x["quality_grade"] = qinfo.get("grade")
        x["quality_breakdown"] = breakdown
        if direction == "NEUTRAL":
            x["size_hint"] = "skip"
        elif x.get("size_hint") not in ("reduce", "normal", "skip"):
            x["size_hint"] = "reduce" if conf < 0.58 or q < 60 else "normal"

        if notes:
            prev = (x.get("reason_ar") or "").strip()
            # ملخص مكوّنات الجودة الأقوى/الأضعف
            if breakdown:
                ranked = sorted(breakdown.items(), key=lambda kv: kv[1])
                weak = ranked[0]
                strong = ranked[-1]
                notes.append(f"مكوّنات: أقوى={strong[0]}({strong[1]}) أضعف={weak[0]}({weak[1]})")
            tag = " ؛ ".join(notes)
            x["reason_ar"] = (prev + " | " if prev else "") + f"فلتر دقة: {tag}"
        out.append(x)
    return out



def apply_learning_confidence_adjustments(
    decisions: List[Dict[str, Any]],
    learning_stats: Dict[str, Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """
    ضبط ثقة القرار بعد الوكيل التنفيذي باستخدام إحصاءات السجل:
      - edge سلبي → خفض الثقة أو تحويل إلى NEUTRAL إذا كانت الثقة أصلاً متوسطة
      - edge إيجابي ومتوافق مع الاتجاه الأقوى تاريخياً → رفع طفيف
    """
    out = []
    for d in decisions:
        x = dict(d)
        sym = x.get("symbol")
        st = learning_stats.get(sym) or {}
        direction = (x.get("direction") or "NEUTRAL").upper()
        try:
            conf = float(x.get("confidence") or 0)
        except Exception:
            conf = 0.0
        edge = st.get("edge_hint") or "insufficient_data"
        buy_wr = st.get("buy_win_rate")
        sell_wr = st.get("sell_win_rate")
        sample_n = int(st.get("sample_n") or 0)
        note = ""

        if direction in ("BUY", "SELL") and sample_n >= 5:
            side_wr = buy_wr if direction == "BUY" else sell_wr
            if side_wr is not None:
                if side_wr < 0.35 and conf < 0.75:
                    x["direction"] = "NEUTRAL"
                    x["confidence"] = min(conf, 0.3)
                    x["size_hint"] = "skip"
                    note = f"تعلّم: نجاح {direction} التاريخي ضعيف ({side_wr:.0%}) → NEUTRAL"
                elif side_wr < 0.45:
                    x["confidence"] = max(0.35, conf * 0.85)
                    note = f"تعلّم: خفض ثقة {direction} (نجاح {side_wr:.0%})"
                elif side_wr >= 0.60 and edge == "positive":
                    x["confidence"] = min(0.9, conf * 1.08 + 0.02)
                    note = f"تعلّم: تعزيز ثقة {direction} (نجاح {side_wr:.0%})"
            elif edge == "negative" and conf < 0.7:
                x["confidence"] = max(0.35, conf * 0.9)
                note = "تعلّم: edge سلبي إجمالي → خفض ثقة"
        if note:
            prev = (x.get("reason_ar") or "").strip()
            x["reason_ar"] = f"{prev} | {note}" if prev else note
        out.append(x)
    return out




async def attach_learning_stats(aggregated: Dict[str, Any], symbols: List[str]) -> Dict[str, Dict[str, Any]]:
    """يحسب إحصاءات التعلّم الكمية ويخزّنها على aggregated لاستخدام ضبط الثقة."""
    if not symbols:
        aggregated["learning_stats"] = {}
        return {}
    try:
        stats_list = await asyncio.gather(
            *[db.get_symbol_learning_stats(sym, lookback=24) for sym in symbols]
        )
        stats = {s["symbol"]: s for s in stats_list}
    except Exception as e:
        logger.error(f"attach_learning_stats error: {e}")
        stats = {}
    aggregated["learning_stats"] = stats
    return stats


def is_error_report(text: str) -> bool:
    """يكتشف إن كان النص المُولَّد رسالة خطأ (وليس تحليلاً فعلياً) لمنع تخزينه أو تقديمه من الكاش."""
    if not text:
        return True
    return text.strip().startswith("❌")


SIGNAL_BLOCK_RE = re.compile(
    r"(?P<emoji>🟢|🔴|⚪)\s*(?P<symbol>[A-Za-z0-9]+)\s*—\s*(?P<direction>[^\n]+)\n"
    r"(?:.*?الدخول\s*:\s*(?P<entry>[^\n]+))?"
    r"(?:.*?SL\s*:\s*(?P<sl>[^\n]+))?"
    r"(?:.*?TP1\s*:\s*(?P<tp1>[^\n]+))?"
    r"(?:.*?TP2\s*:\s*(?P<tp2>[^\n]+))?"
    r"(?:.*?R:R\s*:\s*(?P<rr>[^\n]+))?",
    re.DOTALL
)



def decisions_to_signal_rows(decision: Dict[str, Any]) -> List[Dict[str, Optional[str]]]:
    """تحويل قرارات الوكيل إلى صفوف إشارة للتخزين (أوثق من regex وحده)."""
    rows = []
    for d in decision.get("decisions") or []:
        if not isinstance(d, dict) or not d.get("symbol"):
            continue
        direction = str(d.get("direction") or "NEUTRAL").upper()
        if direction == "BUY":
            emoji, dir_ar = "🟢", "شراء (BUY)"
        elif direction == "SELL":
            emoji, dir_ar = "🔴", "بيع (SELL)"
        else:
            emoji, dir_ar = "⚪", "لا توجد إشارة (NEUTRAL)"
        entry, sl, tp1, tp2 = d.get("entry"), d.get("sl"), d.get("tp1"), d.get("tp2")
        rows.append({
            "emoji": emoji,
            "symbol": str(d["symbol"]).upper(),
            "direction": dir_ar,
            "entry": str(entry) if entry is not None else None,
            "sl": str(sl) if sl is not None else None,
            "tp1": str(tp1) if tp1 is not None else None,
            "tp2": str(tp2) if tp2 is not None else None,
            "rr": str(d["rr"]) if d.get("rr") is not None else None,
        })
    return rows


async def persist_signals_from_report_and_decision(
    report: str,
    aggregated: Dict[str, Any],
    tfs: List[str],
) -> int:
    """يحفظ من JSON القرار أولاً ثم من كتل النص."""
    rows = []
    decision = aggregated.get("ensemble_decision") or {}
    if decision:
        rows = decisions_to_signal_rows(decision)
    have = {r["symbol"] for r in rows}
    for r in parse_signal_blocks(report):
        if r["symbol"] not in have:
            rows.append(r)
    n = 0
    for sig in rows:
        tech = _primary_tech_for_symbol(
            aggregated.get("symbols_technical") or {}, sig["symbol"], tfs
        )
        await db.save_symbol_signal(
            sig["symbol"], sig["emoji"], sig["direction"],
            sig.get("entry"), sig.get("sl"), sig.get("tp1"), sig.get("tp2"), sig.get("rr"),
            rsi_14=tech.get("rsi_14"),
            ema_20=tech.get("ema_20"),
            ema_50=tech.get("ema_50"),
            atr_14=tech.get("atr_14"),
            primary_tf=tech.get("_tf"),
            source="full",
        )
        n += 1
    # إشارات فنية لكل إطار (سريعة) — لا تستدعي AI
    try:
        n += await persist_per_tf_technical_signals(aggregated, tfs or ["H1"])
    except Exception as e:
        logger.error(f"persist_per_tf_technical_signals: {e}")
    return n


def parse_signal_blocks(report_text: str) -> List[Dict[str, Optional[str]]]:
    """
    يستخرج بطاقات إشارة كل زوج (🟢/🔴/⚪ + Entry/SL/TP1/TP2/R:R) من نص "التوصية الشاملة"،
    حيث يُلزَم النموذج بإرفاقها مباشرة تحت تحليل كل زوج. تُستخدم لتخزين توصية كل زوج
    منفصلة في جدول symbol_signals، بعيداً عن نص التحليل السردي الكامل.
    """
    results = []
    seen = set()
    # نبحث عن كل بداية إشارة، ثم نأخذ المقطع حتى بداية الإشارة التالية (أو نهاية النص)
    # كي لا "يسرق" السطر الأول أرقام إشارة الزوج التالي بالخطأ.
    starts = [m.start() for m in re.finditer(r"(?:🟢|🔴|⚪)\s*[A-Za-z0-9]+\s*—", report_text)]
    for i, start in enumerate(starts):
        end = starts[i + 1] if i + 1 < len(starts) else len(report_text)
        segment = report_text[start:end]
        m = SIGNAL_BLOCK_RE.match(segment)
        if not m:
            continue
        symbol = m.group("symbol")
        if symbol in seen:
            continue
        seen.add(symbol)
        results.append({
            "emoji": m.group("emoji"),
            "symbol": symbol,
            "direction": m.group("direction").strip(),
            "entry": (m.group("entry") or "").strip() or None,
            "sl": (m.group("sl") or "").strip() or None,
            "tp1": (m.group("tp1") or "").strip() or None,
            "tp2": (m.group("tp2") or "").strip() or None,
            "rr": (m.group("rr") or "").strip() or None,
        })
    return results



async def orchestrate_from_signal(
    symbols: List[str],
    tfs: Optional[List[str]] = None,
    trigger_source: str = "manual",
    trigger_payload: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Orchestrator Middleware:
    عند ورود إشارة (TradingView Webhook أو طلب يدوي) يسحب اللقطة من الكاش التشغيلي +
    يحدّث الشموع من cTrader عند الحاجة، ثم يشغّل Ensemble ويحفظ النتيجة.
    لا ينتظر استعلام DB بطيء لكل حقل — DB للتاريخ فقط.
    """
    tfs = tfs or ["H1"]
    symbols = [s.strip().upper() for s in symbols if s and str(s).strip()]
    if not symbols:
        return {"ok": False, "error": "no_symbols"}

    # 1) لقطة كاش فورية
    snap = runtime_cache.snapshot_for_agents()

    # 2) تجميع: يستخدم كاش المصادر (TTL) + cTrader للشموع
    aggregated = await aggregate_multi_symbols_data(symbols, tfs)
    # دمج لقطة الحساب من الكاش إن كانت أحدث من reconcile داخل aggregate
    if snap.get("account"):
        aggregated["account_snapshot_cache"] = snap["account"]
    if trigger_payload:
        aggregated["trigger"] = {"source": trigger_source, "payload": trigger_payload}

    history_text = await build_history_text("full", symbols, tfs)
    signal_history_text = await build_signal_history_text(
        symbols, symbols_technical=aggregated.get("symbols_technical")
    )
    await attach_learning_stats(aggregated, symbols)
    report = await run_ensemble_pipeline(aggregated, history_text, signal_history_text)

    if is_error_report(report):
        return {"ok": False, "error": report, "trigger_source": trigger_source}

    await db.save_report("full", symbols, tfs, report, raw_data=aggregated)
    await db.register_active_watch("full", symbols, tfs)

    await persist_signals_from_report_and_decision(report, aggregated, tfs)

    # إشعار تيليجرام اختياري
    if MY_TELEGRAM_CHAT_ID and telegram_app:
        try:
            await telegram_app.bot.send_message(
                chat_id=int(MY_TELEGRAM_CHAT_ID),
                text=f"⚡ *إشارة عبر {trigger_source}*\n\n{report[:3500]}",
                parse_mode="Markdown",
            )
        except Exception as e:
            logger.error(f"orchestrator telegram notify error: {e}")

    return {
        "ok": True,
        "trigger_source": trigger_source,
        "symbols": symbols,
        "timeframes": tfs,
        "ensemble_decision": aggregated.get("ensemble_decision"),
        "report_preview": report[:500],
    }


async def get_or_generate_report(analysis_type: str, symbols: List[str], tfs: List[str]) -> str:
    """
    الكاش الذكي: يحاول أولاً قراءة آخر تحليل مخزّن في Firestore.
    - إن كان عمره أقل من فترة التحديث التلقائي (AUTO_ANALYSIS_INTERVAL_MINUTES) وليس رسالة خطأ
      مخزّنة سابقاً → يُعاد فوراً "من الذاكرة" دون أي استدعاء جديد للذكاء الاصطناعي (يوفّر الوقت والتكلفة).
    - إن لم يوجد تحليل كافٍ الحداثة، أو كان المخزَّن رسالة خطأ قديمة → يُولَّد تحليل جديد الآن.
      يُخزَّن فقط إن نجح فعلياً (وليس رسالة خطأ)، وتُسجَّل هذه التركيبة كـ'متابعة نشطة' لتُحدَّث
      تلقائياً من الآن فصاعداً كل AUTO_ANALYSIS_INTERVAL_MINUTES دقيقة عبر المهمة المجدولة
      scheduled_analysis_job، دون تدخل أي مستخدم.
    - عند نجاح "التوصية الموحدة الشاملة" (full) تحديداً، تُستخرج توصية كل زوج على حدة من
      النص وتُخزَّن منفصلة في جدول symbol_signals، ليقرأها زر "إشارات تداول فورية" لاحقاً
      مباشرة من قاعدة البيانات دون أي استدعاء جديد للذكاء الاصطناعي.
    """
    cached = await db.get_latest_report(analysis_type, symbols, tfs)
    max_age = timedelta(minutes=db.AUTO_ANALYSIS_INTERVAL_MINUTES)

    if cached and not is_error_report(cached["report_text"]) and (datetime.now(timezone.utc) - cached["created_at"]) < max_age:
        age_min = int((datetime.now(timezone.utc) - cached["created_at"]).total_seconds() // 60)
        return f"🗄️ *(من الذاكرة المخزّنة — آخر تحديث قبل {age_min} دقيقة)*\n\n{cached['report_text']}"

    aggregated = await aggregate_multi_symbols_data(symbols, tfs)
    history_text = await build_history_text(analysis_type, symbols, tfs)
    if analysis_type == "full":
        signal_history_text = await build_signal_history_text(
            symbols, symbols_technical=aggregated.get("symbols_technical")
        )
        await attach_learning_stats(aggregated, symbols)
    else:
        signal_history_text = ""
    report = await run_specific_analysis(analysis_type, aggregated, tfs, history_text, signal_history_text)

    if is_error_report(report):
        # لا نخزّن رسائل الخطأ في قاعدة البيانات، ولا نسجّلها كـ"متابعة نشطة"،
        # حتى لا تُقرأ لاحقاً من الكاش وكأنها تحليل صالح.
        return report


    await db.save_report(analysis_type, symbols, tfs, report, raw_data=aggregated)
    await db.register_active_watch(analysis_type, symbols, tfs)

    if analysis_type == "full":
        await persist_signals_from_report_and_decision(report, aggregated, tfs)

    prefix = f"🆕 *(تحليل جديد الآن — تم تسجيله للتحديث التلقائي كل {db.AUTO_ANALYSIS_INTERVAL_MINUTES} دقيقة)*\n\n"
    return prefix + report


async def refresh_single_watch(watch: Dict[str, Any]) -> bool:
    """
    يُحدّث تحليلاً واحداً (نوع + أزواج + أطر): يجلب شموعاً حية، يحسب المؤشرات الفنية
    ويخزّنها، ثم يولّد تحليل AI جديداً ويخزّنه إن نجح. تُعيد True إن نجح التحديث فعلياً.
    مستخدمة من كل من المهمة المجدولة التلقائية وزر التحديث الفوري اليدوي.
    """
    analysis_type = watch["analysis_type"]
    symbols = [s for s in watch["symbols"].split(",") if s]
    tfs = [t for t in watch["timeframes"].split(",") if t]
    if not symbols or not tfs:
        return False

    try:
        # 1) تحديث المؤشرات الفنية لكل زوج/إطار قبل التحليل (تُستخدم أيضاً كسجل تاريخي مستقل)
        for sym in symbols:
            for tf in tfs:
                bars = await ensure_trendbars_ready(sym, tf)
                if bars:
                    indicators = calculate_technical_indicators(bars, sym)
                    await db.save_technical_snapshot(sym, tf, indicators)

        # 2) توليد التحليل الجديد مع تمرير الذاكرة التاريخية للمقارنة
        aggregated = await aggregate_multi_symbols_data(symbols, tfs)
        history_text = await build_history_text(analysis_type, symbols, tfs)
        if analysis_type == "full":
            signal_history_text = await build_signal_history_text(
                symbols, symbols_technical=aggregated.get("symbols_technical")
            )
            await attach_learning_stats(aggregated, symbols)
        else:
            signal_history_text = ""
        report = await run_specific_analysis(analysis_type, aggregated, tfs, history_text, signal_history_text)


        if is_error_report(report):
            # لا نخزّن رسائل الخطأ (مثل تجاوز حد الاستخدام) كتحليل صالح — نُبقي آخر تحليل
            # ناجح مخزَّن كما هو، ونحاول مجدداً في الدورة القادمة.
            logger.warning(f"⚠️ تخطي تخزين تحليل فاشل لـ {analysis_type}/{symbols}/{tfs}: {report[:200]}")
            await db.mark_watch_run(analysis_type, symbols, tfs)
            return False

        await db.save_report(analysis_type, symbols, tfs, report, raw_data=aggregated)
        await db.mark_watch_run(analysis_type, symbols, tfs)

        if analysis_type == "full":
            await persist_signals_from_report_and_decision(report, aggregated, tfs)

        logger.info(f"✅ تحديث مكتمل: [{analysis_type}] {symbols} / {tfs}")
        return True
    except Exception as e:
        logger.error(f"❌ خطأ أثناء تحديث {analysis_type}/{symbols}/{tfs}: {e}")
        return False


async def scheduled_ctrader_account_refresh():
    """تحديث حساب cTrader (رصيد/هامش/صفقات) بشكل شبه لحظي."""
    if is_ctrader_connected and ACCOUNT_ID:
        request_account_details()


async def scheduled_external_sources_refresh():
    """تحديث مصادر الأخبار/التقويم/TradingView وفق TTL الخاص بكل مصدر."""
    try:
        await asyncio.gather(
            fetch_forex_factory_calendar(),
            fetch_finnhub_news(),
            fetch_tradingview_rss(),
        )
    except Exception as e:
        logger.error(f"external sources refresh error: {e}")



def _tf_seconds(tf: str) -> int:
    return {"M1": 60, "M15": 900, "H1": 3600, "H4": 14400, "D1": 86400}.get((tf or "").upper(), 3600)


def _last_bar_time_from_bars(bars: List[Any]) -> Optional[datetime]:
    if not bars:
        return None
    try:
        rows = decode_ctrader_bars_to_ohlc(bars)
        if rows and rows[-1].get("bar_time"):
            bt = rows[-1]["bar_time"]
            if isinstance(bt, datetime):
                return bt if bt.tzinfo else bt.replace(tzinfo=timezone.utc)
    except Exception:
        pass
    try:
        b = bars[-1]
        ts_ms = getattr(b, "utcTimestampFrom", None) or getattr(b, "utcTimestampTo", None)
        if ts_ms is None:
            return None
        ts_ms = float(ts_ms)
        if ts_ms > 1e12:
            return datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc)
        return datetime.fromtimestamp(ts_ms, tz=timezone.utc)
    except Exception:
        return None


async def _resolve_tf_signal_universe() -> tuple:
    """
    الأزواج والأطر المفعّلة لتحديث إشارات الأطر تلقائياً:
      1) اختيار تلغرام الأخير / حالة التشغيل
      2) وإلا أزواج من active_watches
    """
    symbols: List[str] = []
    tfs: List[str] = []
    # من ذاكرة المستخدم المالك
    if MY_TELEGRAM_CHAT_ID:
        try:
            cid = int(MY_TELEGRAM_CHAT_ID)
            symbols = list(user_selected_symbols.get(cid) or [])
            tfs = list(user_selected_tfs.get(cid) or [])
            trade_tf = user_trading_tf.get(cid)
            if trade_tf and trade_tf not in tfs:
                tfs.append(trade_tf)
        except Exception:
            pass
    if not symbols or not tfs:
        try:
            st = await db.load_runtime_state()
            if st:
                if not symbols and st.get("selected_symbols"):
                    symbols = [s for s in str(st["selected_symbols"]).split(",") if s]
                if not tfs and st.get("analysis_tfs"):
                    tfs = [x for x in str(st["analysis_tfs"]).split(",") if x]
                trade_tf = (st.get("trading_tf") or "").upper()
                if trade_tf and trade_tf not in tfs:
                    tfs.append(trade_tf)
        except Exception:
            pass
    if not symbols or not tfs:
        try:
            ui = await db.get_latest_telegram_ui_selection()
            if ui:
                if not symbols and ui.get("symbols"):
                    symbols = [s for s in str(ui["symbols"]).split(",") if s]
                if not tfs and ui.get("timeframes"):
                    tfs = [x for x in str(ui["timeframes"]).split(",") if x]
                trade_tf = (ui.get("trading_tf") or "").upper()
                if trade_tf and trade_tf not in tfs:
                    tfs.append(trade_tf)
        except Exception:
            pass
    if not symbols or not tfs:
        pairs = await db.get_all_active_symbol_timeframes()
        for p in pairs or []:
            if p.get("symbol") and p["symbol"] not in symbols:
                symbols.append(p["symbol"])
            if p.get("timeframe") and p["timeframe"] not in tfs:
                tfs.append(p["timeframe"])
    # تنظيف
    symbols = [s.strip().upper() for s in symbols if s and str(s).strip()]
    tfs = [x.strip().upper() for x in tfs if x and str(x).strip()]
    # حد حماية
    max_syms = int(os.getenv("TF_SIGNALS_MAX_SYMBOLS", "12"))
    symbols = symbols[:max_syms]
    tfs = [x for x in tfs if x in set(AVAILABLE_TIMEFRAMES)] or ["H1"]
    return symbols, tfs


async def scheduled_tf_signals_refresh():
    """
    تحديث تلقائي خفيف لإشارات الأطر (بدون AI):
    عند رصد شمعة جديدة مغلقة لكل إطار مفعّل → إعادة حساب الإشارة + فلتر الجودة + تخزين.
    """
    global _last_tf_signal_bar
    if not TF_SIGNALS_AUTO_ENABLED:
        return {"ok": False, "reason": "disabled"}
    if not is_ctrader_connected:
        return {"ok": False, "reason": "ctrader_disconnected"}
    if not db.is_storage_ready():
        return {"ok": False, "reason": "no_db"}

    symbols, tfs = await _resolve_tf_signal_universe()
    if not symbols or not tfs:
        return {"ok": False, "reason": "no_universe"}

    updated_pairs = []
    symbols_tech: Dict[str, Dict[str, Any]] = {}

    for sym in symbols:
        if sym not in symbol_id_map:
            continue
        symbols_tech[sym] = {}
        for tf in tfs:
            try:
                bars = await ensure_trendbars_ready(sym, tf, wait_seconds=2.0)
                if not bars or len(bars) < 50:
                    continue
                bar_t = _last_bar_time_from_bars(bars)
                if bar_t is None:
                    continue
                key = f"{sym}|{tf}"
                prev = _last_tf_signal_bar.get(key)
                # شمعة جديدة فقط (أو أول تشغيل)
                if prev is not None and bar_t <= prev:
                    # لا زالت نفس الشمعة — تخطي
                    continue
                ind = calculate_technical_indicators(bars, sym)
                if not ind or "error" in ind:
                    continue
                symbols_tech[sym][tf] = ind
                updated_pairs.append((sym, tf, bar_t))
            except Exception as e:
                logger.error(f"tf-signal refresh prepare [{sym} {tf}]: {e}")

    if not updated_pairs:
        return {"ok": True, "updated": 0, "reason": "no_new_candles"}

    # ابنِ aggregated جزئي للأزواج التي تغيّرت
    touch_symbols = sorted({s for s, _, _ in updated_pairs})
    aggregated = {
        "symbols": touch_symbols,
        "symbols_technical": {
            s: symbols_tech.get(s) or {} for s in touch_symbols
        },
    }
    # أطر لم تُحدَّث لكن موجودة في الكاش — مفيدة لـ multi-TF
    for s in touch_symbols:
        by_tf = aggregated["symbols_technical"].setdefault(s, {})
        for tf in tfs:
            if tf in by_tf:
                continue
            bars = trendbars_cache.get(s, {}).get(tf) or []
            if bars and len(bars) >= 50:
                try:
                    ind = calculate_technical_indicators(bars, s)
                    if ind and "error" not in ind:
                        by_tf[tf] = ind
                except Exception:
                    pass

    try:
        n = await persist_per_tf_technical_signals(aggregated, tfs)
    except Exception as e:
        logger.error(f"persist_per_tf_technical_signals auto: {e}")
        n = 0

    for sym, tf, bar_t in updated_pairs:
        _last_tf_signal_bar[f"{sym}|{tf}"] = bar_t

    logger.info(
        f"⏱️ TF-signals auto: new_candles={len(updated_pairs)} saved≈{n} "
        f"pairs={updated_pairs[:6]}{'...' if len(updated_pairs) > 6 else ''}"
    )
    return {"ok": True, "updated": len(updated_pairs), "saved": n}


async def scheduled_ohlc_refresh():
    """
    أتمتة تحديث شموع OHLC من cTrader وتخزينها في ohlc_bars.
    يستهدف الأزواج/الأطر المسجّلة في active_watches؛ إن لم توجد يستخدم مجموعة افتراضية شائعة.
    """
    if not is_ctrader_connected:
        logger.info("OHLC refresh skipped: cTrader not connected")
        return {"ok": False, "reason": "ctrader_disconnected", "saved": 0}

    pairs = await db.get_all_active_symbol_timeframes()
    if not pairs:
        # افتراضي خفيف إن لم تُسجَّل متابعات بعد
        default_syms = ["EURUSD", "XAUUSD", "BTCUSD", "GBPUSD", "USDJPY"]
        default_tfs = ["H1", "H4"]
        pairs = [{"symbol": s, "timeframe": t} for s in default_syms for t in default_tfs]

    # حد أقصى لحماية الحصة/الوقت على Cloud Run
    max_pairs = int(os.getenv("OHLC_REFRESH_MAX_PAIRS", "24"))
    pairs = pairs[:max_pairs]

    total_saved = 0
    refreshed = 0
    for i, p in enumerate(pairs):
        sym, tf = p["symbol"], p["timeframe"]
        try:
            if sym not in symbol_id_map:
                continue
            bars = await ensure_trendbars_ready(sym, tf, wait_seconds=3.0)
            if bars:
                refreshed += 1
                # ensure_trendbars_ready يخزّن OHLC بالفعل
                total_saved += len(bars)
            if i > 0 and i % 4 == 0:
                await asyncio.sleep(0.5)
        except Exception as e:
            logger.error(f"OHLC refresh error [{sym} {tf}]: {e}")
    logger.info(f"🕯️ OHLC auto-refresh done: pairs={refreshed}/{len(pairs)}")
    return {"ok": True, "pairs": len(pairs), "refreshed": refreshed, "bars_seen": total_saved}


async def scheduled_analysis_job():
    """
    مهمة تعمل تلقائياً عبر APScheduler:
    تُحدّث فقط المتابعات النشطة التي "حان وقتها" وفق فاصل الوضع الحالي.
    """
    if not db.is_storage_ready():
        return
    # قاطع دائرة وقائي قبل أي تنفيذ جديد
    try:
        if await circuit_breaker_check():
            return  # التداول الآلي متوقف — نُكِل التحليلات فقط دون تنفيذ
    except Exception as e:
        logger.error(f"circuit breaker in analysis job: {e}")
    interval_min = (
        AUTO_ANALYSIS_INTERVAL_MINUTES
        if runtime_cheap_mode
        else max(1, NON_CHEAP_ANALYSIS_INTERVAL_MINUTES)
    )
    logger.info(f"⏰ بدء دورة التحليل التلقائي المجدولة (فاصل={interval_min}د، cheap={runtime_cheap_mode})...")
    due = await db.get_due_watches(interval_min)
    for i, watch in enumerate(due):
        if i > 0:
            await asyncio.sleep(3)  # تهدئة بسيطة بين كل تحليل والتالي لتفادي تجاوز حد الطلبات لدى NVIDIA
        await refresh_single_watch(watch)

    # تنفيذ تلقائي مستمر — فقط لمن ضغط «بدء التداول»، ولا يعمل لأي مستخدم آخر
    if auto_trading_enabled and auto_trading_chat_id and is_ctrader_connected:
        try:
            owner_syms = user_selected_symbols.get(auto_trading_chat_id) or []
            owner_tf = user_trading_tf.get(auto_trading_chat_id) or "H1"
            if owner_syms:
                exec_result = await execute_signals_professionally(
                    owner_syms, timeframe=owner_tf, chat_id=auto_trading_chat_id
                )
                executed = exec_result.get("executed") or []
                failed = exec_result.get("failed") or []
                # لا نُرسل رسالة عند عدم وجود شيء جديد لتفادي الإزعاج المتكرر كل دورة
                if (executed or failed) and telegram_app:
                    lines = ["🤖 **دورة تنفيذ تلقائي**"]
                    for r in executed:
                        lines.append(
                            f"  ✅ {r.get('side')} `{r.get('symbol')}` "
                            f"SL={r.get('stop_loss')} TP={r.get('take_profit')}"
                        )
                    for r in failed:
                        lines.append(f"  ❌ `{r.get('symbol')}` — {r.get('error')}")
                    try:
                        await telegram_app.bot.send_message(
                            chat_id=auto_trading_chat_id,
                            text="\n".join(lines),
                            parse_mode="Markdown",
                        )
                    except Exception as se:
                        logger.error(f"auto-trading notify error: {se}")
            else:
                logger.warning("⚠️ التداول الآلي مفعّل لكن لا توجد أزواج مختارة للمالك — تخطي هذه الدورة.")
        except Exception as e:
            logger.error(f"auto-trading cycle error: {e}")



async def scheduled_db_monitor():
    """يسجّل لقطة أداء DB كل ساعة ويُنبّه عند تنبيهات."""
    try:
        report = await db.get_db_performance_report()
        health = report.get("health")
        alerts = report.get("alerts") or []
        m = report.get("app_metrics") or {}
        logger.info(
            f"🗄️ DB monitor health={health} queries={m.get('queries_total')} "
            f"slow={m.get('slow_queries')} avg_ms={m.get('query_ms_avg')} "
            f"pool={report.get('pool')} alerts={len(alerts)}"
        )
        if alerts and health == "degraded":
            await notify_user(
                "⚠️ **تنبيه أداء قاعدة البيانات**\n" + "\n".join(f"• {a}" for a in alerts[:6]),
                key="db_perf_alert",
                min_interval_sec=3600,
            )
    except Exception as e:
        logger.error(f"scheduled_db_monitor error: {e}")


async def scheduled_db_cleanup():
    """تنظيف دوري للوثائق القديمة على Firestore (وأي مرايا BigQuery)."""
    if not db.is_storage_ready():
        return
    try:
        result = await db.cleanup_old_rows()
        logger.info(f"🧹 صيانة قاعدة البيانات: {result}")
    except Exception as e:
        logger.error(f"scheduled_db_cleanup error: {e}")


async def force_refresh_all_watches(notify_chat_id: Optional[int] = None):
    """
    تحديث فوري لكل المتابعات النشطة المسجَّلة (بغض النظر عن آخر موعد تحديث)،
    يُستخدم من زر "تحديث فوري" اليدوي في تيليجرام. يعمل كخلفية (Background Task)
    حتى لا يُعطّل الرد الفوري على ضغطة الزر، ويُرسل إشعاراً عند الانتهاء إن طُلب ذلك.
    """

    if not db.is_storage_ready():
        return
    all_watches = await db.get_due_watches(0)  # 0 دقيقة = كل المتابعات بغض النظر عن آخر تحديث
    logger.info(f"🔄 بدء تحديث فوري يدوي لـ {len(all_watches)} متابعة نشطة...")

    success_count = 0
    for i, watch in enumerate(all_watches):
        if i > 0:
            await asyncio.sleep(3)  # تهدئة بسيطة بين كل تحليل والتالي لتفادي تجاوز حد الطلبات لدى NVIDIA
        if await refresh_single_watch(watch):
            success_count += 1

    logger.info(f"✅ اكتمل التحديث الفوري اليدوي: {success_count}/{len(all_watches)} نجحت.")

    if notify_chat_id and telegram_app:
        await safe_send_message(
            telegram_app.bot,
            notify_chat_id,
            f"✅ **اكتمل التحديث الفوري لكل التحاليل النشطة**\n"
            f"نجح تحديث `{success_count}` من أصل `{len(all_watches)}` متابعة مسجَّلة."
        )

# ==================== لوحات التحكم والأزرار ====================

TELEGRAM_MAX_MESSAGE_LENGTH = 4000  # هامش أمان أقل من حد تلغرام الفعلي (4096) لتفادي القطع عند حدود الأحرف الخاصة


def split_text_for_telegram(text: str, max_len: int = TELEGRAM_MAX_MESSAGE_LENGTH) -> List[str]:
    """
    يقسّم نصاً طويلاً إلى أجزاء لا يتجاوز أي منها حد تلغرام، مع محاولة التقسيم
    عند حدود الأسطر الفارغة أو الأسطر العادية بدل تقطيع الجملة من المنتصف.
    """
    if len(text) <= max_len:
        return [text]

    chunks: List[str] = []
    remaining = text
    while len(remaining) > max_len:
        split_at = remaining.rfind("\n\n", 0, max_len)
        if split_at == -1:
            split_at = remaining.rfind("\n", 0, max_len)
        if split_at == -1:
            split_at = max_len
        chunks.append(remaining[:split_at].strip())
        remaining = remaining[split_at:].strip()
    if remaining:
        chunks.append(remaining)
    return chunks


async def safe_send_message(bot, chat_id: int, text: str, reply_markup=None):
    """
    يحاول إرسال الرسالة بتنسيق Markdown أولاً. نصوص الذكاء الاصطناعي المولَّدة تلقائياً
    قد تحتوي أحياناً على رمز * أو _ أو ` غير متوازن (مثلاً "50% من *الشراء" بدون
    إغلاق)، وهذا يجعل تلغرام يرفض الرسالة كاملة بخطأ:
    "Can't parse entities: can't find end of the entity starting at byte offset X".
    لتفادي فقدان الرسالة بالكامل، إن فشل التحليل نعيد الإرسال كنص عادي بدون تنسيق.

    كما تُقسَّم الرسائل الطويلة (مثل التوصية الشاملة) تلقائياً إلى عدة رسائل متتالية
    لأن تلغرام يرفض أي رسالة تتجاوز 4096 حرفاً بخطأ "Message is too long".
    الأزرار (reply_markup) تُرفق فقط بآخر جزء من الرسالة.
    """
    parts = split_text_for_telegram(text)
    for i, part in enumerate(parts):
        is_last = (i == len(parts) - 1)
        markup = reply_markup if is_last else None
        try:
            await bot.send_message(chat_id=chat_id, text=part, reply_markup=markup, parse_mode="Markdown")
        except BadRequest as e:
            if "can't parse entities" in str(e).lower():
                logger.warning(f"⚠️ فشل تحليل Markdown، إعادة الإرسال كنص عادي: {e}")
                try:
                    await bot.send_message(chat_id=chat_id, text=part, reply_markup=markup)
                except Exception as e2:
                    logger.error(f"❌ فشل الإرسال حتى كنص عادي: {e2}")
            else:
                raise


def is_authorized(update: Update) -> bool:
    """يسمح فقط لـ MY_TELEGRAM_CHAT_ID. إن لم يُضبط المتغير → مفتوح للجميع."""
    if not MY_TELEGRAM_CHAT_ID:
        return True
    allowed = str(MY_TELEGRAM_CHAT_ID).strip()
    # نقارن user id و chat id (في الخاص متطابقان؛ في المجموعات يختلفان)
    candidates = []
    if update.effective_user is not None:
        candidates.append(str(update.effective_user.id))
    if update.effective_chat is not None:
        candidates.append(str(update.effective_chat.id))
    ok = allowed in candidates
    if not ok:
        logger.warning(
            f"⛔ رفض دخول: allowed={allowed!r} candidates={candidates} "
            f"update_type={type(update).__name__}"
        )
    return ok

def main_keyboard(user_id: int):
    selected_syms = user_selected_symbols.get(user_id, [])
    selected_tfs = user_selected_tfs.get(user_id, ["H1"])

    syms_count = len(selected_syms)
    tfs_count = len(selected_tfs)

    # أزرار الوضع: اقتصادي = 0 / غير اقتصادي = 1
    if runtime_cheap_mode:
        mode_row = [
            InlineKeyboardButton("⬜ غير اقتصادي (1)", callback_data="mode_non_cheap"),
            InlineKeyboardButton("✅ اقتصادي (0)", callback_data="mode_cheap"),
        ]
    else:
        mode_row = [
            InlineKeyboardButton("✅ غير اقتصادي (1)", callback_data="mode_non_cheap"),
            InlineKeyboardButton("⬜ اقتصادي (0)", callback_data="mode_cheap"),
        ]

    trade_icon_on = "🟢" if auto_trading_enabled else "▶️"
    trade_icon_off = "⏹" if auto_trading_enabled else "⬜"

    keyboard = [
        mode_row,
        [
            InlineKeyboardButton(f"{trade_icon_on} بدء التداول", callback_data="btn_start_trading"),
            InlineKeyboardButton(f"{trade_icon_off} إيقاف التداول وإغلاق الصفقات", callback_data="btn_stop_trading"),
        ],
        [
            InlineKeyboardButton("💱 أزواج الفوركس", callback_data="category_forex"),
            InlineKeyboardButton("🥇 المعادن والطاقة", callback_data="category_commodities")
        ],
        [
            InlineKeyboardButton("📈 المؤشرات العالمية", callback_data="category_indices"),
            InlineKeyboardButton("₿ الأسهم والعملات", callback_data="category_crypto")
        ],
        [
            InlineKeyboardButton(f"⏱️ أطر التحليل ({tfs_count})", callback_data="open_timeframes_menu")
        ],
        [
            InlineKeyboardButton(
                f"🎯 إطار التنفيذ: {user_trading_tf.get(user_id) or (selected_tfs[0] if selected_tfs else 'H1')}",
                callback_data="open_trading_tf_menu",
            )
        ],
        [
            InlineKeyboardButton("⏱️ إشارات حسب الإطار", callback_data="btn_tf_signals")
        ],
        [
            InlineKeyboardButton(
                f"{'✅' if signals_merge_full else '⬜'} دمج مع الشاملة",
                callback_data="btn_toggle_merge",
            )
        ],
        [
            InlineKeyboardButton("📊 المؤشرات الفنية الرقمية (Volume, RSI, EMA, ATR)", callback_data="calc_indicators")
        ],
        [
            InlineKeyboardButton("📅 تحليل التقويم (Forex Factory)", callback_data="analyze_forexfactory")
        ],
        [
            InlineKeyboardButton("📰 تحليل الأخبار الفورية (Finnhub)", callback_data="analyze_finnhub")
        ],
        [
            InlineKeyboardButton("📉 تحليل الرؤية الفنية (TradingView)", callback_data="analyze_tradingview")
        ],
        [
            InlineKeyboardButton("📈 تحليل الاتجاه العام", callback_data="analyze_trend")
        ],
        [
            InlineKeyboardButton(f"🧠 التوصية الموحدة الشاملة ({syms_count} أزواج)", callback_data="run_full_analysis")
        ],
        [
            InlineKeyboardButton("🎯 إشارات تداول فورية (بدون تعليقات)", callback_data="run_quick_signals")
        ],
        [
            InlineKeyboardButton("📊 حالة النظام", callback_data="btn_status"),
            InlineKeyboardButton("🗄️ أداء DB", callback_data="btn_db_stats")
        ],
        [
            InlineKeyboardButton("💳 معلومات الحساب", callback_data="btn_account")
        ],
        [
            InlineKeyboardButton("📈 الصفقات المفتوحة", callback_data="btn_positions"),
            InlineKeyboardButton("🔄 تحديث البيانات", callback_data="btn_refresh")
        ]
    ]
    return InlineKeyboardMarkup(keyboard)


def symbol_picker_keyboard(user_id: int, category: str):
    symbols = ALL_AVAILABLE_SYMBOLS.get(category, [])
    selected_syms = user_selected_symbols.get(user_id, [])
    
    keyboard = []
    row = []
    for sym in symbols:
        icon = "☑️" if sym in selected_syms else "🔲"
        row.append(InlineKeyboardButton(f"{icon} {sym}", callback_data=f"toggle_sym_{sym}_{category}"))
        if len(row) == 2:
            keyboard.append(row)
            row = []
    if row:
        keyboard.append(row)

    keyboard.append([InlineKeyboardButton("⏱️ ضبط الأطر الزمنية المشتركة", callback_data="open_timeframes_menu")])
    keyboard.append([InlineKeyboardButton("🔙 العودة للقائمة الرئيسية", callback_data="back_main")])
    return InlineKeyboardMarkup(keyboard)

def shared_tf_keyboard(user_id: int):
    selected = user_selected_tfs.get(user_id, ["H1"])
    keyboard = []
    row = []
    
    for tf in AVAILABLE_TIMEFRAMES:
        icon = "☑️" if tf in selected else "🔲"
        row.append(InlineKeyboardButton(f"{icon} {tf}", callback_data=f"toggle_tf_{tf}"))
        if len(row) == 2:
            keyboard.append(row)
            row = []
    if row:
        keyboard.append(row)

    keyboard.append([InlineKeyboardButton("🔙 العودة للقائمة الرئيسية", callback_data="back_main")])
    return InlineKeyboardMarkup(keyboard)

# ==================== معالجة أوامر وأزرار التلغرام ====================


def trading_tf_keyboard(user_id: int):
    """إطار زمني واحد للتنفيذ — منفصل عن أطر التحليل المتعددة."""
    selected_analysis = user_selected_tfs.get(user_id, ["H1"])
    current = user_trading_tf.get(user_id) or (selected_analysis[0] if selected_analysis else "H1")
    keyboard = []
    row = []
    for tf in AVAILABLE_TIMEFRAMES:
        icon = "✅" if tf == current else "⬜"
        row.append(InlineKeyboardButton(f"{icon} {tf}", callback_data=f"set_trading_tf_{tf}"))
        if len(row) == 3:
            keyboard.append(row)
            row = []
    if row:
        keyboard.append(row)
    keyboard.append([
        InlineKeyboardButton("🔙 العودة", callback_data="back_main"),
    ])
    return InlineKeyboardMarkup(keyboard)

async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_authorized(update):
        await update.message.reply_text("⛔ غير مصرح لك باستخدام هذا البوت.")
        return
    
    user_id = update.effective_chat.id
    if user_id not in user_selected_symbols:
        user_selected_symbols[user_id] = ["EURUSD"]
    if user_id not in user_selected_tfs:
        user_selected_tfs[user_id] = ["H1"]
    try:
        await db.save_telegram_ui_selection(
            user_id,
            user_selected_symbols[user_id],
            user_selected_tfs[user_id],
        )
    except Exception as e:
        logger.error(f"save_telegram_ui_selection (start): {e}")

    welcome_text = (
        "🚀 **مرحباً بك في لوحة التداول الذكية المخصصة!**\n\n"
        "• قم باختيار **الأزواج** و **الأطر الزمنية المشتركة**.\n"

        "• يمكنك حساب **المؤشرات الفنية المباشرة**، إجراء تحليل خاص بكل مصدر، أو توليد **التوصية الموحدة الشاملة**."
    )
    await update.message.reply_text(
        welcome_text,
        reply_markup=main_keyboard(user_id),
        parse_mode="Markdown"
    )

async def handle_callback_query(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global runtime_cheap_mode, auto_trading_enabled, signals_merge_full, auto_trading_chat_id
    query = update.callback_query

    try:
        await query.answer()
    except Exception as e:
        logger.error(f"Callback answer error: {e}")

    if not is_authorized(update):
        await query.message.reply_text("⛔ غير مصرح لك باستخدام هذا البوت.")
        return

    user_id = query.from_user.id
    data = query.data

    if user_id not in user_selected_symbols:
        user_selected_symbols[user_id] = ["EURUSD"]
    if user_id not in user_selected_tfs:
        user_selected_tfs[user_id] = ["H1"]

    try:
        if data == "back_main":

            syms = user_selected_symbols.get(user_id, [])
            tfs = user_selected_tfs.get(user_id, [])
            text = (
                "🤖 **لوحة التحكم الرئيسية**\n\n"
                f"🔹 **الأزواج المختارة حالياً ({len(syms)}):** `{', '.join(syms) if syms else 'لا يوجد'}`\n"
                f"⏱️ **الأطر الزمنية المشتركة ({len(tfs)}):** `{', '.join(tfs)}`\n\n"
                "اختر نوع التحليل المطلوب أدناه:"
            )
            await query.edit_message_text(
                text,
                reply_markup=main_keyboard(user_id),
                parse_mode="Markdown"
            )

        elif data.startswith("category_"):
            parts = data.split("_", 1)
            if len(parts) == 2:
                category = parts[1]
                await query.edit_message_text(
                    f"📋 **اختر الأزواج المطلوبة ضمن فئة [{category.upper()}]:**",
                    reply_markup=symbol_picker_keyboard(user_id, category),
                    parse_mode="Markdown"
                )

        elif data.startswith("toggle_sym_"):
            parts = data.split("_")
            if len(parts) >= 4:
                sym = parts[2]
                category = parts[3]
                current_syms = user_selected_symbols.get(user_id, [])

                if sym in current_syms:
                    current_syms.remove(sym)
                else:
                    current_syms.append(sym)
                    for tf in user_selected_tfs.get(user_id, ["H1"]):
                        request_symbol_trendbars(sym, tf)

                user_selected_symbols[user_id] = current_syms
                # حفظ الاختيار الحالي للوحة Streamlit (أزواج تلغرام فقط)
                try:
                    await db.save_telegram_ui_selection(
                        user_id,
                        current_syms,
                        user_selected_tfs.get(user_id, ["H1"]),
                    )
                except Exception as e:
                    logger.error(f"save_telegram_ui_selection (sym): {e}")
                await query.edit_message_reply_markup(reply_markup=symbol_picker_keyboard(user_id, category))

        elif data == "open_timeframes_menu":
            await query.edit_message_text(
                "⏱️ **اختر الأطر الزمنية المشتركة للتحليل:**",
                reply_markup=shared_tf_keyboard(user_id),
                parse_mode="Markdown"
            )

        elif data == "open_trading_tf_menu":
            await query.edit_message_text(
                "🎯 **اختر إطار التنفيذ** (واحد)\n"
                "أطر التحليل تبقى متعددة للتأكيد — التنفيذ يتم على هذا الإطار فقط.",
                reply_markup=trading_tf_keyboard(user_id),
                parse_mode="Markdown",
            )

        elif data.startswith("set_trading_tf_"):
            tf = data.replace("set_trading_tf_", "").upper()
            if tf in AVAILABLE_TIMEFRAMES:
                user_trading_tf[user_id] = tf
                # تأكد أن إطار التنفيذ ضمن أطر التحليل (يُجلب OHLC له)
                cur = user_selected_tfs.get(user_id, ["H1"])
                if tf not in cur:
                    cur = list(cur) + [tf]
                    user_selected_tfs[user_id] = cur
                try:
                    await db.save_telegram_ui_selection(
                        str(user_id),
                        user_selected_symbols.get(user_id, []),
                        user_selected_tfs.get(user_id, ["H1"]),
                        trading_tf=tf,
                    )
                except Exception as e:
                    logger.error(f"save trading_tf selection: {e}")
                await persist_runtime_state_from_memory(user_id)
                await query.edit_message_text(
                    f"✅ إطار التنفيذ: `{tf}`\n"
                    f"أطر التحليل: `{', '.join(user_selected_tfs.get(user_id, []))}`",
                    reply_markup=main_keyboard(user_id),
                    parse_mode="Markdown",
                )


        elif data == "btn_tf_signals":
            selected_syms = user_selected_symbols.get(user_id, [])
            selected_tfs = user_selected_tfs.get(user_id, ["H1"])
            trade_tf = user_trading_tf.get(user_id) or (selected_tfs[0] if selected_tfs else "H1")
            if not selected_syms:
                await query.edit_message_text(
                    "⚠️ اختر زوجاً واحداً على الأقل.",
                    reply_markup=main_keyboard(user_id),
                    parse_mode="Markdown",
                )
            else:
                await query.edit_message_text(
                    f"⏳ جاري بناء مصفوفة الإشارات للأطر `{', '.join(selected_tfs)}` ...",
                    parse_mode="Markdown",
                )
                try:
                    aggregated = await aggregate_multi_symbols_data(selected_syms, selected_tfs)
                    full_map = {}
                    if signals_merge_full:
                        try:
                            max_age = int(os.getenv("SIGNALS_MERGE_MAX_AGE_MINUTES", "90"))
                            full_map = await db.get_latest_full_signals(selected_syms, max_age_minutes=max_age)
                        except Exception as fe:
                            logger.error(f"full_map for matrix: {fe}")
                    report = build_per_tf_signals_matrix(aggregated, selected_tfs, trade_tf, full_map=full_map)
                    # تخزين خفيف لكل إطار
                    try:
                        await persist_per_tf_technical_signals(aggregated, selected_tfs)
                    except Exception as pe:
                        logger.error(f"tf signals persist: {pe}")
                    await safe_send_message(context.bot, user_id, report, reply_markup=main_keyboard(user_id))
                except Exception as e:
                    logger.error(f"btn_tf_signals: {e}")
                    await context.bot.send_message(
                        chat_id=user_id,
                        text=f"⚠️ خطأ أثناء بناء إشارات الأطر:\n{e}",
                        reply_markup=main_keyboard(user_id),
                    )



        elif data.startswith("toggle_tf_"):
            parts = data.split("_")
            if len(parts) >= 3:
                tf = parts[2]
                current_tfs = user_selected_tfs.get(user_id, ["H1"])

                if tf in current_tfs:
                    if len(current_tfs) > 1:
                        current_tfs.remove(tf)
                else:
                    current_tfs.append(tf)
                    for sym in user_selected_symbols.get(user_id, []):
                        request_symbol_trendbars(sym, tf)

                user_selected_tfs[user_id] = current_tfs
                try:
                    await db.save_telegram_ui_selection(
                        user_id,
                        user_selected_symbols.get(user_id, []),
                        current_tfs,
                    )
                except Exception as e:
                    logger.error(f"save_telegram_ui_selection (tf): {e}")
                await query.edit_message_reply_markup(reply_markup=shared_tf_keyboard(user_id))


        elif data == "calc_indicators":
            selected_syms = user_selected_symbols.get(user_id, [])
            selected_tfs = user_selected_tfs.get(user_id, ["H1"])

            if not selected_syms:
                await query.edit_message_text(
                    "⚠️ **لم تقم باختيار أي زوج!**\nالرجاء اختيار زوج واحد على الأقل للبدء.",
                    reply_markup=main_keyboard(user_id),
                    parse_mode="Markdown"
                )
                return

            await query.edit_message_text(
                "⏳ **جاري حساب المؤشرات الفنية المباشرة من cTrader...**",
                parse_mode="Markdown"
            )

            report_lines = ["📊 **التقرير الفني الرقمي المباشر**\n"]

            for sym in selected_syms:
                report_lines.append(f"🔹 **الزوج:** `{sym}`")
                sym_data = trendbars_cache.get(sym, {})
                
                for tf in selected_tfs:
                    bars = sym_data.get(tf, [])
                    if not bars:
                        request_symbol_trendbars(sym, tf)
                        report_lines.append(f"  • **الإطار [{tf}]:** ⚠️ `جاري جلب الشموع، أعد المحاولة بعد ثوانٍ`")
                        continue

                    indicators = calculate_technical_indicators(bars, sym)
                    if "error" in indicators:
                        report_lines.append(f"  • **الإطار [{tf}]:** ⚠️ `{indicators['error']}`")
                    else:
                        await db.save_technical_snapshot(sym, tf, indicators)
                        report_lines.append(
                            f"  ⏱️ **إطار [{tf}]:**\n"
                            f"     • **السعر الحالي:** `{indicators['last_price']}`\n"
                            f"     • **RSI (14):** `{indicators['rsi_14']}`\n"
                            f"     • **ATR (14):** `{indicators['atr_14']}`\n"
                            f"     • **EMA (20):** `{indicators['ema_20']}` | **EMA (50):** `{indicators['ema_50']}`\n"
                            f"     • **Volume:** `{indicators['volume']:,}`\n"
                        )
                report_lines.append("")

            final_report = "\n".join(report_lines)
            await safe_send_message(context.bot, user_id, final_report, reply_markup=main_keyboard(user_id))

        elif data == "run_quick_signals":
            selected_syms = user_selected_symbols.get(user_id, [])
            if not selected_syms:
                await query.edit_message_text(
                    "⚠️ **لم تقم باختيار أي زوج!**\nالرجاء اختيار زوج واحد على الأقل للبدء.",
                    reply_markup=main_keyboard(user_id),
                    parse_mode="Markdown"
                )
                return

            # إشارات إطار التنفيذ فقط (أو آخر إشارة إن لم يُحدد إطار)
            trade_tf = user_trading_tf.get(user_id) or (
                user_selected_tfs.get(user_id, ["H1"]) or ["H1"]
            )[0]
            signals = await db.get_symbol_signals(selected_syms, timeframe=trade_tf)


            lines = []
            for sym in selected_syms:
                sig = signals.get(sym)
                if not sig:
                    lines.append(f"⚪ {sym} — لا توجد توصية مخزَّنة بعد\n(اضغط «🧠 التوصية الموحدة الشاملة» أولاً لهذا الزوج)")
                    continue
                block = f"{sig['emoji']} {sym} — {sig['direction']}"
                if sig.get("entry"):
                    block += (
                        f"\nالدخول: {sig['entry']}\n"
                        f"SL: {sig['sl']}\n"
                        f"TP1: {sig['tp1']}\n"
                        f"TP2: {sig['tp2']}\n"
                        f"R:R: {sig['rr']}"
                    )
                block += f"\n_(آخر تحديث: {sig['created_at'].strftime('%Y-%m-%d %H:%M UTC')})_"
                lines.append(block)

            final_text = "🎯 **إشارات تداول فورية (من آخر توصية شاملة مخزَّنة لكل زوج)**\n\n" + "\n\n".join(lines)
            await safe_send_message(context.bot, user_id, final_text, reply_markup=main_keyboard(user_id))

        elif data in ["analyze_forexfactory", "analyze_finnhub", "analyze_tradingview", "analyze_trend", "run_full_analysis"]:
            selected_syms = user_selected_symbols.get(user_id, [])
            selected_tfs = user_selected_tfs.get(user_id, ["H1"])

            if not selected_syms:
                await query.edit_message_text(
                    "⚠️ **لم تقم باختيار أي زوج!**\nالرجاء اختيار زوج واحد على الأقل للبدء.",
                    reply_markup=main_keyboard(user_id),
                    parse_mode="Markdown"
                )
                return

            type_labels = {
                "analyze_forexfactory": ("📅 التقويم الاقتصادي (Forex Factory) — بيانات خام", "forex_factory"),
                "analyze_finnhub": ("📰 الأخبار الفورية (Finnhub) — بيانات خام", "finnhub"),
                "analyze_tradingview": ("📉 الرؤية الفنية (TradingView) — بيانات خام", "tradingview"),
                "analyze_trend": ("📈 تحليل الاتجاه العام", "trend"),
                "run_full_analysis": ("🧠 التوصية الموحدة الشاملة", "full"),
            }


            label_name, internal_type = type_labels[data]
            needs_ai = internal_type == "full"

            waiting_line = (
                "🧠 **إن لم يوجد تحليل حديث كافٍ سيتم توليد واحد جديد عبر NVIDIA Build AI...**"
                if needs_ai else
                "📥 **إن لم تكن هناك بيانات حديثة كافية سيتم جلبها الآن مباشرة (بدون ذكاء اصطناعي)...**"
            )
            await query.edit_message_text(
                f"⏳ **جاري التحقق من الذاكرة المخزّنة لـ [{label_name}]...**\n"
                f"• **الأزواج:** `{', '.join(selected_syms)}`\n"
                f"• **الأطر الزمنية:** `{', '.join(selected_tfs)}`\n\n"
                f"{waiting_line}",
                parse_mode="Markdown"
            )

            try:
                report = await get_or_generate_report(internal_type, selected_syms, selected_tfs)
                await safe_send_message(context.bot, user_id, report, reply_markup=main_keyboard(user_id))
            except Exception as ai_err:
                logger.error(f"Analysis Execution Error: {ai_err}")
                await context.bot.send_message(
                    chat_id=user_id,
                    text=f"⚠️ حدث خطأ أثناء إجراء التحليل:\n{str(ai_err)}",
                    reply_markup=main_keyboard(user_id)
                )

        elif data == "mode_cheap":
            runtime_cheap_mode = True
            detail = apply_runtime_mode_schedules()
            await persist_runtime_state_from_memory(user_id)
            msg = (
                "💸 **تم تفعيل الوضع الاقتصادي (0)**\n\n"
                "• Cloud Scheduler الخارجي هو من يوقظ الخدمة ويشغّل التحديث.\n"
                "• مناسب مع `min-instances=0` لتقليل التكلفة.\n"
                "• إيقاع OHLC وتحليل AI يبقى بالقيم الاقتصادية الافتراضية.\n\n"
                f"{detail}"
            )
            await query.edit_message_text(msg, reply_markup=main_keyboard(user_id), parse_mode="Markdown")

        elif data == "mode_non_cheap":
            runtime_cheap_mode = False
            detail = apply_runtime_mode_schedules()
            await persist_runtime_state_from_memory(user_id)
            msg = (
                "⚡ **تم تفعيل الوضع غير الاقتصادي (1)**\n\n"
                "• الجدولة الداخلية (APScheduler) تعمل بشكل مستمر 24/7.\n"
                "• يُفضَّل نشر Cloud Run بـ `--min-instances=1` وبدون CPU throttling.\n"
                f"• تحديث OHLC كل `{NON_CHEAP_OHLC_REFRESH_SECONDS}` ثانية.\n"
                f"• تحديث التوصية الموحدة كل `{NON_CHEAP_ANALYSIS_INTERVAL_MINUTES}` دقيقة "
                "(أقل فاصل عملي مع حدود NVIDIA والكود).\n\n"
                f"{detail}"
            )
            await query.edit_message_text(msg, reply_markup=main_keyboard(user_id), parse_mode="Markdown")

        elif data == "btn_start_trading":
            selected_syms = user_selected_symbols.get(user_id, [])

            if not selected_syms:
                await query.edit_message_text(
                    "⚠️ **لم تقم باختيار أي زوج!**\nاختر زوجاً واحداً على الأقل ثم أعد المحاولة.",
                    reply_markup=main_keyboard(user_id),
                    parse_mode="Markdown",
                )
                return
            if not is_ctrader_connected:
                await query.edit_message_text(
                    "🔴 **cTrader غير متصل** — لا يمكن تنفيذ أوامر الآن.\nانتظر إعادة الاتصال ثم أعد المحاولة.",
                    reply_markup=main_keyboard(user_id),
                    parse_mode="Markdown",
                )
                return

            auto_trading_enabled = True
            auto_trading_chat_id = user_id
            await persist_runtime_state_from_memory(user_id)
            trade_tf = user_trading_tf.get(user_id) or (
                user_selected_tfs.get(user_id, ["H1"]) or ["H1"]
            )[0]
            await query.edit_message_text(
                "⏳ **بدء التداول الآلي...**\n"
                f"إطار التنفيذ: `{trade_tf}` · لوت `{DEFAULT_TRADE_LOT}`\n"
                "مع SL/TP من إشارات ذلك الإطار (أو آخر توصية متوافقة).\n"
                f"🔁 سيستمر التنفيذ التلقائي كل دورة تحديث ({NON_CHEAP_ANALYSIS_INTERVAL_MINUTES if not runtime_cheap_mode else AUTO_ANALYSIS_INTERVAL_MINUTES} د) لهذا الحساب فقط، حتى تضغط «إيقاف التداول».",
                parse_mode="Markdown",
            )
            try:
                result = await execute_signals_professionally(
                    selected_syms, timeframe=trade_tf, chat_id=user_id
                )

            except Exception as e:
                logger.error(f"start trading error: {e}")
                await context.bot.send_message(
                    chat_id=user_id,
                    text=f"⚠️ خطأ أثناء التنفيذ: {e}",
                    reply_markup=main_keyboard(user_id),
                )
                return

            lines = [
                "🟢 **التداول الآلي مفعّل**",
                f"📦 حجم اللوت: `{DEFAULT_TRADE_LOT}`",
                "",
            ]
            executed = result.get("executed") or []
            skipped = result.get("skipped") or []
            failed = result.get("failed") or []
            if executed:
                lines.append(f"✅ **نُفِّذ ({len(executed)}):**")
                for r in executed:
                    lines.append(
                        f"  • {r.get('side')} `{r.get('symbol')}` "
                        f"SL={r.get('stop_loss')} TP={r.get('take_profit')}"
                    )
            if skipped:
                lines.append(f"\n⏭ **تم التخطي ({len(skipped)}):**")
                for r in skipped:
                    lines.append(f"  • `{r.get('symbol')}` — {r.get('reason')}")
            if failed:
                lines.append(f"\n❌ **فشل ({len(failed)}):**")
                for r in failed:
                    lines.append(f"  • `{r.get('symbol')}` — {r.get('error')}")
            if not executed and not failed and not skipped:
                lines.append("لا توجد إشارات قابلة للتنفيذ حالياً.")
            lines.append(
                "\n_ملاحظة: تأكد أنك شغّلت «🧠 التوصية الموحدة الشاملة» مسبقاً لتخزين الإشارات._"
            )
            await safe_send_message(
                context.bot, user_id, "\n".join(lines), reply_markup=main_keyboard(user_id)
            )

        elif data == "btn_stop_trading":
            auto_trading_enabled = False
            auto_trading_chat_id = None
            await persist_runtime_state_from_memory(user_id)
            await query.edit_message_text(
                "⏳ **إيقاف التداول وإغلاق كل الصفقات المفتوحة...**",
                parse_mode="Markdown",
            )
            await request_account_details_and_wait()
            results = close_all_open_positions()
            lines = ["⏹ **تم إيقاف التداول الآلي**", ""]
            if not results:
                lines.append("📭 لا توجد صفقات مفتوحة لإغلاقها.")
            else:
                lines.append(f"📤 أُرسلت أوامر إغلاق لـ **{len(results)}** صفقة:")
                for r in results:
                    st = "✅" if r.get("ok") else "❌"
                    lines.append(
                        f"  {st} `{r.get('symbol')}` "
                        f"{'' if r.get('ok') else '— ' + str(r.get('error', ''))}"
                    )
                lines.append("\n_قد يستغرق الوسيط ثانية أو اثنتين لتأكيد الإغلاق — تحقق من «الصفقات المفتوحة»._")
            await request_account_details_and_wait(timeout=3.0)
            await safe_send_message(
                context.bot, user_id, "\n".join(lines), reply_markup=main_keyboard(user_id)
            )

        elif data == "btn_toggle_merge":
            signals_merge_full = not signals_merge_full
            await persist_runtime_state_from_memory(user_id)
            state = "مفعّل ✅" if signals_merge_full else "ملغى ⬜"
            await query.edit_message_text(
                f"🔗 **دمج إشارات الأطر مع التوصية الشاملة:** {state}\n\n"
                "• **مفعّل:** إن تعارض اتجاه إطار التنفيذ مع آخر شاملة حديثة → لا تنفيذ (NEUTRAL).\n"
                "• **ملغى:** كل مسار يعمل مستقلاً.\n\n"
                f"_صلاحية الشاملة للدمج: {os.getenv('SIGNALS_MERGE_MAX_AGE_MINUTES', '90')} دقيقة._",
                reply_markup=main_keyboard(user_id),
                parse_mode="Markdown",
            )


        elif data == "btn_db_stats":
            try:
                report = await db.get_db_performance_report()
                text = db.format_db_performance_telegram(report)
            except Exception as e:
                text = f"⚠️ تعذر جمع إحصاءات DB:\n{e}"
            text = (text or "لا بيانات")[:3900]
            try:
                await query.edit_message_text(
                    text,
                    reply_markup=main_keyboard(user_id),
                    parse_mode="Markdown",
                )
            except BadRequest as e:
                # قيم DB (أسماء جداول/مفاتيح) قد تكسر Markdown — أعد الإرسال كنص عادي
                if "can't parse entities" in str(e).lower():
                    logger.warning(f"⚠️ Markdown فشل في أداء DB، إرسال نص عادي: {e}")
                    await query.edit_message_text(
                        text,
                        reply_markup=main_keyboard(user_id),
                    )
                else:
                    raise

        elif data == "btn_status":
            ctrader_status = "🟢 متصل" if is_ctrader_connected else "🔴 غير متصل"
            db_status = "🟢 متصلة" if db.is_storage_ready() else "🔴 غير متصلة (فعّل USE_FIRESTORE=1 و FIRESTORE_PROJECT)"

            mode_label = "💸 اقتصادي (0) — Cloud Scheduler" if runtime_cheap_mode else "⚡ غير اقتصادي (1) — APScheduler 24/7"
            trade_label = "🟢 مفعّل" if auto_trading_enabled else "⏹ متوقف"
            if runtime_cheap_mode:
                ohlc_line = f"كل `{max(5, OHLC_REFRESH_MINUTES)}` دقيقة"
                ai_line = f"كل `{AUTO_ANALYSIS_INTERVAL_MINUTES}` دقيقة"
            else:
                ohlc_line = f"كل `{NON_CHEAP_OHLC_REFRESH_SECONDS}` ثانية"
                ai_line = f"كل `{NON_CHEAP_ANALYSIS_INTERVAL_MINUTES}` دقيقة"
            status_msg = (
                f"🖥 **حالة الخادم:** 🟢 يعمل بنجاح\n"
                f"⚙️ **وضع التشغيل:** {mode_label}\n"
                f"🤖 **التداول الآلي:** {trade_label}\n"
                f"🔌 **cTrader (مصدر الأسعار الرئيسي):** {ctrader_status}\n"
                f"🗄️ **قاعدة البيانات:** {db_status}\n"
                f"⏱️ **إيقاعات التحديث:**\n"
                f"   • حساب cTrader: كل `{CTRADER_ACCOUNT_REFRESH_SECONDS}` ث\n"
                f"   • Finnhub: كل `{FINNHUB_REFRESH_SECONDS}` ث\n"
                f"   • TradingView (فلتر): كل `{TRADINGVIEW_REFRESH_SECONDS}` ث\n"
                f"   • Forex Factory: `{FOREX_FACTORY_NORMAL_MINUTES}` د (عادي) / `{FOREX_FACTORY_HIGH_IMPACT_MINUTES}` د (خبر قوي)\n"
                f"   • OHLC / الشموع: {ohlc_line}\n"
                f"   • تحليل AI للمتابعات: {ai_line}\n"
                f"📦 **لوت التنفيذ الافتراضي:** `{DEFAULT_TRADE_LOT}`\n"
                f"🧠 **النموذج:** `{AI_MODEL}`"
            )
            await query.edit_message_text(status_msg, reply_markup=main_keyboard(user_id), parse_mode="Markdown")


        elif data == "btn_account":
            await query.edit_message_text(
                "⏳ **جاري جلب بيانات الحساب مباشرةً من cTrader...**",
                parse_mode="Markdown",
            )
            snap = await ensure_ctrader_live_snapshot()
            bal = ctrader_account_info.get("balance", 0.0)
            eq = ctrader_account_info.get("equity", 0.0)
            margin = ctrader_account_info.get("margin", 0.0)
            pnl = eq - bal
            pnl_sign = "+" if pnl >= 0 else ""

            free_m = ctrader_account_info.get("free_margin", 0.0) or 0.0
            ml = ctrader_account_info.get("margin_level", 0.0) or 0.0
            lev = ctrader_account_info.get("leverage")
            upd = ctrader_account_info.get("updated_at")
            upd_s = upd.strftime("%H:%M:%S UTC") if upd else "—"
            msg = (
                f"💳 **تقرير حساب التداول (cTrader لحظي):**\n\n"
                f"{_snapshot_source_label(snap)}\n\n"
                f"🔹 **الرصيد (Balance):** `${bal:,.2f}`\n"
                f"🔹 **الصافي (Equity):** `${eq:,.2f}`\n"
                f"🔹 **الأرباح/الخسائر:** `{pnl_sign}${pnl:,.2f}`\n"
                f"🔹 **الهامش المستغل (Margin):** `${margin:,.2f}`\n"
                f"🔹 **الهامش الحر (Free Margin):** `${free_m:,.2f}`\n"
                f"🔹 **نسبة الهامش (Margin Level):** `{ml:,.2f}%`\n"
                f"🔹 **الرافعة:** `{lev if lev is not None else '—'}`\n"
                f"🕒 **آخر تحديث:** `{upd_s}`"
            )
            await query.edit_message_text(msg, reply_markup=main_keyboard(user_id), parse_mode="Markdown")

        elif data == "btn_positions":
            await query.edit_message_text(
                "⏳ **جاري مزامنة الصفقات مباشرةً من cTrader...**",
                parse_mode="Markdown",
            )
            snap = await ensure_ctrader_live_snapshot()
            src_line = _snapshot_source_label(snap)
            if not active_positions:
                await query.edit_message_text(
                    f"📭 **لا توجد صفقات مفتوحة حالياً.**\n\n{src_line}",
                    reply_markup=main_keyboard(user_id),
                    parse_mode="Markdown",
                )
            else:
                msg = f"📈 **الصفقات المفتوحة حالياً:**\n\n{src_line}\n\n"
                for pos in active_positions:
                    side = "🟢 BUY" if pos.get("trade_type") == "BUY" else "🔴 SELL"
                    msg += (
                        f"🔹 **{pos.get('symbol')}** | {side}\n"
                        f"   • اللوت: `{pos.get('volume')}`\n"
                        f"   • سعر الدخول: `{pos.get('entry_price')}`\n"
                        f"   • الربح/الخسارة: `{pos.get('pnl'):+.2f} USD`\n\n"
                    )
                await query.edit_message_text(msg, reply_markup=main_keyboard(user_id), parse_mode="Markdown")

        elif data == "btn_refresh":
            request_account_details()
            for sym in user_selected_symbols.get(user_id, []):
                for tf in user_selected_tfs.get(user_id, ["H1"]):
                    request_symbol_trendbars(sym, tf)

            await query.edit_message_text(
                "🔄 **جارٍ تحديث كل التحاليل الفنية والأساسية النشطة الآن مباشرة (بدون انتظار الدورة القادمة)...**\n"
                "سيصلك إشعار منفصل فور اكتمال التحديث، قد يستغرق بضع دقائق حسب عدد المتابعات المسجَّلة.",
                reply_markup=main_keyboard(user_id), parse_mode="Markdown"
            )
            # التحديث الفعلي (جلب شموع + مؤشرات فنية + إعادة توليد كل تحليل AI نشط) يعمل كخلفية
            # حتى لا يُعطّل استجابة تيليجرام الفورية لضغطة الزر.
            asyncio.create_task(force_refresh_all_watches(notify_chat_id=user_id))

    except Exception as e:
        error_msg = str(e)
        if "Message is not modified" in error_msg:
            return
        logger.error(f"Error executing callback action for {data}: {e}")
        try:
            # بدون parse_mode — نص الخطأ قد يحتوي ` أو _ ويكسر Markdown
            err_plain = str(e).replace("`", "'")[:500]
            await context.bot.send_message(
                chat_id=user_id,
                text=f"⚠️ حدث خطأ غير متوقع:\n{err_plain}",
                reply_markup=main_keyboard(user_id),
            )
        except Exception:
            pass

# ==================== FastAPI Web Server & Webhook Config ====================

@asynccontextmanager
async def lifespan(app: FastAPI):
    global telegram_app, stop_ctrader_flag, scheduler

    # تهيئة تخزين Firestore (lazy) + ضمان جداول BigQuery إن فُعِّل
    await db.init_db()
    await runtime_cache.init_redis()
    try:
        await restore_runtime_state_on_startup()
    except Exception as e:
        logger.error(f"restore runtime on startup: {e}")

    # تشغيل خيط إعادة الاتصال الخاص بـ cTrader
    stop_ctrader_flag = False
    threading.Thread(target=ctrader_auto_reconnect_loop, daemon=True).start()

    # تهيئة تطبيق تلغرام
    if TELEGRAM_BOT_TOKEN:
        telegram_app = ApplicationBuilder().token(TELEGRAM_BOT_TOKEN).build()
        telegram_app.add_handler(CommandHandler("start", cmd_start))
        telegram_app.add_handler(CallbackQueryHandler(handle_callback_query))

        await telegram_app.initialize()
        await telegram_app.start()

        # إعداد الـ Webhook الخاص بالتلغرام
        try:
            await telegram_app.bot.set_webhook(url=WEBHOOK_URL)
            logger.info(f"Telegram Webhook set to: {WEBHOOK_URL}")
        except Exception as e:
            logger.error(f"Failed to set Telegram Webhook: {e}")

    # جدولة متعددة الإيقاعات: cTrader شبه لحظي، مصادر خارجية بـ TTL، AI للمتابعات النشطة
    scheduler = AsyncIOScheduler(timezone="UTC")
    scheduler.add_job(
        scheduled_ctrader_account_refresh,
        "interval",
        seconds=CTRADER_ACCOUNT_REFRESH_SECONDS,
        id="ctrader_account_refresh",
        next_run_time=datetime.now(timezone.utc) + timedelta(seconds=5),
        max_instances=1,
        coalesce=True,
    )
    scheduler.add_job(
        scheduled_external_sources_refresh,
        "interval",
        seconds=min(FINNHUB_REFRESH_SECONDS, TRADINGVIEW_REFRESH_SECONDS, 30),
        id="external_sources_refresh",
        next_run_time=datetime.now(timezone.utc) + timedelta(seconds=10),
        max_instances=1,
        coalesce=True,
    )
    # صيانة قاعدة البيانات كل 6 ساعات (تنظيف الصفوف القديمة + ANALYZE)
    scheduler.add_job(
        scheduled_db_monitor,
        "interval",
        minutes=int(os.getenv("DB_MONITOR_INTERVAL_MINUTES", "60")),
        id="db_monitor_job",
        next_run_time=datetime.now(timezone.utc) + timedelta(minutes=3),
        max_instances=1,
        coalesce=True,
    )
    scheduler.add_job(
        scheduled_db_cleanup,
        "interval",
        hours=6,
        id="db_cleanup_job",
        next_run_time=datetime.now(timezone.utc) + timedelta(minutes=15),
        max_instances=1,
        coalesce=True,
    )
    # نقطة التعادل / مراقبة الصفقات
    scheduler.add_job(
        manage_breakeven_and_trailing,
        "interval",
        seconds=int(os.getenv("TRADE_BE_CHECK_SECONDS", "20")),
        id="trade_be_trailing",
        next_run_time=datetime.now(timezone.utc) + timedelta(seconds=25),
        max_instances=1,
        coalesce=True,
    )
    # OHLC + تحليل AI تُضبط حسب runtime_cheap_mode (يمكن تبديله لاحقاً من تلغرام)
    scheduler.start()
    mode_detail = apply_runtime_mode_schedules()
    logger.info(
        f"🕒 جدولة المصادر: cTrader حساب كل {CTRADER_ACCOUNT_REFRESH_SECONDS}ث | "
        f"Finnhub كل {FINNHUB_REFRESH_SECONDS}ث | TV كل {TRADINGVIEW_REFRESH_SECONDS}ث | "
        f"FF عادي {FOREX_FACTORY_NORMAL_MINUTES}د / قوي {FOREX_FACTORY_HIGH_IMPACT_MINUTES}د | "
        f"runtime_cheap_mode={runtime_cheap_mode} | env CHEAP_CLOUD_RUN={CHEAP_CLOUD_RUN}"
    )
    logger.info(f"⚙️ {mode_detail}")
    logger.info("🧹 جدولة صيانة DB كل 6 ساعات.")

    if runtime_cheap_mode:
        logger.warning(
            "💸 وضع التوفير مفعّل: يُفضّل min-instances=0 + Cloud Scheduler على /cron/tick "
            "بدل --min-instances=1 و --no-cpu-throttling. الخلفية تعمل فقط أثناء دفء النسخة."
        )
    else:
        logger.warning(
            "⚡ الوضع غير الاقتصادي: يُفضّل --min-instances=1 و --no-cpu-throttling "
            "لكي يعمل APScheduler بشكل مستمر 24/7."
        )


    yield

    # إيقاف الجدولة، التلغرام، cTrader، وقاعدة البيانات عند إيقاف الخادم
    stop_ctrader_flag = True
    if scheduler:
        scheduler.shutdown(wait=False)
    if telegram_app:
        try:
            await telegram_app.stop()
            await telegram_app.shutdown()
        except Exception as e:
            logger.error(f"Error shutting down Telegram App: {e}")
    await runtime_cache.close()
    await db.close_db()

app = FastAPI(lifespan=lifespan)

# لوحة التحكم الويب (بديل Streamlit) على /dashboard
try:
    from web_dashboard import register_dashboard
    register_dashboard(app)
except Exception as _dash_err:
    logging.getLogger("TradingBot").warning(f"Web dashboard not loaded: {_dash_err}")

@app.get("/db/stats")
async def db_stats_endpoint(request: Request):
    """مراقبة أداء قاعدة البيانات — محمي بـ CRON_SECRET إن وُجد."""
    if not _cron_authorized(request):
        raise HTTPException(status_code=401, detail="Unauthorized")
    report = await db.get_db_performance_report()
    return report


@app.get("/")
async def root():
    return {
        "status": "online",
        "service": "AI Trading Assistant",
        "ctrader_connected": is_ctrader_connected,
        "database_connected": db.is_storage_ready(),
        "firestore_enabled": getattr(db, "USE_FIRESTORE", False),
        "dashboard": "/dashboard",
        "db_health": "see /db/stats",
        "runtime_cheap_mode": runtime_cheap_mode,
        "auto_trading_enabled": auto_trading_enabled,
        "auto_analysis_interval_minutes": (
            AUTO_ANALYSIS_INTERVAL_MINUTES
            if runtime_cheap_mode
            else NON_CHEAP_ANALYSIS_INTERVAL_MINUTES
        ),
        "ohlc_refresh": (
            f"{max(5, OHLC_REFRESH_MINUTES)}m"
            if runtime_cheap_mode
            else f"{NON_CHEAP_OHLC_REFRESH_SECONDS}s"
        ),
        "available_timeframes": AVAILABLE_TIMEFRAMES,
        "default_trade_lot": DEFAULT_TRADE_LOT,
        "source_intervals": {
            "ctrader_account_seconds": CTRADER_ACCOUNT_REFRESH_SECONDS,
            "finnhub_seconds": FINNHUB_REFRESH_SECONDS,
            "tradingview_seconds": TRADINGVIEW_REFRESH_SECONDS,
            "forex_factory_normal_minutes": FOREX_FACTORY_NORMAL_MINUTES,
            "forex_factory_high_impact_minutes": FOREX_FACTORY_HIGH_IMPACT_MINUTES,
        },
        "data_hierarchy": "cTrader primary; TradingView/Finnhub/FF filters",
        "bigquery": (bq_sink.status() if bq_sink else {"enabled": False}),
    }




def _cron_authorized(request: Request) -> bool:
    """حماية مسارات Cloud Scheduler الاختيارية."""
    if not CRON_SECRET:
        return True  # مفتوح إن لم يُضبط سر (للتجربة فقط)
    secret = (
        request.headers.get("X-Cron-Secret")
        or request.query_params.get("secret")
        or ""
    )
    return secret == CRON_SECRET


@app.get("/cron/keepalive")
async def cron_keepalive(request: Request):
    """
    نداء رخيص من Cloud Scheduler لإبقاء/إيقاظ النسخة بدون min-instances=1.
    يعيد اتصال cTrader عند الحاجة ويحدّث لقطة الحساب في الكاش.
    """
    if not _cron_authorized(request):
        raise HTTPException(status_code=401, detail="unauthorized")
    # محاولة ضمان اتصال cTrader (الخيط يعمل مع الإقلاع؛ هنا نطلب reconcile)
    if is_ctrader_connected and ACCOUNT_ID:
        request_account_details()
    elif CLIENT_ID and CLIENT_SECRET:
        # الإيقاظ قد يكون بعد Cold Start — الخيط يبدأ من lifespan
        pass
    return {
        "ok": True,
        "mode": "cheap" if runtime_cheap_mode else "always_on",
        "runtime_cheap_mode": runtime_cheap_mode,
        "auto_trading_enabled": auto_trading_enabled,
        "ctrader_connected": is_ctrader_connected,
        "account": {
            "balance": ctrader_account_info.get("balance"),
            "equity": ctrader_account_info.get("equity"),
            "updated_at": str(ctrader_account_info.get("updated_at")),
        },
    }


@app.post("/cron/tick")
@app.get("/cron/tick")
async def cron_tick(request: Request):
    """
    تذكرة دورية رخيصة: تحديث مصادر خارجية + حساب cTrader + (اختياري) المتابعات المستحقة.
    اضبط Cloud Scheduler كل 10–15 دقيقة بدل دفع min-instances=1 طوال اليوم.
    """
    if not _cron_authorized(request):
        raise HTTPException(status_code=401, detail="unauthorized")
    if is_ctrader_connected and ACCOUNT_ID:
        request_account_details()
    try:
        await scheduled_external_sources_refresh()
    except Exception as e:
        logger.error(f"cron tick sources: {e}")
    ohlc_result = None
    try:
        ohlc_result = await scheduled_ohlc_refresh()
    except Exception as e:
        logger.error(f"cron tick ohlc: {e}")
    analysis_ran = False
    if db.is_storage_ready():
        try:
            await scheduled_analysis_job()
            analysis_ran = True
        except Exception as e:
            logger.error(f"cron tick analysis: {e}")
    return {
        "ok": True,
        "ctrader_connected": is_ctrader_connected,
        "analysis_ran": analysis_ran,
        "ohlc": ohlc_result,
        "cache": {
            "has_ff": bool((_source_cache.get("forex_factory") or {}).get("data")),
            "has_news": bool((_source_cache.get("finnhub_news") or {}).get("data")),
            "has_tv": bool((_source_cache.get("tradingview_rss") or {}).get("data")),
        },
    }


@app.get("/cache/snapshot")
async def cache_snapshot():
    """مراقبة اللقطة التشغيلية الجاهزة للوكلاء (بدون أسرار)."""
    snap = runtime_cache.snapshot_for_agents()
    # لا نسرّب توكنات؛ الحساب أرقام فقط
    return {"ok": True, "snapshot": snap, "redis": bool(REDIS_URL)}


@app.post("/hooks/tradingview")
async def tradingview_webhook(request: Request):
    """
    Orchestrator Webhook Receiver — إشارة TradingView.
    أمثلة payload:
      {"symbol": "XAUUSD", "timeframe": "H1", "action": "buy", "secret": "..."}
      {"symbols": ["EURUSD","GBPUSD"], "timeframes": ["H1","H4"]}
    في TradingView Alert: Webhook URL = https://YOUR_SERVICE/hooks/tradingview
    """
    try:
        payload = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="invalid_json")

    if TV_WEBHOOK_SECRET:
        secret = (
            payload.get("secret")
            or request.headers.get("X-Webhook-Secret")
            or request.query_params.get("secret")
        )
        if secret != TV_WEBHOOK_SECRET:
            raise HTTPException(status_code=401, detail="unauthorized")

    symbols: List[str] = []
    if payload.get("symbols"):
        symbols = list(payload["symbols"])
    elif payload.get("symbol"):
        symbols = [str(payload["symbol"])]
    elif payload.get("ticker"):
        symbols = [str(payload["ticker"])]

    tfs: List[str] = []
    if payload.get("timeframes"):
        tfs = list(payload["timeframes"])
    elif payload.get("timeframe") or payload.get("interval"):
        tfs = [str(payload.get("timeframe") or payload.get("interval"))]
    else:
        tfs = ["H1"]

    # تطبيع رموز شائعة من TradingView
    symbols = [s.replace("!", "").replace("FX:", "").replace("OANDA:", "").upper() for s in symbols]

    result = await orchestrate_from_signal(
        symbols=symbols,
        tfs=tfs,
        trigger_source="tradingview_webhook",
        trigger_payload=payload,
    )
    status_code = 200 if result.get("ok") else 502
    return Response(
        content=json.dumps(result, ensure_ascii=False, default=str),
        media_type="application/json",
        status_code=status_code,
    )


@app.post("/orchestrate")
async def orchestrate_api(request: Request):
    """تشغيل يدوي للمنسّق: {"symbols":["XAUUSD"], "timeframes":["H1"]}"""
    try:
        payload = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="invalid_json")
    symbols = payload.get("symbols") or ([payload["symbol"]] if payload.get("symbol") else [])
    tfs = payload.get("timeframes") or [payload.get("timeframe") or "H1"]
    result = await orchestrate_from_signal(
        symbols=list(symbols),
        tfs=list(tfs),
        trigger_source="api_orchestrate",
        trigger_payload=payload,
    )
    return result


@app.post(WEBHOOK_PATH)
async def process_telegram_webhook(request: Request):
    if not telegram_app:
        raise HTTPException(status_code=500, detail="Telegram app not initialized")
    
    try:
        data = await request.json()
        update = Update.de_json(data, telegram_app.bot)
        await telegram_app.process_update(update)
        return Response(status_code=status.HTTP_200_OK)
    except Exception as e:
        logger.error(f"Error processing webhook update: {e}")
        return Response(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR)

if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)