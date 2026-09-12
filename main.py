import os
import json
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
from google import genai
from apscheduler.schedulers.asyncio import AsyncIOScheduler

import db  # طبقة التخزين الدائم على Neon (PostgreSQL) + الذاكرة التاريخية

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
GEMINI_MODEL = os.getenv("GEMINI_MODEL", "gemini-3.6-flash")
api_key = os.getenv("GEMINI_API_KEY")
ai_client = genai.Client(api_key=api_key) if api_key else None

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
MY_TELEGRAM_CHAT_ID = os.getenv("MY_TELEGRAM_CHAT_ID")
WEBHOOK_HOST = os.getenv("RENDER_EXTERNAL_URL", "https://cloud-trading-ai.onrender.com")
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

telegram_app: Optional[Application] = None
scheduler: Optional[AsyncIOScheduler] = None
is_ctrader_connected = False
stop_ctrader_flag = False

# بيانات الحساب والصفقات
ctrader_account_info: Dict[str, Any] = {"balance": 0.0, "equity": 0.0, "margin": 0.0, "free_margin": 0.0}
active_positions: List[Dict[str, Any]] = []

symbol_id_map: Dict[str, int] = {}
symbol_id_to_name: Dict[int, str] = {}
trendbars_cache: Dict[str, Dict[str, Any]] = {}
pending_requests_tf: Dict[int, str] = {}  # لتتبع الإطار الزمني للطلبات المرسلة

# الأطر الزمنية والأزواج المتاحة
AVAILABLE_TIMEFRAMES = ["M15", "H1", "H4", "D1"]

# تخزين اختيارات المستخدم الحالية (في الذاكرة)
user_selected_symbols: Dict[int, List[str]] = {}
user_selected_tfs: Dict[int, List[str]] = {}

ALL_AVAILABLE_SYMBOLS = {
    "forex": ["EURUSD", "GBPUSD", "USDJPY", "USDCHF", "AUDUSD", "USDCAD", "NZDUSD", "EURGBP", "GBPJPY"],
    "commodities": ["XAUUSD", "XAGUSD", "USOIL", "UKOIL", "NGAS"],
    "indices": ["US30", "NAS100", "SPX500", "GER40", "UK100"],
    "crypto": ["BTCUSD", "ETHUSD", "SOLUSD", "AAPL", "NVDA", "TSLA"]
}

# ==================== Technical Analysis Engine ====================

def calculate_technical_indicators(bars: List[Any]) -> Dict[str, Any]:
    """
    تستقبل قائمة من trendbars القادمة من cTrader وتحسب:
    RSI (14), EMA (20), EMA (50), ATR (14), والـ Volume الأخير.
    """
    if len(bars) < 50:
        return {"error": "عدد الشموع غير كافٍ لحساب المؤشرات (يُشترط 50 شمعة على الأقل)"}

    closes = [(b.low + b.deltaClose) / 100000.0 for b in bars]
    highs = [(b.low + b.deltaHigh) / 100000.0 for b in bars]
    lows = [b.low / 100000.0 for b in bars]
    volumes = [b.volume for b in bars]

    last_volume = volumes[-1]

    def calc_ema(period: int, prices: List[float]) -> float:
        k = 2 / (period + 1)
        ema = sum(prices[:period]) / period
        for price in prices[period:]:
            ema = (price * k) + (ema * (1 - k))
        return ema

    ema_20 = calc_ema(20, closes)
    ema_50 = calc_ema(50, closes)

    gains, losses = [], []
    for i in range(1, len(closes)):
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

    tr_list = []
    for i in range(1, len(closes)):
        tr = max(
            highs[i] - lows[i],
            abs(highs[i] - closes[i - 1]),
            abs(lows[i] - closes[i - 1])
        )
        tr_list.append(tr)

    atr_14 = sum(tr_list[-14:]) / 14

    return {
        "last_price": round(closes[-1], 5),
        "volume": last_volume,
        "rsi_14": round(rsi_14, 2),
        "ema_20": round(ema_20, 5),
        "ema_50": round(ema_50, 5),
        "atr_14": round(atr_14, 5)
    }

# ==================== Data Aggregation Layer ====================

async def fetch_forex_factory_calendar() -> List[Dict[str, Any]]:
    url = "https://nfp.ourforecast.com/api/v1/calendar"
    try:
        async with httpx.AsyncClient(timeout=4.0) as http_client:
            resp = await http_client.get(url)
            if resp.status_code == 200:
                return resp.json()[:5]
    except Exception as e:
        logger.error(f"Forex Factory Fetch Error: {e}")
    return []

async def fetch_finnhub_news() -> List[str]:
    if not FINNHUB_API_KEY:
        return []
    url = f"https://finnhub.io/api/v1/news?category=forex&token={FINNHUB_API_KEY}"
    try:
        async with httpx.AsyncClient(timeout=4.0) as http_client:
            resp = await http_client.get(url)
            if resp.status_code == 200:
                articles = resp.json()[:5]
                return [f"- {item.get('headline')}: {item.get('summary')[:120]}..." for item in articles]
    except Exception as e:
        logger.error(f"Finnhub Fetch Error: {e}")
    return []

async def fetch_tradingview_rss() -> List[str]:
    url = "https://www.tradingview.com/feed/"
    try:
        loop = asyncio.get_running_loop()
        feed = await loop.run_in_executor(None, feedparser.parse, url)
        return [f"- {entry.title}: {entry.summary if hasattr(entry, 'summary') else ''}" for entry in feed.entries[:5]]
    except Exception as e:
        logger.error(f"TradingView RSS Fetch Error: {e}")
    return []

async def aggregate_multi_symbols_data(symbols: List[str]) -> Dict[str, Any]:
    ff_task = fetch_forex_factory_calendar()
    fh_task = fetch_finnhub_news()
    tv_task = fetch_tradingview_rss()
    
    ff_data, fh_data, tv_data = await asyncio.gather(ff_task, fh_task, tv_task)
    
    symbols_technical = {}
    for sym in symbols:
        symbols_technical[sym] = trendbars_cache.get(sym, {})
        
    return {
        "symbols": symbols,
        "forex_factory": ff_data,
        "finnhub_news": fh_data,
        "tradingview_rss": tv_data,
        "symbols_technical": symbols_technical
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

    elif msg_type == ProtoOASymbolsListRes().payloadType:
        res = ProtoOASymbolsListRes()
        res.ParseFromString(message.payload)
        for s in res.symbol:
            symbol_id_map[s.symbolName] = s.symbolId
            symbol_id_to_name[s.symbolId] = s.symbolName
        logger.info(f"Loaded {len(symbol_id_map)} Symbol IDs from Broker.")

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
            close_price = (last_bar.low + last_bar.deltaClose) / 100000.0
            logger.info(f"Trendbar Received [{sym_name} - {tf}] - Bars: {len(res.trendbar)}, Latest Close: {close_price}")

    elif msg_type == ProtoOAReconcileRes().payloadType:
        res = ProtoOAReconcileRes()
        res.ParseFromString(message.payload)
        if hasattr(res, 'account'):
            ctrader_account_info["balance"] = res.account.balance / 100.0
            ctrader_account_info["equity"] = res.account.balance / 100.0
        
        active_positions.clear()
        for pos in res.position:
            sym_name = symbol_id_to_name.get(pos.tradeData.symbolId, f"ID_{pos.tradeData.symbolId}")
            active_positions.append({
                "position_id": pos.positionId,
                "symbol": sym_name,
                "trade_type": "BUY" if pos.tradeData.tradeSide == ProtoOATradeSide.BUY else "SELL",
                "volume": pos.tradeData.volume / 100000.0,
                "entry_price": pos.price,
                "pnl": pos.utcLastUpdateTimestamp / 100.0
            })
        logger.info(f"Account Reconciled: Balance ${ctrader_account_info['balance']}, Open Positions: {len(active_positions)}")

def request_account_details():
    if is_ctrader_connected and ACCOUNT_ID:
        req = ProtoOAReconcileReq()
        req.ctidTraderAccountId = ACCOUNT_ID
        ctrader_client.send(req)

def request_symbol_trendbars(symbol_name: str, timeframe: str = "H1"):
    if not is_ctrader_connected or symbol_name not in symbol_id_map:
        return
    
    symbol_id = symbol_id_map[symbol_name]
    pending_requests_tf[symbol_id] = timeframe
    
    period_map = {
        "M15": ProtoOATrendbarPeriod.M15,
        "H1": ProtoOATrendbarPeriod.H1,
        "H4": ProtoOATrendbarPeriod.H4,
        "D1": ProtoOATrendbarPeriod.D1
    }
    
    req = ProtoOAGetTrendbarsReq()
    req.ctidTraderAccountId = ACCOUNT_ID
    req.symbolId = symbol_id
    req.period = period_map.get(timeframe, ProtoOATrendbarPeriod.H1)
    req.fromTimestamp = int((time.time() - 86400 * 30) * 1000)
    req.toTimestamp = int(time.time() * 1000)
    
    ctrader_client.send(req)

async def ensure_trendbars_ready(symbol: str, tf: str, wait_seconds: float = 4.0) -> List[Any]:
    """
    يرجع الشموع المخزّنة محلياً لهذا الزوج/الإطار؛ وإن لم تكن متوفرة بعد،
    يطلبها من cTrader وينتظر قليلاً قبل إعادة المحاولة (تُستخدم من المهمة المجدولة).
    """
    bars = trendbars_cache.get(symbol, {}).get(tf, [])
    if not bars:
        request_symbol_trendbars(symbol, tf)
        await asyncio.sleep(wait_seconds)
        bars = trendbars_cache.get(symbol, {}).get(tf, [])
    return bars


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

# ==================== AI Specific Analysis Prompts ====================

async def run_specific_analysis(analysis_type: str, aggregated_data: Dict[str, Any], selected_tfs: List[str],
                                 history_text: str = "") -> str:
    if not ai_client:
        return "❌ **خطأ:** مفتاح Google Gemini API غير متوفر في متغيرات البيئة."

    symbols_list_str = ", ".join(aggregated_data['symbols'])
    tfs_list_str = ", ".join(selected_tfs)

    memory_block = (
        f"""
        --- الذاكرة التاريخية (تحاليل سابقة لنفس الأزواج والأطر، الأقدم أولاً) ---
        {history_text}
        ملاحظة: قارن قراءتك الحالية بهذه التحاليل السابقة إن وُجدت، ونوّه صراحة إلى
        أي تغيّر في الاتجاه أو التوصية أو استمرارية السيناريو السابق.
        """
        if history_text else ""
    )

    if analysis_type == "forex_factory":
        prompt = f"""
        You are a Senior Economic Analyst. Analyze the Forex Factory Calendar data for the requested symbols: [{symbols_list_str}]
        Shared Timeframes: [{tfs_list_str}]

        Forex Factory Data:
        {json.dumps(aggregated_data['forex_factory'], indent=2)}
        {memory_block}
        Provide a detailed Economic Calendar Analysis in Arabic covering:
        1. Upcoming high-impact economic releases affecting [{symbols_list_str}].
        2. Expected volatility levels during these news events.
        3. Fundamental Bias (Positive / Negative / Neutral) for each symbol based strictly on the economic calendar.
        """

    elif analysis_type == "finnhub":
        prompt = f"""
        You are a Real-Time Financial News Analyst. Analyze the Finnhub Real-time News feed for the requested symbols: [{symbols_list_str}]
        Shared Timeframes: [{tfs_list_str}]

        Finnhub News Data:
        {chr(10).join(aggregated_data['finnhub_news']) if aggregated_data['finnhub_news'] else 'No news items available.'}
        {memory_block}
        Provide a Breaking News Analysis in Arabic covering:
        1. Key market headlines impacting [{symbols_list_str}].
        2. Short-term sentiment analysis (Bullish / Bearish sentiment score).
        3. Immediate risks or catalysts to watch for the specified timeframes [{tfs_list_str}].
        """

    elif analysis_type == "tradingview":
        prompt = f"""
        You are a Chief Technical Strategist. Analyze the TradingView RSS feed and Technical bar data for the requested symbols: [{symbols_list_str}]
        Shared Timeframes: [{tfs_list_str}]

        TradingView Feed:
        {chr(10).join(aggregated_data['tradingview_rss']) if aggregated_data['tradingview_rss'] else 'No RSS items available.'}
        {memory_block}
        Provide a Technical Overview in Arabic covering:
        1. Multi-timeframe trend outlook on [{tfs_list_str}] for each symbol in [{symbols_list_str}].
        2. Major support and resistance zones identified.
        3. Chart patterns or key breakout levels.
        """

    else:  # All combined / Full recommendation
        prompt = f"""
        You are an Institutional Master Trader. Generate a Full Trading Recommendation report combining Economic Calendar, Real-time News, TradingView Feed, and Technicals for: [{symbols_list_str}]
        Shared Timeframes: [{tfs_list_str}]

        --- AGGREGATED DATA ---
        Calendar: {json.dumps(aggregated_data['forex_factory'], indent=2)}
        News: {chr(10).join(aggregated_data['finnhub_news'])}
        TradingView: {chr(10).join(aggregated_data['tradingview_rss'])}
        {memory_block}
        Provide a complete execution report in Arabic for EVERY symbol in [{symbols_list_str}]:
        • Symbol Name & Timeframes
        • Integrated Fundamental & Technical Assessment
        • Signal: BUY / SELL / NEUTRAL
        • Entry Price, Take Profit (TP), Stop Loss (SL), Risk/Reward Ratio
        """

    try:
        loop = asyncio.get_running_loop()
        response = await loop.run_in_executor(
            None,
            lambda: ai_client.models.generate_content(
                model=GEMINI_MODEL,
                contents=prompt
            )
        )
        return response.text
    except Exception as e:
        logger.error(f"Gemini Analysis Error: {e}")
        return f"❌ **خطأ أثناء توليد التحليل عبر الذكاء الاصطناعي:**\n`{str(e)}`"

async def build_history_text(analysis_type: str, symbols: List[str], tfs: List[str]) -> str:
    """يجلب آخر 3 تحاليل سابقة لنفس التركيبة من Neon ليُمرَّرا كـ'ذاكرة' إلى الذكاء الاصطناعي."""
    history = await db.get_report_history(analysis_type, symbols, tfs, limit=3)
    if not history:
        return ""
    lines = []
    for h in reversed(history):  # الأقدم أولاً حتى يقرأها النموذج بترتيب زمني منطقي
        ts = h["created_at"].strftime("%Y-%m-%d %H:%M UTC")
        snippet = h["report_text"][:600]
        lines.append(f"[{ts}]\n{snippet}\n")
    return "\n".join(lines)


async def get_or_generate_report(analysis_type: str, symbols: List[str], tfs: List[str]) -> str:
    """
    الكاش الذكي: يحاول أولاً قراءة آخر تحليل مخزّن في Neon.
    - إن كان عمره أقل من فترة التحديث التلقائي (AUTO_ANALYSIS_INTERVAL_MINUTES) → يُعاد فوراً "من الذاكرة"
      دون أي استدعاء جديد لـ Gemini (يوفّر الوقت والتكلفة).
    - إن لم يوجد تحليل كافٍ الحداثة → يُولَّد تحليل جديد الآن، يُخزَّن، وتُسجَّل هذه التركيبة
      كـ'متابعة نشطة' لتُحدَّث تلقائياً من الآن فصاعداً كل AUTO_ANALYSIS_INTERVAL_MINUTES دقيقة
      عبر المهمة المجدولة scheduled_analysis_job، دون تدخل أي مستخدم.
    """
    cached = await db.get_latest_report(analysis_type, symbols, tfs)
    max_age = timedelta(minutes=db.AUTO_ANALYSIS_INTERVAL_MINUTES)

    if cached and (datetime.now(timezone.utc) - cached["created_at"]) < max_age:
        age_min = int((datetime.now(timezone.utc) - cached["created_at"]).total_seconds() // 60)
        return f"🗄️ *(من الذاكرة المخزّنة — آخر تحديث قبل {age_min} دقيقة)*\n\n{cached['report_text']}"

    aggregated = await aggregate_multi_symbols_data(symbols)
    history_text = await build_history_text(analysis_type, symbols, tfs)
    report = await run_specific_analysis(analysis_type, aggregated, tfs, history_text)

    await db.save_report(analysis_type, symbols, tfs, report, raw_data=aggregated)
    await db.register_active_watch(analysis_type, symbols, tfs)

    prefix = f"🆕 *(تحليل جديد الآن — تم تسجيله للتحديث التلقائي كل {db.AUTO_ANALYSIS_INTERVAL_MINUTES} دقيقة)*\n\n"
    return prefix + report


async def scheduled_analysis_job():
    """
    مهمة تعمل تلقائياً كل AUTO_ANALYSIS_INTERVAL_MINUTES دقيقة (مجدولة عبر APScheduler):
    1) تحدّث المؤشرات الفنية لكل الأزواج/الأطر التابعة للمتابعات النشطة وتخزّنها في Neon.
    2) تعيد توليد كل تحليل AI مسجَّل في active_watches باستخدام أحدث بيانات + الذاكرة التاريخية،
       بحيث يجد المستخدم عند الضغط على أي زر تحليلاً جاهزاً "من الذاكرة" فوراً دون انتظار.
    """
    if not db.pool:
        return
    logger.info("⏰ بدء دورة التحليل التلقائي المجدولة...")
    due = await db.get_due_watches(db.AUTO_ANALYSIS_INTERVAL_MINUTES)

    for watch in due:
        analysis_type = watch["analysis_type"]
        symbols = [s for s in watch["symbols"].split(",") if s]
        tfs = [t for t in watch["timeframes"].split(",") if t]
        if not symbols or not tfs:
            continue

        try:
            # 1) تحديث المؤشرات الفنية لكل زوج/إطار قبل التحليل (تُستخدم أيضاً كسجل تاريخي مستقل)
            for sym in symbols:
                for tf in tfs:
                    bars = await ensure_trendbars_ready(sym, tf)
                    if bars:
                        indicators = calculate_technical_indicators(bars)
                        await db.save_technical_snapshot(sym, tf, indicators)

            # 2) توليد التحليل الجديد مع تمرير الذاكرة التاريخية للمقارنة
            aggregated = await aggregate_multi_symbols_data(symbols)
            history_text = await build_history_text(analysis_type, symbols, tfs)
            report = await run_specific_analysis(analysis_type, aggregated, tfs, history_text)

            await db.save_report(analysis_type, symbols, tfs, report, raw_data=aggregated)
            await db.mark_watch_run(analysis_type, symbols, tfs)

            logger.info(f"✅ تحديث تلقائي مكتمل: [{analysis_type}] {symbols} / {tfs}")
        except Exception as e:
            logger.error(f"❌ خطأ أثناء التحديث التلقائي لـ {analysis_type}/{symbols}/{tfs}: {e}")

# ==================== لوحات التحكم والأزرار ====================

async def safe_send_message(bot, chat_id: int, text: str, reply_markup=None):
    """
    يحاول إرسال الرسالة بتنسيق Markdown أولاً. نصوص Gemini المولَّدة تلقائياً
    قد تحتوي أحياناً على رمز * أو _ أو ` غير متوازن (مثلاً "50% من *الشراء" بدون
    إغلاق)، وهذا يجعل تلغرام يرفض الرسالة كاملة بخطأ:
    "Can't parse entities: can't find end of the entity starting at byte offset X".
    لتفادي فقدان الرسالة بالكامل، إن فشل التحليل نعيد الإرسال كنص عادي بدون تنسيق.
    """
    try:
        await bot.send_message(chat_id=chat_id, text=text, reply_markup=reply_markup, parse_mode="Markdown")
    except BadRequest as e:
        if "can't parse entities" in str(e).lower():
            logger.warning(f"⚠️ فشل تحليل Markdown، إعادة الإرسال كنص عادي: {e}")
            try:
                await bot.send_message(chat_id=chat_id, text=text, reply_markup=reply_markup)
            except Exception as e2:
                logger.error(f"❌ فشل الإرسال حتى كنص عادي: {e2}")
        else:
            raise


def is_authorized(update: Update) -> bool:
    if not MY_TELEGRAM_CHAT_ID:
        return True
    user_id = update.effective_chat.id if update.effective_chat else None
    return str(user_id) == str(MY_TELEGRAM_CHAT_ID)

def main_keyboard(user_id: int):
    selected_syms = user_selected_symbols.get(user_id, [])
    selected_tfs = user_selected_tfs.get(user_id, ["H1"])
    
    syms_count = len(selected_syms)
    tfs_count = len(selected_tfs)

    keyboard = [
        [
            InlineKeyboardButton("💱 أزواج الفوركس", callback_data="category_forex"),
            InlineKeyboardButton("🥇 المعادن والطاقة", callback_data="category_commodities")
        ],
        [
            InlineKeyboardButton("📈 المؤشرات العالمية", callback_data="category_indices"),
            InlineKeyboardButton("₿ الأسهم والعملات", callback_data="category_crypto")
        ],
        [
            InlineKeyboardButton(f"⏱️ الأطر الزمنية المشتركة ({tfs_count})", callback_data="open_timeframes_menu")
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
            InlineKeyboardButton(f"🧠 التوصية الموحدة الشاملة ({syms_count} أزواج)", callback_data="run_full_analysis")
        ],
        [
            InlineKeyboardButton("📊 حالة النظام", callback_data="btn_status"),
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

async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_authorized(update):
        await update.message.reply_text("⛔ غير مصرح لك باستخدام هذا البوت.")
        return
    
    user_id = update.effective_chat.id
    if user_id not in user_selected_symbols:
        user_selected_symbols[user_id] = ["EURUSD"]
    if user_id not in user_selected_tfs:
        user_selected_tfs[user_id] = ["H1"]

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
                await query.edit_message_reply_markup(reply_markup=symbol_picker_keyboard(user_id, category))

        elif data == "open_timeframes_menu":
            await query.edit_message_text(
                "⏱️ **اختر الأطر الزمنية المشتركة للتحليل:**",
                reply_markup=shared_tf_keyboard(user_id),
                parse_mode="Markdown"
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

                    indicators = calculate_technical_indicators(bars)
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

        elif data in ["analyze_forexfactory", "analyze_finnhub", "analyze_tradingview", "run_full_analysis"]:
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
                "analyze_forexfactory": ("📅 تحليل التقويم الاقتصادي (Forex Factory)", "forex_factory"),
                "analyze_finnhub": ("📰 تحليل الأخبار الفورية (Finnhub)", "finnhub"),
                "analyze_tradingview": ("📉 تحليل الرؤية الفنية (TradingView)", "tradingview"),
                "run_full_analysis": ("🧠 التوصية الموحدة الشاملة", "full")
            }

            label_name, internal_type = type_labels[data]

            await query.edit_message_text(
                f"⏳ **جاري التحقق من الذاكرة المخزّنة لـ [{label_name}]...**\n"
                f"• **الأزواج:** `{', '.join(selected_syms)}`\n"
                f"• **الأطر الزمنية:** `{', '.join(selected_tfs)}`\n\n"
                f"🧠 **إن لم يوجد تحليل حديث كافٍ سيتم توليد واحد جديد عبر Gemini AI...**",
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

        elif data == "btn_status":
            ctrader_status = "🟢 متصل" if is_ctrader_connected else "🔴 غير متصل"
            db_status = "🟢 متصلة" if db.pool else "🔴 غير متصلة (تحقق من DATABASE_URL)"
            status_msg = (
                f"🖥 **حالة الخادم:** 🟢 يعمل بنجاح\n"
                f"🔌 **شبكة cTrader API:** {ctrader_status}\n"
                f"🗄️ **قاعدة بيانات Neon:** {db_status}\n"
                f"⏱️ **دورة التحديث التلقائي:** كل `{db.AUTO_ANALYSIS_INTERVAL_MINUTES}` دقيقة\n"
                f"🧠 **النموذج النشط:** `{GEMINI_MODEL}`"
            )
            await query.edit_message_text(status_msg, reply_markup=main_keyboard(user_id), parse_mode="Markdown")

        elif data == "btn_account":
            request_account_details()
            bal = ctrader_account_info.get("balance", 0.0)
            eq = ctrader_account_info.get("equity", 0.0)
            margin = ctrader_account_info.get("margin", 0.0)
            pnl = eq - bal
            pnl_sign = "+" if pnl >= 0 else ""

            msg = (
                f"💳 **تقرير حساب التداول:**\n\n"
                f"🔹 **الرصيد (Balance):** `${bal:,.2f}`\n"
                f"🔹 **الصافي الحالي (Equity):** `${eq:,.2f}`\n"
                f"🔹 **الأرباح/الخسائر:** `{pnl_sign}${pnl:,.2f}`\n"
                f"🔹 **الهامش المستغل (Margin):** `${margin:,.2f}`"
            )
            await query.edit_message_text(msg, reply_markup=main_keyboard(user_id), parse_mode="Markdown")

        elif data == "btn_positions":
            request_account_details()
            if not active_positions:
                await query.edit_message_text("📭 **لا توجد صفقات مفتوحة حالياً.**", reply_markup=main_keyboard(user_id), parse_mode="Markdown")
            else:
                msg = "📈 **الصفقات المفتوحة حالياً:**\n\n"
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
            await query.edit_message_text("🔄 **تم تحديث البيانات وإرسال طلبات الشموع بنجاح!**", reply_markup=main_keyboard(user_id), parse_mode="Markdown")

    except Exception as e:
        error_msg = str(e)
        if "Message is not modified" in error_msg:
            return
        logger.error(f"Error executing callback action for {data}: {e}")
        try:
            await context.bot.send_message(
                chat_id=user_id,
                text=f"⚠️ **حدث خطأ غير متوقع:**\n`{str(e)}`",
                reply_markup=main_keyboard(user_id)
            )
        except Exception:
            pass

# ==================== FastAPI Web Server & Webhook Config ====================

@asynccontextmanager
async def lifespan(app: FastAPI):
    global telegram_app, stop_ctrader_flag, scheduler

    # تهيئة قاعدة بيانات Neon (Connection Pool + إنشاء الجداول إن لم تكن موجودة)
    await db.init_db()

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

    # بدء الجدولة التلقائية لتحديث كل التحاليل "النشطة" دورياً (فقط إن كانت Neon متصلة)
    if db.pool:
        scheduler = AsyncIOScheduler(timezone="UTC")
        scheduler.add_job(
            scheduled_analysis_job,
            "interval",
            minutes=db.AUTO_ANALYSIS_INTERVAL_MINUTES,
            id="auto_analysis_job",
            next_run_time=datetime.now(timezone.utc) + timedelta(seconds=30),  # أول تشغيل بعد 30 ثانية من الإقلاع
            max_instances=1,
            coalesce=True,
        )
        scheduler.start()
        logger.info(f"🕒 تم جدولة التحليل التلقائي كل {db.AUTO_ANALYSIS_INTERVAL_MINUTES} دقيقة.")

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
    await db.close_db()

app = FastAPI(lifespan=lifespan)

@app.get("/")
async def root():
    return {
        "status": "online",
        "service": "AI Trading Assistant",
        "ctrader_connected": is_ctrader_connected,
        "database_connected": db.pool is not None,
        "auto_analysis_interval_minutes": db.AUTO_ANALYSIS_INTERVAL_MINUTES
    }

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