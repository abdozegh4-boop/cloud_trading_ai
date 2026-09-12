import os
import json
import asyncio
import threading
import time
import logging
import httpx
import feedparser
from typing import List, Optional, Dict, Any, Set
from dotenv import load_dotenv
from fastapi import FastAPI, Request, Response, status, HTTPException
from contextlib import asynccontextmanager
from google import genai

# مكتبات التلغرام
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
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
WEBHOOK_PATH = f"/telegram/webhook/{TELEGRAM_BOT_TOKEN}"
WEBHOOK_URL = f"{WEBHOOK_HOST}{WEBHOOK_PATH}"

FINNHUB_API_KEY = os.getenv("FINNHUB_API_KEY", "dae8079r01ql3jf9a350dae8079r01ql3jf9a35g")

# إعدادات cTrader
CTRADER_HOST = os.getenv("CTRADER_HOST", "demo.ctraderapi.com")
CTRADER_PORT = int(os.getenv("CTRADER_PORT", 5035))
CLIENT_ID = os.getenv("CTRADER_CLIENT_ID")
CLIENT_SECRET = os.getenv("CTRADER_CLIENT_SECRET")
ACCESS_TOKEN = os.getenv("CTRADER_ACCESS_TOKEN")
ACCOUNT_ID = int(os.getenv("CTRADER_ACCOUNT_ID", 0)) if os.getenv("CTRADER_ACCOUNT_ID") else 0

telegram_app: Optional[Application] = None
is_ctrader_connected = False
stop_ctrader_flag = False

# بيانات الحساب
ctrader_account_info: Dict[str, Any] = {"balance": 0.0, "equity": 0.0, "margin": 0.0, "free_margin": 0.0}
active_positions: List[Dict[str, Any]] = []

symbol_id_map: Dict[str, int] = {}
trendbars_cache: Dict[str, Dict[str, Any]] = {}

# الأطر الزمنية والأزواج المتاحة
AVAILABLE_TIMEFRAMES = ["M15", "H1", "H4", "D1"]

# تخزين اختيارات المستخدم الحالية (في الذاكرة)
# user_selected_symbols: { user_id: ["EURUSD", "XAUUSD"] }
user_selected_symbols: Dict[int, List[str]] = {}
# user_selected_tfs: { user_id: ["H1", "H4"] }
user_selected_tfs: Dict[int, List[str]] = {}

ALL_AVAILABLE_SYMBOLS = {
    "forex": ["EURUSD", "GBPUSD", "USDJPY", "USDCHF", "AUDUSD", "USDCAD", "NZDUSD", "EURGBP", "GBPJPY"],
    "commodities": ["XAUUSD", "XAGUSD", "USOIL", "UKOIL", "NGAS"],
    "indices": ["US30", "NAS100", "SPX500", "GER40", "UK100"],
    "crypto": ["BTCUSD", "ETHUSD", "SOLUSD", "AAPL", "NVDA", "TSLA"]
}

# ==================== Data Aggregation Layer ====================

async def fetch_forex_factory_calendar() -> List[Dict[str, Any]]:
    url = "https://nfp.ourforecast.com/api/v1/calendar"
    try:
        async with httpx.AsyncClient(timeout=3.0) as http_client:
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
        async with httpx.AsyncClient(timeout=3.0) as http_client:
            resp = await http_client.get(url)
            if resp.status_code == 200:
                articles = resp.json()[:4]
                return [f"- {item.get('headline')}: {item.get('summary')[:120]}..." for item in articles]
    except Exception as e:
        logger.error(f"Finnhub Fetch Error: {e}")
    return []

async def fetch_tradingview_rss() -> List[str]:
    url = "https://www.tradingview.com/feed/"
    try:
        loop = asyncio.get_running_loop()
        feed = await loop.run_in_executor(None, feedparser.parse, url)
        return [f"- {entry.title}" for entry in feed.entries[:4]]
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
    global symbol_id_map, trendbars_cache, ctrader_account_info
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
        logger.info(f"Loaded {len(symbol_id_map)} Symbol IDs from Broker.")

    elif msg_type == ProtoOAGetTrendbarsRes().payloadType:
        res = ProtoOAGetTrendbarsRes()
        res.ParseFromString(message.payload)
        if len(res.trendbar) > 0:
            last_bar = res.trendbar[-1]
            close_price = (last_bar.low + last_bar.deltaClose) / 100000.0
            logger.info(f"Trendbar Received - Bars: {len(res.trendbar)}, Latest Close: {close_price}")

def request_account_details():
    if is_ctrader_connected and ACCOUNT_ID:
        req = ProtoOAReconcileReq()
        req.ctidTraderAccountId = ACCOUNT_ID
        ctrader_client.send(req)

def request_symbol_trendbars(symbol_name: str, timeframe: str = "H1"):
    if not is_ctrader_connected or symbol_name not in symbol_id_map:
        return
    
    symbol_id = symbol_id_map[symbol_name]
    req = ProtoOAGetTrendbarsReq()
    req.ctidTraderAccountId = ACCOUNT_ID
    req.symbolId = symbol_id
    req.period = ProtoMAPeriod.H1 if timeframe == "H1" else ProtoMAPeriod.M15
    req.fromTimestamp = int((time.time() - 86400 * 5) * 1000)
    req.toTimestamp = int(time.time() * 1000)
    
    ctrader_client.send(req)

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

# ==================== AI Analysis Engine ====================

async def generate_comprehensive_analysis(aggregated_data: Dict[str, Any], selected_tfs: List[str]) -> str:
    if not ai_client:
        return "❌ **خطأ:** مفتاح Google Gemini API غير متوفر."

    symbols_list_str = ", ".join(aggregated_data['symbols'])
    tfs_list_str = ", ".join(selected_tfs)

    prompt = f"""
    You are an elite Institutional Quantitative Analyst and Macro Trader.
    Generate a complete, structured multi-asset trading recommendation report for ALL requested symbols:
    
    TARGET SYMBOLS: [{symbols_list_str}]
    SHARED TIMEFRAMES FOR EACH SYMBOL: [{tfs_list_str}]

    --- AGGREGATED MARKET DATA ---
    1. **Forex Factory Calendar:**
    {json.dumps(aggregated_data['forex_factory'], indent=2)}

    2. **Finnhub Real-time News:**
    {chr(10).join(aggregated_data['finnhub_news']) if aggregated_data['finnhub_news'] else 'No news available.'}

    3. **TradingView RSS Analysis:**
    {chr(10).join(aggregated_data['tradingview_rss']) if aggregated_data['tradingview_rss'] else 'No RSS available.'}

    --- INSTRUCTIONS ---
    Provide a clear, detailed breakdown for EVERY single symbol in the target list [{symbols_list_str}].
    Format the output cleanly in Telegram Markdown format (Arabic):

    🎯 **تقرير التحليل الفني والاقتصادي الشامل والجامع**

    📌 **الأزواج المحللة:** {symbols_list_str}
    ⏱️ **الأطر الزمنية المعتمدة لكل زوج:** {tfs_list_str}

    ───────────────────────────
    (Repeat the block below for EACH symbol in [{symbols_list_str}]):

    🔹 **الأصل / الزوج:** [Symbol Name]
    • **الفريمات المحددة:** {tfs_list_str}
    
    📈 **1. النظرة الفنية والتحليل الأخبارى:**
    [دمج الأخبار والاتجاه لكل فريم زمن من الفريمات المحددة]

    ⚡ **2. التوصية التنفيذية:**
    • **نوع الخيار:** 🟢 شراء (BUY) / 🔴 بيع (SELL) / ⚪ محايد (NEUTRAL)
    • **نقطة الدخول:** [Price]
    • **هدف الربح (TP):** [Price]
    • **وقف الخسارة (SL):** [Price]
    • **نسبة المخاطرة إلى العائد:** [R:R Ratio]

    ───────────────────────────

    💡 **توصيات إدارة المخاطر الإجمالية:**
    [نصيحة عامة لتوزيع المخاطرة على الأزواج المختارة]
    """

    try:
        response = ai_client.models.generate_content(
            model=GEMINI_MODEL,
            contents=prompt
        )
        return response.text
    except Exception as e:
        return f"❌ **خطأ أثناء توليد التحليل عبر الذكاء الاصطناعي:**\n`{str(e)}`"

# ==================== لوحات التحكم والأزرار ====================

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
            InlineKeyboardButton(f"🚀 توليد التوصية الموحدة ({syms_count} أزواج)", callback_data="run_multi_analysis")
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
        "🚀 **مرحباً بك في لوحة التداول والتحليل الذكية الموحدة!**\n\n"
        "• قم باختيار **الأزواج المطلوب تحليلها** من الأقسام أدناه.\n"
        "• اضبط **الأطر الزمنية المشتركة** التي تريد تطبيقها على جميع الأزواج.\n"
        "• انقر على **توليد التوصية الموحدة** للحصول على تقرير شامل لكل أزواجك المختارة."
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

    # تهيئة البيانات الافتراضية للمستخدم إن لم تكن موجودة
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
                "اختر الخيار المطلوب أدناه:"
            )
            await query.edit_message_text(
                text,
                reply_markup=main_keyboard(user_id),
                parse_mode="Markdown"
            )

        elif data.startswith("category_"):
            category = data.split("_")[1]
            await query.edit_message_text(
                f"📋 **اختر الأزواج المطلوبة ضمن فئة [{category.upper()}]:**\n"
                f"*(يمكنك تحديد أكثر من زوج، سيتم حفظ الاختيارات تلقائياً)*",
                reply_markup=symbol_picker_keyboard(user_id, category),
                parse_mode="Markdown"
            )

        elif data.startswith("toggle_sym_"):
            _, _, sym, category = data.split("_")
            current_syms = user_selected_symbols.get(user_id, [])
            
            if sym in current_syms:
                current_syms.remove(sym)
            else:
                current_syms.append(sym)
                request_symbol_trendbars(sym, "H1")

            user_selected_symbols[user_id] = current_syms
            await query.edit_message_reply_markup(reply_markup=symbol_picker_keyboard(user_id, category))

        elif data == "open_timeframes_menu":
            await query.edit_message_text(
                "⏱️ **اختر الأطر الزمنية المشتركة للتحليل:**\n"
                "*(هذه الأطر ستطبق على كل الأزواج المحددة)*",
                reply_markup=shared_tf_keyboard(user_id),
                parse_mode="Markdown"
            )

        elif data.startswith("toggle_tf_"):
            tf = data.split("_")[2]
            current_tfs = user_selected_tfs.get(user_id, ["H1"])
            
            if tf in current_tfs:
                if len(current_tfs) > 1:
                    current_tfs.remove(tf)
            else:
                current_tfs.append(tf)
                
            user_selected_tfs[user_id] = current_tfs
            await query.edit_message_reply_markup(reply_markup=shared_tf_keyboard(user_id))

        elif data == "run_multi_analysis":
            selected_syms = user_selected_symbols.get(user_id, [])
            selected_tfs = user_selected_tfs.get(user_id, ["H1"])

            if not selected_syms:
                await query.edit_message_text(
                    "⚠️ **لم تقم باختيار أي زوج!**\nالرجاء اختيار زوج واحد على الأقل من القوائم للبدء.",
                    reply_markup=main_keyboard(user_id),
                    parse_mode="Markdown"
                )
                return

            syms_str = ", ".join(selected_syms)
            tfs_str = ", ".join(selected_tfs)

            await query.edit_message_text(
                f"⏳ **جاري تجميع البيانات والتحليل الموحد...**\n"
                f"• **الأزواج Target:** `{syms_str}`\n"
                f"• **الفريمات:** `{tfs_str}`\n\n"
                f"🧠 **جاري المعالجة بواسطة الذكاء الاصطناعي...**",
                parse_mode="Markdown"
            )

            aggregated = await aggregate_multi_symbols_data(selected_syms)
            report = await generate_comprehensive_analysis(aggregated, selected_tfs)
            await query.message.reply_text(report, reply_markup=main_keyboard(user_id), parse_mode="Markdown")

        elif data == "btn_status":
            ctrader_status = "🟢 متصل" if is_ctrader_connected else "🔴 غير متصل"
            status_msg = (
                f"🖥 **حالة الخادم:** 🟢 يعمل بنجاح\n"
                f"🔌 **شبكة cTrader API:** {ctrader_status}\n"
                f"🧠 **النموذج النشط:** `{GEMINI_MODEL}`"
            )
            await query.edit_message_text(
                status_msg,
                reply_markup=main_keyboard(user_id),
                parse_mode="Markdown"
            )

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
            await query.edit_message_text("🔄 **تم تحديث البيانات بنجاح!**", reply_markup=main_keyboard(user_id), parse_mode="Markdown")

    except Exception as e:
        error_msg = str(e)
        if "Message is not modified" in error_msg:
            return
        logger.error(f"Error executing callback action for {data}: {e}")
        try:
            await query.message.reply_text("⚠️ حدث خطأ غير متوقع، يرجى إعادة المحاولة.", reply_markup=main_keyboard(user_id))
        except Exception:
            pass

# ==================== Service Keep-Alive ====================

async def keep_alive():
    await asyncio.sleep(10)
    limits = httpx.Limits(max_keepalive_connections=5, max_connections=10)
    async with httpx.AsyncClient(limits=limits, timeout=10.0) as client_http:
        while True:
            try:
                res = await client_http.get(WEBHOOK_HOST)
                if res.status_code == 200:
                    logger.info("Keep-Alive Self-Ping Successful")
            except Exception as e:
                logger.error(f"Keep-Alive Error: {e}")
            await asyncio.sleep(480)

# ==================== FastAPI Server & Lifecycle ====================

@asynccontextmanager
async def lifespan(app: FastAPI):
    global telegram_app, stop_ctrader_flag
    
    ping_task = asyncio.create_task(keep_alive())

    stop_ctrader_flag = False
    if CLIENT_ID and CLIENT_SECRET:
        threading.Thread(target=ctrader_auto_reconnect_loop, daemon=True).start()

    if TELEGRAM_BOT_TOKEN:
        try:
            telegram_app = ApplicationBuilder().token(TELEGRAM_BOT_TOKEN).build()
            telegram_app.add_handler(CommandHandler("start", cmd_start))
            telegram_app.add_handler(CallbackQueryHandler(handle_callback_query))

            await telegram_app.initialize()
            await telegram_app.start()

            await telegram_app.bot.set_webhook(
                url=WEBHOOK_URL,
                drop_pending_updates=True,
                max_connections=40
            )
            logger.info(f"Telegram Webhook active on {WEBHOOK_URL}")
        except Exception as e:
            logger.error(f"Telegram App Init Error: {e}")

    yield

    stop_ctrader_flag = True
    ping_task.cancel()
    if telegram_app:
        await telegram_app.bot.delete_webhook()
        await telegram_app.stop()
        await telegram_app.shutdown()

app = FastAPI(title="Cloud Trading AI Engine", lifespan=lifespan)

@app.post(WEBHOOK_PATH)
async def telegram_webhook(request: Request):
    if not telegram_app:
        raise HTTPException(status_code=500, detail="Telegram bot not ready")
    try:
        data = await request.json()
        update = Update.de_json(data, telegram_app.bot)
        await telegram_app.process_update(update)
        return Response(status_code=status.HTTP_200_OK)
    except Exception as e:
        logger.error(f"Webhook Exception: {e}")
        return Response(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR)

@app.get("/")
@app.head("/")
def read_root():
    return {
        "status": "online",
        "model": GEMINI_MODEL,
        "ctrader_connected": is_ctrader_connected
    }

if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", 8080))
    uvicorn.run("main:app", host="0.0.0.0", port=port)