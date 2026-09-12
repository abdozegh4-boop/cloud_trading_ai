import os
import json
import asyncio
import threading
import time
import httpx
import feedparser
from typing import List, Optional, Dict, Any, Set
from dotenv import load_dotenv
from fastapi import FastAPI, Request, Response, status, HTTPException
from contextlib import asynccontextmanager
from pydantic import BaseModel
from google import genai
from google.genai import types

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

load_dotenv()

# ==================== المتغيرات البيئية والإعدادات ====================
GEMINI_MODEL = "gemini-3.6-flash"
api_key = os.getenv("GEMINI_API_KEY")
ai_client = genai.Client(api_key=api_key) if api_key else None

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
MY_TELEGRAM_CHAT_ID = os.getenv("MY_TELEGRAM_CHAT_ID")
WEBHOOK_HOST = os.getenv("RENDER_EXTERNAL_URL", "https://cloud-trading-ai.onrender.com")
WEBHOOK_PATH = f"/telegram/webhook/{TELEGRAM_BOT_TOKEN}"
WEBHOOK_URL = f"{WEBHOOK_HOST}{WEBHOOK_PATH}"

# مفتاح Finnhub API
FINNHUB_API_KEY = os.getenv("FINNHUB_API_KEY", "dae8079r01ql3jf9a350dae8079r01ql3jf9a35g")

# إعدادات cTrader Open API
CTRADER_HOST = os.getenv("CTRADER_HOST", "demo.ctraderapi.com")
CTRADER_PORT = int(os.getenv("CTRADER_PORT", 5035))
CLIENT_ID = os.getenv("CTRADER_CLIENT_ID")
CLIENT_SECRET = os.getenv("CTRADER_CLIENT_SECRET")
ACCESS_TOKEN = os.getenv("CTRADER_ACCESS_TOKEN")
ACCOUNT_ID = int(os.getenv("CTRADER_ACCOUNT_ID", 0)) if os.getenv("CTRADER_ACCOUNT_ID") else 0

telegram_app: Optional[Application] = None
is_ctrader_connected = False
stop_ctrader_flag = False

# بيانات الحساب والصفقات المفتوحة
ctrader_account_info: Dict[str, Any] = {"balance": 0.0, "equity": 0.0, "margin": 0.0, "free_margin": 0.0}
active_positions: List[Dict[str, Any]] = []

# ذاكرة ديناميكية لخريطة الأزواج وبيانات الشموع
symbol_id_map: Dict[str, int] = {}
trendbars_cache: Dict[str, Dict[str, Any]] = {}

# الأطر الزمنية والأزواج المتاحة لكل فئة
AVAILABLE_TIMEFRAMES = ["M15", "H1", "H4", "D1"]
user_selected_tfs: Dict[int, List[str]] = {}

ALL_AVAILABLE_SYMBOLS = {
    "forex": ["EURUSD", "GBPUSD", "USDJPY", "USDCHF", "AUDUSD", "USDCAD", "NZDUSD", "EURGBP", "GBPJPY"],
    "commodities": ["XAUUSD", "XAGUSD", "USOIL", "UKOIL", "NGAS"],
    "indices": ["US30", "NAS100", "SPX500", "GER40", "UK100"],
    "crypto": ["BTCUSD", "ETHUSD", "SOLUSD", "AAPL", "NVDA", "TSLA"]
}

user_selections: Dict[int, Dict[str, Set[str]]] = {}

# ==================== Data Aggregation Layer ====================

async def fetch_forex_factory_calendar() -> List[Dict[str, Any]]:
    """سحب تقويم الأحداث الاقتصادية"""
    url = "https://nfp.ourforecast.com/api/v1/calendar"
    try:
        async with httpx.AsyncClient(timeout=5.0) as http_client:
            resp = await http_client.get(url)
            if resp.status_code == 200:
                return resp.json()[:5]
    except Exception as e:
        print(f"⚠️ Forex Factory Fetch Error: {e}")
    return []

async def fetch_finnhub_news() -> List[str]:
    """سحب الأخبار المالية الفورية من Finnhub API"""
    if not FINNHUB_API_KEY:
        return []
    url = f"https://finnhub.io/api/v1/news?category=forex&token={FINNHUB_API_KEY}"
    try:
        async with httpx.AsyncClient(timeout=5.0) as http_client:
            resp = await http_client.get(url)
            if resp.status_code == 200:
                articles = resp.json()[:4]
                return [f"- {item.get('headline')}: {item.get('summary')[:120]}..." for item in articles]
    except Exception as e:
        print(f"⚠️ Finnhub Fetch Error: {e}")
    return []

async def fetch_tradingview_rss() -> List[str]:
    """سحب RSS Feed الخاص بـ TradingView"""
    url = "https://www.tradingview.com/feed/"
    try:
        loop = asyncio.get_running_loop()
        feed = await loop.run_in_executor(None, feedparser.parse, url)
        return [f"- {entry.title}" for entry in feed.entries[:4]]
    except Exception as e:
        print(f"⚠️ TradingView RSS Fetch Error: {e}")
    return []

async def aggregate_market_data(symbol: str) -> Dict[str, Any]:
    """تجميع الأخبار والبيانات الحقيقية بالتوازي"""
    ff_task = fetch_forex_factory_calendar()
    fh_task = fetch_finnhub_news()
    tv_task = fetch_tradingview_rss()
    
    ff_data, fh_data, tv_data = await asyncio.gather(ff_task, fh_task, tv_task)
    
    return {
        "symbol": symbol,
        "forex_factory": ff_data,
        "finnhub_news": fh_data,
        "tradingview_rss": tv_data,
        "technical_bars": trendbars_cache.get(symbol, {})
    }

# ==================== cTrader Open API Client ====================

ctrader_client = Client(CTRADER_HOST, CTRADER_PORT, TcpProtocol)

def on_connected(client):
    global is_ctrader_connected
    is_ctrader_connected = True
    print("✅ Connected to cTrader Open API Engine")
    if CLIENT_ID and CLIENT_SECRET:
        req = ProtoOAApplicationAuthReq()
        req.clientId = CLIENT_ID
        req.clientSecret = CLIENT_SECRET
        client.send(req)

def on_disconnected(client, reason):
    global is_ctrader_connected
    is_ctrader_connected = False
    print(f"❌ Disconnected from cTrader Open API: {reason}")

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
        print(f"🚀 cTrader Account {ACCOUNT_ID} Authenticated!")
        sym_req = ProtoOASymbolsListReq()
        sym_req.ctidTraderAccountId = ACCOUNT_ID
        client.send(sym_req)

    elif msg_type == ProtoOASymbolsListRes().payloadType:
        res = ProtoOASymbolsListRes()
        res.ParseFromString(message.payload)
        for s in res.symbol:
            symbol_id_map[s.symbolName] = s.symbolId
        print(f"📊 Loaded {len(symbol_id_map)} Symbol IDs from Broker.")

    elif msg_type == ProtoOAGetTrendbarsRes().payloadType:
        res = ProtoOAGetTrendbarsRes()
        res.ParseFromString(message.payload)
        if len(res.trendbar) > 0:
            last_bar = res.trendbar[-1]
            close_price = (last_bar.low + last_bar.deltaClose) / 100000.0
            print(f"📈 Trendbar Received - Bars: {len(res.trendbar)}, Latest Close: {close_price}")

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
        print(f"⚠️ cTrader Reactor Exception: {e}")

def ctrader_auto_reconnect_loop():
    global stop_ctrader_flag
    threading.Thread(target=start_ctrader_reactor, daemon=True).start()
    
    while not stop_ctrader_flag:
        if not is_ctrader_connected and CLIENT_ID and CLIENT_SECRET:
            try:
                reactor.callFromThread(ctrader_client.startService)
            except Exception as e:
                print(f"⚠️ Reconnect Error: {e}")
        for _ in range(30):
            if stop_ctrader_flag:
                break
            time.sleep(1)

# ==================== AI Analysis Engine ====================

async def generate_comprehensive_analysis(aggregated_data: Dict[str, Any], selected_tfs: List[str]) -> str:
    if not ai_client:
        return "❌ **خطأ:** مفتاح Google Gemini API غير متوفر."

    prompt = f"""
    You are an elite Institutional Quantitative Analyst and Macro Trader.
    Generate a complete trading recommendation and analysis report for: **{aggregated_data['symbol']}**
    Target Timeframes: {', '.join(selected_tfs)}

    --- AGGREGATED MARKET DATA ---
    1. **Forex Factory Calendar:**
    {json.dumps(aggregated_data['forex_factory'], indent=2)}

    2. **Finnhub Real-time News:**
    {chr(10).join(aggregated_data['finnhub_news']) if aggregated_data['finnhub_news'] else 'No news available.'}

    3. **TradingView RSS Analysis:**
    {chr(10).join(aggregated_data['tradingview_rss']) if aggregated_data['tradingview_rss'] else 'No RSS available.'}

    --- INSTRUCTIONS ---
    Format the output cleanly in Telegram Markdown format (Arabic):

    🎯 **تقرير التحليل الفني والاقتصادي الشامل**

    🔹 **الزوج/الأصل:** {aggregated_data['symbol']}
    🔹 **الأطر الزمنية المستهدفة:** {', '.join(selected_tfs)}

    📊 **1. التحليل الأخباري والتأثير الاقتصادي:**
    [صغ تحليلاً دقيقاً بناءً على أخبار Finnhub وتقويم Forex Factory]

    📈 **2. النظرة الفنية متعددة الفريمات:**
    [حلل الاتجاه ومستويات السيولة والـ ATR المتوقعة بناءً على الفريمات المحددة]

    ⚡ **3. التوصية التنفيذية:**
    • **نوع الخيار:** 🟢 شراء (BUY) / 🔴 بيع (SELL) / ⚪ محايد (NEUTRAL)
    • **نقطة الدخول:** [Price]
    • **هدف الربح (TP):** [Price]
    • **وقف الخسارة (SL):** [Price]
    • **نسبة المخاطرة إلى العائد:** [R:R Ratio]

    💡 **4. توصيات إدارة المخاطر:**
    [نصيحة سريعة لإدارة رأس المال]
    """

    try:
        response = ai_client.models.generate_content(
            model=GEMINI_MODEL,
            contents=prompt
        )
        return response.text
    except Exception as e:
        return f"❌ **خطأ أثناء توليد التحليل عبر الذكاء الاصطناعي:**\n`{str(e)}`"

# ==================== لوحات التحكم والأزرار الكاملة ====================

def is_authorized(update: Update) -> bool:
    if not MY_TELEGRAM_CHAT_ID:
        return True
    user_id = update.effective_chat.id if update.effective_chat else None
    return str(user_id) == str(MY_TELEGRAM_CHAT_ID)

def main_keyboard():
    keyboard = [
        [
            InlineKeyboardButton("💱 توصيات الفوركس", callback_data="sig_forex"),
            InlineKeyboardButton("🥇 المعادن والطاقة", callback_data="sig_commodities")
        ],
        [
            InlineKeyboardButton("📈 توصيات المؤشرات", callback_data="sig_indices"),
            InlineKeyboardButton("₿ الأسهم والعملات", callback_data="sig_crypto")
        ],
        [
            InlineKeyboardButton("⚙️ تعديل قائمة الأزواج المتاحة", callback_data="manage_categories")
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

def category_selection_keyboard():
    keyboard = [
        [
            InlineKeyboardButton("💱 أزواج الفوركس", callback_data="editcat_forex"),
            InlineKeyboardButton("🥇 المعادن والطاقة", callback_data="editcat_commodities")
        ],
        [
            InlineKeyboardButton("📈 المؤشرات العالمية", callback_data="editcat_indices"),
            InlineKeyboardButton("₿ الأسهم والعملات الرقمية", callback_data="editcat_crypto")
        ],
        [
            InlineKeyboardButton("🔙 العودة للقائمة الرئيسية", callback_data="back_main")
        ]
    ]
    return InlineKeyboardMarkup(keyboard)

def symbol_picker_keyboard(category: str):
    symbols = ALL_AVAILABLE_SYMBOLS.get(category, [])
    keyboard = []
    row = []
    for sym in symbols:
        row.append(InlineKeyboardButton(f"📈 {sym}", callback_data=f"selectsym_{sym}"))
        if len(row) == 2:
            keyboard.append(row)
            row = []
    if row:
        keyboard.append(row)
    keyboard.append([InlineKeyboardButton("🔙 العودة للقائمة الرئيسية", callback_data="back_main")])
    return InlineKeyboardMarkup(keyboard)

def tf_selection_keyboard(user_id: int, symbol: str):
    selected = user_selected_tfs.get(user_id, ["H1"])
    keyboard = []
    row = []
    
    for tf in AVAILABLE_TIMEFRAMES:
        icon = "☑️" if tf in selected else "🔲"
        row.append(InlineKeyboardButton(f"{icon} {tf}", callback_data=f"tf_toggle_{tf}_{symbol}"))
        if len(row) == 2:
            keyboard.append(row)
            row = []
    if row:
        keyboard.append(row)

    keyboard.append([InlineKeyboardButton("🚀 ابدأ التحليل الشامل", callback_data=f"run_analysis_{symbol}")])
    keyboard.append([InlineKeyboardButton("🔙 العودة للقائمة الرئيسية", callback_data="back_main")])
    return InlineKeyboardMarkup(keyboard)

# ==================== معالجة أوامر وأزرار التلغرام ====================

async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_authorized(update):
        await update.message.reply_text("⛔ غير مصرح لك باستخدام هذا البوت.")
        return
    await update.message.reply_text(
        "🤖 **مرحباً بك في لوحة التحليل والتداول التفاعلية!**\n\n"
        "اختر خياراً من القائمة أدناه للبدء:",
        reply_markup=main_keyboard(),
        parse_mode="Markdown"
    )

async def handle_callback_query(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    if not is_authorized(update):
        await query.message.reply_text("⛔ غير مصرح لك باستخدام هذا البوت.")
        return

    user_id = query.from_user.id
    data = query.data

    if data == "back_main":
        await query.edit_message_text(
            "🤖 **مرحباً بك في لوحة التحليل والتداول التفاعلية!**\n\n"
            "اختر خياراً من القائمة أدناه:",
            reply_markup=main_keyboard(),
            parse_mode="Markdown"
        )

    # 1. أزرار اختيار الفئات
    elif data in ["sig_forex", "sig_commodities", "sig_indices", "sig_crypto"]:
        cat_map = {"sig_forex": "forex", "sig_commodities": "commodities", "sig_indices": "indices", "sig_crypto": "crypto"}
        cat_name = cat_map[data]
        await query.edit_message_text(
            f"📋 **اختر الزوج المطلوب لتحليله ضمن فئة [{cat_name.upper()}]:**",
            reply_markup=symbol_picker_keyboard(cat_name),
            parse_mode="Markdown"
        )

    # 2. اختيار الزوج وضبط الفريمات
    elif data.startswith("selectsym_"):
        symbol = data.split("_")[1]
        if user_id not in user_selected_tfs:
            user_selected_tfs[user_id] = ["H1"]
        
        request_symbol_trendbars(symbol, "H1")
        
        await query.edit_message_text(
            f"⚙️ **إدارة الأطر الزمنية لـ [{symbol}]**\n\n"
            f"حدد الفريمات المطلوبة ثم انقر على **ابدأ التحليل الشامل**:",
            reply_markup=tf_selection_keyboard(user_id, symbol),
            parse_mode="Markdown"
        )

    elif data.startswith("tf_toggle_"):
        _, _, tf, symbol = data.split("_")
        current_tfs = user_selected_tfs.get(user_id, ["H1"])
        
        if tf in current_tfs:
            if len(current_tfs) > 1:
                current_tfs.remove(tf)
        else:
            current_tfs.append(tf)
            
        user_selected_tfs[user_id] = current_tfs
        await query.edit_message_reply_markup(reply_markup=tf_selection_keyboard(user_id, symbol))

    # 3. تشغيل التحليل
    elif data.startswith("run_analysis_"):
        symbol = data.split("_")[2]
        selected_tfs = user_selected_tfs.get(user_id, ["H1"])

        await query.edit_message_text(
            f"⏳ **جاري تجميع البيانات لـ [{symbol}]...**\n"
            f"• Finnhub News ✅\n• Forex Factory Calendar ✅\n• cTrader Open API ✅\n\n"
            f"🧠 **جاري المعالجة عبر الذكاء الاصطناعي...**",
            parse_mode="Markdown"
        )

        aggregated = await aggregate_market_data(symbol)
        report = await generate_comprehensive_analysis(aggregated, selected_tfs)
        await query.message.reply_text(report, reply_markup=main_keyboard(), parse_mode="Markdown")

    # 4. حالة النظام والمطلوبات الأخرى
    elif data == "manage_categories":
        await query.edit_message_text(
            "⚙️ **تعديل قائمة الأزواج المتاحة**\n\n"
            "الرجاء اختيار الفئة التي تريد إدارة أزواجها:",
            reply_markup=category_selection_keyboard(),
            parse_mode="Markdown"
        )

    elif data == "btn_status":
        ctrader_status = "🟢 متصل" if is_ctrader_connected else "🔴 غير متصل"
        status_msg = (
            f"🖥 **حالة الخادم:** 🟢 يعمل بنجاح\n"
            f"🔌 **شبكة cTrader API:** {ctrader_status}\n"
            f"🧠 **نموذج الذكاء الاصطناعي:** `{GEMINI_MODEL}`"
        )
        await query.message.reply_text(status_msg, reply_markup=main_keyboard(), parse_mode="Markdown")

    elif data == "btn_account":
        request_account_details()
        bal = ctrader_account_info.get("balance", 0.0)
        eq = ctrader_account_info.get("equity", 0.0)
        margin = ctrader_account_info.get("margin", 0.0)
        pnl = eq - bal
        pnl_sign = "🟢 +" if pnl >= 0 else "🔴 "

        msg = (
            f"💳 **تقرير حساب التداول:**\n\n"
            f"🔹 **الرصيد (Balance):** `${bal:,.2f}`\n"
            f"🔹 **الصافي الحالي (Equity):** `${eq:,.2f}`\n"
            f"🔹 **الأرباح/الخسائر:** {pnl_sign}`${pnl:,.2f}`\n"
            f"🔹 **الهامش المستغل (Margin):** `${margin:,.2f}`"
        )
        await query.message.reply_text(msg, reply_markup=main_keyboard(), parse_mode="Markdown")

    elif data == "btn_positions":
        request_account_details()
        if not active_positions:
            await query.message.reply_text("📭 **لا توجد صفقات مفتوحة حالياً.**", reply_markup=main_keyboard(), parse_mode="Markdown")
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
            await query.message.reply_text(msg, reply_markup=main_keyboard(), parse_mode="Markdown")

    elif data == "btn_refresh":
        request_account_details()
        await query.message.reply_text("🔄 **تم تحديث البيانات.**", reply_markup=main_keyboard(), parse_mode="Markdown")

# ==================== Service Keep-Alive ====================

async def keep_alive():
    await asyncio.sleep(10)
    limits = httpx.Limits(max_keepalive_connections=5, max_connections=10)
    async with httpx.AsyncClient(limits=limits, timeout=10.0) as client_http:
        while True:
            try:
                res = await client_http.get(WEBHOOK_HOST)
                if res.status_code == 200:
                    print("🟢 Keep-Alive Self-Ping Successful")
            except Exception as e:
                print(f"⚠️ Keep-Alive Error: {e}")
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
            print(f"🌐 Telegram Webhook active on {WEBHOOK_URL}")
        except Exception as e:
            print(f"❌ Telegram App Init Error: {e}")

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
        print(f"⚠️ Webhook Exception: {e}")
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