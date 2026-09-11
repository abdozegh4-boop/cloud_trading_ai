import os
import json
import asyncio
import threading
import time
import httpx
from psycopg2 import pool
from psycopg2.extras import RealDictCursor
from typing import List, Optional, Dict, Any
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Request, Response, status
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

# ==================== المتغيرات البيئية ====================
GEMINI_MODEL = "gemini-3.6-flash"
api_key = os.getenv("GEMINI_API_KEY")
client = genai.Client(api_key=api_key) if api_key else None

DATABASE_URL = os.getenv("DATABASE_URL")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
MY_TELEGRAM_CHAT_ID = os.getenv("MY_TELEGRAM_CHAT_ID")
WEBHOOK_HOST = os.getenv("RENDER_EXTERNAL_URL", "https://cloud-trading-ai.onrender.com")
WEBHOOK_PATH = f"/telegram/webhook/{TELEGRAM_BOT_TOKEN}"
WEBHOOK_URL = f"{WEBHOOK_HOST}{WEBHOOK_PATH}"

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
db_pool: Optional[pool.SimpleConnectionPool] = None

ctrader_account_info: Dict[str, Any] = {"balance": 0.0, "equity": 0.0, "margin": 0.0, "free_margin": 0.0}
active_positions: List[Dict[str, Any]] = []

# ==================== إدارة قاعدة البيانات ====================

def init_db_pool():
    global db_pool
    if DATABASE_URL:
        try:
            db_pool = pool.SimpleConnectionPool(1, 10, dsn=DATABASE_URL)
            print("✅ Database connection pool initialized successfully.")
        except Exception as e:
            print(f"❌ Failed to create DB pool: {e}")

def init_db():
    if not db_pool:
        return
    conn = None
    try:
        conn = db_pool.getconn()
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS bot_settings (
                    key VARCHAR(50) PRIMARY KEY,
                    value TEXT NOT NULL,
                    updated_at TIMESTAMP DEFAULT NOW()
                );
            """)
            conn.commit()
    except Exception as e:
        print(f"❌ Error initializing database tables: {e}")
    finally:
        if conn:
            db_pool.putconn(conn)

# ==================== Self-Ping Task ====================

async def keep_alive():
    await asyncio.sleep(10)
    limits = httpx.Limits(max_keepalive_connections=5, max_connections=10)
    async with httpx.AsyncClient(limits=limits, timeout=10.0) as client_http:
        while True:
            try:
                response = await client_http.get(WEBHOOK_HOST)
                if response.status_code == 200:
                    print("🟢 Self-Ping Successful")
            except Exception as e:
                print(f"⚠️ Self-Ping Failed: {e}")
            await asyncio.sleep(480)

# ==================== cTrader Open API ====================

ctrader_client = Client(CTRADER_HOST, CTRADER_PORT, TcpProtocol)

def on_connected(client):
    global is_ctrader_connected
    is_ctrader_connected = True
    print("✅ Connected to cTrader Open API")
    if CLIENT_ID and CLIENT_SECRET:
        request = ProtoOAApplicationAuthReq()
        request.clientId = CLIENT_ID
        request.clientSecret = CLIENT_SECRET
        client.send(request)

def on_disconnected(client, reason):
    global is_ctrader_connected
    is_ctrader_connected = False
    print(f"❌ Disconnected from cTrader Open API: {reason}")

def on_message_received(client, message):
    global ctrader_account_info, active_positions
    msg_type = message.payloadType
    
    if msg_type == ProtoOAApplicationAuthRes().payloadType:
        if ACCOUNT_ID and ACCESS_TOKEN:
            acc_auth_req = ProtoOAAccountAuthReq()
            acc_auth_req.ctidTraderAccountId = int(ACCOUNT_ID)
            acc_auth_req.accessToken = str(ACCESS_TOKEN)
            client.send(acc_auth_req)
            
    elif msg_type == ProtoOAAccountAuthRes().payloadType:
        print(f"🚀 Account {ACCOUNT_ID} Authenticated successfully!")
        request_account_details()

def request_account_details():
    if is_ctrader_connected and ACCOUNT_ID:
        req = ProtoOAReconcileReq()
        req.ctidTraderAccountId = ACCOUNT_ID
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
        print(f"⚠️ cTrader Reactor Error: {e}")

def ctrader_auto_reconnect_loop():
    global stop_ctrader_flag
    reactor_thread = threading.Thread(target=start_ctrader_reactor, daemon=True)
    reactor_thread.start()
    
    while not stop_ctrader_flag:
        if not is_ctrader_connected and CLIENT_ID and CLIENT_SECRET:
            try:
                reactor.callFromThread(ctrader_client.startService)
            except Exception as e:
                print(f"⚠️ cTrader Reconnect Error: {e}")
        for _ in range(30):
            if stop_ctrader_flag:
                break
            time.sleep(1)

# ==================== لوحة التحكم والأزرار المخصصة ====================

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
            InlineKeyboardButton("📊 حالة النظام", callback_data="btn_status"),
            InlineKeyboardButton("💳 معلومات الحساب", callback_data="btn_account")
        ],
        [
            InlineKeyboardButton("📈 الصفقات المفتوحة", callback_data="btn_positions"),
            InlineKeyboardButton("🔄 تحديث البيانات", callback_data="btn_refresh")
        ]
    ]
    return InlineKeyboardMarkup(keyboard)

# ==================== توليد التوصيات عبر AI ====================

async def generate_market_signals(category: str, symbols: List[str]) -> str:
    """استدعاء Gemini لتوليد توصية فنية مباشرة بناءً على الفئة والأزواج المتاحة"""
    if not client:
        return "❌ **خطأ:** مفتاح Google Gemini API غير معرف."

    symbols_str = ", ".join(symbols)
    
    prompt = f"""
    You are an elite Institutional Quantitative Analyst and Forex Trader.
    Generate actionable market signals/recommendations for the following assets available at the broker:
    Category: {category}
    Assets: {symbols_str}

    For each asset (or the most trending 2-3 assets in this group), provide a concise trading signal in Arabic with this exact format:

    🎯 **توصية تحليليّة - [{category}]**

    🔹 **الزوج/الأصل:** [Symbol]
    • **الاتجاه:** 🟢 شراء (BUY) أو 🔴 بيع (SELL)
    • **نقطة الدخول:** [Current/Ideal Entry Price]
    • **هدف الربح (TP):** [Take Profit Price]
    • **وقف الخسارة (SL):** [Stop Loss Price]
    • **نسبة المخاطرة:** Low / Medium
    • **التحليل الفني السريع:** [1 sentence technical reasoning based on current price structure and momentum]

    Keep the tone professional, direct, and formatted with clean Markdown.
    """

    try:
        response = client.models.generate_content(
            model=GEMINI_MODEL,
            contents=prompt
        )
        return response.text
    except Exception as e:
        return f"❌ **حدث خطأ أثناء توليد التحليل:**\n`{str(e)}`"

# ==================== معالجة أوامر التلغرام ====================

async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_authorized(update):
        await update.message.reply_text("⛔ غير مصرح لك باستخدام هذا البوت.")
        return
    await update.message.reply_text(
        "🤖 **مرحباً بك في لوحة التداول وتوصيات الذكاء الاصطناعي!**\n\n"
        "اختر فئة السوق للحصول على تحليلات وتوصيات مباشرة للأزواج المتاحة لدى البروكر:",
        reply_markup=main_keyboard(),
        parse_mode="Markdown"
    )

async def handle_callback_query(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    if not is_authorized(update):
        await query.message.reply_text("⛔ غير مصرح لك باستخدام هذا البوت.")
        return

    # --- توصيات الفوركس ---
    if query.data == "sig_forex":
        await query.message.reply_text("⏳ **جاري تحليل أزواج الفوركس (Forex Majors & Minors)...**")
        forex_symbols = ["EURUSD", "GBPUSD", "USDJPY", "AUDUSD", "USDCAD", "EURGBP"]
        signals = await generate_market_signals("أزواج الفوركس (Forex)", forex_symbols)
        await query.message.reply_text(signals, reply_markup=main_keyboard(), parse_mode="Markdown")

    # --- توصيات المعادن والطاقة ---
    elif query.data == "sig_commodities":
        await query.message.reply_text("⏳ **جاري تحليل المعادن والطاقة (Commodities)...**")
        comm_symbols = ["XAUUSD", "XAGUSD", "USOIL", "UKOIL"]
        signals = await generate_market_signals("المعادن والطاقة (Commodities)", comm_symbols)
        await query.message.reply_text(signals, reply_markup=main_keyboard(), parse_mode="Markdown")

    # --- توصيات المؤشرات ---
    elif query.data == "sig_indices":
        await query.message.reply_text("⏳ **جاري تحليل المؤشرات العالمية (Indices)...**")
        indices_symbols = ["US30", "NAS100", "SPX500", "GER40"]
        signals = await generate_market_signals("المؤشرات العالمية (Indices)", indices_symbols)
        await query.message.reply_text(signals, reply_markup=main_keyboard(), parse_mode="Markdown")

    # --- توصيات الأسهم والعملات الرقمية ---
    elif query.data == "sig_crypto":
        await query.message.reply_text("⏳ **جاري تحليل الأسهم والعملات الرقمية (Crypto & Stocks)...**")
        crypto_symbols = ["BTCUSD", "ETHUSD", "AAPL", "NVDA", "TSLA"]
        signals = await generate_market_signals("الأسهم والعملات الرقمية", crypto_symbols)
        await query.message.reply_text(signals, reply_markup=main_keyboard(), parse_mode="Markdown")

    # --- أزرار حالة النظام والحساب ---
    elif query.data == "btn_status":
        ctrader_status = "🟢 متصل" if is_ctrader_connected else "🔴 غير متصل"
        status_msg = (
            f"🖥 **حالة الخادم:** 🟢 يعمل بنجاح\n"
            f"🔌 **شبكة cTrader API:** {ctrader_status}\n"
            f"🧠 **نموذج الذكاء الاصطناعي:** `{GEMINI_MODEL}`"
        )
        await query.message.reply_text(status_msg, reply_markup=main_keyboard(), parse_mode="Markdown")

    elif query.data == "btn_account":
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

    elif query.data == "btn_positions":
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

    elif query.data == "btn_refresh":
        request_account_details()
        await query.message.reply_text("🔄 **تم تحديث بيانات الحساب والشبكة.**", reply_markup=main_keyboard(), parse_mode="Markdown")

# ==================== دورة حياة التطبيق (Lifespan) ====================

@asynccontextmanager
async def lifespan(app: FastAPI):
    global telegram_app, stop_ctrader_flag
    
    init_db_pool()
    init_db()
    ping_task = asyncio.create_task(keep_alive())

    stop_ctrader_flag = False
    if CLIENT_ID and CLIENT_SECRET:
        ctrader_thread = threading.Thread(target=ctrader_auto_reconnect_loop, daemon=True)
        ctrader_thread.start()

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

            if MY_TELEGRAM_CHAT_ID:
                try:
                    await telegram_app.bot.send_message(
                        chat_id=int(MY_TELEGRAM_CHAT_ID),
                        text=f"🚀 **تم تشغيل لوحة التوصيات بنجاح!**\n🤖 النموذج النشط: `{GEMINI_MODEL}`",
                        reply_markup=main_keyboard(),
                        parse_mode="Markdown"
                    )
                except Exception as e:
                    print(f"⚠️ Telegram notification error: {e}")

        except Exception as e:
            print(f"❌ Webhook configuration failed: {e}")

    yield

    stop_ctrader_flag = True
    ping_task.cancel()

    if telegram_app:
        try:
            await telegram_app.bot.delete_webhook()
            await telegram_app.stop()
            await telegram_app.shutdown()
        except Exception as e:
            print(f"⚠️ Error stopping Telegram Bot: {e}")

    if db_pool:
        db_pool.closeall()

app = FastAPI(title="Cloud Trading AI Backend", lifespan=lifespan)

# ==================== Endpoints ====================

@app.post(WEBHOOK_PATH)
async def telegram_webhook(request: Request):
    if not telegram_app:
        raise HTTPException(status_code=500, detail="Telegram application not initialized")
    try:
        data = await request.json()
        update = Update.de_json(data, telegram_app.bot)
        await telegram_app.process_update(update)
        return Response(status_code=status.HTTP_200_OK)
    except Exception as e:
        print(f"⚠️ Error processing Webhook update: {e}")
        return Response(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR)

@app.get("/")
@app.head("/")
def read_root():
    return {
        "status": "online",
        "active_model": GEMINI_MODEL,
        "ctrader_connected": is_ctrader_connected
    }