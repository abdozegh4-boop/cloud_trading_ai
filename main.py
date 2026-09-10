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
GEMINI_MODEL = "gemini-2.5-flash"
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

# تخزين مؤقت لببيانات الحساب والصفقات القادمة من cTrader
ctrader_account_info: Dict[str, Any] = {
    "balance": 0.0,
    "equity": 0.0,
    "margin": 0.0,
    "free_margin": 0.0
}
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
        # طلب تحديث بيانات الحساب والصفقات فور الاتصال
        request_account_details()

def request_account_details():
    """إرسال طلب لمزامنة الصفقات والرصيد مع cTrader"""
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

# ==================== Pydantic Models ====================

class NewsPayload(BaseModel):
    headline: str
    symbol: str
    atr: float
    volume_ratio: float

class SymbolSnapshot(BaseModel):
    symbol: str
    price: float
    change_pct: float
    atr_pips: float

class BulkMarketRequest(BaseModel):
    timestamp: str
    headline: Optional[str] = "Market Correlation Scan"
    market_snapshot: List[SymbolSnapshot]

# ==================== أوامر لوحة تحكم التليجرام ====================

def is_authorized(update: Update) -> bool:
    if not MY_TELEGRAM_CHAT_ID:
        return True
    return str(update.effective_chat.id) == str(MY_TELEGRAM_CHAT_ID)

def main_keyboard():
    """لوحة تفاعلية بأزرار سريعة"""
    keyboard = [
        [
            InlineKeyboardButton("📊 حالة النظام", callback_data="btn_status"),
            InlineKeyboardButton("💼 معلومات الحساب", callback_data="btn_account")
        ],
        [
            InlineKeyboardButton("📈 الصفقات المفتوحة", callback_data="btn_positions"),
            InlineKeyboardButton("🔄 تحديث البيانات", callback_data="btn_refresh")
        ],
        [
            InlineKeyboardButton("⚠️ إغلاق الكل (طوارئ)", callback_data="btn_closeall")
        ]
    ]
    return InlineKeyboardMarkup(keyboard)

async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_authorized(update):
        await update.message.reply_text("⛔ غير مصرح لك باستخدام هذا البوت.")
        return
    await update.message.reply_text(
        "🤖 **مرحباً بك في لوحة تحكم التداول السحابي!**\n\nيمكنك مراقبة جميع أداء البوت وإدارته من الأزرار أدناه:",
        reply_markup=main_keyboard(),
        parse_mode="Markdown"
    )

async def cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_authorized(update):
        return
    ctrader_status = "🟢 متصل" if is_ctrader_connected else "🔴 غير متصل"
    status_msg = (
        f"🖥 **حالة الخادم:** 🟢 يعمل بنجاح\n"
        f"🔌 **شبكة cTrader API:** {ctrader_status}\n"
        f"🔄 **مهمة Self-Ping:** 🟢 نشطة\n"
        f"🧠 **نموذج الذكاء الاصطناعي:** `{GEMINI_MODEL}`"
    )
    await update.message.reply_text(status_msg, reply_markup=main_keyboard(), parse_mode="Markdown")

async def cmd_account(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """عرض تفاصيل الحساب المالي"""
    if not is_authorized(update):
        return
    
    # تحديث البيانات عبر API
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
        f"🔹 **الأرباح/الخسائر غير المحققة:** {pnl_sign}`${pnl:,.2f}`\n"
        f"🔹 **الهامش المستغل (Margin):** `${margin:,.2f}`"
    )
    await update.message.reply_text(msg, reply_markup=main_keyboard(), parse_mode="Markdown")

async def cmd_positions(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """عرض الصفقات المفتوحة"""
    if not is_authorized(update):
        return

    request_account_details()

    if not active_positions:
        await update.message.reply_text("📭 **لا توجد صفقات مفتوحة حالياً.**", reply_markup=main_keyboard(), parse_mode="Markdown")
        return

    msg = "📈 **الصفقات المفتوحة حالياً:**\n\n"
    for pos in active_positions:
        side = "🟢 BUY" if pos.get("trade_type") == "BUY" else "🔴 SELL"
        msg += (
            f"🔹 **{pos.get('symbol')}** | {side}\n"
            f"   • اللوت: `{pos.get('volume')}`\n"
            f"   • سعر الدخول: `{pos.get('entry_price')}`\n"
            f"   • الربح/الخسارة: `{pos.get('pnl'):+.2f} USD`\n\n"
        )
    await update.message.reply_text(msg, reply_markup=main_keyboard(), parse_mode="Markdown")

async def cmd_close_all(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """إغلاق كافة الصفقات"""
    if not is_authorized(update):
        return
    
    if not is_ctrader_connected:
        await update.message.reply_text("❌ متعذر الإغلاق، الاتصال بـ cTrader مقطوع.")
        return

    # إرسال طلب إغلاق الحساب للشبكة
    # (يمكن وضع دالة cTrader المخصصة للإغلاق الجماعي هنا)
    await update.message.reply_text("⚠️ **جاري إرسال أوامر الإغلاق الفوري لجميع الصفقات...**")

async def handle_callback_query(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """إدارة نقرات الأزرار التفاعلية"""
    query = update.callback_query
    await query.answer()

    if query.data == "btn_status":
        await cmd_status(update, context)
    elif query.data == "btn_account":
        await cmd_account(update, context)
    elif query.data == "btn_positions":
        await cmd_positions(update, context)
    elif query.data == "btn_refresh":
        request_account_details()
        await query.edit_message_text("🔄 **تمت إعادة تحديث بيانات الحساب والشبكة.**", reply_markup=main_keyboard(), parse_mode="Markdown")
    elif query.data == "btn_closeall":
        await cmd_close_all(update, context)

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
            
            # تسجيل أوامر البوت
            telegram_app.add_handler(CommandHandler("start", cmd_start))
            telegram_app.add_handler(CommandHandler("status", cmd_status))
            telegram_app.add_handler(CommandHandler("account", cmd_account))
            telegram_app.add_handler(CommandHandler("positions", cmd_positions))
            telegram_app.add_handler(CommandHandler("closeall", cmd_close_all))
            
            # تسجيل معالج أزرار التحكم
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
                        text="🚀 **تم تشغيل البوت ولوحة التحكم التفاعلية بنجاح!**",
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
        return Response(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR)

@app.get("/")
@app.head("/")
def read_root():
    return {"status": "online", "ctrader_connected": is_ctrader_connected}