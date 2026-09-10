import os
import json
import asyncio
import psycopg2
from psycopg2.extras import RealDictCursor
from typing import List, Optional
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException
from contextlib import asynccontextmanager
from pydantic import BaseModel
from google import genai
from google.genai import types

# مكتبات التلغرام
from telegram import Update
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes

# مكتبات cTrader Open API
from ctrader_open_api import Client, TcpProtocol
from ctrader_open_api.messages.OpenApiCommonMessages_pb2 import *
from ctrader_open_api.messages.OpenApiMessages_pb2 import *

# تحميل المتغيرات البيئية
load_dotenv()

# إعدادات الذكاء الاصطناعي وقواعد البيانات
GEMINI_MODEL = "gemini-2.5-flash"
api_key = os.getenv("GEMINI_API_KEY")
client = genai.Client(api_key=api_key) if api_key else None

DATABASE_URL = os.getenv("DATABASE_URL")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")

# إعدادات cTrader Open API
CTRADER_HOST = os.getenv("CTRADER_HOST", "demo.ctraderapi.com")
CTRADER_PORT = int(os.getenv("CTRADER_PORT", 5035))
CLIENT_ID = os.getenv("CTRADER_CLIENT_ID")
CLIENT_SECRET = os.getenv("CTRADER_CLIENT_SECRET")
ACCESS_TOKEN = os.getenv("CTRADER_ACCESS_TOKEN")
ACCOUNT_ID = int(os.getenv("CTRADER_ACCOUNT_ID", 0)) if os.getenv("CTRADER_ACCOUNT_ID") else 0

# ==================== إعداد عميل cTrader Open API ====================

ctrader_client = Client(CTRADER_HOST, CTRADER_PORT, TcpProtocol)

def on_connected(client):
    print("✅ Connected to cTrader Open API")
    if CLIENT_ID and CLIENT_SECRET:
        request = ProtoOAApplicationAuthReq()
        request.clientId = CLIENT_ID
        request.clientSecret = CLIENT_SECRET
        client.send(request)

def on_disconnected(client, reason):
    print(f"❌ Disconnected from cTrader Open API: {reason}")

def on_message_received(client, message):
    msg_type = message.payloadType

    # استجابة توثيق التطبيق
    if msg_type == ProtoOAApplicationAuthRes().payloadType:
        print("✅ Application Authenticated successfully.")
        if ACCOUNT_ID and ACCESS_TOKEN:
            acc_auth_req = ProtoOAAccountAuthReq()
            acc_auth_req.ctraderAccountId = ACCOUNT_ID
            acc_auth_req.accessToken = ACCESS_TOKEN
            client.send(acc_auth_req)

    # استجابة توثيق الحساب
    elif msg_type == ProtoOAAccountAuthRes().payloadType:
        print(f"🚀 Account {ACCOUNT_ID} Authenticated successfully! Ready for trading operations.")

ctrader_client.setConnectedCallback(on_connected)
ctrader_client.setDisconnectedCallback(on_disconnected)
ctrader_client.setMessageReceivedCallback(on_message_received)


# ==================== إدارة قاعدة البيانات ====================

def get_db_connection():
    if not DATABASE_URL:
        return None
    try:
        conn = psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)
        return conn
    except Exception as e:
        print(f"Database connection error: {e}")
        return None

def init_db():
    conn = get_db_connection()
    if conn:
        try:
            cur = conn.cursor()
            cur.execute("""
                CREATE TABLE IF NOT EXISTS bot_settings (
                    key VARCHAR(50) PRIMARY KEY,
                    value TEXT NOT NULL,
                    updated_at TIMESTAMP DEFAULT NOW()
                );
            """)
            conn.commit()
            cur.close()
            conn.close()
            print("✅ Database tables initialized successfully.")
        except Exception as e:
            print(f"❌ Error initializing database tables: {e}")


# ==================== نماذج البيانات (Pydantic Models) ====================

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


# ==================== أوامر بوت التلغرام ====================

async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("🤖 أهلاً بك! بوت التداول السحابي متصل بنجاح مع cTrader Open API.")

async def cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("🟢 الحالة: السيرفر يعمل والاتصال مع cTrader و DB نشط.")

async def cmd_set_symbols(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("❌ صيغة غير صحيحة.\nاستخدم: `/set_symbols EURUSD,GBPUSD,XAUUSD`", parse_mode="Markdown")
        return

    raw_input = "".join(context.args).replace("\u200b", "").strip()
    symbols_list = [s.strip().upper() for s in raw_input.split(",") if s.strip()]

    if not symbols_list:
        await update.message.reply_text("❌ لم يتم التعرف على أزواج صالحة.")
        return

    conn = get_db_connection()
    if conn:
        try:
            cur = conn.cursor()
            symbols_str = ",".join(symbols_list)
            cur.execute("""
                INSERT INTO bot_settings (key, value, updated_at)
                VALUES ('active_symbols', %s, NOW())
                ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = NOW();
            """, (symbols_str,))
            conn.commit()
            cur.close()
            conn.close()
            
            await update.message.reply_text(f"✅ **تم تحديث الرموز المستهدفة وحفظها بنجاح:**\n`{', '.join(symbols_list)}`", parse_mode="Markdown")
        except Exception as e:
            await update.message.reply_text(f"⚠️ حدث خطأ عند الحفظ في قاعدة البيانات: {e}")


# ==================== إدارة دورة حياة التطبيق (Lifespan) ====================

@asynccontextmanager
async def lifespan(app: FastAPI):
    # 1. تهيئة قاعدة البيانات
    init_db()

    # 2. تشغيل اتصال cTrader Open API
    if CLIENT_ID and CLIENT_SECRET:
        print("🔌 Starting cTrader Open API Client...")
        try:
            ctrader_client.startService()
        except Exception as e:
            print(f"❌ Failed to start cTrader Client: {e}")

    # 3. تشغيل بوت التلغرام
    if TELEGRAM_BOT_TOKEN:
        print("🤖 Initializing Telegram Bot...")
        try:
            telegram_app = ApplicationBuilder().token(TELEGRAM_BOT_TOKEN).build()
            telegram_app.add_handler(CommandHandler("start", cmd_start))
            telegram_app.add_handler(CommandHandler("status", cmd_status))
            telegram_app.add_handler(CommandHandler("set_symbols", cmd_set_symbols))

            await telegram_app.initialize()
            await telegram_app.start()
            asyncio.create_task(telegram_app.updater.start_polling())
            print("🚀 Telegram Bot is polling...")
        except Exception as e:
            print(f"❌ Failed to start Telegram Bot: {e}")

    yield

    # إيقاف الخدمات عند إغلاق السيرفر
    print("🛑 Stopping Services...")
    if CLIENT_ID and CLIENT_SECRET:
        ctrader_client.stopService()


app = FastAPI(title="Cloud Trading AI Backend", lifespan=lifespan)


# ==================== FastAPI Endpoints ====================

@app.get("/")
def read_root():
    return {
        "status": "online",
        "message": "Cloud Trading AI Backend connected to cTrader Open API",
        "active_model": GEMINI_MODEL
    }