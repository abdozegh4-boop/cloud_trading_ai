import os
import json
import asyncio
import threading
import time
import httpx
import psycopg2
from psycopg2.extras import RealDictCursor
from typing import List, Optional
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Request, Response, status
from contextlib import asynccontextmanager
from pydantic import BaseModel
from google import genai
from google.genai import types

# مكتبات التلغرام
from telegram import Update
from telegram.ext import Application, ApplicationBuilder, CommandHandler, ContextTypes

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

# ==================== Self-Ping Task (منع الخمول) ====================

async def keep_alive():
    """وظيفة تُبقي خادم Render مستيقظاً بإرسال طلب كل 8 دقائق"""
    await asyncio.sleep(10)
    print(f"🔄 Starting Self-Ping Task targetting: {WEBHOOK_HOST}")
    
    limits = httpx.Limits(max_keepalive_connections=5, max_connections=10)
    async with httpx.AsyncClient(limits=limits, timeout=5.0) as client_http:
        while True:
            try:
                response = await client_http.get(WEBHOOK_HOST)
                if response.status_code == 200:
                    print(f"🟢 Self-Ping Successful | Status Code: {response.status_code}")
                else:
                    print(f"⚠️ Self-Ping Warning | Status Code: {response.status_code}")
            except Exception as e:
                print(f"⚠️ Self-Ping Failed: {e}")
            
            await asyncio.sleep(480)  # كل 8 دقائق

# ==================== cTrader Open API & Auto-Reconnect ====================

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
    msg_type = message.payloadType
    if msg_type == ProtoOAApplicationAuthRes().payloadType:
        print("✅ Application Authenticated successfully.")
        if ACCOUNT_ID and ACCESS_TOKEN:
            acc_auth_req = ProtoOAAccountAuthReq()
            # ✅ تم تصحيح اسم الخاصية إلى ctidTraderAccountId المعتمدة في Protobuf
            acc_auth_req.ctidTraderAccountId = int(ACCOUNT_ID)
            acc_auth_req.accessToken = str(ACCESS_TOKEN)
            
            client.send(acc_auth_req)
            
    elif msg_type == ProtoOAAccountAuthRes().payloadType:
        print(f"🚀 Account {ACCOUNT_ID} Authenticated successfully!")

ctrader_client.setConnectedCallback(on_connected)
ctrader_client.setDisconnectedCallback(on_disconnected)
ctrader_client.setMessageReceivedCallback(on_message_received)

def start_ctrader_reactor():
    """تشغيل Twisted Reactor وتفعيل خدمة cTrader في Thread مستقل"""
    try:
        print("🔌 Starting cTrader Twisted Reactor...")
        ctrader_client.startService()
        if not reactor.running:
            reactor.run(installSignalHandlers=False)
    except Exception as e:
        print(f"⚠️ cTrader Reactor Error: {e}")

def ctrader_auto_reconnect_loop():
    """مهمة خلفية تفحص حالة الاتصال وتطلب إعادة الاتصال عبر Twisted Event Loop"""
    global stop_ctrader_flag, is_ctrader_connected
    print("🔌 cTrader Monitor Loop started.")
    
    # تشغيل الاتصال والـ Reactor لأول مرة في Thread منفصل
    reactor_thread = threading.Thread(target=start_ctrader_reactor, daemon=True)
    reactor_thread.start()
    
    while not stop_ctrader_flag:
        if not is_ctrader_connected and CLIENT_ID and CLIENT_SECRET:
            print("🔄 Attempting to connect/reconnect to cTrader Open API...")
            try:
                reactor.callFromThread(ctrader_client.startService)
            except Exception as e:
                print(f"⚠️ cTrader Reconnect Error: {e}")
        
        # فحص حالة الاتصال كل 30 ثانية
        for _ in range(30):
            if stop_ctrader_flag:
                break
            time.sleep(1)

# ==================== قاعدة البيانات ====================

def get_db_connection():
    if not DATABASE_URL:
        return None
    try:
        return psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)
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

# ==================== أوامر بوت التلغرام ====================

async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("🤖 أهلاً بك! بوت التداول السحابي يعمل بنجاح عبر Webhook.")

async def cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    ctrader_status = "🟢 متصل" if is_ctrader_connected else "🔴 غير متصل (جاري إعادة الاتصال)"
    status_msg = (
        f"🟢 **حالة السيرفر:** يعمل بنجاح\n"
        f"🔌 **حالة cTrader API:** {ctrader_status}\n"
        f"🔄 **مهمة Self-Ping:** نشطة"
    )
    await update.message.reply_text(status_msg, parse_mode="Markdown")

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
            await update.message.reply_text(f"✅ **تم تحديث الرموز المستهدفة:**\n`{', '.join(symbols_list)}`", parse_mode="Markdown")
        except Exception as e:
            await update.message.reply_text(f"⚠️ حدث خطأ عند الحفظ: {e}")

# ==================== دورة حياة التطبيق (Lifespan) ====================

@asynccontextmanager
async def lifespan(app: FastAPI):
    global telegram_app, stop_ctrader_flag
    
    init_db()

    # 1. تشغيل مهمة منع الخمول
    ping_task = asyncio.create_task(keep_alive())

    # 2. تشغيل cTrader Auto-Reconnect والـ Reactor في Thread مستقل
    stop_ctrader_flag = False
    if CLIENT_ID and CLIENT_SECRET:
        ctrader_thread = threading.Thread(target=ctrader_auto_reconnect_loop, daemon=True)
        ctrader_thread.start()

    # 3. تهيئة بوت التليجرام
    if TELEGRAM_BOT_TOKEN:
        print("🤖 Initializing Telegram Bot for Webhook...")
        try:
            telegram_app = ApplicationBuilder().token(TELEGRAM_BOT_TOKEN).build()
            telegram_app.add_handler(CommandHandler("start", cmd_start))
            telegram_app.add_handler(CommandHandler("status", cmd_status))
            telegram_app.add_handler(CommandHandler("set_symbols", cmd_set_symbols))

            await telegram_app.initialize()
            await telegram_app.start()

            print(f"🔗 Setting Webhook to: {WEBHOOK_URL}")
            await telegram_app.bot.set_webhook(
                url=WEBHOOK_URL,
                drop_pending_updates=True,
                max_connections=40
            )
            print("🚀 Telegram Webhook configured successfully!")

            # إرسال إشعار الإقلاع للمشرف
            if MY_TELEGRAM_CHAT_ID:
                try:
                    await telegram_app.bot.send_message(
                        chat_id=int(MY_TELEGRAM_CHAT_ID),
                        text="🚀 **تم إكتمال الـ Deployment بنجاح!**\nالسيرفر، cTrader API، وبوت التداول جاهزان للعمل الآن.",
                        parse_mode="Markdown"
                    )
                    print("📨 Startup notification sent to Telegram admin!")
                except Exception as e:
                    print(f"⚠️ Failed to send startup notification: {e}")

        except Exception as e:
            print(f"❌ Failed to configure Telegram Webhook: {e}")

    yield

    # عند إغلاق السيرفر
    print("🛑 Stopping Services...")
    stop_ctrader_flag = True
    ping_task.cancel()

    if telegram_app:
        try:
            print("🔗 Removing Telegram Webhook...")
            await telegram_app.bot.delete_webhook()
            await telegram_app.stop()
            await telegram_app.shutdown()
            print("🛑 Telegram Bot stopped cleanly.")
        except Exception as e:
            print(f"⚠️ Error shutting down Telegram Bot: {e}")

    if CLIENT_ID and CLIENT_SECRET:
        try:
            if reactor.running:
                reactor.callFromThread(reactor.stop)
            ctrader_client.stopService()
            print("🛑 cTrader Client stopped.")
        except Exception as e:
            print(f"⚠️ Error stopping cTrader Client: {e}")

app = FastAPI(title="Cloud Trading AI Backend", lifespan=lifespan)

# ==================== Webhook Endpoint ====================

@app.post(WEBHOOK_PATH)
async def telegram_webhook(request: Request):
    """استقبال التحديثات القادمة من تلغرام وتمريرها إلى البوت"""
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

# ==================== FastAPI Endpoints ====================

@app.get("/")
@app.head("/")
def read_root():
    return {
        "status": "online",
        "mode": "webhook",
        "ctrader_connected": is_ctrader_connected,
        "message": "Cloud Trading AI Backend connected to cTrader Open API",
        "active_model": GEMINI_MODEL
    }

@app.get("/api/get-active-symbols")
@app.get("/get-active-symbols")
def get_active_symbols():
    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="Database connection failed")
    try:
        cur = conn.cursor()
        cur.execute("SELECT value FROM bot_settings WHERE key = 'active_symbols';")
        row = cur.fetchone()
        cur.close()
        conn.close()

        if row and row.get("value"):
            symbols_list = [s.strip() for s in row["value"].split(",") if s.strip()]
            return {"status": "success", "symbols": symbols_list, "raw_symbols": row["value"]}
        else:
            return {"status": "default", "symbols": ["EURUSD", "GBPUSD", "XAUUSD"], "raw_symbols": "EURUSD,GBPUSD,XAUUSD"}
    except Exception as e:
        if conn:
            conn.close()
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")

@app.post("/api/calculate-grid-params")
@app.post("/calculate-grid-params")
def calculate_grid_params(data: NewsPayload):
    if not client:
        raise HTTPException(status_code=500, detail="Gemini API Key missing")
    prompt = f"""
    You are an expert Forex Quantitative Trader.
    Analyze market conditions:
    - Headline: {data.headline}
    - Symbol: {data.symbol}
    - ATR: {data.atr}
    - Volume Ratio: {data.volume_ratio}

    Provide recommended Grid spacing in pips and Basket Take-Profit in pips.
    Return ONLY JSON with structure:
    {{"recommended_grid_pips": int, "recommended_basket_tp": int}}
    """
    try:
        response = client.models.generate_content(
            model=GEMINI_MODEL,
            contents=prompt,
            config=types.GenerateContentConfig(response_mime_type="application/json")
        )
        return json.loads(response.text)
    except Exception as e:
        base_grid = int(data.atr * 10000 * 1.5) if data.atr > 0 else 20
        return {
            "recommended_grid_pips": max(base_grid, 10),
            "recommended_basket_tp": 10,
            "error_fallback": str(e)
        }

@app.post("/api/calculate-correlated-grid")
@app.post("/calculate-correlated-grid")
def calculate_correlated_grid(data: BulkMarketRequest):
    if not client:
        raise HTTPException(status_code=500, detail="Gemini API Key missing")
    snapshot_summary = "".join([
        f"- Symbol: {item.symbol} | Price: {item.price} | Change: {item.change_pct}% | ATR: {item.atr_pips} pips\n"
        for item in data.market_snapshot
    ])

    prompt = f"""
    You are an expert AI Risk Manager and Quantitative Grid Trading Strategist.
    Analyze the following multi-asset market snapshot captured at the exact same time:

    {snapshot_summary}

    Global Market Event / News Context: {data.headline}

    STRICT RESPONSE FORMAT:
    Return ONLY a valid JSON object matching this structure:
    {{
      "currency_strength_summary": "Brief analysis",
      "symbols_config": {{
        "EURUSD": {{
          "grid_spacing_pips": 25,
          "basket_tp_pips": 30,
          "risk_mode": "BALANCED",
          "bias": "NEUTRAL"
        }}
      }}
    }}
    """
    try:
        response = client.models.generate_content(
            model=GEMINI_MODEL,
            contents=prompt,
            config=types.GenerateContentConfig(response_mime_type="application/json")
        )
        return {"status": "success", "data": json.loads(response.text)}
    except Exception as e:
        return {"status": "warning", "message": "AI calculation failed", "error": str(e)}