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

# تحميل المتغيرات البيئية
load_dotenv()

GEMINI_MODEL = "gemini-2.5-flash"
api_key = os.getenv("GEMINI_API_KEY")
client = genai.Client(api_key=api_key) if api_key else None

DATABASE_URL = os.getenv("DATABASE_URL")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")

def get_db_connection():
    if not DATABASE_URL:
        return None
    try:
        conn = psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)
        return conn
    except Exception as e:
        print(f"Database connection error: {e}")
        return None


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

class TradeOpenRequest(BaseModel):
    position_id: str
    symbol: str
    trade_type: str  # BUY or SELL
    volume: float
    entry_price: float

class TradeCloseRequest(BaseModel):
    position_id: str
    close_price: float
    profit: float

class PriceUpdate(BaseModel):
    symbol: str
    bid: float
    ask: float
    change_pct: float = 0.0


# ==================== أوامر بوت التلغرام (Telegram Handlers) ====================

async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("🤖 أهلاً بك! بوت التداول السحابي متصل ويعمل بنجاح.")

async def cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("🟢 الحالة: السيرفر يعمل بشكل طبيعي والاتصال بنشاط.")

async def cmd_set_symbols(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    معالجة الأمر: /set_symbols EURUSD,GBPUSD,XAUUSD
    """
    if not context.args:
        await update.message.reply_text(
            "❌ صيغة غير صحيحة.\nيرجى كتابة الأزواج بعد الأمر مباشرة كالتالي:\n`/set_symbols EURUSD,GBPUSD,XAUUSD`",
            parse_mode="Markdown"
        )
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
            
            await update.message.reply_text(
                f"✅ **تم تحديث الرموز المستهدفة بنجاح:**\n`{', '.join(symbols_list)}`",
                parse_mode="Markdown"
            )
        except Exception as e:
            await update.message.reply_text(f"⚠️ تم تحديد الرموز محلياً ولكن حدث خطأ عند الحفظ في قاعدة البيانات: {e}")
    else:
        await update.message.reply_text(
            f"✅ تم استقبال الرموز: `{', '.join(symbols_list)}` (لم يتم الحفظ: لا يوجد اتصال بقاعدة البيانات).",
            parse_mode="Markdown"
        )


# ==================== إدارة دورة حياة التطبيق (Lifespan Manager) ====================

@asynccontextmanager
async def lifespan(app: FastAPI):
    # كود الإقلاع (Startup)
    bot_task = None
    if TELEGRAM_BOT_TOKEN:
        print("🤖 Initializing Telegram Bot...")
        try:
            telegram_app = ApplicationBuilder().token(TELEGRAM_BOT_TOKEN).build()
            telegram_app.add_handler(CommandHandler("start", cmd_start))
            telegram_app.add_handler(CommandHandler("status", cmd_status))
            telegram_app.add_handler(CommandHandler("set_symbols", cmd_set_symbols))

            await telegram_app.initialize()
            await telegram_app.start()
            
            # تشغيل الـ Polling بشكل منفصل في الخلفية لمنع تعليق السيرفر
            bot_task = asyncio.create_task(telegram_app.updater.start_polling())
            print("🚀 Telegram Bot is polling...")
        except Exception as e:
            print(f"❌ Failed to start Telegram Bot: {e}")
    else:
        print("⚠️ TELEGRAM_BOT_TOKEN missing in environment variables.")

    yield  # السيرفر يعمل هنا ويستقبل الطلبات

    # كود الإيقاف (Shutdown)
    if TELEGRAM_BOT_TOKEN and 'telegram_app' in locals():
        print("🛑 Stopping Telegram Bot...")
        await telegram_app.updater.stop()
        await telegram_app.stop()
        await telegram_app.shutdown()


app = FastAPI(title="Cloud Trading AI Backend", lifespan=lifespan)


# ==================== FastAPI Endpoints ====================

@app.get("/")
def read_root():
    return {
        "status": "online", 
        "message": "Cloud Trading AI Backend operational",
        "active_model": GEMINI_MODEL
    }

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

@app.post("/api/trades/open")
@app.post("/trades/open")
def record_open_trade(trade: TradeOpenRequest):
    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="Database connection failed")
    try:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO trades (position_id, symbol, trade_type, volume, entry_price, status, created_at)
            VALUES (%s, %s, %s, %s, %s, 'OPEN', NOW());
            """,
            (trade.position_id, trade.symbol, trade.trade_type, trade.volume, trade.entry_price)
        )
        conn.commit()
        cur.close()
        return {"status": "success", "message": "Trade logged successfully"}
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()

@app.post("/api/trades/close")
@app.post("/trades/close")
def record_close_trade(trade: TradeCloseRequest):
    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="Database connection failed")
    try:
        cur = conn.cursor()
        cur.execute(
            """
            UPDATE trades 
            SET status = 'CLOSED', close_price = %s, profit = %s, closed_at = NOW()
            WHERE position_id = %s;
            """,
            (trade.close_price, trade.profit, trade.position_id)
        )
        conn.commit()
        cur.close()
        return {"status": "success", "message": "Trade close logged successfully"}
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()

@app.post("/api/update-price")
@app.post("/update-price")
def update_price(data: PriceUpdate):
    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="Database connection failed")
    try:
        cur = conn.cursor()
        query = """
            INSERT INTO symbol_prices (symbol, bid, ask, change_pct, updated_at)
            VALUES (%s, %s, %s, %s, NOW())
            ON CONFLICT (symbol) 
            DO UPDATE SET bid = EXCLUDED.bid, ask = EXCLUDED.ask, change_pct = EXCLUDED.change_pct, updated_at = NOW();
        """
        cur.execute(query, (data.symbol, data.bid, data.ask, data.change_pct))
        conn.commit()
        cur.close()
        return {"status": "success", "message": f"Price updated for {data.symbol}"}
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()