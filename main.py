import os
import json
import psycopg2
from psycopg2.extras import RealDictCursor
from typing import List, Optional
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from google import genai
from google.genai import types

# تحميل المتغيرات البيئية
load_dotenv()

app = FastAPI(title="Cloud Trading AI Backend")

# 🎯 اسم النموذج المعتمد
GEMINI_MODEL = "gemini-2.5-flash"

# إعداد كائن الاتصال بـ Gemini AI
api_key = os.getenv("GEMINI_API_KEY")
client = genai.Client(api_key=api_key)

# رابط قاعدة البيانات Neon PostgreSQL
DATABASE_URL = os.getenv("DATABASE_URL")

def get_db_connection():
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


# ==================== نقاط الاتصال (Endpoints) ====================

@app.get("/")
def read_root():
    return {
        "status": "online", 
        "message": "Cloud Trading AI Backend operational",
        "active_model": GEMINI_MODEL
    }


# 1️⃣ حساب إعدادات الـ Grid لزوج واحد
@app.post("/api/calculate-grid-params")
@app.post("/calculate-grid-params")
def calculate_grid_params(data: NewsPayload):
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
            config=types.GenerateContentConfig(
                response_mime_type="application/json"
            )
        )
        result = json.loads(response.text)
        return result
    except Exception as e:
        base_grid = int(data.atr * 10000 * 1.5) if data.atr > 0 else 20
        return {
            "recommended_grid_pips": max(base_grid, 10),
            "recommended_basket_tp": 10,
            "error_fallback": str(e)
        }


# 2️⃣ حساب ترابط الأزواج المتقاطعة (Bulk Correlated Grid)
@app.post("/api/calculate-correlated-grid")
@app.post("/calculate-correlated-grid")
def calculate_correlated_grid(data: BulkMarketRequest):
    snapshot_summary = ""
    for item in data.market_snapshot:
        snapshot_summary += f"- Symbol: {item.symbol} | Price: {item.price} | Change: {item.change_pct}% | ATR: {item.atr_pips} pips\n"

    prompt = f"""
    You are an expert AI Risk Manager and Quantitative Grid Trading Strategist.
    Analyze the following multi-asset market snapshot captured at the exact same time:

    {snapshot_summary}

    Global Market Event / News Context: {data.headline}

    Your Analysis Tasks:
    1. Determine current Currency/Asset Strength & Weakness across the dataset.
    2. Detect inter-market correlation risks (e.g., strong USD rallying affecting all pairs simultaneously).
    3. For EVERY symbol listed in the snapshot, determine optimal Grid parameters that protect against drawdown:
       - `grid_spacing_pips`: Spacing between grid orders in pips.
       - `basket_tp_pips`: Target cumulative profit for the grid basket in pips.
       - `risk_mode`: Options: "CONSERVATIVE", "BALANCED", or "AGGRESSIVE".
       - `bias`: Market bias for the pair ("BUY", "SELL", or "NEUTRAL").

    STRICT RESPONSE FORMAT:
    Return ONLY a valid JSON object matching this structure:
    {{
      "currency_strength_summary": "Brief analysis of overall market strength/weakness",
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
            config=types.GenerateContentConfig(
                response_mime_type="application/json"
            )
        )
        json_data = json.loads(response.text)
        return {"status": "success", "data": json_data}

    except Exception as e:
        return {
            "status": "warning",
            "message": "AI calculation failed, fallback applied.",
            "error": str(e)
        }


# 3️⃣ تسجيل فتح صفقة جديدة في Neon DB
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


# 4️⃣ تسجيل إغلاق الصفقة في Neon DB
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


# 5️⃣ تحديث الأسعار الحية وقوة العملات من cBot
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
            DO UPDATE SET 
                bid = EXCLUDED.bid, 
                ask = EXCLUDED.ask, 
                change_pct = EXCLUDED.change_pct, 
                updated_at = NOW();
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