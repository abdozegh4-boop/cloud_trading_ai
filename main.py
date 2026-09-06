import os
import json
from dotenv import load_dotenv
from fastapi import FastAPI
from pydantic import BaseModel
from google import genai

load_dotenv()

app = FastAPI()

api_key = os.getenv("GEMINI_API_KEY")
client = genai.Client(api_key=api_key)

class NewsPayload(BaseModel):
    headline: str
    symbol: str
    atr: float
    volume_ratio: float

@app.get("/")
def read_root():
    return {"status": "online", "message": "Cloud Trading AI Backend operational"}

@app.post("/api/calculate-grid-params")
def calculate_grid_params(data: NewsPayload):
    prompt = f"""
    You are an expert Forex Quantitative Trader.
    Analyze market conditions:
    - Headline: {data.headline}
    - Symbol: {data.symbol}
    - ATR: {data.atr}
    - Volume Ratio: {data.volume_ratio}

    Provide recommended Grid spacing in pips and Basket Take-Profit in pips.
    Return ONLY raw JSON with format:
    {{"recommended_grid_pips": int, "recommended_basket_tp": int}}
    """
    try:
        response = client.models.generate_content(
            model='gemini-1.5-flash',
            contents=prompt,
        )
        text = response.text.strip().replace("```json", "").replace("```", "")
        result = json.loads(text)
        return result
    except Exception as e:
        base_grid = int(data.atr * 10000 * 1.5) if data.atr > 0 else 20
        return {
            "recommended_grid_pips": max(base_grid, 10),
            "recommended_basket_tp": 10,
            "error_fallback": str(e)
        }