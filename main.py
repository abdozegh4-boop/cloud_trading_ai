import os
import asyncio
import logging
import threading
import httpx
import feedparser
from typing import List, Dict, Any

from fastapi import FastAPI
import uvicorn

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    CallbackQueryHandler,
    ContextTypes
)

# استخدام المكتبة الرسمية الجديدة لـ Gemini
from google import genai

# استخدام المكتبة الرسمية لـ cTrader Open API
from ctrader_open_api import Client, Protobuf, TcpProtocol, EndPoints

# =====================================================================
# 1. الإعدادات العامة والتهيئات (Configurations)
# =====================================================================

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)

# مفاتيح البيئة (Environment Variables)
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "YOUR_TELEGRAM_BOT_TOKEN")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "YOUR_GEMINI_API_KEY")
FINNHUB_API_KEY = os.getenv("FINNHUB_API_KEY", "YOUR_FINNHUB_API_KEY")

# بيانات cTrader Open API
CTRADER_CLIENT_ID = os.getenv("CTRADER_CLIENT_ID", "YOUR_CTRADER_CLIENT_ID")
CTRADER_CLIENT_SECRET = os.getenv("CTRADER_CLIENT_SECRET", "YOUR_CTRADER_CLIENT_SECRET")
CTRADER_ACCESS_TOKEN = os.getenv("CTRADER_ACCESS_TOKEN", "YOUR_CTRADER_ACCESS_TOKEN")
CTRADER_ACCOUNT_ID = int(os.getenv("CTRADER_ACCOUNT_ID", "12345678"))

# تهيئة عميل Gemini SDK
if GEMINI_API_KEY and GEMINI_API_KEY != "YOUR_GEMINI_API_KEY":
    gemini_client = genai.Client(api_key=GEMINI_API_KEY)
else:
    gemini_client = genai.Client()

# حالة إعدادات المستخدمين (في الذاكرة)
USER_SETTINGS = {
    "selected_timeframes": ["M15", "H1", "H4"],
    "symbols_forex": ["EURUSD", "GBPUSD"]
}

TIMEFRAME_MAP = {
    "M15": "M15",
    "M30": "M30",
    "H1": "H1",
    "H4": "H4",
    "D1": "D1"
}

# =====================================================================
# 2. خادم HTTP لتفادي توقف الخدمة على Render
# =====================================================================

fastapi_app = FastAPI()

@fastapi_app.get("/")
def health_check():
    return {"status": "ok", "message": "cTrader & Gemini Analysis Engine is running fine!"}

def run_web_server():
    port = int(os.getenv("PORT", 8080))
    uvicorn.run(fastapi_app, host="0.0.0.0", port=port, log_level="warning")

threading.Thread(target=run_web_server, daemon=True).start()

# =====================================================================
# 3. محرك الأخبار والتقويم الاقتصادي (News Engine)
# =====================================================================

async def get_forex_factory_calendar() -> str:
    """جلب التقويم الاقتصادي والأحداث عالية التأثير من Forex Factory"""
    url = "https://nodedata.forexfactory.com/ff_calendar_thisweek.json"
    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            res = await client.get(url)
            if res.status_code == 200:
                events = res.json()
                high_impact = [
                    f"• {e.get('title')} ({e.get('country')}) | Date: {e.get('date')}"
                    for e in events if e.get("impact") == "High"
                ]
                if high_impact:
                    return "\n".join(high_impact[:5])
    except Exception as e:
        logging.error(f"⚠️ Forex Factory Error: {e}")
    return "لا توجد أحداث عالية التأثير قريبة."

async def get_finnhub_news() -> str:
    """جلب الأخبار المالية المتخصصة من Finnhub"""
    if not FINNHUB_API_KEY or FINNHUB_API_KEY == "YOUR_FINNHUB_API_KEY":
        return "Finnhub API Key غير مفعّل."
    
    url = f"https://finnhub.io/api/v1/news?category=forex&token={FINNHUB_API_KEY}"
    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            res = await client.get(url)
            if res.status_code == 200:
                news = res.json()
                items = [f"• {item.get('headline')}" for item in news[:4]]
                if items:
                    return "\n".join(items)
    except Exception as e:
        logging.error(f"⚠️ Finnhub Error: {e}")
    return "تعذر جلب أخبار Finnhub."

def get_tradingview_rss() -> str:
    """جلب العناوين العامة من TradingView RSS"""
    try:
        url = "https://www.tradingview.com/feed/"
        feed = feedparser.parse(url)
        headlines = [f"• {entry.title}" for entry in feed.entries[:5]]
        if headlines:
            return "\n".join(headlines)
    except Exception as e:
        logging.error(f"⚠️ TradingView RSS Error: {e}")
    return "تعذر جلب تغذية TradingView RSS."

# =====================================================================
# 4. محرك cTrader Open API الحقيقي (Real Market Data & Symbol ID)
# =====================================================================

async def get_symbol_id_by_name(client: Client, account_id: int, symbol_name: str) -> int:
    """جلب Symbol ID الخاص بالشركة ديناميكياً"""
    symbols_req = Protobuf.ProtoOASymbolsListReq()
    symbols_req.ctidTraderAccountId = account_id
    
    response = await client.send(symbols_req)
    
    if hasattr(response, 'symbol'):
        for sym in response.symbol:
            if symbol_name.upper() in sym.symbolName.upper():
                logging.info(f"✅ Symbol ID Found: {sym.symbolName} -> {sym.symbolId}")
                return sym.symbolId
    return None

async def fetch_ctrader_data_for_timeframes(symbol: str, timeframes: List[str]) -> Dict[str, Any]:
    """الاتصال بسيرفر cTrader وجلب الشموع والبيانات الفنية الفعلية"""
    market_data = {}
    
    # اختيار السيرفر Live أو Demo
    host = EndPoints.PROTOBUF_HOST_LIVE
    port = EndPoints.PROTOBUF_PORT
    
    client = Client(host, port, TcpProtocol)
    
    try:
        await client.startService()
        
        # 1. App Authentication
        auth_app = Protobuf.ProtoOAApplicationAuthReq()
        auth_app.clientId = CTRADER_CLIENT_ID
        auth_app.clientSecret = CTRADER_CLIENT_SECRET
        await client.send(auth_app)
        
        # 2. Account Authentication
        auth_acc = Protobuf.ProtoOAAccountAuthReq()
        auth_acc.ctidTraderAccountId = CTRADER_ACCOUNT_ID
        auth_acc.accessToken = CTRADER_ACCESS_TOKEN
        await client.send(auth_acc)
        
        # 3. Dynamic Symbol ID Lookup
        symbol_id = await get_symbol_id_by_name(client, CTRADER_ACCOUNT_ID, symbol)
        if not symbol_id:
            return {"error": f"Symbol {symbol} not found on broker."}
            
        # 4. Fetch Trendbars for each timeframe
        for tf in timeframes:
            if tf not in TIMEFRAME_MAP:
                continue
                
            trendbar_req = Protobuf.ProtoOAGetTrendbarsReq()
            trendbar_req.ctidTraderAccountId = CTRADER_ACCOUNT_ID
            trendbar_req.symbolId = symbol_id
            trendbar_req.period = TIMEFRAME_MAP[tf]
            trendbar_req.count = 14
            
            response = await client.send(trendbar_req)
            
            if hasattr(response, 'trendbar') and len(response.trendbar) > 0:
                bars = response.trendbar
                last_bar = bars[-1]
                
                close_price = (last_bar.low + last_bar.deltaClose) / 100000.0
                high_price = (last_bar.low + last_bar.deltaHigh) / 100000.0
                low_price = last_bar.low / 100000.0
                
                prev_close = (bars[-2].low + bars[-2].deltaClose) / 100000.0 if len(bars) > 1 else close_price
                trend = "Bullish" if close_price >= prev_close else "Bearish"
                atr = round(abs(high_price - low_price), 5)
                
                market_data[tf] = {
                    "close": round(close_price, 5),
                    "atr": atr,
                    "volume": last_bar.volume,
                    "trend": trend
                }
            else:
                market_data[tf] = {"status": "No data"}
                
    except Exception as e:
        logging.error(f"⚠️ cTrader Connection Error: {e}")
        for tf in timeframes:
            market_data[tf] = {"error": "cTrader API connection issue"}
    finally:
        await client.stopService()
        
    return market_data

# =====================================================================
# 5. تجميع المعطيات وتوليد التقرير عبر Gemini 2.0 Flash
# =====================================================================

async def generate_analysis_report(symbol: str) -> str:
    """تجميع البيانات الاقتصادية والفنية وإرسالها لـ Gemini"""
    selected_tfs = USER_SETTINGS.get("selected_timeframes", ["H1", "H4"])
    
    calendar_data, finnhub_data = await asyncio.gather(
        get_forex_factory_calendar(),
        get_finnhub_news()
    )
    tradingview_data = get_tradingview_rss()
    ctrader_data = await fetch_ctrader_data_for_timeframes(symbol, selected_tfs)
    
    prompt = f"""
أنت خبير تحليل فني واقتصادي مؤسساتي. قم بتحليل زوج {symbol} بناءً على كافة المعطيات المجمعة:

=== 📅 1. التقويم الاقتصادي (Forex Factory) ===
{calendar_data}

=== 📰 2. الأخبار المالية المتخصصة (Finnhub) ===
{finnhub_data}

=== 🌐 3. العناوين الاقتصادية العامة (TradingView RSS) ===
{tradingview_data}

=== 📈 4. البيانات الفنية الحقيقية من cTrader ({', '.join(selected_tfs)}) ===
{ctrader_data}

=== المطلوب ===
• تقديم تحليل فني مستوفى لكل فريم زمني محدد.
• تحديد الاتجاه العام وتوضيح توافق/تضارب الأخبار الاقتصادية مع البيانات الفنية.
• إعطاء قرار نهائي صريح (شراء / بيع / انتظار) مع تحديد أهداف الدخول ووقف الخسارة.
    """
    
    try:
        response = gemini_client.models.generate_content(
            model='gemini-2.0-flash',
            contents=prompt,
        )
        return response.text
    except Exception as e:
        logging.error(f"خطأ أثناء استدعاء Gemini API: {e}")
        return "حدث خطأ أثناء إعداد التحليل بواسطة الذكاء الاصطناعي."

# =====================================================================
# 6. واجهة تلغرام وبوت الإشارات (Telegram Bot Handlers)
# =====================================================================

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    keyboard = [
        [InlineKeyboardButton("⏱ تعديل الفريمات الزمنية", callback_data="manage_timeframes")],
        [InlineKeyboardButton("💱 تحليل EURUSD المباشر", callback_data="analyze_EURUSD")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    await update.message.reply_text("أهلاً بك في نظام التحليل الفني والمالي المتكامل المرتبط بـ cTrader. اختر من القائمة:", reply_markup=reply_markup)

async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    if query.data == "manage_timeframes":
        all_tfs = ["M15", "M30", "H1", "H4", "D1"]
        current = USER_SETTINGS["selected_timeframes"]
        
        keyboard = []
        for tf in all_tfs:
            status = "☑️" if tf in current else "🔲"
            keyboard.append([InlineKeyboardButton(f"{status} {tf}", callback_data=f"toggle_{tf}")])
            
        keyboard.append([InlineKeyboardButton("💾 حفظ الفريمات", callback_data="save_tfs")])
        reply_markup = InlineKeyboardMarkup(keyboard)
        await query.edit_message_text("اختر الفريمات الزمنية المعتمدة للتحليل:", reply_markup=reply_markup)

    elif query.data.startswith("toggle_"):
        tf_to_toggle = query.data.split("_")[1]
        current = USER_SETTINGS["selected_timeframes"]
        
        if tf_to_toggle in current:
            current.remove(tf_to_toggle)
        else:
            current.append(tf_to_toggle)
            
        all_tfs = ["M15", "M30", "H1", "H4", "D1"]
        keyboard = []
        for tf in all_tfs:
            status = "☑️" if tf in current else "🔲"
            keyboard.append([InlineKeyboardButton(f"{status} {tf}", callback_data=f"toggle_{tf}")])
            
        keyboard.append([InlineKeyboardButton("💾 حفظ الفريمات", callback_data="save_tfs")])
        reply_markup = InlineKeyboardMarkup(keyboard)
        await query.edit_message_text("اختر الفريمات الزمنية المعتمدة للتحليل:", reply_markup=reply_markup)

    elif query.data == "save_tfs":
        tfs_str = ", ".join(USER_SETTINGS["selected_timeframes"])
        await query.edit_message_text(f"تم حفظ الفريمات بنجاح! الفريمات الحالية: {tfs_str}")

    elif query.data == "analyze_EURUSD":
        await query.edit_message_text("⏳ جاري الاتصال بـ cTrader وجلب الأخبار وتوليد التقرير بواسطة Gemini...")
        report = await generate_analysis_report("EURUSD")
        
        try:
            await query.message.reply_text(report, parse_mode="Markdown")
        except Exception:
            await query.message.reply_text(report)

# =====================================================================
# 7. التشغيل الرئيسي (Main Entry Point)
# =====================================================================

async def post_init(application):
    """حذف الـ Webhook تلقائياً للتأكد من عمل Polling بشكل صحيح بدون تعارضات"""
    await application.bot.delete_webhook(drop_pending_updates=True)
    logging.info("تم مسح الـ Webhook القديم بنجاح بنسبة 100%.")

def main():
    if TELEGRAM_BOT_TOKEN == "8624993506:AAEZO-faBjZ7fbFx36zrry7pwBu6UDPJAbc":
        raise ValueError("يرجى إدخال TELEGRAM_BOT_TOKEN في متغيرات البيئة!")

    telegram_app = (
        ApplicationBuilder()
        .token(TELEGRAM_BOT_TOKEN)
        .post_init(post_init)
        .build()
    )

    telegram_app.add_handler(CommandHandler("start", start))
    telegram_app.add_handler(CallbackQueryHandler(button_handler))

    logging.info("تم تشغيل الخدمة بنجاح واستقبال الطلبات...")
    telegram_app.run_polling()

if __name__ == "__main__":
    main()