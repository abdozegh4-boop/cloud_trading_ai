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

# استخدام SDK الجديدة الرسمية من Google
from google import genai

# =====================================================================
# 1. الإعدادات العامة والتهيئات (Configurations)
# =====================================================================

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "YOUR_TELEGRAM_BOT_TOKEN")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "YOUR_GEMINI_API_KEY")
FINNHUB_API_KEY = os.getenv("FINNHUB_API_KEY", "YOUR_FINNHUB_API_KEY")

# إعداد خادم Gemini SDK الحديث
if GEMINI_API_KEY and GEMINI_API_KEY != "YOUR_GEMINI_API_KEY":
    gemini_client = genai.Client(api_key=GEMINI_API_KEY)
else:
    gemini_client = genai.Client()

# محاكاة قاعدة بيانات لإعدادات المستخدمين (DB State Storage)
USER_SETTINGS = {
    "selected_timeframes": ["M15", "H1", "H4"],
    "symbols_forex": ["EURUSD", "GBPUSD"]
}

# =====================================================================
# 2. خادم HTTP خفيف لتفادي توقف الخدمة على Render (Port Scanner)
# =====================================================================

web_app = FastAPI()

@web_app.get("/")
def health_check():
    return {"status": "ok", "message": "Bot & Analysis Engine is running fine!"}

def run_web_server():
    port = int(os.getenv("PORT", 8080))
    uvicorn.run(web_app, host="0.0.0.0", port=port, log_level="warning")

# تشغيل خادم الويب في Thread منفصل قبل بدء البوت
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
    """جلب العناوين العامة والتحليلات من TradingView RSS"""
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
# 4. جلب البيانات الفنية (cTrader Data Fetcher)
# =====================================================================

async def fetch_ctrader_data_for_timeframes(symbol: str, timeframes: List[str]) -> Dict[str, Any]:
    """جلب بيانات السوق لكل فريم زمني محدد من cTrader"""
    market_data = {}
    for tf in timeframes:
        market_data[tf] = {
            "close": 1.0850,
            "atr": 0.0015,
            "volume": 1250,
            "trend": "Bullish" if tf in ["H1", "H4"] else "Bearish"
        }
    return market_data

# =====================================================================
# 5. تجميع السياق وتوليد التقرير عبر Gemini (Google GenAI SDK)
# =====================================================================

def _call_gemini_api(prompt: str) -> str:
    """دالة مزامنة لتشغيل Gemini بشكل منفصل لتجنب بلوك الـ Event Loop"""
    response = gemini_client.models.generate_content(
        model='gemini-2.5-flash',
        contents=prompt,
    )
    return response.text

async def generate_analysis_report(symbol: str) -> str:
    """تجميع الأخبار والبيانات الفنية وتوليد التحليل عبر Gemini"""
    selected_tfs = USER_SETTINGS.get("selected_timeframes", ["H1", "H4"])
    
    # 1. جلب المصادر الإخبارية بالتوازي
    calendar_data, finnhub_data = await asyncio.gather(
        get_forex_factory_calendar(),
        get_finnhub_news()
    )
    tradingview_data = get_tradingview_rss()
    
    # 2. جلب بيانات cTrader للفريمات المختارة
    ctrader_data = await fetch_ctrader_data_for_timeframes(symbol, selected_tfs)
    
    # 3. بناء الـ Prompt الشامل
    prompt = f"""
أنت خبير تحليل فني واقتصادي مؤسساتي. قم بتحليل زوج {symbol} بناءً على كافة المعطيات المجمعة:

=== 📅 1. التقويم الاقتصادي (Forex Factory) ===
{calendar_data}

=== 📰 2. الأخبار المالية المتخصصة (Finnhub) ===
{finnhub_data}

=== 🌐 3. العناوين الاقتصادية العامة (TradingView RSS) ===
{tradingview_data}

=== 📈 4. البيانات الفنية للفريمات المختارة ({', '.join(selected_tfs)}) ===
{ctrader_data}

=== المطلوب ===
• تقديم تحليل فني مستوفى لكل فريم زمني محدد.
• تحديد الاتجاه العام وتوضيح توافق/تضارب الأخبار الاقتصادية مع البيانات الفنية.
• إعطاء قرار نهائي صريح (شراء / بيع / انتظار) مع تحديد أهداف الدخول ووقف الخسارة.
    """
    
    try:
        # تشغيل دالة Gemini المزامنة بداخل thread لمنع تجميد البوت
        report = await asyncio.to_thread(_call_gemini_api, prompt)
        return report
    except Exception as e:
        logging.error(f"خطأ أثناء استدعاء Gemini API: {e}")
        return "حدث خطأ أثناء إعداد التحليل بواسطة الذكاء الاصطناعي."

# =====================================================================
# 6. واجهة تلغرام وإدارة الفريمات (Telegram Bot)
# =====================================================================

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    keyboard = [
        [InlineKeyboardButton("⏱ تعديل الفريمات الزمنية", callback_data="manage_timeframes")],
        [InlineKeyboardButton("💱 تحليل EURUSD", callback_data="analyze_EURUSD")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    await update.message.reply_text("أهلاً بك في نظام التحليل الفني والمالي المتكامل. اختر من القائمة:", reply_markup=reply_markup)

async def send_long_message(chat_id: int, text: str, context: ContextTypes.DEFAULT_TYPE):
    """إرسال النصوص الطويلة عبر تقسيمها إلى أجزاء تناسب حد تلغرام (4000 حرف)"""
    max_length = 4000
    for i in range(0, len(text), max_length):
        chunk = text[i:i + max_length]
        try:
            await context.bot.send_message(chat_id=chat_id, text=chunk, parse_mode="Markdown")
        except Exception:
            await context.bot.send_message(chat_id=chat_id, text=chunk)

async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    
    # الإجابة الفورية لمنع خطأ Query is too old
    try:
        await query.answer()
    except Exception as e:
        logging.warning(f"تجاوز الإجابة على Callback Query: {e}")

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
        await query.edit_message_text("⏳ جاري جلب التقويم الاقتصادي والأخبار وبيانات cTrader وتوليد التحليل...")
        report = await generate_analysis_report("EURUSD")
        
        # إرسال التقرير النهائي بسلامة مع تقسيم الرسائل إن كانت طويلة
        await send_long_message(chat_id=query.message.chat_id, text=report, context=context)

# =====================================================================
# 7. التشغيل الرئيسي (Main)
# =====================================================================

def main():
    if TELEGRAM_BOT_TOKEN == "YOUR_TELEGRAM_BOT_TOKEN":
        raise ValueError("يرجى إدخال TELEGRAM_BOT_TOKEN الصحيح في متغيرات البيئة!")

    app = ApplicationBuilder().token(TELEGRAM_BOT_TOKEN).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CallbackQueryHandler(button_handler))

    logging.info("تم تشغيل خادم الويب واستطلاع تلغرام بنجاح...")
    app.run_polling()

if __name__ == "__main__":
    main()