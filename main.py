import os
import json
import asyncio
import threading
import time
import httpx
from psycopg2 import pool
from psycopg2.extras import RealDictCursor
from typing import List, Optional, Dict, Any, Set
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
GEMINI_MODEL = "gemini-3.6-flash"  # غيّره إلى النموذج المتاح لديك
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

# ==================== قائمة الأزواج المتاحة ====================
ALL_AVAILABLE_SYMBOLS = {
    "forex": [
        "EURUSD", "GBPUSD", "USDJPY", "USDCHF", "AUDUSD", "USDCAD", "NZDUSD",
        "EURGBP", "EURJPY", "GBPJPY", "AUDJPY", "EURAUD", "EURCAD", "GBPAUD"
    ],
    "commodities": [
        "XAUUSD", "XAGUSD", "USOIL", "UKOIL", "NGAS", "COPPER"
    ],
    "indices": [
        "US30", "NAS100", "SPX500", "GER40", "UK100", "JPN225", "FRA40"
    ],
    "crypto": [
        "BTCUSD", "ETHUSD", "SOLUSD", "XRPUSD", "ADAUSD", "DOGEUSD", "AAPL", "NVDA", "TSLA"
    ]
}

# ذاكرة مؤقتة للتحديدات
user_selections: Dict[int, Dict[str, Set[str]]] = {}

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
            
            default_settings = {
                "symbols_forex": "EURUSD,GBPUSD,USDJPY,AUDUSD,USDCAD",
                "symbols_commodities": "XAUUSD,XAGUSD,USOIL,UKOIL",
                "symbols_indices": "US30,NAS100,SPX500,GER40",
                "symbols_crypto": "BTCUSD,ETHUSD,AAPL,NVDA,TSLA"
            }
            
            for key, val in default_settings.items():
                cur.execute("""
                    INSERT INTO bot_settings (key, value)
                    VALUES (%s, %s)
                    ON CONFLICT (key) DO NOTHING;
                """, (key, val))
                
            conn.commit()
    except Exception as e:
        print(f"❌ Error initializing database tables: {e}")
    finally:
        if conn:
            db_pool.putconn(conn)

def get_broker_symbols_by_category(category_key: str, default_list: List[str]) -> List[str]:
    if not db_pool:
        return default_list
    
    conn = None
    try:
        conn = db_pool.getconn()
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT value FROM bot_settings WHERE key = %s;", (f"symbols_{category_key}",))
            row = cur.fetchone()
            if row and row.get("value"):
                symbols = [s.strip() for s in row["value"].split(",") if s.strip()]
                return symbols if symbols else default_list
    except Exception as e:
        print(f"⚠️ DB Read Error for {category_key}: {e}")
    finally:
        if conn:
            db_pool.putconn(conn)
            
    return default_list

def save_broker_symbols_by_category(category_key: str, symbols: List[str]) -> bool:
    if not db_pool:
        return False
    
    conn = None
    try:
        conn = db_pool.getconn()
        with conn.cursor() as cur:
            symbols_str = ",".join(symbols)
            cur.execute("""
                INSERT INTO bot_settings (key, value, updated_at)
                VALUES (%s, %s, NOW())
                ON CONFLICT (key) DO UPDATE
                SET value = EXCLUDED.value, updated_at = NOW();
            """, (f"symbols_{category_key}", symbols_str))
            conn.commit()
            return True
    except Exception as e:
        print(f"❌ DB Write Error for {category_key}: {e}")
        return False
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

# ==================== لوحات التحكم والأزرار ====================

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

def build_symbols_checkbox_keyboard(category: str, selected_symbols: set):
    all_symbols = ALL_AVAILABLE_SYMBOLS.get(category, [])
    keyboard = []
    row = []
    
    for symbol in all_symbols:
        is_checked = symbol in selected_symbols
        icon = "☑️" if is_checked else "🔲"
        btn_text = f"{icon} {symbol}"
        # استخدام | لتجنب مشاكل الـ split
        callback_data = f"toggle|{category}|{symbol}"
        
        row.append(InlineKeyboardButton(btn_text, callback_data=callback_data))
        if len(row) == 2:
            keyboard.append(row)
            row = []
    if row:
        keyboard.append(row)

    keyboard.append([
        InlineKeyboardButton("💾 حفظ التغييرات", callback_data=f"save|{category}"),
        InlineKeyboardButton("❌ إلغاء", callback_data="manage_categories")
    ])
    
    return InlineKeyboardMarkup(keyboard)

# ==================== توليد التوصيات عبر AI ====================

async def generate_market_signals(category: str, symbols: List[str]) -> str:
    if not client:
        return "❌ **خطأ:** مفتاح Google Gemini API غير معرف."

    symbols_str = ", ".join(symbols)
    
    prompt = f"""
    You are an elite Institutional Quantitative Analyst and Forex Trader.
    Generate actionable market signals/recommendations for the following assets available at the broker:
    Category: {category}
    Broker Available Assets: {symbols_str}

    For each asset (or the top 2-3 most volatile assets in this list), provide a concise trading signal in Arabic with this exact format:

    🎯 **توصية تحليليّة - [{category}]**

    🔹 **الزوج/الأصل:** [Symbol]
    • **الاتجاه:** 🟢 شراء (BUY) أو 🔴 بيع (SELL)
    • **نقطة الدخول:** [Current/Ideal Entry Price]
    • **هدف الربح (TP):** [Take Profit Price]
    • **وقف الخسارة (SL):** [Stop Loss Price]
    • **نسبة المخاطرة:** Low / Medium
    • **التحليل الفني السريع:** [1 sentence technical reasoning based on market structure and momentum]

    Keep the response concise, strictly structured, and fully formatted with clean Telegram Markdown.
    """

    try:
        response = client.models.generate_content(
            model=GEMINI_MODEL,
            contents=prompt
        )
        return response.text
    except Exception as e:
        return f"❌ **حدث خطأ أثناء توليد التحليل:**\n`{str(e)}`"

# ==================== معالجة أزرار التلغرام ====================

async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_authorized(update):
        await update.message.reply_text("⛔ غير مصرح لك باستخدام هذا البوت.")
        return
    await update.message.reply_text(
        "🤖 **مرحباً بك في لوحة التداول وتوصيات الذكاء الاصطناعي!**\n\n"
        "اختر خياراً من القائمة أدناه:",
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

    # ---------- القائمة الرئيسية ----------
    if data == "back_main":
        await query.edit_message_text(
            "🤖 **مرحباً بك في لوحة التداول وتوصيات الذكاء الاصطناعي!**\n\n"
            "اختر خياراً من القائمة أدناه:",
            reply_markup=main_keyboard(),
            parse_mode="Markdown"
        )

    # ---------- إدارة الفئات ----------
    elif data == "manage_categories":
        await query.edit_message_text(
            "⚙️ **تعديل قائمة الأزواج المتاحة للبروكر**\n\n"
            "الرجاء اختيار قائمة الأزواج التي تريد التعديل عليها:",
            reply_markup=category_selection_keyboard(),
            parse_mode="Markdown"
        )

    # ---------- فتح فئة للتعديل ----------
    elif data.startswith("editcat_"):
        category = data.split("_", 1)[1]
        current_db_symbols = set(
            get_broker_symbols_by_category(category, ALL_AVAILABLE_SYMBOLS.get(category, []))
        )

        if user_id not in user_selections:
            user_selections[user_id] = {}
        user_selections[user_id][category] = current_db_symbols.copy()

        kb = build_symbols_checkbox_keyboard(category, current_db_symbols)
        await query.edit_message_text(
            f"📋 **تحديد أزواج فئة [{category.upper()}]**\n\n"
            f"انقر على الأزواج للتحديد (☑️) أو الإلغاء (🔲)، ثم اضغط **حفظ التغييرات**:",
            reply_markup=kb,
            parse_mode="Markdown"
        )

    # ---------- تبديل زوج (Toggle) ----------
    elif data.startswith("toggle|"):
        parts = data.split("|")
        if len(parts) != 3:
            return
        _, category, symbol = parts

        # حماية قوية ضد فقدان الجلسة
        if user_id not in user_selections:
            user_selections[user_id] = {}
        if category not in user_selections[user_id]:
            user_selections[user_id][category] = set(
                get_broker_symbols_by_category(category, ALL_AVAILABLE_SYMBOLS.get(category, []))
            )

        selected_set = user_selections[user_id][category]
        if symbol in selected_set:
            selected_set.remove(symbol)
        else:
            selected_set.add(symbol)

        kb = build_symbols_checkbox_keyboard(category, selected_set)
        await query.edit_message_reply_markup(reply_markup=kb)

    # ---------- حفظ التغييرات (مع حماية) ----------
    elif data.startswith("save|"):
        category = data.split("|", 1)[1]

        # إذا ضاعت الجلسة نرجع لما في قاعدة البيانات بدلاً من تفريغها
        selected_set = user_selections.get(user_id, {}).get(category)
        if selected_set is None:
            selected_set = set(
                get_broker_symbols_by_category(category, ALL_AVAILABLE_SYMBOLS.get(category, []))
            )

        symbols_list = sorted(list(selected_set))
        success = save_broker_symbols_by_category(category, symbols_list)

        if success:
            # نحدث الذاكرة أيضاً
            if user_id not in user_selections:
                user_selections[user_id] = {}
            user_selections[user_id][category] = set(symbols_list)

            await query.edit_message_text(
                f"✅ **تم تحديث قاعدة البيانات بنجاح!**\n\n"
                f"الأزواج المعتمدة حالياً لفئة **[{category.upper()}]** هي:\n"
                f"`{', '.join(symbols_list) if symbols_list else 'لا يوجد أزواج محددة'}`",
                reply_markup=main_keyboard(),
                parse_mode="Markdown"
            )
        else:
            await query.edit_message_text(
                "❌ **حدث خطأ أثناء حفظ التغييرات في قاعدة البيانات.**",
                reply_markup=main_keyboard(),
                parse_mode="Markdown"
            )

    # ---------- إشارات التحليل ----------
    elif data == "sig_forex":
        symbols = get_broker_symbols_by_category("forex", ["EURUSD", "GBPUSD", "USDJPY"])
        if not symbols:
            await query.message.reply_text(
                "⚠️ لا توجد أزواج محددة لفئة الفوركس. قم بتعديل القائمة أولاً.",
                reply_markup=main_keyboard()
            )
            return
        await query.message.reply_text(
            f"⏳ **جاري تحليل أزواج الفوركس المتاحة (`{', '.join(symbols)}`)...**",
            parse_mode="Markdown"
        )
        signals = await generate_market_signals("أزواج الفوركس", symbols)
        await query.message.reply_text(signals, reply_markup=main_keyboard(), parse_mode="Markdown")

    elif data == "sig_commodities":
        symbols = get_broker_symbols_by_category("commodities", ["XAUUSD", "XAGUSD", "USOIL"])
        if not symbols:
            await query.message.reply_text(
                "⚠️ لا توجد أزواج محددة لفئة المعادن والطاقة. قم بتعديل القائمة أولاً.",
                reply_markup=main_keyboard()
            )
            return
        await query.message.reply_text(
            f"⏳ **جاري تحليل المعادن والطاقة (`{', '.join(symbols)}`)...**",
            parse_mode="Markdown"
        )
        signals = await generate_market_signals("المعادن والطاقة", symbols)
        await query.message.reply_text(signals, reply_markup=main_keyboard(), parse_mode="Markdown")

    elif data == "sig_indices":
        symbols = get_broker_symbols_by_category("indices", ["US30", "NAS100", "SPX500"])
        if not symbols:
            await query.message.reply_text(
                "⚠️ لا توجد أزواج محددة لفئة المؤشرات. قم بتعديل القائمة أولاً.",
                reply_markup=main_keyboard()
            )
            return
        await query.message.reply_text(
            f"⏳ **جاري تحليل المؤشرات العالمية (`{', '.join(symbols)}`)...**",
            parse_mode="Markdown"
        )
        signals = await generate_market_signals("المؤشرات العالمية", symbols)
        await query.message.reply_text(signals, reply_markup=main_keyboard(), parse_mode="Markdown")

    elif data == "sig_crypto":
        symbols = get_broker_symbols_by_category("crypto", ["BTCUSD", "ETHUSD", "AAPL"])
        if not symbols:
            await query.message.reply_text(
                "⚠️ لا توجد أزواج محددة لفئة الأسهم والعملات الرقمية. قم بتعديل القائمة أولاً.",
                reply_markup=main_keyboard()
            )
            return
        await query.message.reply_text(
            f"⏳ **جاري تحليل الأسهم والعملات الرقمية (`{', '.join(symbols)}`)...**",
            parse_mode="Markdown"
        )
        signals = await generate_market_signals("الأسهم والعملات الرقمية", symbols)
        await query.message.reply_text(signals, reply_markup=main_keyboard(), parse_mode="Markdown")

    # ---------- حالة النظام ----------
    elif data == "btn_status":
        ctrader_status = "🟢 متصل" if is_ctrader_connected else "🔴 غير متصل"
        status_msg = (
            f"🖥 **حالة الخادم:** 🟢 يعمل بنجاح\n"
            f"🔌 **شبكة cTrader API:** {ctrader_status}\n"
            f"🧠 **نموذج الذكاء الاصطناعي:** `{GEMINI_MODEL}`"
        )
        await query.message.reply_text(status_msg, reply_markup=main_keyboard(), parse_mode="Markdown")

    # ---------- معلومات الحساب ----------
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

    # ---------- الصفقات المفتوحة ----------
    elif data == "btn_positions":
        request_account_details()
        if not active_positions:
            await query.message.reply_text(
                "📭 **لا توجد صفقات مفتوحة حالياً.**",
                reply_markup=main_keyboard(),
                parse_mode="Markdown"
            )
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

    # ---------- تحديث البيانات ----------
    elif data == "btn_refresh":
        request_account_details()
        await query.message.reply_text(
            "🔄 **تم تحديث البيانات.**",
            reply_markup=main_keyboard(),
            parse_mode="Markdown"
        )

# ==================== Pydantic Model ====================

class UpdateSymbolsPayload(BaseModel):
    category: str
    symbols: List[str]

# ==================== دورة حياة التطبيق ====================

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
                        text=f"🚀 **تم تشغيل النظام ولوحة التحكم التفاعلية بنجاح!**\n🤖 النموذج النشط: `{GEMINI_MODEL}`",
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

@app.post("/api/update-symbols")
def update_symbols(data: UpdateSymbolsPayload):
    if not db_pool:
        raise HTTPException(status_code=500, detail="Database connection pool unavailable")
    
    success = save_broker_symbols_by_category(data.category.lower().strip(), data.symbols)
    if success:
        return {"status": "success", "category": data.category, "updated_symbols": data.symbols}
    else:
        raise HTTPException(status_code=500, detail="Failed to save symbols to database")

# ==================== إعدادات التشغيل لـ Render ====================
import uvicorn

if __name__ == "__main__":
    port = int(os.getenv("PORT", 8000))
    uvicorn.run("main:app", host="0.0.0.0", port=port)