"""
db.py
=====
طبقة الاتصال بقاعدة بيانات Neon (PostgreSQL) لتخزين:
  - لقطات المؤشرات الفنية (RSI/EMA/ATR/السعر...) بشكل دوري لكل زوج/إطار زمني.
  - تقارير تحليل الذكاء الاصطناعي (اقتصادي/إخباري/فني/توصية شاملة) مع تاريخها الزمني.
  - قائمة "المتابعات النشطة" (Active Watches): أي تركيبة (نوع تحليل + أزواج + أطر زمنية)
    طلبها أي مستخدم، ليتم تحديثها تلقائياً بشكل دوري دون أن يطلبها أحد مجدداً.

كل الدوال هنا "آمنة عند غياب قاعدة البيانات": إن لم يتم تعريف DATABASE_URL،
فإن pool تبقى None وتتصرف كل الدوال بصمت (fallback) دون كسر عمل البوت،
أي أن البوت سيستمر بالعمل تماماً كالسابق (تحليل مباشر بدون تخزين أو ذاكرة).
"""

import os
import json
import logging
import ssl
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse, urlunparse, parse_qs, urlencode

import asyncpg

logger = logging.getLogger("TradingBot.DB")

# رابط الاتصال بقاعدة بيانات Neon، يُنسخ من console.neon.tech -> Connection Details
# مثال: postgresql://user:password@ep-xxxx.aws.neon.tech/dbname?sslmode=require
DATABASE_URL = os.getenv("DATABASE_URL", "").strip()

# كل كم دقيقة يُعاد توليد التحاليل النشطة تلقائياً (قابل للتغيير من متغيرات البيئة)
AUTO_ANALYSIS_INTERVAL_MINUTES = int(os.getenv("AUTO_ANALYSIS_INTERVAL_MINUTES", "10"))

pool: Optional[asyncpg.Pool] = None


def _clean_dsn(dsn: str) -> str:
    """يزيل sslmode/channel_binding من الرابط لأننا نمرر إعداد SSL يدوياً إلى asyncpg."""
    parsed = urlparse(dsn)
    query = parse_qs(parsed.query)
    query.pop("sslmode", None)
    query.pop("channel_binding", None)
    new_query = urlencode(query, doseq=True)
    return urlunparse(parsed._replace(query=new_query))


async def init_db():
    """ينشئ Connection Pool مع Neon، وينشئ الجداول المطلوبة إن لم تكن موجودة."""
    global pool
    if not DATABASE_URL:
        logger.warning("⚠️ DATABASE_URL غير معرّف — سيعمل البوت بدون تخزين دائم أو ذاكرة تاريخية (بدون Neon).")
        return

    try:
        ssl_context = ssl.create_default_context()
        pool = await asyncpg.create_pool(
            dsn=_clean_dsn(DATABASE_URL),
            ssl=ssl_context,
            min_size=1,
            max_size=5,
            command_timeout=15,
        )
        async with pool.acquire() as conn:
            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS technical_snapshots (
                    id BIGSERIAL PRIMARY KEY,
                    symbol TEXT NOT NULL,
                    timeframe TEXT NOT NULL,
                    last_price DOUBLE PRECISION,
                    rsi_14 DOUBLE PRECISION,
                    ema_20 DOUBLE PRECISION,
                    ema_50 DOUBLE PRECISION,
                    atr_14 DOUBLE PRECISION,
                    volume BIGINT,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
                );
                CREATE INDEX IF NOT EXISTS idx_tech_lookup
                    ON technical_snapshots (symbol, timeframe, created_at DESC);

                CREATE TABLE IF NOT EXISTS ai_reports (
                    id BIGSERIAL PRIMARY KEY,
                    analysis_type TEXT NOT NULL,
                    symbols_key TEXT NOT NULL,
                    timeframes_key TEXT NOT NULL,
                    symbols TEXT NOT NULL,
                    timeframes TEXT NOT NULL,
                    report_text TEXT NOT NULL,
                    raw_data JSONB,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
                );
                CREATE INDEX IF NOT EXISTS idx_report_lookup
                    ON ai_reports (analysis_type, symbols_key, timeframes_key, created_at DESC);

                CREATE TABLE IF NOT EXISTS active_watches (
                    analysis_type TEXT NOT NULL,
                    symbols_key TEXT NOT NULL,
                    timeframes_key TEXT NOT NULL,
                    symbols TEXT NOT NULL,
                    timeframes TEXT NOT NULL,
                    last_run_at TIMESTAMPTZ,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    PRIMARY KEY (analysis_type, symbols_key, timeframes_key)
                );
                """
            )
        logger.info("✅ تم الاتصال بقاعدة بيانات Neon وتجهيز الجداول بنجاح.")
    except Exception as e:
        logger.error(f"❌ فشل الاتصال بقاعدة بيانات Neon: {e}")
        pool = None


async def close_db():
    global pool
    if pool:
        await pool.close()
        pool = None


def make_key(items: List[str]) -> str:
    """مفتاح تخزين ثابت بغض النظر عن ترتيب اختيار المستخدم للأزواج/الأطر."""
    return ",".join(sorted(items))


# ==================== لقطات المؤشرات الفنية ====================

async def save_technical_snapshot(symbol: str, timeframe: str, indicators: Dict[str, Any]):
    if not pool or not indicators or "error" in indicators:
        return
    try:
        async with pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO technical_snapshots
                    (symbol, timeframe, last_price, rsi_14, ema_20, ema_50, atr_14, volume)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                """,
                symbol, timeframe,
                indicators.get("last_price"),
                indicators.get("rsi_14"),
                indicators.get("ema_20"),
                indicators.get("ema_50"),
                indicators.get("atr_14"),
                int(indicators.get("volume") or 0),
            )
    except Exception as e:
        logger.error(f"DB save_technical_snapshot error: {e}")


async def get_technical_history(symbol: str, timeframe: str, limit: int = 5) -> List[Dict[str, Any]]:
    if not pool:
        return []
    try:
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT last_price, rsi_14, ema_20, ema_50, atr_14, volume, created_at
                FROM technical_snapshots
                WHERE symbol = $1 AND timeframe = $2
                ORDER BY created_at DESC
                LIMIT $3
                """,
                symbol, timeframe, limit
            )
            return [dict(r) for r in rows]
    except Exception as e:
        logger.error(f"DB get_technical_history error: {e}")
        return []


# ==================== تقارير الذكاء الاصطناعي (كاش + ذاكرة تاريخية) ====================

async def get_latest_report(analysis_type: str, symbols: List[str], timeframes: List[str]) -> Optional[Dict[str, Any]]:
    """يرجع آخر تقرير مخزّن لهذه التركيبة (بغض النظر عن عمره — الفحص العمري يتم خارج هذه الدالة)."""
    if not pool:
        return None
    symbols_key = make_key(symbols)
    tfs_key = make_key(timeframes)
    try:
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT report_text, created_at
                FROM ai_reports
                WHERE analysis_type = $1 AND symbols_key = $2 AND timeframes_key = $3
                ORDER BY created_at DESC
                LIMIT 1
                """,
                analysis_type, symbols_key, tfs_key
            )
            return dict(row) if row else None
    except Exception as e:
        logger.error(f"DB get_latest_report error: {e}")
        return None


async def get_report_history(analysis_type: str, symbols: List[str], timeframes: List[str], limit: int = 3) -> List[Dict[str, Any]]:
    """آخر N تقارير سابقة (تُستخدم كـ"ذاكرة" تُمرَّر لـ Gemini ليقارن ويتعلّم من الاتجاه عبر الوقت)."""
    if not pool:
        return []
    symbols_key = make_key(symbols)
    tfs_key = make_key(timeframes)
    try:
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT report_text, created_at
                FROM ai_reports
                WHERE analysis_type = $1 AND symbols_key = $2 AND timeframes_key = $3
                ORDER BY created_at DESC
                LIMIT $4
                """,
                analysis_type, symbols_key, tfs_key, limit
            )
            return [dict(r) for r in rows]
    except Exception as e:
        logger.error(f"DB get_report_history error: {e}")
        return []


async def save_report(analysis_type: str, symbols: List[str], timeframes: List[str],
                       report_text: str, raw_data: Optional[Dict[str, Any]] = None):
    if not pool:
        return
    symbols_key = make_key(symbols)
    tfs_key = make_key(timeframes)
    try:
        async with pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO ai_reports
                    (analysis_type, symbols_key, timeframes_key, symbols, timeframes, report_text, raw_data)
                VALUES ($1, $2, $3, $4, $5, $6, $7)
                """,
                analysis_type, symbols_key, tfs_key,
                ",".join(symbols), ",".join(timeframes),
                report_text,
                json.dumps(raw_data or {}, ensure_ascii=False, default=str),
            )
    except Exception as e:
        logger.error(f"DB save_report error: {e}")


# ==================== المتابعات النشطة (التحديث التلقائي) ====================

async def register_active_watch(analysis_type: str, symbols: List[str], timeframes: List[str]):
    """يسجّل تركيبة (نوع تحليل + أزواج + أطر) لتُحدَّث تلقائياً كل AUTO_ANALYSIS_INTERVAL_MINUTES دقيقة."""
    if not pool:
        return
    symbols_key = make_key(symbols)
    tfs_key = make_key(timeframes)
    try:
        async with pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO active_watches (analysis_type, symbols_key, timeframes_key, symbols, timeframes, last_run_at)
                VALUES ($1, $2, $3, $4, $5, now())
                ON CONFLICT (analysis_type, symbols_key, timeframes_key)
                DO UPDATE SET last_run_at = EXCLUDED.last_run_at
                """,
                analysis_type, symbols_key, tfs_key,
                ",".join(symbols), ",".join(timeframes),
            )
    except Exception as e:
        logger.error(f"DB register_active_watch error: {e}")


async def get_due_watches(interval_minutes: int) -> List[Dict[str, Any]]:
    """يرجع كل المتابعات النشطة التي حان وقت تحديثها (مرّ عليها interval_minutes أو أكثر)."""
    if not pool:
        return []
    try:
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT analysis_type, symbols, timeframes
                FROM active_watches
                WHERE last_run_at IS NULL
                   OR last_run_at < now() - ($1 || ' minutes')::interval
                """,
                str(interval_minutes)
            )
            return [dict(r) for r in rows]
    except Exception as e:
        logger.error(f"DB get_due_watches error: {e}")
        return []


async def mark_watch_run(analysis_type: str, symbols: List[str], timeframes: List[str]):
    if not pool:
        return
    symbols_key = make_key(symbols)
    tfs_key = make_key(timeframes)
    try:
        async with pool.acquire() as conn:
            await conn.execute(
                """
                UPDATE active_watches SET last_run_at = now()
                WHERE analysis_type = $1 AND symbols_key = $2 AND timeframes_key = $3
                """,
                analysis_type, symbols_key, tfs_key
            )
    except Exception as e:
        logger.error(f"DB mark_watch_run error: {e}")