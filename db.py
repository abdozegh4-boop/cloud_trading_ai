"""
db.py
=====
طبقة التخزين الموحَّدة — Google Firestore (رئيسي) + Google BigQuery (اختياري للبيانات الثقيلة).

تم حذف دعم PostgreSQL / Neon بالكامل. جميع البيانات الحية تُخزَّن على Firestore،
وبيانات OHLC الثقيلة تُخزَّن على BigQuery إن ضُبطت BIGQUERY_PROJECT/BIGQUERY_DATASET
وإلا تُخزَّن على Firestore كاحتياط.

كل الدوال "آمنة عند غياب التخزين": إن لم يكن Firestore متاحاً تتصرف بصمت (fallback)
دون كسر عمل البوت.
"""

import os
import json
import logging
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional

import firestore_db as fs  # noqa: F401  (الواجهة الرئيسية للقراءة/الكتابة)

try:
    import bq as bq_sink
except Exception:  # pragma: no cover
    bq_sink = None

logger = logging.getLogger("TradingBot.DB")

# ==================== إعدادات ====================
USE_FIRESTORE = bool(fs and getattr(fs, "FIRESTORE_ENABLED", False))

# كل كم دقيقة يُعاد توليد التحاليل النشطة تلقائياً
AUTO_ANALYSIS_INTERVAL_MINUTES = int(os.getenv("AUTO_ANALYSIS_INTERVAL_MINUTES", "10"))

# الاحتفاظ (أيام قبل حذف الوثائق القديمة؛ 0 = تعطيل التنظيف)
DB_RETENTION_TECH_DAYS = int(os.getenv("DB_RETENTION_TECH_DAYS", "14"))
DB_RETENTION_OHLC_DAYS = int(os.getenv("DB_RETENTION_OHLC_DAYS", "30"))
DB_RETENTION_REPORT_DAYS = int(os.getenv("DB_RETENTION_REPORT_DAYS", "30"))
DB_RETENTION_SIGNAL_DAYS = int(os.getenv("DB_RETENTION_SIGNAL_DAYS", "60"))

# عتبة الاستعلام البطيء (للمراقبة فقط)
DB_SLOW_MS = float(os.getenv("DB_SLOW_MS", "500"))

# علم توافق: بعض الوحدات تتحقق من pool — دائماً None الآن (لم نعد نستخدم PostgreSQL)
pool: Optional[Any] = None


def is_storage_ready() -> bool:
    """True إذا كانت Firestore مفعّلة وجاهزة."""
    if USE_FIRESTORE:
        return fs.is_enabled()
    return False


async def init_db():
    """تهيئة التخزين: Firestore (lazy) + ضمان جداول BigQuery إن فُعِّل."""
    if bq_sink is not None:
        try:
            bq_sink.ensure_dataset_and_tables()
        except Exception as e:
            logger.error(f"BigQuery ensure tables failed: {e}")
    if USE_FIRESTORE:
        logger.info("✅ طبقة التخزين: Firestore (مع BigQuery اختياري للبيانات الثقيلة).")
    else:
        logger.warning(
            "⚠️ Firestore غير مفعّل — سيعمل البوت بدون تخزين دائم أو ذاكرة تاريخية. "
            "اضبط USE_FIRESTORE=1 و FIRESTORE_PROJECT."
        )


async def close_db():
    """لا يوجد اتصال دائم لإغلاقه على Firestore (lazy client)."""
    return


def make_key(items: List[str]) -> str:
    """مفتاح تخزين ثابت بغض النظر عن ترتيب اختيار المستخدم للأزواج/الأطر."""
    return ",".join(sorted(items))


# ==================== لقطات المؤشرات الفنية ====================

async def save_technical_snapshot(symbol: str, timeframe: str, indicators: Dict[str, Any]):
    if not indicators or "error" in indicators:
        return
    await fs.save_technical_snapshot(symbol, timeframe, indicators)
    if bq_sink is not None:
        try:
            await _bq_wrap(bq_sink.insert_technical_snapshot, symbol, timeframe, indicators)
        except Exception as bqe:
            logger.error(f"BQ technical mirror error: {bqe}")


async def get_technical_history(symbol: str, timeframe: str, limit: int = 5) -> List[Dict[str, Any]]:
    if USE_FIRESTORE:
        return await fs.get_technical_history(symbol, timeframe, limit)
    return []


# ==================== شموع OHLC ====================

async def save_ohlc_bars(symbol: str, timeframe: str, bars: List[Dict[str, Any]],
                         source: str = "ctrader") -> int:
    """
    يخزّن شموع OHLC: BigQuery أولاً (إن فُعِّل)، وإلا Firestore.
    bars: قائمة dict بمفاتيح open, high, low, close, volume, bar_time.
    """
    if not bars:
        return 0
    saved = 0
    if bq_sink is not None and getattr(bq_sink, "BIGQUERY_ENABLED", False):
        try:
            saved = await _bq_wrap(bq_sink.insert_ohlc_bars, symbol, timeframe, bars, source=source)
        except Exception as bqe:
            logger.error(f"BQ ohlc insert error: {bqe}")
            saved = 0
    if not saved and USE_FIRESTORE:
        try:
            saved = await fs.save_ohlc_bars(symbol, timeframe, bars, source=source)
        except Exception as e:
            logger.error(f"DB save_ohlc_bars error: {e}")
            saved = 0
    return saved


async def get_ohlc_bars(symbol: str, timeframe: str, limit: int = 100) -> List[Dict[str, Any]]:
    """آخر N شمعة OHLC لزوج/إطار (الأقدم أولاً)."""
    if USE_FIRESTORE:
        try:
            return await fs.get_ohlc_bars(symbol, timeframe, limit)
        except Exception as e:
            logger.error(f"DB get_ohlc_bars error: {e}")
            return []
    return []


async def _bq_wrap(fn, *args, **kwargs):
    """يغلّف استدعاء BigQuery المتزامن داخل executor لتفادي حظر الحلقة."""
    import asyncio
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        return fn(*args, **kwargs)
    return await loop.run_in_executor(None, lambda: fn(*args, **kwargs))


# ==================== تقارير الذكاء الاصطناعي ====================

async def get_latest_report(analysis_type: str, symbols: List[str], timeframes: List[str]) -> Optional[Dict[str, Any]]:
    if not USE_FIRESTORE:
        return None
    return await fs.get_latest_report(analysis_type, symbols, timeframes)


async def get_report_history(analysis_type: str, symbols: List[str], timeframes: List[str], limit: int = 3) -> List[Dict[str, Any]]:
    """آخر N تقارير سابقة (تُستخدم كـ"ذاكرة" للوكلاء)."""
    if not USE_FIRESTORE:
        return []
    return await fs.get_report_history(analysis_type, symbols, timeframes, limit)


async def save_report(analysis_type: str, symbols: List[str], timeframes: List[str],
                      report_text: str, raw_data: Optional[Dict[str, Any]] = None):
    if not USE_FIRESTORE:
        return
    await fs.save_report(analysis_type, symbols, timeframes, report_text, raw_data)
    if bq_sink is not None:
        try:
            await _bq_wrap(
                bq_sink.insert_ai_report_meta, analysis_type, symbols, timeframes, report_text,
            )
        except Exception as bqe:
            logger.error(f"BQ report meta mirror error: {bqe}")


# ==================== المتابعات النشطة (التحديث التلقائي) ====================

async def register_active_watch(analysis_type: str, symbols: List[str], timeframes: List[str]):
    """يسجّل تركيبة (نوع تحليل + أزواج + أطر) لتُحدَّث تلقائياً."""
    if not USE_FIRESTORE:
        return
    await fs.register_active_watch(analysis_type, symbols, timeframes)


async def get_due_watches(interval_minutes: int) -> List[Dict[str, Any]]:
    """يرجع كل المتابعات النشطة التي حان وقت تحديثها."""
    if not USE_FIRESTORE:
        return []
    return await fs.get_due_watches(interval_minutes)


async def mark_watch_run(analysis_type: str, symbols: List[str], timeframes: List[str]):
    if not USE_FIRESTORE:
        return
    await fs.mark_watch_run(analysis_type, symbols, timeframes)


async def get_all_active_symbol_timeframes() -> List[Dict[str, str]]:
    """كل أزواج (symbol, timeframe) من المتابعات النشطة — لأتمتة تحديث OHLC."""
    if not USE_FIRESTORE:
        return []
    return await fs.get_all_active_symbol_timeframes()


# ==================== اختيارات التيليجرام ====================

async def save_telegram_ui_selection(
    chat_id: str,
    symbols: List[str],
    timeframes: List[str],
    trading_tf: Optional[str] = None,
) -> None:
    syms = ",".join([s.strip().upper() for s in symbols if s and str(s).strip()])
    tfs = ",".join([x.strip().upper() for x in timeframes if x and str(x).strip()]) or "H1"
    trade_tf = (trading_tf or (timeframes[0] if timeframes else "H1") or "H1").strip().upper()
    if not USE_FIRESTORE:
        return
    await fs.save_telegram_ui_selection(str(chat_id), syms, tfs, trade_tf)


async def get_latest_telegram_ui_selection() -> Dict[str, Any]:
    if not USE_FIRESTORE:
        return {}
    return await fs.get_latest_telegram_ui_selection()


# ==================== توصية كل زوج + ذاكرة التعلّم ====================

async def save_symbol_signal(symbol: str, emoji: str, direction: str,
                             entry: Optional[str], sl: Optional[str],
                             tp1: Optional[str], tp2: Optional[str], rr: Optional[str],
                             rsi_14: Optional[float] = None,
                             ema_20: Optional[float] = None,
                             ema_50: Optional[float] = None,
                             atr_14: Optional[float] = None,
                             primary_tf: Optional[str] = None,
                             source: str = "tf"):
    """يُضيف توصية جديدة — source: 'full' | 'tf'."""
    if not USE_FIRESTORE:
        return
    await fs.save_symbol_signal(
        symbol, emoji, direction, entry, sl, tp1, tp2, rr,
        rsi_14, ema_20, ema_50, atr_14, primary_tf,
    )
    if bq_sink is not None:
        try:
            await _bq_wrap(
                bq_sink.insert_symbol_signal,
                symbol, emoji, direction, entry, sl, tp1, tp2, rr,
                rsi_14=rsi_14, ema_20=ema_20, ema_50=ema_50, atr_14=atr_14,
                primary_tf=primary_tf,
            )
        except Exception as bqe:
            logger.error(f"BQ signal mirror error: {bqe}")


async def get_symbol_signals(
    symbols: List[str],
    timeframe: Optional[str] = None,
) -> Dict[str, Dict[str, Any]]:
    """
    آخر توصية لكل زوج (مفتاح: اسم الزوج بالأحرف الكبيرة).
    إن مُرّر timeframe تُفضَّل الإشارات المحفوظة لهذا الإطار (primary_tf).
    """
    if not USE_FIRESTORE or not symbols:
        return {}
    rows = await fs.get_symbol_signals(symbols, timeframe)
    return {r.get("symbol", "").upper(): r for r in rows if r.get("symbol")}


async def get_latest_full_signals(
    symbols: List[str],
    max_age_minutes: Optional[int] = None,
) -> Dict[str, Dict[str, Any]]:
    """آخر إشارة من التوصية الموحدة الشاملة (source='full') لكل زوج."""
    if not USE_FIRESTORE or not symbols:
        return {}
    age = int(max_age_minutes) if max_age_minutes else 120
    return await fs.get_latest_full_signals(symbols, max_age_minutes=age)


async def get_symbol_signal_history(symbol: str, limit: int = 8) -> List[Dict[str, Any]]:
    """آخر N توصية سابقة لهذا الزوج (الأقدم أولاً)."""
    if not USE_FIRESTORE:
        return []
    return await fs.get_symbol_signal_history(symbol, limit)


async def get_recent_strong_signals(exclude_symbols: Optional[List[str]] = None,
                                    limit: int = 12) -> List[Dict[str, Any]]:
    """آخر إشارات قوية (شراء/بيع) عبر كل الأزواج — لتعلّم الكوروليشن."""
    if not USE_FIRESTORE:
        return []
    return await fs.get_recent_strong_signals(exclude_symbols, limit)


async def get_similar_condition_signals(
    symbol: str,
    rsi: Optional[float],
    ema_20: Optional[float],
    ema_50: Optional[float],
    limit: int = 6,
    atr: Optional[float] = None,
) -> List[Dict[str, Any]]:
    """توصيات سابقة لنفس الزوج تحت ظروف فنية قريبة (مرتبة بالتشابه)."""
    if not USE_FIRESTORE:
        return []
    return await fs.get_similar_condition_signals(symbol, rsi, ema_20, ema_50, limit, atr=atr)


async def get_symbol_learning_stats(symbol: str, lookback: int = 24) -> Dict[str, Any]:
    """إحصاءات تعلّم كمية لزوج (win-rate لكل جانب، edge_hint)."""
    if not USE_FIRESTORE:
        return {
            "symbol": symbol, "sample_n": 0, "buy_n": 0, "sell_n": 0,
            "buy_win_rate": None, "sell_win_rate": None, "overall_win_rate": None,
            "last_direction": None, "edge_hint": "insufficient_data",
            "realized_n": 0, "realized_sum_pnl": None,
        }
    return await fs.get_symbol_learning_stats(symbol, lookback)


async def infer_signal_outcome(symbol: str, entry: Optional[str], sl: Optional[str],
                               tp1: Optional[str], direction: str,
                               signal_time) -> Optional[str]:
    """تقدير تقريبي لنتيجة توصية سابقة عبر أسعار لاحقة."""
    if not USE_FIRESTORE:
        return None
    return await fs.infer_signal_outcome(symbol, entry, sl, tp1, direction, signal_time)


# ==================== سجل الصفقات + حالة التشغيل ====================

async def save_runtime_state(
    cheap_mode: bool,
    auto_trading: bool,
    trading_tf: str = "H1",
    analysis_tfs: Optional[List[str]] = None,
    selected_symbols: Optional[List[str]] = None,
    merge_full: Optional[bool] = None,
    auto_trading_chat_id: Optional[int] = None,
) -> None:
    if not USE_FIRESTORE:
        return
    await fs.save_runtime_state(
        cheap_mode, auto_trading, trading_tf, analysis_tfs,
        selected_symbols, merge_full, auto_trading_chat_id,
    )


async def load_runtime_state() -> Dict[str, Any]:
    if not USE_FIRESTORE:
        return {}
    return await fs.load_runtime_state()


async def insert_trade_journal(
    symbol: str,
    direction: str,
    timeframe: Optional[str] = None,
    signal_id: Optional[int] = None,
    lots: Optional[float] = None,
    entry_price: Optional[float] = None,
    sl: Optional[float] = None,
    tp1: Optional[float] = None,
    tp2: Optional[float] = None,
    position_id: Optional[int] = None,
    status: str = "pending",
    notes: Optional[str] = None,
) -> Optional[Any]:
    if not USE_FIRESTORE:
        return None
    return await fs.insert_trade_journal(
        symbol, direction, timeframe, signal_id, lots,
        entry_price, sl, tp1, tp2, notes,
    )


async def attach_position_to_journal(
    journal_id: Any,
    position_id: int,
    entry_price: Optional[float] = None,
) -> None:
    if not USE_FIRESTORE:
        return
    await fs.attach_position_to_journal(str(journal_id), position_id)


async def match_pending_journal_to_position(
    symbol: str,
    direction: str,
    position_id: int,
    entry_price: Optional[float] = None,
) -> Optional[Any]:
    """يربط أحدث سجل pending/open بلا position_id بنفس الزوج والاتجاه."""
    if not USE_FIRESTORE:
        return None
    return await fs.match_pending_journal_to_position(symbol, direction, position_id, entry_price)


async def get_open_journal_trades() -> List[Dict[str, Any]]:
    if not USE_FIRESTORE:
        return []
    return await fs.get_open_journal_trades()


async def mark_journal_be_moved(journal_id: Any, new_sl: float) -> None:
    if not USE_FIRESTORE:
        return
    await fs.mark_journal_be_moved(str(journal_id), float(new_sl))


async def close_journal_trade(
    position_id: Optional[int] = None,
    journal_id: Optional[Any] = None,
    close_price: Optional[float] = None,
    realized_pnl: Optional[float] = None,
    status: str = "closed",
    notes: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    if not USE_FIRESTORE:
        return None
    if journal_id is not None:
        await fs.close_journal_trade(
            str(journal_id),
            float(close_price or 0),
            float(realized_pnl or 0),
            status or "closed",
        )
        return {"id": str(journal_id), "status": status}
    # لا يوجد position_id→journal lookup موثوق على Firestore بدون فهرس مركّب؛
    # السجل يُغلق عادةً عبر journal_id من match_pending_journal_to_position.
    return None


async def get_recent_closed_trades(limit: int = 20) -> List[Dict[str, Any]]:
    if not USE_FIRESTORE:
        return []
    return await fs.get_recent_closed_trades(limit)


async def mark_journal_rejected(journal_id: Any, reason: str = "broker_rejected") -> None:
    """يعلّم سجل صفقة كـ rejected (الوسيط رفض الأمر أو لم يصل التنفيذ)."""
    if not USE_FIRESTORE:
        return
    await fs.mark_journal_rejected(str(journal_id), reason)


async def reject_stale_pending_journals(max_age_seconds: int = 300) -> int:
    """يرفض السجلات المعلّقة العالقة بدون تنفيذ (تنظيف دوري)."""
    if not USE_FIRESTORE:
        return 0
    return await fs.reject_stale_pending_journals(max_age_seconds)


async def get_symbol_realized_stats(symbol: str, lookback: int = 30) -> Dict[str, Any]:
    if not USE_FIRESTORE:
        return {"symbol": symbol, "n": 0, "win_rate": None, "avg_pnl": None, "sum_pnl": None}
    return await fs.get_symbol_realized_stats(symbol, lookback)


# ==================== صيانة: تنظيف الوثائق القديمة ====================

async def cleanup_old_rows() -> Dict[str, int]:
    """يحذف البيانات الأقدم من حدود الاحتفاظ على Firestore."""
    if not USE_FIRESTORE:
        return {}
    retention = {
        "technical_snapshots": DB_RETENTION_TECH_DAYS,
        "ohlc_bars": DB_RETENTION_OHLC_DAYS,
        "symbol_signals_history": DB_RETENTION_SIGNAL_DAYS,
        "ai_reports": DB_RETENTION_REPORT_DAYS,
    }
    return await fs.cleanup_old_rows(retention)


# ==================== مراقبة أداء التخزين ====================

_db_metrics: Dict[str, Any] = {
    "queries_total": 0,
    "queries_error": 0,
    "started_at": None,
}


async def get_db_performance_report() -> Dict[str, Any]:
    """لقطة حالة التخزين (Firestore + BigQuery)."""
    report: Dict[str, Any] = {
        "ok": is_storage_ready(),
        "backend": "firestore",
        "firestore": {
            "enabled": bool(USE_FIRESTORE),
            "project": getattr(fs, "FIRESTORE_PROJECT", "") if fs else "",
            "database": getattr(fs, "FIRESTORE_DATABASE", "(default)") if fs else "(default)",
        },
        "bigquery": (bq_sink.status() if bq_sink else {"enabled": False}),
        "tables": [],
        "alerts": [],
        "collected_at": datetime.now(timezone.utc).isoformat(),
    }
    if not is_storage_ready():
        report["alerts"].append(
            "Firestore غير متاح — تأكد من USE_FIRESTORE=1 و FIRESTORE_PROJECT وصلاحية datastore.user"
        )
        report["health"] = "down"
        return report
    try:
        runtime = await fs.load_runtime_state()
        signals = await fs.get_symbol_signals(limit=50)
        opens = await fs.get_open_journal_trades()
        closed = await fs.get_recent_closed_trades(limit=20)
        report["signals_count"] = len(signals or [])
        report["open_trades"] = len(opens or [])
        report["closed_trades"] = len(closed or [])
        report["runtime_keys"] = list((runtime or {}).keys())
        report["tables"] = [
            {"name": "symbol_signals", "count": report["signals_count"]},
            {"name": "trade_journal_open", "count": report["open_trades"]},
            {"name": "trade_journal_closed", "count": report["closed_trades"]},
            {"name": "runtime/state", "count": 1 if runtime else 0},
        ]
    except Exception as e:
        report["ok"] = False
        report["alerts"].append(f"Firestore: {e}")
        logger.error(f"Firestore performance report error: {e}")
    report["health"] = "degraded" if report["alerts"] else ("ok" if report["ok"] else "down")
    return report


def _md_safe(val: Any) -> str:
    s = str(val if val is not None else "—")
    for ch in ("`", "*", "_", "[", "]"):
        s = s.replace(ch, " ")
    return s


def format_db_performance_telegram(report: Dict[str, Any]) -> str:
    """نص مختصر لتلغرام — يتجنب كسر parse_mode=Markdown."""
    lines = ["🔥 مراقبة التخزين (Firestore + BigQuery)", ""]
    if not report.get("ok"):
        lines.append("🔴 غير متصل")
        for a in report.get("alerts") or []:
            lines.append(f"• {_md_safe(a)}")
        return "\n".join(lines)

    fs_info = report.get("firestore") or {}
    health = _md_safe(report.get("health", "ok"))
    emoji = "🟢" if health == "ok" else "🟡"
    lines.append(f"الحالة: {emoji} {health}")
    lines.append(f"المشروع: {_md_safe(fs_info.get('project') or '—')}")
    lines.append(f"قاعدة البيانات: {_md_safe(fs_info.get('database') or '(default)')}")

    bq_info = report.get("bigquery") or {}
    if bq_info.get("enabled"):
        lines.append(
            f"BigQuery: {_md_safe(bq_info.get('project'))}/{_md_safe(bq_info.get('dataset'))} "
            f"جاهز={'نعم' if bq_info.get('ready') else 'لا'}"
        )
    else:
        lines.append("BigQuery: ⬜ غير مفعّل (OHLC على Firestore)")

    lines.append("")
    lines.append(f"🎯 إشارات محفوظة: {report.get('signals_count', 0)}")
    lines.append(f"📋 صفقات مفتوحة: {report.get('open_trades', 0)}")
    lines.append(f"📕 صفقات مغلقة: {report.get('closed_trades', 0)}")
    keys = report.get("runtime_keys") or []
    keys_s = ", ".join(_md_safe(k) for k in keys[:6])
    lines.append(f"⚙️ مفاتيح حالة التشغيل: {len(keys)}" + (f" ({keys_s})" if keys_s else ""))
    if report.get("alerts"):
        lines.append("")
        lines.append("⚠️ تنبيهات:")
        for a in report["alerts"]:
            lines.append(f"• {_md_safe(a)}")
    lines.append("")
    lines.append("اللوحة: /dashboard")
    return "\n".join(lines)
