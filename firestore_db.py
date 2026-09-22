"""
firestore_db.py — طبقة التخزين الحية على Google Firestore
=========================================================
تُستخدم للبيانات المرنة والسريعة (حالة البوت، اختيارات التيليجرام،
المراقبة النشطة، سجل الصفقات، آخر الإشارات).

البيانات التاريخية الثقيلة (OHLC + technical_snapshots) تبقى على BigQuery.

التفعيل:
  FIRESTORE_PROJECT=your-gcp-project-id
  (اختياري) FIRESTORE_DATABASE=(default)   # أو اسم قاعدة بيانات أخرى
  USE_FIRESTORE=1

المصادقة على Cloud Run: حساب الخدمة المرتبط بالخدمة (ADC).
محلياً: GOOGLE_APPLICATION_CREDENTIALS=/path/to/sa.json
"""

from __future__ import annotations

import asyncio
import logging
import math
import os
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional

logger = logging.getLogger("TradingBot.Firestore")

FIRESTORE_PROJECT = os.getenv("FIRESTORE_PROJECT", "").strip() or os.getenv("GOOGLE_CLOUD_PROJECT", "").strip()
FIRESTORE_DATABASE = os.getenv("FIRESTORE_DATABASE", "(default)").strip() or "(default)"
USE_FIRESTORE = os.getenv("USE_FIRESTORE", "0").strip().lower() in ("1", "true", "yes")
FIRESTORE_ENABLED = bool(FIRESTORE_PROJECT and USE_FIRESTORE)

_client = None
_db = None


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _get_db():
    """إرجاع عميل Firestore (lazy)."""
    global _client, _db
    if not FIRESTORE_ENABLED:
        return None
    if _db is not None:
        return _db
    try:
        from google.cloud import firestore  # type: ignore

        _client = firestore.Client(project=FIRESTORE_PROJECT, database=FIRESTORE_DATABASE)
        _db = _client
        logger.info(f"✅ Firestore connected: project={FIRESTORE_PROJECT} db={FIRESTORE_DATABASE}")
        return _db
    except Exception as e:
        logger.error(f"Firestore client init failed: {e}")
        return None


def is_enabled() -> bool:
    return FIRESTORE_ENABLED and _get_db() is not None


# ---------------------------------------------------------------------------
# bot_runtime_state  (مستند واحد: runtime/state)
# ---------------------------------------------------------------------------

async def save_runtime_state(
    cheap_mode: bool,
    auto_trading: bool,
    trading_tf: str = "H1",
    analysis_tfs: Optional[List[str]] = None,
    selected_symbols: Optional[List[str]] = None,
    merge_full: Optional[bool] = None,
    auto_trading_chat_id: Optional[int] = None,
) -> None:
    db = _get_db()
    if not db:
        return
    try:
        from google.cloud import firestore  # type: ignore

        data: Dict[str, Any] = {
            "cheap_mode": bool(cheap_mode),
            "auto_trading": bool(auto_trading),
            "trading_tf": (trading_tf or "H1").upper(),
            "analysis_tfs": ",".join(analysis_tfs or ["H1"]),
            "selected_symbols": ",".join(selected_symbols or []),
            "updated_at": _now(),
        }
        if merge_full is not None:
            data["merge_full"] = bool(merge_full)
        if auto_trading_chat_id is not None:
            data["auto_trading_chat_id"] = int(auto_trading_chat_id)

        ref = db.collection("runtime").document("state")
        ref.set(data, merge=True)
    except Exception as e:
        logger.error(f"Firestore save_runtime_state error: {e}")


async def load_runtime_state() -> Dict[str, Any]:
    db = _get_db()
    if not db:
        return {}
    try:
        ref = db.collection("runtime").document("state")
        snap = ref.get()
        if not snap.exists:
            return {}
        data = snap.to_dict() or {}
        # تطبيع التواريخ إلى ISO إن لزم
        for k, v in list(data.items()):
            if hasattr(v, "isoformat"):
                data[k] = v
        return data
    except Exception as e:
        logger.error(f"Firestore load_runtime_state error: {e}")
        return {}


# ---------------------------------------------------------------------------
# telegram_ui_selection  (مستند لكل chat_id)
# ---------------------------------------------------------------------------

async def save_telegram_ui_selection(
    chat_id: str,
    symbols: str = "",
    timeframes: str = "H1",
    trading_tf: str = "H1",
) -> None:
    db = _get_db()
    if not db:
        return
    try:
        ref = db.collection("telegram_ui").document(str(chat_id))
        ref.set(
            {
                "chat_id": str(chat_id),
                "symbols": symbols or "",
                "timeframes": timeframes or "H1",
                "trading_tf": (trading_tf or "H1").upper(),
                "updated_at": _now(),
            },
            merge=True,
        )
    except Exception as e:
        logger.error(f"Firestore save_telegram_ui_selection error: {e}")


async def get_latest_telegram_ui_selection() -> Dict[str, Any]:
    db = _get_db()
    if not db:
        return {}
    try:
        # أحدث مستند حسب updated_at
        docs = (
            db.collection("telegram_ui")
            .order_by("updated_at", direction="DESCENDING")
            .limit(1)
            .stream()
        )
        for doc in docs:
            data = doc.to_dict() or {}
            return data
        return {}
    except Exception as e:
        logger.error(f"Firestore get_latest_telegram_ui_selection error: {e}")
        return {}


# ---------------------------------------------------------------------------
# active_watches
# ---------------------------------------------------------------------------

def _watch_id(analysis_type: str, symbols_key: str, timeframes_key: str) -> str:
    return f"{analysis_type}__{symbols_key}__{timeframes_key}"


async def register_active_watch(analysis_type: str, symbols: List[str], timeframes: List[str]) -> None:
    db = _get_db()
    if not db:
        return
    try:
        symbols_key = ",".join(sorted(s.upper() for s in symbols))
        timeframes_key = ",".join(sorted(t.upper() for t in timeframes))
        doc_id = _watch_id(analysis_type, symbols_key, timeframes_key)
        ref = db.collection("active_watches").document(doc_id)
        ref.set(
            {
                "analysis_type": analysis_type,
                "symbols_key": symbols_key,
                "timeframes_key": timeframes_key,
                "symbols": ",".join(symbols),
                "timeframes": ",".join(timeframes),
                "last_run_at": None,
                "created_at": _now(),
            },
            merge=True,
        )
    except Exception as e:
        logger.error(f"Firestore register_active_watch error: {e}")


async def mark_watch_run(analysis_type: str, symbols: List[str], timeframes: List[str]) -> None:
    db = _get_db()
    if not db:
        return
    try:
        symbols_key = ",".join(sorted(s.upper() for s in symbols))
        timeframes_key = ",".join(sorted(t.upper() for t in timeframes))
        doc_id = _watch_id(analysis_type, symbols_key, timeframes_key)
        ref = db.collection("active_watches").document(doc_id)
        ref.set({"last_run_at": _now()}, merge=True)
    except Exception as e:
        logger.error(f"Firestore mark_watch_run error: {e}")


async def get_due_watches(interval_minutes: int) -> List[Dict[str, Any]]:
    db = _get_db()
    if not db:
        return []
    try:
        cutoff = _now() - timedelta(minutes=max(1, interval_minutes))
        results = []
        # Firestore لا يدعم OR بسهولة لـ NULL + < cutoff، لذا نجلب الكل ونصفّي
        docs = db.collection("active_watches").stream()
        for doc in docs:
            data = doc.to_dict() or {}
            last = data.get("last_run_at")
            if last is None or (hasattr(last, "replace") and last.replace(tzinfo=timezone.utc) < cutoff):
                results.append(data)
        return results
    except Exception as e:
        logger.error(f"Firestore get_due_watches error: {e}")
        return []


async def get_all_active_symbol_timeframes() -> List[Dict[str, str]]:
    db = _get_db()
    if not db:
        return []
    try:
        pairs = []
        docs = db.collection("active_watches").stream()
        for doc in docs:
            data = doc.to_dict() or {}
            symbols = [s.strip() for s in (data.get("symbols") or "").split(",") if s.strip()]
            tfs = [t.strip() for t in (data.get("timeframes") or "").split(",") if t.strip()]
            for s in symbols:
                for t in tfs:
                    pairs.append({"symbol": s, "timeframe": t})
        return pairs
    except Exception as e:
        logger.error(f"Firestore get_all_active_symbol_timeframes error: {e}")
        return []


# ---------------------------------------------------------------------------
# symbol_signals  (مجموعة، آخر إشارة لكل رمز تُحفظ كمستند symbol)
# ---------------------------------------------------------------------------

async def save_symbol_signal(
    symbol: str,
    emoji: str,
    direction: str,
    entry: Optional[str] = None,
    sl: Optional[str] = None,
    tp1: Optional[str] = None,
    tp2: Optional[str] = None,
    rr: Optional[str] = None,
    rsi_14: Optional[float] = None,
    ema_20: Optional[float] = None,
    ema_50: Optional[float] = None,
    atr_14: Optional[float] = None,
    primary_tf: Optional[str] = None,
    status: str = "pending",
) -> None:
    db = _get_db()
    if not db:
        return
    try:
        sym = symbol.upper().strip()
        data = {
            "symbol": sym,
            "emoji": emoji,
            "direction": direction,
            "entry": entry,
            "sl": sl,
            "tp1": tp1,
            "tp2": tp2,
            "rr": rr,
            "rsi_14": rsi_14,
            "ema_20": ema_20,
            "ema_50": ema_50,
            "atr_14": atr_14,
            "primary_tf": primary_tf,
            "status": status or "pending",
            "created_at": _now(),
        }
        # أحدث إشارة للرمز
        db.collection("symbol_signals").document(sym).set(data, merge=True)
        # سجل تاريخي (اختياري، محدود)
        hist_ref = db.collection("symbol_signals_history").document()
        hist_ref.set(data)
    except Exception as e:
        logger.error(f"Firestore save_symbol_signal error: {e}")


async def get_symbol_signals(
    symbols: Optional[List[str]] = None,
    timeframe: Optional[str] = None,
    limit: int = 50,
) -> List[Dict[str, Any]]:
    db = _get_db()
    if not db:
        return []
    try:
        results = []
        if symbols:
            for sym in symbols:
                doc = db.collection("symbol_signals").document(sym.upper()).get()
                if doc.exists:
                    data = doc.to_dict() or {}
                    if timeframe and data.get("primary_tf") and data["primary_tf"].upper() != timeframe.upper():
                        continue
                    results.append(data)
        else:
            q = db.collection("symbol_signals").order_by("created_at", direction="DESCENDING").limit(limit)
            for doc in q.stream():
                results.append(doc.to_dict() or {})
        return results
    except Exception as e:
        logger.error(f"Firestore get_symbol_signals error: {e}")
        return []


async def get_latest_full_signals(
    symbols: List[str],
    max_age_minutes: int = 120,
) -> Dict[str, Dict[str, Any]]:
    db = _get_db()
    if not db:
        return {}
    try:
        cutoff = _now() - timedelta(minutes=max(1, max_age_minutes))
        out: Dict[str, Dict[str, Any]] = {}
        for sym in symbols:
            doc = db.collection("symbol_signals").document(sym.upper()).get()
            if not doc.exists:
                continue
            data = doc.to_dict() or {}
            created = data.get("created_at")
            if created is not None:
                if hasattr(created, "replace"):
                    created = created.replace(tzinfo=timezone.utc) if created.tzinfo is None else created
                if created < cutoff:
                    continue
            out[sym.upper()] = data
        return out
    except Exception as e:
        logger.error(f"Firestore get_latest_full_signals error: {e}")
        return {}


async def get_symbol_signal_history(symbol: str, limit: int = 8) -> List[Dict[str, Any]]:
    db = _get_db()
    if not db:
        return []
    try:
        q = (
            db.collection("symbol_signals_history")
            .where("symbol", "==", symbol.upper())
            .order_by("created_at", direction="DESCENDING")
            .limit(limit)
        )
        return [doc.to_dict() or {} for doc in q.stream()]
    except Exception as e:
        logger.error(f"Firestore get_symbol_signal_history error: {e}")
        return []


async def get_recent_strong_signals(
    exclude_symbols: Optional[List[str]] = None,
    limit: int = 20,
) -> List[Dict[str, Any]]:
    db = _get_db()
    if not db:
        return []
    try:
        exclude = {s.upper() for s in (exclude_symbols or [])}
        # نجلب آخر الإشارات ونصفّي القوية
        q = db.collection("symbol_signals").order_by("created_at", direction="DESCENDING").limit(80)
        results = []
        for doc in q.stream():
            data = doc.to_dict() or {}
            if data.get("emoji") not in ("🟢", "🔴"):
                continue
            if data.get("symbol", "").upper() in exclude:
                continue
            results.append(data)
            if len(results) >= limit:
                break
        return results
    except Exception as e:
        logger.error(f"Firestore get_recent_strong_signals error: {e}")
        return []


# ---------------------------------------------------------------------------
# trade_journal
# ---------------------------------------------------------------------------

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
    notes: Optional[str] = None,
) -> Optional[str]:
    """يُرجع document id كسلسلة."""
    db = _get_db()
    if not db:
        return None
    try:
        ref = db.collection("trade_journal").document()
        data = {
            "id": ref.id,
            "signal_id": signal_id,
            "symbol": symbol.upper(),
            "direction": direction,
            "timeframe": timeframe,
            "position_id": None,
            "lots": lots,
            "entry_price": entry_price,
            "sl": sl,
            "tp1": tp1,
            "tp2": tp2,
            "close_price": None,
            "realized_pnl": None,
            "status": "pending",
            "be_moved": False,
            "trail_active": False,
            "notes": notes,
            "opened_at": None,
            "closed_at": None,
            "created_at": _now(),
        }
        ref.set(data)
        return ref.id
    except Exception as e:
        logger.error(f"Firestore insert_trade_journal error: {e}")
        return None


async def attach_position_to_journal(journal_id: str, position_id: int, opened_at: Optional[datetime] = None) -> None:
    db = _get_db()
    if not db:
        return
    try:
        ref = db.collection("trade_journal").document(str(journal_id))
        ref.set(
            {
                "position_id": int(position_id),
                "status": "open",
                "opened_at": opened_at or _now(),
            },
            merge=True,
        )
    except Exception as e:
        logger.error(f"Firestore attach_position_to_journal error: {e}")


async def get_open_journal_trades() -> List[Dict[str, Any]]:
    db = _get_db()
    if not db:
        return []
    try:
        # status in ['pending', 'open']
        results = []
        for status in ("pending", "open"):
            q = db.collection("trade_journal").where("status", "==", status).stream()
            for doc in q:
                data = doc.to_dict() or {}
                data["id"] = doc.id
                results.append(data)
        return results
    except Exception as e:
        logger.error(f"Firestore get_open_journal_trades error: {e}")
        return []


async def mark_journal_be_moved(journal_id: str, new_sl: float) -> None:
    db = _get_db()
    if not db:
        return
    try:
        ref = db.collection("trade_journal").document(str(journal_id))
        ref.set({"be_moved": True, "sl": float(new_sl)}, merge=True)
    except Exception as e:
        logger.error(f"Firestore mark_journal_be_moved error: {e}")


async def close_journal_trade(
    journal_id: str,
    close_price: float,
    realized_pnl: float,
    status: str = "closed",
) -> None:
    db = _get_db()
    if not db:
        return
    try:
        ref = db.collection("trade_journal").document(str(journal_id))
        ref.set(
            {
                "close_price": float(close_price),
                "realized_pnl": float(realized_pnl),
                "status": status or "closed",
                "closed_at": _now(),
            },
            merge=True,
        )
    except Exception as e:
        logger.error(f"Firestore close_journal_trade error: {e}")


async def mark_journal_rejected(journal_id: str, reason: str = "broker_rejected") -> None:
    """يعلّم سجل صفقة كـ rejected (الوسيط رفض الأمر) بدل بقائه pending للأبد."""
    db = _get_db()
    if not db:
        return
    try:
        ref = db.collection("trade_journal").document(str(journal_id))
        ref.set(
            {
                "status": "rejected",
                "reject_reason": str(reason)[:500],
                "rejected_at": _now(),
            },
            merge=True,
        )
    except Exception as e:
        logger.error(f"Firestore mark_journal_rejected error: {e}")


async def reject_stale_pending_journals(max_age_seconds: int = 300) -> int:
    """
    يُغلق أي سجل ما زال 'pending' أو 'open' بدون position_id بعد max_age_seconds
    من إنشائه — الوسيط إما رفضه أو لم يصله أبداً. يُستدعى دورياً من sync_trade_journal.
    يعيد عدد السجلات التي تم رفضها.
    """
    db = _get_db()
    if not db:
        return 0
    rejected = 0
    try:
        cutoff = _now() - timedelta(seconds=max(30, int(max_age_seconds)))
        q = db.collection("trade_journal").where("status", "in", ["pending", "open"]).limit(100)
        for doc in q.stream():
            data = doc.to_dict() or {}
            created = data.get("created_at")
            if created is None:
                continue
            if hasattr(created, "replace"):
                created = created.replace(tzinfo=timezone.utc)
            if created < cutoff and not data.get("position_id"):
                await mark_journal_rejected(doc.id, reason="stale_no_fill")
                rejected += 1
    except Exception as e:
        logger.error(f"Firestore reject_stale_pending_journals error: {e}")
    if rejected:
        logger.info(f"🧹 رفض {rejected} سجل صفقة عالق بدون تنفيذ")
    return rejected


async def get_recent_closed_trades(limit: int = 20) -> List[Dict[str, Any]]:
    db = _get_db()
    if not db:
        return []
    try:
        q = (
            db.collection("trade_journal")
            .where("status", ">=", "closed")
            .order_by("status")
            .order_by("closed_at", direction="DESCENDING")
            .limit(limit)
        )
        results = []
        for doc in q.stream():
            data = doc.to_dict() or {}
            data["id"] = doc.id
            results.append(data)
        return results
    except Exception as e:
        # fallback: جلب بدون فهرس مركّب
        try:
            q = db.collection("trade_journal").order_by("created_at", direction="DESCENDING").limit(limit * 3)
            results = []
            for doc in q.stream():
                data = doc.to_dict() or {}
                if not str(data.get("status", "")).startswith("closed"):
                    continue
                data["id"] = doc.id
                results.append(data)
                if len(results) >= limit:
                    break
            return results
        except Exception as e2:
            logger.error(f"Firestore get_recent_closed_trades error: {e2}")
            return []


async def get_symbol_realized_stats(symbol: str, lookback: int = 30) -> Dict[str, Any]:
    db = _get_db()
    if not db:
        return {"wins": 0, "losses": 0, "total_pnl": 0.0, "count": 0}
    try:
        q = (
            db.collection("trade_journal")
            .where("symbol", "==", symbol.upper())
            .order_by("closed_at", direction="DESCENDING")
            .limit(lookback)
        )
        wins = losses = 0
        total_pnl = 0.0
        count = 0
        for doc in q.stream():
            data = doc.to_dict() or {}
            if not str(data.get("status", "")).startswith("closed"):
                continue
            pnl = data.get("realized_pnl")
            if pnl is None:
                continue
            pnl = float(pnl)
            total_pnl += pnl
            count += 1
            if pnl > 0:
                wins += 1
            elif pnl < 0:
                losses += 1
        return {"wins": wins, "losses": losses, "total_pnl": total_pnl, "count": count}
    except Exception as e:
        logger.error(f"Firestore get_symbol_realized_stats error: {e}")
        return {"wins": 0, "losses": 0, "total_pnl": 0.0, "count": 0}


# ---------------------------------------------------------------------------
# ai_reports (ملخص فقط — النص الكامل يمكن وضعه في Cloud Storage لاحقاً)
# ---------------------------------------------------------------------------

async def save_report(
    analysis_type: str,
    symbols: List[str],
    timeframes: List[str],
    report_text: str,
    raw_data: Optional[Dict] = None,
) -> None:
    db = _get_db()
    if not db:
        return
    try:
        symbols_key = ",".join(sorted(s.upper() for s in symbols))
        timeframes_key = ",".join(sorted(t.upper() for t in timeframes))
        ref = db.collection("ai_reports").document()
        ref.set(
            {
                "analysis_type": analysis_type,
                "symbols_key": symbols_key,
                "timeframes_key": timeframes_key,
                "symbols": ",".join(symbols),
                "timeframes": ",".join(timeframes),
                "report_text": (report_text or "")[:50000],  # حد معقول
                "raw_data": raw_data,
                "created_at": _now(),
            }
        )
    except Exception as e:
        logger.error(f"Firestore save_report error: {e}")


async def get_latest_report(
    analysis_type: str,
    symbols: List[str],
    timeframes: List[str],
) -> Optional[Dict[str, Any]]:
    db = _get_db()
    if not db:
        return None
    try:
        symbols_key = ",".join(sorted(s.upper() for s in symbols))
        timeframes_key = ",".join(sorted(t.upper() for t in timeframes))
        q = (
            db.collection("ai_reports")
            .where("analysis_type", "==", analysis_type)
            .where("symbols_key", "==", symbols_key)
            .where("timeframes_key", "==", timeframes_key)
            .order_by("created_at", direction="DESCENDING")
            .limit(1)
        )
        for doc in q.stream():
            return doc.to_dict()
        return None
    except Exception as e:
        logger.error(f"Firestore get_latest_report error: {e}")
        return None


async def get_report_history(
    analysis_type: str,
    symbols: List[str],
    timeframes: List[str],
    limit: int = 3,
) -> List[Dict[str, Any]]:
    db = _get_db()
    if not db:
        return []
    try:
        symbols_key = ",".join(sorted(s.upper() for s in symbols))
        timeframes_key = ",".join(sorted(t.upper() for t in timeframes))
        q = (
            db.collection("ai_reports")
            .where("analysis_type", "==", analysis_type)
            .where("symbols_key", "==", symbols_key)
            .where("timeframes_key", "==", timeframes_key)
            .order_by("created_at", direction="DESCENDING")
            .limit(limit)
        )
        return [doc.to_dict() or {} for doc in q.stream()]
    except Exception as e:
        logger.error(f"Firestore get_report_history error: {e}")
        return []


# ---------------------------------------------------------------------------
# technical_snapshots (سجل لقطات المؤشرات الفنية — للتعلّم وتقدير النتائج)
# ---------------------------------------------------------------------------

async def save_technical_snapshot(symbol: str, timeframe: str, indicators: Dict[str, Any]) -> None:
    db = _get_db()
    if not db or not indicators or "error" in indicators:
        return
    try:
        ref = db.collection("technical_snapshots").document()
        ref.set({
            "symbol": symbol.upper(),
            "timeframe": (timeframe or "").upper(),
            "last_price": indicators.get("last_price"),
            "rsi_14": indicators.get("rsi_14"),
            "ema_20": indicators.get("ema_20"),
            "ema_50": indicators.get("ema_50"),
            "atr_14": indicators.get("atr_14"),
            "volume": int(indicators.get("volume") or 0),
            "created_at": _now(),
        })
    except Exception as e:
        logger.error(f"Firestore save_technical_snapshot error: {e}")


async def get_technical_history(symbol: str, timeframe: str, limit: int = 5) -> List[Dict[str, Any]]:
    db = _get_db()
    if not db:
        return []
    try:
        q = (
            db.collection("technical_snapshots")
            .where("symbol", "==", symbol.upper())
            .where("timeframe", "==", (timeframe or "").upper())
            .order_by("created_at", direction="DESCENDING")
            .limit(limit)
        )
        return [doc.to_dict() or {} for doc in q.stream()]
    except Exception as e:
        logger.error(f"Firestore get_technical_history error: {e}")
        return []


# ---------------------------------------------------------------------------
# infer_signal_outcome — تقدير نتيجة توصية من لقطات الأسعار اللاحقة
# ---------------------------------------------------------------------------

async def infer_signal_outcome(symbol: str, entry: Optional[str], sl: Optional[str],
                               tp1: Optional[str], direction: str,
                               signal_time) -> Optional[str]:
    db = _get_db()
    if not db or not entry:
        return None
    try:
        entry_f = float(str(entry).replace(",", "").strip())
        sl_f = float(str(sl).replace(",", "").strip()) if sl else None
        tp1_f = float(str(tp1).replace(",", "").strip()) if tp1 else None
    except (ValueError, TypeError):
        return None

    is_buy = ("شراء" in (direction or "")) or ("BUY" in (direction or "").upper()) or ("🟢" in (direction or ""))
    try:
        st = signal_time
        if hasattr(st, "replace"):
            st = st.replace(tzinfo=timezone.utc) if st.tzinfo is None else st
        q = (
            db.collection("technical_snapshots")
            .where("symbol", "==", symbol.upper())
            .where("created_at", ">", st)
            .order_by("created_at", direction="ASCENDING")
            .limit(40)
        )
        rows = [doc.to_dict() or {} for doc in q.stream()]
        prices = [float(r["last_price"]) for r in rows if r.get("last_price") is not None]
        if not prices:
            return None

        max_p, min_p, last_p = max(prices), min(prices), prices[-1]
        if is_buy:
            if tp1_f and max_p >= tp1_f * 0.998:
                return "وصل/تجاوز TP1 تقريباً ✓"
            if sl_f and min_p <= sl_f * 1.002:
                return "ضرب/اقترب من SL ✗"
            if last_p > entry_f:
                return "تحرك مع الاتجاه (لم يُحسم بعد)"
            return "تحرك ضد الاتجاه أو جانبي"
        else:
            if tp1_f and min_p <= tp1_f * 1.002:
                return "وصل/تجاوز TP1 تقريباً ✓"
            if sl_f and max_p >= sl_f * 0.998:
                return "ضرب/اقترب من SL ✗"
            if last_p < entry_f:
                return "تحرك مع الاتجاه (لم يُحسم بعد)"
            return "تحرك ضد الاتجاه أو جانبي"
    except Exception as e:
        logger.error(f"Firestore infer_signal_outcome error: {e}")
        return None


# ---------------------------------------------------------------------------
# get_similar_condition_signals — ظروف فنية مشابهة (تحسب محلياً من السجل)
# ---------------------------------------------------------------------------

async def get_similar_condition_signals(
    symbol: str,
    rsi: Optional[float],
    ema_20: Optional[float],
    ema_50: Optional[float],
    limit: int = 6,
    atr: Optional[float] = None,
) -> List[Dict[str, Any]]:
    db = _get_db()
    if not db or rsi is None:
        return []
    try:
        q = (
            db.collection("symbol_signals_history")
            .where("symbol", "==", symbol.upper())
            .order_by("created_at", direction="DESCENDING")
            .limit(60)
        )
        rows = [doc.to_dict() or {} for doc in q.stream()]
        scored: List[Dict[str, Any]] = []
        for d in rows:
            r = d.get("rsi_14")
            if r is None:
                continue
            try:
                r_f = float(r)
            except Exception:
                continue
            if not (-8.0 <= r_f - float(rsi) <= 8.0):
                continue
            dist = abs(r_f - float(rsi))
            if atr is not None and d.get("atr_14") is not None:
                try:
                    a0, a1 = float(atr), float(d["atr_14"])
                    if a0 > 0:
                        ratio = abs(a1 - a0) / a0
                        dist += 5.0 if ratio > 0.4 else ratio * 2.0
                except Exception:
                    pass
            d["_similarity_score"] = dist
            scored.append(d)
        scored.sort(key=lambda x: x.get("_similarity_score", 99))
        top = scored[:limit]
        for t in top:
            t.pop("_similarity_score", None)
        return top
    except Exception as e:
        logger.error(f"Firestore get_similar_condition_signals error: {e}")
        return []


# ---------------------------------------------------------------------------
# get_symbol_learning_stats — إحصاءات تعلّم كمية
# ---------------------------------------------------------------------------

def _classify_outcome_label(label: Optional[str]) -> Optional[str]:
    if not label:
        return None
    s = label.lower()
    if "tp1" in s or "تجاوز" in label or "✓" in label:
        return "win"
    if "sl" in s or "✗" in label or "ضرب" in label:
        return "loss"
    if "مع الاتجاه" in label:
        return "open_win"
    if "ضد" in label:
        return "open_loss"
    return "open"


async def get_symbol_learning_stats(symbol: str, lookback: int = 24) -> Dict[str, Any]:
    empty: Dict[str, Any] = {
        "symbol": symbol,
        "sample_n": 0,
        "buy_n": 0,
        "sell_n": 0,
        "buy_win_rate": None,
        "sell_win_rate": None,
        "overall_win_rate": None,
        "last_direction": None,
        "edge_hint": "insufficient_data",
        "realized_n": 0,
        "realized_sum_pnl": None,
    }
    db = _get_db()
    if not db:
        return empty
    try:
        hist = await get_symbol_signal_history(symbol, limit=lookback)
        if not hist:
            return empty

        buy_w = buy_l = sell_w = sell_l = 0
        overall_w = overall_l = 0
        last_dir = None
        for h in hist:
            direction = (h.get("direction") or "")
            emoji = h.get("emoji") or ""
            is_buy = ("شراء" in direction) or ("BUY" in direction.upper()) or emoji == "🟢"
            is_sell = ("بيع" in direction) or ("SELL" in direction.upper()) or emoji == "🔴"
            if not is_buy and not is_sell:
                continue
            last_dir = "BUY" if is_buy else "SELL"
            outcome = await infer_signal_outcome(
                symbol, h.get("entry"), h.get("sl"), h.get("tp1"),
                direction, h.get("created_at"),
            )
            cls = _classify_outcome_label(outcome)
            if cls in ("win", "open_win"):
                overall_w += 1
                if is_buy:
                    buy_w += 1
                else:
                    sell_w += 1
            elif cls in ("loss", "open_loss"):
                overall_l += 1
                if is_buy:
                    buy_l += 1
                else:
                    sell_l += 1

        def _rate(w: int, l: int):
            t = w + l
            return round(w / t, 3) if t > 0 else None

        buy_n, sell_n = buy_w + buy_l, sell_w + sell_l
        overall_n = overall_w + overall_l
        buy_wr = _rate(buy_w, buy_l)
        sell_wr = _rate(sell_w, sell_l)
        overall_wr = _rate(overall_w, overall_l)

        edge = "insufficient_data"
        if overall_n >= 5:
            if overall_wr is not None and overall_wr >= 0.55:
                edge = "positive"
            elif overall_wr is not None and overall_wr <= 0.40:
                edge = "negative"
            else:
                edge = "neutral"

        realized = await get_symbol_realized_stats(symbol, lookback=max(12, lookback))
        if realized.get("n", 0) >= 3 and realized.get("win_rate") is not None:
            overall_wr = realized["win_rate"]
            overall_n = max(overall_n, int(realized["n"]))
            if overall_wr >= 0.55:
                edge = "positive"
            elif overall_wr <= 0.40:
                edge = "negative"
            else:
                edge = "neutral"

        return {
            "symbol": symbol,
            "sample_n": overall_n,
            "buy_n": buy_n,
            "sell_n": sell_n,
            "buy_win_rate": buy_wr,
            "sell_win_rate": sell_wr,
            "overall_win_rate": overall_wr,
            "last_direction": last_dir,
            "edge_hint": edge,
            "realized_n": int(realized.get("n") or 0),
            "realized_sum_pnl": realized.get("sum_pnl"),
        }
    except Exception as e:
        logger.error(f"Firestore get_symbol_learning_stats error: {e}")
        return empty


# ---------------------------------------------------------------------------
# match_pending_journal_to_position — ربط سجل pending بالصفقة الحية
# ---------------------------------------------------------------------------

async def match_pending_journal_to_position(
    symbol: str,
    direction: str,
    position_id: int,
    entry_price: Optional[float] = None,
) -> Optional[str]:
    """يربط أحدث سجل pending/open بلا position_id بنفس الزوج والاتجاه. يُرجع document id."""
    db = _get_db()
    if not db:
        return None
    try:
        # قاعدة بيانات واحدة لكل صفقة: نجلب آخر pending/open مطابق
        q = (
            db.collection("trade_journal")
            .where("symbol", "==", symbol.upper())
            .order_by("created_at", direction="DESCENDING")
            .limit(20)
        )
        for doc in q.stream():
            data = doc.to_dict() or {}
            if str(data.get("direction", "")).upper() != str(direction or "").upper():
                continue
            if data.get("status") not in ("pending", "open"):
                continue
            if data.get("position_id"):
                continue
            jid = doc.id
            doc.reference.set(
                {
                    "position_id": int(position_id),
                    "status": "open",
                    "entry_price": entry_price if entry_price is not None else data.get("entry_price"),
                    "opened_at": _now(),
                },
                merge=True,
            )
            return jid
        return None
    except Exception as e:
        logger.error(f"Firestore match_pending_journal_to_position error: {e}")
        return None


# ---------------------------------------------------------------------------
# ohlc_bars — نسخة Firestore (احتياطية؛ الأساسي BigQuery إن فُعِّل)
# ---------------------------------------------------------------------------

async def save_ohlc_bars(symbol: str, timeframe: str, bars: List[Dict[str, Any]],
                         source: str = "ctrader") -> int:
    db = _get_db()
    if not db or not bars:
        return 0
    saved = 0
    batch = db.batch()
    n_in_batch = 0
    for b in bars:
        bt = b.get("bar_time")
        if bt is None:
            continue
        if hasattr(bt, "isoformat"):
            bt_s = bt.isoformat()
        else:
            bt_s = str(bt)
        doc_id = f"{symbol.upper()}_{(timeframe or '').upper()}_{bt_s}"
        ref = db.collection("ohlc_bars").document(doc_id)
        batch.set(ref, {
            "symbol": symbol.upper(),
            "timeframe": (timeframe or "").upper(),
            "bar_time": bt,
            "open": float(b["open"]),
            "high": float(b["high"]),
            "low": float(b["low"]),
            "close": float(b["close"]),
            "volume": int(b.get("volume") or 0),
            "source": source,
            "ingested_at": _now(),
        }, merge=True)
        saved += 1
        n_in_batch += 1
        if n_in_batch >= 400:
            await _commit_batch(batch)
            batch = db.batch()
            n_in_batch = 0
    if n_in_batch:
        await _commit_batch(batch)
    return saved


async def _commit_batch(batch) -> None:
    """تنفيذ batch بشكل غير متزامن (عميل Firestore متزامن)."""
    try:
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, batch.commit)
    except Exception as e:
        logger.error(f"Firestore batch commit error: {e}")


async def get_ohlc_bars(symbol: str, timeframe: str, limit: int = 100) -> List[Dict[str, Any]]:
    db = _get_db()
    if not db:
        return []
    try:
        q = (
            db.collection("ohlc_bars")
            .where("symbol", "==", symbol.upper())
            .where("timeframe", "==", (timeframe or "").upper())
            .order_by("bar_time", direction="DESCENDING")
            .limit(limit)
        )
        rows = [doc.to_dict() or {} for doc in q.stream()]
        return list(reversed(rows))
    except Exception as e:
        logger.error(f"Firestore get_ohlc_bars error: {e}")
        return []


# ---------------------------------------------------------------------------
# أداء/تنظيف Firestore
# ---------------------------------------------------------------------------

async def cleanup_old_rows(retention: Dict[str, int]) -> Dict[str, int]:
    """
    تنظيف الوثائق القديمة من المجموعات كثيفة الكتابة.
    retention: {'technical_snapshots': days, 'ohlc_bars': days, 'symbol_signals_history': days, 'ai_reports': days}
    """
    db = _get_db()
    result = {
        "technical_snapshots": 0,
        "ohlc_bars": 0,
        "symbol_signals_history": 0,
        "ai_reports": 0,
    }
    if not db:
        return result
    now = _now()
    jobs = {
        "technical_snapshots": ("created_at", retention.get("technical_snapshots", 14)),
        "ohlc_bars": ("bar_time", retention.get("ohlc_bars", 30)),
        "symbol_signals_history": ("created_at", retention.get("symbol_signals_history", 60)),
        "ai_reports": ("created_at", retention.get("ai_reports", 30)),
    }
    for coll, (field, days) in jobs.items():
        if not days or days <= 0:
            continue
        cutoff = now - timedelta(days=int(days))
        try:
            q = db.collection(coll).where(field, "<", cutoff).limit(400)
            deleted = 0
            while True:
                snaps = list(q.stream())
                if not snaps:
                    break
                batch = db.batch()
                for s in snaps:
                    batch.delete(s.reference)
                await _commit_batch(batch)
                deleted += len(snaps)
                if len(snaps) < 400:
                    break
            result[coll] = deleted
        except Exception as e:
            logger.error(f"Firestore cleanup {coll} error: {e}")
    logger.info(f"🧹 Firestore cleanup done: {result}")
    return result


# ---------------------------------------------------------------------------
# دوال مساعدة للوحة التحكم
# ---------------------------------------------------------------------------

async def get_dashboard_summary() -> Dict[str, Any]:
    """ملخص سريع للوحة التحكم."""
    db = _get_db()
    summary: Dict[str, Any] = {
        "runtime": {},
        "open_trades": [],
        "recent_closed": [],
        "latest_signals": [],
        "firestore_ok": False,
    }
    if not db:
        return summary
    try:
        summary["firestore_ok"] = True
        summary["runtime"] = await load_runtime_state()
        summary["open_trades"] = await get_open_journal_trades()
        summary["recent_closed"] = await get_recent_closed_trades(limit=15)
        summary["latest_signals"] = await get_symbol_signals(limit=30)
    except Exception as e:
        logger.error(f"Firestore get_dashboard_summary error: {e}")
    return summary
