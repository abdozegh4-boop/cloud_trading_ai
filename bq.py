"""
bq.py — تخزين اختياري في Google BigQuery
========================================
يعمل فقط إذا ضُبطت BIGQUERY_PROJECT و BIGQUERY_DATASET.
عند الغياب: كل الدوال no-op دون كسر البوت.

الجداول (تُنشأ تلقائياً إن أمكن):
  - ohlc_bars
  - technical_snapshots
  - symbol_signals
  - ai_reports_meta  (ملخص تقارير بدون نص طويل كامل إن رغبت لاحقاً)

المصادقة على Cloud Run: حساب الخدمة المرتبط بالخدمة (ADC).
محلياً: GOOGLE_APPLICATION_CREDENTIALS=/path/to/sa.json
"""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

logger = logging.getLogger("TradingBot.BQ")

BIGQUERY_PROJECT = os.getenv("BIGQUERY_PROJECT", "").strip()
BIGQUERY_DATASET = os.getenv("BIGQUERY_DATASET", "").strip()
BIGQUERY_LOCATION = os.getenv("BIGQUERY_LOCATION", "US").strip() or "US"
BIGQUERY_ENABLED = bool(BIGQUERY_PROJECT and BIGQUERY_DATASET)

_client = None
_dataset_ready = False


def _get_client():
    global _client
    if not BIGQUERY_ENABLED:
        return None
    if _client is not None:
        return _client
    try:
        from google.cloud import bigquery  # type: ignore

        _client = bigquery.Client(project=BIGQUERY_PROJECT, location=BIGQUERY_LOCATION)
        return _client
    except Exception as e:
        logger.error(f"BigQuery client init failed: {e}")
        return None


def _table_id(name: str) -> str:
    return f"{BIGQUERY_PROJECT}.{BIGQUERY_DATASET}.{name}"


def ensure_dataset_and_tables() -> bool:
    """إنشاء الـ dataset والجداول إن لم تكن موجودة."""
    global _dataset_ready
    if not BIGQUERY_ENABLED:
        return False
    if _dataset_ready:
        return True
    client = _get_client()
    if client is None:
        return False
    try:
        from google.cloud import bigquery  # type: ignore
        from google.cloud.exceptions import NotFound  # type: ignore

        ds_id = f"{BIGQUERY_PROJECT}.{BIGQUERY_DATASET}"
        try:
            client.get_dataset(ds_id)
        except NotFound:
            ds = bigquery.Dataset(ds_id)
            ds.location = BIGQUERY_LOCATION
            client.create_dataset(ds, exists_ok=True)
            logger.info(f"✅ BigQuery dataset created: {ds_id}")

        schemas = {
            "ohlc_bars": [
                bigquery.SchemaField("symbol", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("timeframe", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("bar_time", "TIMESTAMP", mode="REQUIRED"),
                bigquery.SchemaField("open", "FLOAT64"),
                bigquery.SchemaField("high", "FLOAT64"),
                bigquery.SchemaField("low", "FLOAT64"),
                bigquery.SchemaField("close", "FLOAT64"),
                bigquery.SchemaField("volume", "INT64"),
                bigquery.SchemaField("source", "STRING"),
                bigquery.SchemaField("ingested_at", "TIMESTAMP"),
            ],
            "technical_snapshots": [
                bigquery.SchemaField("symbol", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("timeframe", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("last_price", "FLOAT64"),
                bigquery.SchemaField("rsi_14", "FLOAT64"),
                bigquery.SchemaField("ema_20", "FLOAT64"),
                bigquery.SchemaField("ema_50", "FLOAT64"),
                bigquery.SchemaField("atr_14", "FLOAT64"),
                bigquery.SchemaField("volume", "INT64"),
                bigquery.SchemaField("ingested_at", "TIMESTAMP"),
            ],
            "symbol_signals": [
                bigquery.SchemaField("symbol", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("emoji", "STRING"),
                bigquery.SchemaField("direction", "STRING"),
                bigquery.SchemaField("entry", "STRING"),
                bigquery.SchemaField("sl", "STRING"),
                bigquery.SchemaField("tp1", "STRING"),
                bigquery.SchemaField("tp2", "STRING"),
                bigquery.SchemaField("rr", "STRING"),
                bigquery.SchemaField("rsi_14", "FLOAT64"),
                bigquery.SchemaField("ema_20", "FLOAT64"),
                bigquery.SchemaField("ema_50", "FLOAT64"),
                bigquery.SchemaField("atr_14", "FLOAT64"),
                bigquery.SchemaField("primary_tf", "STRING"),
                bigquery.SchemaField("ingested_at", "TIMESTAMP"),
            ],
            "ai_reports_meta": [
                bigquery.SchemaField("analysis_type", "STRING"),
                bigquery.SchemaField("symbols", "STRING"),
                bigquery.SchemaField("timeframes", "STRING"),
                bigquery.SchemaField("report_chars", "INT64"),
                bigquery.SchemaField("used_fallback", "BOOL"),
                bigquery.SchemaField("ingested_at", "TIMESTAMP"),
            ],
        }

        for tname, schema in schemas.items():
            tid = _table_id(tname)
            try:
                client.get_table(tid)
            except NotFound:
                table = bigquery.Table(tid, schema=schema)
                # تقسيم زمني اختياري على ingested_at للجداول الكبيرة
                if tname == "ohlc_bars":
                    table.time_partitioning = bigquery.TimePartitioning(
                        type_=bigquery.TimePartitioningType.DAY,
                        field="bar_time",
                    )
                client.create_table(table)
                logger.info(f"✅ BigQuery table created: {tid}")

        _dataset_ready = True
        return True
    except Exception as e:
        logger.error(f"BigQuery ensure tables failed: {e}")
        return False


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _insert_rows(table: str, rows: List[Dict[str, Any]]) -> int:
    if not rows or not BIGQUERY_ENABLED:
        return 0
    if not ensure_dataset_and_tables():
        return 0
    client = _get_client()
    if client is None:
        return 0
    try:
        errors = client.insert_rows_json(_table_id(table), rows)
        if errors:
            logger.error(f"BigQuery insert errors on {table}: {errors[:3]}")
            return 0
        return len(rows)
    except Exception as e:
        logger.error(f"BigQuery insert {table} failed: {e}")
        return 0


def insert_ohlc_bars(symbol: str, timeframe: str, bars: List[Dict[str, Any]],
                     source: str = "ctrader") -> int:
    rows = []
    now = _now().isoformat()
    for b in bars:
        bt = b.get("bar_time")
        if hasattr(bt, "isoformat"):
            bt = bt.isoformat()
        rows.append({
            "symbol": symbol,
            "timeframe": timeframe,
            "bar_time": bt,
            "open": b.get("open"),
            "high": b.get("high"),
            "low": b.get("low"),
            "close": b.get("close"),
            "volume": int(b.get("volume") or 0),
            "source": source,
            "ingested_at": now,
        })
    n = _insert_rows("ohlc_bars", rows)
    if n:
        logger.info(f"BQ OHLC +{n} [{symbol} {timeframe}]")
    return n


def insert_technical_snapshot(symbol: str, timeframe: str, indicators: Dict[str, Any]) -> int:
    if not indicators or "error" in indicators:
        return 0
    row = {
        "symbol": symbol,
        "timeframe": timeframe,
        "last_price": indicators.get("last_price"),
        "rsi_14": indicators.get("rsi_14"),
        "ema_20": indicators.get("ema_20"),
        "ema_50": indicators.get("ema_50"),
        "atr_14": indicators.get("atr_14"),
        "volume": int(indicators.get("volume") or 0),
        "ingested_at": _now().isoformat(),
    }
    return _insert_rows("technical_snapshots", [row])


def insert_symbol_signal(
    symbol: str, emoji: str, direction: str,
    entry: Optional[str], sl: Optional[str], tp1: Optional[str], tp2: Optional[str], rr: Optional[str],
    rsi_14: Optional[float] = None, ema_20: Optional[float] = None,
    ema_50: Optional[float] = None, atr_14: Optional[float] = None,
    primary_tf: Optional[str] = None,
) -> int:
    row = {
        "symbol": symbol,
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
        "ingested_at": _now().isoformat(),
    }
    return _insert_rows("symbol_signals", [row])


def insert_ai_report_meta(analysis_type: str, symbols: List[str], timeframes: List[str],
                          report_text: str, used_fallback: bool = False) -> int:
    row = {
        "analysis_type": analysis_type,
        "symbols": ",".join(symbols),
        "timeframes": ",".join(timeframes),
        "report_chars": len(report_text or ""),
        "used_fallback": used_fallback,
        "ingested_at": _now().isoformat(),
    }
    return _insert_rows("ai_reports_meta", [row])


def status() -> Dict[str, Any]:
    return {
        "enabled": BIGQUERY_ENABLED,
        "project": BIGQUERY_PROJECT or None,
        "dataset": BIGQUERY_DATASET or None,
        "location": BIGQUERY_LOCATION,
        "ready": _dataset_ready,
    }
