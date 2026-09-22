"""
web_dashboard.py — لوحة تحكم ويب كاملة (بديل Streamlit)
=======================================================
المسارات:
  GET /dashboard          → الواجهة
  GET /api/dashboard      → ملخص شامل
  GET /api/signals        → التوصيات الحية
  GET /api/reports        → تقارير AI المخزنة
  GET /api/ohlc           → شموع OHLC (من BigQuery إن وُجد)
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from fastapi import FastAPI, Query
from fastapi.responses import HTMLResponse, JSONResponse

logger = logging.getLogger("TradingBot.WebDashboard")

DASHBOARD_HTML = r"""<!DOCTYPE html>
<html lang="ar" dir="rtl">
<head>
<meta charset="utf-8"/>
<meta name="viewport" content="width=device-width, initial-scale=1"/>
<title>Cloud Trading AI — Dashboard</title>
<script src="https://cdn.plot.ly/plotly-2.27.0.min.js"></script>
<style>
  :root {
    --bg:#0b1220; --card:#151d2e; --card2:#1a2438; --text:#e8eef7;
    --muted:#8b9bb4; --accent:#38bdf8; --green:#22c55e; --red:#ef4444;
    --yellow:#eab308; --border:#243044;
  }
  * { box-sizing:border-box; margin:0; padding:0; }
  body { font-family: system-ui, -apple-system, 'Segoe UI', Tahoma, sans-serif; background:var(--bg); color:var(--text); min-height:100vh; }
  header {
    background:linear-gradient(120deg,#0f1c33 0%,#152238 50%,#0b1220 100%);
    padding:1rem 1.25rem; border-bottom:1px solid var(--border);
    display:flex; justify-content:space-between; align-items:center; flex-wrap:wrap; gap:.75rem;
    position:sticky; top:0; z-index:50;
  }
  header h1 { font-size:1.25rem; font-weight:700; letter-spacing:.2px; }
  .badge { padding:.28rem .75rem; border-radius:999px; font-size:.78rem; font-weight:600; }
  .badge.warn { background:#eab30822; color:var(--yellow); }
  .badge.ok { background:#22c55e22; color:var(--green); }
  .badge.bad { background:#ef444422; color:var(--red); }
  .btn {
    background:var(--accent); color:#061018; border:none; padding:.5rem 1rem;
    border-radius:8px; cursor:pointer; font-weight:700; font-size:.9rem;
  }
  .btn:hover { filter:brightness(1.08); }
  .btn.ghost { background:transparent; color:var(--accent); border:1px solid var(--accent); }
  .container { max-width:1280px; margin:0 auto; padding:1rem 1.1rem 2.5rem; }
  .tabs { display:flex; gap:.4rem; flex-wrap:wrap; margin:0 0 1.1rem; border-bottom:1px solid var(--border); padding-bottom:.5rem; }
  .tab {
    background:transparent; border:1px solid transparent; color:var(--muted);
    padding:.45rem .85rem; border-radius:8px; cursor:pointer; font-size:.88rem; font-weight:600;
  }
  .tab:hover { color:var(--text); background:#1a2438; }
  .tab.active { color:var(--accent); background:#1a2a40; border-color:#2a4060; }
  .panel { display:none; }
  .panel.active { display:block; }
  .grid { display:grid; grid-template-columns:repeat(auto-fit,minmax(160px,1fr)); gap:.85rem; margin-bottom:1.2rem; }
  .card {
    background:var(--card); border:1px solid var(--border); border-radius:12px; padding:1rem;
  }
  .card h3 { font-size:.78rem; color:var(--muted); margin-bottom:.35rem; font-weight:600; }
  .card .val { font-size:1.45rem; font-weight:800; }
  .section { margin-bottom:1.4rem; }
  .section-title {
    display:flex; align-items:center; justify-content:space-between; gap:.75rem;
    margin-bottom:.7rem; flex-wrap:wrap;
  }
  .section-title h2 { font-size:1.05rem; color:var(--accent); font-weight:700; }
  table { width:100%; border-collapse:collapse; font-size:.88rem; }
  th, td { padding:.55rem .65rem; text-align:right; border-bottom:1px solid var(--border); }
  th { color:var(--muted); font-weight:600; font-size:.8rem; }
  tr:hover td { background:#1a243888; }
  .emoji { font-size:1.05rem; }
  .empty { color:var(--muted); padding:1.2rem; text-align:center; font-size:.92rem; }
  #status { font-size:.8rem; color:var(--muted); margin-bottom:.8rem; }
  .chart-box { background:var(--card); border:1px solid var(--border); border-radius:12px; padding:.5rem; min-height:320px; }
  .row2 { display:grid; grid-template-columns:1.4fr 1fr; gap:1rem; }
  @media (max-width:900px) { .row2 { grid-template-columns:1fr; } }
  select, input[type=text] {
    background:var(--card2); color:var(--text); border:1px solid var(--border);
    border-radius:8px; padding:.4rem .65rem; font-size:.88rem;
  }
  .controls { display:flex; gap:.5rem; flex-wrap:wrap; align-items:center; }
  .signal-card {
    background:var(--card); border:1px solid var(--border); border-radius:12px;
    padding:.9rem 1rem; margin-bottom:.6rem;
  }
  .signal-card .top { display:flex; justify-content:space-between; align-items:center; gap:.5rem; margin-bottom:.4rem; }
  .signal-card .sym { font-weight:800; font-size:1.05rem; }
  .signal-card .meta { display:grid; grid-template-columns:repeat(auto-fit,minmax(90px,1fr)); gap:.35rem; font-size:.85rem; color:var(--muted); }
  .signal-card .meta b { color:var(--text); }
  .buy { color:var(--green); }
  .sell { color:var(--red); }
  .report-box {
    background:var(--card); border:1px solid var(--border); border-radius:12px;
    padding:1rem; margin-bottom:.75rem; white-space:pre-wrap; font-size:.88rem; line-height:1.55;
    max-height:280px; overflow:auto;
  }
  .report-meta { font-size:.8rem; color:var(--muted); margin-bottom:.5rem; }
  .hint { font-size:.82rem; color:var(--muted); margin-top:.4rem; }
</style>
</head>
<body>
<header>
  <h1>📈 Cloud Trading AI</h1>
  <div style="display:flex;align-items:center;gap:.7rem;flex-wrap:wrap;">
    <span class="badge" id="fs-badge">Firestore …</span>
    <span class="badge" id="ctrader-badge">cTrader …</span>
    <span class="badge warn" id="mode-badge" style="display:none;"></span>
    <button class="btn" onclick="loadAll()">تحديث 🔄</button>
  </div>
</header>

<div class="container">
  <p id="status">جاري التحميل…</p>

  <div class="tabs" id="tabs">
    <button class="tab active" data-tab="overview">🏠 العامة</button>
    <button class="tab" data-tab="signals">🎯 التوصيات الحية</button>
    <button class="tab" data-tab="chart">📈 الرسم + الشموع</button>
    <button class="tab" data-tab="compare">📊 مقارنة الأزواج</button>
    <button class="tab" data-tab="reports">🤖 تقارير AI</button>
  </div>

  <!-- ===== العامة ===== -->
  <div class="panel active" id="panel-overview">
    <div class="grid" id="kpis"></div>
    <div class="grid" id="account-kpis"></div>
    <div class="row2">
      <div class="section">
        <div class="section-title"><h2>📋 الصفقات المفتوحة (السجل)</h2></div>
        <div class="card" id="open-trades"><div class="empty">لا توجد صفقات مفتوحة</div></div>
      </div>
      <div class="section">
        <div class="section-title"><h2>📊 الصفقات المغلقة</h2></div>
        <div class="card" id="closed-trades"><div class="empty">لا توجد صفقات مغلقة</div></div>
      </div>
    </div>
    <div class="section">
      <div class="section-title"><h2>⚡ الصفقات الحية على cTrader</h2></div>
      <div class="card" id="live-positions"><div class="empty">—</div></div>
    </div>
    <div class="section">
      <div class="section-title"><h2>⚙️ حالة التشغيل</h2></div>
      <div class="card" id="runtime"><div class="empty">—</div></div>
    </div>
  </div>

  <!-- ===== التوصيات الحية ===== -->
  <div class="panel" id="panel-signals">
    <div class="section">
      <div class="section-title">
        <h2>🎯 التوصيات الحية</h2>
        <span class="hint">Entry / SL / TP مع اتجاه كل زوج</span>
      </div>
      <div id="signals-list"><div class="empty">لا توجد إشارات حالياً — شغّل تحليلاً من تيليجرام</div></div>
    </div>
    <div class="section">
      <div class="section-title"><h2>📍 نقاط التوصيات (مستويات السعر)</h2></div>
      <div class="card" id="levels-table"><div class="empty">—</div></div>
    </div>
  </div>

  <!-- ===== الرسم + الشموع ===== -->
  <div class="panel" id="panel-chart">
    <div class="section">
      <div class="section-title">
        <h2>🕯️ الشموع اليابانية + نقاط التوصية</h2>
        <div class="controls">
          <select id="chart-symbol"></select>
          <select id="chart-tf">
            <option value="M15">M15</option>
            <option value="H1" selected>H1</option>
            <option value="H4">H4</option>
            <option value="D1">D1</option>
          </select>
          <button class="btn ghost" onclick="loadChart()">تحميل</button>
        </div>
      </div>
      <div class="chart-box" id="candle-chart"></div>
      <p class="hint" id="chart-hint">الشموع تُجلب من BigQuery إن فُعِّل وإلا من Firestore — مع مستويات التوصية الحالية.</p>
    </div>
  </div>

  <!-- ===== مقارنة الأزواج ===== -->
  <div class="panel" id="panel-compare">
    <div class="section">
      <div class="section-title"><h2>📊 مقارنة الأزواج (آخر الإشارات)</h2></div>
      <div class="chart-box" id="compare-chart"></div>
    </div>
    <div class="section">
      <div class="section-title"><h2>جدول المقارنة</h2></div>
      <div class="card" id="compare-table"><div class="empty">—</div></div>
    </div>
  </div>

  <!-- ===== تقارير AI ===== -->
  <div class="panel" id="panel-reports">
    <div class="section">
      <div class="section-title"><h2>🤖 تقارير AI المخزنة</h2></div>
      <div id="reports-list"><div class="empty">لا توجد تقارير بعد — نفّذ تحليلاً من تيليجرام</div></div>
    </div>
  </div>
</div>

<script>
// ---- Tabs ----
document.querySelectorAll('.tab').forEach(btn => {
  btn.addEventListener('click', () => {
    document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
    document.querySelectorAll('.panel').forEach(p => p.classList.remove('active'));
    btn.classList.add('active');
    document.getElementById('panel-' + btn.dataset.tab).classList.add('active');
    if (btn.dataset.tab === 'chart') loadChart();
    if (btn.dataset.tab === 'compare') renderCompare();
  });
});

let STATE = { signals: [], open: [], closed: [], runtime: {}, reports: [], ohlc: [], status: {} };
let NEXT_ANALYSIS_AT = null;
let LAST_LOAD_AT = '—';

function esc(s) {
  return String(s ?? '').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');
}

function fmtNum(v, d = 2) {
  const n = Number(v);
  if (v == null || isNaN(n)) return '—';
  return n.toLocaleString('en-US', { minimumFractionDigits: d, maximumFractionDigits: d });
}

function timeAgo(iso) {
  if (!iso) return '—';
  const t = new Date(iso);
  if (isNaN(t)) return '—';
  const secs = Math.max(0, Math.round((Date.now() - t.getTime()) / 1000));
  if (secs < 60) return `قبل ${secs} ث`;
  if (secs < 3600) return `قبل ${Math.round(secs / 60)} د`;
  return `قبل ${Math.round(secs / 3600)} س`;
}

function dirClass(d) {
  const x = (d || '').toLowerCase();
  if (x.includes('buy') || x.includes('long') || x === 'شراء') return 'buy';
  if (x.includes('sell') || x.includes('short') || x === 'بيع') return 'sell';
  return '';
}

async function loadAll() {
  document.getElementById('status').textContent = 'جاري التحديث…';
  try {
    const [dash, sigs, reps, st] = await Promise.all([
      fetch('/api/dashboard').then(r => r.json()),
      fetch('/api/signals').then(r => r.json()).catch(() => ({signals:[]})),
      fetch('/api/reports').then(r => r.json()).catch(() => ({reports:[]})),
      fetch('/api/status').then(r => r.json()).catch(() => ({})),
    ]);

    STATE.signals = sigs.signals || dash.latest_signals || [];
    STATE.open = dash.open_trades || [];
    STATE.closed = dash.recent_closed || [];
    STATE.runtime = dash.runtime || {};
    STATE.reports = reps.reports || [];
    STATE.status = st || {};

    const fsOk = !!dash.firestore_ok;
    const badge = document.getElementById('fs-badge');
    badge.textContent = fsOk ? 'Firestore ✅' : 'Firestore ❌';
    badge.className = 'badge ' + (fsOk ? 'ok' : 'bad');

    renderStatusBadges();
    renderKPIs();
    renderAccountKPIs();
    renderTrades();
    renderLivePositions();
    renderRuntime();
    renderSignals();
    renderLevels();
    fillSymbolSelect();
    renderReports();
    renderCompare();

    document.getElementById('status').textContent =
      'آخر تحديث: ' + new Date().toLocaleTimeString('ar') + ' · التحديث القادم خلال —';
    LAST_LOAD_AT = new Date().toLocaleTimeString('ar');
  } catch (e) {
    document.getElementById('status').textContent = 'خطأ: ' + e.message;
  }
}

function renderStatusBadges() {
  const st = STATE.status || {};
  const cb = document.getElementById('ctrader-badge');
  if (st.available === false) {
    cb.textContent = 'cTrader ⚠️';
    cb.className = 'badge warn';
  } else {
    const ok = !!st.ctrader_connected;
    cb.textContent = ok ? 'cTrader ✅' : 'cTrader ❌';
    cb.className = 'badge ' + (ok ? 'ok' : 'bad');
  }

  const mb = document.getElementById('mode-badge');
  const iv = (st.intervals || {});
  if (st.available === false || iv.analysis_minutes == null) {
    mb.style.display = 'none';
    NEXT_ANALYSIS_AT = null;
    return;
  }
  const cheap = !!st.cheap_mode;
  mb.textContent = cheap
    ? `💸 وضع اقتصادي · تحليل كل ${iv.analysis_minutes} د`
    : `🚀 always-on · تحليل كل ${iv.analysis_minutes} د · OHLC ${iv.ohlc}`;
  mb.className = 'badge ' + (cheap ? 'warn' : 'ok');
  mb.style.display = '';
  NEXT_ANALYSIS_AT = Date.now() + iv.analysis_minutes * 60 * 1000;
}

function tickCountdown() {
  if (!NEXT_ANALYSIS_AT) return;
  const left = Math.max(0, NEXT_ANALYSIS_AT - Date.now());
  const m = Math.floor(left / 60000);
  const s = Math.floor((left % 60000) / 1000);
  document.getElementById('status').textContent =
    'آخر تحديث: ' + LAST_LOAD_AT + ' · التحليل القادم خلال ' + m + ':' + String(s).padStart(2, '0');
}

function renderAccountKPIs() {
  const st = STATE.status || {};
  const a = st.account || {};
  if (st.available === false || a.balance == null) {
    document.getElementById('account-kpis').innerHTML = '';
    return;
  }
  const equity = Number(a.equity || 0);
  const balance = Number(a.balance || 0);
  const pnl = equity - balance;
  document.getElementById('account-kpis').innerHTML = `
    <div class="card"><h3>الرصيد (${esc(a.deposit_asset || 'USD')})</h3><div class="val">${fmtNum(balance)}</div></div>
    <div class="card"><h3>حقوق الملكية</h3><div class="val">${fmtNum(equity)}</div></div>
    <div class="card"><h3>الربح/الخسارة العائمة</h3><div class="val" style="color:${pnl>=0?'#22c55e':'#ef4444'}">${pnl>=0?'+':''}${fmtNum(pnl)}</div></div>
    <div class="card"><h3>الهامش المتاح</h3><div class="val">${fmtNum(a.free_margin)}</div></div>
    <div class="card"><h3>مستوى الهامش %</h3><div class="val">${fmtNum(a.margin_level, 1)}</div></div>
    <div class="card"><h3>الرافعة</h3><div class="val">${a.leverage != null ? '1:' + a.leverage : '—'}</div></div>
    <div class="card"><h3>صفقات cTrader مفتوحة</h3><div class="val">${(st.positions || []).length}</div></div>
    <div class="card"><h3>آخر تحديث للحساب</h3><div class="val" style="font-size:1rem;">${timeAgo(a.updated_at)}</div></div>
  `;
}

function renderLivePositions() {
  const st = STATE.status || {};
  const pos = st.positions || [];
  const el = document.getElementById('live-positions');
  if (st.available === false) {
    el.innerHTML = '<div class="empty">حالة cTrader غير متاحة</div>';
    return;
  }
  if (!pos.length) {
    el.innerHTML = '<div class="empty">لا توجد صفقات مفتوحة على cTrader</div>';
    return;
  }
  el.innerHTML = `<table>
    <thead><tr><th>الزوج</th><th>الاتجاه</th><th>الحجم</th><th>الدخول</th><th>السعر الحالي</th><th>SL</th><th>TP</th><th>PnL</th><th>#</th></tr></thead>
    <tbody>${pos.map(p => {
      const pnl = Number(p.pnl ?? p.profit ?? 0);
      const dir = (p.side || p.direction || '').toString().toLowerCase();
      const dcls = dir.includes('buy') || dir.includes('long') ? 'buy' : (dir.includes('sell') || dir.includes('short') ? 'sell' : '');
      const dlabel = dcls === 'buy' ? 'شراء' : (dcls === 'sell' ? 'بيع' : esc(p.side || p.direction || '—'));
      return `<tr>
        <td><b>${esc(p.symbol || p.symbolName)}</b></td>
        <td class="${dcls}">${dlabel}</td>
        <td>${fmtNum(p.volume ?? p.lots, 2)}</td>
        <td>${fmtNum(p.entryPrice ?? p.open_price, 5)}</td>
        <td>${fmtNum(p.currentPrice ?? p.bid, 5)}</td>
        <td>${p.stopLoss != null ? fmtNum(p.stopLoss, 5) : '—'}</td>
        <td>${p.takeProfit != null ? fmtNum(p.takeProfit, 5) : '—'}</td>
        <td style="color:${pnl>=0?'#22c55e':'#ef4444'}">${pnl>=0?'+':''}${fmtNum(pnl)}</td>
        <td>${esc(p.positionId ?? p.id ?? '—')}</td>
      </tr>`;
    }).join('')}</tbody></table>`;
}

function renderKPIs() {
  const open = STATE.open, closed = STATE.closed, rt = STATE.runtime;
  const totalPnl = closed.reduce((s,t) => s + (parseFloat(t.realized_pnl)||0), 0);
  const wins = closed.filter(t => (parseFloat(t.realized_pnl)||0) > 0).length;
  const wr = closed.length ? (wins / closed.length * 100).toFixed(1) : '0';
  const strong = STATE.signals.filter(s => s.emoji === '🟢' || s.emoji === '🔴').length;

  document.getElementById('kpis').innerHTML = `
    <div class="card"><h3>صفقات مفتوحة</h3><div class="val">${open.length}</div></div>
    <div class="card"><h3>صفقات مغلقة</h3><div class="val">${closed.length}</div></div>
    <div class="card"><h3>صافي الربح ($)</h3><div class="val" style="color:${totalPnl>=0?'#22c55e':'#ef4444'}">${totalPnl.toFixed(2)}</div></div>
    <div class="card"><h3>نسبة النجاح</h3><div class="val">${wr}%</div></div>
    <div class="card"><h3>إشارات قوية</h3><div class="val">${strong}</div></div>
    <div class="card"><h3>التداول الآلي</h3><div class="val">${rt.auto_trading ? '🟢 مفعّل' : '🔴 متوقف'}</div></div>
    <div class="card"><h3>إطار التنفيذ</h3><div class="val">${esc(rt.trading_tf || '—')}</div></div>
    <div class="card"><h3>عدد الإشارات</h3><div class="val">${STATE.signals.length}</div></div>
  `;
}

function renderTrades() {
  const open = STATE.open, closed = STATE.closed;
  if (open.length) {
    document.getElementById('open-trades').innerHTML = `<table>
      <thead><tr><th>الزوج</th><th>الاتجاه</th><th>الدخول</th><th>وقف</th><th>هدف</th><th>الحالة</th></tr></thead>
      <tbody>${open.map(t => `<tr>
        <td><b>${esc(t.symbol)}</b></td>
        <td class="${dirClass(t.direction)}">${esc(t.direction)}</td>
        <td>${t.entry_price ?? '—'}</td>
        <td>${t.sl ?? '—'}</td>
        <td>${t.tp1 ?? '—'}</td>
        <td>${esc(t.status)}</td>
      </tr>`).join('')}</tbody></table>`;
  } else {
    document.getElementById('open-trades').innerHTML = '<div class="empty">لا توجد صفقات مفتوحة</div>';
  }

  if (closed.length) {
    document.getElementById('closed-trades').innerHTML = `<table>
      <thead><tr><th>الزوج</th><th>الاتجاه</th><th>PnL</th><th>الحالة</th></tr></thead>
      <tbody>${closed.map(t => {
        const pnl = parseFloat(t.realized_pnl)||0;
        return `<tr>
          <td><b>${esc(t.symbol)}</b></td>
          <td class="${dirClass(t.direction)}">${esc(t.direction)}</td>
          <td style="color:${pnl>=0?'#22c55e':'#ef4444'}">${pnl.toFixed(2)}</td>
          <td>${esc(t.status)}</td>
        </tr>`;
      }).join('')}</tbody></table>`;
  } else {
    document.getElementById('closed-trades').innerHTML = '<div class="empty">لا توجد صفقات مغلقة بعد</div>';
  }
}

function renderRuntime() {
  const rt = STATE.runtime || {};
  if (!Object.keys(rt).length) {
    document.getElementById('runtime').innerHTML = '<div class="empty">لم تُحفظ حالة بعد — غيّر إعداداً من تيليجرام</div>';
    return;
  }
  document.getElementById('runtime').innerHTML = `<pre style="white-space:pre-wrap;font-size:.85rem;line-height:1.5;">${esc(JSON.stringify(rt, null, 2))}</pre>`;
}

function renderSignals() {
  const list = STATE.signals || [];
  if (!list.length) {
    document.getElementById('signals-list').innerHTML = '<div class="empty">لا توجد إشارات حالياً — شغّل تحليلاً من تيليجرام</div>';
    return;
  }
  document.getElementById('signals-list').innerHTML = list.map(s => `
    <div class="signal-card">
      <div class="top">
        <span class="sym">${esc(s.emoji||'')} ${esc(s.symbol||'')}</span>
        <span class="${dirClass(s.direction)}"><b>${esc(s.direction||'')}</b></span>
      </div>
      <div class="meta">
        <div>الدخول: <b>${esc(s.entry||'—')}</b></div>
        <div>وقف: <b>${esc(s.sl||'—')}</b></div>
        <div>هدف 1: <b>${esc(s.tp1||'—')}</b></div>
        <div>هدف 2: <b>${esc(s.tp2||'—')}</b></div>
        <div>R:R <b>${esc(s.rr||'—')}</b></div>
        <div>الإطار: <b>${esc(s.primary_tf||'—')}</b></div>
        <div>RSI: <b>${s.rsi_14 != null ? Number(s.rsi_14).toFixed(1) : '—'}</b></div>
        <div>ATR: <b>${s.atr_14 != null ? Number(s.atr_14).toFixed(5) : '—'}</b></div>
      </div>
    </div>
  `).join('');
}

function renderLevels() {
  const list = STATE.signals || [];
  if (!list.length) {
    document.getElementById('levels-table').innerHTML = '<div class="empty">لا توجد مستويات</div>';
    return;
  }
  document.getElementById('levels-table').innerHTML = `<table>
    <thead><tr>
      <th></th><th>الزوج</th><th>الاتجاه</th><th>Entry</th><th>SL</th><th>TP1</th><th>TP2</th><th>الإطار</th>
    </tr></thead>
    <tbody>${list.map(s => `<tr>
      <td class="emoji">${esc(s.emoji||'')}</td>
      <td><b>${esc(s.symbol)}</b></td>
      <td class="${dirClass(s.direction)}">${esc(s.direction)}</td>
      <td>${esc(s.entry||'—')}</td>
      <td>${esc(s.sl||'—')}</td>
      <td>${esc(s.tp1||'—')}</td>
      <td>${esc(s.tp2||'—')}</td>
      <td>${esc(s.primary_tf||'—')}</td>
    </tr>`).join('')}</tbody>
  </table>`;
}

function fillSymbolSelect() {
  const sel = document.getElementById('chart-symbol');
  const symbols = [...new Set((STATE.signals||[]).map(s => s.symbol).filter(Boolean))];
  if (!symbols.length) symbols.push('XAUUSD', 'EURUSD', 'GBPUSD');
  const cur = sel.value;
  sel.innerHTML = symbols.map(s => `<option value="${esc(s)}">${esc(s)}</option>`).join('');
  if (cur && symbols.includes(cur)) sel.value = cur;
}

async function loadChart() {
  const symbol = document.getElementById('chart-symbol').value || 'XAUUSD';
  const tf = document.getElementById('chart-tf').value || 'H1';
  const hint = document.getElementById('chart-hint');
  const el = document.getElementById('candle-chart');

  let ohlc = [];
  try {
    const res = await fetch(`/api/ohlc?symbol=${encodeURIComponent(symbol)}&timeframe=${encodeURIComponent(tf)}&limit=120`);
    const data = await res.json();
    ohlc = data.bars || [];
    hint.textContent = data.source
      ? `المصدر: ${data.source} · ${ohlc.length} شمعة`
      : 'لا توجد بيانات OHLC — فعّل BigQuery أو انتظر مزامنة الشموع';
  } catch (e) {
    hint.textContent = 'تعذر جلب OHLC: ' + e.message;
  }

  const sig = (STATE.signals || []).find(s => (s.symbol || '').toUpperCase() === symbol.toUpperCase());

  // شموع
  const times = ohlc.map(b => b.bar_time || b.time);
  const traceCandle = {
    x: times,
    open: ohlc.map(b => b.open),
    high: ohlc.map(b => b.high),
    low: ohlc.map(b => b.low),
    close: ohlc.map(b => b.close),
    type: 'candlestick',
    name: symbol,
    increasing: {line: {color: '#22c55e'}},
    decreasing: {line: {color: '#ef4444'}},
  };

  const shapes = [];
  const annotations = [];
  function addLevel(price, color, label) {
    if (price == null || price === '' || isNaN(Number(price))) return;
    const p = Number(price);
    shapes.push({
      type: 'line', xref: 'paper', x0: 0, x1: 1,
      y0: p, y1: p, line: {color, width: 1.5, dash: 'dash'}
    });
    annotations.push({
      xref: 'paper', x: 1.01, y: p, xanchor: 'left',
      text: label + ' ' + p, showarrow: false,
      font: {color, size: 11}
    });
  }

  if (sig) {
    addLevel(sig.entry, '#38bdf8', 'Entry');
    addLevel(sig.sl, '#ef4444', 'SL');
    addLevel(sig.tp1, '#22c55e', 'TP1');
    addLevel(sig.tp2, '#eab308', 'TP2');
  }

  const layout = {
    paper_bgcolor: '#151d2e',
    plot_bgcolor: '#151d2e',
    font: {color: '#e8eef7', size: 11},
    margin: {t: 30, r: 90, b: 40, l: 50},
    xaxis: {rangeslider: {visible: false}, gridcolor: '#243044'},
    yaxis: {gridcolor: '#243044', side: 'right'},
    shapes,
    annotations,
    showlegend: false,
    height: 420,
  };

  if (!ohlc.length && sig) {
    // لا شموع — نرسم مستويات فقط كخطوط أفقية على محور وهمي
    const prices = [sig.entry, sig.sl, sig.tp1, sig.tp2].map(Number).filter(n => !isNaN(n));
    if (prices.length) {
      const mid = prices.reduce((a,b)=>a+b,0)/prices.length;
      Plotly.newPlot(el, [{
        x: [0, 1], y: [mid, mid], mode: 'lines', line: {color: 'transparent'},
        hoverinfo: 'skip'
      }], layout, {responsive: true, displayModeBar: false});
      return;
    }
  }

  if (!ohlc.length) {
    el.innerHTML = '<div class="empty">لا توجد شموع للعرض بعد — انتظر مزامنة OHLC من cTrader.</div>';
    return;
  }

  Plotly.newPlot(el, [traceCandle], layout, {responsive: true, displayModeBar: false});
}

function renderCompare() {
  const list = STATE.signals || [];
  const el = document.getElementById('compare-chart');
  const table = document.getElementById('compare-table');

  if (!list.length) {
    el.innerHTML = '<div class="empty">لا توجد إشارات للمقارنة</div>';
    table.innerHTML = '<div class="empty">—</div>';
    return;
  }

  // مقارنة RSI بين الأزواج
  const symbols = list.map(s => s.symbol || '');
  const rsi = list.map(s => s.rsi_14 != null ? Number(s.rsi_14) : null);
  const atr = list.map(s => s.atr_14 != null ? Number(s.atr_14) : null);

  const traces = [{
    x: symbols,
    y: rsi,
    type: 'bar',
    name: 'RSI(14)',
    marker: {color: rsi.map(v => v == null ? '#64748b' : (v >= 70 ? '#ef4444' : (v <= 30 ? '#22c55e' : '#38bdf8')))}
  }];

  Plotly.newPlot(el, traces, {
    paper_bgcolor: '#151d2e', plot_bgcolor: '#151d2e',
    font: {color: '#e8eef7', size: 11},
    margin: {t: 20, r: 20, b: 60, l: 40},
    yaxis: {title: 'RSI', gridcolor: '#243044', range: [0, 100]},
    xaxis: {gridcolor: '#243044'},
    height: 320,
    showlegend: false,
  }, {responsive: true, displayModeBar: false});

  table.innerHTML = `<table>
    <thead><tr><th>الزوج</th><th>الاتجاه</th><th>RSI</th><th>EMA20</th><th>EMA50</th><th>ATR</th><th>Entry</th><th>SL</th><th>TP1</th></tr></thead>
    <tbody>${list.map(s => `<tr>
      <td><b>${esc(s.symbol)}</b></td>
      <td class="${dirClass(s.direction)}">${esc(s.direction)}</td>
      <td>${s.rsi_14 != null ? Number(s.rsi_14).toFixed(1) : '—'}</td>
      <td>${s.ema_20 != null ? Number(s.ema_20).toFixed(5) : '—'}</td>
      <td>${s.ema_50 != null ? Number(s.ema_50).toFixed(5) : '—'}</td>
      <td>${s.atr_14 != null ? Number(s.atr_14).toFixed(5) : '—'}</td>
      <td>${esc(s.entry||'—')}</td>
      <td>${esc(s.sl||'—')}</td>
      <td>${esc(s.tp1||'—')}</td>
    </tr>`).join('')}</tbody>
  </table>`;
}

function renderReports() {
  const list = STATE.reports || [];
  if (!list.length) {
    document.getElementById('reports-list').innerHTML = '<div class="empty">لا توجد تقارير مخزنة بعد</div>';
    return;
  }
  document.getElementById('reports-list').innerHTML = list.map(r => {
    const when = r.created_at ? new Date(r.created_at).toLocaleString('ar') : '—';
    return `<div class="report-box">
      <div class="report-meta">
        <b>${esc(r.analysis_type || 'report')}</b> ·
        ${esc(r.symbols || r.symbols_key || '')} ·
        ${esc(r.timeframes || r.timeframes_key || '')} ·
        ${esc(when)}
      </div>
      ${esc((r.report_text || '').slice(0, 4000))}
    </div>`;
  }).join('');
}

loadAll();
setInterval(loadAll, 20000);
setInterval(tickCountdown, 1000);
</script>
</body>
</html>
"""


def _serialize(obj: Any) -> Any:
    """تحويل قيم Firestore/datetime لتكون JSON-safe."""
    if obj is None:
        return None
    if isinstance(obj, datetime):
        if obj.tzinfo is None:
            obj = obj.replace(tzinfo=timezone.utc)
        return obj.isoformat()
    if isinstance(obj, dict):
        return {k: _serialize(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_serialize(x) for x in obj]
    # Firestore timestamp-like
    if hasattr(obj, "isoformat"):
        try:
            return obj.isoformat()
        except Exception:
            pass
    return obj


async def _build_summary() -> Dict[str, Any]:
    import db

    summary: Dict[str, Any] = {
        "runtime": {},
        "open_trades": [],
        "recent_closed": [],
        "latest_signals": [],
        "firestore_ok": False,
    }
    try:
        if getattr(db, "USE_FIRESTORE", False) and getattr(db, "fs", None):
            raw = await db.fs.get_dashboard_summary()
            summary = _serialize(raw)
            return summary
    except Exception as e:
        logger.error(f"dashboard firestore summary error: {e}")

    try:
        summary["runtime"] = _serialize(await db.load_runtime_state())
        summary["open_trades"] = _serialize(await db.get_open_journal_trades())
    except Exception:
        pass
    return summary


async def _get_signals() -> List[Dict[str, Any]]:
    import db

    try:
        if getattr(db, "USE_FIRESTORE", False) and getattr(db, "fs", None):
            rows = await db.fs.get_symbol_signals(limit=50)
            return _serialize(rows)
    except Exception as e:
        logger.error(f"get signals error: {e}")

    try:
        # fallback: empty when no firestore
        return []
    except Exception:
        return []


async def _get_reports(limit: int = 10) -> List[Dict[str, Any]]:
    import db

    try:
        if getattr(db, "USE_FIRESTORE", False) and getattr(db, "fs", None):
            fs = db.fs
            client = fs._get_db()
            if not client:
                return []
            q = (
                client.collection("ai_reports")
                .order_by("created_at", direction="DESCENDING")
                .limit(limit)
            )
            return _serialize([doc.to_dict() or {} for doc in q.stream()])
    except Exception as e:
        logger.error(f"get reports error: {e}")
    return []


async def _get_ohlc(symbol: str, timeframe: str, limit: int = 120) -> Dict[str, Any]:
    """BigQuery أولاً (إن فُعِّل)، ثم Firestore كاحتياط — لا PostgreSQL بعد الآن."""
    symbol = (symbol or "XAUUSD").upper().strip()
    timeframe = (timeframe or "H1").upper().strip()
    bars: List[Dict[str, Any]] = []
    source = None

    # BigQuery (البيانات الثقيلة)
    try:
        import bq as bq_sink

        if getattr(bq_sink, "BIGQUERY_ENABLED", False):
            client = bq_sink._get_client()
            if client:
                from google.cloud import bigquery  # type: ignore

                table = f"{bq_sink.BIGQUERY_PROJECT}.{bq_sink.BIGQUERY_DATASET}.ohlc_bars"
                sql = f"""
                    SELECT bar_time, open, high, low, close, volume
                    FROM `{table}`
                    WHERE symbol = @symbol AND timeframe = @tf
                    ORDER BY bar_time DESC
                    LIMIT @lim
                """
                job_config = bigquery.QueryJobConfig(
                    query_parameters=[
                        bigquery.ScalarQueryParameter("symbol", "STRING", symbol),
                        bigquery.ScalarQueryParameter("tf", "STRING", timeframe),
                        bigquery.ScalarQueryParameter("lim", "INT64", int(limit)),
                    ]
                )
                rows = list(client.query(sql, job_config=job_config).result())
                bars = [
                    {
                        "bar_time": r.bar_time.isoformat() if hasattr(r.bar_time, "isoformat") else str(r.bar_time),
                        "open": r.open,
                        "high": r.high,
                        "low": r.low,
                        "close": r.close,
                        "volume": r.volume,
                    }
                    for r in reversed(rows)
                ]
                if bars:
                    source = "bigquery"
    except Exception as e:
        logger.warning(f"OHLC BigQuery fetch failed: {e}")

    # Firestore fallback
    if not bars:
        try:
            import db

            rows = await db.get_ohlc_bars(symbol, timeframe, limit=limit)
            for r in rows or []:
                bt = r.get("bar_time")
                if hasattr(bt, "isoformat"):
                    bt = bt.isoformat()
                bars.append(
                    {
                        "bar_time": bt,
                        "open": r.get("open"),
                        "high": r.get("high"),
                        "low": r.get("low"),
                        "close": r.get("close"),
                        "volume": r.get("volume"),
                    }
                )
            if bars:
                source = "firestore"
        except Exception as e:
            logger.warning(f"OHLC Firestore fetch failed: {e}")

    return {"symbol": symbol, "timeframe": timeframe, "bars": bars, "source": source}


async def _get_technical(symbol: str, timeframe: str, limit: int = 5) -> List[Dict[str, Any]]:
    """آخر لقطات المؤشرات الفنية لزوج/إطار (RSI, EMA, AATR, MACD...)."""
    symbol = (symbol or "XAUUSD").upper().strip()
    timeframe = (timeframe or "H1").upper().strip()
    try:
        import db

        return _serialize(await db.get_technical_history(symbol, timeframe, limit=limit))
    except Exception as e:
        logger.warning(f"technical history fetch failed: {e}")
        return []


def _live_status() -> Dict[str, Any]:
    """حالة cTrader + وضع التشغيل الحيّ من main (استيراد داخلي لتفادي الاستيراد الدائري)."""
    import sys

    # نفضّل الوحدة قيد التشغيل فعلياً (python main.py → __main__) لتفادي
    # إعادة تنفيذ main.py كوحدة منفصلة. وإن استُورد المشروع كحزمة نعود لـ import main.
    _main = sys.modules.get("__main__")
    if _main is None or not hasattr(_main, "ctrader_account_info"):
        try:
            import main as _main
        except Exception as e:
            logger.warning(f"live status: main module unavailable: {e}")
            return {"available": False}

    try:
        info = dict(getattr(_main, "ctrader_account_info", {}) or {})
        pos = list(getattr(_main, "active_positions", []) or [])
        cheap = bool(getattr(_main, "runtime_cheap_mode", True))
        return {
            "available": True,
            "ctrader_connected": bool(getattr(_main, "is_ctrader_connected", False)),
            "cheap_mode": cheap,
            "auto_trading_enabled": bool(getattr(_main, "auto_trading_enabled", False)),
            "account": {
                "balance": info.get("balance"),
                "equity": info.get("equity"),
                "margin": info.get("margin"),
                "free_margin": info.get("free_margin"),
                "margin_level": info.get("margin_level"),
                "leverage": info.get("leverage"),
                "deposit_asset": info.get("deposit_asset"),
                "updated_at": info.get("updated_at"),
            },
            "positions": _serialize(pos),
            "intervals": {
                "analysis_minutes": (
                    int(getattr(_main, "AUTO_ANALYSIS_INTERVAL_MINUTES", 10))
                    if cheap
                    else int(getattr(_main, "NON_CHEAP_ANALYSIS_INTERVAL_MINUTES", 5))
                ),
                "ohlc": (
                    f"{max(5, int(getattr(_main, 'OHLC_REFRESH_MINUTES', 15)))}m"
                    if cheap
                    else f"{int(getattr(_main, 'NON_CHEAP_OHLC_REFRESH_SECONDS', 60))}s"
                ),
                "account_refresh_seconds": int(getattr(_main, "CTRADER_ACCOUNT_REFRESH_SECONDS", 20)),
            },
        }
    except Exception as e:
        logger.error(f"live status error: {e}")
        return {"available": False, "error": str(e)}


def register_dashboard(app: FastAPI) -> None:
    """تسجيل مسارات اللوحة على تطبيق FastAPI."""

    @app.get("/dashboard", response_class=HTMLResponse)
    async def web_dashboard():
        return HTMLResponse(content=DASHBOARD_HTML)

    @app.get("/api/dashboard")
    async def api_dashboard():
        try:
            data = await _build_summary()
            return JSONResponse(content=data)
        except Exception as e:
            logger.error(f"api_dashboard error: {e}")
            return JSONResponse(
                content={
                    "firestore_ok": False,
                    "error": str(e),
                    "runtime": {},
                    "open_trades": [],
                    "recent_closed": [],
                    "latest_signals": [],
                }
            )

    @app.get("/api/signals")
    async def api_signals():
        try:
            signals = await _get_signals()
            return JSONResponse(content={"signals": signals})
        except Exception as e:
            return JSONResponse(content={"signals": [], "error": str(e)})

    @app.get("/api/reports")
    async def api_reports(limit: int = Query(10, ge=1, le=50)):
        try:
            reports = await _get_reports(limit=limit)
            return JSONResponse(content={"reports": reports})
        except Exception as e:
            return JSONResponse(content={"reports": [], "error": str(e)})

    @app.get("/api/ohlc")
    async def api_ohlc(
        symbol: str = Query("XAUUSD"),
        timeframe: str = Query("H1"),
        limit: int = Query(120, ge=10, le=500),
    ):
        try:
            data = await _get_ohlc(symbol, timeframe, limit)
            return JSONResponse(content=data)
        except Exception as e:
            return JSONResponse(content={"bars": [], "error": str(e)})

    @app.get("/api/technical")
    async def api_technical(
        symbol: str = Query("XAUUSD"),
        timeframe: str = Query("H1"),
        limit: int = Query(5, ge=1, le=30),
    ):
        try:
            rows = await _get_technical(symbol, timeframe, limit)
            return JSONResponse(content={"symbol": symbol.upper(), "timeframe": timeframe.upper(), "snapshots": rows})
        except Exception as e:
            return JSONResponse(content={"snapshots": [], "error": str(e)})

    @app.get("/api/status")
    async def api_status():
        """حالة cTrader الحية + وضع التشغيل + أوقات التحديث (من الذاكرة المباشرة)."""
        try:
            return JSONResponse(content=_live_status())
        except Exception as e:
            return JSONResponse(content={"available": False, "error": str(e)})

    @app.get("/api/storage-health")
    async def api_storage_health():
        """اختبار كتابة/قراءة Firestore للتشخيص."""
        import os
        import db

        result: Dict[str, Any] = {
            "use_firestore_flag": getattr(db, "USE_FIRESTORE", False),
            "storage_ready": db.is_storage_ready(),
            "env_USE_FIRESTORE": os.getenv("USE_FIRESTORE"),
            "env_FIRESTORE_PROJECT": os.getenv("FIRESTORE_PROJECT") or os.getenv("GOOGLE_CLOUD_PROJECT"),
            "write_ok": False,
            "read_ok": False,
            "error": None,
            "signals_count": 0,
            "runtime_keys": [],
        }
        try:
            if not getattr(db, "fs", None):
                result["error"] = "firestore_db module not loaded"
                return JSONResponse(content=result)

            fs = db.fs
            client = fs._get_db()
            if client is None:
                result["error"] = "Firestore client is None — check IAM / database exists"
                return JSONResponse(content=result)

            from datetime import datetime, timezone

            ref = client.collection("_health").document("ping")
            ref.set({"ts": datetime.now(timezone.utc), "ok": True})
            result["write_ok"] = True
            result["read_ok"] = bool(ref.get().exists)

            runtime = await fs.load_runtime_state()
            result["runtime_keys"] = list((runtime or {}).keys())
            signals = await fs.get_symbol_signals(limit=20)
            result["signals_count"] = len(signals or [])
            result["sample_symbols"] = [s.get("symbol") for s in (signals or [])[:5]]
        except Exception as e:
            result["error"] = str(e)
            logger.error(f"storage-health error: {e}")

        return JSONResponse(content=result)

    logger.info("✅ Web dashboard registered: /dashboard + APIs + /api/storage-health")
