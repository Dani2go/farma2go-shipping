"""
app.py — Farma2go Shipping P&L Web App
Run with: python app.py
Then open: http://localhost:5000
"""

import os, sys, json, io, traceback
from datetime import datetime
from flask import Flask, request, jsonify, send_file, render_template_string
import pandas as pd

# Add app dir to path
sys.path.insert(0, os.path.dirname(__file__))
from parsers import (parse_ctt, parse_inpost, parse_spring, parse_gls,
                     parse_ups, parse_odoo_sales, parse_shopify_revenue, parse_google_ads)
from engine import save_data, load_data, list_saved, build_pnl, compute_shipping_margin, DATA_DIR
from exporter import generate_pnl_excel, generate_reclamacion_csv

app = Flask(__name__)
app.config['MAX_CONTENT_LENGTH'] = 50 * 1024 * 1024  # 50MB

UPLOADS_DIR = os.path.join(os.path.dirname(__file__), 'uploads')
os.makedirs(UPLOADS_DIR, exist_ok=True)

# ─────────────────────────────────────────────────────────────────
# HTML TEMPLATE
# ─────────────────────────────────────────────────────────────────
HTML = r"""<!DOCTYPE html>
<html lang="es">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Farma2go — Shipping P&L</title>
<link href="https://fonts.googleapis.com/css2?family=Libre+Baskerville:ital,wght@0,400;0,700;1,400&family=DM+Sans:ital,opsz,wght@0,9..40,300;0,9..40,400;0,9..40,500;1,9..40,300&display=swap" rel="stylesheet">
<style>
:root {
  --bg: #f7f5f0;
  --paper: #ffffff;
  --paper2: #faf9f6;
  --bdr: #e2ddd6;
  --bdr2: #d0c9bf;
  --text: #1a1714;
  --sub: #5a524a;
  --muted: #9a8f84;
  --acc: #2d5a8e;
  --acc-lt: #e8f0f9;
  --red: #b93535;
  --red-lt: #fdf0f0;
  --grn: #2a7a4b;
  --grn-lt: #eef7f2;
  --org: #b85c0a;
  --org-lt: #fef4ec;
  --serif: 'Libre Baskerville', Georgia, serif;
  --sans: 'DM Sans', system-ui, sans-serif;
  --mono: 'DM Mono', 'Courier New', monospace;
  --r: 4px;
  --shadow: 0 1px 3px rgba(0,0,0,.08), 0 1px 1px rgba(0,0,0,.04);
  --shadow-md: 0 4px 12px rgba(0,0,0,.08), 0 2px 4px rgba(0,0,0,.04);
}
* { box-sizing: border-box; margin: 0; padding: 0; }
body { background: var(--bg); color: var(--text); font-family: var(--sans); font-size: 13.5px; min-height: 100vh; }

/* HEADER */
header {
  background: var(--paper);
  border-bottom: 1px solid var(--bdr);
  padding: 0 32px;
  height: 56px;
  display: flex;
  align-items: center;
  justify-content: space-between;
  position: sticky; top: 0; z-index: 100;
  box-shadow: var(--shadow);
}
.logo { font-family: var(--serif); font-size: 18px; font-weight: 700; color: var(--text); display: flex; align-items: baseline; gap: 6px; }
.logo .sep { color: var(--bdr2); font-weight: 400; }
.logo .sub { font-size: 13px; font-weight: 400; font-style: italic; color: var(--muted); }
.header-right { display: flex; gap: 8px; align-items: center; }

/* LAYOUT */
.layout { display: grid; grid-template-columns: 260px 1fr; min-height: calc(100vh - 56px); }
.sidebar {
  background: var(--paper);
  border-right: 1px solid var(--bdr);
  padding: 20px 16px;
  display: flex;
  flex-direction: column;
  gap: 16px;
  overflow-y: auto;
}
.main { padding: 24px 28px; background: var(--bg); }

/* SIDEBAR */
.sgroup { display: flex; flex-direction: column; gap: 4px; }
.sgroup-title {
  font-size: 9px; text-transform: uppercase; letter-spacing: 1.8px;
  color: var(--muted); font-family: var(--sans); font-weight: 500;
  padding: 0 4px 6px; border-bottom: 1px solid var(--bdr); margin-bottom: 2px;
}
.carrier-btn {
  width: 100%; padding: 8px 10px; border: 1px solid var(--bdr);
  border-radius: var(--r); background: var(--paper2);
  color: var(--sub); cursor: pointer; font-family: var(--sans);
  font-size: 12px; display: flex; align-items: center; gap: 10px;
  transition: all .12s; text-align: left;
}
.carrier-btn:hover { border-color: var(--acc); background: var(--acc-lt); color: var(--text); }
.carrier-btn.loaded { border-color: var(--grn); background: var(--grn-lt); color: var(--grn); }
.carrier-btn .cb-icon { font-size: 13px; flex-shrink: 0; opacity: .8; }
.carrier-btn .cb-name { font-weight: 500; display: block; }
.carrier-btn .cb-status { font-size: 10px; color: var(--muted); display: block; }
.carrier-btn.loaded .cb-status { color: var(--grn); opacity: .8; }

/* DATA STATUS */
.data-status { display: flex; flex-direction: column; gap: 3px; }
.ds-row { display: flex; align-items: center; gap: 7px; padding: 5px 6px; border-radius: var(--r); font-size: 11px; }
.ds-dot { width: 6px; height: 6px; border-radius: 50%; flex-shrink: 0; }
.ds-dot.ok { background: var(--grn); }
.ds-dot.no { background: var(--bdr2); }
.ds-dot.warn { background: var(--org); }
.ds-label { flex: 1; color: var(--sub); }
.ds-val { color: var(--muted); font-size: 10px; font-family: var(--mono); }

/* BUTTONS */
.btn { padding: 7px 16px; border-radius: var(--r); border: none; cursor: pointer; font-family: var(--sans); font-size: 12.5px; font-weight: 500; transition: all .12s; display: inline-flex; align-items: center; gap: 6px; letter-spacing: .2px; }
.btn-primary { background: var(--acc); color: #fff; }
.btn-primary:hover { background: #234d7a; }
.btn-outline { background: var(--paper); border: 1px solid var(--bdr); color: var(--sub); }
.btn-outline:hover { border-color: var(--acc); color: var(--acc); }
.btn-red { background: var(--red-lt); border: 1px solid #e8b8b8; color: var(--red); }
.btn-red:hover { background: #fbe5e5; }
.btn-grn { background: var(--grn-lt); border: 1px solid #b0dcc0; color: var(--grn); }
.btn-grn:hover { background: #dff2e8; }
.btn-sm { padding: 5px 12px; font-size: 11.5px; }

/* MONTH SELECT */
.month-sel {
  padding: 7px 12px; border-radius: var(--r); border: 1px solid var(--bdr);
  background: var(--paper2); color: var(--text); font-family: var(--sans);
  font-size: 12.5px; outline: none; cursor: pointer;
}
.month-sel:focus { border-color: var(--acc); }

/* TABS */
.tabs-wrap { border-bottom: 1px solid var(--bdr); margin-bottom: 22px; display: flex; gap: 0; }
.tab {
  padding: 9px 18px; font-size: 12.5px; color: var(--muted);
  background: none; border: none; cursor: pointer;
  border-bottom: 2px solid transparent; margin-bottom: -1px;
  transition: all .12s; font-family: var(--sans); font-weight: 400;
}
.tab:hover { color: var(--text); }
.tab.active { color: var(--acc); border-bottom-color: var(--acc); font-weight: 500; }
.tab-panel { display: none; }
.tab-panel.active { display: block; }

/* PAGE TITLE */
.page-header { margin-bottom: 20px; }
.page-header h1 { font-family: var(--serif); font-size: 22px; font-weight: 700; color: var(--text); margin-bottom: 3px; }
.page-header p { font-size: 12px; color: var(--muted); }

/* KPI CARDS */
.kpi-grid { display: grid; grid-template-columns: repeat(4, 1fr); gap: 12px; margin-bottom: 20px; }
.kpi-card {
  background: var(--paper); border: 1px solid var(--bdr);
  border-radius: var(--r); padding: 16px 18px;
  box-shadow: var(--shadow);
}
.kpi-label { font-size: 10px; text-transform: uppercase; letter-spacing: 1.2px; color: var(--muted); margin-bottom: 8px; font-weight: 500; }
.kpi-val { font-family: var(--serif); font-size: 24px; font-weight: 700; line-height: 1; }
.kpi-val.grn { color: var(--grn); }
.kpi-val.red { color: var(--red); }
.kpi-val.acc { color: var(--acc); }
.kpi-val.org { color: var(--org); }
.kpi-sub { font-size: 11px; color: var(--muted); margin-top: 5px; }

/* CARDS */
.card { background: var(--paper); border: 1px solid var(--bdr); border-radius: var(--r); box-shadow: var(--shadow); margin-bottom: 16px; }
.card-header { padding: 14px 18px 0; border-bottom: 1px solid var(--bdr); margin-bottom: 0; display: flex; align-items: baseline; justify-content: space-between; padding-bottom: 10px; }
.card-title { font-family: var(--serif); font-size: 13.5px; font-weight: 700; font-style: italic; color: var(--text); }
.card-sub { font-size: 11px; color: var(--muted); }
.card-body { padding: 16px 18px; }

/* TABLE */
.tbl { width: 100%; border-collapse: collapse; font-size: 12.5px; }
.tbl thead tr { border-bottom: 2px solid var(--bdr); }
.tbl th { font-size: 10px; text-transform: uppercase; letter-spacing: 1px; color: var(--muted); padding: 8px 12px; text-align: left; font-weight: 500; background: var(--paper2); }
.tbl th.r { text-align: right; }
.tbl td { padding: 9px 12px; border-bottom: 1px solid var(--bdr); color: var(--text); }
.tbl td.r { text-align: right; font-family: var(--mono); font-size: 12px; }
.tbl tbody tr:hover td { background: var(--paper2); }
.tbl tbody tr:last-child td { border-bottom: none; }
.neg { color: var(--red); font-weight: 500; }
.pos { color: var(--grn); font-weight: 500; }
.dim { color: var(--muted); }

/* BADGE */
.badge { display: inline-block; padding: 2px 8px; border-radius: 20px; font-size: 10.5px; font-weight: 500; }
.badge-red { background: var(--red-lt); color: var(--red); }
.badge-grn { background: var(--grn-lt); color: var(--grn); }
.badge-org { background: var(--org-lt); color: var(--org); }
.badge-acc { background: var(--acc-lt); color: var(--acc); }

/* ALERT STRIP */
.alert-strip { background: var(--red-lt); border: 1px solid #e8b8b8; border-radius: var(--r); padding: 10px 16px; margin-bottom: 16px; display: flex; align-items: center; gap: 12px; font-size: 12.5px; color: var(--red); }
.alert-strip strong { font-weight: 600; }

/* DIVIDER */
.divider { height: 1px; background: var(--bdr); margin: 20px 0; }

/* EMPTY STATE */
.empty { text-align: center; padding: 56px 20px; }
.empty-icon { font-size: 36px; opacity: .25; margin-bottom: 14px; }
.empty-title { font-family: var(--serif); font-size: 16px; font-style: italic; color: var(--sub); margin-bottom: 6px; }
.empty-sub { font-size: 12px; color: var(--muted); max-width: 320px; margin: 0 auto; }

/* TOAST */
#toast { position: fixed; bottom: 24px; right: 24px; background: var(--paper); border: 1px solid var(--bdr); border-radius: var(--r); padding: 11px 16px; font-size: 12.5px; z-index: 9999; display: none; max-width: 300px; box-shadow: var(--shadow-md); }
#toast.success { border-color: #b0dcc0; color: var(--grn); background: var(--grn-lt); }
#toast.error { border-color: #e8b8b8; color: var(--red); background: var(--red-lt); }
#toast.info { border-color: #b8d0e8; color: var(--acc); background: var(--acc-lt); }

/* LOADING */
.spin { display: inline-block; width: 18px; height: 18px; border: 2px solid var(--bdr); border-top-color: var(--acc); border-radius: 50%; animation: spin .7s linear infinite; }
@keyframes spin { to { transform: rotate(360deg); } }
.loading-overlay { display: none; position: fixed; inset: 0; background: rgba(247,245,240,.75); backdrop-filter: blur(2px); z-index: 500; align-items: center; justify-content: center; flex-direction: column; gap: 16px; }
.loading-overlay.show { display: flex; }
.loading-card { background: var(--paper); border: 1px solid var(--bdr); border-radius: var(--r); padding: 28px 40px; display: flex; flex-direction: column; align-items: center; gap: 14px; box-shadow: var(--shadow-md); }
.loading-msg { font-size: 13px; color: var(--sub); }
.progress-bar { height: 3px; background: var(--bdr); border-radius: 2px; overflow: hidden; width: 200px; }
.progress-fill { height: 100%; background: var(--acc); border-radius: 2px; transition: width .3s; width: 0%; }

input[type=file] { display: none; }

@media(max-width:900px) {
  .layout { grid-template-columns: 1fr; }
  .sidebar { border-right: none; border-bottom: 1px solid var(--bdr); }
  .kpi-grid { grid-template-columns: 1fr 1fr; }
}
</style>
</head>
<body>

<div class="loading-overlay" id="loading-overlay">
  <div class="loading-card">
    <div class="spin"></div>
    <div class="loading-msg" id="loading-msg">Procesando...</div>
    <div class="progress-bar"><div class="progress-fill" id="progress-fill"></div></div>
  </div>
</div>

<div id="toast"></div>

<header>
  <div class="logo">
    Farma2go
    <span class="sep">/</span>
    <span class="sub">Shipping P&L</span>
  </div>
  <div class="header-right">
    <select class="month-sel" id="global-month" onchange="loadPnl()">
      <option value="">Todos los meses</option>
    </select>
    <button class="btn btn-primary" onclick="loadPnl()">Calcular P&L</button>
    <button class="btn btn-grn" onclick="exportExcel()">↓ Excel</button>
    <button class="btn btn-red" id="btn-reclamaciones" onclick="exportReclamaciones()" style="display:none">Reclamar</button>
  </div>
</header>

<div class="layout">

<aside class="sidebar">

  <div class="sgroup">
    <div class="sgroup-title">Facturas transportistas</div>
    <button class="carrier-btn" id="btn-ctt" onclick="triggerUpload('ctt')">
      <span class="cb-icon">📦</span>
      <span><span class="cb-name">CTT Express</span><span class="cb-status">Excel (.xlsx)</span></span>
    </button>
    <button class="carrier-btn" id="btn-inpost" onclick="triggerUpload('inpost')">
      <span class="cb-icon">🟡</span>
      <span><span class="cb-name">InPost</span><span class="cb-status">ZIP con CSV</span></span>
    </button>
    <button class="carrier-btn" id="btn-spring" onclick="triggerUpload('spring')">
      <span class="cb-icon">🌱</span>
      <span><span class="cb-name">Spring</span><span class="cb-status">Excel (.xlsx)</span></span>
    </button>
    <button class="carrier-btn" id="btn-gls" onclick="triggerUpload('gls')">
      <span class="cb-icon">🔵</span>
      <span><span class="cb-name">GLS</span><span class="cb-status">Excel (.xlsx)</span></span>
    </button>
    <button class="carrier-btn" id="btn-ups" onclick="triggerUpload('ups')">
      <span class="cb-icon">🟤</span>
      <span><span class="cb-name">UPS</span><span class="cb-status">CSV</span></span>
    </button>
    <button class="carrier-btn" id="btn-asendia" onclick="triggerUpload('asendia')">
      <span class="cb-icon">✉️</span>
      <span><span class="cb-name">Asendia</span><span class="cb-status">Excel (.xlsx)</span></span>
    </button>
  </div>

  <div class="sgroup">
    <div class="sgroup-title">Ventas &amp; Ingresos</div>
    <button class="carrier-btn" id="btn-odoo" onclick="triggerUpload('odoo')">
      <span class="cb-icon">🛒</span>
      <span><span class="cb-name">Odoo — Ventas</span><span class="cb-status">sale_order__.xlsx</span></span>
    </button>
    <button class="carrier-btn" id="btn-shopify" onclick="triggerUpload('shopify')">
      <span class="cb-icon">💳</span>
      <span><span class="cb-name">Shopify revenue</span><span class="cb-status">CSV precios envío</span></span>
    </button>
  </div>

  <div class="sgroup">
    <div class="sgroup-title">Marketing</div>
    <button class="carrier-btn" id="btn-ads" onclick="triggerUpload('ads')">
      <span class="cb-icon">📣</span>
      <span><span class="cb-name">Google Ads</span><span class="cb-status">Inversión_Google_Ads.xlsx</span></span>
    </button>
  </div>

  <div class="sgroup" style="margin-top:auto; padding-top:12px; border-top:1px solid var(--bdr);">
    <div class="sgroup-title">Datos cargados</div>
    <div class="data-status" id="data-status">
      <div class="ds-row"><div class="ds-dot no"></div><div class="ds-label">Sin datos cargados</div></div>
    </div>
    <button class="btn btn-outline btn-sm" style="margin-top:8px; width:100%; justify-content:center;" onclick="clearAll()">Limpiar todo</button>
    <input type="file" id="file-seed" accept=".zip" onchange="uploadSeed(this)" style="display:none">
    <button class="btn btn-outline btn-sm" style="width:100%;justify-content:center;margin-top:4px;" onclick="document.getElementById('file-seed').click()">↑ Importar histórico</button>
  </div>

</aside>

<main class="main">

  <div class="tabs-wrap">
    <button class="tab active" onclick="setTab('resumen')">Resumen</button>
    <button class="tab" onclick="setTab('paises')">Por País</button>
    <button class="tab" onclick="setTab('carriers')">Carriers</button>
    <button class="tab" onclick="setTab('alertas')">Reclamaciones</button>
    <button class="tab" onclick="setTab('ads')">Google Ads</button>
  </div>

  <!-- RESUMEN -->
  <div class="tab-panel active" id="panel-resumen">
    <div class="kpi-grid">
      <div class="kpi-card">
        <div class="kpi-label">Margen final</div>
        <div class="kpi-val grn" id="kpi-mg">—</div>
        <div class="kpi-sub">producto + envío</div>
      </div>
      <div class="kpi-card">
        <div class="kpi-label">% Margen</div>
        <div class="kpi-val acc" id="kpi-pct">—</div>
        <div class="kpi-sub">sobre venta total</div>
      </div>
      <div class="kpi-card">
        <div class="kpi-label">Margen envíos</div>
        <div class="kpi-val" id="kpi-ship">—</div>
        <div class="kpi-sub">cobrado − coste carrier</div>
      </div>
      <div class="kpi-card">
        <div class="kpi-label">Reclamaciones</div>
        <div class="kpi-val red" id="kpi-alerts">—</div>
        <div class="kpi-sub" id="kpi-alerts-sub">envíos con pérdida anormal</div>
      </div>
    </div>
    <div id="resumen-content">
      <div class="empty">
        <div class="empty-icon">◎</div>
        <div class="empty-title">Carga datos para ver el P&L</div>
        <div class="empty-sub">Sube las facturas de transportistas y el listado de ventas de Odoo</div>
      </div>
    </div>
  </div>

  <!-- PAÍSES -->
  <div class="tab-panel" id="panel-paises">
    <div id="paises-content">
      <div class="empty">
        <div class="empty-icon">◎</div>
        <div class="empty-title">Sin datos por país</div>
        <div class="empty-sub">Sube el listado de ventas de Odoo para el desglose por país</div>
      </div>
    </div>
  </div>

  <!-- CARRIERS -->
  <div class="tab-panel" id="panel-carriers">
    <div id="carriers-content">
      <div class="empty">
        <div class="empty-icon">◎</div>
        <div class="empty-title">Sin facturas de transportistas</div>
        <div class="empty-sub">Sube las facturas de CTT, InPost, Spring, GLS o UPS</div>
      </div>
    </div>
  </div>

  <!-- ALERTAS -->
  <div class="tab-panel" id="panel-alertas">
    <div id="alertas-content">
      <div class="empty">
        <div class="empty-icon">◎</div>
        <div class="empty-title">Sin alertas detectadas</div>
        <div class="empty-sub">Las alertas aparecen cuando el coste supera 3× lo cobrado y la pérdida es mayor de 8€</div>
      </div>
    </div>
  </div>

  <!-- ADS -->
  <div class="tab-panel" id="panel-ads">
    <div id="ads-content">
      <div class="empty">
        <div class="empty-icon">◎</div>
        <div class="empty-title">Sin datos de publicidad</div>
        <div class="empty-sub">Sube el archivo de inversión de Google Ads</div>
      </div>
    </div>
  </div>

</main>
</div>

<!-- File inputs -->
<input type="file" id="file-ctt"     accept=".xlsx,.xls" onchange="uploadFile('ctt',this)">
<input type="file" id="file-inpost"  accept=".zip,.csv"  onchange="uploadFile('inpost',this)">
<input type="file" id="file-spring"  accept=".xlsx,.xls" onchange="uploadFile('spring',this)">
<input type="file" id="file-gls"     accept=".xlsx,.xls" onchange="uploadFile('gls',this)">
<input type="file" id="file-ups"     accept=".csv"       onchange="uploadFile('ups',this)">
<input type="file" id="file-asendia" accept=".xlsx,.xls" onchange="uploadFile('asendia',this)">
<input type="file" id="file-odoo"    accept=".xlsx,.xls" onchange="uploadFile('odoo',this)">
<input type="file" id="file-shopify" accept=".csv"       onchange="uploadFile('shopify',this)">
<input type="file" id="file-ads"     accept=".xlsx,.xls" onchange="uploadFile('ads',this)">

<script>
let pnlData = null;

function triggerUpload(carrier) { document.getElementById('file-' + carrier).click(); }

function fmtEur(v, dec=0) {
  if (v === null || v === undefined || isNaN(v)) return '—';
  const abs = Math.abs(v).toLocaleString('es-ES', {minimumFractionDigits:dec, maximumFractionDigits:dec});
  return (v < 0 ? '−' : '+') + abs + '\u202f€';
}
function fmtN(v) { return v == null ? '—' : Number(v).toLocaleString('es-ES'); }
function fmtPct(v) { if (v == null || isNaN(v)) return '—'; return (v*100).toFixed(1) + '%'; }
function clsM(v) { return v == null ? '' : v >= 0 ? 'pos' : 'neg'; }

function showToast(msg, type='info') {
  const t = document.getElementById('toast');
  t.textContent = msg; t.className = type; t.style.display = 'block';
  clearTimeout(t._timer);
  t._timer = setTimeout(() => t.style.display = 'none', 4000);
}
function showLoading(msg='Procesando...') {
  document.getElementById('loading-msg').textContent = msg;
  document.getElementById('loading-overlay').classList.add('show');
  document.getElementById('progress-fill').style.width = '0%';
}
function hideLoading() { document.getElementById('loading-overlay').classList.remove('show'); }
function setProgress(p) { document.getElementById('progress-fill').style.width = p + '%'; }

async function uploadFile(carrier, input) {
  const file = input.files[0]; if (!file) return;
  showLoading('Procesando ' + carrier.toUpperCase() + '…');
  const fd = new FormData(); fd.append('file', file); fd.append('carrier', carrier);
  try {
    setProgress(30);
    const res = await fetch('/upload', { method: 'POST', body: fd });
    setProgress(80);
    const data = await res.json();
    setProgress(100); hideLoading();
    if (data.ok) {
      showToast(carrier.toUpperCase() + ': ' + data.rows + ' envíos cargados', 'success');
      const btn = document.getElementById('btn-' + carrier);
      if (btn) {
        btn.classList.add('loaded');
        btn.querySelector('.cb-status').textContent = '✓ ' + data.rows + (data.months ? '  ·  ' + data.months : '');
      }
      refreshStatus();
    } else { showToast('Error en ' + carrier + ': ' + data.error, 'error'); }
  } catch(e) { hideLoading(); showToast('Error de conexión: ' + e.message, 'error'); }
  input.value = '';
}

async function loadPnl() {
  const month = document.getElementById('global-month').value;
  showLoading('Calculando P&L…');
  try {
    setProgress(50);
    const res = await fetch('/pnl?month=' + encodeURIComponent(month));
    setProgress(90);
    const data = await res.json();
    hideLoading();
    if (data.error) { showToast(data.error, 'error'); return; }
    pnlData = data; renderPnl(data);
    showToast('P&L actualizado', 'success');
  } catch(e) { hideLoading(); showToast('Error: ' + e.message, 'error'); }
}

function renderPnl(data) {
  // KPIs
  if (data.pnl_by_country) {
    const R = data.pnl_by_country;
    const mg   = R.reduce((a,r) => a+(r.mg_final||0), 0);
    const vta  = R.reduce((a,r) => a+(r.venta||0), 0);
    const inge = R.reduce((a,r) => a+(r.ing_envio||0), 0);
    const cste = R.reduce((a,r) => a+(r.cost_envio||0), 0);
    const pct  = (vta+inge) > 0 ? mg/(vta+inge) : null;
    const mge  = inge - cste;
    set('kpi-mg', fmtEur(mg,0), mg>=0?'grn':'red');
    set('kpi-pct', fmtPct(pct), pct&&pct>=0?'acc':'red');
    set('kpi-ship', fmtEur(mge,0), mge>=0?'grn':'red');
  } else if (data.country_shipping) {
    const mge = data.country_shipping.reduce((a,r) => a+(r.margen_envio||0), 0);
    set('kpi-ship', fmtEur(mge,0), mge>=0?'grn':'red');
  }
  if (data.alert_count != null) {
    set('kpi-alerts', fmtN(data.alert_count), 'red');
    document.getElementById('kpi-alerts-sub').textContent = 'Pérdida total: ' + fmtEur(data.alert_total_loss||0,0);
    document.getElementById('btn-reclamaciones').style.display = data.alert_count > 0 ? 'inline-flex' : 'none';
  }
  renderResumen(data); renderPaises(data); renderCarriers(data); renderAlertas(data); renderAds(data);
}

function set(id, val, colorClass) {
  const el = document.getElementById(id);
  el.textContent = val;
  el.className = 'kpi-val' + (colorClass ? ' ' + colorClass : '');
}

function card(title, sub, bodyHtml) {
  return `<div class="card">
    <div class="card-header"><span class="card-title">${title}</span>${sub?`<span class="card-sub">${sub}</span>`:''}  </div>
    <div class="card-body" style="padding:0;overflow-x:auto">${bodyHtml}</div>
  </div>`;
}

function renderResumen(data) {
  if (!data.pnl_by_country && !data.country_shipping) return;
  let html = '';
  if (data.pnl_by_country) {
    const byM = {};
    data.pnl_by_country.forEach(r => {
      if (!byM[r.ym]) byM[r.ym] = {venta:0,mg_prod:0,ing_envio:0,cost_envio:0,mg_final:0,n_pedidos:0};
      Object.keys(byM[r.ym]).forEach(k => byM[r.ym][k] += (r[k]||0));
    });
    const months = Object.keys(byM).sort();
    let tbl = `<table class="tbl"><thead><tr>
      <th>Mes</th><th class="r">Pedidos</th><th class="r">Venta</th><th class="r">Mg Producto</th><th class="r">% Mg</th><th class="r">Coste Envío</th><th class="r">Mg Envío</th><th class="r">Margen Final</th></tr></thead><tbody>`;
    let tV=0,tMp=0,tCe=0,tMf=0,tN=0,tIe=0;
    months.forEach(ym => {
      const d=byM[ym]; const pct=(d.venta+d.ing_envio)?d.mg_final/(d.venta+d.ing_envio):0;
      tV+=d.venta;tMp+=d.mg_prod;tCe+=d.cost_envio;tMf+=d.mg_final;tN+=d.n_pedidos;tIe+=d.ing_envio;
      const mge = d.ing_envio - d.cost_envio;
      tbl+=`<tr><td>${ym}</td><td class="r dim">${fmtN(d.n_pedidos)}</td>
        <td class="r">${fmtEur(d.venta,0)}</td>
        <td class="r ${clsM(d.mg_prod)}">${fmtEur(d.mg_prod,0)}</td>
        <td class="r ${clsM(pct)}">${fmtPct(pct)}</td>
        <td class="r neg">${fmtEur(-d.cost_envio,0).replace('−','')}</td>
        <td class="r ${clsM(mge)}">${fmtEur(mge,0)}</td>
        <td class="r ${clsM(d.mg_final)}"><strong>${fmtEur(d.mg_final,0)}</strong></td></tr>`;
    });
    const totPct=(tV+tIe)?tMf/(tV+tIe):0; const totMge=tIe-tCe;
    tbl+=`</tbody><tfoot><tr style="border-top:2px solid var(--bdr2)">
      <td><strong>Total</strong></td><td class="r">${fmtN(tN)}</td>
      <td class="r">${fmtEur(tV,0)}</td><td class="r ${clsM(tMp)}">${fmtEur(tMp,0)}</td>
      <td class="r ${clsM(totPct)}">${fmtPct(totPct)}</td>
      <td class="r neg">${fmtEur(-tCe,0).replace('−','')}</td>
      <td class="r ${clsM(totMge)}">${fmtEur(totMge,0)}</td>
      <td class="r ${clsM(tMf)}"><strong>${fmtEur(tMf,0)}</strong></td>
    </tr></tfoot></table>`;
    html = card('P&L Mensual', '', tbl);
  }
  document.getElementById('resumen-content').innerHTML = html || '<div class="empty"><div class="empty-icon">◎</div><div class="empty-title">Sin datos suficientes</div></div>';
}

function renderPaises(data) {
  if (!data.pnl_by_country) return;
  const byC = {};
  data.pnl_by_country.forEach(r => {
    const c = r.country||'Desconocido';
    if (!byC[c]) byC[c]={venta:0,cogs:0,mg_prod:0,ing_envio:0,cost_envio:0,mg_final:0,n_pedidos:0};
    Object.keys(byC[c]).forEach(k => byC[c][k]+=(r[k]||0));
  });
  const sorted = Object.entries(byC).sort((a,b) => b[1].venta-a[1].venta);
  let tbl = `<table class="tbl"><thead><tr><th>País</th><th class="r">Pedidos</th><th class="r">Venta</th><th class="r">Mg Producto</th><th class="r">% Mg Prod</th><th class="r">Coste Envío</th><th class="r">Mg Envío</th><th class="r">Margen Final</th><th class="r">% Final</th></tr></thead><tbody>`;
  sorted.forEach(([c,d]) => {
    const base=d.venta+d.ing_envio; const pct=base?d.mg_final/base:0; const pprod=d.venta?d.mg_prod/d.venta:0;
    const mge=d.ing_envio-d.cost_envio;
    tbl+=`<tr><td><strong>${c}</strong></td><td class="r dim">${fmtN(d.n_pedidos)}</td>
      <td class="r">${fmtEur(d.venta,0)}</td><td class="r ${clsM(d.mg_prod)}">${fmtEur(d.mg_prod,0)}</td>
      <td class="r ${clsM(pprod)}">${fmtPct(pprod)}</td>
      <td class="r neg">${fmtEur(-d.cost_envio,0).replace('−','')}</td>
      <td class="r ${clsM(mge)}">${fmtEur(mge,0)}</td>
      <td class="r ${clsM(d.mg_final)}"><strong>${fmtEur(d.mg_final,0)}</strong></td>
      <td class="r ${clsM(pct)}">${fmtPct(pct)}</td></tr>`;
  });
  tbl += '</tbody></table>';
  document.getElementById('paises-content').innerHTML = card('Rentabilidad por País', '', tbl);
}

function renderCarriers(data) {
  const rows = data.shipping; if (!rows||!rows.length) return;
  const byC = {};
  rows.forEach(r => {
    const k=r.carrier;
    if (!byC[k]) byC[k]={n:0,cost:0,ing:0,mg:0};
    byC[k].n+=r.n_envios||0; byC[k].cost+=r.coste_total||0; byC[k].ing+=r.ingreso_total||0; byC[k].mg+=r.margen_envio||0;
  });
  let tbl = `<table class="tbl"><thead><tr><th>Carrier</th><th class="r">Envíos</th><th class="r">Ingreso</th><th class="r">Coste</th><th class="r">Margen</th><th class="r">€/envío</th></tr></thead><tbody>`;
  Object.entries(byC).sort((a,b)=>a[1].mg-b[1].mg).forEach(([c,d]) => {
    tbl+=`<tr><td><strong>${c}</strong></td><td class="r dim">${fmtN(d.n)}</td>
      <td class="r">${fmtEur(d.ing,0)}</td><td class="r neg">${fmtEur(-d.cost,0).replace('−','')}</td>
      <td class="r ${clsM(d.mg)}">${fmtEur(d.mg,0)}</td>
      <td class="r ${clsM(d.mg/Math.max(d.n,1))}">${fmtEur(d.mg/Math.max(d.n,1),2)}</td></tr>`;
  });
  tbl+='</tbody></table>';
  document.getElementById('carriers-content').innerHTML = card('Margen por Carrier', '', tbl);
}

function renderAlertas(data) {
  const alerts=data.alerts; if (!alerts||!alerts.length) return;
  let html = `<div class="alert-strip">⚠️  <strong>${fmtN(alerts.length)} envíos</strong> con pérdida anormal &nbsp;·&nbsp; Pérdida total: <strong>${fmtEur(data.alert_total_loss||0,0)}</strong>
    <button class="btn btn-red btn-sm" style="margin-left:auto" onclick="exportReclamaciones()">Descargar CSV</button></div>`;
  let tbl=`<table class="tbl"><thead><tr><th>Ref Pedido</th><th>Carrier</th><th>País</th><th class="r">Peso kg</th><th class="r">Cobrado</th><th class="r">Coste</th><th class="r">Pérdida</th><th class="r">Ratio</th></tr></thead><tbody>`;
  alerts.slice(0,200).forEach(a => {
    const ratio=a.precio_envio>0?Math.abs(a.cost_eur/a.precio_envio):null;
    tbl+=`<tr><td><strong>${a.ref}</strong></td><td>${a.carrier}</td><td>${a.country||'—'}</td>
      <td class="r">${(a.weight_kg||0).toFixed(2)}</td>
      <td class="r">${fmtEur(a.precio_envio,2)}</td>
      <td class="r neg">${fmtEur(-a.cost_eur,2).replace('−','')}</td>
      <td class="r neg"><strong>${fmtEur(a.margin,2)}</strong></td>
      <td class="r neg">${ratio?ratio.toFixed(1)+'×':'—'}</td></tr>`;
  });
  tbl+='</tbody></table>';
  html += card('Envíos para reclamar', 'coste &gt; 3× cobrado y pérdida &gt; 8€', tbl);
  document.getElementById('alertas-content').innerHTML = html;
}

function renderAds(data) {
  const ads=data.ads; if (!ads||!ads.length) return;
  const pnl=data.pnl_by_country||[];
  const mgLookup={};
  pnl.forEach(r => { mgLookup[`${r.country}|${r.ym}`]=r.mg_final||0; });
  let tbl=`<table class="tbl"><thead><tr><th>País</th><th>Mes</th><th class="r">Gasto Ads</th><th class="r">Conv.</th><th class="r">Valor Conv.</th><th class="r">ROAS</th><th class="r">Mg Final</th><th class="r">Post-Ads</th></tr></thead><tbody>`;
  ads.sort((a,b)=>a.pais.localeCompare(b.pais)||a.ym.localeCompare(b.ym)).forEach(a => {
    const roas=a.roas||0;
    const rClass=roas>=6?'pos':roas>=3?'':' style="color:var(--org)"';
    const mg=mgLookup[`${a.pais}|${a.ym}`]||null;
    const postAds=mg!=null?mg-(a.gasto_ads||0):null;
    tbl+=`<tr><td><strong>${a.pais}</strong></td><td class="dim">${a.ym}</td>
      <td class="r neg">${fmtEur(-(a.gasto_ads||0),0).replace('−','')}</td>
      <td class="r dim">${fmtN(Math.round(a.conversiones||0))}</td>
      <td class="r">${fmtEur(a.valor_conv||0,0)}</td>
      <td class="r"><span class="${roas>=6?'badge badge-grn':roas>=3?'badge badge-org':'badge badge-red'}">${roas?roas.toFixed(1)+'×':'—'}</span></td>
      <td class="r ${mg!=null?clsM(mg):''}">${mg!=null?fmtEur(mg,0):'—'}</td>
      <td class="r ${postAds!=null?clsM(postAds):''}">${postAds!=null?`<strong>${fmtEur(postAds,0)}</strong>`:'—'}</td></tr>`;
  });
  tbl+='</tbody></table>';
  document.getElementById('ads-content').innerHTML = card('Google Ads + Margen post-publicidad', '', tbl);
}

async function exportExcel() {
  const month = document.getElementById('global-month').value;
  showLoading('Generando Excel…');
  try {
    const res = await fetch('/export/excel?month=' + encodeURIComponent(month));
    const blob = await res.blob();
    const a = document.createElement('a'); a.href = URL.createObjectURL(blob);
    a.download = `Farma2go_PL_${month||'completo'}_${new Date().toISOString().slice(0,10)}.xlsx`;
    a.click(); URL.revokeObjectURL(a.href);
    hideLoading(); showToast('Excel descargado', 'success');
  } catch(e) { hideLoading(); showToast('Error: ' + e.message, 'error'); }
}

async function exportReclamaciones() {
  const month = document.getElementById('global-month').value;
  showLoading('Generando CSV…');
  try {
    const res = await fetch('/export/reclamaciones?month=' + encodeURIComponent(month));
    const blob = await res.blob();
    const a = document.createElement('a'); a.href = URL.createObjectURL(blob);
    a.download = `Reclamaciones_${month||'completo'}_${new Date().toISOString().slice(0,10)}.csv`;
    a.click(); URL.revokeObjectURL(a.href);
    hideLoading(); showToast('CSV descargado', 'success');
  } catch(e) { hideLoading(); showToast('Error: ' + e.message, 'error'); }
}

async function clearAll() {
  if (!confirm('¿Borrar todos los datos cargados?')) return;
  await fetch('/clear', { method: 'POST' });
  showToast('Datos borrados', 'info');
  document.querySelectorAll('.carrier-btn').forEach(b => {
    b.classList.remove('loaded');
    const map={'ctt':'Excel (.xlsx)','inpost':'ZIP con CSV','spring':'Excel (.xlsx)','gls':'Excel (.xlsx)','ups':'CSV','asendia':'Excel (.xlsx)','odoo':'sale_order__.xlsx','shopify':'CSV precios envío','ads':'Inversión_Google_Ads.xlsx'};
    const id = b.id.replace('btn-',''); const s = b.querySelector('.cb-status');
    if (s && map[id]) s.textContent = map[id];
  });
  refreshStatus(); pnlData = null;
  ['resumen-content','paises-content','carriers-content','alertas-content','ads-content'].forEach(id => {
    document.getElementById(id).innerHTML = '<div class="empty"><div class="empty-icon">◎</div><div class="empty-sub">Sin datos</div></div>';
  });
  ['kpi-mg','kpi-pct','kpi-ship','kpi-alerts'].forEach(id => { document.getElementById(id).textContent='—'; document.getElementById(id).className='kpi-val'; });
}

async function refreshStatus() {
  const res = await fetch('/status'); const data = await res.json();
  const container = document.getElementById('data-status');
  if (!data.files||!Object.keys(data.files).length) {
    container.innerHTML='<div class="ds-row"><div class="ds-dot no"></div><div class="ds-label">Sin datos cargados</div></div>'; return;
  }
  if (data.months&&data.months.length) {
    const sel=document.getElementById('global-month'); const cur=sel.value;
    sel.innerHTML='<option value="">Todos los meses</option>'+data.months.map(m=>`<option value="${m}" ${m===cur?'selected':''}>${m}</option>`).join('');
  }
  container.innerHTML=Object.entries(data.files).map(([k,v])=>`<div class="ds-row"><div class="ds-dot ${v.rows&&v.rows>0?'ok':'warn'}"></div><div class="ds-label">${k}</div><div class="ds-val">${v.rows||0}</div></div>`).join('');
}

function setTab(id) {
  document.querySelectorAll('.tab').forEach(t=>t.classList.remove('active'));
  document.querySelectorAll('.tab-panel').forEach(p=>p.classList.remove('active'));
  event.target.classList.add('active');
  document.getElementById('panel-'+id).classList.add('active');
}


async function uploadSeed(input) {
  const file = input.files[0]; if (!file) return;
  showLoading('Importando histórico...');
  const fd = new FormData(); fd.append('file', file);
  try {
    setProgress(40);
    const res = await fetch('/seed', { method: 'POST', body: fd });
    setProgress(90);
    const data = await res.json();
    hideLoading();
    if (data.ok) {
      showToast('Histórico importado: ' + data.imported.join(', '), 'success');
      refreshStatus();
      await loadPnl();
    } else { showToast('Error: ' + data.error, 'error'); }
  } catch(e) { hideLoading(); showToast('Error: ' + e.message, 'error'); }
  input.value = '';
}

refreshStatus();
</script>
</body>
</html>"""


# ─────────────────────────────────────────────────────────────────
# ROUTES
# ─────────────────────────────────────────────────────────────────

@app.route('/')
def index():
    return render_template_string(HTML)


@app.route('/upload', methods=['POST'])
def upload():
    carrier = request.form.get('carrier', '').lower()
    file = request.files.get('file')
    if not file:
        return jsonify({'ok': False, 'error': 'No se recibió archivo'})

    file_bytes = file.read()
    filename = file.filename

    try:
        if carrier == 'ctt':
            df = parse_ctt(file_bytes, filename)
            _merge_shipping(df, 'ctt')
        elif carrier == 'inpost':
            df = parse_inpost(file_bytes, filename)
            _merge_shipping(df, 'inpost')
        elif carrier == 'spring':
            df = parse_spring(file_bytes, filename)
            _merge_shipping(df, 'spring')
        elif carrier == 'gls':
            df = parse_gls(file_bytes, filename)
            _merge_shipping(df, 'gls')
        elif carrier == 'ups':
            df_farma, df_skinvity = parse_ups(file_bytes, filename)
            _merge_shipping(df_farma, 'ups')
            if len(df_skinvity):
                existing = load_data('skinvity') or pd.DataFrame()
                combined = pd.concat([existing, df_skinvity], ignore_index=True) if len(existing) else df_skinvity
                save_data('skinvity', combined)
        elif carrier == 'asendia':
            df = parse_ctt(file_bytes, filename)  # similar format
            df['carrier'] = 'Asendia'
            _merge_shipping(df, 'asendia')
        elif carrier == 'odoo':
            df = parse_odoo_sales(file_bytes, filename)
            existing = load_data('odoo_sales')
            if existing is not None and len(existing):
                # Deduplicate by ref_odoo + producto
                combined = pd.concat([existing, df], ignore_index=True)
                combined = combined.drop_duplicates(subset=['ref_odoo', 'producto'], keep='last')
            else:
                combined = df
            save_data('odoo_sales', combined)
            return jsonify({'ok': True, 'rows': len(df),
                            'months': _get_months_str(df),
                            'auto_pnl': False})
        elif carrier == 'shopify':
            revenue = parse_shopify_revenue(file_bytes, filename)
            existing_rev = load_data('shopify_revenue')
            if isinstance(existing_rev, dict):
                existing_rev.update(revenue)
            else:
                existing_rev = revenue
            save_data('shopify_revenue', existing_rev)
            return jsonify({'ok': True, 'rows': len(revenue), 'months': ''})
        elif carrier == 'ads':
            df = parse_google_ads(file_bytes, filename)
            existing = load_data('google_ads')
            if existing is not None and len(existing):
                combined = pd.concat([existing, df], ignore_index=True).drop_duplicates(
                    subset=['pais','ym'], keep='last')
            else:
                combined = df
            save_data('google_ads', combined)
            return jsonify({'ok': True, 'rows': len(df), 'months': _get_months_str(df)})
        else:
            return jsonify({'ok': False, 'error': f'Carrier desconocido: {carrier}'})

        # Get row count from shipping_costs
        shipping = load_data('shipping_costs')
        rows = len(shipping[shipping['carrier'] == carrier.upper()]) if shipping is not None else 0
        return jsonify({'ok': True, 'rows': rows, 'months': _get_months_str(df), 'auto_pnl': False})

    except Exception as e:
        traceback.print_exc()
        return jsonify({'ok': False, 'error': str(e)})


def _merge_shipping(df, carrier_name):
    """Merge new carrier data into the global shipping_costs store."""
    if df is None or len(df) == 0:
        return
    df = df.copy()
    df['carrier'] = df.get('carrier', carrier_name.upper())

    # Add pricing from Shopify revenue if available
    revenue = load_data('shopify_revenue')
    if isinstance(revenue, dict) and len(revenue):
        df = compute_shipping_margin(df, revenue)
    else:
        df['precio_envio'] = 0.0
        df['margin'] = -df['cost_eur']

    existing = load_data('shipping_costs')
    if existing is not None and len(existing):
        # Remove old data for this carrier and re-add
        carrier_upper = carrier_name.upper()
        existing = existing[existing['carrier'] != carrier_upper]
        combined = pd.concat([existing, df], ignore_index=True)
    else:
        combined = df

    save_data('shipping_costs', combined)


def _get_months_str(df):
    if 'ym' in df.columns:
        months = sorted(df['ym'].dropna().unique())
        if months:
            return f"{months[0]} → {months[-1]}"
    return ''


@app.route('/pnl')
def pnl():
    month = request.args.get('month', '').strip()
    month_filter = month if month else None
    result = build_pnl(month_filter)
    return jsonify(result)


@app.route('/status')
def status():
    files = list_saved()
    # Get available months
    shipping = load_data('shipping_costs')
    odoo = load_data('odoo_sales')
    months = set()
    if shipping is not None and 'ym' in shipping.columns:
        months.update(shipping['ym'].dropna().unique())
    if odoo is not None and 'ym' in odoo.columns:
        months.update(odoo['ym'].dropna().unique())
    return jsonify({'files': files, 'months': sorted(months)})


@app.route('/export/excel')
def export_excel():
    month = request.args.get('month', '').strip()
    month_filter = month if month else None
    data = build_pnl(month_filter)
    month_label = month or 'completo'
    excel_bytes = generate_pnl_excel(data, month_label)
    return send_file(
        io.BytesIO(excel_bytes),
        mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        as_attachment=True,
        download_name=f'Farma2go_PL_{month_label}.xlsx'
    )


@app.route('/export/reclamaciones')
def export_reclamaciones():
    month = request.args.get('month', '').strip()
    data = build_pnl(month if month else None)
    alerts = data.get('alerts', [])
    csv_bytes = generate_reclamacion_csv(alerts)
    return send_file(
        io.BytesIO(csv_bytes),
        mimetype='text/csv',
        as_attachment=True,
        download_name=f'Reclamaciones_{month or "completo"}.csv'
    )


@app.route('/clear', methods=['POST'])
def clear():
    import shutil
    data_dir = os.path.join(os.path.dirname(__file__), 'data')
    for f in os.listdir(data_dir):
        if f.endswith('.json'):
            os.remove(os.path.join(data_dir, f))
    return jsonify({'ok': True})



@app.route('/seed', methods=['POST'])
def seed():
    """Import pre-built historical JSON files from a ZIP."""
    import zipfile, io
    file = request.files.get('file')
    if not file:
        return jsonify({'ok': False, 'error': 'No se recibió archivo'})
    try:
        zf = zipfile.ZipFile(io.BytesIO(file.read()))
        imported = []
        for name in zf.namelist():
            if not name.endswith('.json'): continue
            key = name.replace('.json', '')
            content = zf.read(name).decode('utf-8')
            dest = os.path.join(DATA_DIR, name)
            with open(dest, 'w', encoding='utf-8') as f_out:
                f_out.write(content)
            try:
                import pandas as pd
                df = pd.read_json(dest, orient='records')
                imported.append(f'{key}: {len(df):,} filas')
            except:
                imported.append(key)
        return jsonify({'ok': True, 'imported': imported})
    except Exception as e:
        return jsonify({'ok': False, 'error': str(e)})

if __name__ == '__main__':
    print("\n" + "="*55)
    print("  Farma2go Shipping P&L — App arrancada")
    print("  Abre en el navegador: http://localhost:5000")
    print("="*55 + "\n")
    app.run(debug=False, host='0.0.0.0', port=5000)
