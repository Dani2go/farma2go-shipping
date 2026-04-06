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

import math

def clean_nan(obj):
    """Recursively replace float NaN/Inf with None so jsonify works."""
    if isinstance(obj, dict):
        return {k: clean_nan(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [clean_nan(i) for i in obj]
    if isinstance(obj, float) and (math.isnan(obj) or math.isinf(obj)):
        return None
    return obj


def _seed_from_bundle():
    """On first startup, decompress bundled .json.gz files into DATA_DIR."""
    import gzip, shutil
    bundle_dir = os.path.join(os.path.dirname(__file__), 'data_bundle')
    if not os.path.isdir(bundle_dir):
        return
    for gz_file in os.listdir(bundle_dir):
        if not gz_file.endswith('.json.gz'):
            continue
        dest = os.path.join(DATA_DIR, gz_file[:-3])  # strip .gz
        if os.path.exists(dest):
            continue  # already decompressed, skip
        src = os.path.join(bundle_dir, gz_file)
        print(f'Seeding {gz_file} → {dest}')
        with gzip.open(src, 'rb') as f_in, open(dest, 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)
        print(f'  Done: {os.path.getsize(dest)/1024/1024:.1f}MB')

_seed_from_bundle()


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
<title>Farma2go — P&L</title>
<link href="https://fonts.googleapis.com/css2?family=Libre+Baskerville:ital,wght@0,400;0,700;1,400&family=DM+Sans:opsz,wght@9..40,300;9..40,400;9..40,500;9..40,600&family=DM+Mono:wght@400;500&display=swap" rel="stylesheet">
<style>
:root {
  --bg:#f5f3ee; --paper:#fff; --paper2:#faf9f6; --bdr:#e4dfd8; --bdr2:#cec7be;
  --text:#1a1714; --sub:#5a524a; --muted:#9a8f84; --acc:#1e4d7b; --acc-lt:#e8f0f9;
  --red:#b93535; --red-lt:#fdf0f0; --grn:#2a7a4b; --grn-lt:#eef7f2;
  --org:#b85c0a; --org-lt:#fef4ec; --yel:#7a6800; --yel-lt:#fffbe6;
  --serif:'Libre Baskerville',Georgia,serif;
  --sans:'DM Sans',system-ui,sans-serif;
  --mono:'DM Mono','Courier New',monospace;
  --r:4px; --shadow:0 1px 3px rgba(0,0,0,.07),0 1px 2px rgba(0,0,0,.04);
  --shadow-md:0 4px 16px rgba(0,0,0,.08);
}
*{box-sizing:border-box;margin:0;padding:0}
body{background:var(--bg);color:var(--text);font-family:var(--sans);font-size:13.5px;min-height:100vh}

/* HEADER */
header{background:var(--paper);border-bottom:1px solid var(--bdr);padding:0 28px;height:54px;display:flex;align-items:center;justify-content:space-between;position:sticky;top:0;z-index:100;box-shadow:var(--shadow)}
.logo{font-family:var(--serif);font-size:17px;font-weight:700;display:flex;align-items:baseline;gap:8px}
.logo .slash{color:var(--bdr2);font-weight:400}
.logo .sub{font-size:13px;font-weight:400;font-style:italic;color:var(--muted)}
.header-right{display:flex;gap:8px;align-items:center}

/* LAYOUT */
.layout{display:grid;grid-template-columns:248px 1fr;min-height:calc(100vh - 54px)}
.sidebar{background:var(--paper);border-right:1px solid var(--bdr);padding:18px 14px;display:flex;flex-direction:column;gap:14px;overflow-y:auto}
.main{padding:22px 26px;background:var(--bg);overflow-y:auto}

/* SIDEBAR */
.sgroup{display:flex;flex-direction:column;gap:3px}
.sgroup-title{font-size:9px;text-transform:uppercase;letter-spacing:1.8px;color:var(--muted);font-weight:500;padding:0 4px 6px;border-bottom:1px solid var(--bdr);margin-bottom:3px}
.carrier-btn{width:100%;padding:7px 10px;border:1px solid var(--bdr);border-radius:var(--r);background:var(--paper2);color:var(--sub);cursor:pointer;font-family:var(--sans);font-size:12px;display:flex;align-items:center;gap:9px;transition:all .12s;text-align:left}
.carrier-btn:hover{border-color:var(--acc);background:var(--acc-lt);color:var(--text)}
.carrier-btn.loaded{border-color:var(--grn);background:var(--grn-lt);color:var(--grn)}
.carrier-btn .cb-name{font-weight:500;display:block}
.carrier-btn .cb-status{font-size:10px;color:var(--muted);display:block}
.carrier-btn.loaded .cb-status{color:var(--grn);opacity:.8}
.ds-row{display:flex;align-items:center;gap:7px;padding:4px 6px;border-radius:var(--r);font-size:11px}
.ds-dot{width:6px;height:6px;border-radius:50%;flex-shrink:0}
.ds-dot.ok{background:var(--grn)}.ds-dot.no{background:var(--bdr2)}.ds-dot.warn{background:var(--org)}
.ds-label{flex:1;color:var(--sub)}.ds-val{color:var(--muted);font-size:10px;font-family:var(--mono)}

/* BUTTONS */
.btn{padding:7px 15px;border-radius:var(--r);border:none;cursor:pointer;font-family:var(--sans);font-size:12.5px;font-weight:500;transition:all .12s;display:inline-flex;align-items:center;gap:6px}
.btn-primary{background:var(--acc);color:#fff}.btn-primary:hover{background:#163d63}
.btn-outline{background:var(--paper);border:1px solid var(--bdr);color:var(--sub)}.btn-outline:hover{border-color:var(--acc);color:var(--acc)}
.btn-red{background:var(--red-lt);border:1px solid #e8b8b8;color:var(--red)}.btn-red:hover{background:#fbe5e5}
.btn-grn{background:var(--grn-lt);border:1px solid #b0dcc0;color:var(--grn)}.btn-grn:hover{background:#dff2e8}
.btn-sm{padding:5px 11px;font-size:11.5px}
.sel{padding:7px 12px;border-radius:var(--r);border:1px solid var(--bdr);background:var(--paper2);color:var(--text);font-family:var(--sans);font-size:12.5px;outline:none;cursor:pointer}
.sel:focus{border-color:var(--acc)}

/* TABS */
.tabs{border-bottom:2px solid var(--bdr);margin-bottom:20px;display:flex;gap:0}
.tab{padding:9px 17px;font-size:12.5px;color:var(--muted);background:none;border:none;cursor:pointer;border-bottom:2px solid transparent;margin-bottom:-2px;transition:all .12s;font-family:var(--sans)}
.tab:hover{color:var(--text)}
.tab.active{color:var(--acc);border-bottom-color:var(--acc);font-weight:500}
.tab-panel{display:none}.tab-panel.active{display:block}

/* P&L TABLE — the main financial statement */
.pnl-table{width:100%;border-collapse:collapse;font-size:13px}
.pnl-table td{padding:7px 14px;border-bottom:1px solid var(--bdr)}
.pnl-table .row-label{color:var(--sub);font-family:var(--sans)}
.pnl-table .row-label.indent{padding-left:28px;color:var(--muted)}
.pnl-table .row-label.total{font-family:var(--serif);font-weight:700;font-size:14px;color:var(--text)}
.pnl-table .row-label.section{font-size:9px;text-transform:uppercase;letter-spacing:1.5px;color:var(--muted);padding-top:14px;padding-bottom:4px;border-bottom:none;background:var(--paper2)}
.pnl-table .val{text-align:right;font-family:var(--mono);font-size:13px}
.pnl-table .val.pos{color:var(--grn)}.pnl-table .val.neg{color:var(--red)}
.pnl-table .val.total{font-family:var(--serif);font-size:15px;font-weight:700}
.pnl-table .val.pct{font-size:11px;color:var(--muted);text-align:right;font-family:var(--mono)}
.pnl-table .val.pct.pos{color:var(--grn)}.pnl-table .val.pct.neg{color:var(--red)}
.pnl-table tr.separator td{border-bottom:2px solid var(--bdr2);padding:0;height:1px}
.pnl-table tr.highlight td{background:var(--paper2)}
.pnl-table th{font-size:10px;text-transform:uppercase;letter-spacing:1px;color:var(--muted);padding:8px 14px;text-align:right;font-weight:500;background:var(--paper2);border-bottom:2px solid var(--bdr2)}
.pnl-table th:first-child{text-align:left}

/* COMPARE COLUMNS */
.col-a{border-left:2px solid var(--acc-lt)}.col-b{border-left:2px solid var(--grn-lt)}
.col-delta{border-left:1px solid var(--bdr);color:var(--muted)}
.col-delta.up{color:var(--grn)}.col-delta.down{color:var(--red)}

/* SEMAPHORE GRID */
.semaphore-grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(200px,1fr));gap:10px;margin-bottom:18px}
.sem-card{background:var(--paper);border:1px solid var(--bdr);border-radius:var(--r);padding:14px 16px;box-shadow:var(--shadow)}
.sem-card.grn{border-left:4px solid var(--grn)}.sem-card.yel{border-left:4px solid #f0c040}.sem-card.red{border-left:4px solid var(--red)}
.sem-country{font-weight:600;font-size:13px;margin-bottom:8px}
.sem-metrics{display:flex;gap:8px}
.sem-m{display:flex;flex-direction:column;align-items:center;flex:1}
.sem-m-label{font-size:9px;text-transform:uppercase;letter-spacing:.8px;color:var(--muted);margin-bottom:3px}
.sem-m-val{font-family:var(--mono);font-size:13px;font-weight:500}
.sem-m-val.pos{color:var(--grn)}.sem-m-val.neg{color:var(--red)}.sem-m-val.warn{color:#c0800a}

/* ALERT CARDS */
.alert-cards{display:flex;flex-direction:column;gap:8px;margin-bottom:18px}
.alert-card{background:var(--paper);border:1px solid var(--bdr);border-radius:var(--r);padding:12px 16px;display:flex;align-items:flex-start;gap:12px;box-shadow:var(--shadow)}
.alert-card.urgent{border-left:4px solid var(--red)}
.alert-card.warn{border-left:4px solid #f0c040}
.alert-card.info{border-left:4px solid var(--acc)}
.alert-icon{font-size:18px;flex-shrink:0;margin-top:1px}
.alert-body{flex:1}
.alert-title{font-weight:600;font-size:13px;margin-bottom:3px}
.alert-desc{font-size:12px;color:var(--sub);line-height:1.5}
.alert-action{font-size:11px;font-weight:500;margin-top:5px}
.alert-action.red{color:var(--red)}.alert-action.org{color:var(--org)}.alert-action.acc{color:var(--acc)}

/* CARDS */
.card{background:var(--paper);border:1px solid var(--bdr);border-radius:var(--r);box-shadow:var(--shadow);margin-bottom:14px;overflow:hidden}
.card-hdr{padding:12px 16px;border-bottom:1px solid var(--bdr);display:flex;align-items:baseline;justify-content:space-between;background:var(--paper2)}
.card-title{font-family:var(--serif);font-size:13px;font-weight:700;font-style:italic}
.card-sub{font-size:11px;color:var(--muted)}

/* GENERIC TABLE */
.tbl{width:100%;border-collapse:collapse;font-size:12.5px}
.tbl thead th{font-size:9.5px;text-transform:uppercase;letter-spacing:.8px;color:var(--muted);padding:8px 12px;text-align:left;font-weight:500;background:var(--paper2);border-bottom:1px solid var(--bdr2)}
.tbl thead th.r{text-align:right}
.tbl tbody td{padding:8px 12px;border-bottom:1px solid var(--bdr)}
.tbl tbody td.r{text-align:right;font-family:var(--mono);font-size:12px}
.tbl tbody tr:hover td{background:var(--paper2)}
.tbl tbody tr:last-child td{border-bottom:none}
.pos{color:var(--grn);font-weight:500}.neg{color:var(--red);font-weight:500}.dim{color:var(--muted)}

/* COMPARISON */
.cmp-header{display:flex;align-items:center;gap:10px;margin-bottom:18px;flex-wrap:wrap}
.cmp-label{font-size:11px;color:var(--muted);text-transform:uppercase;letter-spacing:.8px}
.cmp-vs{font-family:var(--serif);font-style:italic;color:var(--muted);font-size:13px}

/* TOAST */
#toast{position:fixed;bottom:22px;right:22px;background:var(--paper);border:1px solid var(--bdr);border-radius:var(--r);padding:10px 16px;font-size:12.5px;z-index:9999;display:none;max-width:300px;box-shadow:var(--shadow-md)}
#toast.success{border-color:#b0dcc0;color:var(--grn);background:var(--grn-lt)}
#toast.error{border-color:#e8b8b8;color:var(--red);background:var(--red-lt)}
#toast.info{border-color:#b8d0e8;color:var(--acc);background:var(--acc-lt)}

/* LOADING */
.spin{display:inline-block;width:18px;height:18px;border:2px solid var(--bdr);border-top-color:var(--acc);border-radius:50%;animation:spin .7s linear infinite}
@keyframes spin{to{transform:rotate(360deg)}}
.loading-overlay{display:none;position:fixed;inset:0;background:rgba(245,243,238,.8);backdrop-filter:blur(2px);z-index:500;align-items:center;justify-content:center}
.loading-overlay.show{display:flex}
.loading-card{background:var(--paper);border:1px solid var(--bdr);border-radius:var(--r);padding:24px 36px;display:flex;flex-direction:column;align-items:center;gap:12px;box-shadow:var(--shadow-md)}
.loading-msg{font-size:13px;color:var(--sub)}
.prog-bar{height:3px;background:var(--bdr);border-radius:2px;overflow:hidden;width:180px}
.prog-fill{height:100%;background:var(--acc);border-radius:2px;transition:width .3s;width:0%}

/* EMPTY */
.empty{text-align:center;padding:52px 20px}
.empty-icon{font-size:32px;opacity:.2;margin-bottom:12px}
.empty-title{font-family:var(--serif);font-size:15px;font-style:italic;color:var(--sub);margin-bottom:5px}
.empty-sub{font-size:12px;color:var(--muted);max-width:300px;margin:0 auto}

input[type=file]{display:none}
</style>
</head>
<body>

<div class="loading-overlay" id="lo">
  <div class="loading-card">
    <div class="spin"></div>
    <div class="loading-msg" id="lm">Calculando...</div>
    <div class="prog-bar"><div class="prog-fill" id="pf"></div></div>
  </div>
</div>
<div id="toast"></div>

<header>
  <div class="logo">Farma2go <span class="slash">/</span> <span class="sub">Shipping P&L</span></div>
  <div class="header-right">
    <div style="display:flex;align-items:center;gap:6px;flex-wrap:wrap">
      <button class="btn btn-outline btn-sm" id="btn-f-all" onclick="setFilter('')">Todo</button>
      <button class="btn btn-outline btn-sm" id="btn-f-2025" onclick="setFilter('2025')">2025</button>
      <button class="btn btn-outline btn-sm" id="btn-f-2026" onclick="setFilter('2026')">2026</button>
      <select class="sel" id="global-month" onchange="onMonthChange(this.value)" style="min-width:110px">
        <option value="">Mes concreto…</option>
      </select>
    </div>
    <button class="btn btn-primary" onclick="loadPnl()">Calcular P&L</button>
    <button class="btn btn-grn" onclick="exportExcel()">↓ Excel</button>
  </div>
</header>

<div class="layout">
<aside class="sidebar">
  <div class="sgroup">
    <div class="sgroup-title">Facturas transportistas</div>
    <button class="carrier-btn" id="btn-ctt"     onclick="triggerUpload('ctt')">    <span>📦</span><span><span class="cb-name">CTT Express</span><span class="cb-status">Excel (.xlsx)</span></span></button>
    <button class="carrier-btn" id="btn-inpost"  onclick="triggerUpload('inpost')"> <span>🟡</span><span><span class="cb-name">InPost</span><span class="cb-status">ZIP con CSV</span></span></button>
    <button class="carrier-btn" id="btn-spring"  onclick="triggerUpload('spring')"> <span>🌱</span><span><span class="cb-name">Spring</span><span class="cb-status">Excel (.xlsx)</span></span></button>
    <button class="carrier-btn" id="btn-gls"     onclick="triggerUpload('gls')">    <span>🔵</span><span><span class="cb-name">GLS</span><span class="cb-status">Excel (.xlsx)</span></span></button>
    <button class="carrier-btn" id="btn-ups"     onclick="triggerUpload('ups')">    <span>🟤</span><span><span class="cb-name">UPS</span><span class="cb-status">CSV</span></span></button>
    <button class="carrier-btn" id="btn-asendia" onclick="triggerUpload('asendia')"><span>✉️</span><span><span class="cb-name">Asendia</span><span class="cb-status">Excel (.xlsx)</span></span></button>
  </div>
  <div class="sgroup">
    <div class="sgroup-title">Ventas &amp; Ingresos</div>
    <button class="carrier-btn" id="btn-odoo"    onclick="triggerUpload('odoo')">   <span>🛒</span><span><span class="cb-name">Odoo — Ventas</span><span class="cb-status">sale_order__.xlsx</span></span></button>
    <button class="carrier-btn" id="btn-shopify" onclick="triggerUpload('shopify')"><span>💳</span><span><span class="cb-name">Shopify revenue</span><span class="cb-status">CSV precios envío</span></span></button>
  </div>
  <div class="sgroup">
    <div class="sgroup-title">Marketing</div>
    <button class="carrier-btn" id="btn-ads" onclick="triggerUpload('ads')"><span>📣</span><span><span class="cb-name">Google Ads</span><span class="cb-status">Inversión_Google_Ads.xlsx</span></span></button>
  </div>
  <div class="sgroup" style="margin-top:auto;padding-top:12px;border-top:1px solid var(--bdr)">
    <div class="sgroup-title">Datos cargados</div>
    <div id="data-status"><div class="ds-row"><div class="ds-dot no"></div><div class="ds-label">Sin datos</div></div></div>
    <input type="file" id="file-seed" accept=".zip" onchange="uploadSeed(this)">
    <button class="btn btn-outline btn-sm" style="width:100%;justify-content:center;margin-top:6px" onclick="document.getElementById('file-seed').click()">↑ Importar histórico</button>
    <button class="btn btn-outline btn-sm" style="width:100%;justify-content:center;margin-top:4px" onclick="clearAll()">Limpiar todo</button>
  </div>
</aside>

<main class="main">
  <div class="tabs">
    <button class="tab active" onclick="setTab('resumen')">P&L</button>
    <button class="tab" onclick="setTab('paises')">Por País</button>
    <button class="tab" onclick="setTab('evolucion')">Evolución</button>
    <button class="tab" onclick="setTab('comparar')">Comparar</button>
    <button class="tab" onclick="setTab('carriers')">Carriers</button>
    <button class="tab" onclick="setTab('ads')">Google Ads</button>
  </div>

  <!-- P&L RESUMEN -->
  <div class="tab-panel active" id="panel-resumen">
    <div id="alerts-strip"></div>
    <div id="semaphore"></div>
    <div id="pnl-content">
      <div class="empty"><div class="empty-icon">◎</div><div class="empty-title">Carga datos para ver el P&L</div><div class="empty-sub">Sube las facturas de transportistas y el export de Odoo</div></div>
    </div>
  </div>

  <!-- POR PAÍS -->
  <div class="tab-panel" id="panel-paises">
    <div id="paises-content"><div class="empty"><div class="empty-icon">◎</div><div class="empty-title">Sin datos por país</div></div></div>
  </div>

  <!-- EVOLUCIÓN -->
  <div class="tab-panel" id="panel-evolucion">
    <div id="evolucion-content"><div class="empty"><div class="empty-icon">◎</div><div class="empty-title">Calcula el P&L primero</div></div></div>
  </div>

  <!-- COMPARAR -->
  <div class="tab-panel" id="panel-comparar">
    <div class="cmp-header">
      <span class="cmp-label">Comparar</span>
      <select class="sel" id="cmp-a"><option value="">Período A</option></select>
      <span class="cmp-vs">vs</span>
      <select class="sel" id="cmp-b"><option value="">Período B</option></select>
      <button class="btn btn-primary btn-sm" onclick="runCompare()">Comparar</button>
    </div>
    <div id="compare-content"><div class="empty"><div class="empty-icon">◎</div><div class="empty-title">Selecciona dos períodos y pulsa Comparar</div></div></div>
  </div>

  <!-- CARRIERS -->
  <div class="tab-panel" id="panel-carriers">
    <div id="carriers-content"><div class="empty"><div class="empty-icon">◎</div><div class="empty-title">Sin facturas de transportistas</div></div></div>
  </div>

  <!-- RECLAMACIONES -->

  <!-- ADS -->
  <div class="tab-panel" id="panel-ads">
    <div id="ads-content"><div class="empty"><div class="empty-icon">◎</div><div class="empty-title">Sin datos de publicidad</div></div></div>
  </div>
</main>
</div>

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

// ── UTILS ──────────────────────────────────────────────────────
function fe(v,d=0){
  if(v==null||isNaN(v)) return '—';
  const s=Math.abs(v).toLocaleString('es-ES',{minimumFractionDigits:d,maximumFractionDigits:d});
  return (v<0?'−':'')+s+'\u202f€';
}
function fp(v){return v==null||isNaN(v)?'—':(v*100).toFixed(1)+'%'}
function fn(v){return v==null?'—':Number(v).toLocaleString('es-ES')}
function cm(v){return v==null?'':v>=0?'pos':'neg'}
function delta(b,a,pct=false){
  if(b==null||a==null) return {v:null,cls:'',str:'—'};
  const d=b-a;
  const cls=d>0?'up':d<0?'down':'';
  const str=pct?fp(d):(d>=0?'+':'')+fe(d,0);
  return {v:d,cls,str};
}

// ── LOADING ────────────────────────────────────────────────────
function showL(msg='Calculando...'){document.getElementById('lm').textContent=msg;document.getElementById('lo').classList.add('show');document.getElementById('pf').style.width='0%'}
function hideL(){document.getElementById('lo').classList.remove('show')}
function prog(p){document.getElementById('pf').style.width=p+'%'}

function toast(msg,type='info'){
  const t=document.getElementById('toast');
  t.textContent=msg;t.className=type;t.style.display='block';
  clearTimeout(t._t);t._t=setTimeout(()=>t.style.display='none',4000);
}

function triggerUpload(c){document.getElementById('file-'+c).click()}

async function uploadFile(carrier,input){
  const file=input.files[0];if(!file)return;
  showL('Procesando '+carrier.toUpperCase()+'…');
  const fd=new FormData();fd.append('file',file);fd.append('carrier',carrier);
  try{
    prog(30);
    const res=await fetch('/upload',{method:'POST',body:fd});prog(85);
    const data=await res.json();prog(100);hideL();
    if(data.ok){
      toast(carrier.toUpperCase()+': '+data.rows+' envíos cargados','success');
      const btn=document.getElementById('btn-'+carrier);
      if(btn){btn.classList.add('loaded');btn.querySelector('.cb-status').textContent='✓ '+data.rows+(data.months?' · '+data.months:'');}
      refreshStatus();
    }else toast('Error en '+carrier+': '+data.error,'error');
  }catch(e){hideL();toast('Error: '+e.message,'error')}
  input.value='';
}

async function uploadSeed(input){
  const file=input.files[0];if(!file)return;
  showL('Importando histórico…');
  const fd=new FormData();fd.append('file',file);
  try{
    prog(40);const res=await fetch('/seed',{method:'POST',body:fd});prog(90);
    const data=await res.json();hideL();
    if(data.ok){toast('Importado: '+data.imported.join(', '),'success');refreshStatus();await loadPnl();}
    else toast('Error: '+data.error,'error');
  }catch(e){hideL();toast('Error: '+e.message,'error')}
  input.value='';
}

// ── P&L LOAD & RENDER ──────────────────────────────────────────
async function loadPnl(){
  const month=getFilterValue();
  showL('Calculando P&L…');
  try{
    prog(50);const res=await fetch('/pnl?month='+encodeURIComponent(month));prog(90);
    const data=await res.json();hideL();
    if(data.error){toast(data.error,'error');return;}
    pnlData=data;renderAll(data);toast('P&L actualizado','success');
  }catch(e){hideL();toast('Error: '+e.message,'error')}
}

function renderAll(data){
  renderPnlTable(data);
  renderSemaphore(data);
  renderAlertStrip(data);
  renderPaises(data);
  renderEvolucion(data);
  renderCarriers(data);
  renderAds(data);
}

// ── P&L TABLE ─────────────────────────────────────────────────
function renderPnlTable(data){
  const R=data.pnl_by_country||[];
  if(!R.length){document.getElementById('pnl-content').innerHTML='';return;}

  const venta       =R.reduce((a,r)=>a+(r.venta||0),0);
  const cogs        =R.reduce((a,r)=>a+(r.cogs||0),0);
  const mg_prod     =R.reduce((a,r)=>a+(r.mg_prod||0),0);
  const ing_env     =R.reduce((a,r)=>a+(r.ing_envio||0),0);
  const cost_env    =R.reduce((a,r)=>a+(r.cost_envio||0),0);
  const mg_env      =ing_env-cost_env;
  const mg_final    =R.reduce((a,r)=>a+(r.mg_final||0),0);
  const ads         =R.reduce((a,r)=>a+(r.gasto_ads||0),0);
  const mg_post     =R.reduce((a,r)=>a+(r.mg_post_ads!=null?r.mg_post_ads:r.mg_final||0),0);
  const retail_media=R.reduce((a,r)=>a+(r.retail_media||0),0);
  const inpost_comp =R.reduce((a,r)=>a+(r.inpost_comp||0),0);
  const n_ped       =R.reduce((a,r)=>a+(r.n_pedidos||0),0);
  const base        =venta+ing_env;

  const month=document.getElementById('global-month').value;
  const label=month?month:'Acumulado';

  const html=`
  <div class="card">
    <div class="card-hdr"><span class="card-title">Cuenta de resultados — ${label}</span><span class="card-sub">${fn(n_ped)} pedidos</span></div>
    <table class="pnl-table">
      <colgroup><col style="width:55%"><col style="width:22%"><col style="width:23%"></colgroup>
      <tr class="highlight"><td class="row-label section" colspan="3">INGRESOS</td></tr>
      <tr><td class="row-label indent">Venta bruta productos</td><td class="val">${fe(venta,0)}</td><td class="val pct"></td></tr>
      <tr><td class="row-label indent">Ingreso envío cobrado</td><td class="val">${fe(ing_env,0)}</td><td class="val pct"></td></tr>
      <tr class="highlight"><td class="row-label section" colspan="3">COSTES</td></tr>
      <tr><td class="row-label indent">Coste de mercancía (COGS)</td><td class="val neg">− ${fe(cogs,0)}</td><td class="val pct neg">${fp(cogs/venta)}</td></tr>
      <tr class="separator"><td colspan="3"></td></tr>
      <tr class="highlight"><td class="row-label total">Margen bruto producto</td><td class="val total ${cm(mg_prod)}">${fe(mg_prod,0)}</td><td class="val pct ${cm(mg_prod/venta)}">${fp(mg_prod/venta)}</td></tr>
      <tr class="separator"><td colspan="3"></td></tr>
      <tr><td class="row-label indent">Coste envío (carriers)</td><td class="val neg">− ${fe(cost_env,0)}</td><td class="val pct"></td></tr>
      <tr class="separator"><td colspan="3"></td></tr>
      <tr class="highlight"><td class="row-label total">Margen envío</td><td class="val total ${cm(mg_env)}">${fe(mg_env,0)}</td><td class="val pct ${cm(mg_env/base)}">${fp(mg_env/base)}</td></tr>
      <tr class="separator"><td colspan="3"></td></tr>
      <tr class="highlight" style="background:var(--acc-lt)"><td class="row-label total" style="color:var(--acc)">MARGEN OPERATIVO</td><td class="val total ${cm(mg_final)}" style="font-size:17px">${fe(mg_final,0)}</td><td class="val pct ${cm(mg_final/base)}" style="font-size:12px">${fp(mg_final/base)}</td></tr>
      ${(retail_media>0||inpost_comp>0)?`
      <tr><td class="row-label section" colspan="3">INGRESOS ADICIONALES</td></tr>
      ${retail_media>0?`<tr><td class="row-label indent">Retail media (marcas)</td><td class="val pos">+ ${fe(retail_media,0)}</td><td class="val pct pos">${fp(retail_media/base)}</td></tr>`:''}
      ${inpost_comp>0?`<tr><td class="row-label indent">Compensación InPost (Mondial Relay)</td><td class="val pos">+ ${fe(inpost_comp,0)}</td><td class="val pct pos">${fp(inpost_comp/base)}</td></tr>`:''}
      `:''}
      ${ads>0?`
      <tr><td class="row-label section" colspan="3">MARKETING</td></tr>
      <tr><td class="row-label indent">Google Ads</td><td class="val neg">− ${fe(ads,0)}</td><td class="val pct neg">${fp(ads/base)}</td></tr>
      `:''}
      ${(ads>0||retail_media>0||inpost_comp>0)?`
      <tr class="separator"><td colspan="3"></td></tr>
      <tr class="highlight" style="background:${mg_post>=0?'var(--grn-lt)':'var(--red-lt)'}"><td class="row-label total">RESULTADO FINAL</td><td class="val total ${cm(mg_post)}" style="font-size:17px">${fe(mg_post,0)}</td><td class="val pct ${cm(mg_post/base)}" style="font-size:12px">${fp(mg_post/base)}</td></tr>
      `:''}
    </table>
  </div>`;
  document.getElementById('pnl-content').innerHTML=html;
}

// ── SEMAPHORE ─────────────────────────────────────────────────
function renderSemaphore(data){
  const R=data.pnl_by_country||[];if(!R.length){document.getElementById('semaphore').innerHTML='';return;}
  const COUNTRIES=['España','Portugal','Francia','Italia','Alemania','Reino Unido'];
  const byC={};
  R.forEach(r=>{
    if(!byC[r.country])byC[r.country]={venta:0,mg_prod:0,ing_envio:0,cost_envio:0,mg_final:0,gasto_ads:0,mg_post_ads:0};
    ['venta','mg_prod','ing_envio','cost_envio','mg_final','gasto_ads'].forEach(k=>byC[r.country][k]+=(r[k]||0));
    byC[r.country].mg_post_ads+=(r.mg_post_ads!=null?r.mg_post_ads:r.mg_final||0);
  });
  const cards=COUNTRIES.filter(c=>byC[c]&&byC[c].venta>0).map(c=>{
    const d=byC[c];
    const base=d.venta+d.ing_envio;
    const pp=d.venta?d.mg_prod/d.venta:0;
    const pe=base?(d.ing_envio-d.cost_envio)/base:0;
    const pa=base?d.mg_post_ads/base:0;
    const overall=pa>0.08?'grn':pa>0.02?'yel':'red';
    const mv=(v,thr1,thr2)=>v>thr1?'pos':v>thr2?'warn':'neg';
    return `<div class="sem-card ${overall}">
      <div class="sem-country">${c}</div>
      <div class="sem-metrics">
        <div class="sem-m"><div class="sem-m-label">Mg Prod</div><div class="sem-m-val ${mv(pp,.15,.08)}">${fp(pp)}</div></div>
        <div class="sem-m"><div class="sem-m-label">Mg Envío</div><div class="sem-m-val ${mv(pe,.02,0)}">${fp(pe)}</div></div>
        <div class="sem-m"><div class="sem-m-label">Post-Ads</div><div class="sem-m-val ${mv(pa,.08,.02)}">${fp(pa)}</div></div>
      </div>
    </div>`;
  }).join('');
  document.getElementById('semaphore').innerHTML=`<div class="semaphore-grid" style="margin-bottom:16px">${cards}</div>`;
}

// ── ALERT STRIP ────────────────────────────────────────────────
function renderAlertStrip(data){
  const R=data.pnl_by_country||[];
  const ship=data.shipping||[];
  const ads=data.ads||[];
  const alerts=[];

  // Alert: country losing money post-ads
  const byC={};
  R.forEach(r=>{
    if(!byC[r.country])byC[r.country]={mg_post_ads:0,venta:0,ing_envio:0,gasto_ads:0};
    byC[r.country].mg_post_ads+=(r.mg_post_ads!=null?r.mg_post_ads:r.mg_final||0);
    byC[r.country].venta+=(r.venta||0);byC[r.country].ing_envio+=(r.ing_envio||0);
    byC[r.country].gasto_ads+=(r.gasto_ads||0);
  });
  Object.entries(byC).forEach(([c,d])=>{
    const base=d.venta+d.ing_envio;const pct=base?d.mg_post_ads/base:0;
    if(d.venta>5000&&pct<0)alerts.push({type:'urgent',icon:'🔴',title:`${c}: resultado negativo después de Ads`,desc:`Margen post-Ads ${fp(pct)} (${fe(d.mg_post_ads,0)}). Los Ads cuestan ${fe(d.gasto_ads,0)}.`,action:`Reducir inversión en Ads o revisar precio de envío en ${c}`,cls:'red'});
    else if(d.venta>5000&&pct<0.03)alerts.push({type:'warn',icon:'🟡',title:`${c}: margen muy ajustado post-Ads`,desc:`Solo un ${fp(pct)} de margen final. Cualquier subida de costes lo pondría en negativo.`,action:`Revisar estructura de costes en ${c}`,cls:'org'});
  });

  // Alert: carrier losing big
  const byCarrier={};
  ship.forEach(r=>{if(!byCarrier[r.carrier])byCarrier[r.carrier]={mg:0,n:0};byCarrier[r.carrier].mg+=(r.margen_envio||0);byCarrier[r.carrier].n+=(r.n_envios||0);});
  Object.entries(byCarrier).forEach(([c,d])=>{
    if(d.mg<-5000)alerts.push({type:'warn',icon:'📦',title:`${c}: −${Math.round(Math.abs(d.mg)).toLocaleString('es-ES')}€ en envíos`,desc:`${fn(d.n)} envíos con margen negativo acumulado. Coste real superior al precio contratado.`,action:'Revisar tarifas con el carrier o ajustar precios cobrados',cls:'org'});
  });

  // Alert: reclamaciones
  if(data.alert_count>0)alerts.push({type:'info',icon:'⚠️',title:`${fn(data.alert_count)} envíos para reclamar`,desc:`Pérdida total de ${fe(data.alert_total_loss||0,0)} en envíos donde el coste supera 3× lo cobrado.`,action:'Revisar envíos con pérdida anormal',cls:'acc'});

  if(!alerts.length){document.getElementById('alerts-strip').innerHTML='';return;}
  const html=alerts.slice(0,4).map(a=>`
    <div class="alert-card ${a.type}">
      <div class="alert-icon">${a.icon}</div>
      <div class="alert-body">
        <div class="alert-title">${a.title}</div>
        <div class="alert-desc">${a.desc}</div>
        <div class="alert-action ${a.cls}">→ ${a.action}</div>
      </div>
    </div>`).join('');
  document.getElementById('alerts-strip').innerHTML=`<div class="alert-cards">${html}</div>`;
}

// ── POR PAÍS ──────────────────────────────────────────────────
function renderPaises(data){
  const R=data.pnl_by_country||[];if(!R.length)return;
  const byC={};
  R.forEach(r=>{
    const c=r.country||'Desconocido';
    if(!byC[c])byC[c]={venta:0,cogs:0,mg_prod:0,ing_envio:0,cost_envio:0,mg_final:0,gasto_ads:0,mg_post_ads:0,n_pedidos:0};
    ['venta','cogs','mg_prod','ing_envio','cost_envio','mg_final','gasto_ads','n_pedidos'].forEach(k=>byC[c][k]+=(r[k]||0));
    byC[c].mg_post_ads+=(r.mg_post_ads!=null?r.mg_post_ads:r.mg_final||0);
  });
  const sorted=Object.entries(byC).filter(([,d])=>d.venta>0).sort((a,b)=>b[1].venta-a[1].venta);
  let tbl=`<table class="tbl"><thead><tr><th>País</th><th class="r">Pedidos</th><th class="r">Venta</th><th class="r">Mg Producto</th><th class="r">% Mg Prod</th><th class="r">Mg Envío</th><th class="r">Mg Final</th><th class="r">Ads</th><th class="r">Post-Ads</th><th class="r">%</th></tr></thead><tbody>`;
  sorted.forEach(([c,d])=>{
    const mge=d.ing_envio-d.cost_envio;const base=d.venta+d.ing_envio;
    const pp=d.venta?d.mg_prod/d.venta:0;const pa=base?d.mg_post_ads/base:0;
    tbl+=`<tr><td><strong>${c}</strong></td><td class="r dim">${fn(d.n_pedidos)}</td>
      <td class="r">${fe(d.venta,0)}</td><td class="r ${cm(d.mg_prod)}">${fe(d.mg_prod,0)}</td>
      <td class="r ${cm(pp)}">${fp(pp)}</td><td class="r ${cm(mge)}">${fe(mge,0)}</td>
      <td class="r ${cm(d.mg_final)}">${fe(d.mg_final,0)}</td>
      <td class="r" style="color:var(--org)">${d.gasto_ads>0?fe(-d.gasto_ads,0).replace('−',''):'—'}</td>
      <td class="r ${cm(d.mg_post_ads)}"><strong>${fe(d.mg_post_ads,0)}</strong></td>
      <td class="r ${cm(pa)}">${fp(pa)}</td></tr>`;
  });
  tbl+='</tbody></table>';
  document.getElementById('paises-content').innerHTML=`<div class="card"><div class="card-hdr"><span class="card-title">Rentabilidad por País</span><span class="card-sub">con Google Ads</span></div><div style="overflow-x:auto">${tbl}</div></div>`;
}

// ── EVOLUCIÓN ─────────────────────────────────────────────────
function renderEvolucion(data){
  const evo=data.monthly_by_country;const months=data.all_months||[];
  if(!evo||!months.length)return;
  const CTRS=['España','Portugal','Francia','Italia','Alemania','Reino Unido'];
  const MH=months.map(m=>m.replace('2025-','').replace('2026-',"'26-"));

  const mkTable=(label,valFn,fmtFn)=>{
    let t=`<table class="tbl"><thead><tr><th>${label}</th>${MH.map(m=>`<th class="r">${m}</th>`).join('')}<th class="r">Total</th></tr></thead><tbody>`;
    CTRS.forEach(c=>{
      const d=evo[c];if(!d)return;
      const vals=months.map(ym=>d[ym]?valFn(d[ym]):null);
      const tot=vals.reduce((a,v)=>a+(v||0),0);
      t+=`<tr><td><strong>${c}</strong></td>`;
      vals.forEach(v=>{t+=v!=null?`<td class="r ${cm(v)}">${fmtFn(v)}</td>`:`<td class="r dim">—</td>`;});
      t+=`<td class="r ${cm(tot)}"><strong>${fmtFn(tot)}</strong></td></tr>`;
    });
    return t+'</tbody></table>';
  };

  const html=
    `<div class="card"><div class="card-hdr"><span class="card-title">Margen post-Ads por País y Mes</span><span class="card-sub">margen final − Google Ads</span></div><div style="overflow-x:auto">${mkTable('País',r=>r.mg_post_ads!=null?r.mg_post_ads:r.mg_final,v=>fe(v,0))}</div></div>`+
    `<div class="card"><div class="card-hdr"><span class="card-title">% Margen post-Ads</span></div><div style="overflow-x:auto">${mkTable('País',r=>r.mg_post_ads_pct!=null?r.mg_post_ads_pct:r.mg_pct,v=>fp(v))}</div></div>`+
    `<div class="card"><div class="card-hdr"><span class="card-title">Pedidos por País y Mes</span></div><div style="overflow-x:auto">${mkTable('País',r=>r.n,v=>fn(v))}</div></div>`;
  document.getElementById('evolucion-content').innerHTML=html;
}

// ── COMPARAR ──────────────────────────────────────────────────
async function runCompare(){
  const a=document.getElementById('cmp-a').value;
  const b=document.getElementById('cmp-b').value;
  if(!a||!b){toast('Selecciona dos períodos','error');return;}
  showL('Comparando '+a+' vs '+b+'…');
  try{
    prog(50);const res=await fetch(`/compare?a=${a}&b=${b}`);prog(90);
    const data=await res.json();hideL();
    if(data.error){toast(data.error,'error');return;}
    renderCompare(data);
  }catch(e){hideL();toast('Error: '+e.message,'error')}
}

function renderCompare(data){
  const {a,b,delta:d,ym_a,ym_b}=data;
  if(!a||!b){document.getElementById('compare-content').innerHTML='<div class="empty"><div class="empty-title">Sin datos para uno o ambos períodos</div></div>';return;}

  const ROWS=[
    {label:'Pedidos',            key:'n_pedidos',      fmt:fn,   pct:false},
    {label:'Venta bruta',        key:'venta',           fmt:v=>fe(v,0), pct:false},
    {label:'',sep:true},
    {label:'COGS',               key:'cogs',            fmt:v=>fe(v,0), pct:false, neg:true},
    {label:'Margen producto',    key:'mg_prod',         fmt:v=>fe(v,0), pct:false, bold:true},
    {label:'% Margen producto',  key:'mg_prod_pct',     fmt:fp, pct:true},
    {label:'',sep:true},
    {label:'Coste envío',        key:'cost_envio',      fmt:v=>fe(v,0), pct:false, neg:true},
    {label:'Margen envío',       key:'mg_envio',        fmt:v=>fe(v,0), pct:false},
    {label:'',sep:true},
    {label:'Margen final',       key:'mg_final',        fmt:v=>fe(v,0), pct:false, bold:true},
    {label:'% Margen final',     key:'mg_final_pct',    fmt:fp, pct:true},
    {label:'',sep:true},
    {label:'Google Ads',         key:'gasto_ads',       fmt:v=>fe(v,0), pct:false, neg:true},
    {label:'Resultado post-Ads', key:'mg_post_ads',     fmt:v=>fe(v,0), pct:false, bold:true},
    {label:'% Post-Ads',         key:'mg_post_ads_pct', fmt:fp, pct:true},
  ];

  let tbl=`<table class="pnl-table">
    <colgroup><col style="width:36%"><col style="width:20%"><col style="width:20%"><col style="width:24%"></colgroup>
    <thead><tr>
      <th>Concepto</th>
      <th class="col-a" style="text-align:right;padding:8px 14px">${ym_a}</th>
      <th class="col-b" style="text-align:right;padding:8px 14px">${ym_b}</th>
      <th style="text-align:right;padding:8px 14px">Variación</th>
    </tr></thead><tbody>`;

  ROWS.forEach(row=>{
    if(row.sep){tbl+=`<tr class="separator"><td colspan="4"></td></tr>`;return;}
    const va=a[row.key];const vb=b[row.key];
    const diff=delta(vb,va,row.pct);
    const arrow=diff.v==null?'':diff.v>0?'↑':diff.v<0?'↓':'→';
    const bold=row.bold?'font-weight:700;':'';
    const fs=row.bold?'font-size:14px;':'';
    tbl+=`<tr ${row.bold?'class="highlight"':''}>
      <td class="row-label${row.bold?' total':''}">${row.label}</td>
      <td class="val col-a ${cm(row.neg?-(va||0):(va||0))}" style="${bold}${fs}">${va!=null?row.fmt(va):'—'}</td>
      <td class="val col-b ${cm(row.neg?-(vb||0):(vb||0))}" style="${bold}${fs}">${vb!=null?row.fmt(vb):'—'}</td>
      <td class="val col-delta ${diff.cls}" style="${bold}${fs}">${arrow} ${diff.str}</td>
    </tr>`;
  });
  tbl+='</tbody></table>';

  // Country breakdown comparison
  let cmp_ctrs='';
  if(a.by_country&&b.by_country){
    const all_c=[...new Set([...Object.keys(a.by_country||{}),...Object.keys(b.by_country||{})])].filter(c=>
      (a.by_country[c]?.venta||0)+(b.by_country[c]?.venta||0)>0
    ).sort((x,y)=>(b.by_country[y]?.venta||0)-(b.by_country[x]?.venta||0));

    let ct=`<table class="tbl"><thead><tr><th>País</th><th class="r">${ym_a} Post-Ads</th><th class="r">${ym_b} Post-Ads</th><th class="r">Variación</th><th class="r">${ym_a} %</th><th class="r">${ym_b} %</th></tr></thead><tbody>`;
    all_c.forEach(c=>{
      const ra=a.by_country[c]||{};const rb=b.by_country[c]||{};
      const mpa_a=ra.mg_post_ads!=null?ra.mg_post_ads:ra.mg_final||0;
      const mpa_b=rb.mg_post_ads!=null?rb.mg_post_ads:rb.mg_final||0;
      const diff=delta(mpa_b,mpa_a);
      const ppa=ra.mg_post_ads_pct!=null?ra.mg_post_ads_pct:ra.mg_pct||0;
      const ppb=rb.mg_post_ads_pct!=null?rb.mg_post_ads_pct:rb.mg_pct||0;
      ct+=`<tr><td><strong>${c}</strong></td>
        <td class="r ${cm(mpa_a)}">${fe(mpa_a,0)}</td>
        <td class="r ${cm(mpa_b)}">${fe(mpa_b,0)}</td>
        <td class="r col-delta ${diff.cls}">${diff.v>0?'↑':'↓'} ${diff.str}</td>
        <td class="r ${cm(ppa)}">${fp(ppa)}</td>
        <td class="r ${cm(ppb)}">${fp(ppb)}</td></tr>`;
    });
    ct+='</tbody></table>';
    cmp_ctrs=`<div class="card" style="margin-top:14px"><div class="card-hdr"><span class="card-title">Por País</span></div><div style="overflow-x:auto">${ct}</div></div>`;
  }

  document.getElementById('compare-content').innerHTML=
    `<div class="card"><div class="card-hdr"><span class="card-title">${ym_a} vs ${ym_b}</span><span class="card-sub">cuenta de resultados comparada</span></div>${tbl}</div>${cmp_ctrs}`;
}

// ── CARRIERS ──────────────────────────────────────────────────
function renderCarriers(data){
  const rows=data.shipping;if(!rows||!rows.length)return;

  // InPost compensation by month (from pnl_by_country España)
  const inpostCompByYm={};
  (data.pnl_by_country||[]).filter(r=>r.country==='España').forEach(r=>{
    if(r.inpost_comp) inpostCompByYm[r.ym]=(inpostCompByYm[r.ym]||0)+r.inpost_comp;
  });

  // ── TABLE: by carrier, with InPost comp column ────────────────
  const byC={};
  rows.forEach(r=>{
    const k=r.carrier;
    if(!byC[k])byC[k]={n:0,cost:0,ing:0,mg:0};
    byC[k].n+=r.n_envios||0;byC[k].cost+=r.coste_total||0;
    byC[k].ing+=r.ingreso_total||0;byC[k].mg+=r.margen_envio||0;
  });
  const totalComp=Object.values(inpostCompByYm).reduce((a,v)=>a+v,0);

  let t=`<table class="tbl"><thead><tr>
    <th>Carrier</th><th class="r">Envíos</th><th class="r">Ingreso cobrado</th>
    <th class="r">Coste carrier</th><th class="r">Margen bruto</th>
    <th class="r">Compensación</th><th class="r">Margen neto</th><th class="r">€/envío neto</th>
  </tr></thead><tbody>`;
  Object.entries(byC).sort((a,b)=>a[1].mg-b[1].mg).forEach(([c,d])=>{
    const comp=c==='InPost'?totalComp:0;
    const mgNet=d.mg+comp;const ppe=d.n?mgNet/d.n:0;
    t+=`<tr>
      <td><strong>${c}</strong></td><td class="r dim">${fn(d.n)}</td>
      <td class="r">${fe(d.ing,0)}</td>
      <td class="r neg">${fe(-d.cost,0).replace('−','')}</td>
      <td class="r ${cm(d.mg)}">${fe(d.mg,0)}</td>
      <td class="r ${comp>0?'pos':'dim'}">${comp>0?'+ '+fe(comp,0):'—'}</td>
      <td class="r ${cm(mgNet)}"><strong>${fe(mgNet,0)}</strong></td>
      <td class="r ${cm(ppe)}">${fe(ppe,2)}</td>
    </tr>`;
  });
  t+='</tbody></table>';

  // ── CHART 1: envíos por carrier por mes ──────────────────────
  const months=data.all_months||[];
  const carriers=[...new Set(rows.map(r=>r.carrier))].sort();
  const COLORS={'CTT':'#1e4d7b','InPost':'#2a7a4b','Spring':'#b85c0a','GLS':'#7030a0','UPS':'#b93535','Asendia':'#5a524a'};
  const BAR_H=22;const CHART_W=560;const LEFT=60;const TOP=30;const BOTTOM=30;
  const monthEnvios={};
  rows.forEach(r=>{
    const k=r.ym;if(!monthEnvios[k])monthEnvios[k]={};
    monthEnvios[k][r.carrier]=(monthEnvios[k][r.carrier]||0)+(r.n_envios||0);
  });
  const visMonths=months.filter(m=>monthEnvios[m]);
  const maxEnvios=Math.max(...visMonths.map(m=>Object.values(monthEnvios[m]||{}).reduce((a,v)=>a+v,0)));
  const chartH=visMonths.length*BAR_H+TOP+BOTTOM;

  let bars='';
  visMonths.forEach((ym,mi)=>{
    const y=TOP+mi*BAR_H;const total=Object.values(monthEnvios[ym]||{}).reduce((a,v)=>a+v,0);
    let xOff=LEFT;
    carriers.forEach(c=>{
      const n=monthEnvios[ym]?.[c]||0;if(!n)return;
      const w=Math.round((n/maxEnvios)*(CHART_W-LEFT-4));
      bars+=`<rect x="${xOff}" y="${y+2}" width="${w}" height="${BAR_H-4}" fill="${COLORS[c]||'#888'}" rx="2" opacity="0.85">
        <title>${c}: ${fn(n)} envíos</title></rect>`;
      xOff+=w;
    });
    bars+=`<text x="${LEFT-4}" y="${y+BAR_H/2+4}" text-anchor="end" font-size="9" fill="#9a8f84">${ym.replace('2025-','').replace('2026-',"'26-")}</text>`;
    bars+=`<text x="${xOff+4}" y="${y+BAR_H/2+4}" font-size="9" fill="#5a524a">${fn(total)}</text>`;
  });
  // Legend
  let legend='';let lx=LEFT;
  carriers.forEach(c=>{
    legend+=`<rect x="${lx}" y="${chartH-16}" width="10" height="10" fill="${COLORS[c]||'#888'}" rx="2"/>`;
    legend+=`<text x="${lx+13}" y="${chartH-7}" font-size="9" fill="#5a524a">${c}</text>`;
    lx+=60;
  });
  const svgEnvios=`<svg width="100%" viewBox="0 0 ${CHART_W} ${chartH}" xmlns="http://www.w3.org/2000/svg" style="font-family:'DM Sans',sans-serif">
    <text x="${LEFT}" y="16" font-size="11" font-weight="600" fill="#1a1714">Envíos por carrier y mes</text>
    ${bars}${legend}
  </svg>`;

  // ── CHART 2: margen neto por carrier por mes ──────────────────
  const monthMg={};
  rows.forEach(r=>{
    const k=r.ym;if(!monthMg[k])monthMg[k]={};
    monthMg[k][r.carrier]=(monthMg[k][r.carrier]||0)+(r.margen_envio||0);
  });
  // Add InPost compensation
  Object.entries(inpostCompByYm).forEach(([ym,comp])=>{
    if(!monthMg[ym])monthMg[ym]={};
    monthMg[ym]['InPost']=(monthMg[ym]['InPost']||0)+comp;
  });

  const allMgVals=visMonths.flatMap(m=>Object.values(monthMg[m]||{}));
  const maxMg=Math.max(...allMgVals.map(Math.abs),1);
  const MG_W=CHART_W-LEFT-80;const ZERO=LEFT+Math.round(MG_W*0.4);
  const chartH2=visMonths.length*BAR_H+TOP+BOTTOM+20;
  let bars2='';
  // Zero line
  bars2+=`<line x1="${ZERO}" y1="${TOP}" x2="${ZERO}" y2="${chartH2-BOTTOM-20}" stroke="#e4dfd8" stroke-width="1"/>`;
  bars2+=`<text x="${ZERO}" y="${TOP-4}" text-anchor="middle" font-size="8" fill="#9a8f84">0</text>`;
  visMonths.forEach((ym,mi)=>{
    const y=TOP+mi*BAR_H;
    carriers.forEach(c=>{
      const v=monthMg[ym]?.[c]||0;if(!v)return;
      const w=Math.round((Math.abs(v)/maxMg)*(MG_W*0.55));
      const x=v>=0?ZERO:ZERO-w;
      bars2+=`<rect x="${x}" y="${y+2}" width="${w}" height="${BAR_H-4}" fill="${COLORS[c]||'#888'}" rx="2" opacity="${v>=0?0.85:0.6}">
        <title>${c} ${ym}: ${fe(v,0)}</title></rect>`;
    });
    const totalMg=Object.values(monthMg[ym]||{}).reduce((a,v)=>a+v,0);
    bars2+=`<text x="${LEFT-4}" y="${y+BAR_H/2+4}" text-anchor="end" font-size="9" fill="#9a8f84">${ym.replace('2025-','').replace('2026-',"'26-")}</text>`;
    const tx=totalMg>=0?ZERO+Math.round((totalMg/maxMg)*MG_W*0.55)+4:ZERO-Math.round((Math.abs(totalMg)/maxMg)*MG_W*0.55)-4;
    bars2+=`<text x="${tx}" y="${y+BAR_H/2+4}" font-size="9" fill="${totalMg>=0?'#2a7a4b':'#b93535'}" text-anchor="${totalMg>=0?'start':'end'}">${fe(totalMg,0)}</text>`;
  });
  bars2+=legend.replace(`y="${chartH-16}"`,`y="${chartH2-36}"`).replace(`y="${chartH-7}"`,`y="${chartH2-27}"`);
  const svgMg=`<svg width="100%" viewBox="0 0 ${CHART_W} ${chartH2}" xmlns="http://www.w3.org/2000/svg" style="font-family:'DM Sans',sans-serif">
    <text x="${LEFT}" y="16" font-size="11" font-weight="600" fill="#1a1714">Margen neto por carrier y mes (con compensación InPost)</text>
    ${bars2}
  </svg>`;

  document.getElementById('carriers-content').innerHTML=
    `<div class="card"><div class="card-hdr"><span class="card-title">Resumen por Carrier</span><span class="card-sub">compensación InPost/Mondial Relay incluida</span></div><div style="overflow-x:auto">${t}</div></div>`+
    `<div style="display:grid;grid-template-columns:1fr 1fr;gap:14px;margin-top:14px">`+
    `<div class="card"><div class="card-body" style="padding:14px">${svgEnvios}</div></div>`+
    `<div class="card"><div class="card-body" style="padding:14px">${svgMg}</div></div>`+
    `</div>`;
}


// ── ADS ───────────────────────────────────────────────────────
function renderAds(data){
  const ads=data.ads;if(!ads||!ads.length)return;
  const R=data.pnl_by_country||[];
  const mgLookup={};R.forEach(r=>{mgLookup[`${r.country}|${r.ym}`]=r.mg_post_ads!=null?r.mg_post_ads:r.mg_final||0;});
  let t=`<table class="tbl"><thead><tr><th>País</th><th>Mes</th><th class="r">Gasto Ads</th><th class="r">Conversiones</th><th class="r">ROAS</th><th class="r">Mg Final</th><th class="r">Post-Ads</th></tr></thead><tbody>`;
  ads.sort((a,b)=>a.pais.localeCompare(b.pais)||a.ym.localeCompare(b.ym)).forEach(a=>{
    const roas=a.roas||0;const rc=roas>=6?'pos':roas>=3?'warn':'neg';
    const mg=mgLookup[`${a.pais}|${a.ym}`]??null;
    const pa=mg!=null?mg-(a.gasto_ads||0):null;
    t+=`<tr><td><strong>${a.pais}</strong></td><td class="dim">${a.ym}</td>
      <td class="r neg">${fe(-a.gasto_ads,0).replace('−','')}</td>
      <td class="r dim">${fn(Math.round(a.conversiones||0))}</td>
      <td class="r ${rc}">${roas?roas.toFixed(1)+'×':'—'}</td>
      <td class="r ${mg!=null?cm(mg):''}">${mg!=null?fe(mg,0):'—'}</td>
      <td class="r ${pa!=null?cm(pa):''}">${pa!=null?`<strong>${fe(pa,0)}</strong>`:'—'}</td></tr>`;
  });
  t+='</tbody></table>';
  document.getElementById('ads-content').innerHTML=`<div class="card"><div class="card-hdr"><span class="card-title">Google Ads + Margen post-publicidad</span></div><div style="overflow-x:auto">${t}</div></div>`;
}

// ── EXPORTS ───────────────────────────────────────────────────
async function exportExcel(){
  const month=getFilterValue();
  showL('Generando Excel…');
  try{const res=await fetch('/export/excel?month='+encodeURIComponent(month));const blob=await res.blob();const a=document.createElement('a');a.href=URL.createObjectURL(blob);a.download=`Farma2go_PL_${month||'completo'}_${new Date().toISOString().slice(0,10)}.xlsx`;a.click();URL.revokeObjectURL(a.href);hideL();toast('Excel descargado','success');}
  catch(e){hideL();toast('Error: '+e.message,'error')}
}

// ── STATUS & UTILS ────────────────────────────────────────────
async function clearAll(){
  if(!confirm('¿Borrar todos los datos?'))return;
  await fetch('/clear',{method:'POST'});toast('Datos borrados','info');
  document.querySelectorAll('.carrier-btn').forEach(b=>b.classList.remove('loaded'));
  refreshStatus();pnlData=null;
  ['pnl-content','semaphore','alerts-strip','paises-content','evolucion-content','compare-content','carriers-content','ads-content'].forEach(id=>{
    const el=document.getElementById(id);
    if(el)el.innerHTML='<div class="empty"><div class="empty-icon">◎</div><div class="empty-sub">Sin datos</div></div>';
  });
}

const MONTHS_ES = {
  '01':'Ene','02':'Feb','03':'Mar','04':'Abr','05':'May','06':'Jun',
  '07':'Jul','08':'Ago','09':'Sep','10':'Oct','11':'Nov','12':'Dic'
};
function fmtMonth(ym) {
  const [y,m] = ym.split('-');
  return `${MONTHS_ES[m]||m} ${y.slice(2)}`;  // "Mar 25"
}

let _allMonths = [];
let _activeFilter = '';

function setFilter(val) {
  _activeFilter = val;
  // Highlight active filter buttons
  const filters = {'': 'btn-f-all', '2025': 'btn-f-2025', '2026': 'btn-f-2026'};
  Object.entries(filters).forEach(([fval, fid]) => {
    const btn = document.getElementById(fid);
    if(!btn) return;
    const active = (val === fval) || (val && val.length===7 && fval==='' );
    const isActive = val === fval;
    btn.style.background = isActive ? 'var(--acc)' : '';
    btn.style.color = isActive ? '#fff' : '';
    btn.style.borderColor = isActive ? 'var(--acc)' : '';
  });
  // Sync month dropdown
  const sel = document.getElementById('global-month');
  if(sel){ sel.value = (val && val.length===7) ? val : ''; }
  loadPnl();
}

function onMonthChange(val) {
  // Called when user picks a specific month from the dropdown
  if(val) setFilter(val);
  else setFilter('');
}

function getFilterValue() {
  // Returns the value to pass to /pnl
  if(_activeFilter === '2025') return '2025';
  if(_activeFilter === '2026') return '2026';
  if(_activeFilter && _activeFilter.length === 7) return _activeFilter;
  return '';
}

async function refreshStatus(){
  const res=await fetch('/status');const data=await res.json();
  const el=document.getElementById('data-status');
  if(!data.files||!Object.keys(data.files).length){el.innerHTML='<div class="ds-row"><div class="ds-dot no"></div><div class="ds-label">Sin datos cargados</div></div>';return;}
  if(data.months&&data.months.length){
    // Sort chronologically
    _allMonths = [...data.months].sort();
    const years = [...new Set(_allMonths.map(m=>m.split('-')[0]))].sort();

    // Main selector: grouped by year
    const sel = document.getElementById('global-month');
    const cur = sel.value;
    let opts = '<option value="">Mes concreto…</option>';
    years.reverse().forEach(y => {
      const yMonths = _allMonths.filter(m=>m.startsWith(y)).reverse();
      opts += `<optgroup label="${y}">`;
      yMonths.forEach(m => { opts += `<option value="${m}">${fmtMonth(m)}</option>`; });
      opts += '</optgroup>';
    });
    sel.innerHTML = opts;
    if(cur && _allMonths.includes(cur)) sel.value = cur;

    // Comparison selectors
    const b = document.getElementById('cmp-a');
    const c = document.getElementById('cmp-b');
    let cmpOpts = '<option value="">— elegir —</option>';
    [..._allMonths].reverse().forEach(m => { cmpOpts += `<option value="${m}">${fmtMonth(m)} (${m})</option>`; });
    b.innerHTML = cmpOpts;
    c.innerHTML = cmpOpts;
    // Default comparison: last two available months
    if(_allMonths.length >= 2) {
      b.value = _allMonths[_allMonths.length-2];
      c.value = _allMonths[_allMonths.length-1];
    }
  }
  el.innerHTML=Object.entries(data.files).map(([k,v])=>`<div class="ds-row"><div class="ds-dot ${v.rows&&v.rows>0?'ok':'warn'}"></div><div class="ds-label">${k}</div><div class="ds-val">${v.rows||0}</div></div>`).join('');
}

function setTab(id){
  document.querySelectorAll('.tab').forEach(t=>t.classList.remove('active'));
  document.querySelectorAll('.tab-panel').forEach(p=>p.classList.remove('active'));
  event.target.classList.add('active');
  document.getElementById('panel-'+id).classList.add('active');
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
    return jsonify(clean_nan(result))


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




@app.route('/compare')
def compare():
    ym_a = request.args.get('a', '').strip()
    ym_b = request.args.get('b', '').strip()
    if not ym_a or not ym_b:
        return jsonify({'error': 'Faltan períodos'})
    from engine import build_comparison
    result = build_comparison(ym_a, ym_b)
    return jsonify(clean_nan(result))

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
            raw = zf.read(name).decode('utf-8')
            # Fix NaN before saving (pandas may have written NaN instead of null)
            import re as _re
            raw = _re.sub(r':NaN\b', ':null', raw)
            raw = _re.sub(r':Infinity\b', ':null', raw)
            raw = _re.sub(r':-Infinity\b', ':null', raw)
            dest = os.path.join(DATA_DIR, name)
            with open(dest, 'w', encoding='utf-8') as f_out:
                f_out.write(raw)
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
