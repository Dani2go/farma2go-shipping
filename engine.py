"""
engine.py — Farma2go P&L Calculation Engine
Combines carrier invoices + Odoo sales + Ads spend into P&L.
"""

import pandas as pd
import numpy as np
import json
import os
from datetime import datetime

# Use STORAGE_PATH env var if set (Railway volume), else local /data folder
DATA_DIR = os.environ.get('STORAGE_PATH', os.path.join(os.path.dirname(__file__), 'data'))
os.makedirs(DATA_DIR, exist_ok=True)

ALERT_THRESHOLD_EUR = 8.0   # flag shipment if loss > 8€ AND paid
ALERT_RATIO         = 3.0   # flag if cost/charged > 3x

COUNTRY_NORM = {
    'ES':'España','España':'España','PT':'Portugal','Portugal':'Portugal',
    'FR':'Francia','Francia':'Francia','DE':'Alemania','Alemania':'Alemania',
    'IT':'Italia','Italia':'Italia','GB':'Reino Unido','UK':'Reino Unido','Reino Unido':'Reino Unido',
    'BE':'Bélgica','Bélgica':'Bélgica','NL':'Holanda','Países Bajos':'Holanda','Holanda':'Holanda',
    'LU':'Luxemburgo',
}


def save_data(key, df_or_dict):
    path = os.path.join(DATA_DIR, f'{key}.json')
    if isinstance(df_or_dict, pd.DataFrame):
        # Use to_json with double_precision and then fix NaN → null
        import re
        raw = df_or_dict.to_json(orient='records', force_ascii=False)
        # pandas writes NaN as NaN which is invalid JSON — replace with null
        raw = re.sub(r':NaN', ':null', raw)
        raw = re.sub(r':Infinity', ':null', raw)
        raw = re.sub(r':-Infinity', ':null', raw)
        with open(path, 'w', encoding='utf-8') as f:
            f.write(raw)
    else:
        with open(path, 'w', encoding='utf-8') as f:
            json.dump(df_or_dict, f, ensure_ascii=False)


def load_data(key):
    path = os.path.join(DATA_DIR, f'{key}.json')
    if not os.path.exists(path):
        return None
    try:
        with open(path, 'r', encoding='utf-8') as f:
            raw = json.load(f)
        # If it's a list of records → DataFrame; if dict → return as dict
        if isinstance(raw, list):
            return pd.DataFrame(raw)
        return raw  # dict (e.g. retail_media, shopify_revenue)
    except Exception:
        return None


def list_saved():
    files = {}
    for f in os.listdir(DATA_DIR):
        if f.endswith('.json'):
            key = f[:-5]
            path = os.path.join(DATA_DIR, f)
            size = os.path.getsize(path)
            mtime = datetime.fromtimestamp(os.path.getmtime(path)).strftime('%Y-%m-%d %H:%M')
            # Try to get row count
            try:
                df = pd.read_json(path, orient='records')
                rows = len(df)
            except:
                rows = '?'
            files[key] = {'rows': rows, 'size_kb': round(size/1024, 1), 'updated': mtime}
    return files


def build_pnl(ym_filter=None):
    """
    Build complete P&L combining all loaded data.
    Returns dict with pnl_by_country, pnl_by_carrier, alerts, summary.
    """
    # Load all available data
    shipping_df  = load_data('shipping_costs')   # carrier invoices
    sales_df     = load_data('odoo_sales')        # Odoo order lines
    ads_df       = load_data('google_ads')        # Ads spend
    retail_raw   = load_data('retail_media')      # Retail media income + InPost compensation

    # Parse retail_media (stored as dict, not DataFrame)
    retail_media_by_ym      = {}  # ym → retail media income
    inpost_comp_by_ym       = {}  # ym → InPost/Mondial compensation
    if isinstance(retail_raw, dict):
        retail_media_by_ym  = retail_raw.get('retail_media', {})
        inpost_comp_by_ym   = retail_raw.get('inpost_compensacion', {})
    elif retail_raw is not None and hasattr(retail_raw, 'iterrows'):
        # fallback if loaded as dataframe
        pass

    if shipping_df is None and sales_df is None:
        return {'error': 'No hay datos cargados. Sube al menos las facturas de transportistas o el listado de ventas de Odoo.'}

    # Exclude 2024 — incomplete data
    for df_name, df_obj in [('shipping_df', shipping_df), ('sales_df', sales_df), ('ads_df', ads_df)]:
        pass  # done per-df below
    if shipping_df is not None:
        shipping_df = shipping_df[~shipping_df['ym'].astype(str).str.startswith('2024')]
    if sales_df is not None:
        sales_df = sales_df[~sales_df['ym'].astype(str).str.startswith('2024')]
    if ads_df is not None:
        ads_df = ads_df[~ads_df['ym'].astype(str).str.startswith('2024')]

    results = {}

    # ── SHIPPING MARGIN ───────────────────────────────────────────
    if shipping_df is not None:
        if ym_filter:
            if isinstance(ym_filter, str) and len(ym_filter) == 4:
                # Year filter e.g. "2025" or "2026"
                shipping_df = shipping_df[shipping_df['ym'].str.startswith(ym_filter)]
            else:
                months = [ym_filter] if isinstance(ym_filter, str) else ym_filter
                shipping_df = shipping_df[shipping_df['ym'].isin(months)]

        ship_summary = shipping_df.groupby(['carrier','country','ym']).agg(
            n_envios=('ref','count'),
            coste_total=('cost_eur','sum'),
            ingreso_total=('precio_envio','sum'),
            margen_envio=('margin','sum'),
        ).reset_index()
        results['shipping'] = ship_summary.to_dict('records')

        # Country × month shipping
        cty_ship = shipping_df.groupby(['country','ym']).agg(
            n_envios=('ref','count'),
            coste_envio=('cost_eur','sum'),
            ingreso_envio=('precio_envio','sum'),
            margen_envio=('margin','sum'),
        ).reset_index()
        results['country_shipping'] = cty_ship.to_dict('records')

    # ── PRODUCT MARGIN ────────────────────────────────────────────
    if sales_df is not None:
        if ym_filter:
            if isinstance(ym_filter, str) and len(ym_filter) == 4:
                sales_df = sales_df[sales_df['ym'].str.startswith(ym_filter)]
            else:
                months = [ym_filter] if isinstance(ym_filter, str) else ym_filter
                sales_df = sales_df[sales_df['ym'].isin(months)]

        # Detect format: pre-aggregated (has 'venta','cogs','mg_prod')
        # vs raw lines (has 'venta_total','is_shipping')
        is_aggregated = 'mg_prod' in sales_df.columns and 'venta' in sales_df.columns

        if is_aggregated:
            # Already order-level — use directly
            order_merged = sales_df.copy()
            order_merged['ing_envio'] = pd.to_numeric(order_merged.get('ing_envio', 0), errors='coerce').fillna(0)
        else:
            # Raw lines format — aggregate
            from parsers import SHIPPING_PRODUCTS
            prod_df   = sales_df[~sales_df['is_shipping']].copy() if 'is_shipping' in sales_df.columns else sales_df
            ship_lines = sales_df[sales_df['is_shipping']].copy() if 'is_shipping' in sales_df.columns else pd.DataFrame()

            order_prod = prod_df.groupby('ref_odoo').agg(
                plataforma=('plataforma','first'),
                ym=('ym','first'),
                venta=('venta_total','sum'),
                cogs=('coste_total','sum'),
            ).reset_index()
            order_prod['mg_prod'] = order_prod['venta'] - order_prod['cogs']

            order_ship_rev = ship_lines.groupby('ref_odoo').agg(
                ing_envio=('venta_total','sum')
            ).reset_index() if len(ship_lines) else pd.DataFrame(columns=['ref_odoo','ing_envio'])

            order_merged = order_prod.merge(order_ship_rev, on='ref_odoo', how='left')
            order_merged['ing_envio'] = order_merged['ing_envio'].fillna(0)

        # Merge with carrier shipping costs via ref_shopify
        if shipping_df is not None and 'ref' in shipping_df.columns:
            ship_cost_by_order = shipping_df.groupby('ref').agg(
                cost_envio=('cost_eur','sum'),
                carrier=('carrier','first'),
                country_carrier=('country','first'),
            ).reset_index()
            join_col = 'ref_shopify' if 'ref_shopify' in order_merged.columns else None
            if join_col:
                order_merged[join_col] = order_merged[join_col].astype(str).str.lstrip('#').str.strip()
                order_merged = order_merged.merge(
                    ship_cost_by_order, left_on=join_col, right_on='ref', how='left'
                )

        if 'cost_envio' in order_merged.columns:
            order_merged['cost_envio'] = pd.to_numeric(order_merged['cost_envio'], errors='coerce').fillna(0)
        else:
            order_merged['cost_envio'] = 0

        order_merged['mg_envio'] = order_merged['ing_envio'] - order_merged['cost_envio']
        order_merged['mg_final'] = order_merged['mg_prod'] + order_merged['mg_envio']

        results['order_count'] = len(order_merged)

        # Country × month P&L
        # 'country' col comes from odoo_sales (enriched via shipping crosswalk)
        # Use it directly; fall back to country_carrier or plataforma
        if 'country' in order_merged.columns and order_merged['country'].notna().mean() > 0.5:
            grp_col = 'country'
        elif 'country_carrier' in order_merged.columns and order_merged['country_carrier'].notna().mean() > 0.5:
            grp_col = 'country_carrier'
            order_merged[grp_col] = order_merged[grp_col].replace({'94': 'España'})
        else:
            grp_col = 'plataforma'
        # Clean up stray codes
        if grp_col in order_merged.columns:
            order_merged[grp_col] = order_merged[grp_col].replace({'94': 'España'})

        agg_cols = dict(
            n_pedidos=('ref_odoo','count'),
            venta=('venta','sum'),
            cogs=('cogs','sum'),
            mg_prod=('mg_prod','sum'),
            ing_envio=('ing_envio','sum'),
            cost_envio=('cost_envio','sum'),
            mg_final=('mg_final','sum'),
        )
        cty_pnl = order_merged.groupby([grp_col, 'ym']).agg(**agg_cols).reset_index()
        cty_pnl.columns = ['country' if c == grp_col else c for c in cty_pnl.columns]
        cty_pnl['mg_pct'] = cty_pnl['mg_final'] / (cty_pnl['venta'] + cty_pnl['ing_envio']).replace(0, np.nan)
        results['pnl_by_country'] = cty_pnl.to_dict('records')

        # Monthly evolution per country (for the country evolution tab)
        MAIN_COUNTRIES = ['España','Portugal','Francia','Italia','Alemania','Reino Unido']
        monthly_evo = {}
        all_months = sorted(order_merged['ym'].dropna().unique())
        for country in MAIN_COUNTRIES:
            g = order_merged[order_merged[grp_col] == country]
            if not len(g): continue
            monthly_evo[country] = {}
            for ym in all_months:
                gm = g[g['ym'] == ym]
                if not len(gm): continue
                v = float(gm['venta'].sum()); mf = float(gm['mg_final'].sum())
                monthly_evo[country][ym] = {
                    'n': int(len(gm)), 'venta': round(v, 2),
                    'mg_prod': round(float(gm['mg_prod'].sum()), 2),
                    'ing_envio': round(float(gm['ing_envio'].sum()), 2),
                    'cost_envio': round(float(gm['cost_envio'].sum()), 2),
                    'mg_final': round(mf, 2),
                    'mg_pct': round(mf / (v + float(gm['ing_envio'].sum())) if (v + float(gm['ing_envio'].sum())) else 0, 4),
                }
        results['monthly_by_country'] = monthly_evo
        results['all_months'] = list(all_months)

    # ── ADS INTEGRATION ───────────────────────────────────────────
    if ads_df is not None:
        if ym_filter:
            if isinstance(ym_filter, str) and len(ym_filter) == 4:
                ads_df = ads_df[ads_df['ym'].str.startswith(ym_filter)]
            else:
                months = [ym_filter] if isinstance(ym_filter, str) else ym_filter
                ads_df = ads_df[ads_df['ym'].isin(months)]

        ads_summary = ads_df.groupby(['pais','ym']).agg(
            gasto_ads=('coste','sum'),
            conversiones=('conversiones','sum'),
            valor_conv=('valor_conv','sum'),
            roas=('roas','mean'),
        ).reset_index()
        results['ads'] = ads_summary.to_dict('records')
        results['total_ads'] = float(ads_df['coste'].sum())

        # Integrate ads into P&L: subtract from pnl_by_country and monthly_by_country
        # Build lookup: country × ym → gasto_ads
        ads_lookup = {}
        for _, row in ads_df.iterrows():
            ads_lookup[(str(row['pais']), str(row['ym']))] = float(row.get('coste', 0) or 0)

        if 'pnl_by_country' in results:
            for row in results['pnl_by_country']:
                gasto = ads_lookup.get((str(row.get('country','')), str(row.get('ym',''))), 0)
                row['gasto_ads']   = round(gasto, 2)
                row['mg_post_ads'] = round(float(row.get('mg_final', 0) or 0) - gasto, 2)
                base = float(row.get('venta',0) or 0) + float(row.get('ing_envio',0) or 0)
                row['mg_post_ads_pct'] = round(row['mg_post_ads'] / base, 4) if base else 0

        if 'monthly_by_country' in results:
            for country, months_data in results['monthly_by_country'].items():
                for ym, row in months_data.items():
                    gasto = ads_lookup.get((country, ym), 0)
                    row['gasto_ads']   = round(gasto, 2)
                    row['mg_post_ads'] = round(float(row.get('mg_final', 0) or 0) - gasto, 2)
                    base = float(row.get('venta',0) or 0) + float(row.get('ing_envio',0) or 0)
                    row['mg_post_ads_pct'] = round(row['mg_post_ads'] / base, 4) if base else 0

    # ── RETAIL MEDIA & INPOST COMPENSATION ──────────────────────
    # retail_media: income from brand partnerships → improves product margin
    # inpost_compensacion: Mondial Relay pays us for InPost losses → improves shipping margin
    if retail_media_by_ym or inpost_comp_by_ym:
        # Add to pnl_by_country (distributed proportionally by country sales if multiple countries)
        # Simplification: assign 100% to España (all retail media is domestic)
        if 'pnl_by_country' in results:
            for row in results['pnl_by_country']:
                ym = str(row.get('ym',''))
                if row.get('country') == 'España':
                    rm  = float(retail_media_by_ym.get(ym, 0) or 0)
                    ipc = float(inpost_comp_by_ym.get(ym, 0) or 0)
                    row['retail_media']       = round(rm, 2)
                    row['inpost_comp']        = round(ipc, 2)
                    row['mg_prod']            = round((row.get('mg_prod') or 0) + rm, 2)
                    row['mg_final']           = round((row.get('mg_final') or 0) + rm + ipc, 2)
                    row['mg_post_ads']        = round((row.get('mg_post_ads') or row.get('mg_final') or 0) + rm + ipc, 2)
                    base = float(row.get('venta',0) or 0) + float(row.get('ing_envio',0) or 0)
                    row['mg_post_ads_pct']    = round(row['mg_post_ads'] / base, 4) if base else 0
                else:
                    row['retail_media'] = 0; row['inpost_comp'] = 0

        if 'monthly_by_country' in results:
            for country, months_data in results['monthly_by_country'].items():
                if country != 'España': continue
                for ym, row in months_data.items():
                    rm  = float(retail_media_by_ym.get(ym, 0) or 0)
                    ipc = float(inpost_comp_by_ym.get(ym, 0) or 0)
                    row['retail_media']  = round(rm, 2)
                    row['inpost_comp']   = round(ipc, 2)
                    row['mg_prod']       = round((row.get('mg_prod') or 0) + rm, 2)
                    row['mg_final']      = round((row.get('mg_final') or 0) + rm + ipc, 2)
                    row['mg_post_ads']   = round((row.get('mg_post_ads') or row.get('mg_final') or 0) + rm + ipc, 2)
                    base = float(row.get('venta',0) or 0) + float(row.get('ing_envio',0) or 0)
                    row['mg_post_ads_pct'] = round(row['mg_post_ads'] / base, 4) if base else 0

        results['total_retail_media']  = round(sum(float(v or 0) for v in retail_media_by_ym.values()), 2)
        results['total_inpost_comp']   = round(sum(float(v or 0) for v in inpost_comp_by_ym.values()), 2)

    # ── ALERTS ────────────────────────────────────────────────────
    if shipping_df is not None:
        alerts = shipping_df[
            (shipping_df.get('margin', pd.Series(dtype=float)) < -ALERT_THRESHOLD_EUR) &
            (shipping_df.get('precio_envio', pd.Series(dtype=float)) > 0)
        ].copy() if 'margin' in shipping_df.columns and 'precio_envio' in shipping_df.columns else pd.DataFrame()

        if len(alerts):
            alerts['ratio'] = (alerts['cost_eur'] / alerts['precio_envio'].replace(0, np.nan)).round(2)
            alerts = alerts[alerts['ratio'] >= ALERT_RATIO] if 'ratio' in alerts.columns else alerts
            alerts = alerts.nsmallest(min(100, len(alerts)), 'margin')
            results['alerts'] = alerts[['ref','carrier','country','weight_kg','precio_envio','cost_eur','margin']].to_dict('records')
            results['alert_count'] = len(alerts)
            results['alert_total_loss'] = float(alerts['margin'].sum())

    return results


def compute_shipping_margin(carrier_df, shopify_revenue: dict):
    """
    Add pricing to carrier invoice data.
    carrier_df: has ref, carrier, country, weight_kg, cost_eur
    shopify_revenue: dict {order_ref → shipping_charged}
    Returns carrier_df with precio_envio and margin columns.
    """
    carrier_df = carrier_df.copy()
    carrier_df['precio_envio'] = carrier_df['ref'].map(
        lambda r: shopify_revenue.get(str(r).lstrip('#'), shopify_revenue.get(str(r), None))
    )
    # 0 = free shipping (order exists but no shipping charge)
    # None = not found (exclude from analysis)
    carrier_df['has_price'] = carrier_df['precio_envio'].notna()
    carrier_df['precio_envio'] = carrier_df['precio_envio'].fillna(0)

    # Convert to s/IVA if needed (Shopify charges include IVA for domestic)
    # For Spain: divide by 1.21; International: already 0% IVA
    mask_spain = carrier_df['country'] == 'España'
    carrier_df.loc[mask_spain, 'precio_envio'] = carrier_df.loc[mask_spain, 'precio_envio'] / 1.21

    carrier_df['margin'] = carrier_df['precio_envio'] - carrier_df['cost_eur']
    return carrier_df

def build_comparison(ym_a, ym_b):
    """Side-by-side P&L for two periods."""
    full = build_pnl()
    rows = full.get('pnl_by_country', [])

    def agg(ym):
        # Handle year filter
        if len(ym) == 4:
            month_rows = [r for r in rows if str(r.get('ym','')).startswith(ym)]
        else:
            month_rows = [r for r in rows if r.get('ym') == ym]
        if not month_rows: return None
        d = {
            'n_pedidos':   sum(r.get('n_pedidos',0) or 0 for r in month_rows),
            'venta':       round(sum(r.get('venta',0) or 0 for r in month_rows), 2),
            'cogs':        round(sum(r.get('cogs',0) or 0 for r in month_rows), 2),
            'mg_prod':     round(sum(r.get('mg_prod',0) or 0 for r in month_rows), 2),
            'ing_envio':   round(sum(r.get('ing_envio',0) or 0 for r in month_rows), 2),
            'cost_envio':  round(sum(r.get('cost_envio',0) or 0 for r in month_rows), 2),
            'mg_final':    round(sum(r.get('mg_final',0) or 0 for r in month_rows), 2),
            'gasto_ads':   round(sum(r.get('gasto_ads',0) or 0 for r in month_rows), 2),
            'mg_post_ads': round(sum((r.get('mg_post_ads') if r.get('mg_post_ads') is not None else r.get('mg_final',0)) or 0 for r in month_rows), 2),
        }
        base = d['venta'] + d['ing_envio']
        d['mg_envio']        = round(d['ing_envio'] - d['cost_envio'], 2)
        d['mg_prod_pct']     = round(d['mg_prod'] / d['venta'], 4) if d['venta'] else 0
        d['mg_final_pct']    = round(d['mg_final'] / base, 4) if base else 0
        d['mg_post_ads_pct'] = round(d['mg_post_ads'] / base, 4) if base else 0
        # By country
        by_c = {}
        for r in month_rows:
            c = r.get('country','?')
            if c not in by_c:
                by_c[c] = {k: 0 for k in ['venta','mg_prod','mg_final','gasto_ads','mg_post_ads','mg_pct','mg_post_ads_pct']}
            for k in ['venta','mg_prod','mg_final','gasto_ads']:
                by_c[c][k] += (r.get(k) or 0)
            mpa = r.get('mg_post_ads') if r.get('mg_post_ads') is not None else r.get('mg_final',0)
            by_c[c]['mg_post_ads'] += (mpa or 0)
        for c, cd in by_c.items():
            b2 = cd['venta']
            cd['mg_post_ads_pct'] = round(cd['mg_post_ads'] / b2, 4) if b2 else 0
        d['by_country'] = by_c
        return d

    a = agg(ym_a)
    b = agg(ym_b)

    delta = None
    if a and b:
        delta = {}
        for k in ['n_pedidos','venta','cogs','mg_prod','mg_envio','mg_final',
                  'gasto_ads','mg_post_ads','mg_prod_pct','mg_final_pct','mg_post_ads_pct']:
            va = a.get(k, 0) or 0
            vb = b.get(k, 0) or 0
            delta[k] = round(vb - va, 4 if 'pct' in k else 2)
            delta[f'{k}_pct_change'] = round((vb - va) / abs(va), 4) if va else None

    return {'a': a, 'b': b, 'ym_a': ym_a, 'ym_b': ym_b, 'delta': delta,
            'months': sorted(set(r.get('ym','') for r in rows if r.get('ym')))}
