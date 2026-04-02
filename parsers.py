"""
parsers.py — Farma2go Shipping P&L
Parsers for each carrier invoice format, based on real invoice structures.
"""

import pandas as pd
import numpy as np
import zipfile
import io
import os
import re

TRAMOS_BREAKS = [0.5, 1, 2, 3, 4, 5, 10, 20]
TRAMOS_LABELS = ['0-0.5','0.5-1','1-2','2-3','3-4','4-5','5-10','10-20','>20']

COUNTRY_NORM = {
    'ES':'España','España':'España','ESPAÑA':'España','Spain':'España',
    'PT':'Portugal','Portugal':'Portugal','PORTUGAL':'Portugal',
    'FR':'Francia','Francia':'Francia','FRANCE':'Francia','France':'Francia',
    'DE':'Alemania','Alemania':'Alemania','GERMANY':'Alemania','Germany':'Alemania','Deutschland':'Alemania',
    'IT':'Italia','Italia':'Italia','ITALY':'Italia','Italy':'Italia',
    'GB':'Reino Unido','UK':'Reino Unido','Reino Unido':'Reino Unido','United Kingdom':'Reino Unido',
    'BE':'Bélgica','Bélgica':'Bélgica','Belgium':'Bélgica',
    'NL':'Holanda','Holanda':'Holanda','Netherlands':'Holanda','Países Bajos':'Holanda',
    'LU':'Luxemburgo','Luxembourg':'Luxemburgo',
}

SHIPPING_PRODUCTS = {
    'Home Delivery','InPost Punto de Recogida','GLS Internacional',
    'Punto InPost','Service Point Delivery: InPost ES',
    'Service Point Delivery: Correos','Correos Express 24horas',
    'CTT Express','MRW 24h*','Recogida por el cliente..',
    'Sorteo Cupón 100€ x Inpost','Envío','envío',
    'Home delivery','INPOST','InPost',
}

def norm_country(x):
    if not x or str(x).strip() in ('nan','None',''): return None
    return COUNTRY_NORM.get(str(x).strip(), str(x).strip())

def get_tramo(w):
    try:
        w = float(w)
    except: return None
    breaks = TRAMOS_BREAKS
    labels = TRAMOS_LABELS
    for i, b in enumerate(breaks):
        if w <= b:
            return labels[i]
    return labels[-1]


# ─────────────────────────────────────────────────────────────────
# CTT Express  —  Excel with sheets: Articulos / Servicios
# ─────────────────────────────────────────────────────────────────
def parse_ctt(file_bytes, filename=''):
    """
    Parse CTT invoice Excel.
    Sheet 'Servicios': header at row 7 (0-indexed), data from row 8.
    Key cols: Cód. Servicio (= order ref), Kilos, Valor €, CP Destino
    Returns DataFrame with cols: ref, carrier, country, weight_kg, cost_eur, ym
    """
    try:
        xl = pd.ExcelFile(io.BytesIO(file_bytes))
    except Exception as e:
        raise ValueError(f"CTT: no se pudo abrir el archivo — {e}")

    # Try sheet named 'Servicios' or first sheet
    sheet = 'Servicios' if 'Servicios' in xl.sheet_names else xl.sheet_names[0]
    df_raw = pd.read_excel(io.BytesIO(file_bytes), sheet_name=sheet, header=None)

    # Find header row (contains 'Cód. Servicio' or 'Referencia')
    header_row = None
    for i in range(min(15, len(df_raw))):
        row_str = ' '.join(str(v) for v in df_raw.iloc[i].values)
        if 'servicio' in row_str.lower() or 'referencia' in row_str.lower():
            header_row = i
            break
    if header_row is None:
        header_row = 7  # fallback

    df = pd.read_excel(io.BytesIO(file_bytes), sheet_name=sheet,
                       header=header_row, dtype=str)
    df.columns = [str(c).strip() for c in df.columns]

    # Find relevant columns
    def find_col(df, candidates):
        for c in candidates:
            matches = [col for col in df.columns if c.lower() in col.lower()]
            if matches: return matches[0]
        return None

    col_ref    = find_col(df, ['Cód. Servicio','Referencia','Expedicion','Albarán'])
    col_weight = find_col(df, ['Kilos','Peso','Weight','Kg'])
    col_cost   = find_col(df, ['Valor €','Valor','Total','Importe','Cost'])
    col_cp     = find_col(df, ['CP Destino','CP','Postal','Código Postal'])
    col_country = find_col(df, ['País','Pais','Country','Destino'])

    if not col_ref:
        raise ValueError("CTT: no se encontró columna de referencia de pedido")

    records = []
    for _, row in df.iterrows():
        ref = str(row.get(col_ref,'')).strip()
        if not ref or ref in ('nan','None','') or ref.startswith('Cód'):
            continue
        # Skip returns (CP 39300 = Torrelavega warehouse)
        cp = str(row.get(col_cp,'')) if col_cp else ''
        if '39300' in cp:
            continue

        weight = float(str(row[col_weight]).replace(',','.')) if col_weight and str(row.get(col_weight,'')).strip() not in ('nan','','None') else 0.0
        try: weight = float(weight)
        except: weight = 0.0

        cost = str(row[col_cost]).replace(',','.').replace('€','').strip() if col_cost else '0'
        try: cost = abs(float(cost))
        except: cost = 0.0

        country = None
        if col_country:
            country = norm_country(row.get(col_country))
        if not country:
            # Infer from CP
            if cp.startswith(('1','2','3','4','5','6','7','8','9','0')) and len(cp) == 5:
                country = 'España'
            else:
                country = 'España'  # CTT default

        records.append({
            'ref': ref.lstrip('#'),
            'carrier': 'CTT',
            'country': country,
            'weight_kg': weight,
            'cost_eur': cost,
        })

    if not records:
        raise ValueError("CTT: no se encontraron líneas de envío")

    result = pd.DataFrame(records)
    # Aggregate by ref (multiple charge lines per shipment)
    result = result.groupby(['ref','carrier','country']).agg(
        weight_kg=('weight_kg','max'),
        cost_eur=('cost_eur','sum')
    ).reset_index()
    result['tramo'] = result['weight_kg'].apply(get_tramo)
    return result


# ─────────────────────────────────────────────────────────────────
# InPost  —  ZIP containing CSV(s), sep=';', encoding=latin-1
# ─────────────────────────────────────────────────────────────────
def parse_inpost(file_bytes, filename=''):
    """
    InPost ZIP with CSV files. 
    Cols: Référence client (=order_ref), Pays de livraison, Poids en gr, Total htva
    """
    records = []

    if filename.lower().endswith('.zip'):
        try:
            zf = zipfile.ZipFile(io.BytesIO(file_bytes))
            csv_files = [n for n in zf.namelist() if n.lower().endswith('.csv')]
        except Exception as e:
            raise ValueError(f"InPost: no se pudo abrir el ZIP — {e}")

        for csv_name in csv_files:
            with zf.open(csv_name) as f:
                raw = f.read()
            records += _parse_inpost_csv(raw)
    else:
        records += _parse_inpost_csv(file_bytes)

    if not records:
        raise ValueError("InPost: no se encontraron envíos en el archivo")

    result = pd.DataFrame(records)
    result['tramo'] = result['weight_kg'].apply(get_tramo)
    return result


def _parse_inpost_csv(raw_bytes):
    records = []
    for enc in ['latin-1', 'cp1252', 'utf-8']:
        try:
            df = pd.read_csv(io.BytesIO(raw_bytes), sep=';', encoding=enc,
                             dtype=str, low_memory=False)
            break
        except:
            df = None
    if df is None:
        return []

    df.columns = [str(c).strip() for c in df.columns]

    def find_col(df, candidates):
        for c in candidates:
            matches = [col for col in df.columns if c.lower() in col.lower()]
            if matches: return matches[0]
        return None

    col_ref    = find_col(df, ['Référence client','Reference client','Ref client','order','pedido'])
    col_weight = find_col(df, ['Poids en gr','Poids','Weight','Peso'])
    col_cost   = find_col(df, ['Total htva','Total ht','Coste','Cost','Montant'])
    col_country = find_col(df, ['Pays','Country','Pais','País'])

    if not col_ref:
        return []

    for _, row in df.iterrows():
        ref = str(row.get(col_ref,'')).strip()
        if not ref or ref in ('nan','None','') or not re.match(r'[A-Z0-9]{5,}', ref):
            continue
        # Skip if no cost
        cost_raw = str(row.get(col_cost,'0')).replace(',','.').replace('€','').strip() if col_cost else '0'
        try: cost = abs(float(cost_raw))
        except: cost = 0.0
        if cost == 0: continue

        weight_raw = str(row.get(col_weight,'0')).replace(',','.') if col_weight else '0'
        try: weight = float(weight_raw) / 1000.0  # grams → kg
        except: weight = 0.0
        if weight > 100: weight /= 1000.0  # already in grams?

        country = norm_country(row.get(col_country)) if col_country else 'España'

        records.append({
            'ref': ref.lstrip('#'),
            'carrier': 'InPost',
            'country': country or 'España',
            'weight_kg': weight,
            'cost_eur': cost,
        })
    return records


# ─────────────────────────────────────────────────────────────────
# Spring  —  Excel, Customer Ref = Sendcloud parcel_id
# ─────────────────────────────────────────────────────────────────
def parse_spring(file_bytes, filename=''):
    """
    Spring invoice Excel.
    Cols: CONNOTE, Customer Ref (parcel_id), Amount, Actual Kilos, Country
    NOTE: Customer Ref is parcel_id, not order number. 
          Without Sendcloud data, we use Customer Ref as ref.
    """
    try:
        xl = pd.ExcelFile(io.BytesIO(file_bytes))
    except Exception as e:
        raise ValueError(f"Spring: no se pudo abrir — {e}")

    sheet = xl.sheet_names[0]
    # Find header row
    df_raw = pd.read_excel(io.BytesIO(file_bytes), sheet_name=sheet, header=None)
    header_row = 0
    for i in range(min(10, len(df_raw))):
        row_str = ' '.join(str(v) for v in df_raw.iloc[i].values).lower()
        if 'connote' in row_str or 'customer' in row_str or 'amount' in row_str:
            header_row = i
            break

    df = pd.read_excel(io.BytesIO(file_bytes), sheet_name=sheet,
                       header=header_row, dtype=str)
    df.columns = [str(c).strip() for c in df.columns]

    def find_col(df, candidates):
        for c in candidates:
            matches = [col for col in df.columns if c.lower() in col.lower()]
            if matches: return matches[0]
        return None

    col_ref    = find_col(df, ['Customer Ref','Customer Reference','Ref','Order'])
    col_connote = find_col(df, ['CONNOTE','Connote','Tracking'])
    col_amount = find_col(df, ['Amount','Importe','Total','Cost'])
    col_weight = find_col(df, ['Actual Kilos','Kilos','Weight','Peso','Kg'])
    col_country = find_col(df, ['Country','Pais','País','Destination'])

    if not col_ref and not col_connote:
        raise ValueError("Spring: no se encontró columna de referencia")

    use_ref = col_ref or col_connote

    # Aggregate by CONNOTE (multiple charge rows per shipment)
    records = []
    for _, row in df.iterrows():
        ref = str(row.get(use_ref,'')).strip()
        if not ref or ref in ('nan','None',''):
            continue
        cost_raw = str(row.get(col_amount,'0')).replace(',','.').replace('€','').strip() if col_amount else '0'
        try: cost = float(cost_raw)
        except: cost = 0.0

        weight_raw = str(row.get(col_weight,'0')).replace(',','.') if col_weight else '0'
        try: weight = float(weight_raw)
        except: weight = 0.0

        country = norm_country(row.get(col_country)) if col_country else None

        records.append({'ref': ref.lstrip('#'), 'carrier':'Spring',
                        'country': country, 'weight_kg': weight, 'cost_eur': cost})

    if not records:
        raise ValueError("Spring: no se encontraron envíos")

    result = pd.DataFrame(records)
    result = result.groupby(['ref','carrier','country']).agg(
        weight_kg=('weight_kg','max'),
        cost_eur=('cost_eur','sum')
    ).reset_index()
    result['tramo'] = result['weight_kg'].apply(get_tramo)
    return result


# ─────────────────────────────────────────────────────────────────
# GLS  —  Excel, Albarán = order number
# ─────────────────────────────────────────────────────────────────
def parse_gls(file_bytes, filename=''):
    """
    GLS invoice Excel.
    Key: Albarán = Shopify order number (direct match!)
    Cols: Albarán, Kilos, Total/PORTES, País Destino
    """
    try:
        xl = pd.ExcelFile(io.BytesIO(file_bytes))
    except Exception as e:
        raise ValueError(f"GLS: no se pudo abrir — {e}")

    sheet = xl.sheet_names[0]
    df_raw = pd.read_excel(io.BytesIO(file_bytes), sheet_name=sheet, header=None)

    # Find header row
    header_row = 0
    for i in range(min(10, len(df_raw))):
        row_str = ' '.join(str(v) for v in df_raw.iloc[i].values).lower()
        if 'albarán' in row_str or 'albaran' in row_str or 'expedición' in row_str:
            header_row = i
            break

    df = pd.read_excel(io.BytesIO(file_bytes), sheet_name=sheet,
                       header=header_row, dtype=str)
    df.columns = [str(c).strip() for c in df.columns]

    def find_col(df, candidates):
        for c in candidates:
            matches = [col for col in df.columns if c.lower() in col.lower()]
            if matches: return matches[0]
        return None

    col_ref    = find_col(df, ['Albarán','Albaran','Nº Pedido','Order','Referencia'])
    col_weight = find_col(df, ['Kilos','Peso','Weight','Kg'])
    col_cost   = find_col(df, ['PORTES','Total','Importe','Coste','Cost','Precio'])
    col_country = find_col(df, ['País Destino','Pais Destino','Country','País','Pais'])

    if not col_ref:
        raise ValueError("GLS: no se encontró columna de referencia (Albarán)")

    records = []
    for _, row in df.iterrows():
        ref = str(row.get(col_ref,'')).strip().lstrip('#')
        if not ref or ref in ('nan','None','') or not re.search(r'\d', ref):
            continue
        # Skip returns/repos
        if 'repo' in ref.lower() or 'dev' in ref.lower():
            continue

        cost_raw = str(row.get(col_cost,'0')).replace(',','.').replace('€','').strip() if col_cost else '0'
        try: cost = abs(float(cost_raw))
        except: cost = 0.0

        weight_raw = str(row.get(col_weight,'0')).replace(',','.') if col_weight else '0'
        try: weight = abs(float(weight_raw))
        except: weight = 0.0

        country = norm_country(row.get(col_country)) if col_country else 'España'

        records.append({'ref': ref, 'carrier':'GLS',
                        'country': country or 'España', 'weight_kg': weight, 'cost_eur': cost})

    if not records:
        raise ValueError("GLS: no se encontraron envíos")

    result = pd.DataFrame(records)
    result = result.groupby(['ref','carrier','country']).agg(
        weight_kg=('weight_kg','max'),
        cost_eur=('cost_eur','sum')
    ).reset_index()
    result['tramo'] = result['weight_kg'].apply(get_tramo)
    return result


# ─────────────────────────────────────────────────────────────────
# UPS  —  CSV, ref column, separate Farma2go vs Skinvity
# ─────────────────────────────────────────────────────────────────
def parse_ups(file_bytes, filename=''):
    """
    UPS invoice CSV.
    Refs #NNNNNN → Farma2go; SHP/MNL SKINVIT* → Skinvity (separated)
    """
    records_farma = []
    records_skinvity = []

    for enc in ['latin-1', 'cp1252', 'utf-8']:
        try:
            df = pd.read_csv(io.BytesIO(file_bytes), encoding=enc,
                             dtype=str, low_memory=False)
            break
        except:
            df = None
    if df is None:
        raise ValueError("UPS: no se pudo leer el CSV")

    df.columns = [str(c).strip() for c in df.columns]

    def find_col(df, candidates):
        for c in candidates:
            matches = [col for col in df.columns if c.lower() in col.lower()]
            if matches: return matches[0]
        return None

    col_ref      = find_col(df, ['Reference','Ref','Customer Reference','Referencia','reference_number'])
    col_tracking = find_col(df, ['Tracking','Track','Envío','Shipment'])
    col_cost     = find_col(df, ['Net Charge','Net Amount','Coste','Cost','Amount','charge'])
    col_weight   = find_col(df, ['Weight','Peso','Kilos','Billed Weight'])
    col_country  = find_col(df, ['Destination Country','Country','País','Pais','dest_country'])
    col_sender   = find_col(df, ['Sender','Remitente','Shipper'])

    if not col_ref and not col_tracking:
        raise ValueError("UPS: no se encontró columna de referencia")

    use_ref = col_ref or col_tracking

    for _, row in df.iterrows():
        ref_raw = str(row.get(use_ref,'')).strip()
        if not ref_raw or ref_raw in ('nan','None',''):
            continue
        # Skip tax rows
        row_str = ' '.join(str(v) for v in row.values).upper()
        if 'TAX' in row_str and 'TOTALTAX' not in row_str.replace(' ',''):
            continue

        cost_raw = str(row.get(col_cost,'0')).replace(',','.').replace('€','').strip() if col_cost else '0'
        try: cost = abs(float(cost_raw))
        except: cost = 0.0
        if cost == 0: continue

        weight_raw = str(row.get(col_weight,'0')).replace(',','.') if col_weight else '0'
        try: weight = abs(float(weight_raw))
        except: weight = 0.0

        country = norm_country(row.get(col_country)) if col_country else None

        # Identify Skinvity
        sender = str(row.get(col_sender,'')) if col_sender else ''
        is_skinvity = ('SKINVIT' in ref_raw.upper() or 'SKINVIT' in sender.upper() or
                       ref_raw.upper().startswith(('SHP ','MNL ')))

        rec = {'ref': ref_raw.lstrip('#'), 'carrier':'UPS',
               'country': country, 'weight_kg': weight, 'cost_eur': cost}

        if is_skinvity:
            records_skinvity.append(rec)
        else:
            records_farma.append(rec)

    result_farma = pd.DataFrame(records_farma) if records_farma else pd.DataFrame()
    result_skinvity = pd.DataFrame(records_skinvity) if records_skinvity else pd.DataFrame()

    if len(result_farma):
        result_farma = result_farma.groupby(['ref','carrier','country']).agg(
            weight_kg=('weight_kg','max'), cost_eur=('cost_eur','sum')
        ).reset_index()
        result_farma['tramo'] = result_farma['weight_kg'].apply(get_tramo)

    if len(result_skinvity):
        result_skinvity = result_skinvity.groupby(['ref','carrier','country']).agg(
            weight_kg=('weight_kg','max'), cost_eur=('cost_eur','sum')
        ).reset_index()
        result_skinvity['tramo'] = result_skinvity['weight_kg'].apply(get_tramo)

    return result_farma, result_skinvity


# ─────────────────────────────────────────────────────────────────
# Odoo Sales Export  —  Excel with order lines
# ─────────────────────────────────────────────────────────────────
def parse_odoo_sales(file_bytes, filename=''):
    """
    Parse Odoo sales export (sale_order__XX_.xlsx).
    Returns two DataFrames: orders (order-level) and product lines.
    """
    try:
        df = pd.read_excel(io.BytesIO(file_bytes), header=0, dtype=str)
    except Exception as e:
        raise ValueError(f"Odoo: no se pudo abrir — {e}")

    # Normalize column names
    col_map = {}
    for col in df.columns:
        cl = col.lower().strip()
        if 'referencia del pedido' in cl: col_map[col] = 'ref_odoo'
        elif 'referencia cliente' in cl: col_map[col] = 'ref_shopify'
        elif 'fecha' in cl: col_map[col] = 'fecha'
        elif 'origen' in cl: col_map[col] = 'plataforma'
        elif 'ean' in cl or 'ean' in cl: col_map[col] = 'ean'
        elif 'producto/nombre' in cl or 'nombre' in cl and 'producto' in cl: col_map[col] = 'producto'
        elif 'cms' in cl: col_map[col] = 'cms_id'
        elif 'cantidad' in cl: col_map[col] = 'cantidad'
        elif 'coste' in cl: col_map[col] = 'coste_unitario'
        elif 'precio unitario' in cl or 'precio de venta' in cl: col_map[col] = 'precio_unitario'

    df = df.rename(columns=col_map)
    needed = ['ref_odoo','plataforma','producto','cantidad','coste_unitario','precio_unitario']
    missing = [c for c in needed if c not in df.columns]
    if missing:
        raise ValueError(f"Odoo: faltan columnas: {missing}. Columnas encontradas: {df.columns.tolist()}")

    # Forward fill order metadata
    df['ref_odoo'] = df['ref_odoo'].ffill()
    df['plataforma'] = df['plataforma'].ffill()
    if 'fecha' in df.columns: df['fecha'] = df['fecha'].ffill()
    if 'ref_shopify' in df.columns: df['ref_shopify'] = df['ref_shopify'].ffill()

    # Numeric
    for col in ['cantidad','coste_unitario','precio_unitario']:
        df[col] = pd.to_numeric(df[col].str.replace(',','.') if df[col].dtype == object else df[col], errors='coerce').fillna(0)

    if 'fecha' in df.columns:
        df['fecha'] = pd.to_datetime(df['fecha'], errors='coerce')
        df['ym'] = df['fecha'].dt.strftime('%Y-%m')
    else:
        df['ym'] = None

    # Filter quantity > 0
    df = df[df['cantidad'] > 0].copy()

    # Mark shipping lines
    df['is_shipping'] = df['producto'].isin(SHIPPING_PRODUCTS) | (df['producto'] == 'Envío')

    # Calculated fields
    df['venta_total'] = df['cantidad'] * df['precio_unitario']
    df['coste_total'] = df['cantidad'] * df['coste_unitario']
    df['margen_linea'] = df['venta_total'] - df['coste_total']

    if 'ref_shopify' in df.columns:
        df['ref_shopify_clean'] = df['ref_shopify'].astype(str).str.replace('#','').str.strip()
    else:
        df['ref_shopify_clean'] = None

    return df


# ─────────────────────────────────────────────────────────────────
# Shopify Revenue Lookup (from Odoo shipping column)
# ─────────────────────────────────────────────────────────────────
def parse_shopify_revenue(file_bytes, filename=''):
    """
    Parse shopify_shipping_master.csv or similar.
    Returns dict: order_name → shipping_charge
    """
    for enc in ['utf-8', 'latin-1', 'cp1252']:
        try:
            df = pd.read_csv(io.BytesIO(file_bytes), encoding=enc, dtype=str, low_memory=False)
            break
        except:
            df = None
    if df is None:
        raise ValueError("Shopify: no se pudo leer el archivo")

    # Look for order + shipping columns
    def find_col(df, candidates):
        for c in candidates:
            matches = [col for col in df.columns if c.lower() in col.lower()]
            if matches: return matches[0]
        return None

    col_order = find_col(df, ['order','pedido','name','referencia'])
    col_ship  = find_col(df, ['shipping','envio','ship_charge','delivery'])

    if not col_order or not col_ship:
        raise ValueError(f"Shopify: columnas no encontradas. Disponibles: {df.columns.tolist()}")

    result = {}
    for _, row in df.iterrows():
        order = str(row[col_order]).strip().lstrip('#')
        try:
            ship = float(str(row[col_ship]).replace(',','.'))
        except:
            ship = 0.0
        result[order] = ship

    return result


# ─────────────────────────────────────────────────────────────────
# Google Ads  —  Excel (same format as provided)
# ─────────────────────────────────────────────────────────────────
def parse_google_ads(file_bytes, filename=''):
    """
    Parse Google Ads investment Excel (same format as Inversion_Google_Ads_Farma2Go).
    Returns DataFrame: pais, ym, coste, clicks, conversiones, valor_conv, roas, cpa
    """
    MONTHS_MAP = {
        'Ene':'01','Feb':'02','Mar':'03','Abr':'04','May':'05','Jun':'06',
        'Jul':'07','Ago':'08','Sep':'09','Oct':'10','Nov':'11','Dic':'12',
    }
    COUNTRY_SHEETS = {'España':'España','Francia':'Francia','Alemania':'Alemania',
                      'Italia':'Italia','Portugal':'Portugal','UK':'Reino Unido'}
    METRIC_ROWS = {'coste':3,'clicks':4,'impresiones':5,'conversiones':6,
                   'valor_conv':7,'cpc':8,'ctr':9,'tasa_conv':10,'cpa':11,'roas':12}

    try:
        xl = pd.ExcelFile(io.BytesIO(file_bytes))
    except Exception as e:
        raise ValueError(f"Ads: no se pudo abrir — {e}")

    records = []
    for sheet, pais in COUNTRY_SHEETS.items():
        if sheet not in xl.sheet_names:
            continue
        df = pd.read_excel(io.BytesIO(file_bytes), sheet_name=sheet, header=None)
        # Get month headers from row 2 (cols 1-14)
        month_headers = [str(v).strip() for v in df.iloc[2, 1:15].values]

        for col_idx, month_label in enumerate(month_headers):
            # Parse "Ene 2025" → "2025-01"
            parts = month_label.split()
            if len(parts) != 2: continue
            month_abbr, year = parts
            month_num = MONTHS_MAP.get(month_abbr)
            if not month_num: continue
            ym = f"{year}-{month_num}"

            row = {'pais': pais, 'ym': ym}
            for metric, row_num in METRIC_ROWS.items():
                if row_num < len(df):
                    val = pd.to_numeric(df.iloc[row_num, col_idx+1], errors='coerce')
                    row[metric] = float(val) if pd.notna(val) else 0.0
                else:
                    row[metric] = 0.0
            records.append(row)

    if not records:
        raise ValueError("Ads: no se encontraron datos en el archivo")

    return pd.DataFrame(records)
