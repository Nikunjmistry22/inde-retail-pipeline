import pandas as pd
from utils import _clean_numeric, _aggregate


def ingest_blinkit(path, logger):
    df = pd.read_csv(path)
    df['date'] = pd.to_datetime(df['date']).dt.date
    df['product_identifier'] = df['item_id'].astype(str).str.strip()
    df['total_units'] = _clean_numeric(df['qty_sold'])
    df['total_revenue'] = df['total_units'] * _clean_numeric(df['mrp'])
    df['data_source'] = 'Blinkit'
    return _aggregate(df, logger)


def ingest_zepto(path, logger):
    df = pd.read_csv(path)
    df['date'] = pd.to_datetime(df['Date'], dayfirst=True).dt.date
    df['product_identifier'] = df['SKU Number'].astype(str).str.strip()
    df['total_units'] = _clean_numeric(df['Sales (Qty) - Units'])
    df['total_revenue'] = _clean_numeric(df['Gross Merchandise Value'])
    df['data_source'] = 'Zepto'
    return _aggregate(df, logger)


def ingest_nykaa(path, logger):
    df = pd.read_csv(path)
    df['date'] = pd.to_datetime(df['date']).dt.date
    df['product_identifier'] = df['SKU Code'].astype(str).str.strip()
    df['total_units'] = _clean_numeric(df['Total Qty'])
    df['total_revenue'] = _clean_numeric(df['Selling Price'])
    df['data_source'] = 'Nykaa'
    return _aggregate(df, logger)


def ingest_myntra(path, logger):
    df = pd.read_csv(path)
    df['date'] = pd.to_datetime(df['order_created_date'].astype(str), format='%Y%m%d').dt.date
    df['product_identifier'] = df['style_id'].astype(str).str.strip()
    df['total_units'] = _clean_numeric(df['sales'])
    df['total_revenue'] = _clean_numeric(df['mrp_revenue']) - _clean_numeric(df['vendor_disc'])
    df['data_source'] = 'Myntra'
    return _aggregate(df, logger)