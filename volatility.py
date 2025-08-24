#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Volatility Calculator + Orderbook Percentile Depth Tables (5-minute snapshots)

This version EXPLICITLY imports your shared utilities from:
    /home/eric/Documents/growi/orderbook_utils.py

What this script does
---------------------
1) Reads CSVs with 5-minute snapshots of many crypto pairs.
   Required columns: ts, symbol, base_price, bids, asks
   - bids / asks are blob strings that may be messy; we normalize them using
     parse_orderbook_blob(...) from your shared module.

2) Computes standard volatility metrics (scaled from 5-min returns).

3) Builds *percentile* orderbook depth tables, per **level** across **all snapshots**:
   - For each (symbol, side, level):
       pct_diff_pXX : percentile of % difference between level px and base_price
       qty_pXX      : percentile of level size in asset units
       usd_pXX      : percentile of level USD value (price * qty)
     Accumulated columns are cumulative sums of the percentile values by level.

4) Writes TWO outputs:
   - Excel file (volatility_analysis.xlsx) with:
       Table 1: Volatility (% numbers, no % symbol)
       Table 2: Buy target prices (base + vol)
       Table 3: Sell target prices (base - vol)
       Table 4: BID & ASK level tables using **percentile values across all snapshots**
   - Big CSV (orderbook_levels_p01.csv) with the percentile-by-level rows for all symbols/sides.

Usage
-----
python volatility.py your_data.csv               # all symbols
python volatility.py your_data.csv BTC ETH SOL   # subset of symbols

Notes
-----
- Default percentile is p01 (=1%) which means: in 99% of snapshots the level has at least
  these quantities/values. You can change PERC_Q if you want a different percentile.
"""

import json
import sys
from pathlib import Path
from typing import Optional, Dict, Tuple, List

import numpy as np
import pandas as pd

# Optional Excel formatting (kept similar to your original script)
import openpyxl
from openpyxl.styles import Font, PatternFill, Alignment
from openpyxl.utils import get_column_letter

# ----------------------------------------------------------------------------
# Import your shared utilities from the exact path you specified
# ----------------------------------------------------------------------------
sys.path.append("/home/eric/Documents/growi")
from orderbook_utils import (
    parse_orderbook_blob,
    validate_required_columns,
    REQUIRED_CSV_COLUMNS,
)

# ----------------------------------------------------------------------------
# Config
# ----------------------------------------------------------------------------

# Percentile to use (0.01 => p01). Change to 0.05 for p05, etc.
PERC_Q = 0.5

# Output filenames
EXCEL_FILENAME = "volatility_analysis.xlsx"
CSV_PCT_FILENAME = "orderbook_levels_p01.csv"

# ----------------------------------------------------------------------------
# Math helpers
# ----------------------------------------------------------------------------

def calculate_log_returns(prices: pd.Series) -> pd.Series:
    prices = prices[prices > 0]
    log_returns = np.log(prices / prices.shift(1))
    return log_returns.dropna()

def calculate_volatility_metrics(log_returns: pd.Series) -> Dict[str, float]:
    if len(log_returns) < 2:
        return {
            '3min_vol': 0.0, '5min_vol': 0.0, '10min_vol': 0.0, '30min_vol': 0.0,
            '60min_vol': 0.0, '90min_vol': 0.0, '1day_vol': 0.0, '1year_vol': 0.0,
            'sample_size': len(log_returns), 'mean_return': 0.0
        }
    vol_5min = log_returns.std()
    return {
        '3min_vol':  vol_5min * np.sqrt(3/5),
        '5min_vol':  vol_5min,
        '10min_vol': vol_5min * np.sqrt(10/5),
        '30min_vol': vol_5min * np.sqrt(30/5),
        '60min_vol': vol_5min * np.sqrt(60/5),
        '90min_vol': vol_5min * np.sqrt(90/5),
        '1day_vol':  vol_5min * np.sqrt(1440/5),
        '1year_vol': vol_5min * np.sqrt(525600/5),
        'sample_size': len(log_returns),
        'mean_return': log_returns.mean()
    }

# ----------------------------------------------------------------------------
# Core analysis
# ----------------------------------------------------------------------------

def analyze_csv(csv_path: str, symbol_filters: Optional[list] = None) -> Dict:
    try:
        df = pd.read_csv(csv_path)
    except Exception as e:
        raise SystemExit(f"Error reading CSV: {e}")

    validate_required_columns(df.columns.tolist(), set(REQUIRED_CSV_COLUMNS))

    if symbol_filters:
        df = df[df['symbol'].isin(symbol_filters)]
        if len(df) == 0:
            raise SystemExit(f"No data found for symbols: {symbol_filters}")
        found_symbols = df['symbol'].unique()
        missing_symbols = set(symbol_filters) - set(found_symbols)
        if missing_symbols:
            print(f"Warning: Symbols not found in data: {missing_symbols}")

    # Mixed timestamp formats supported
    if 'ts' in df.columns:
        df['ts'] = pd.to_datetime(df['ts'], format='mixed')
        df = df.sort_values('ts')

    results = {}

    for symbol in df['symbol'].unique():
        sdf = df[df['symbol'] == symbol].copy()
        prices = sdf['base_price'].astype(float)
        if 'ts' in sdf.columns:
            prices.index = sdf['ts']

        log_returns = calculate_log_returns(prices)
        vol_metrics = calculate_volatility_metrics(log_returns)

        results[symbol] = {
            'df': sdf,
            'volatility_metrics': vol_metrics,
            'price_stats': {
                'min_price': prices.min(),
                'max_price': prices.max(),
                'mean_price': prices.mean(),
                'latest_price': prices.iloc[-1],
                'price_range_pct': ((prices.max() - prices.min()) / prices.mean()) * 100 if prices.mean() else 0.0,
            },
            'data_quality': {
                'total_observations': len(prices),
                'missing_values': prices.isna().sum(),
                'time_span_hours': float((prices.index[-1] - prices.index[0]).total_seconds() / 3600.0) if len(prices) > 1 else 0.0
            }
        }

    return results

# ----------------------------------------------------------------------------
# Percentile-by-level builder
# ----------------------------------------------------------------------------

def _collect_levels_for_symbol(symbol_df: pd.DataFrame, side: str) -> Dict[int, Dict[str, list]]:
    """
    Build a dict: level_index -> {'pct_diff': [...], 'qty': [...], 'usd': [...]}
    across all snapshots for one side ('bids' or 'asks').
    """
    out: Dict[int, Dict[str, list]] = {}
    for _, row in symbol_df.iterrows():
        base_price = float(row['base_price'])
        if side not in row or pd.isna(row[side]):
            continue
        try:
            levels = parse_orderbook_blob(row[side])
        except Exception:
            continue
        for i, lvl in enumerate(levels, start=1):  # 1-indexed levels
            try:
                px = float(lvl['px'])
                sz = float(lvl['sz'])
            except Exception:
                continue
            pct_diff = (px / base_price - 1.0) * 100.0
            usd = px * sz
            bucket = out.setdefault(i, {'pct_diff': [], 'qty': [], 'usd': []})
            bucket['pct_diff'].append(pct_diff)
            bucket['qty'].append(sz)
            bucket['usd'].append(usd)
    return out

def _percentile(series: List[float], q: float) -> float:
    if not series:
        return float('nan')
    return float(pd.Series(series).quantile(q))

def build_percentile_depth(results: Dict, q: float = PERC_Q) -> pd.DataFrame:
    """
    For each symbol and side, compute pXX depth table per level, across all snapshots.
    Returns a tidy DataFrame ready to write to CSV and to feed the Excel builder.
    """
    rows = []
    for symbol, blob in results.items():
        sdf = blob['df']
        for side in ('bids', 'asks'):
            buckets = _collect_levels_for_symbol(sdf, side)
            acc_qty = 0.0
            acc_usd = 0.0
            for lvl in sorted(buckets.keys()):
                b = buckets[lvl]
                pct_p = _percentile(b['pct_diff'], q)
                qty_p = _percentile(b['qty'], q)
                usd_p = _percentile(b['usd'], q)
                if not np.isnan(qty_p):
                    acc_qty += qty_p
                if not np.isnan(usd_p):
                    acc_usd += usd_p
                rows.append({
                    'symbol': symbol,
                    'side': 'bid' if side == 'bids' else 'ask',
                    'level': lvl,
                    'pct_diff': round(pct_p, 3) if not np.isnan(pct_p) else np.nan,
                    'qty': round(qty_p, 6) if not np.isnan(qty_p) else np.nan,
                    'usd': round(usd_p, 2) if not np.isnan(usd_p) else np.nan,
                    'acc_qty': round(acc_qty, 6) if not np.isnan(acc_qty) else np.nan,
                    'acc_usd': round(acc_usd, 2) if not np.isnan(acc_usd) else np.nan,
                    'obs_count': len(b['qty'])
                })
    df = pd.DataFrame(rows)
    if not df.empty:
        df = df.sort_values(['symbol', 'side', 'level']).reset_index(drop=True)
    return df

def save_percentile_depth_csv(results: Dict, output_csv: str = CSV_PCT_FILENAME, q: float = PERC_Q) -> Path:
    df = build_percentile_depth(results, q=q)
    out_path = Path(output_csv)
    df.rename(columns={
        'pct_diff': f'pct_diff_p{int(q*100):02d}',
        'qty':      f'qty_p{int(q*100):02d}',
        'usd':      f'usd_p{int(q*100):02d}',
        'acc_qty':  f'acc_qty_p{int(q*100):02d}',
        'acc_usd':  f'acc_usd_p{int(q*100):02d}',
    }).to_csv(out_path, index=False)
    return out_path

# ----------------------------------------------------------------------------
# Excel builder (Table 4 uses percentile data)
# ----------------------------------------------------------------------------

def _fmt_price(p: float) -> str:
    if p is None or (isinstance(p, float) and np.isnan(p)): return "N/A"
    if p >= 1000: return f"{p:,.0f}"
    if p >= 1:    return f"{p:.2f}"
    return f"{p:.4f}"

def create_excel_tables(results: Dict, pct_df: pd.DataFrame, output_filename: str = EXCEL_FILENAME, q: float = PERC_Q) -> None:
    workbook = openpyxl.Workbook()
    ws = workbook.active
    ws.title = "Trading Analysis"

    header_font = Font(bold=True, size=12, color="FFFFFF")
    header_fill = PatternFill(start_color="366092", end_color="366092", fill_type="solid")
    title_font  = Font(bold=True, size=14, color="000000")
    center      = Alignment(horizontal="center", vertical="center")
    pos_fill    = PatternFill(start_color="E8F5E8", end_color="E8F5E8", fill_type="solid")
    neg_fill    = PatternFill(start_color="FFF2F2", end_color="FFF2F2", fill_type="solid")

    symbols = list(results.keys())
    time_labels = ["3min", "5min", "10min", "30min", "60min", "90min", "1day", "1year"]
    vol_keys    = ['3min_vol','5min_vol','10min_vol','30min_vol','60min_vol','90min_vol','1day_vol','1year_vol']

    row = 1
    ws.cell(row=row, column=1, value="NOTE: All percentage values in tables below are numbers without % symbol for easier Excel editing").font = Font(italic=True, size=10, color="666666")
    row += 2

    # TABLE 1: Volatility Analysis
    ws.cell(row=row, column=1, value="1. Volatility Analysis - Delta Percentages").font = title_font
    row += 2

    ws.cell(row=row, column=1, value="Minutes").font = header_font
    ws.cell(row=row, column=1).fill = header_fill
    ws.cell(row=row, column=1).alignment = center

    for c, sym in enumerate(symbols, start=2):
        ws.cell(row=row, column=c, value=f"{sym} (% values)").font = header_font
        ws.cell(row=row, column=c).fill = header_fill
        ws.cell(row=row, column=c).alignment = center
    row += 1

    for tl, vk in zip(time_labels, vol_keys):
        label = "1 day" if tl=="1day" else "1 year" if tl=="1year" else tl
        ws.cell(row=row, column=1, value=label).alignment = center
        ws.cell(row=row, column=1).font = Font(bold=True)
        for c, sym in enumerate(symbols, start=2):
            v = results[sym]['volatility_metrics'][vk]
            ws.cell(row=row, column=c, value=round(v*100, 3)).alignment = center
        row += 1

    row += 2

    # TABLE 2: Buy target prices
    ws.cell(row=row, column=1, value="2. Buy Target Prices - Base Price + Slippage").font = title_font
    row += 2

    ws.cell(row=row, column=1, value="Minutes").font = header_font
    ws.cell(row=row, column=1).fill = header_fill
    ws.cell(row=row, column=1).alignment = center

    for c, sym in enumerate(symbols, start=2):
        bp = results[sym]['price_stats']['latest_price']
        ws.cell(row=row, column=c, value=f"{sym} ($ {_fmt_price(bp)})").font = header_font
        ws.cell(row=row, column=c).fill = header_fill
        ws.cell(row=row, column=c).alignment = center
    row += 1

    for tl, vk in zip(time_labels, vol_keys):
        label = "1 day" if tl=="1day" else "1 year" if tl=="1year" else tl
        ws.cell(row=row, column=1, value=label).alignment = center
        ws.cell(row=row, column=1).font = Font(bold=True)
        for c, sym in enumerate(symbols, start=2):
            base_price = results[sym]['price_stats']['latest_price']
            vol = results[sym]['volatility_metrics'][vk]
            price = base_price + (vol * base_price)
            ws.cell(row=row, column=c, value=_fmt_price(price)).alignment = center
        row += 1

    row += 2

    # TABLE 3: Sell target prices
    ws.cell(row=row, column=1, value="3. Sell Target Prices - Base Price - Slippage").font = title_font
    row += 2

    ws.cell(row=row, column=1, value="Minutes").font = header_font
    ws.cell(row=row, column=1).fill = header_fill
    ws.cell(row=row, column=1).alignment = center

    for c, sym in enumerate(symbols, start=2):
        mean_price = results[sym]['price_stats']['mean_price']
        ws.cell(row=row, column=c, value=f"{sym} ($ {_fmt_price(mean_price)})").font = header_font
        ws.cell(row=row, column=c).fill = header_fill
        ws.cell(row=row, column=c).alignment = center
    row += 1

    for tl, vk in zip(time_labels, vol_keys):
        label = "1 day" if tl=="1day" else "1 year" if tl=="1year" else tl
        ws.cell(row=row, column=1, value=label).alignment = center
        ws.cell(row=row, column=1).font = Font(bold=True)
        for c, sym in enumerate(symbols, start=2):
            base_price = results[sym]['price_stats']['latest_price']
            vol = results[sym]['volatility_metrics'][vk]
            price = base_price - (vol * base_price)
            ws.cell(row=row, column=c, value=_fmt_price(price)).alignment = center
        row += 1

    row += 2

    # TABLE 4: Orderbook Levels Percentile Tables
    p_label = f"p{int(q*100):02d}"
    ws.cell(row=row, column=1, value=f"4. Orderbook Levels Analysis ({p_label}) - % Difference from Base Price (per level, across snapshots)").font = title_font
    row += 2

    # ----- BID TABLE -----
    ws.cell(row=row, column=1, value="BID LEVELS (% Below Base Price)").font = Font(bold=True, color="008000")
    row += 1

    ws.cell(row=row, column=1, value="Level").font = header_font
    ws.cell(row=row, column=1).fill = header_fill
    ws.cell(row=row, column=1).alignment = center

    col = 2
    for sym in symbols:
        for hdr in [f"{sym} %", f"{sym} Qty", f"{sym} USD", f"{sym} Acc Qty", f"{sym} Acc USD"]:
            ws.cell(row=row, column=col, value=hdr).font = header_font
            ws.cell(row=row, column=col).fill = header_fill
            ws.cell(row=row, column=col).alignment = center
            col += 1
    row += 1

    # Compute max bid level across symbols from percentile DF
    max_bid_level = 0
    for sym in symbols:
        r = pct_df[(pct_df['symbol']==sym) & (pct_df['side']=='bid')]['level']
        if not r.empty:
            max_bid_level = max(max_bid_level, int(r.max()))

    for lvl in range(1, max_bid_level+1):
        ws.cell(row=row, column=1, value=lvl).alignment = center
        ws.cell(row=row, column=1).font = Font(bold=True)
        col = 2
        for sym in symbols:
            r = pct_df[(pct_df['symbol']==sym) & (pct_df['side']=='bid') & (pct_df['level']==lvl)]
            if len(r)==1:
                pct, qty, usd, accq, accusd = r.iloc[0][['pct_diff','qty','usd','acc_qty','acc_usd']]
                cells = [pct, qty, usd, accq, accusd]
                fills = [pos_fill]*5
            else:
                cells = ["N/A"]*5
                fills = [pos_fill]*5
            for val, f in zip(cells, fills):
                if isinstance(val, (int, float)) and not (isinstance(val, float) and np.isnan(val)):
                    disp = round(val, 3) if isinstance(val, float) else val
                else:
                    disp = "N/A"
                c = ws.cell(row=row, column=col, value=disp)
                c.alignment = center
                c.fill = f
                col += 1
        row += 1

    row += 1

    # ----- ASK TABLE -----
    ws.cell(row=row, column=1, value="ASK LEVELS (% Above Base Price)").font = Font(bold=True, color="CC0000")
    row += 1

    ws.cell(row=row, column=1, value="Level").font = header_font
    ws.cell(row=row, column=1).fill = header_fill
    ws.cell(row=row, column=1).alignment = center

    col = 2
    for sym in symbols:
        for hdr in [f"{sym} %", f"{sym} Qty", f"{sym} USD", f"{sym} Acc Qty", f"{sym} Acc USD"]:
            ws.cell(row=row, column=col, value=hdr).font = header_font
            ws.cell(row=row, column=col).fill = header_fill
            ws.cell(row=row, column=col).alignment = center
            col += 1
    row += 1

    max_ask_level = 0
    for sym in symbols:
        r = pct_df[(pct_df['symbol']==sym) & (pct_df['side']=='ask')]['level']
        if not r.empty:
            max_ask_level = max(max_ask_level, int(r.max()))

    for lvl in range(1, max_ask_level+1):
        ws.cell(row=row, column=1, value=lvl).alignment = center
        ws.cell(row=row, column=1).font = Font(bold=True)
        col = 2
        for sym in symbols:
            r = pct_df[(pct_df['symbol']==sym) & (pct_df['side']=='ask') & (pct_df['level']==lvl)]
            if len(r)==1:
                pct, qty, usd, accq, accusd = r.iloc[0][['pct_diff','qty','usd','acc_qty','acc_usd']]
                cells = [pct, qty, usd, accq, accusd]
                fills = [neg_fill]*5
            else:
                cells = ["N/A"]*5
                fills = [neg_fill]*5
            for val, f in zip(cells, fills):
                if isinstance(val, (int, float)) and not (isinstance(val, float) and np.isnan(val)):
                    disp = round(val, 3) if isinstance(val, float) else val
                else:
                    disp = "N/A"
                c = ws.cell(row=row, column=col, value=disp)
                c.alignment = center
                c.fill = f
                col += 1
        row += 1

    # Widths
    ws.column_dimensions['A'].width = 10
    # Table 1-3
    base_cols = len(symbols)+1
    for c in range(2, base_cols+1):
        ws.column_dimensions[get_column_letter(c)].width = 15
    # Table 4
    total_cols = len(symbols)*5 + 1
    for c in range(2, total_cols+1):
        ws.column_dimensions[get_column_letter(c)].width = 12

    workbook.save(output_filename)

# ----------------------------------------------------------------------------
# CLI
# ----------------------------------------------------------------------------

def main():
    if len(sys.argv) < 2:
        print("Usage: python volatility.py <csv_file> [symbol1] [symbol2] ...")
        sys.exit(1)

    csv_path = sys.argv[1]
    symbol_filters = sys.argv[2:] if len(sys.argv) > 2 else None

    if not Path(csv_path).exists():
        raise SystemExit(f"CSV file not found: {csv_path}")

    # Analyze
    results = analyze_csv(csv_path, symbol_filters)

    # Build percentile depth (global across all snapshots)
    pct_df = build_percentile_depth(results, q=PERC_Q)

    # Save big CSV
    out_csv = save_percentile_depth_csv(results, output_csv=CSV_PCT_FILENAME, q=PERC_Q)
    print(f"Wrote percentile depth CSV -> {out_csv}")

    # Excel workbook (uses percentile tables for Table 4)
    create_excel_tables(results, pct_df, output_filename=EXCEL_FILENAME, q=PERC_Q)
    print(f"Wrote Excel workbook -> {EXCEL_FILENAME}")

if __name__ == "__main__":
    main()