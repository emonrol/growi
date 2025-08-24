#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Volatility Calculator + Orderbook Percentile Depth Tables + Leverage Information (5-minute snapshots)

This version EXPLICITLY imports your shared utilities from:
    /home/eric/Documents/growi/orderbook_utils.py

And now also fetches leverage information using API.py

What this script does
---------------------
1) Reads CSVs with 5-minute snapshots of many crypto pairs.
   Required columns: ts, symbol, base_price, bids, asks
   - bids / asks are blob strings that may be messy; we normalize them using
     parse_orderbook_blob(...) from your shared module.

2) Computes standard volatility metrics (scaled from 5-min returns).

3) Fetches leverage information for each asset from Hyperliquid API.

4) Builds *percentile* orderbook depth tables, per **level** across **all snapshots**:
   - For each (symbol, side, level):
       pct_diff_pXX : percentile of % difference between level px and base_price
       qty_pXX      : percentile of level size in asset units
       usd_pXX      : percentile of level USD value (price * qty)
     Accumulated columns are cumulative sums of the percentile values by level.

5) Writes outputs:
   - Excel file (volatility_analysis.xlsx) with:
       Table 1: Volatility (% numbers, no % symbol)
       Table 2: Buy target prices (base + vol)
       Table 3: Sell target prices (base - vol)
       Table 4: Leverage information per symbol
       Table 5+: BID & ASK level tables for each requested percentile
   - CSV files for each percentile (orderbook_levels_pXX.csv)

Usage
-----
python volatility.py data.csv                              # Default percentiles [1, 10, 25, 50]
python volatility.py data.csv BTC ETH                      # Subset of symbols, default percentiles
python volatility.py data.csv --percentiles 5 25 75       # Custom percentiles (5%, 25%, 75%)
python volatility.py data.csv BTC --percentiles 10 50     # Symbols + custom percentiles

Arguments
---------
--percentiles: Space-separated list of percentiles (1-99). Default: 1 10 25 50
               Example: --percentiles 5 25 50 75 90

Notes
-----
- Percentile pXX means: in (100-XX)% of snapshots the level has at least these quantities/values
- p01 = 1st percentile: 99% of snapshots have at least this much
- p50 = 50th percentile (median): 50% of snapshots have at least this much
"""

import json
import sys
import argparse
from pathlib import Path
from typing import Optional, Dict, Tuple, List

import numpy as np
import pandas as pd

# Optional Excel formatting
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

# Import API module for leverage information
from API import effective_max_leverage_for_notional, HLMetaError

# ----------------------------------------------------------------------------
# Config
# ----------------------------------------------------------------------------

# Default percentiles to calculate
DEFAULT_PERCENTILES = [1, 10, 25, 50]

# Output filenames
EXCEL_FILENAME = "volatility_analysis.xlsx"

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
# Leverage fetching helper
# ----------------------------------------------------------------------------

def fetch_leverage_info(symbol: str, dex: str = "") -> Dict:
    """
    Fetch leverage information for a symbol.
    
    Returns:
        Dict with leverage info or error info if failed
    """
    try:
        leverage_data = effective_max_leverage_for_notional(symbol, dex=dex)
        if symbol in leverage_data:
            tiers = leverage_data[symbol]
            # Process tiers for easier handling
            max_leverage = max(lev for _, lev in tiers) if tiers else 0
            min_leverage = min(lev for _, lev in tiers) if tiers else 0
            num_tiers = len(tiers)
            
            return {
                'status': 'success',
                'max_leverage': max_leverage,
                'min_leverage': min_leverage,
                'num_tiers': num_tiers,
                'tiers': tiers,
                'tiers_formatted': [(f"${lb:,.0f}", f"{lev}x") for lb, lev in tiers]
            }
        else:
            return {
                'status': 'error',
                'error': f'Symbol {symbol} not found in API response',
                'max_leverage': 0,
                'min_leverage': 0,
                'num_tiers': 0,
                'tiers': [],
                'tiers_formatted': []
            }
    except HLMetaError as e:
        return {
            'status': 'error',
            'error': f'HLMetaError: {str(e)}',
            'max_leverage': 0,
            'min_leverage': 0,
            'num_tiers': 0,
            'tiers': [],
            'tiers_formatted': []
        }
    except Exception as e:
        return {
            'status': 'error',
            'error': f'Unexpected error: {str(e)}',
            'max_leverage': 0,
            'min_leverage': 0,
            'num_tiers': 0,
            'tiers': [],
            'tiers_formatted': []
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
        print(f"Processing {symbol}...")
        sdf = df[df['symbol'] == symbol].copy()
        prices = sdf['base_price'].astype(float)
        if 'ts' in sdf.columns:
            prices.index = sdf['ts']

        log_returns = calculate_log_returns(prices)
        vol_metrics = calculate_volatility_metrics(log_returns)
        
        # Fetch leverage information
        print(f"  Fetching leverage info for {symbol}...")
        leverage_info = fetch_leverage_info(symbol)
        if leverage_info['status'] == 'error':
            print(f"  Warning: Could not fetch leverage for {symbol}: {leverage_info['error']}")

        results[symbol] = {
            'df': sdf,
            'volatility_metrics': vol_metrics,
            'leverage_info': leverage_info,
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
# Percentile-by-level builder (modified to handle multiple percentiles)
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

def build_percentile_depth(results: Dict, percentiles: List[int]) -> Dict[int, pd.DataFrame]:
    """
    For each symbol and side, compute percentile depth tables per level, across all snapshots.
    Returns a dict: {percentile: DataFrame} for each requested percentile.
    """
    percentile_dfs = {}
    
    for perc in percentiles:
        q = perc / 100.0
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
        percentile_dfs[perc] = df
    
    return percentile_dfs

def save_percentile_depth_csvs(results: Dict, percentiles: List[int]) -> List[Path]:
    """
    Save CSV files for each percentile.
    Returns list of output file paths.
    """
    percentile_dfs = build_percentile_depth(results, percentiles)
    output_paths = []
    
    for perc, df in percentile_dfs.items():
        output_csv = f"orderbook_levels_p{perc:02d}.csv"
        out_path = Path(output_csv)
        
        # Rename columns to include percentile
        df_renamed = df.rename(columns={
            'pct_diff': f'pct_diff_p{perc:02d}',
            'qty':      f'qty_p{perc:02d}',
            'usd':      f'usd_p{perc:02d}',
            'acc_qty':  f'acc_qty_p{perc:02d}',
            'acc_usd':  f'acc_usd_p{perc:02d}',
        })
        
        df_renamed.to_csv(out_path, index=False)
        output_paths.append(out_path)
    
    return output_paths

# ----------------------------------------------------------------------------
# Excel builder (now handles multiple percentile tables)
# ----------------------------------------------------------------------------

def _fmt_price(p: float) -> str:
    if p is None or (isinstance(p, float) and np.isnan(p)): return "N/A"
    if p >= 1000: return f"{p:,.0f}"
    if p >= 1:    return f"{p:.2f}"
    return f"{p:.4f}"

def _get_percentile_colors(perc: int) -> Tuple[PatternFill, PatternFill]:
    """Get colors for bid/ask tables based on percentile."""
    # Darker colors for lower percentiles (more conservative)
    if perc <= 5:
        return (PatternFill(start_color="D4F3D4", end_color="D4F3D4", fill_type="solid"),  # Light green
                PatternFill(start_color="FFE4E4", end_color="FFE4E4", fill_type="solid"))  # Light red
    elif perc <= 25:
        return (PatternFill(start_color="E8F5E8", end_color="E8F5E8", fill_type="solid"),  # Medium green  
                PatternFill(start_color="FFF2F2", end_color="FFF2F2", fill_type="solid"))  # Medium red
    else:
        return (PatternFill(start_color="F0F8F0", end_color="F0F8F0", fill_type="solid"),  # Light green
                PatternFill(start_color="FFF8F8", end_color="FFF8F8", fill_type="solid"))  # Light red

def create_excel_tables(results: Dict, percentiles: List[int], output_filename: str = EXCEL_FILENAME) -> None:
    workbook = openpyxl.Workbook()
    ws = workbook.active
    ws.title = "Trading Analysis"

    header_font = Font(bold=True, size=12, color="FFFFFF")
    header_fill = PatternFill(start_color="366092", end_color="366092", fill_type="solid")
    title_font  = Font(bold=True, size=14, color="000000")
    center      = Alignment(horizontal="center", vertical="center")
    leverage_fill = PatternFill(start_color="FFF8DC", end_color="FFF8DC", fill_type="solid")

    symbols = list(results.keys())
    time_labels = ["3min", "5min", "10min", "30min", "60min", "90min", "1day", "1year"]
    vol_keys    = ['3min_vol','5min_vol','10min_vol','30min_vol','60min_vol','90min_vol','1day_vol','1year_vol']

    # Build percentile data for all requested percentiles
    percentile_dfs = build_percentile_depth(results, percentiles)

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

    # TABLE 4: Leverage Information
    ws.cell(row=row, column=1, value="4. Leverage Information").font = title_font
    row += 2

    # Headers for leverage table
    headers = ["Symbol", "Max Leverage", "Min Leverage", "Tiers Count", "Status", "Leverage Tiers"]
    for c, hdr in enumerate(headers, start=1):
        cell = ws.cell(row=row, column=c, value=hdr)
        cell.font = header_font
        cell.fill = header_fill
        cell.alignment = center
    row += 1

    # Data rows for leverage
    for sym in symbols:
        lev_info = results[sym]['leverage_info']
        
        # Symbol
        ws.cell(row=row, column=1, value=sym).alignment = center
        ws.cell(row=row, column=1).font = Font(bold=True)
        ws.cell(row=row, column=1).fill = leverage_fill
        
        # Max Leverage
        max_lev = f"{lev_info['max_leverage']}x" if lev_info['max_leverage'] > 0 else "N/A"
        ws.cell(row=row, column=2, value=max_lev).alignment = center
        ws.cell(row=row, column=2).fill = leverage_fill
        
        # Min Leverage
        min_lev = f"{lev_info['min_leverage']}x" if lev_info['min_leverage'] > 0 else "N/A"
        ws.cell(row=row, column=3, value=min_lev).alignment = center
        ws.cell(row=row, column=3).fill = leverage_fill
        
        # Tiers Count
        ws.cell(row=row, column=4, value=lev_info['num_tiers']).alignment = center
        ws.cell(row=row, column=4).fill = leverage_fill
        
        # Status
        status = "✓" if lev_info['status'] == 'success' else "✗"
        ws.cell(row=row, column=5, value=status).alignment = center
        ws.cell(row=row, column=5).fill = leverage_fill
        
        # Leverage Tiers (formatted as text)
        if lev_info['tiers_formatted']:
            tiers_text = "; ".join([f"{lb} → {lev}" for lb, lev in lev_info['tiers_formatted']])
        else:
            tiers_text = "N/A"
        ws.cell(row=row, column=6, value=tiers_text).alignment = Alignment(horizontal="left", vertical="center")
        ws.cell(row=row, column=6).fill = leverage_fill
        
        row += 1

    row += 2

    # TABLES 5+: Orderbook Levels Percentile Tables (one for each percentile)
    table_num = 5
    for perc in sorted(percentiles):
        pct_df = percentile_dfs[perc]
        pos_fill, neg_fill = _get_percentile_colors(perc)
        
        ws.cell(row=row, column=1, value=f"{table_num}. Orderbook Levels Analysis (p{perc:02d}) - % Difference from Base Price").font = title_font
        row += 2

        # ----- BID TABLE -----
        ws.cell(row=row, column=1, value=f"BID LEVELS p{perc:02d} (% Below Base Price)").font = Font(bold=True, color="008000")
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

        # Compute max bid level for this percentile
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
                    pct_val, qty, usd, accq, accusd = r.iloc[0][['pct_diff','qty','usd','acc_qty','acc_usd']]
                    cells = [pct_val, qty, usd, accq, accusd]
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
        ws.cell(row=row, column=1, value=f"ASK LEVELS p{perc:02d} (% Above Base Price)").font = Font(bold=True, color="CC0000")
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
                    pct_val, qty, usd, accq, accusd = r.iloc[0][['pct_diff','qty','usd','acc_qty','acc_usd']]
                    cells = [pct_val, qty, usd, accq, accusd]
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

        row += 3  # Extra space between percentile tables
        table_num += 1

    # Adjust column widths
    ws.column_dimensions['A'].width = 12
    
    # Table 1-3: Base columns
    base_cols = len(symbols)+1
    for c in range(2, base_cols+1):
        ws.column_dimensions[get_column_letter(c)].width = 15
    
    # Table 4: Leverage table columns
    ws.column_dimensions[get_column_letter(2)].width = 12  # Max Leverage
    ws.column_dimensions[get_column_letter(3)].width = 12  # Min Leverage
    ws.column_dimensions[get_column_letter(4)].width = 10  # Tiers Count
    ws.column_dimensions[get_column_letter(5)].width = 8   # Status
    ws.column_dimensions[get_column_letter(6)].width = 40  # Leverage Tiers
    
    # Table 5+: Orderbook levels
    total_cols = len(symbols)*5 + 1
    for c in range(2, total_cols+1):
        ws.column_dimensions[get_column_letter(c)].width = 12

    workbook.save(output_filename)

# ----------------------------------------------------------------------------
# Argument parsing
# ----------------------------------------------------------------------------

def parse_arguments():
    parser = argparse.ArgumentParser(
        description="Volatility and orderbook analysis with configurable percentiles",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python volatility.py data.csv                          # Default percentiles [1, 10, 25, 50]
  python volatility.py data.csv BTC ETH SOL             # Specific symbols, default percentiles
  python volatility.py data.csv --percentiles 5 25 50   # Custom percentiles
  python volatility.py data.csv BTC --percentiles 1 90  # Symbols + custom percentiles
        """
    )
    
    parser.add_argument('csv_file', help='Path to CSV file with orderbook data')
    parser.add_argument('symbols', nargs='*', help='Optional: specific symbols to analyze')
    parser.add_argument('--percentiles', nargs='+', type=int, default=DEFAULT_PERCENTILES,
                       help=f'Percentiles to calculate (1-99). Default: {DEFAULT_PERCENTILES}')
    
    args = parser.parse_args()
    
    # Validate percentiles
    for p in args.percentiles:
        if not 1 <= p <= 99:
            parser.error(f"Percentile {p} must be between 1 and 99")
    
    # Sort percentiles for consistent output
    args.percentiles = sorted(set(args.percentiles))
    
    return args

# ----------------------------------------------------------------------------
# CLI
# ----------------------------------------------------------------------------

def main():
    args = parse_arguments()
    
    if not Path(args.csv_file).exists():
        raise SystemExit(f"CSV file not found: {args.csv_file}")

    print("Starting analysis...")
    print(f"Requested percentiles: {args.percentiles}")
    if args.symbols:
        print(f"Filtering symbols: {args.symbols}")
    
    # Analyze (includes leverage fetching)
    results = analyze_csv(args.csv_file, args.symbols)

    # Save CSV files for each percentile
    print("Saving CSV files...")
    csv_paths = save_percentile_depth_csvs(results, args.percentiles)
    for path in csv_paths:
        print(f"  Wrote -> {path}")

    # Excel workbook (includes all percentile tables)
    print("Creating Excel workbook...")
    create_excel_tables(results, args.percentiles, output_filename=EXCEL_FILENAME)
    print(f"Wrote Excel workbook -> {EXCEL_FILENAME}")

    # Print summary
    print("\n" + "="*60)
    print("ANALYSIS SUMMARY")
    print("="*60)
    print(f"Percentiles analyzed: {args.percentiles}")
    print(f"CSV files created: {len(csv_paths)}")
    print(f"Excel tables: 4 + {len(args.percentiles)} percentile tables")
    print("-"*60)
    for symbol in results.keys():
        lev_info = results[symbol]['leverage_info']
        status = "✓" if lev_info['status'] == 'success' else "✗"
        max_lev = f"{lev_info['max_leverage']}x" if lev_info['max_leverage'] > 0 else "N/A"
        print(f"{symbol:>6}: Max Leverage {max_lev:>5} | Status {status}")

if __name__ == "__main__":
    main()