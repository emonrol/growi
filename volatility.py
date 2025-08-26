#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Volatility Calculator + Orderbook Percentile Depth Tables + Leverage Information

Analyzes 5-minute crypto snapshots to compute:
1) Volatility metrics (scaled from 5-min returns)
2) Leverage information from Hyperliquid API
3) Percentile orderbook depth tables using absolute spread distances

Usage:
python volatility.py data.csv                              # Default percentiles [1, 10, 25, 50]
python volatility.py data.csv BTC ETH                      # Subset of symbols
python volatility.py data.csv --percentiles 5 25 75       # Custom percentiles
python volatility.py data.csv --sep ";"                   # Custom CSV separator
"""

import sys
import argparse
from pathlib import Path
from typing import Optional, Dict, Tuple, List
import numpy as np
import pandas as pd
import openpyxl
from openpyxl.styles import Font, PatternFill, Alignment
from openpyxl.utils import get_column_letter

sys.path.append("/home/eric/Documents/growi")
from orderbook_utils import parse_orderbook_blob, validate_required_columns, REQUIRED_CSV_COLUMNS, _normalize_number
from API import get_leverage_info_safe, HLMetaError

DEFAULT_PERCENTILES = [1, 10, 25, 50]
EXCEL_FILENAME = "volatility_analysis.xlsx"

def calculate_log_returns(prices: pd.Series) -> pd.Series:
    prices = prices[prices > 0]
    log_returns = np.log(prices / prices.shift(1))
    return log_returns.dropna()

def calculate_volatility_metrics(log_returns: pd.Series) -> Dict[str, float]:
    if len(log_returns) < 2:
        return {'3min_vol': 0.0, '5min_vol': 0.0, '10min_vol': 0.0, '30min_vol': 0.0,
                '60min_vol': 0.0, '90min_vol': 0.0, '1day_vol': 0.0, '1year_vol': 0.0,
                'sample_size': len(log_returns), 'mean_return': 0.0}
    vol_5min = log_returns.std()
    return {
        '3min_vol': vol_5min * np.sqrt(3/5), '5min_vol': vol_5min, '10min_vol': vol_5min * np.sqrt(10/5),
        '30min_vol': vol_5min * np.sqrt(30/5), '60min_vol': vol_5min * np.sqrt(60/5),
        '90min_vol': vol_5min * np.sqrt(90/5), '1day_vol': vol_5min * np.sqrt(1440/5),
        '1year_vol': vol_5min * np.sqrt(525600/5), 'sample_size': len(log_returns),
        'mean_return': log_returns.mean()
    }

def fetch_leverage_info(symbol: str) -> Dict:
    return get_leverage_info_safe(symbol)

def analyze_csv(csv_path: str, symbol_filters: Optional[list] = None, sep: str = ',') -> Dict:
    try:
        df = pd.read_csv(csv_path, sep=sep, engine='python')
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

    if 'ts' in df.columns:
        df['ts'] = pd.to_datetime(df['ts'], format='mixed')
        df = df.sort_values('ts')

    results = {}
    for symbol in df['symbol'].unique():
        sdf = df[df['symbol'] == symbol].copy()
        prices = sdf['base_price'].apply(_normalize_number)
        prices = pd.Series(prices, dtype=float)  # Convert to float Series
        
        if 'ts' in sdf.columns:
            prices.index = sdf['ts']

        log_returns = calculate_log_returns(prices)
        vol_metrics = calculate_volatility_metrics(log_returns)
        leverage_info = fetch_leverage_info(symbol)
        
        if leverage_info['status'] == 'error':
            print(f"Warning: Could not fetch leverage for {symbol}: {leverage_info['error']}")

        results[symbol] = {
            'df': sdf, 'volatility_metrics': vol_metrics, 'leverage_info': leverage_info,
            'price_stats': {
                'min_price': prices.min(), 'max_price': prices.max(), 'mean_price': prices.mean(),
                'latest_price': prices.iloc[-1],
                'price_range_pct': ((prices.max() - prices.min()) / prices.mean()) * 100 if prices.mean() else 0.0,
            },
            'data_quality': {
                'total_observations': len(prices), 'missing_values': prices.isna().sum(),
                'time_span_hours': float((prices.index[-1] - prices.index[0]).total_seconds() / 3600.0) if len(prices) > 1 else 0.0
            }
        }
    return results

def _collect_levels_for_symbol(symbol_df: pd.DataFrame, side: str) -> Dict[int, Dict[str, list]]:
    out = {}
    for _, row in symbol_df.iterrows():
        base_price = _normalize_number(row['base_price'])  # Handle European decimal format
        if side not in row or pd.isna(row[side]):
            continue
        try:
            levels = parse_orderbook_blob(row[side])
        except Exception:
            continue
        for i, lvl in enumerate(levels, start=1):
            try:
                px, sz = float(lvl['px']), float(lvl['sz'])
            except Exception:
                continue
            
            spread_distance = abs((px / base_price - 1.0) * 100.0)
            usd = px * sz
            bucket = out.setdefault(i, {'spread_distance': [], 'qty': [], 'usd': []})
            bucket['spread_distance'].append(spread_distance)
            bucket['qty'].append(sz)
            bucket['usd'].append(usd)
    return out

def _percentile(series: List[float], q: float) -> float:
    return float(pd.Series(series).quantile(q)) if series else float('nan')

def build_percentile_depth(results: Dict, percentiles: List[int]) -> Dict[int, pd.DataFrame]:
    percentile_dfs = {}
    
    for perc in percentiles:
        q = perc / 100.0
        rows = []
        
        for symbol, blob in results.items():
            sdf = blob['df']
            for side in ('bids', 'asks'):
                buckets = _collect_levels_for_symbol(sdf, side)
                acc_qty = acc_usd = 0.0
                for lvl in sorted(buckets.keys()):
                    b = buckets[lvl]
                    spread_p, qty_p, usd_p = _percentile(b['spread_distance'], q), _percentile(b['qty'], q), _percentile(b['usd'], q)
                    if not np.isnan(qty_p): acc_qty += qty_p
                    if not np.isnan(usd_p): acc_usd += usd_p
                    rows.append({
                        'symbol': symbol, 'side': 'bid' if side == 'bids' else 'ask', 'level': lvl,
                        'spread_distance': round(spread_p, 3) if not np.isnan(spread_p) else np.nan,
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
    percentile_dfs = build_percentile_depth(results, percentiles)
    output_paths = []
    
    for perc, df in percentile_dfs.items():
        output_csv = f"orderbook_levels_p{perc:02d}.csv"
        out_path = Path(output_csv)
        
        df_renamed = df.rename(columns={
            'spread_distance': f'spread_distance_p{perc:02d}', 'qty': f'qty_p{perc:02d}',
            'usd': f'usd_p{perc:02d}', 'acc_qty': f'acc_qty_p{perc:02d}', 'acc_usd': f'acc_usd_p{perc:02d}',
        })
        
        df_renamed.to_csv(out_path, index=False)
        output_paths.append(out_path)
    
    return output_paths

def _fmt_price(p: float) -> str:
    if p is None or (isinstance(p, float) and np.isnan(p)): return "N/A"
    if p >= 1000: return f"{p:,.0f}"
    if p >= 1: return f"{p:.2f}"
    return f"{p:.4f}"

def create_excel_tables(results: Dict, percentiles: List[int], output_filename: str = EXCEL_FILENAME) -> None:
    workbook = openpyxl.Workbook()
    ws = workbook.active
    ws.title = "Trading Analysis"

    header_font = Font(bold=True, size=12, color="FFFFFF")
    header_fill = PatternFill(start_color="366092", end_color="366092", fill_type="solid")
    title_font = Font(bold=True, size=14, color="000000")
    center = Alignment(horizontal="center", vertical="center")
    leverage_fill = PatternFill(start_color="FFF8DC", end_color="FFF8DC", fill_type="solid")

    symbols = list(results.keys())
    time_labels = ["3min", "5min", "10min", "30min", "60min", "90min", "1day", "1year"]
    vol_keys = ['3min_vol','5min_vol','10min_vol','30min_vol','60min_vol','90min_vol','1day_vol','1year_vol']
    percentile_dfs = build_percentile_depth(results, percentiles)

    row = 1
    ws.cell(row=row, column=1, value="NOTE: All percentage values are numbers without % symbol for Excel editing").font = Font(italic=True, size=10, color="666666")
    row += 2

    # Table 1: Volatility Analysis
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

    # Table 2: Buy target prices
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

    # Table 3: Sell target prices
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

    # Table 4: Leverage Information
    ws.cell(row=row, column=1, value="4. Leverage Information").font = title_font
    row += 2

    headers = ["Symbol", "Max Leverage", "Min Leverage", "Tiers Count", "Status", "Leverage Tiers"]
    for c, hdr in enumerate(headers, start=1):
        cell = ws.cell(row=row, column=c, value=hdr)
        cell.font = header_font
        cell.fill = header_fill
        cell.alignment = center
    row += 1

    for sym in symbols:
        lev_info = results[sym]['leverage_info']
        
        ws.cell(row=row, column=1, value=sym).alignment = center
        ws.cell(row=row, column=1).font = Font(bold=True)
        ws.cell(row=row, column=1).fill = leverage_fill
        
        max_lev = f"{lev_info['max_leverage']}x" if lev_info['max_leverage'] > 0 else "N/A"
        ws.cell(row=row, column=2, value=max_lev).alignment = center
        ws.cell(row=row, column=2).fill = leverage_fill
        
        min_lev = f"{lev_info['min_leverage']}x" if lev_info['min_leverage'] > 0 else "N/A"
        ws.cell(row=row, column=3, value=min_lev).alignment = center
        ws.cell(row=row, column=3).fill = leverage_fill
        
        ws.cell(row=row, column=4, value=lev_info['num_tiers']).alignment = center
        ws.cell(row=row, column=4).fill = leverage_fill
        
        status = "✓" if lev_info['status'] == 'success' else "✗"
        ws.cell(row=row, column=5, value=status).alignment = center
        ws.cell(row=row, column=5).fill = leverage_fill
        
        tiers_text = "; ".join([f"{lb} → {lev}" for lb, lev in lev_info['tiers_formatted']]) if lev_info['tiers_formatted'] else "N/A"
        ws.cell(row=row, column=6, value=tiers_text).alignment = Alignment(horizontal="left", vertical="center")
        ws.cell(row=row, column=6).fill = leverage_fill
        
        row += 1

    row += 2

    # Tables 5+: Orderbook Levels Percentile Tables
    table_num = 5
    for perc in sorted(percentiles):
        pct_df = percentile_dfs[perc]
        
        ws.cell(row=row, column=1, value=f"{table_num}. Orderbook Levels Analysis (p{perc:02d}) - Absolute Spread Distance").font = title_font
        row += 2

        # BID TABLE
        ws.cell(row=row, column=1, value=f"BID LEVELS p{perc:02d} (% Distance Below Base Price)").font = Font(bold=True, color="008000")
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
                    spread_val, qty, usd, accq, accusd = r.iloc[0][['spread_distance','qty','usd','acc_qty','acc_usd']]
                    cells = [spread_val, qty, usd, accq, accusd]
                else:
                    cells = ["N/A"]*5
                for val in cells:
                    disp = round(val, 3) if isinstance(val, float) and not np.isnan(val) else "N/A" if not isinstance(val, (int, float)) else val
                    c = ws.cell(row=row, column=col, value=disp)
                    c.alignment = center
                    col += 1
            row += 1

        row += 1

        # ASK TABLE
        ws.cell(row=row, column=1, value=f"ASK LEVELS p{perc:02d} (% Distance Above Base Price)").font = Font(bold=True, color="CC0000")
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
                    spread_val, qty, usd, accq, accusd = r.iloc[0][['spread_distance','qty','usd','acc_qty','acc_usd']]
                    cells = [spread_val, qty, usd, accq, accusd]
                else:
                    cells = ["N/A"]*5
                for val in cells:
                    disp = round(val, 3) if isinstance(val, float) and not np.isnan(val) else "N/A" if not isinstance(val, (int, float)) else val
                    c = ws.cell(row=row, column=col, value=disp)
                    c.alignment = center
                    col += 1
            row += 1

        row += 3
        table_num += 1

    # Column widths
    ws.column_dimensions['A'].width = 12
    for c in range(2, len(symbols)+2):
        ws.column_dimensions[get_column_letter(c)].width = 15
    
    ws.column_dimensions[get_column_letter(2)].width = 12
    ws.column_dimensions[get_column_letter(3)].width = 12
    ws.column_dimensions[get_column_letter(4)].width = 10
    ws.column_dimensions[get_column_letter(5)].width = 8
    ws.column_dimensions[get_column_letter(6)].width = 40
    
    for c in range(2, len(symbols)*5 + 2):
        ws.column_dimensions[get_column_letter(c)].width = 12

    workbook.save(output_filename)

def parse_arguments():
    parser = argparse.ArgumentParser(description="Volatility and orderbook analysis with configurable percentiles")
    parser.add_argument('csv_file', help='Path to CSV file with orderbook data')
    parser.add_argument('symbols', nargs='*', help='Optional: specific symbols to analyze')
    parser.add_argument('--percentiles', nargs='+', type=int, default=DEFAULT_PERCENTILES,
                       help=f'Percentiles to calculate (1-99). Default: {DEFAULT_PERCENTILES}')
    parser.add_argument('--sep', default=',', 
                       help='CSV separator/delimiter (default: ",")')
    
    args = parser.parse_args()
    
    for p in args.percentiles:
        if not 1 <= p <= 99:
            parser.error(f"Percentile {p} must be between 1 and 99")
    
    args.percentiles = sorted(set(args.percentiles))
    return args

def main():
    args = parse_arguments()
    
    if not Path(args.csv_file).exists():
        raise SystemExit(f"CSV file not found: {args.csv_file}")

    results = analyze_csv(args.csv_file, args.symbols, sep=args.sep)
    csv_paths = save_percentile_depth_csvs(results, args.percentiles)
    create_excel_tables(results, args.percentiles, output_filename=EXCEL_FILENAME)
    
    for path in csv_paths:
        print(f"{path}")
    print(f"{EXCEL_FILENAME}")

if __name__ == "__main__":
    main()