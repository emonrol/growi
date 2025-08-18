#!/usr/bin/env python3
"""
python3 ReadCSV.py --csv orderbook_snapshots.csv --side asks --max-impact-pct 1 --sep ,

Orderbook Price-Impact Estimator (CSV-Optimized)

- Parses a known CSV format: ts;symbol;base_price;bids;asks
- Cleans and parses messy orderbook blobs with odd quotes, semicolons, and decimal commas.
- Computes cumulative VWAP until a target slippage is reached.
- Works for buys (consume asks) or sells (consume bids).
- NEW: Analyzes price impact for varying USD investment amounts
"""

from __future__ import annotations

import argparse
import json
import re
from dataclasses import dataclass
from typing import Iterable, List, Dict, Optional, Tuple

import pandas as pd
import numpy as np

_key_quote_pattern = re.compile(r"(?<=\{|,)\s*([A-Za-z_][A-Zael0-9_]*)\s*:")

def _quote_unquoted_keys(s: str) -> str:
    return re.sub(_key_quote_pattern, r'"\1":', s)

def _normalize_number(x) -> float:
    if isinstance(x, (int, float)):
        return float(x)
    s = str(x).strip().replace(",", ".").replace('"', '').replace("'", '')
    try:
        return float(s)
    except ValueError:
        m = re.search(r"[-+]?\d*\.?\d+", s)
        return float(m.group(0)) if m else 0.0

def parse_orderbook_blob(blob: str) -> List[Dict[str, float]]:
    if not blob:
        return []
    s = str(blob).strip()
    if s.startswith('"') and s.endswith('"'):
        s = s[1:-1]
    s = s.replace('""', '"').replace(';', ',').replace('}{', '},{')
    s = _quote_unquoted_keys(s)
    if not s.startswith('['):
        s = f'[{s}]'
    try:
        raw = json.loads(s)
    except json.JSONDecodeError:
        raw = json.loads(re.sub(r'}\s*{', '},{', s))
    return [{
        'px': _normalize_number(level.get('px')), 
        'sz': _normalize_number(level.get('sz'))
    } for level in raw]

@dataclass
class ImpactPoint:
    cum_qty: float
    notional: float
    avg_px: float

def calculate_impact_for_usd_amount(levels: Iterable[Dict[str, float]], start_price: float, usd_amount: float, side: str) -> Tuple[float, float, float]:
    """
    Calculate the price impact for a given USD investment amount.
    Returns: (actual_usd_spent, quantity_bought_sold, avg_price_paid_received)
    """
    side = side.lower()
    if side not in {"asks", "bids"}:
        raise ValueError("side must be 'asks' or 'bids'")
    
    remaining_usd = usd_amount
    cum_qty = 0.0
    cum_notional = 0.0
    
    for lvl in levels:
        px, sz = float(lvl['px']), float(lvl['sz'])
        if sz <= 0 or remaining_usd <= 0:
            continue
            
        # Calculate how much of this level we can afford
        level_notional = px * sz
        
        if level_notional <= remaining_usd:
            # We can consume this entire level
            remaining_usd -= level_notional
            cum_qty += sz
            cum_notional += level_notional
        else:
            # We can only partially consume this level
            partial_qty = remaining_usd / px
            cum_qty += partial_qty
            cum_notional += remaining_usd
            remaining_usd = 0
            break
    
    if cum_qty == 0:
        return 0.0, 0.0, start_price
        
    avg_px = cum_notional / cum_qty
    return cum_notional, cum_qty, avg_px

def estimate_price_impact(levels: Iterable[Dict[str, float]], start_price: float, max_impact_pct: float, side: str) -> List[ImpactPoint]:
    side = side.lower()
    if side not in {"asks", "bids"}:
        raise ValueError("side must be 'asks' or 'bids'")
    target_px = start_price * (1.0 + max_impact_pct / 100.0) if side == 'asks' else start_price * (1.0 - max_impact_pct / 100.0)
    cum_qty = 0.0
    notional = 0.0
    for lvl in levels:
        px, sz = float(lvl['px']), float(lvl['sz'])
        if sz <= 0:
            continue
        cum_qty += sz
        notional += px * sz
        avg = notional / cum_qty
        crossed = (avg >= target_px) if side == 'asks' else (avg <= target_px)
        if crossed:
            # Partial fill at this level to hit target
            denom = (px - target_px)
            if denom == 0:
                break
            x = (target_px * (cum_qty - sz) - (notional - px * sz)) / denom
            notional_at_target = (notional - px * sz) + px * x
            return notional_at_target, x + (cum_qty - sz), target_px
    return notional, cum_qty, avg

def analyze_usd_impact_curve(levels: List[Dict[str, float]], start_price: float, side: str, max_total_liquidity: float) -> Tuple[List[float], List[float]]:
    """
    Analyze price impact for a range of USD investment amounts.
    Returns: (usd_amounts, price_impacts_pct)
    """
    # Create a range of USD amounts to test
    max_test_amount = min(max_total_liquidity, 500000)  # Cap at 100k or total liquidity
    usd_amounts = []
    price_impacts = []
    
    # Generate logarithmic spacing for better coverage
    test_amounts = np.logspace(1, np.log10(max_test_amount), 50)  # 50 points from $10 to max
    
    for usd_amount in test_amounts:
        actual_usd, qty, avg_px = calculate_impact_for_usd_amount(levels, start_price, usd_amount, side)
        
        if actual_usd > 0 and avg_px != start_price:
            # Calculate price impact percentage
            if side == 'asks':  # buying
                impact_pct = ((avg_px - start_price) / start_price) * 100
            else:  # selling
                impact_pct = ((start_price - avg_px) / start_price) * 100
            
            usd_amounts.append(actual_usd)
            price_impacts.append(impact_pct)
        
        # Stop if we've consumed all available liquidity
        if actual_usd < usd_amount * 0.99:  # 99% threshold to account for rounding
            break
    
    return usd_amounts, price_impacts

def main():
    p = argparse.ArgumentParser(description="Orderbook price-impact estimator")
    p.add_argument('--csv', required=True, help='Path to CSV file (semicolon-separated).')
    p.add_argument('--symbol', default="BTC", help='Filter by symbol.')
    p.add_argument('--side', choices=['asks', 'bids'], default='asks', help='Buy (asks) or sell (bids).')
    p.add_argument('--max-impact-pct', type=float, required=True, help='Max tolerated price impact (percentage).')
    p.add_argument('--usd-amount', type=float, default=None, help='Optional: USD amount to invest.')
    p.add_argument('--all-rows', action='store_true', help='Process all rows for the symbol.')
    p.add_argument('--sep', default=',', help='CSV separator, e.g. , or :')
    args = p.parse_args()
    
    df = pd.read_csv(args.csv, sep=args.sep)
    df = pd.read_csv(args.csv, sep=args.sep, usecols=['ts', 'symbol', 'base_price', 'bids', 'asks'])
    if args.symbol:
        df = df[df['symbol'] == args.symbol].reset_index(drop=True)
    if df.empty:
        raise SystemExit("No matching rows found.")

    results = []
    price_diff_list = []
    investable_list = []
    
    # For the USD impact analysis, we'll use the first row as representative
    # (or you could modify this to average across multiple rows)
    representative_row = df.iloc[0]
    rep_levels = parse_orderbook_blob(representative_row['asks'] if args.side == 'asks' else representative_row['bids'])
    rep_levels = sorted(rep_levels, key=lambda d: d['px'], reverse=(args.side == 'bids'))
    rep_base_price = _normalize_number(representative_row['base_price'])
    
    # Calculate total available liquidity for scaling the USD impact analysis
    total_liquidity = sum(level['px'] * level['sz'] for level in rep_levels)
    
    rows = df.iterrows()  # Always process all rows
    for idx, row in rows:
        levels = parse_orderbook_blob(row['asks'] if args.side == 'asks' else row['bids'])
        if not levels:
            continue  # Skip rows with no levels
        levels = sorted(levels, key=lambda d: d['px'], reverse=(args.side == 'bids'))
        # Do not round base price, keep full precision
        base_price_num = _normalize_number(row['base_price'])
        notional, qty, avg_px = estimate_price_impact(levels, base_price_num, args.max_impact_pct, args.side)
        try:
            dt = pd.to_datetime(str(row['ts']))
            time_str = dt.strftime('%H:%M')
            day_str = str(dt.date())
        except Exception:
            time_str = ''
            day_str = ''
        price_diff = avg_px - base_price_num  # no rounding
        if args.usd_amount is not None:
            max_investable = round(min(notional, args.usd_amount), 3)
        else:
            max_investable = round(notional, 3)
        side_str = 'Buy' if args.side == 'asks' else 'Sell'
        price_diff_list.append(price_diff)
        investable_list.append(max_investable)
        result = {
            'time': time_str,
            'symbol': row['symbol'],
            'side': side_str,
            'base_price': base_price_num,  # full precision
            'price_diff': price_diff,
            'max_investable_usd': max_investable,
            '_day': day_str
        }
        results.append(result)

    # Calculate metric (out of 100) for each row
    if results:
        price_diff_arr = pd.Series(price_diff_list)
        investable_arr = pd.Series(investable_list)
        if args.side == 'asks':  # Buy
            price_score = 100 * (1 - (price_diff_arr - price_diff_arr.min()) / (price_diff_arr.max() - price_diff_arr.min() + 1e-9))
        else:  # Sell
            price_score = 100 * (price_diff_arr - price_diff_arr.min()) / (price_diff_arr.max() - price_diff_arr.min() + 1e-9)
        invest_score = 100 * (investable_arr - investable_arr.min()) / (investable_arr.max() - investable_arr.min() + 1e-9)
        metric = (price_score + invest_score) / 2
        for i, m in enumerate(metric):
            results[i]['best_time_metric'] = round(m, 1)

    # Always save one Excel per day as Results_<day>.xlsx
    import matplotlib.pyplot as plt
    import os

    df_out = pd.DataFrame(results)
    note = f"This Excel is for price impact: {args.max_impact_pct}%"

    for day, group in df_out.groupby('_day'):
        group = group.drop(columns=['_day'])
        excel_path = f"Results_{day}.xlsx"
        
        # Generate USD impact analysis
        usd_amounts, price_impacts = analyze_usd_impact_curve(rep_levels, rep_base_price, args.side, total_liquidity)
        
        # Create graphs for this day
        plt.style.use('default')
        
        # Original graphs
        plt.figure(figsize=(10, 6))
        plt.plot(group['time'], group['price_diff'], marker='o')
        plt.xticks(rotation=45)
        plt.title('Price Diff Over Time')
        plt.xlabel('Time')
        plt.ylabel('Price Diff')
        plt.tight_layout()
        diff_min = group['price_diff'].min()
        diff_max = group['price_diff'].max()
        if abs(diff_max - diff_min) < 0.01:
            plt.ylim(diff_min - 0.01, diff_max + 0.01)
        else:
            plt.ylim(diff_min - 0.1 * abs(diff_max - diff_min), diff_max + 0.1 * abs(diff_max - diff_min))
        plt.savefig('price_diff_plot.png', dpi=150, bbox_inches='tight')
        plt.close()

        plt.figure(figsize=(10, 6))
        plt.plot(group['time'], group['max_investable_usd'], marker='o', color='green')
        plt.xticks(rotation=45)
        plt.title('Max Investable USD Over Time')
        plt.xlabel('Time')
        plt.ylabel('Max Investable USD')
        plt.tight_layout()
        plt.savefig('max_investable_plot.png', dpi=150, bbox_inches='tight')
        plt.close()

        plt.figure(figsize=(10, 6))
        plt.plot(group['time'], group['best_time_metric'], marker='o', color='purple')
        plt.xticks(rotation=45)
        plt.title('Best Time Metric Over Time')
        plt.xlabel('Time')
        plt.ylabel('Best Time Metric')
        plt.tight_layout()
        plt.savefig('best_time_metric_plot.png', dpi=150, bbox_inches='tight')
        plt.close()

        # NEW: USD Investment Amount vs Price Impact graph
        plt.figure(figsize=(12, 8))
        plt.semilogx(usd_amounts, price_impacts, marker='o', linewidth=2, markersize=4, color='red')
        plt.grid(True, alpha=0.3)
        plt.title(f'Price Impact vs USD Investment Amount\n({args.symbol} - {args.side.title()})', fontsize=14)
        plt.xlabel('USD Investment Amount (log scale)', fontsize=12)
        plt.ylabel('Price Impact (%)', fontsize=12)
        
        # Add some reference lines
        if price_impacts:
            max_impact = max(price_impacts)
            plt.axhline(y=0.1, color='green', linestyle='--', alpha=0.7, label='0.1% Impact')
            plt.axhline(y=0.5, color='orange', linestyle='--', alpha=0.7, label='0.5% Impact')
            plt.axhline(y=1.0, color='red', linestyle='--', alpha=0.7, label='1.0% Impact')
            plt.legend()
        
        # Format the plot
        plt.tight_layout()
        plt.savefig('usd_impact_curve.png', dpi=150, bbox_inches='tight')
        plt.close()

        # Create a summary table for the USD impact analysis
        impact_summary = pd.DataFrame({
            'USD_Amount': usd_amounts,
            'Price_Impact_Pct': price_impacts
        })
        
        # Add some key thresholds
        thresholds = [100, 500, 1000, 5000, 10000]
        threshold_impacts = []
        for threshold in thresholds:
            if usd_amounts:
                # Find closest USD amount to threshold
                closest_idx = min(range(len(usd_amounts)), key=lambda i: abs(usd_amounts[i] - threshold))
                if abs(usd_amounts[closest_idx] - threshold) / threshold < 0.1:  # Within 10%
                    threshold_impacts.append({
                        'USD_Amount': threshold,
                        'Price_Impact_Pct': price_impacts[closest_idx]
                    })
        
        threshold_df = pd.DataFrame(threshold_impacts)

        with pd.ExcelWriter(excel_path, engine='xlsxwriter') as writer:
            # Main results
            pd.DataFrame([{'note': note}]).to_excel(writer, sheet_name='Main Results', index=False, header=False, startrow=0)
            group.to_excel(writer, sheet_name='Main Results', index=False, startrow=2)
            
            # USD Impact Analysis
            if not impact_summary.empty:
                impact_summary.to_excel(writer, sheet_name='USD Impact Analysis', index=False)
            
            # Threshold summary
            if not threshold_df.empty:
                threshold_df.to_excel(writer, sheet_name='Impact Thresholds', index=False)
            
            workbook = writer.book
            main_worksheet = writer.sheets['Main Results']
            
            # Insert images into main results sheet
            main_worksheet.insert_image('H2', 'price_diff_plot.png')
            main_worksheet.insert_image('H22', 'max_investable_plot.png')
            main_worksheet.insert_image('H42', 'best_time_metric_plot.png')
            main_worksheet.insert_image('H62', 'usd_impact_curve.png')
            
        print(f"Results and graphs saved to {excel_path}")
        print(f"USD Impact Analysis: {len(usd_amounts)} data points generated")
        if usd_amounts:
            print(f"Max analyzable amount: ${max(usd_amounts):,.2f}")
            print(f"Price impact range: {min(price_impacts):.3f}% - {max(price_impacts):.3f}%")

if __name__ == '__main__':
    main()