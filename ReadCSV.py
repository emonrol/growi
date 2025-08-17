#!/usr/bin/env python3
"""
python3 ReadCSV.py --csv orderbook_snapshots.csv --side asks --max-impact-pct 1 --sep ,

Orderbook Price-Impact Estimator (CSV-Optimized)

- Parses a known CSV format: ts;symbol;base_price;bids;asks
- Cleans and parses messy orderbook blobs with odd quotes, semicolons, and decimal commas.
- Computes cumulative VWAP until a target slippage is reached.
- Works for buys (consume asks) or sells (consume bids).
"""

from __future__ import annotations

import argparse
import json
import re
from dataclasses import dataclass
from typing import Iterable, List, Dict, Optional, Tuple

import pandas as pd

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


def main():
    p = argparse.ArgumentParser(description="Orderbook price-impact estimator")
    p.add_argument('--csv', required=True, help='Path to CSV file (semicolon-separated).')
    p.add_argument('--symbol', default=None, help='Filter by symbol.')
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
    rows = df.iterrows()  # Always process all rows
    for idx, row in rows:
        levels = parse_orderbook_blob(row['asks'] if args.side == 'asks' else row['bids'])
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
    note = f"This Excel is for price impact: {args.max_impact_pct}"

    for day, group in df_out.groupby('_day'):
        group = group.drop(columns=['_day'])
        excel_path = f"Results_{day}.xlsx"
        # Create graphs for this day

        # Adapt y-axis for price_diff depending on max difference
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
        plt.savefig('price_diff_plot.png')

        plt.figure(figsize=(10, 6))
        plt.plot(group['time'], group['max_investable_usd'], marker='o', color='green')
        plt.xticks(rotation=45)
        plt.title('Max Investable USD Over Time')
        plt.xlabel('Time')
        plt.ylabel('Max Investable USD')
        plt.tight_layout()
        plt.savefig('max_investable_plot.png')

        plt.figure(figsize=(10, 6))
        plt.plot(group['time'], group['best_time_metric'], marker='o', color='purple')
        plt.xticks(rotation=45)
        plt.title('Best Time Metric Over Time')
        plt.xlabel('Time')
        plt.ylabel('Best Time Metric')
        plt.tight_layout()
        plt.savefig('best_time_metric_plot.png')

        with pd.ExcelWriter(excel_path, engine='xlsxwriter') as writer:
            pd.DataFrame([{'note': note}]).to_excel(writer, index=False, header=False, startrow=0)
            group.to_excel(writer, index=False, startrow=2)
            workbook = writer.book
            worksheet = writer.sheets['Sheet1']
            worksheet.insert_image('H2', 'price_diff_plot.png')
            worksheet.insert_image('H22', 'max_investable_plot.png')
            worksheet.insert_image('H42', 'best_time_metric_plot.png')
        print(f"Results and graphs saved to {excel_path}")
if __name__ == '__main__':
    main()
