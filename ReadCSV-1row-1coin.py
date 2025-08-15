#!/usr/bin/env python3
"""
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

_key_quote_pattern = re.compile(r"(?<=\{|,)\s*([A-Za-z_][A-Za-z0-9_]*)\s*:")

def _quote_unquoted_keys(s: str) -> str:
    return re.sub(_key_quote_pattern, r'"\1":', s)

def _normalize_number(x) -> float:
    if x is None:
        return 0.0
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


def estimate_price_impact(levels: Iterable[Dict[str, float]], start_price: float, slippage_pct: float, side: str) -> List[ImpactPoint]:
    side = side.lower()
    if side not in {"asks", "bids"}:
        raise ValueError("side must be 'asks' or 'bids'")
    max_up = start_price * (1.0 + slippage_pct / 100.0)
    max_dn = start_price * (1.0 - slippage_pct / 100.0)
    cum_qty = 0.0
    notional = 0.0
    out: List[ImpactPoint] = []
    for lvl in levels:
        px, sz = float(lvl['px']), float(lvl['sz'])
        if sz <= 0:
            continue
        cum_qty += sz
        notional += px * sz
        avg = notional / cum_qty
        out.append(ImpactPoint(cum_qty, notional, avg))
        if (avg >= max_up and side == 'asks') or (avg <= max_dn and side == 'bids'):
            break
    return out


def solve_partial_at_target(prev_notional: float, prev_qty: float, level_px: float, level_sz: float, target_avg_px: float) -> Optional[Tuple[float, float]]:
    """Return (x_qty, notional_at_target) needed within this level to hit target avg.

    Solve for x in (N + p*x)/(Q + x) = T  =>  x = (T*Q - N)/(p - T)
    """
    denom = (level_px - target_avg_px)
    x = (target_avg_px * prev_qty - prev_notional) / denom
    notional_at_target = prev_notional + level_px * x
    return x, notional_at_target


def exact_threshold_allocation(levels: Iterable[Dict[str, float]], start_price: float, slippage_pct: float, side: str) -> Optional[Tuple[float, float, float]]:
    """Walk the book and return (qty_at_target, notional_at_target, avg_px_target) at the exact slippage threshold.
    If the threshold is never reached, return None.
    """
    side = side.lower()
    if side not in {"asks", "bids"}:
        raise ValueError("side must be 'asks' or 'bids'")
    target = start_price * (1.0 + slippage_pct / 100.0) if side == 'asks' else start_price * (1.0 - slippage_pct / 100.0)

    Q = 0.0
    N = 0.0
    for lvl in levels:
        p, z = float(lvl['px']), float(lvl['sz'])
        if z <= 0:
            continue
        # Check if taking the whole level crosses the target
        avg_full = (N + p * z) / (Q + z)
        crossed = (avg_full >= target) if side == 'asks' else (avg_full <= target)
        if crossed:
            sol = solve_partial_at_target(N, Q, p, z, target)
            x, N_star = sol
            Q_star = Q + x
            return Q_star, N_star, target
        # Otherwise, consume whole level and continue
        Q += z
        N += p * z
    return None


def load_snapshot_rows(csv_path: str, symbol: Optional[str] = None) -> pd.DataFrame:
    df = pd.read_csv(csv_path, sep=';', usecols=['symbol', 'base_price', 'bids', 'asks'])
    df['base_price'] = df['base_price'].apply(_normalize_number)
    if symbol:
        df = df[df['symbol'] == symbol].reset_index(drop=True)
    return df


def analyze_row(row: pd.Series, side: str, slippage_pct: float) -> Tuple[List[ImpactPoint], List[Dict[str, float]]]:
    levels = parse_orderbook_blob(row['asks'] if side == 'asks' else row['bids'])
    levels_sorted = sorted(levels, key=lambda d: d['px'], reverse=(side == 'bids'))
    points = estimate_price_impact(levels_sorted, _normalize_number(row['base_price']), slippage_pct, side)
    return points, levels_sorted


def _build_cli() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Orderbook price-impact estimator")
    p.add_argument('--csv', required=True, help='Path to CSV file (semicolon-separated).')
    p.add_argument('--symbol', default=None, help='Filter by symbol.')
    p.add_argument('--side', choices=['asks', 'bids'], default='asks', help='Buy (asks) or sell (bids).')
    p.add_argument('--slippage-pct', type=float, default=2.0, help='Target slippage percentage.')
    p.add_argument('--row-index', type=int, default=0, help='Row index to analyze.')
    return p


def main():
    args = _build_cli().parse_args()
    df = load_snapshot_rows(args.csv, args.symbol)
    if df.empty:
        raise SystemExit("No matching rows found.")
    row = df.iloc[max(0, min(args.row_index, len(df) - 1))]

    points, levels_sorted = analyze_row(row, args.side, args.slippage_pct)
    if not points:
        print("No levels parsed or zero sizes.")
        return

    base_price_num = _normalize_number(row['base_price'])
    direction = 'up' if args.side == 'asks' else 'down'
    target_px = (base_price_num * (1 + args.slippage_pct/100)) if args.side == 'asks' else (base_price_num * (1 - args.slippage_pct/100))

    # Compute exact notional/qty at which the average equals target slippage
    exact = exact_threshold_allocation(levels_sorted, base_price_num, args.slippage_pct, args.side)

    print("\n=== Price Impact Summary ===")
    print(f"Symbol: {row['symbol']} | Side: {args.side} | Start: {base_price_num:.6f} | Target avg {direction} to: {target_px:.6f}")

    last = points[-1]
    print(f"Levels consumed: {len(points)} | Cumulative qty (last step): {last.cum_qty:.6f} | Average fill px (last step): {last.avg_px:.6f}")

    print("\nImpact by notional (pre-threshold steps):")
    for p in points:
        # Only show pre-threshold steps; recompute threshold check
        is_cross = (p.avg_px >= target_px) if args.side == 'asks' else (p.avg_px <= target_px)
        if is_cross:
            break
        move_pct = ((p.avg_px / base_price_num) - 1.0) * 100.0
        move_abs = p.avg_px - base_price_num
        print(f"{p.notional:,.2f} USD → {move_pct:+.2f}% (avg {p.avg_px:.6f}, Δ ${move_abs:.6f})")
 
    if exact is not None:
        qty_star, notional_star, avg_star = exact
        move_pct_star = ((avg_star / base_price_num) - 1.0) * 100.0
        move_abs_star = avg_star - base_price_num
        print("\nMax notional at EXACT target slippage:")
        print(f"{notional_star:,.2f} USD → {move_pct_star:+.2f}% (avg {avg_star:.6f}, Δ ${move_abs_star:.6f}, qty {qty_star:.6f})")
    else:
        print("\nThreshold not reached within available depth.")
        
if __name__ == '__main__':
    main()
