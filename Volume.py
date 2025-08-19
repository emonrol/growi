#!/usr/bin/env python3
"""
  python3 Volume.py --csv orderbook_snapshots_last_10000_ejemplo.csv --band-pct 0.05 --sep ";"
   * Up volume (asks): base_price <= px <= base_price*(1 + band_pct/100)

"""

from __future__ import annotations

import argparse
import csv
import json
import re
from typing import List, Dict, Tuple, Optional
import os

import pandas as pd
import matplotlib.pyplot as plt

# ------------------------ Parsing helpers ------------------------

_key_quote_pattern = re.compile(r'(?<=\{|,)\s*([A-Za-z_][A-Za-z0-9_]*)\s*:')

def _quote_unquoted_keys(s: str) -> str:
    return re.sub(_key_quote_pattern, r'"\1":', s)

def _normalize_number(x) -> float:
    """Turn things like '4,77' or ' " 4,77 " ' into float 4.77; fallback to 0.0."""
    if isinstance(x, (int, float)):
        return float(x)
    s = str(x).strip().replace(",", ".").replace('"', '').replace("'", '')
    try:
        return float(s)
    except ValueError:
        m = re.search(r"[-+]?\d*\.?\d+", s)
        return float(m.group(0)) if m else 0.0

def parse_orderbook_blob(blob: str) -> List[Dict[str, float]]:
    """
    Parse a messy bids/asks blob into a list of {'px': float, 'sz': float}.
    Handles doubled quotes, semicolons as separators, glued objects, etc.
    """
    if not blob or (isinstance(blob, float) and pd.isna(blob)):
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
        s2 = re.sub(r'}\s*{', '},{', s)
        raw = json.loads(s2)
    out = []
    for level in raw:
        px = _normalize_number(level.get('px'))
        sz = _normalize_number(level.get('sz'))
        out.append({'px': px, 'sz': sz})
    return out

# ------------------------ Timestamp handling ------------------------

_ts_frac_and_tz = re.compile(r'(\d{2}:\d{2}:\d{2}),(\d+)')      # HH:MM:SS,ffffff -> HH:MM:SS.ffffff
_tz_hhmm = re.compile(r'([+-]\d{2})(\d{2})$')                   # +0000 -> +00:00
_tz_hh = re.compile(r'([+-]\d{2})$')                            # +00 -> +00:00

def _clean_ts_for_parsing(s: str) -> str:
    """
    Normalize '2025-08-09 15:26:17,243742+00' ->
              '2025-08-09 15:26:17.243742+00:00'
    """
    if s is None:
        return ""
    s = str(s).strip()
    s = _ts_frac_and_tz.sub(r'\1.\2', s)
    s = _tz_hhmm.sub(r'\1:\2', s)
    s = _tz_hh.sub(r'\1:00', s)
    return s

def parse_ts_naive_utc(s: str) -> Optional[pd.Timestamp]:
    """
    Parse to tz-aware UTC, then strip tz to make Excel-safe (tz-naive).
    """
    cleaned = _clean_ts_for_parsing(s)
    ts = pd.to_datetime(cleaned, utc=True, errors='coerce')
    if pd.isna(ts):
        return pd.NaT
    try:
        if getattr(ts, 'tz', None) is not None:
            ts = ts.tz_convert('UTC')
        return ts.tz_localize(None)
    except Exception:
        return ts

# ------------------------ Band volume logic ------------------------

def sum_sizes_within_band(levels: List[Dict[str, float]], lower_px: float, upper_px: float) -> float:
    """Sum 'sz' for levels where lower_px <= px <= upper_px."""
    if lower_px > upper_px:
        lower_px, upper_px = upper_px, lower_px
    total = 0.0
    for lvl in levels:
        px = float(lvl.get('px', 0.0))
        sz = float(lvl.get('sz', 0.0))
        if sz <= 0:
            continue
        if lower_px <= px <= upper_px:
            total += sz
    return total

def compute_up_down_volumes(base_price: float,
                            asks: List[Dict[str, float]],
                            bids: List[Dict[str, float]],
                            band_pct: float) -> tuple[float, float]:
    """
    Up volume (asks): base_price <= px <= base_price*(1 + band_pct/100)
    Down volume (bids): base_price*(1 - band_pct/100) <= px <= base_price
    """
    bp = float(base_price)
    up_upper = bp * (1.0 + band_pct / 100.0)
    down_lower = bp * (1.0 - band_pct / 100.0)
    up_volume = sum_sizes_within_band(asks, bp, up_upper)
    down_volume = sum_sizes_within_band(bids, down_lower, bp)
    return up_volume, down_volume

# ------------------------ CSV reading ------------------------

REQUIRED_COLS = {'ts', 'symbol', 'base_price', 'bids', 'asks'}

def read_input_csv(path: str, sep_arg: Optional[str]) -> pd.DataFrame:
    sep = ';' if sep_arg is None else sep_arg
    df = pd.read_csv(path,
                     sep=sep,
                     engine='python',
                     dtype=str,
                     quoting=csv.QUOTE_MINIMAL)
    missing = REQUIRED_COLS.difference(df.columns)
    if missing:
        raise SystemExit(f"CSV is missing required columns: {sorted(missing)}")
    return df[['ts', 'symbol', 'base_price', 'bids', 'asks']].copy()

# ------------------------ Main ------------------------

def main():
    ap = argparse.ArgumentParser(description="Compute volumes in a band, export one Excel with all data and plots in main sheets.")
    ap.add_argument('--csv', required=True, help='Path to CSV file.')
    ap.add_argument('--band-pct', type=float, required=True, help='Percentage band, e.g. 0.05 for ±0.05%%.')
    ap.add_argument('--sep', default=';', help='CSV separator (default ;).')
    ap.add_argument('--symbol', default=None, help='Optional: filter to one symbol.')
    args = ap.parse_args()

    # Read CSV (as strings)
    df = read_input_csv(args.csv, args.sep)

    # Optional filter
    if args.symbol:
        df = df[df['symbol'] == args.symbol].reset_index(drop=True)
    if df.empty:
        raise SystemExit("No matching rows found after filtering.")

    # Parse Excel-safe timestamp & derive hour bucket and date
    df['timestamp'] = df['ts'].apply(parse_ts_naive_utc)   # tz-naive
    df['hour'] = df['timestamp'].dt.floor('H')             # hourly bucket
    df['date'] = df['timestamp'].dt.date                   # daily bucket

    # Compute volumes per row
    rows = []
    for _, row in df.iterrows():
        base_price = _normalize_number(row['base_price'])
        asks = parse_orderbook_blob(row['asks'])
        bids = parse_orderbook_blob(row['bids'])

        # Not required for band sums, but tidy:
        asks = sorted(asks, key=lambda d: d['px'])
        bids = sorted(bids, key=lambda d: d['px'], reverse=True)

        up_vol, down_vol = compute_up_down_volumes(base_price, asks, bids, args.band_pct)

        rows.append({
            'timestamp': row['timestamp'],   # tz-naive (Excel-safe)
            'hour': row['hour'],             # hourly bucket
            'date': row['date'],             # daily bucket
            'ts_raw': row['ts'],
            'symbol': row['symbol'],
            'base_price': base_price,
            'band_pct': args.band_pct,
            'up_volume_asks': up_vol,
            'down_volume_bids': down_vol,
        })

    out = pd.DataFrame(rows)

    # ---- Hourly means per (symbol, hour) ----
    hourly = (
        out.groupby(['symbol', 'hour'], as_index=False)
           .agg(hourly_up_volume_mean=('up_volume_asks', 'mean'),
                hourly_down_volume_mean=('down_volume_bids', 'mean'))
    )

    # ---- Daily means per (symbol, date) ----
    daily = (
        out.groupby(['symbol', 'date'], as_index=False)
           .agg(daily_up_volume_mean=('up_volume_asks', 'mean'),
                daily_down_volume_mean=('down_volume_bids', 'mean'))
    )

    # Map hourly means back to each row as extra columns
    out = out.merge(hourly, on=['symbol', 'hour'], how='left')
    
    # Map daily means back to each row as extra columns
    out = out.merge(daily, on=['symbol', 'date'], how='left')

    # ---- Excel output: one file, main sheet per currency with all data and charts ----
    excel_path = "Results.xlsx"
    os.makedirs("plots", exist_ok=True)  # store plot images

    with pd.ExcelWriter(excel_path, engine='xlsxwriter') as writer:
        for sym, g in out.groupby('symbol'):
            # Main sheet per currency: rows sorted by time, includes hourly mean columns
            g_sorted = g.sort_values(['hour', 'timestamp'], kind='mergesort')
            sheet_main = (sym or 'UNKNOWN')[:31]
            g_sorted.to_excel(writer, sheet_name=sheet_main, index=False)

            # Get hourly and daily data for this symbol
            h = hourly[hourly['symbol'] == sym].sort_values('hour')
            d = daily[daily['symbol'] == sym].sort_values('date')

            # ---- Find the last day in the data ----
            last_date = g['date'].max()
            last_day_hourly = h[h['hour'].dt.date == last_date].copy()

            # ---- Plot 1: Last day hourly means (24 hours) ----
            if not last_day_hourly.empty:
                fig1 = plt.figure(figsize=(12, 6))
                x1 = pd.to_datetime(last_day_hourly['hour'])
                plt.plot(x1, last_day_hourly['hourly_up_volume_mean'], marker='o', markersize=6, 
                        label='Up Volume (mean)', linewidth=2)
                plt.plot(x1, last_day_hourly['hourly_down_volume_mean'], marker='o', markersize=6, 
                        label='Down Volume (mean)', linewidth=2)
                plt.title(f'{sym} — Last Day Hourly Volumes ({last_date}) ±{args.band_pct}%')
                plt.xlabel('Hour')
                plt.ylabel('Volume (mean)')
                plt.grid(True, alpha=0.3)
                plt.legend()
                plt.xticks(rotation=45)
                plt.tight_layout()

                plot1_path = os.path.join("plots", f"{sym}_last_day_hourly.png")
                fig1.savefig(plot1_path, dpi=150, bbox_inches='tight')
                plt.close(fig1)
            else:
                plot1_path = None

            # ---- Plot 2: Daily means across all days ----
            if not d.empty:
                fig2 = plt.figure(figsize=(12, 6))
                x2 = pd.to_datetime(d['date'])
                plt.plot(x2, d['daily_up_volume_mean'], marker='o', markersize=6, 
                        label='Daily Up Volume (mean)', linewidth=2)
                plt.plot(x2, d['daily_down_volume_mean'], marker='o', markersize=6, 
                        label='Daily Down Volume (mean)', linewidth=2)
                plt.title(f'{sym} — Daily Mean Volumes ±{args.band_pct}%')
                plt.xlabel('Date')
                plt.ylabel('Volume (mean)')
                plt.grid(True, alpha=0.3)
                plt.legend()
                plt.xticks(rotation=45)
                plt.tight_layout()

                plot2_path = os.path.join("plots", f"{sym}_daily_means.png")
                fig2.savefig(plot2_path, dpi=150, bbox_inches='tight')
                plt.close(fig2)
            else:
                plot2_path = None

            # Insert both plots into the main sheet only
            ws_main = writer.sheets[sheet_main]
            if plot1_path:
                ws_main.insert_image('J2', plot1_path)
            if plot2_path:
                ws_main.insert_image('J25', plot2_path)

    print(f"Wrote {excel_path} ")

if __name__ == '__main__':
    main()