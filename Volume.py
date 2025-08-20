#!/usr/bin/env python3
"""
  python3 Volume.py --csv orderbook_snapshots_last_10000_ejemplo.csv --band-pct 5 --sep ";"
   * Up volume (asks): base_price <= px <= base_price*(1 + band_pct/100)

"""

from __future__ import annotations

import argparse
import csv
import re
from typing import List, Dict, Tuple, Optional
import os

import pandas as pd
import matplotlib.pyplot as plt

# Import shared utilities
from orderbook_utils import _normalize_number, parse_orderbook_blob, validate_required_columns, REQUIRED_CSV_COLUMNS

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

def sum_values_within_band(levels: List[Dict[str, float]], lower_px: float, upper_px: float) -> float:
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
            total += sz*px
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

    up_volume = sum_values_within_band(asks, bp, up_upper)
    down_volume = sum_values_within_band(bids, down_lower, bp)
    return up_volume, down_volume

# ------------------------ CSV reading ------------------------

def read_input_csv(path: str, sep_arg: Optional[str]) -> pd.DataFrame:
    sep = ';' if sep_arg is None else sep_arg
    df = pd.read_csv(path,
                     sep=sep,
                     engine='python',
                     dtype=str,
                     quoting=csv.QUOTE_MINIMAL)
    validate_required_columns(df.columns, REQUIRED_CSV_COLUMNS)
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
    df['hour'] = df['timestamp'].dt.floor('h')             # hourly bucket
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
           .agg(hourly_up_volume_usd_mean=('up_volume_asks', 'mean'),
                hourly_down_volume_usd_mean=('down_volume_bids', 'mean'))
    )

    # ---- Daily means per (symbol, date) ----
    daily = (
        out.groupby(['symbol', 'date'], as_index=False)
           .agg(daily_up_volume_usd_mean=('up_volume_asks', 'mean'),
                daily_down_volume_usd_mean=('down_volume_bids', 'mean'))
    )

    # Map hourly means back to each row as extra columns
    out = out.merge(hourly, on=['symbol', 'hour'], how='left')
    
    # Map daily means back to each row as extra columns
    out = out.merge(daily, on=['symbol', 'date'], how='left')

    # Print processing statistics
    total_days = out['date'].nunique()
    total_records = len(out)
    symbols_processed = out['symbol'].nunique()
    
    print(f"\n=== Processing Summary ===")
    print(f"Total days processed: {total_days}")
    print(f"Total records processed: {total_records}")
    print(f"Symbols processed: {symbols_processed}")
    
    for sym in out['symbol'].unique():
        sym_data = out[out['symbol'] == sym]
        sym_days = sym_data['date'].nunique()
        sym_records = len(sym_data)
        print(f"  {sym}: {sym_days} days, {sym_records} records")
    print("========================\n")

    # ---- Excel output: one file, main sheet per currency with all data and charts ----
    excel_path = "Results.xlsx"
    os.makedirs("plots", exist_ok=True)  # store plot images

    with pd.ExcelWriter(excel_path, engine='xlsxwriter') as writer:
        for sym, g in out.groupby('symbol'):
            # Main sheet per currency: rows sorted by time, includes hourly mean columns
            g_sorted = g.sort_values(['hour', 'timestamp'], kind='mergesort')
            sheet_main = (sym or 'UNKNOWN')[:31]
            g_sorted.to_excel(writer, sheet_name=sheet_main, index=False)

    # ---- Create aggregated data for plotting ----
    
    # Mean and median of hourly means by hour of day (across all days)
    hourly['hour_of_day'] = hourly['hour'].dt.hour
    hourly_by_hour = (
    hourly.groupby(['symbol', 'hour_of_day'], as_index=False)
          .agg(mean_up_volume_usd=('hourly_up_volume_usd_mean', 'mean'),
               median_up_volume_usd=('hourly_up_volume_usd_mean', 'median'),
               up_volume_usd_p25=('hourly_up_volume_usd_mean', lambda x: x.quantile(0.25)),
               mean_down_volume_usd=('hourly_down_volume_usd_mean', 'mean'),
               median_down_volume_usd=('hourly_down_volume_usd_mean', 'median'),
               down_volume_usd_p25=('hourly_down_volume_usd_mean', lambda x: x.quantile(0.25)))
)
    
    # Mean of daily means by day of week (across all weeks)  
    daily['day_of_week'] = pd.to_datetime(daily['date']).dt.day_name()
    daily_by_weekday = (
        daily.groupby(['symbol', 'day_of_week'], as_index=False)
             .agg(mean_up_volume_usd=('daily_up_volume_usd_mean', 'mean'),
                  mean_down_volume_usd=('daily_down_volume_usd_mean', 'mean'))
    )
    
    # Order days of week properly
    day_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
    daily_by_weekday['day_of_week'] = pd.Categorical(daily_by_weekday['day_of_week'], categories=day_order, ordered=True)
    daily_by_weekday = daily_by_weekday.sort_values(['symbol', 'day_of_week'])

    # ---- Create plots ----
    # Plot mean and median together in single plot
    for sym, h_group in hourly_by_hour.groupby('symbol'):
        plt.figure(figsize=(12, 6))
        
        # Ask volume - mean, median, and 25th percentile
        plt.plot(h_group['hour_of_day'], h_group['mean_up_volume_usd'], 
                label='Ask Mean', marker='o', linewidth=2.5, color='#2ca02c')
        plt.plot(h_group['hour_of_day'], h_group['median_up_volume_usd'], 
                label='Ask Median', marker='s', linewidth=2, color='#2ca02c', linestyle='--', alpha=0.8)
        plt.plot(h_group['hour_of_day'], h_group['up_volume_usd_p25'], 
                label='Ask 25th Percentile', marker='^', linewidth=1.5, color='#2ca02c', linestyle=':', alpha=0.6)
        
        # Bid volume - mean, median, and 25th percentile
        plt.plot(h_group['hour_of_day'], h_group['mean_down_volume_usd'], 
                label='Bid Mean', marker='o', linewidth=2.5, color='#d62728')
        plt.plot(h_group['hour_of_day'], h_group['median_down_volume_usd'], 
                label='Bid Median', marker='s', linewidth=2, color='#d62728', linestyle='--', alpha=0.8)
        plt.plot(h_group['hour_of_day'], h_group['down_volume_usd_p25'], 
                label='Bid 25th Percentile', marker='^', linewidth=1.5, color='#d62728', linestyle=':', alpha=0.6)
    
        plt.xlabel('Hour of Day (UTC time +00)')
        plt.ylabel('Volume (USD)')
        plt.title(f'Mean vs Median Hourly Volume by Hour of Day - {sym} ({total_days} days) with a {args.band_pct}% band')
        plt.legend()

        plt.grid(True, alpha=0.3)
        plt.xticks(range(0, 24))
        plt.tight_layout()
        
        # Save plot
        plot_path = f"plots/hourly_volume_mean_median_{sym}.png"
        plt.savefig(plot_path, dpi=150, bbox_inches='tight')
        print(f"Saved hourly mean/median plot: {plot_path}")
        plt.show()

    # Plot mean of daily means by day of week
    for sym, d_group in daily_by_weekday.groupby('symbol'):
        plt.figure(figsize=(10, 6))
        plt.plot(d_group['day_of_week'], d_group['mean_up_volume_usd'], label='Ask Volume USD', marker='o', linewidth=2)
        plt.plot(d_group['day_of_week'], d_group['mean_down_volume_usd'], label='Bid Volume USD', marker='s', linewidth=2)
        plt.xlabel('Day of Week')
        plt.ylabel('Mean of Daily Means (USD)')
        plt.title(f'Average Volume by Day of Week ({sym}) ({total_days} days) with a {args.band_pct}% band')
        plt.legend()
        plt.grid(True, alpha=0.3)
        plt.xticks(rotation=45)
        plt.tight_layout()
        
        # Save plot
        plot_path = f"plots/weekly_volume_{sym}.png"
        plt.savefig(plot_path, dpi=150, bbox_inches='tight')
        print(f"Saved weekly plot: {plot_path}")
        plt.show()

    print(f"Results saved to: {excel_path}")

if __name__ == '__main__':
    main()