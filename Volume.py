#!/usr/bin/env python3
"""
Band-Volumes Around Base Price (Single Excel, Sheets by Currency)

Usage examples:
  python3 Volume.py --csv orderbook_snapshots_last_10000_ejemplo.csv --band-pct 0.05 --sep ";"
  python3 Volume.py --csv data.csv --band-pct 1.0                  # defaults to sep=';'

What it does:
- Reads CSV with columns: ts;symbol;base_price;bids;asks
- Cleans/normalizes:
    * decimal commas -> dots
    * messy JSON-ish blobs in bids/asks (quotes, semicolons, braces)
- For each snapshot (row):
    * Up volume (asks): base_price <= px <= base_price*(1 + band_pct/100)
    * Down volume (bids): base_price*(1 - band_pct/100) <= px <= base_price
- Creates ONE Excel workbook "Results_ByCurrency.xlsx"
  with one sheet per symbol, sorted by timestamp (tz-naive, Excel-safe).
"""

from __future__ import annotations

import argparse
import csv
import json
import re
from typing import Iterable, List, Dict, Tuple, Optional

import pandas as pd

# ------------------------ Parsing helpers ------------------------

# Quote unquoted keys like {px: "4,77", sz: "43944,24"} -> {"px": "4,77", "sz": "43944,24"}
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
    # Remove outer quotes if present
    if s.startswith('"') and s.endswith('"'):
        s = s[1:-1]
    # Common cleanups
    s = s.replace('""', '"').replace(';', ',').replace('}{', '},{')
    s = _quote_unquoted_keys(s)
    if not s.startswith('['):
        s = f'[{s}]'
    try:
        raw = json.loads(s)
    except json.JSONDecodeError:
        # Last-ditch: add commas between adjacent objects
        s2 = re.sub(r'}\s*{', '},{', s)
        raw = json.loads(s2)
    # map to floats
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
    Also supports '+0000' -> '+00:00'.
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
    Parse to a tz-aware UTC timestamp, then strip tz to make it Excel-safe (tz-naive).
    Returns pandas NaT on failure.
    """
    cleaned = _clean_ts_for_parsing(s)
    ts = pd.to_datetime(cleaned, utc=True, errors='coerce')
    if pd.isna(ts):
        return pd.NaT
    # convert to UTC (if needed) and strip tz
    try:
        if getattr(ts, 'tz', None) is not None:
            ts = ts.tz_convert('UTC')
        return ts.tz_localize(None)
    except Exception:
        # If already tz-naive
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
                            band_pct: float) -> Tuple[float, float]:
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
    """
    Reads CSV as text (dtype=str) so we can clean/parse later.
    Defaults to sep=';' because your sample uses semicolons.
    """
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
    ap = argparse.ArgumentParser(description="Compute up/down volumes within a percentage band around base price and export one Excel (sheets by currency).")
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

    # Parse an Excel-safe timestamp column (tz-naive)
    df['timestamp'] = df['ts'].apply(parse_ts_naive_utc)

    # Compute volumes
    rows = []
    for _, row in df.iterrows():
        base_price = _normalize_number(row['base_price'])
        asks = parse_orderbook_blob(row['asks'])
        bids = parse_orderbook_blob(row['bids'])

        # Sorting not necessary for band sums, but makes sense for sanity
        asks = sorted(asks, key=lambda d: d['px'])
        bids = sorted(bids, key=lambda d: d['px'], reverse=True)

        up_vol, down_vol = compute_up_down_volumes(base_price, asks, bids, args.band_pct)

        rows.append({
            'timestamp': row['timestamp'],       # tz-naive (Excel-safe)
            'ts_raw': row['ts'],                 # original string, if you want to see it
            'symbol': row['symbol'],
            'base_price': base_price,
            'band_pct': args.band_pct,
            'up_volume_asks': up_vol,
            'down_volume_bids': down_vol,
        })

    out = pd.DataFrame(rows)

    # Write ONE Excel: sheet per currency, sorted by date
    excel_path = "Results_ByCurrency.xlsx"
    with pd.ExcelWriter(excel_path, engine='xlsxwriter') as writer:
        for sym, g in out.groupby('symbol'):
            g_sorted = g.sort_values('timestamp', kind='mergesort')
            sheet = (sym or 'UNKNOWN')[:31]   # Excel sheet name limit
            g_sorted.to_excel(writer, sheet_name=sheet, index=False)

    print(f"Wrote {excel_path}")

if __name__ == '__main__':
    main()
