#!/usr/bin/env python3
"""
Shared utilities for parsing orderbook data from CSV files.

This module contains common functions used by both ReadCSV.py and Volume.py
to parse and normalize orderbook data.
"""

import json
import re
from typing import List, Dict


# Regex pattern to quote unquoted JSON keys
_key_quote_pattern = re.compile(r'(?<=\{|,)\s*([A-Za-z_][A-Za-z0-9_]*)\s*:')


def _quote_unquoted_keys(s: str) -> str:
    """
    Add quotes around unquoted JSON keys.
    
    Args:
        s: JSON-like string with potentially unquoted keys
        
    Returns:
        String with properly quoted keys
    """
    return re.sub(_key_quote_pattern, r'"\1":', s)


def _normalize_number(x) -> float:
    """
    Convert various number formats to float.
    
    Handles:
    - Decimal commas (4,77 -> 4.77)
    - Quoted numbers ("4.77" -> 4.77)
    - Whitespace and mixed quotes
    
    Args:
        x: Number in various formats (int, float, string)
        
    Returns:
        Normalized float value, 0.0 if conversion fails
    """
    if isinstance(x, (int, float)):
        return float(x)
    
    s = str(x).strip().replace(",", ".").replace('"', '').replace("'", '')
    try:
        return float(s)
    except ValueError:
        # Extract first valid number found in string
        m = re.search(r"[-+]?\d*\.?\d+", s)
        return float(m.group(0)) if m else 0.0


def parse_orderbook_blob(blob: str) -> List[Dict[str, float]]:
    """
    Parse a messy orderbook blob into a clean list of price-size levels.
    
    Handles various formatting issues:
    - Doubled quotes around the entire string
    - Semicolons used as separators instead of commas
    - Concatenated JSON objects without proper array formatting
    - Unquoted JSON keys
    
    Args:
        blob: Raw orderbook string from CSV
        
    Returns:
        List of dictionaries with 'px' (price) and 'sz' (size) keys
        
    Example:
        >>> parse_orderbook_blob('px:100,sz:0.5;px:101,sz:0.3')
        [{'px': 100.0, 'sz': 0.5}, {'px': 101.0, 'sz': 0.3}]
    """
    if not blob:
        return []
    
    s = str(blob).strip()
    
    # Remove outer quotes if present
    if s.startswith('"') and s.endswith('"'):
        s = s[1:-1]
    
    # Clean up common formatting issues
    s = s.replace('""', '"')        # Handle doubled quotes
    s = s.replace(';', ',')         # Replace semicolons with commas
    s = s.replace('}{', '},{')      # Separate concatenated objects
    
    # Quote unquoted keys
    s = _quote_unquoted_keys(s)
    
    # Ensure it's a valid JSON array
    if not s.startswith('['):
        s = f'[{s}]'
    
    try:
        raw = json.loads(s)
    except json.JSONDecodeError:
        # Final attempt: fix any remaining object concatenation issues
        s2 = re.sub(r'}\s*{', '},{', s)
        raw = json.loads(s2)
    
    # Normalize all price and size values
    return [
        {
            'px': _normalize_number(level.get('px')), 
            'sz': _normalize_number(level.get('sz'))
        } 
        for level in raw
    ]


def validate_required_columns(df_columns: List[str], required_cols: set) -> None:
    """
    Validate that a DataFrame has all required columns.
    
    Args:
        df_columns: List of column names from DataFrame
        required_cols: Set of required column names
        
    Raises:
        SystemExit: If any required columns are missing
    """
    missing = required_cols.difference(df_columns)
    if missing:
        raise SystemExit(f"CSV is missing required columns: {sorted(missing)}")


# Common constants that both scripts might use
REQUIRED_CSV_COLUMNS = {'ts', 'symbol', 'base_price', 'bids', 'asks'}