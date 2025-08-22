#!/usr/bin/env python3
"""
Volatility Calculator for 5-minute orderbook data.

Reads CSV files with 5-minute price updates and calculates:
- Multiple time period volatilities (3, 5, 10, 30, 60, 90 minutes, 1 day, 1 year)
- Exports results to Excel with formatted tables
"""

import pandas as pd
import numpy as np
import sys
import json
from pathlib import Path
from typing import Optional, Dict, Tuple, List
from orderbook_utils import validate_required_columns, REQUIRED_CSV_COLUMNS
import openpyxl
from openpyxl.styles import Font, PatternFill, Alignment
from openpyxl.utils import get_column_letter


def calculate_log_returns(prices: pd.Series) -> pd.Series:
    """
    Calculate logarithmic returns from price series.
    
    Args:
        prices: Series of prices
        
    Returns:
        Series of log returns (first value will be NaN)
    """
    # Remove any zero or negative prices (shouldn't happen but just in case)
    prices = prices[prices > 0]
    
    # Calculate log returns: ln(P_t / P_{t-1})
    log_returns = np.log(prices / prices.shift(1))
    
    return log_returns.dropna()


def parse_orderbook_levels(orderbook_str: str) -> Tuple[List[float], List[float]]:
    """
    Parse orderbook JSON string and extract all price levels and quantities.
    
    Args:
        orderbook_str: JSON string containing orderbook data
        
    Returns:
        Tuple of (prices, quantities) as lists of floats
    """
    try:
        orderbook = json.loads(orderbook_str)
        prices = [float(level["px"]) for level in orderbook]
        quantities = [float(level["sz"]) for level in orderbook]
        return prices, quantities
    except (json.JSONDecodeError, KeyError, ValueError):
        return [], []


def calculate_volatility_metrics(log_returns: pd.Series) -> Dict[str, float]:
    """
    Calculate volatility metrics for multiple time periods from log returns.
    
    Args:
        log_returns: Series of logarithmic returns (from 5-minute data)
        
    Returns:
        Dictionary with volatility metrics for different time periods
    """
    if len(log_returns) < 2:
        return {
            '3min_vol': 0.0,
            '5min_vol': 0.0,
            '10min_vol': 0.0,
            '30min_vol': 0.0,
            '60min_vol': 0.0,
            '90min_vol': 0.0,
            '1day_vol': 0.0,
            '1year_vol': 0.0,
            'sample_size': len(log_returns)
        }
    
    # 5-minute volatility (standard deviation of log returns) - this is our base
    vol_5min = log_returns.std()
    
    # Scale to different time periods using sqrt(time_ratio)
    vol_3min = vol_5min * np.sqrt(3/5)        # 3 minutes
    vol_10min = vol_5min * np.sqrt(10/5)      # 10 minutes = sqrt(2)
    vol_30min = vol_5min * np.sqrt(30/5)      # 30 minutes = sqrt(6)
    vol_60min = vol_5min * np.sqrt(60/5)      # 60 minutes = sqrt(12)
    vol_90min = vol_5min * np.sqrt(90/5)      # 90 minutes = sqrt(18)
    vol_1day = vol_5min * np.sqrt(1440/5)     # 1 day = 1440 minutes = sqrt(288)
    vol_1year = vol_5min * np.sqrt(525600/5)  # 1 year = 525600 minutes = sqrt(105120)
    
    return {
        '3min_vol': vol_3min,
        '5min_vol': vol_5min,
        '10min_vol': vol_10min,
        '30min_vol': vol_30min,
        '60min_vol': vol_60min,
        '90min_vol': vol_90min,
        '1day_vol': vol_1day,
        '1year_vol': vol_1year,
        'sample_size': len(log_returns),
        'mean_return': log_returns.mean()
    }


def analyze_csv_volatility(csv_path: str, symbol_filters: Optional[list] = None) -> Dict:
    """
    Main function to analyze volatility from CSV file.
    
    Args:
        csv_path: Path to CSV file
        symbol_filters: Optional list of symbols to filter (e.g., ['BTCUSDT', 'ETHUSDT'])
        
    Returns:
        Dictionary with analysis results
    """
    try:
        df = pd.read_csv(csv_path)
    except Exception as e:
        raise SystemExit(f"Error reading CSV: {e}")
    
    # Validate required columns
    validate_required_columns(df.columns.tolist(), REQUIRED_CSV_COLUMNS)
    
    # Filter by symbols if specified
    if symbol_filters:
        df = df[df['symbol'].isin(symbol_filters)]
        if len(df) == 0:
            raise SystemExit(f"No data found for symbols: {symbol_filters}")
        
        # Check which symbols were actually found
        found_symbols = df['symbol'].unique()
        missing_symbols = set(symbol_filters) - set(found_symbols)
        if missing_symbols:
            print(f"Warning: Symbols not found in data: {missing_symbols}")
    
    # Convert timestamp to datetime if it's not already
    if 'ts' in df.columns:
        # Handle mixed timestamp formats (some with microseconds, some without)
        df['ts'] = pd.to_datetime(df['ts'], format='mixed')
        df = df.sort_values('ts')
    
    results = {} 
    
    # Analyze each symbol separately
    for symbol in df['symbol'].unique():
        symbol_data = df[df['symbol'] == symbol].copy()
        
        # Extract prices
        prices = symbol_data['base_price'].astype(float)
        
        # Set datetime index for time series analysis
        if 'ts' in symbol_data.columns:
            prices.index = symbol_data['ts']
        
        # Calculate log returns
        log_returns = calculate_log_returns(prices)
        
        # Calculate volatility metrics
        vol_metrics = calculate_volatility_metrics(log_returns)
        
        # Parse orderbook data for levels (get latest snapshot)
        latest_row = symbol_data.iloc[-1]
        bid_levels = []
        bid_quantities = []
        ask_levels = []
        ask_quantities = []
        
        if 'bids' in symbol_data.columns and pd.notna(latest_row['bids']):
            bid_levels, bid_quantities = parse_orderbook_levels(latest_row['bids'])
        
        if 'asks' in symbol_data.columns and pd.notna(latest_row['asks']):
            ask_levels, ask_quantities = parse_orderbook_levels(latest_row['asks'])
        
        results[symbol] = {
            'volatility_metrics': vol_metrics,
            'price_stats': {
                'min_price': prices.min(),
                'max_price': prices.max(),
                'mean_price': prices.mean(),
                'latest_price': prices.iloc[-1],  # Last snapshot price
                'price_range_pct': ((prices.max() - prices.min()) / prices.mean()) * 100
            },
            'orderbook_levels': {
                'bid_levels': bid_levels,
                'bid_quantities': bid_quantities,
                'ask_levels': ask_levels,
                'ask_quantities': ask_quantities
            },
            'data_quality': {
                'total_observations': len(prices),
                'missing_values': prices.isna().sum(),
                'time_span_hours': (prices.index[-1] - prices.index[0]).total_seconds() / 3600 if hasattr(prices.index, 'total_seconds') else None
            }
        }
    
    return results


def create_excel_volatility_tables(results: Dict, output_filename: str = "volatility_analysis.xlsx") -> None:
    """
    Create Excel file with four consolidated tables:
    1. Volatility Analysis (% values without % symbol)
    2. Buy Target Prices
    3. Sell Target Prices
    4. Orderbook Levels Analysis - Bid and Ask tables using actual orderbook prices
    
    Table 4 has 5 columns per asset: % Difference, Quantity, USD Value, Accumulated Qty, Accumulated USD
    
    Args:
        results: Results from analyze_csv_volatility()
        output_filename: Name of the Excel file to create
    """
    # Create workbook and worksheet
    workbook = openpyxl.Workbook()
    worksheet = workbook.active
    worksheet.title = "Trading Analysis"
    
    # Define styles
    header_font = Font(bold=True, size=12, color="FFFFFF")
    header_fill = PatternFill(start_color="366092", end_color="366092", fill_type="solid")
    table_title_font = Font(bold=True, size=14, color="000000")
    cell_alignment = Alignment(horizontal="center", vertical="center")
    positive_fill = PatternFill(start_color="E8F5E8", end_color="E8F5E8", fill_type="solid")  # Light green
    negative_fill = PatternFill(start_color="FFF2F2", end_color="FFF2F2", fill_type="solid")  # Light red
    
    # Get symbols and time periods
    symbols = list(results.keys())
    time_periods = [3, 5, 10, 30, 60, 90, 1440, 525600]  # Added 1 day (1440 min) and 1 year (525600 min)
    time_labels = ["3min", "5min", "10min", "30min", "60min", "90min", "1day", "1year"]
    vol_keys = ['3min_vol', '5min_vol', '10min_vol', '30min_vol', '60min_vol', '90min_vol', '1day_vol', '1year_vol']
    
    current_row = 1
    
    # Add note about percentages at the top
    worksheet.cell(row=current_row, column=1, value="NOTE: All percentage values in tables below are numbers without % symbol for easier Excel editing")
    worksheet.cell(row=current_row, column=1).font = Font(italic=True, size=10, color="666666")
    current_row += 2
    
    # TABLE 1: Volatility Analysis (Delta Percentages)
    worksheet.cell(row=current_row, column=1, value="1. Volatility Analysis - Delta Percentages")
    worksheet.cell(row=current_row, column=1).font = table_title_font
    current_row += 2
    
    # Headers
    worksheet.cell(row=current_row, column=1, value="Minutes")
    worksheet.cell(row=current_row, column=1).font = header_font
    worksheet.cell(row=current_row, column=1).fill = header_fill
    worksheet.cell(row=current_row, column=1).alignment = cell_alignment
    
    for col_idx, symbol in enumerate(symbols, start=2):
        worksheet.cell(row=current_row, column=col_idx, value=f"{symbol} (% values)")
        worksheet.cell(row=current_row, column=col_idx).font = header_font
        worksheet.cell(row=current_row, column=col_idx).fill = header_fill
        worksheet.cell(row=current_row, column=col_idx).alignment = cell_alignment
    
    current_row += 1
    
    # Data rows for volatility
    for row_idx, (minutes, time_label, vol_key) in enumerate(zip(time_periods, time_labels, vol_keys)):
        if time_label == "1day":
            display_label = "1 day"
        elif time_label == "1year":
            display_label = "1 year"
        else:
            display_label = str(minutes)
            
        worksheet.cell(row=current_row, column=1, value=display_label)
        worksheet.cell(row=current_row, column=1).alignment = cell_alignment
        worksheet.cell(row=current_row, column=1).font = Font(bold=True)
        
        for col_idx, symbol in enumerate(symbols, start=2):
            volatility = results[symbol]['volatility_metrics'][vol_key]
            worksheet.cell(row=current_row, column=col_idx, value=round(volatility*100, 3))
            worksheet.cell(row=current_row, column=col_idx).alignment = cell_alignment
        
        current_row += 1
    
    current_row += 2
    
    # TABLE 2: Buy Target Prices
    worksheet.cell(row=current_row, column=1, value="2. Buy Target Prices - Base Price + Slippage")
    worksheet.cell(row=current_row, column=1).font = table_title_font
    current_row += 2
    
    # Headers
    worksheet.cell(row=current_row, column=1, value="Minutes")
    worksheet.cell(row=current_row, column=1).font = header_font
    worksheet.cell(row=current_row, column=1).fill = header_fill
    worksheet.cell(row=current_row, column=1).alignment = cell_alignment
    
    for col_idx, symbol in enumerate(symbols, start=2):
        base_price = results[symbol]['price_stats']['latest_price']
        
        # Format base price for header
        if base_price >= 1000:
            base_price_str = f"{base_price:,.0f}"
        elif base_price >= 1:
            base_price_str = f"{base_price:.2f}"
        else:
            base_price_str = f"{base_price:.4f}"
            
        worksheet.cell(row=current_row, column=col_idx, value=f"{symbol} (${base_price_str})")
        worksheet.cell(row=current_row, column=col_idx).font = header_font
        worksheet.cell(row=current_row, column=col_idx).fill = header_fill
        worksheet.cell(row=current_row, column=col_idx).alignment = cell_alignment
    
    current_row += 1
    
    # Data rows for buy prices
    for row_idx, (minutes, time_label, vol_key) in enumerate(zip(time_periods, time_labels, vol_keys)):
        if time_label == "1day":
            display_label = "1 day"
        elif time_label == "1year":
            display_label = "1 year"
        else:
            display_label = str(minutes)
            
        worksheet.cell(row=current_row, column=1, value=display_label)
        worksheet.cell(row=current_row, column=1).alignment = cell_alignment
        worksheet.cell(row=current_row, column=1).font = Font(bold=True)
        
        for col_idx, symbol in enumerate(symbols, start=2):
            base_price = results[symbol]['price_stats']['latest_price']
            volatility = results[symbol]['volatility_metrics'][vol_key]
            buy_price = base_price + (volatility * base_price)
            
            # Format based on price magnitude
            if buy_price >= 1000:
                price_str = f"{buy_price:,.0f}"
            elif buy_price >= 1:
                price_str = f"{buy_price:.2f}"
            else:
                price_str = f"{buy_price:.4f}"
                
            worksheet.cell(row=current_row, column=col_idx, value=price_str)
            worksheet.cell(row=current_row, column=col_idx).alignment = cell_alignment
        
        current_row += 1
    
    current_row += 2
    
    # TABLE 3: Sell Target Prices
    worksheet.cell(row=current_row, column=1, value="3. Sell Target Prices - Base Price - Slippage")
    worksheet.cell(row=current_row, column=1).font = table_title_font
    current_row += 2
    
    # Headers
    worksheet.cell(row=current_row, column=1, value="Minutes")
    worksheet.cell(row=current_row, column=1).font = header_font
    worksheet.cell(row=current_row, column=1).fill = header_fill
    worksheet.cell(row=current_row, column=1).alignment = cell_alignment
    
    for col_idx, symbol in enumerate(symbols, start=2):
        base_price = results[symbol]['price_stats']['mean_price']
        
        # Format base price for header
        if base_price >= 1000:
            base_price_str = f"{base_price:,.0f}"
        elif base_price >= 1:
            base_price_str = f"{base_price:.2f}"
        else:
            base_price_str = f"{base_price:.4f}"
            
        worksheet.cell(row=current_row, column=col_idx, value=f"{symbol} (${base_price_str})")
        worksheet.cell(row=current_row, column=col_idx).font = header_font
        worksheet.cell(row=current_row, column=col_idx).fill = header_fill
        worksheet.cell(row=current_row, column=col_idx).alignment = cell_alignment
    
    current_row += 1
    
    # Data rows for sell prices
    for row_idx, (minutes, time_label, vol_key) in enumerate(zip(time_periods, time_labels, vol_keys)):
        if time_label == "1day":
            display_label = "1 day"
        elif time_label == "1year":
            display_label = "1 year"
        else:
            display_label = str(minutes)
            
        worksheet.cell(row=current_row, column=1, value=display_label)
        worksheet.cell(row=current_row, column=1).alignment = cell_alignment
        worksheet.cell(row=current_row, column=1).font = Font(bold=True)
        
        for col_idx, symbol in enumerate(symbols, start=2):
            base_price = results[symbol]['price_stats']['latest_price']
            volatility = results[symbol]['volatility_metrics'][vol_key]
            sell_price = base_price - (volatility * base_price)
            
            # Format based on price magnitude
            if sell_price >= 1000:
                price_str = f"{sell_price:,.0f}"
            elif sell_price >= 1:
                price_str = f"{sell_price:.2f}"
            else:
                price_str = f"{sell_price:.4f}"
                
            worksheet.cell(row=current_row, column=col_idx, value=price_str)
            worksheet.cell(row=current_row, column=col_idx).alignment = cell_alignment
        
        current_row += 1
    
    current_row += 2
    
    # TABLE 4: Levels Analysis - Bid and Ask Tables
    worksheet.cell(row=current_row, column=1, value="4. Orderbook Levels Analysis - Percentage Difference from Base Price")
    worksheet.cell(row=current_row, column=1).font = table_title_font
    current_row += 2
    
    # BID LEVELS TABLE
    worksheet.cell(row=current_row, column=1, value="BID LEVELS (% Below Base Price)")
    worksheet.cell(row=current_row, column=1).font = Font(bold=True, color="008000")  # Green
    current_row += 1
    
    # Headers for bid levels - 5 columns per symbol
    worksheet.cell(row=current_row, column=1, value="Level")
    worksheet.cell(row=current_row, column=1).font = header_font
    worksheet.cell(row=current_row, column=1).fill = header_fill
    worksheet.cell(row=current_row, column=1).alignment = cell_alignment
    
    col_idx = 2
    for symbol in symbols:
        # Column 1: Percentage
        worksheet.cell(row=current_row, column=col_idx, value=f"{symbol} %")
        worksheet.cell(row=current_row, column=col_idx).font = header_font
        worksheet.cell(row=current_row, column=col_idx).fill = header_fill
        worksheet.cell(row=current_row, column=col_idx).alignment = cell_alignment
        
        # Column 2: Quantity
        worksheet.cell(row=current_row, column=col_idx + 1, value=f"{symbol} Qty")
        worksheet.cell(row=current_row, column=col_idx + 1).font = header_font
        worksheet.cell(row=current_row, column=col_idx + 1).fill = header_fill
        worksheet.cell(row=current_row, column=col_idx + 1).alignment = cell_alignment
        
        # Column 3: USD Value
        worksheet.cell(row=current_row, column=col_idx + 2, value=f"{symbol} USD")
        worksheet.cell(row=current_row, column=col_idx + 2).font = header_font
        worksheet.cell(row=current_row, column=col_idx + 2).fill = header_fill
        worksheet.cell(row=current_row, column=col_idx + 2).alignment = cell_alignment
        
        # Column 4: Accumulated Quantity
        worksheet.cell(row=current_row, column=col_idx + 3, value=f"{symbol} Acc Qty")
        worksheet.cell(row=current_row, column=col_idx + 3).font = header_font
        worksheet.cell(row=current_row, column=col_idx + 3).fill = header_fill
        worksheet.cell(row=current_row, column=col_idx + 3).alignment = cell_alignment
        
        # Column 5: Accumulated USD
        worksheet.cell(row=current_row, column=col_idx + 4, value=f"{symbol} Acc USD")
        worksheet.cell(row=current_row, column=col_idx + 4).font = header_font
        worksheet.cell(row=current_row, column=col_idx + 4).fill = header_fill
        worksheet.cell(row=current_row, column=col_idx + 4).alignment = cell_alignment
        
        col_idx += 5  # Move to next symbol's columns
    
    current_row += 1
    
    # Data rows for bid levels - using actual orderbook bid prices (all levels)
    max_bid_levels = max([len(results[symbol]['orderbook_levels']['bid_levels']) for symbol in symbols]) if symbols else 0
    
    # Initialize accumulators for each symbol
    symbol_accumulators = {}
    for symbol in symbols:
        symbol_accumulators[symbol] = {'qty': 0.0, 'usd': 0.0}
    
    for level in range(1, max_bid_levels + 1):
        worksheet.cell(row=current_row, column=1, value=level)
        worksheet.cell(row=current_row, column=1).alignment = cell_alignment
        worksheet.cell(row=current_row, column=1).font = Font(bold=True)
        
        col_idx = 2
        for symbol in symbols:
            base_price = results[symbol]['price_stats']['latest_price']
            bid_levels = results[symbol]['orderbook_levels']['bid_levels']
            bid_quantities = results[symbol]['orderbook_levels']['bid_quantities']
            
            if level <= len(bid_levels):
                level_price = bid_levels[level - 1]  # level 1 = index 0
                level_quantity = bid_quantities[level - 1]
                usd_value = level_quantity * level_price
                
                # Update accumulators
                symbol_accumulators[symbol]['qty'] += level_quantity
                symbol_accumulators[symbol]['usd'] += usd_value
                
                # Column 1: Percentage difference
                pct_diff = (level_price / base_price * 100) - 100
                cell = worksheet.cell(row=current_row, column=col_idx, value=round(pct_diff, 3))
                cell.alignment = cell_alignment
                cell.fill = positive_fill
                
                # Column 2: Quantity in asset currency
                cell = worksheet.cell(row=current_row, column=col_idx + 1, value=round(level_quantity, 6))
                cell.alignment = cell_alignment
                cell.fill = positive_fill
                
                # Column 3: USD value (quantity * price)
                cell = worksheet.cell(row=current_row, column=col_idx + 2, value=round(usd_value, 2))
                cell.alignment = cell_alignment
                cell.fill = positive_fill
                
                # Column 4: Accumulated quantity
                cell = worksheet.cell(row=current_row, column=col_idx + 3, value=round(symbol_accumulators[symbol]['qty'], 6))
                cell.alignment = cell_alignment
                cell.fill = positive_fill
                
                # Column 5: Accumulated USD
                cell = worksheet.cell(row=current_row, column=col_idx + 4, value=round(symbol_accumulators[symbol]['usd'], 2))
                cell.alignment = cell_alignment
                cell.fill = positive_fill
                
            else:
                # No data for this level - fill all 5 columns with N/A
                for i in range(5):
                    cell = worksheet.cell(row=current_row, column=col_idx + i, value="N/A")
                    cell.alignment = cell_alignment
            
            col_idx += 5  # Move to next symbol's columns
        
        current_row += 1
    
    current_row += 1
    
    # ASK LEVELS TABLE
    worksheet.cell(row=current_row, column=1, value="ASK LEVELS (% Above Base Price)")
    worksheet.cell(row=current_row, column=1).font = Font(bold=True, color="CC0000")  # Red
    current_row += 1
    
    # Headers for ask levels - 5 columns per symbol
    worksheet.cell(row=current_row, column=1, value="Level")
    worksheet.cell(row=current_row, column=1).font = header_font
    worksheet.cell(row=current_row, column=1).fill = header_fill
    worksheet.cell(row=current_row, column=1).alignment = cell_alignment
    
    col_idx = 2
    for symbol in symbols:
        # Column 1: Percentage
        worksheet.cell(row=current_row, column=col_idx, value=f"{symbol} %")
        worksheet.cell(row=current_row, column=col_idx).font = header_font
        worksheet.cell(row=current_row, column=col_idx).fill = header_fill
        worksheet.cell(row=current_row, column=col_idx).alignment = cell_alignment
        
        # Column 2: Quantity
        worksheet.cell(row=current_row, column=col_idx + 1, value=f"{symbol} Qty")
        worksheet.cell(row=current_row, column=col_idx + 1).font = header_font
        worksheet.cell(row=current_row, column=col_idx + 1).fill = header_fill
        worksheet.cell(row=current_row, column=col_idx + 1).alignment = cell_alignment
        
        # Column 3: USD Value
        worksheet.cell(row=current_row, column=col_idx + 2, value=f"{symbol} USD")
        worksheet.cell(row=current_row, column=col_idx + 2).font = header_font
        worksheet.cell(row=current_row, column=col_idx + 2).fill = header_fill
        worksheet.cell(row=current_row, column=col_idx + 2).alignment = cell_alignment
        
        # Column 4: Accumulated Quantity
        worksheet.cell(row=current_row, column=col_idx + 3, value=f"{symbol} Acc Qty")
        worksheet.cell(row=current_row, column=col_idx + 3).font = header_font
        worksheet.cell(row=current_row, column=col_idx + 3).fill = header_fill
        worksheet.cell(row=current_row, column=col_idx + 3).alignment = cell_alignment
        
        # Column 5: Accumulated USD
        worksheet.cell(row=current_row, column=col_idx + 4, value=f"{symbol} Acc USD")
        worksheet.cell(row=current_row, column=col_idx + 4).font = header_font
        worksheet.cell(row=current_row, column=col_idx + 4).fill = header_fill
        worksheet.cell(row=current_row, column=col_idx + 4).alignment = cell_alignment
        
        col_idx += 5  # Move to next symbol's columns
    
    current_row += 1
    
    # Data rows for ask levels - using actual orderbook ask prices (all levels)
    max_ask_levels = max([len(results[symbol]['orderbook_levels']['ask_levels']) for symbol in symbols]) if symbols else 0
    
    # Reset accumulators for ask levels
    symbol_accumulators = {}
    for symbol in symbols:
        symbol_accumulators[symbol] = {'qty': 0.0, 'usd': 0.0}
    
    for level in range(1, max_ask_levels + 1):
        worksheet.cell(row=current_row, column=1, value=level)
        worksheet.cell(row=current_row, column=1).alignment = cell_alignment
        worksheet.cell(row=current_row, column=1).font = Font(bold=True)
        
        col_idx = 2
        for symbol in symbols:
            base_price = results[symbol]['price_stats']['latest_price']
            ask_levels = results[symbol]['orderbook_levels']['ask_levels']
            ask_quantities = results[symbol]['orderbook_levels']['ask_quantities']
            
            if level <= len(ask_levels):
                level_price = ask_levels[level - 1]  # level 1 = index 0
                level_quantity = ask_quantities[level - 1]
                usd_value = level_quantity * level_price
                
                # Update accumulators
                symbol_accumulators[symbol]['qty'] += level_quantity
                symbol_accumulators[symbol]['usd'] += usd_value
                
                # Column 1: Percentage difference
                pct_diff = (level_price / base_price * 100) - 100
                cell = worksheet.cell(row=current_row, column=col_idx, value=round(pct_diff, 3))
                cell.alignment = cell_alignment
                cell.fill = negative_fill
                
                # Column 2: Quantity in asset currency
                cell = worksheet.cell(row=current_row, column=col_idx + 1, value=round(level_quantity, 6))
                cell.alignment = cell_alignment
                cell.fill = negative_fill
                
                # Column 3: USD value (quantity * price)
                cell = worksheet.cell(row=current_row, column=col_idx + 2, value=round(usd_value, 2))
                cell.alignment = cell_alignment
                cell.fill = negative_fill
                
                # Column 4: Accumulated quantity
                cell = worksheet.cell(row=current_row, column=col_idx + 3, value=round(symbol_accumulators[symbol]['qty'], 6))
                cell.alignment = cell_alignment
                cell.fill = negative_fill
                
                # Column 5: Accumulated USD
                cell = worksheet.cell(row=current_row, column=col_idx + 4, value=round(symbol_accumulators[symbol]['usd'], 2))
                cell.alignment = cell_alignment
                cell.fill = negative_fill
                
            else:
                # No data for this level - fill all 5 columns with N/A
                for i in range(5):
                    cell = worksheet.cell(row=current_row, column=col_idx + i, value="N/A")
                    cell.alignment = cell_alignment
            
            col_idx += 5  # Move to next symbol's columns
        
        current_row += 1
    
    # Adjust column widths
    worksheet.column_dimensions['A'].width = 10
    # For tables 1-3: normal width
    for col_idx in range(2, len(symbols) + 2):
        col_letter = get_column_letter(col_idx)
        worksheet.column_dimensions[col_letter].width = 15
    # For table 4: wider columns (5 columns per symbol)
    total_table4_cols = len(symbols) * 5
    for col_idx in range(2, total_table4_cols + 2):
        col_letter = get_column_letter(col_idx)
        worksheet.column_dimensions[col_letter].width = 12
    
    # Save the workbook
    workbook.save(output_filename)


def main():
    """Main execution function."""
    if len(sys.argv) < 2:
        print("Usage: python volatility_calculator.py <csv_file> [symbol1] [symbol2] [symbol3] ...")
        print("Examples:")
        print("  python volatility_calculator.py orderbook_data.csv                    # Analyze all symbols")
        print("  python volatility_calculator.py orderbook_data.csv BTC               # Analyze one symbol")
        print("  python volatility_calculator.py orderbook_data.csv BTC ETH SOL       # Analyze multiple symbols")
        sys.exit(1)
    
    csv_path = sys.argv[1]
    symbol_filters = sys.argv[2:] if len(sys.argv) > 2 else None  # Get all symbols after csv_path
    
    if not Path(csv_path).exists():
        raise SystemExit(f"CSV file not found: {csv_path}")
    
    # Analyze volatility
    results = analyze_csv_volatility(csv_path, symbol_filters)
    
    # Create Excel file with volatility tables
    create_excel_volatility_tables(results)


if __name__ == "__main__":
    main()