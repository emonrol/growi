#!/usr/bin/env python3
"""
Volatility Calculator for 5-minute orderbook data.

Reads CSV files with 5-minute price updates and calculates:
- Multiple time period volatilities (3, 5, 10, 30, 60, 90 minutes)
- Exports results to Excel with formatted tables
"""

import pandas as pd
import numpy as np
import sys
from pathlib import Path
from typing import Optional, Dict, Tuple
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
            'sample_size': len(log_returns)
        }
    
    # 5-minute volatility (standard deviation of log returns) - this is our base
    vol_5min = log_returns.std()
    
    # Scale to different time periods using sqrt(time_ratio)
    vol_3min = vol_5min * np.sqrt(3/5)      # 3 minutes
    vol_10min = vol_5min * np.sqrt(10/5)    # 10 minutes = sqrt(2)
    vol_30min = vol_5min * np.sqrt(30/5)    # 30 minutes = sqrt(6)
    vol_60min = vol_5min * np.sqrt(60/5)    # 60 minutes = sqrt(12)
    vol_90min = vol_5min * np.sqrt(90/5)    # 90 minutes = sqrt(18)
    
    return {
        '3min_vol': vol_3min,
        '5min_vol': vol_5min,
        '10min_vol': vol_10min,
        '30min_vol': vol_30min,
        '60min_vol': vol_60min,
        '90min_vol': vol_90min,
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
        df['ts'] = pd.to_datetime(df['ts'])
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
        
        results[symbol] = {
            'volatility_metrics': vol_metrics,
            'price_stats': {
                'min_price': prices.min(),
                'max_price': prices.max(),
                'mean_price': prices.mean(),
                'price_range_pct': ((prices.max() - prices.min()) / prices.mean()) * 100
            },
            'data_quality': {
                'total_observations': len(prices),
                'missing_values': prices.isna().sum(),
                'time_span_hours': (prices.index[-1] - prices.index[0]).total_seconds() / 3600 if hasattr(prices.index, 'total_seconds') else None
            }
        }
    
    return results


def print_volatility_report(results: Dict) -> None:
    """
    Print a formatted volatility report.
    
    Args:
        results: Results from analyze_csv_volatility()
    """
    print("\n" + "="*60)
    print("VOLATILITY ANALYSIS REPORT")
    print("="*60)
    
    for symbol, data in results.items():
        vol = data['volatility_metrics']
        
        print(f"\n📊 SYMBOL: {symbol}")
        
        print(f"\n📈 VOLATILITY METRICS:")
        print(f"   3-minute vol:  ±{vol['3min_vol']*100:.3f}% (68% confidence), ±{vol['3min_vol']*100*1.96:.3f}% (95% confidence)")
        print(f"   5-minute vol:  ±{vol['5min_vol']*100:.3f}% (68% confidence), ±{vol['5min_vol']*100*1.96:.3f}% (95% confidence)")
        print(f"   10-minute vol: ±{vol['10min_vol']*100:.3f}% (68% confidence), ±{vol['10min_vol']*100*1.96:.3f}% (95% confidence)")
        print(f"   30-minute vol: ±{vol['30min_vol']*100:.2f}% (68% confidence), ±{vol['30min_vol']*100*1.96:.2f}% (95% confidence)")
        print(f"   60-minute vol: ±{vol['60min_vol']*100:.2f}% (68% confidence), ±{vol['60min_vol']*100*1.96:.2f}% (95% confidence)")
        print(f"   90-minute vol: ±{vol['90min_vol']*100:.2f}% (68% confidence), ±{vol['90min_vol']*100*1.96:.2f}% (95% confidence)")


def create_excel_volatility_tables(results: Dict, output_filename: str = "volatility_analysis.xlsx") -> None:
    """
    Create Excel file with consolidated volatility table.
    
    Args:
        results: Results from analyze_csv_volatility()
        output_filename: Name of the Excel file to create
    """
    # Create workbook and worksheet
    workbook = openpyxl.Workbook()
    worksheet = workbook.active
    worksheet.title = "Volatility Analysis"
    
    # Define styles
    header_font = Font(bold=True, size=12, color="FFFFFF")
    header_fill = PatternFill(start_color="366092", end_color="366092", fill_type="solid")
    cell_alignment = Alignment(horizontal="center", vertical="center")
    
    # Title
    worksheet.cell(row=1, column=1, value="Volatility Analysis - Multiple Time Periods")
    worksheet.cell(row=1, column=1).font = Font(bold=True, size=14)
    
    # Headers - first column is Minutes, then each symbol
    worksheet.cell(row=3, column=1, value="Minutes")
    worksheet.cell(row=3, column=1).font = header_font
    worksheet.cell(row=3, column=1).fill = header_fill
    worksheet.cell(row=3, column=1).alignment = cell_alignment
    
    # Add symbol headers
    symbols = list(results.keys())
    for col_idx, symbol in enumerate(symbols, start=2):
        worksheet.cell(row=3, column=col_idx, value=f"{symbol} (%)")
        worksheet.cell(row=3, column=col_idx).font = header_font
        worksheet.cell(row=3, column=col_idx).fill = header_fill
        worksheet.cell(row=3, column=col_idx).alignment = cell_alignment
    
    # Time periods
    time_periods = [3, 5, 10, 30, 60, 90]
    vol_keys = ['3min_vol', '5min_vol', '10min_vol', '30min_vol', '60min_vol', '90min_vol']
    
    # Fill data rows
    for row_idx, (minutes, vol_key) in enumerate(zip(time_periods, vol_keys), start=4):
        # Minutes column
        worksheet.cell(row=row_idx, column=1, value=minutes)
        worksheet.cell(row=row_idx, column=1).alignment = cell_alignment
        worksheet.cell(row=row_idx, column=1).font = Font(bold=True)
        
        # Volatility columns for each symbol
        for col_idx, symbol in enumerate(symbols, start=2):
            volatility = results[symbol]['volatility_metrics'][vol_key]
            worksheet.cell(row=row_idx, column=col_idx, value=f"{volatility*100:.3f}%")
            worksheet.cell(row=row_idx, column=col_idx).alignment = cell_alignment
    
    # Adjust column widths
    worksheet.column_dimensions['A'].width = 10
    for col_idx in range(2, len(symbols) + 2):
        col_letter = get_column_letter(col_idx)
        worksheet.column_dimensions[col_letter].width = 12
    
    # Save the workbook
    workbook.save(output_filename)
    print(f"\n📊 Excel file saved: {output_filename}")


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
    
    # Print report
    print_volatility_report(results)
    
    # Create Excel file with volatility tables
    create_excel_volatility_tables(results)


if __name__ == "__main__":
    main()