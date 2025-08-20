#!/usr/bin/env python3
"""
Volatility Calculator for 5-minute orderbook data.

Reads CSV files with 5-minute price updates and calculates:
- 5-minute volatility (from log returns)
- Daily volatility (scaled)
- Annual volatility (scaled)
"""

import pandas as pd
import numpy as np
import sys
from pathlib import Path
from typing import Optional, Dict, Tuple
from orderbook_utils import validate_required_columns, REQUIRED_CSV_COLUMNS


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
    Calculate various volatility metrics from log returns.
    
    Args:
        log_returns: Series of logarithmic returns
        
    Returns:
        Dictionary with volatility metrics
    """
    if len(log_returns) < 2:
        return {
            '5min_vol': 0.0,
            'daily_vol': 0.0,
            'annual_vol': 0.0,
            'sample_size': len(log_returns)
        }
    
    # 5-minute volatility (standard deviation of log returns)
    vol_5min = log_returns.std()
    
    # Scale to daily volatility
    # There are 288 five-minute periods in a day (24*60/5 = 288)
    vol_daily = vol_5min * np.sqrt(288)
    
    # Scale to annual volatility 
    # Method 1: From 5-min to annual directly
    # vol_annual = vol_5min * np.sqrt(288 * 365)
    
    # Method 2: From daily to annual (more common)
    vol_annual = vol_daily * np.sqrt(365)
    
    return {
        '5min_vol': vol_5min,
        'daily_vol': vol_daily, 
        'annual_vol': vol_annual,
        'sample_size': len(log_returns),
        'mean_return': log_returns.mean(),
        'skewness': log_returns.skew(),
        'kurtosis': log_returns.kurtosis()
    }


def calculate_rolling_volatility(prices: pd.Series, window_hours: int = 24) -> pd.DataFrame:
    """
    Calculate rolling volatility over time.
    
    Args:
        prices: Series of prices with datetime index
        window_hours: Rolling window size in hours (default 24 = 1 day)
        
    Returns:
        DataFrame with rolling volatility metrics
    """
    # Calculate window size in 5-minute periods
    window_periods = window_hours * 12  # 12 five-minute periods per hour
    
    # Calculate log returns (this will have one less element than prices)
    log_returns = calculate_log_returns(prices)
    
    # Rolling standard deviation (5-min volatility)
    rolling_vol_5min = log_returns.rolling(window=window_periods).std()
    
    # Scale to daily
    rolling_vol_daily = rolling_vol_5min * np.sqrt(288)
    
    # Create aligned series by reindexing to match the original prices index
    # This automatically handles the length differences
    log_returns_aligned = log_returns.reindex(prices.index)
    rolling_vol_5min_aligned = rolling_vol_5min.reindex(prices.index)
    rolling_vol_daily_aligned = rolling_vol_daily.reindex(prices.index)
    
    # Create result DataFrame - now all series have the same length
    result = pd.DataFrame({
        'price': prices,
        'log_return': log_returns_aligned,
        'vol_5min_rolling': rolling_vol_5min_aligned,
        'vol_daily_rolling': rolling_vol_daily_aligned
    })
    
    return result


def analyze_csv_volatility(csv_path: str, symbol_filter: Optional[str] = None) -> Dict:
    """
    Main function to analyze volatility from CSV file.
    
    Args:
        csv_path: Path to CSV file
        symbol_filter: Optional symbol to filter (e.g., 'BTCUSDT')
        
    Returns:
        Dictionary with analysis results
    """
    print(f"Reading CSV: {csv_path}")
    
    try:
        df = pd.read_csv(csv_path)
    except Exception as e:
        raise SystemExit(f"Error reading CSV: {e}")
    
    # Validate required columns
    validate_required_columns(df.columns.tolist(), REQUIRED_CSV_COLUMNS)
    
    print(f"Loaded {len(df)} rows")
    print(f"Columns: {list(df.columns)}")
    print(f"Symbols in data: {df['symbol'].unique()}")
    
    # Filter by symbol if specified
    if symbol_filter:
        df = df[df['symbol'] == symbol_filter]
        if len(df) == 0:
            raise SystemExit(f"No data found for symbol: {symbol_filter}")
        print(f"Filtered to {len(df)} rows for symbol: {symbol_filter}")
    
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
        
        # Calculate rolling volatility (24-hour window)
        rolling_data = calculate_rolling_volatility(prices, window_hours=24)
        
        results[symbol] = {
            'volatility_metrics': vol_metrics,
            'rolling_data': rolling_data,
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
        price_stats = data['price_stats']
        quality = data['data_quality']
        
        print(f"\n📊 SYMBOL: {symbol}")
        print(f"   Sample size: {vol['sample_size']:,} observations")
        print(f"   Time span: {quality['time_span_hours']:.1f} hours" if quality['time_span_hours'] else "")
        
        print(f"\n💰 PRICE STATISTICS:")
        print(f"   Range: ${price_stats['min_price']:,.2f} - ${price_stats['max_price']:,.2f}")
        print(f"   Average: ${price_stats['mean_price']:,.2f}")
        print(f"   Total range: {price_stats['price_range_pct']:.1f}%")
        
        print(f"\n📈 VOLATILITY METRICS:")
        print(f"   5-minute vol:  {vol['5min_vol']*100:.3f}%")
        print(f"   Daily vol:     {vol['daily_vol']*100:.2f}%")
        print(f"   Annual vol:    {vol['annual_vol']*100:.1f}%")
        
        print(f"\n📊 RETURN STATISTICS:")
        print(f"   Mean return:   {vol['mean_return']*100:.4f}% (per 5min)")
        print(f"   Skewness:      {vol['skewness']:.3f}")
        print(f"   Kurtosis:      {vol['kurtosis']:.3f}")
        
        # Volatility interpretation
        daily_vol_pct = vol['daily_vol'] * 100
        if daily_vol_pct < 2:
            vol_level = "LOW"
        elif daily_vol_pct < 5:
            vol_level = "MODERATE" 
        elif daily_vol_pct < 10:
            vol_level = "HIGH"
        else:
            vol_level = "EXTREME"
            
        print(f"\n🎯 INTERPRETATION:")
        print(f"   Volatility level: {vol_level}")
        print(f"   Expected daily move: ±{daily_vol_pct:.2f}% (68% confidence)")
        print(f"   Expected daily move: ±{daily_vol_pct*1.96:.2f}% (95% confidence)")


def main():
    """Main execution function."""
    if len(sys.argv) < 2:
        print("Usage: python volatility_calculator.py <csv_file> [symbol]")
        print("Example: python volatility_calculator.py orderbook_data.csv BTCUSDT")
        sys.exit(1)
    
    csv_path = sys.argv[1]
    symbol_filter = sys.argv[2] if len(sys.argv) > 2 else None
    
    if not Path(csv_path).exists():
        raise SystemExit(f"CSV file not found: {csv_path}")
    
    # Analyze volatility
    results = analyze_csv_volatility(csv_path, symbol_filter)
    
    # Print report
    print_volatility_report(results)
    
    # Optional: Save detailed results to files
    for symbol, data in results.items():
        # Save rolling volatility data
        output_file = f"volatility_analysis_{symbol}.csv"
        data['rolling_data'].to_csv(output_file, index=True)
        print(f"\n💾 Detailed data saved to: {output_file}")


if __name__ == "__main__":
    main()