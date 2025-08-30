import sys
import argparse
from pathlib import Path
from typing import Optional, Dict, Tuple, List
import numpy as np
import pandas as pd
import openpyxl
from openpyxl.styles import Font, PatternFill, Alignment, NamedStyle
from openpyxl.utils import get_column_letter
from openpyxl.drawing.image import Image
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
sys.path.append('/home/eric/Documents/growi')
from orderbook_utils import parse_orderbook_blob, validate_required_columns, REQUIRED_CSV_COLUMNS, _normalize_number
from API import get_leverage_info_from_file, LEVERAGE_DATA_FILE
DEFAULT_PERCENTILES = [1, 10, 25, 50]
EXCEL_FILENAME = 'volatility_analysis.xlsx'
CHART_FILENAME = 'leverage_vs_usd_chart.png'
PLOTS_DIR = Path("plots")
PLOTS_DIR.mkdir(exist_ok=True)


def calculate_log_returns(prices: pd.Series) -> pd.Series:
    prices = prices[prices > 0]
    log_returns = np.log(prices / prices.shift(1))
    return log_returns.dropna()

def calculate_volatility_metrics(log_returns: pd.Series) -> Dict[str, float]:
    if len(log_returns) < 2:
        return {'3min_vol': 0.0, '5min_vol': 0.0, '10min_vol': 0.0, '30min_vol': 0.0, '60min_vol': 0.0, '90min_vol': 0.0, '1day_vol': 0.0, '1year_vol': 0.0, 'sample_size': len(log_returns), 'mean_return': 0.0}
    vol_5min = log_returns.std()
    return {'3min_vol': vol_5min * np.sqrt(3 / 5), '5min_vol': vol_5min, '10min_vol': vol_5min * np.sqrt(10 / 5), '30min_vol': vol_5min * np.sqrt(30 / 5), '60min_vol': vol_5min * np.sqrt(60 / 5), '90min_vol': vol_5min * np.sqrt(90 / 5), '1day_vol': vol_5min * np.sqrt(1440 / 5), '1year_vol': vol_5min * np.sqrt(525600 / 5), 'sample_size': len(log_returns), 'mean_return': log_returns.mean()}

def fetch_leverage_info(symbol: str) -> Dict:
    return get_leverage_info_from_file(symbol, LEVERAGE_DATA_FILE)

def get_effective_leverage_for_usd_amount(usd_amount: float, leverage_tiers: List[Tuple[float, float, int]]) -> int:
    if not leverage_tiers:
        return 0
    for lower_bound, upper_bound, leverage in leverage_tiers:
        if lower_bound <= usd_amount < upper_bound:
            return leverage
    return leverage_tiers[-1][2] if leverage_tiers else 0

def extract_median_usd_from_p50(results: Dict, percentiles: List[int], orderbook_level: int=1) -> Dict[str, Dict]:
    if 50 not in percentiles:
        raise ValueError('Percentile 50 must be included to extract median USD data')
    percentile_dfs = build_percentile_depth(results, percentiles)
    p50_df = percentile_dfs[50]
    leverage_usd_data = {}
    for symbol in results.keys():
        leverage_info = results[symbol]['leverage_info']
        leverage_tiers = leverage_info.get('tiers', [])
        bid_data = p50_df[(p50_df['symbol'] == symbol) & (p50_df['side'] == 'bid') & (p50_df['level'] == orderbook_level)]
        ask_data = p50_df[(p50_df['symbol'] == symbol) & (p50_df['side'] == 'ask') & (p50_df['level'] == orderbook_level)]
        bid_usd = bid_data['usd'].iloc[0] if len(bid_data) > 0 and (not pd.isna(bid_data['usd'].iloc[0])) else 0.0
        ask_usd = ask_data['usd'].iloc[0] if len(ask_data) > 0 and (not pd.isna(ask_data['usd'].iloc[0])) else 0.0
        avg_usd = (bid_usd + ask_usd) / 2 if bid_usd > 0 or ask_usd > 0 else 0.0
        effective_lev_bid = get_effective_leverage_for_usd_amount(bid_usd, leverage_tiers)
        effective_lev_ask = get_effective_leverage_for_usd_amount(ask_usd, leverage_tiers)
        effective_lev_avg = get_effective_leverage_for_usd_amount(avg_usd, leverage_tiers)
        leverage_usd_data[symbol] = {'bid_usd': bid_usd, 'ask_usd': ask_usd, 'avg_usd': avg_usd, 'effective_leverage_bid': effective_lev_bid, 'effective_leverage_ask': effective_lev_ask, 'effective_leverage_avg': effective_lev_avg}
    return leverage_usd_data

def create_leverage_vs_usd_chart(leverage_usd_data: Dict, output_filename: str=CHART_FILENAME, chart_type: str='avg') -> None:
    chart_data = []
    for symbol, data in leverage_usd_data.items():
        if chart_type == 'bid':
            leverage = data['effective_leverage_bid']
            usd_amount = data['bid_usd']
        elif chart_type == 'ask':
            leverage = data['effective_leverage_ask']
            usd_amount = data['ask_usd']
        else:
            leverage = data['effective_leverage_avg']
            usd_amount = data['avg_usd']
        if leverage > 0 and usd_amount > 0:
            chart_data.append({'symbol': symbol, 'leverage': leverage, 'usd_amount': usd_amount})
    if not chart_data:
        return
    df = pd.DataFrame(chart_data)
    plt.figure(figsize=(12, 8))
    leverage_groups = df.groupby('leverage')
    colors = plt.cm.Set3(np.linspace(0, 1, len(df)))
    bar_width = 0.8
    positions = []
    labels = []
    values = []
    symbol_labels = []
    for i, (leverage, group) in enumerate(leverage_groups):
        symbols = group['symbol'].tolist()
        usd_amounts = group['usd_amount'].tolist()
        if len(symbols) == 1:
            positions.append(leverage)
            values.append(usd_amounts[0])
            symbol_labels.append(symbols[0])
        else:
            n_bars = len(symbols)
            sub_width = bar_width / n_bars
            for j, (symbol, usd_amount) in enumerate(zip(symbols, usd_amounts)):
                pos = leverage + (j - (n_bars - 1) / 2) * sub_width * 0.3
                positions.append(pos)
                values.append(usd_amount)
                symbol_labels.append(symbol)
    max_symbols_per_leverage = df.groupby('leverage').size().max() if len(df) > 0 else 1
    adjusted_bar_width = bar_width / max(1, max_symbols_per_leverage)
    bars = plt.bar(positions, values, width=adjusted_bar_width, color=colors[:len(positions)], alpha=0.8, edgecolor='black', linewidth=0.5)
    for bar, symbol in zip(bars, symbol_labels):
        height = bar.get_height()
        plt.text(bar.get_x() + bar.get_width() / 2.0, height + height * 0.01, symbol, ha='center', va='bottom', fontweight='bold', fontsize=10)
    plt.xlabel('Effective Leverage (x)', fontsize=12, fontweight='bold')
    plt.ylabel('Median USD Quantity (p50)', fontsize=12, fontweight='bold')
    chart_type_label = chart_type.upper() if chart_type != 'avg' else 'AVERAGE'
    plt.title(f'Effective Leverage vs Median USD Quantity ({chart_type_label})\nOrderbook Level 1 - Percentile 50', fontsize=14, fontweight='bold', pad=20)
    ax = plt.gca()
    ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'${x:,.0f}' if x >= 1000 else f'${x:.0f}'))
    leverage_values = sorted(df['leverage'].unique())
    plt.xticks(leverage_values, [f'{int(lev)}x' for lev in leverage_values])
    plt.grid(True, alpha=0.3, axis='y')
    plt.tight_layout()
    plt.savefig(output_filename, dpi=300, bbox_inches='tight')
    plt.close()
    for leverage in sorted(leverage_values):
        symbols_at_lev = df[df['leverage'] == leverage]['symbol'].tolist()
        amounts_at_lev = df[df['leverage'] == leverage]['usd_amount'].tolist()

def extract_time_series_levels(results: Dict, max_levels: int=5) -> Dict[str, Dict]:
    time_series_data = {}
    for symbol, blob in results.items():
        sdf = blob['df'].copy()
        if 'ts' in sdf.columns:
            sdf = sdf.sort_values('ts')
        data = {'timestamps': [], 'bid_levels': {i: [] for i in range(1, max_levels + 1)}, 'ask_levels': {i: [] for i in range(1, max_levels + 1)}, 'base_prices': []}
        for _, row in sdf.iterrows():
            if 'ts' in row and (not pd.isna(row['ts'])):
                data['timestamps'].append(row['ts'])
            else:
                data['timestamps'].append(pd.Timestamp.now() + pd.Timedelta(seconds=len(data['timestamps']) * 300))
            base_price = _normalize_number(row['base_price']) if not pd.isna(row['base_price']) else np.nan
            data['base_prices'].append(base_price)
            for level in range(1, max_levels + 1):
                data['bid_levels'][level].append(np.nan)
                data['ask_levels'][level].append(np.nan)
            if 'bids' in row and (not pd.isna(row['bids'])):
                try:
                    bid_levels = parse_orderbook_blob(row['bids'])
                    for i, level in enumerate(bid_levels[:max_levels], start=1):
                        px = float(level['px'])
                        data['bid_levels'][i][-1] = px
                except Exception as e:
                    pass
            if 'asks' in row and (not pd.isna(row['asks'])):
                try:
                    ask_levels = parse_orderbook_blob(row['asks'])
                    for i, level in enumerate(ask_levels[:max_levels], start=1):
                        px = float(level['px'])
                        data['ask_levels'][i][-1] = px
                except Exception as e:
                    pass
        time_series_data[symbol] = data
    return time_series_data

def create_token_levels_chart(time_series_data: Dict[str, Dict], symbol: str, output_filename: Optional[str]=None, max_levels: int=5) -> None:
    if symbol not in time_series_data:
        return
    if not output_filename:
        output_filename = PLOTS_DIR / f'token_levels_over_time_{symbol}.png'
    data = time_series_data[symbol]
    timestamps = data['timestamps']
    if not timestamps:
        return
    plt.figure(figsize=(15, 10))
    timestamps = pd.to_datetime(timestamps)
    base_prices = data['base_prices']
    plt.plot(timestamps, base_prices, label='Base Price', color='black', linewidth=2, alpha=0.8)
    bid_colors = plt.cm.Blues(np.linspace(0.4, 1, max_levels))
    ask_colors = plt.cm.Reds(np.linspace(0.4, 1, max_levels))
    for level in range(1, max_levels + 1):
        bid_prices = data['bid_levels'][level]
        if any((not np.isnan(p) for p in bid_prices)):
            bid_prices_clean = [p if not np.isnan(p) else None for p in bid_prices]
            plt.plot(timestamps, bid_prices_clean, label=f'Bid L{level}', color=bid_colors[level - 1], alpha=0.7, linewidth=1.5, linestyle='-')
    for level in range(1, max_levels + 1):
        ask_prices = data['ask_levels'][level]
        if any((not np.isnan(p) for p in ask_prices)):
            ask_prices_clean = [p if not np.isnan(p) else None for p in ask_prices]
            plt.plot(timestamps, ask_prices_clean, label=f'Ask L{level}', color=ask_colors[level - 1], alpha=0.7, linewidth=1.5, linestyle='--')
    plt.xlabel('Time', fontsize=12, fontweight='bold')
    plt.ylabel('Price (Token Units)', fontsize=12, fontweight='bold')
    plt.title(f'{symbol} - Price Levels Over Time (Token Units)', fontsize=14, fontweight='bold')
    plt.legend(bbox_to_anchor=(1.05, 1), loc='upper left')
    plt.grid(True, alpha=0.3)
    ax = plt.gca()
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%m/%d %H:%M'))
    ax.xaxis.set_major_locator(mdates.HourLocator(interval=6))
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig(output_filename, dpi=300, bbox_inches='tight')
    plt.close()

def create_usd_levels_chart(time_series_data: Dict[str, Dict], symbol: str, output_filename: Optional[str]=None, max_levels: int=5) -> None:
    if symbol not in time_series_data:
        return
    if not output_filename:
        output_filename = PLOTS_DIR / f'usd_levels_over_time_{symbol}.png'
    data = time_series_data[symbol]
    timestamps = data['timestamps']
    if not timestamps:
        return
    plt.figure(figsize=(15, 10))
    timestamps = pd.to_datetime(timestamps)
    base_prices = data['base_prices']
    plt.plot(timestamps, base_prices, label='Base Price', color='black', linewidth=2, alpha=0.8)
    bid_colors = plt.cm.Blues(np.linspace(0.4, 1, max_levels))
    ask_colors = plt.cm.Reds(np.linspace(0.4, 1, max_levels))
    for level in range(1, max_levels + 1):
        bid_prices = data['bid_levels'][level]
        if any((not np.isnan(p) for p in bid_prices)):
            bid_prices_clean = [p if not np.isnan(p) else None for p in bid_prices]
            plt.plot(timestamps, bid_prices_clean, label=f'Bid L{level}', color=bid_colors[level - 1], alpha=0.7, linewidth=1.5, linestyle='-')
    for level in range(1, max_levels + 1):
        ask_prices = data['ask_levels'][level]
        if any((not np.isnan(p) for p in ask_prices)):
            ask_prices_clean = [p if not np.isnan(p) else None for p in ask_prices]
            plt.plot(timestamps, ask_prices_clean, label=f'Ask L{level}', color=ask_colors[level - 1], alpha=0.7, linewidth=1.5, linestyle='--')
    plt.xlabel('Time', fontsize=12, fontweight='bold')
    plt.ylabel('Price (USD)', fontsize=12, fontweight='bold')
    plt.title(f'{symbol} - Price Levels Over Time (USD)', fontsize=14, fontweight='bold')
    plt.legend(bbox_to_anchor=(1.05, 1), loc='upper left')
    plt.grid(True, alpha=0.3)
    ax = plt.gca()
    ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'${x:,.2f}'))
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%m/%d %H:%M'))
    ax.xaxis.set_major_locator(mdates.HourLocator(interval=6))
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig(output_filename, dpi=300, bbox_inches='tight')
    plt.close()

def analyze_csv(csv_path: str, symbol_filters: Optional[list]=None, sep: str=',') -> Dict:
    try:
        df = pd.read_csv(csv_path, sep=sep, engine='python')
    except Exception as e:
        raise SystemExit(f'Error reading CSV: {e}')
    validate_required_columns(df.columns.tolist(), set(REQUIRED_CSV_COLUMNS))
    if symbol_filters:
        df = df[df['symbol'].isin(symbol_filters)]
        if len(df) == 0:
            raise SystemExit(f'No data found for symbols: {symbol_filters}')
        found_symbols = df['symbol'].unique()
        missing_symbols = set(symbol_filters) - set(found_symbols)
    if 'ts' in df.columns:
        df['ts'] = pd.to_datetime(df['ts'], format='mixed')
        df = df.sort_values('ts')
    results = {}
    for symbol in df['symbol'].unique():
        sdf = df[df['symbol'] == symbol].copy()
        prices = sdf['base_price'].apply(_normalize_number)
        prices = pd.Series(prices, dtype=float)
        if 'ts' in sdf.columns:
            prices.index = sdf['ts']
        log_returns = calculate_log_returns(prices)
        vol_metrics = calculate_volatility_metrics(log_returns)
        leverage_info = fetch_leverage_info(symbol)
        if leverage_info['status'] == 'error':
            print("error fetching leverage info for", symbol, leverage_info['error'])
        results[symbol] = {'df': sdf, 'volatility_metrics': vol_metrics, 'leverage_info': leverage_info, 'price_stats': {'min_price': prices.min(), 'max_price': prices.max(), 'mean_price': prices.mean(), 'latest_price': prices.iloc[-1], 'price_range_pct': (prices.max() - prices.min()) / prices.mean() * 100 if prices.mean() else 0.0}, 'data_quality': {'total_observations': len(prices), 'missing_values': prices.isna().sum(), 'time_span_hours': float((prices.index[-1] - prices.index[0]).total_seconds() / 3600.0) if len(prices) > 1 else 0.0}}
    return results

def _collect_levels_for_symbol(symbol_df: pd.DataFrame, side: str) -> Dict[int, Dict[str, list]]:
    out = {}
    for _, row in symbol_df.iterrows():
        base_price = _normalize_number(row['base_price'])
        if side not in row or pd.isna(row[side]):
            continue
        try:
            levels = parse_orderbook_blob(row[side])
        except Exception:
            continue
        for i, lvl in enumerate(levels, start=1):
            try:
                px, sz = (float(lvl['px']), float(lvl['sz']))
            except Exception:
                continue
            spread_distance = abs((px / base_price - 1.0) * 100.0)
            usd = px * sz
            bucket = out.setdefault(i, {'spread_distance': [], 'qty': [], 'usd': []})
            bucket['spread_distance'].append(spread_distance)
            bucket['qty'].append(sz)
            bucket['usd'].append(usd)
    return out

def _percentile(series: List[float], q: float) -> float:
    return float(pd.Series(series).quantile(q)) if series else float('nan')

def _mean(series: List[float]) -> float:
    return float(pd.Series(series).mean()) if series else float('nan')

def build_percentile_depth(results: Dict, percentiles: List[int]) -> Dict[int, pd.DataFrame]:
    percentile_dfs = {}
    for perc in percentiles:
        q = (100 - perc) / 100.0  # Fix percentile interpretation
        rows = []
        for symbol, blob in results.items():
            sdf = blob['df']
            for side in ('bids', 'asks'):
                buckets = _collect_levels_for_symbol(sdf, side)
                acc_qty = acc_usd = 0.0
                for lvl in sorted(buckets.keys()):
                    b = buckets[lvl]
                    spread_p, qty_p, usd_p = (_percentile(b['spread_distance'], q), _percentile(b['qty'], q), _percentile(b['usd'], q))
                    if not np.isnan(qty_p):
                        acc_qty += qty_p
                    if not np.isnan(usd_p):
                        acc_usd += usd_p
                    rows.append({'symbol': symbol, 'side': 'bid' if side == 'bids' else 'ask', 'level': lvl, 'spread_distance': round(spread_p, 3) if not np.isnan(spread_p) else np.nan, 'qty': round(qty_p, 6) if not np.isnan(qty_p) else np.nan, 'usd': round(usd_p, 2) if not np.isnan(usd_p) else np.nan, 'acc_qty': round(acc_qty, 6) if not np.isnan(acc_qty) else np.nan, 'acc_usd': round(acc_usd, 2) if not np.isnan(acc_usd) else np.nan, 'obs_count': len(b['qty'])})
        df = pd.DataFrame(rows)
        if not df.empty:
            df = df.sort_values(['symbol', 'side', 'level']).reset_index(drop=True)
        percentile_dfs[perc] = df
    return percentile_dfs

def build_mean_depth(results: Dict) -> pd.DataFrame:
    rows = []
    for symbol, blob in results.items():
        sdf = blob['df']
        for side in ('bids', 'asks'):
            buckets = _collect_levels_for_symbol(sdf, side)
            acc_qty = acc_usd = 0.0
            for lvl in sorted(buckets.keys()):
                b = buckets[lvl]
                spread_m, qty_m, usd_m = (_mean(b['spread_distance']), _mean(b['qty']), _mean(b['usd']))
                if not np.isnan(qty_m):
                    acc_qty += qty_m
                if not np.isnan(usd_m):
                    acc_usd += usd_m
                rows.append({'symbol': symbol, 'side': 'bid' if side == 'bids' else 'ask', 'level': lvl, 'spread_distance': round(spread_m, 3) if not np.isnan(spread_m) else np.nan, 'qty': round(qty_m, 6) if not np.isnan(qty_m) else np.nan, 'usd': round(usd_m, 2) if not np.isnan(usd_m) else np.nan, 'acc_qty': round(acc_qty, 6) if not np.isnan(acc_qty) else np.nan, 'acc_usd': round(acc_usd, 2) if not np.isnan(acc_usd) else np.nan, 'obs_count': len(b['qty'])})
    df = pd.DataFrame(rows)
    if not df.empty:
        df = df.sort_values(['symbol', 'side', 'level']).reset_index(drop=True)
    return df

def calculate_volume_by_spread_comparison(results: Dict, percentiles: List[int], percentile_dfs: Dict, mean_df: pd.DataFrame) -> Dict:
    """
    Calculate investment volumes by comparing volatility % with spread distances %
    Returns dict with structure: {percentile: {symbol: {vol_key: volume}}}
    """
    volume_data = {}
    symbols = list(results.keys())
    vol_keys = ['3min_vol', '5min_vol', '10min_vol', '30min_vol', '60min_vol', '90min_vol', '1day_vol', '1year_vol']
    
    # Process percentile data
    for perc in percentiles:
        volume_data[f'p{perc}'] = {}
        spread_df = percentile_dfs[perc]
        usd_df = percentile_dfs[perc]  # Use same percentile for USD values
        
        for symbol in symbols:
            volume_data[f'p{perc}'][symbol] = {}
            
            for vol_key in vol_keys:
                volatility_pct = results[symbol]['volatility_metrics'][vol_key] * 100.0  # Convert to percentage
                total_volume = 0.0
                
                # Get spread and USD data for this symbol, sorted by level
                symbol_spread = spread_df[spread_df['symbol'] == symbol].copy()
                symbol_usd = usd_df[usd_df['symbol'] == symbol].copy()
                
                if symbol_spread.empty or symbol_usd.empty:
                    volume_data[f'p{perc}'][symbol][vol_key] = 0.0
                    continue
                
                # Get max level for this symbol
                max_level = symbol_spread['level'].max()
                prev_level_spread = 0.0
                prev_level_volume = 0.0
                
                for level in range(1, max_level + 1):
                    # Get spread distance (mean of ask/bid)
                    ask_spread = symbol_spread[(symbol_spread['side'] == 'ask') & (symbol_spread['level'] == level)]
                    bid_spread = symbol_spread[(symbol_spread['side'] == 'bid') & (symbol_spread['level'] == level)]
                    
                    ask_spread_val = ask_spread['spread_distance'].iloc[0] if len(ask_spread) > 0 and not pd.isna(ask_spread['spread_distance'].iloc[0]) else np.nan
                    bid_spread_val = bid_spread['spread_distance'].iloc[0] if len(bid_spread) > 0 and not pd.isna(bid_spread['spread_distance'].iloc[0]) else np.nan
                    
                    if not np.isnan(ask_spread_val) and not np.isnan(bid_spread_val):
                        level_spread = (ask_spread_val + bid_spread_val) / 2
                    elif not np.isnan(ask_spread_val):
                        level_spread = ask_spread_val
                    elif not np.isnan(bid_spread_val):
                        level_spread = bid_spread_val
                    else:
                        continue
                    
                    # Get USD volume (mean of ask/bid)
                    ask_usd = symbol_usd[(symbol_usd['side'] == 'ask') & (symbol_usd['level'] == level)]
                    bid_usd = symbol_usd[(symbol_usd['side'] == 'bid') & (symbol_usd['level'] == level)]
                    
                    ask_usd_val = ask_usd['acc_usd'].iloc[0] if len(ask_usd) > 0 and not pd.isna(ask_usd['acc_usd'].iloc[0]) else np.nan
                    bid_usd_val = bid_usd['acc_usd'].iloc[0] if len(bid_usd) > 0 and not pd.isna(bid_usd['acc_usd'].iloc[0]) else np.nan
                    
                    if not np.isnan(ask_usd_val) and not np.isnan(bid_usd_val):
                        level_volume = (ask_usd_val + bid_usd_val) / 2
                    elif not np.isnan(ask_usd_val):
                        level_volume = ask_usd_val
                    elif not np.isnan(bid_usd_val):
                        level_volume = bid_usd_val
                    else:
                        continue
                    
                    # Compare volatility with spread
                    if volatility_pct >= level_spread:
                        # Add this level's volume
                        total_volume += level_volume
                    else:
                        # Interpolate between previous level and current level
                        if level > 1 and prev_level_spread < volatility_pct < level_spread:
                            spread_range = level_spread - prev_level_spread
                            vol_offset = volatility_pct - prev_level_spread
                            interpolation_factor = vol_offset / spread_range if spread_range > 0 else 0
                            interpolated_volume = prev_level_volume + (level_volume - prev_level_volume) * interpolation_factor
                            total_volume += interpolated_volume
                        break
                    
                    prev_level_spread = level_spread
                    prev_level_volume = level_volume
                
                volume_data[f'p{perc}'][symbol][vol_key] = total_volume
    
    # Process mean data
    volume_data['mean'] = {}
    for symbol in symbols:
        volume_data['mean'][symbol] = {}
        
        for vol_key in vol_keys:
            volatility_pct = results[symbol]['volatility_metrics'][vol_key] * 100.0
            total_volume = 0.0
            
            symbol_spread = mean_df[mean_df['symbol'] == symbol].copy()
            symbol_usd = mean_df[mean_df['symbol'] == symbol].copy()
            
            if symbol_spread.empty or symbol_usd.empty:
                volume_data['mean'][symbol][vol_key] = 0.0
                continue
            
            max_level = symbol_spread['level'].max()
            prev_level_spread = 0.0
            prev_level_volume = 0.0
            
            for level in range(1, max_level + 1):
                ask_spread = symbol_spread[(symbol_spread['side'] == 'ask') & (symbol_spread['level'] == level)]
                bid_spread = symbol_spread[(symbol_spread['side'] == 'bid') & (symbol_spread['level'] == level)]
                
                ask_spread_val = ask_spread['spread_distance'].iloc[0] if len(ask_spread) > 0 and not pd.isna(ask_spread['spread_distance'].iloc[0]) else np.nan
                bid_spread_val = bid_spread['spread_distance'].iloc[0] if len(bid_spread) > 0 and not pd.isna(bid_spread['spread_distance'].iloc[0]) else np.nan
                
                if not np.isnan(ask_spread_val) and not np.isnan(bid_spread_val):
                    level_spread = (ask_spread_val + bid_spread_val) / 2
                elif not np.isnan(ask_spread_val):
                    level_spread = ask_spread_val
                elif not np.isnan(bid_spread_val):
                    level_spread = bid_spread_val
                else:
                    continue
                
                ask_usd = symbol_usd[(symbol_usd['side'] == 'ask') & (symbol_usd['level'] == level)]
                bid_usd = symbol_usd[(symbol_usd['side'] == 'bid') & (symbol_usd['level'] == level)]
                
                ask_usd_val = ask_usd['acc_usd'].iloc[0] if len(ask_usd) > 0 and not pd.isna(ask_usd['acc_usd'].iloc[0]) else np.nan
                bid_usd_val = bid_usd['acc_usd'].iloc[0] if len(bid_usd) > 0 and not pd.isna(bid_usd['acc_usd'].iloc[0]) else np.nan
                
                if not np.isnan(ask_usd_val) and not np.isnan(bid_usd_val):
                    level_volume = (ask_usd_val + bid_usd_val) / 2
                elif not np.isnan(ask_usd_val):
                    level_volume = ask_usd_val
                elif not np.isnan(bid_usd_val):
                    level_volume = bid_usd_val
                else:
                    continue
                
                if volatility_pct >= level_spread:
                    total_volume += level_volume
                else:
                    if level > 1 and prev_level_spread < volatility_pct < level_spread:
                        spread_range = level_spread - prev_level_spread
                        vol_offset = volatility_pct - prev_level_spread
                        interpolation_factor = vol_offset / spread_range if spread_range > 0 else 0
                        interpolated_volume = prev_level_volume + (level_volume - prev_level_volume) * interpolation_factor
                        total_volume += interpolated_volume
                    break
                
                prev_level_spread = level_spread
                prev_level_volume = level_volume
            
            volume_data['mean'][symbol][vol_key] = total_volume
    
    return volume_data

def get_date_range_info(results: Dict) -> Dict:
    all_timestamps = []
    for symbol, data in results.items():
        df = data['df']
        if 'ts' in df.columns:
            timestamps = df['ts'].dropna()
            all_timestamps.extend(timestamps.tolist())
    
    if not all_timestamps:
        return {'initial_date': None, 'last_date': None, 'time_span_hours': 0.0, 'num_days': 0.0}
    
    all_timestamps = pd.to_datetime(all_timestamps)
    initial_date = all_timestamps.min()
    last_date = all_timestamps.max()
    time_span_hours = (last_date - initial_date).total_seconds() / 3600.0
    num_days = time_span_hours / 24.0
    
    return {
        'initial_date': initial_date,
        'last_date': last_date, 
        'time_span_hours': time_span_hours,
        'num_days': num_days
    }

def create_orderbook_depth_plots(results: Dict, percentiles: List[int]) -> List[str]:
    percentile_dfs = build_percentile_depth(results, percentiles)
    mean_df = build_mean_depth(results)
    symbols = list(results.keys())
    max_levels = 5
    colors = plt.cm.Set1(np.linspace(0, 1, len(symbols)))
    plot_files = []
    for perc in sorted(percentiles):
        filename = PLOTS_DIR / f'orderbook_depth_p{perc:02d}.png'
        plot_files.append(filename)
        plt.figure(figsize=(12, 8))
        pct_df = percentile_dfs[perc]
        for i, symbol in enumerate(symbols):
            color = colors[i]
            ask_data = pct_df[(pct_df['symbol'] == symbol) & (pct_df['side'] == 'ask')].sort_values('level')
            bid_data = pct_df[(pct_df['symbol'] == symbol) & (pct_df['side'] == 'bid')].sort_values('level')
            ask_levels = []
            ask_values = []
            for level in range(1, max_levels + 1):
                level_data = ask_data[ask_data['level'] == level]
                if len(level_data) > 0:
                    ask_levels.append(level)
                    value = level_data['acc_usd'].iloc[0]
                    ask_values.append(max(value, 1) if not pd.isna(value) else 1)
            bid_levels = []
            bid_values = []
            for level in range(1, max_levels + 1):
                level_data = bid_data[bid_data['level'] == level]
                if len(level_data) > 0:
                    bid_levels.append(level)
                    value = level_data['acc_usd'].iloc[0]
                    bid_values.append(max(value, 1) if not pd.isna(value) else 1)
            if ask_levels and ask_values:
                plt.plot(ask_levels, ask_values, color=color, linewidth=2, linestyle='-', label=f'{symbol} Ask', marker='o', markersize=6)
            if bid_levels and bid_values:
                plt.plot(bid_levels, bid_values, color=color, linewidth=2, linestyle='--', label=f'{symbol} Bid', marker='s', markersize=6)
        plt.yscale('log')
        plt.xlabel('Orderbook Level', fontsize=12, fontweight='bold')
        plt.ylabel('Accumulated USD Value (Log Scale)', fontsize=12, fontweight='bold')
        plt.title(f'Orderbook Depth - Percentile {perc}% (Accumulated USD)', fontsize=14, fontweight='bold')
        plt.grid(True, alpha=0.3, which='both')
        plt.xticks(range(1, max_levels + 1), [f'N{i}' for i in range(1, max_levels + 1)])
        ax = plt.gca()
        ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'${x:,.0f}' if x >= 1000 else f'${x:.0f}'))
        plt.tight_layout()
        plt.savefig(filename, dpi=300, bbox_inches='tight')
        plt.close()
    filename = PLOTS_DIR / 'orderbook_depth_mean.png'
    plot_files.append(filename)
    plt.figure(figsize=(12, 8))
    for i, symbol in enumerate(symbols):
        color = colors[i]
        ask_data = mean_df[(mean_df['symbol'] == symbol) & (mean_df['side'] == 'ask')].sort_values('level')
        bid_data = mean_df[(mean_df['symbol'] == symbol) & (mean_df['side'] == 'bid')].sort_values('level')
        ask_levels = []
        ask_values = []
        for level in range(1, max_levels + 1):
            level_data = ask_data[ask_data['level'] == level]
            if len(level_data) > 0:
                ask_levels.append(level)
                value = level_data['acc_usd'].iloc[0]
                ask_values.append(max(value, 1) if not pd.isna(value) else 1)
        bid_levels = []
        bid_values = []
        for level in range(1, max_levels + 1):
            level_data = bid_data[bid_data['level'] == level]
            if len(level_data) > 0:
                bid_levels.append(level)
                value = level_data['acc_usd'].iloc[0]
                bid_values.append(max(value, 1) if not pd.isna(value) else 1)
        if ask_levels and ask_values:
            plt.plot(ask_levels, ask_values, color=color, linewidth=2, linestyle='-', label=f'{symbol} Ask', marker='o', markersize=6)
        if bid_levels and bid_values:
            plt.plot(bid_levels, bid_values, color=color, linewidth=2, linestyle='--', label=f'{symbol} Bid', marker='s', markersize=6)
    plt.yscale('log')
    plt.xlabel('Orderbook Level', fontsize=12, fontweight='bold')
    plt.ylabel('Accumulated USD Value (Log Scale)', fontsize=12, fontweight='bold')
    plt.title('Orderbook Depth - Mean Values (Accumulated USD)', fontsize=14, fontweight='bold')
    plt.grid(True, alpha=0.3, which='both')
    plt.xticks(range(1, max_levels + 1), [f'N{i}' for i in range(1, max_levels + 1)])
    ax = plt.gca()
    ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'${x:,.0f}' if x >= 1000 else f'${x:.0f}'))
    plt.tight_layout()
    plt.savefig(filename, dpi=300, bbox_inches='tight')
    plt.close()
    return plot_files

def create_excel_tables(results: Dict, percentiles: List[int], output_filename: str=EXCEL_FILENAME) -> None:
    workbook = openpyxl.Workbook()
    ws = workbook.active
    ws.title = 'Trading Analysis'
    
    # Create styles
    header_font = Font(bold=True, size=12, color='FFFFFF')
    header_fill = PatternFill(start_color='366092', end_color='366092', fill_type='solid')
    title_font = Font(bold=True, size=14, color='000000')
    center = Alignment(horizontal='center', vertical='center')
    
    # Create number formats
    currency_style = NamedStyle(name="currency", number_format='[$$-409]#,##0.00')
    percentage_style = NamedStyle(name="percentage", number_format='0.000%')
    
    if "currency" not in workbook.named_styles:
        workbook.add_named_style(currency_style)
    if "percentage" not in workbook.named_styles:
        workbook.add_named_style(percentage_style)

    symbols = list(results.keys())
    time_labels = ['3min', '5min', '10min', '30min', '60min', '90min', '1day', '1year']
    vol_keys = ['3min_vol', '5min_vol', '10min_vol', '30min_vol', '60min_vol', '90min_vol', '1day_vol', '1year_vol']
    
    # Get date range info
    date_info = get_date_range_info(results)
    
    row = 1
    
    # Add date range information
    if date_info['initial_date'] and date_info['last_date']:
        ws.cell(row=row, column=1, value=f"Initial Date: {date_info['initial_date'].strftime('%d/%m/%Y %H:%M')}").font = Font(bold=True, size=12)
        row += 1
        ws.cell(row=row, column=1, value=f"Last Date: {date_info['last_date'].strftime('%d/%m/%Y %H:%M')}").font = Font(bold=True, size=12)
        row += 1
        ws.cell(row=row, column=1, value=f"Time Span: {date_info['num_days']:.1f} days ({date_info['time_span_hours']:.1f} hours)").font = Font(bold=True, size=12)
        row += 2
    
    row += 1
    
    # Table 1: Volatility Analysis
    ws.cell(row=row, column=1, value='1. Volatility Analysis - Delta Percentages + Leverage Info').font = title_font
    table1_start = row + 1
    row += 2
    headers = ['Symbol'] + ['1 day' if tl == '1day' else '1 year' if tl == '1year' else tl for tl in time_labels] + ['Real Max Leverage', 'Max Quantity', 'Second Leverage']
    for c, hdr in enumerate(headers, start=1):
        cell = ws.cell(row=row, column=c, value=hdr)
        cell.font = header_font
        cell.fill = header_fill
        cell.alignment = center
    row += 1
    for sym in symbols:
        ws.cell(row=row, column=1, value=sym).font = Font(bold=True)
        ws.cell(row=row, column=1).alignment = center
        for c, vk in enumerate(vol_keys, start=2):
            v = results[sym]['volatility_metrics'][vk]
            cell = ws.cell(row=row, column=c, value=v)
            cell.alignment = center
            cell.style = percentage_style
        lev_info = results[sym]['leverage_info']
        tiers = lev_info.get('tiers', [])
        real_max_lev = lev_info.get('max_leverage', 0)
        ws.cell(row=row, column=len(headers) - 2, value=real_max_lev).alignment = center
        if len(tiers) >= 2:
            max_quantity = tiers[0][1]
            second_leverage = tiers[1][2]
            if max_quantity == float('inf'):
                max_qty_val = None
            else:
                max_qty_val = max_quantity
            cell = ws.cell(row=row, column=len(headers) - 1, value=max_qty_val)
            cell.alignment = center
            if max_qty_val:
                cell.style = currency_style
            ws.cell(row=row, column=len(headers), value=second_leverage).alignment = center
        else:
            ws.cell(row=row, column=len(headers) - 1, value=None).alignment = center
            ws.cell(row=row, column=len(headers), value=None).alignment = center
        row += 1
    table1_end = row - 1
    
    row += 2
    
    # Table 2: Spread Distances  
    ws.cell(row=row, column=1, value="2. Spread Distances by Percentile + Mean").font = title_font
    table2_start = row + 1
    row += 2

    percentile_dfs = build_percentile_depth(results, percentiles)
    mean_df = build_mean_depth(results)
    
    max_levels = {}
    for perc in percentiles:
        pct_df = percentile_dfs[perc]
        for symbol in symbols:
            symbol_data = pct_df[pct_df['symbol'] == symbol]
            if not symbol_data.empty:
                max_level = symbol_data['level'].max()
                max_levels[symbol] = max(max_levels.get(symbol, 0), max_level)
    
    for symbol in symbols:
        symbol_data = mean_df[mean_df['symbol'] == symbol]
        if not symbol_data.empty:
            max_level = symbol_data['level'].max()
            max_levels[symbol] = max(max_levels.get(symbol, 0), max_level)
    
    overall_max_level = max(max_levels.values()) if max_levels else 5

    headers = ["Token", "Level"]
    for perc in percentiles:
        headers.append(f"p{perc}")
    headers.append("mean")

    for c, hdr in enumerate(headers, start=1):
        cell = ws.cell(row=row, column=c, value=hdr)
        cell.font = header_font
        cell.fill = header_fill
        cell.alignment = center
    row += 1

    for symbol in symbols:
        for level in range(1, max_levels.get(symbol, overall_max_level) + 1):
            col = 1
            ws.cell(row=row, column=col, value=symbol).alignment = center
            col += 1
            ws.cell(row=row, column=col, value=f"N{level}").alignment = center
            col += 1

            # Calculate mean spread distance from ask and bid
            for perc in percentiles:
                pct_df = percentile_dfs[perc]
                ask_val = pct_df.loc[(pct_df['symbol'] == symbol) &
                                     (pct_df['side'] == 'ask') &
                                     (pct_df['level'] == level), 'spread_distance']
                bid_val = pct_df.loc[(pct_df['symbol'] == symbol) &
                                     (pct_df['side'] == 'bid') &
                                     (pct_df['level'] == level), 'spread_distance']
                
                ask_num = ask_val.iloc[0] if len(ask_val) > 0 and not pd.isna(ask_val.iloc[0]) else np.nan
                bid_num = bid_val.iloc[0] if len(bid_val) > 0 and not pd.isna(bid_val.iloc[0]) else np.nan
                
                if not np.isnan(ask_num) and not np.isnan(bid_num):
                    mean_val = (ask_num + bid_num) / 2 / 100.0  # Convert to decimal for percentage format
                elif not np.isnan(ask_num):
                    mean_val = ask_num / 100.0
                elif not np.isnan(bid_num):
                    mean_val = bid_num / 100.0
                else:
                    mean_val = None
                
                cell = ws.cell(row=row, column=col, value=mean_val)
                cell.alignment = center
                if mean_val is not None:
                    cell.style = percentage_style
                col += 1

            # Mean values
            ask_val = mean_df.loc[(mean_df['symbol'] == symbol) &
                                  (mean_df['side'] == 'ask') &
                                  (mean_df['level'] == level), 'spread_distance']
            bid_val = mean_df.loc[(mean_df['symbol'] == symbol) &
                                  (mean_df['side'] == 'bid') &
                                  (mean_df['level'] == level), 'spread_distance']
            
            ask_num = ask_val.iloc[0] if len(ask_val) > 0 and not pd.isna(ask_val.iloc[0]) else np.nan
            bid_num = bid_val.iloc[0] if len(bid_val) > 0 and not pd.isna(bid_val.iloc[0]) else np.nan
            
            if not np.isnan(ask_num) and not np.isnan(bid_num):
                mean_val = (ask_num + bid_num) / 2 / 100.0
            elif not np.isnan(ask_num):
                mean_val = ask_num / 100.0
            elif not np.isnan(bid_num):
                mean_val = bid_num / 100.0
            else:
                mean_val = None
                
            cell = ws.cell(row=row, column=col, value=mean_val)
            cell.alignment = center
            if mean_val is not None:
                cell.style = percentage_style

            row += 1
    
    table2_end = row - 1
    row += 2

    # Table 3: Token Quantities (Mean of Ask/Bid)
    ws.cell(row=row, column=1, value='3. Token Quantities - Mean of Ask/Bid').font = title_font
    table3_start = row + 1
    row += 2
    
    # Create headers for token quantities table
    headers = ["Symbol", "Level"]
    for perc in percentiles:
        headers.append(f"p{perc}")
    headers.append("mean")
    
    for c, hdr in enumerate(headers, start=1):
        cell = ws.cell(row=row, column=c, value=hdr)
        cell.font = header_font
        cell.fill = header_fill
        cell.alignment = center
    row += 1
    
    for symbol in symbols:
        for level in range(1, max_levels.get(symbol, overall_max_level) + 1):
            col = 1
            ws.cell(row=row, column=col, value=symbol).alignment = center
            col += 1
            ws.cell(row=row, column=col, value=f"N{level}").alignment = center
            col += 1
            
            # Percentiles
            for perc in percentiles:
                pct_df = percentile_dfs[perc]
                
                # Quantity mean
                ask_qty = pct_df.loc[(pct_df['symbol'] == symbol) & (pct_df['side'] == 'ask') & (pct_df['level'] == level), 'acc_qty']
                bid_qty = pct_df.loc[(pct_df['symbol'] == symbol) & (pct_df['side'] == 'bid') & (pct_df['level'] == level), 'acc_qty']
                
                ask_qty_val = ask_qty.iloc[0] if len(ask_qty) > 0 and not pd.isna(ask_qty.iloc[0]) else np.nan
                bid_qty_val = bid_qty.iloc[0] if len(bid_qty) > 0 and not pd.isna(bid_qty.iloc[0]) else np.nan
                
                if not np.isnan(ask_qty_val) and not np.isnan(bid_qty_val):
                    qty_mean = (ask_qty_val + bid_qty_val) / 2
                elif not np.isnan(ask_qty_val):
                    qty_mean = ask_qty_val
                elif not np.isnan(bid_qty_val):
                    qty_mean = bid_qty_val
                else:
                    qty_mean = None
                    
                ws.cell(row=row, column=col, value=qty_mean).alignment = center
                col += 1
            
            # Mean values
            ask_qty = mean_df.loc[(mean_df['symbol'] == symbol) & (mean_df['side'] == 'ask') & (mean_df['level'] == level), 'acc_qty']
            bid_qty = mean_df.loc[(mean_df['symbol'] == symbol) & (mean_df['side'] == 'bid') & (mean_df['level'] == level), 'acc_qty']
            
            ask_qty_val = ask_qty.iloc[0] if len(ask_qty) > 0 and not pd.isna(ask_qty.iloc[0]) else np.nan
            bid_qty_val = bid_qty.iloc[0] if len(bid_qty) > 0 and not pd.isna(bid_qty.iloc[0]) else np.nan
            
            if not np.isnan(ask_qty_val) and not np.isnan(bid_qty_val):
                qty_mean = (ask_qty_val + bid_qty_val) / 2
            elif not np.isnan(ask_qty_val):
                qty_mean = ask_qty_val
            elif not np.isnan(bid_qty_val):
                qty_mean = bid_qty_val
            else:
                qty_mean = None
                
            ws.cell(row=row, column=col, value=qty_mean).alignment = center
            
            row += 1
    
    table3_end = row - 1
    row += 2

    # Table 4: USD Values (Mean of Ask/Bid)
    ws.cell(row=row, column=1, value='4. USD Values - Mean of Ask/Bid').font = title_font
    table4_start = row + 1
    row += 2
    
    # Create headers for USD values table
    headers = ["Symbol", "Level"]
    for perc in percentiles:
        headers.append(f"p{perc}")
    headers.append("mean")
    
    for c, hdr in enumerate(headers, start=1):
        cell = ws.cell(row=row, column=c, value=hdr)
        cell.font = header_font
        cell.fill = header_fill
        cell.alignment = center
    row += 1
    
    for symbol in symbols:
        for level in range(1, max_levels.get(symbol, overall_max_level) + 1):
            col = 1
            ws.cell(row=row, column=col, value=symbol).alignment = center
            col += 1
            ws.cell(row=row, column=col, value=f"N{level}").alignment = center
            col += 1
            
            # Percentiles
            for perc in percentiles:
                pct_df = percentile_dfs[perc]
                
                # USD mean
                ask_usd = pct_df.loc[(pct_df['symbol'] == symbol) & (pct_df['side'] == 'ask') & (pct_df['level'] == level), 'acc_usd']
                bid_usd = pct_df.loc[(pct_df['symbol'] == symbol) & (pct_df['side'] == 'bid') & (pct_df['level'] == level), 'acc_usd']
                
                ask_usd_val = ask_usd.iloc[0] if len(ask_usd) > 0 and not pd.isna(ask_usd.iloc[0]) else np.nan
                bid_usd_val = bid_usd.iloc[0] if len(bid_usd) > 0 and not pd.isna(bid_usd.iloc[0]) else np.nan
                
                if not np.isnan(ask_usd_val) and not np.isnan(bid_usd_val):
                    usd_mean = (ask_usd_val + bid_usd_val) / 2
                elif not np.isnan(ask_usd_val):
                    usd_mean = ask_usd_val
                elif not np.isnan(bid_usd_val):
                    usd_mean = bid_usd_val
                else:
                    usd_mean = None
                    
                cell = ws.cell(row=row, column=col, value=usd_mean)
                cell.alignment = center
                if usd_mean is not None:
                    cell.style = currency_style
                col += 1
            
            # Mean values
            ask_usd = mean_df.loc[(mean_df['symbol'] == symbol) & (mean_df['side'] == 'ask') & (mean_df['level'] == level), 'acc_usd']
            bid_usd = mean_df.loc[(mean_df['symbol'] == symbol) & (mean_df['side'] == 'bid') & (mean_df['level'] == level), 'acc_usd']
            
            ask_usd_val = ask_usd.iloc[0] if len(ask_usd) > 0 and not pd.isna(ask_usd.iloc[0]) else np.nan
            bid_usd_val = bid_usd.iloc[0] if len(bid_usd) > 0 and not pd.isna(bid_usd.iloc[0]) else np.nan
            
            if not np.isnan(ask_usd_val) and not np.isnan(bid_usd_val):
                usd_mean = (ask_usd_val + bid_usd_val) / 2
            elif not np.isnan(ask_usd_val):
                usd_mean = ask_usd_val
            elif not np.isnan(bid_usd_val):
                usd_mean = bid_usd_val
            else:
                usd_mean = None
                
            cell = ws.cell(row=row, column=col, value=usd_mean)
            cell.alignment = center
            if usd_mean is not None:
                cell.style = currency_style
            
            row += 1
    
    table4_end = row - 1
    row += 2

    # Calculate volume data using new approach
    volume_data = calculate_volume_by_spread_comparison(results, percentiles, percentile_dfs, mean_df)
    
    time_labels = ['3min', '5min', '10min', '30min', '60min', '90min', '1day', '1year']
    vol_keys = ['3min_vol', '5min_vol', '10min_vol', '30min_vol', '60min_vol', '90min_vol', '1day_vol', '1year_vol']
    
    table_starts = []
    table_ends = []
    
    # Create 5 separate volume tables
    volume_tables = [f'p{p}' for p in percentiles] + ['mean']
    
    for i, table_key in enumerate(volume_tables):
        table_num = 5 + i
        ws.cell(row=row, column=1, value=f'{table_num}. Max Investment Volume - {table_key.upper()}').font = title_font
        table_start = row + 1
        table_starts.append(table_start)
        row += 2
        
        headers = ['Symbol'] + ['1 day' if tl == '1day' else '1 year' if tl == '1year' else tl for tl in time_labels]
        for c, hdr in enumerate(headers, start=1):
            cell = ws.cell(row=row, column=c, value=hdr)
            cell.font = header_font
            cell.fill = header_fill
            cell.alignment = center
        row += 1
        
        for sym in symbols:
            ws.cell(row=row, column=1, value=sym).font = Font(bold=True)
            ws.cell(row=row, column=1).alignment = center
            
            for c, vk in enumerate(vol_keys, start=2):
                volume = volume_data[table_key][sym][vk]
                cell = ws.cell(row=row, column=c, value=volume if volume > 0 else None)
                cell.alignment = center
                if volume > 0:
                    cell.style = currency_style
            row += 1
        
        table_end = row - 1
        table_ends.append(table_end)
        row += 2
    
    row += 1
    
    # Create plots section
    final_table_num = 5 + len(volume_tables)
    ws.cell(row=row, column=1, value=f'{final_table_num}. Orderbook Depth Analysis Plots').font = title_font
    row += 2
    depth_plot_files = create_orderbook_depth_plots(results, percentiles)
    plot_row = row
    plots_per_row = 2
    plot_width_cells = 15
    plot_height_cells = 25
    for i, plot_file in enumerate(depth_plot_files):
        if Path(plot_file).exists():
            plot_col = 1 + i % plots_per_row * (plot_width_cells + 1)
            current_plot_row = plot_row + i // plots_per_row * (plot_height_cells + 2)
            plot_name = plot_file.name.replace('.png', '').replace('orderbook_depth_', '').upper()
            ws.cell(row=current_plot_row, column=plot_col, value=f'Plot: {plot_name}').font = Font(bold=True, size=12)
            img = Image(plot_file)
            img.width = 600
            img.height = 400
            img.anchor = f'{get_column_letter(plot_col)}{current_plot_row + 1}'
            ws.add_image(img)

    # Add grouping/outlining for tables (one at a time as requested)
    try:
        ws.row_dimensions.group(table1_start, table1_end, outline_level=1, hidden=False)
        ws.row_dimensions.group(table2_start, table2_end, outline_level=1, hidden=False)
        ws.row_dimensions.group(table3_start, table3_end, outline_level=1, hidden=False) 
        ws.row_dimensions.group(table4_start, table4_end, outline_level=1, hidden=False)
        # Group each volume table individually
        for table_start, table_end in zip(table_starts, table_ends):
            ws.row_dimensions.group(table_start, table_end, outline_level=1, hidden=False)
    except:
        pass  # If grouping fails, continue without it

    # Set column widths
    ws.column_dimensions['A'].width = 12
    for c in range(2, len(symbols) + 50):
        ws.column_dimensions[get_column_letter(c)].width = 12
    
    workbook.save(output_filename)

def parse_arguments():
    parser = argparse.ArgumentParser(description='Volatility and orderbook analysis with configurable percentiles, leverage chart, and time series charts')
    parser.add_argument('csv_file', help='Path to CSV file with orderbook data')
    parser.add_argument('symbols', nargs='*', help='Optional: specific symbols to analyze')
    parser.add_argument('--percentiles', nargs='+', type=int, default=DEFAULT_PERCENTILES, help=f'Percentiles to calculate (1-99). Default: {DEFAULT_PERCENTILES}')
    parser.add_argument('--sep', default=',', help='CSV separator/delimiter (default: ",")')
    parser.add_argument('--chart-level', type=int, default=1, help='Orderbook level to use for leverage chart (default: 1)')
    parser.add_argument('--chart-type', choices=['bid', 'ask', 'avg'], default='avg', help='Type of data to use for leverage chart: bid, ask, or avg (default: avg)')
    parser.add_argument('--max-levels', type=int, default=5, help='Maximum number of orderbook levels to plot in time series charts (default: 5)')
    args = parser.parse_args()
    for p in args.percentiles:
        if not 1 <= p <= 99:
            parser.error(f'Percentile {p} must be between 1 and 99')
    args.percentiles = sorted(set(args.percentiles))
    if 50 not in args.percentiles:
        args.percentiles.append(50)
        args.percentiles = sorted(args.percentiles)
    return args

def main():
    args = parse_arguments()
    if not Path(args.csv_file).exists():
        raise SystemExit(f'CSV file not found: {args.csv_file}')
    if not Path(LEVERAGE_DATA_FILE).exists():
        print(f"Warning: Leverage data file not found: {LEVERAGE_DATA_FILE}")
    results = analyze_csv(args.csv_file, args.symbols, sep=args.sep)
    create_excel_tables(results, args.percentiles, output_filename=EXCEL_FILENAME)
    leverage_usd_data = extract_median_usd_from_p50(results, args.percentiles, args.chart_level)
    create_leverage_vs_usd_chart(leverage_usd_data, output_filename=CHART_FILENAME, chart_type=args.chart_type)
    time_series_data = extract_time_series_levels(results, max_levels=args.max_levels)
    chart_files = []
    for symbol in results.keys():
        token_chart = PLOTS_DIR / f'token_levels_over_time_{symbol}.png'
        usd_chart = PLOTS_DIR / f'usd_levels_over_time_{symbol}.png'
        create_token_levels_chart(time_series_data, symbol, token_chart, max_levels=args.max_levels)
        create_usd_levels_chart(time_series_data, symbol, usd_chart, max_levels=args.max_levels)
        chart_files.extend([token_chart, usd_chart])
if __name__ == '__main__':
    main()