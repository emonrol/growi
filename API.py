import requests
from typing import Dict, List, Tuple, Optional
import json
import sys
import argparse
from pathlib import Path
import time

INFO_URL = "https://api.hyperliquid.xyz/info"
LEVERAGE_DATA_FILE = "leverage_data.json"

class HLMetaError(Exception):
    pass

def fetch_perp_meta(dex: str = "") -> Dict:
    payload = {"type": "meta"}
    if dex:
        payload["dex"] = dex
    r = requests.post(INFO_URL, json=payload, timeout=10)
    r.raise_for_status()
    data = r.json()
    if not isinstance(data, dict) or "universe" not in data or "marginTables" not in data:
        raise HLMetaError(f"Unexpected meta schema: {data}")
    return data

def _index_universe(universe: List[Dict]) -> Dict[str, Dict]:
    return {a["name"]: a for a in universe}

def _index_margin_tables(tables_raw: List[List]) -> Dict[int, Dict]:
    out = {}
    for pair in tables_raw:
        if not isinstance(pair, list) or len(pair) != 2:
            continue
        table_id, table = pair
        tiers = table.get("marginTiers", [])
        
        raw_tiers = []
        for t in tiers:
            lb = float(t["lowerBound"])
            lev = int(t["maxLeverage"])
            raw_tiers.append((lb, lev))
        raw_tiers.sort(key=lambda x: x[0])
        
        parsed = []
        for i, (lb, lev) in enumerate(raw_tiers):
            if i < len(raw_tiers) - 1:
                ub = raw_tiers[i + 1][0]
            else:
                ub = float('inf')
            parsed.append((lb, ub, lev))
        
        out[int(table_id)] = {
            "description": table.get("description", ""),
            "tiers": parsed
        }
    return out

def pick_margin_table_id_for_asset(asset: Dict, margin_tables: Dict[int, Dict]) -> Optional[int]:
    if 'marginTableId' in asset:
        margin_table_id = int(asset['marginTableId'])
        if margin_table_id in margin_tables:
            return margin_table_id
    
    asset_max = int(asset["maxLeverage"])

    if asset_max in margin_tables:
        tiers = margin_tables[asset_max]["tiers"]
        if len(tiers) == 1 and tiers[0][2] == asset_max:
            return asset_max

    candidates = []
    for tid, info in margin_tables.items():
        tiers = info["tiers"]
        if not tiers:
            continue
        top_lev = max(lev for _, _, lev in tiers)
        if top_lev == asset_max:
            candidates.append((tid, info))

    if len(candidates) == 1:
        return candidates[0][0]
    elif len(candidates) > 1:
        def score(item):
            tid, info = item
            desc = (info.get("description") or "").lower()
            tiers = info["tiers"]
            
            position_limit = 0
            for lb, ub, lev in tiers:
                if lev == asset_max:
                    position_limit = ub if ub != float('inf') else 999999999999
                    break
            
            import re
            number_match = re.search(r'\((\d+)\)', desc)
            desc_number = int(number_match.group(1)) if number_match else 0
            
            has_tiered = "tiered" in desc
            n_tiers = len(tiers)
            
            return (position_limit, desc_number, has_tiered, n_tiers)
        
        candidates.sort(key=score, reverse=True)
        return candidates[0][0]

    return None

def effective_max_leverage_for_notional(
    token: str,
    notional_usdc: float = 0.0,
    dex: str = "",
    override_margin_table_id: Optional[int] = None
) -> Dict[str, List[Tuple[float, float, int]]]:
    meta = fetch_perp_meta(dex=dex)
    uni = _index_universe(meta["universe"])
    if token not in uni:
        raise HLMetaError(f"Token '{token}' not found in universe")
    asset = uni[token]
    tables = _index_margin_tables(meta["marginTables"])

    table_id = override_margin_table_id or pick_margin_table_id_for_asset(asset, tables)
    if table_id is None:
        available = {tid: tables[tid]["description"] for tid in sorted(tables)}
        raise HLMetaError(
            f"Couldn't infer margin table for {token}. "
            f"Pass override_margin_table_id from one of: {available}"
        )

    tiers = tables[table_id]["tiers"]
    return {token: tiers}

def get_leverage_info_safe(token: str, dex: str = "") -> Dict:
    try:
        meta = fetch_perp_meta(dex=dex)
        uni = _index_universe(meta["universe"])
        if token not in uni:
            return {
                'status': 'error',
                'token': token,
                'error': f'Token {token} not found in universe',
                'max_leverage': 0,
                'min_leverage': 0,
                'num_tiers': 0,
                'tiers': [],
                'tiers_formatted': []
            }
        
        asset = uni[token]
        asset_true_max = int(asset["maxLeverage"])
        
        try:
            leverage_data = effective_max_leverage_for_notional(token, dex=dex)
            if token in leverage_data:
                tiers = leverage_data[token]
                
                capped_tiers = []
                for lb, ub, lev in tiers:
                    capped_lev = min(lev, asset_true_max)
                    capped_tiers.append((lb, ub, capped_lev))
                
                max_lev = max(lev for _, _, lev in capped_tiers) if capped_tiers else asset_true_max
                min_lev = min(lev for _, _, lev in capped_tiers) if capped_tiers else asset_true_max
                
                max_lev = min(max_lev, asset_true_max)
                min_lev = min(min_lev, asset_true_max)
                
                formatted_tiers = []
                for lb, ub, lev in capped_tiers:
                    if ub == float('inf'):
                        range_str = f"${lb:,.0f}+"
                    else:
                        range_str = f"${lb:,.0f} - ${ub:,.0f}"
                    formatted_tiers.append((range_str, f"{lev}x"))
                
                return {
                    'status': 'success',
                    'token': token,
                    'max_leverage': max_lev,
                    'min_leverage': min_lev,
                    'num_tiers': len(capped_tiers),
                    'tiers': capped_tiers,
                    'tiers_formatted': formatted_tiers,
                    'error': None,
                    'asset_true_max': asset_true_max,
                    'note': 'Leverage capped at asset maximum' if any(lev != orig_lev for (_, _, lev), (_, _, orig_lev) in zip(capped_tiers, tiers)) else None
                }
        except HLMetaError:
            return {
                'status': 'partial_success',
                'token': token,
                'max_leverage': asset_true_max,
                'min_leverage': asset_true_max,
                'num_tiers': 1,
                'tiers': [(0.0, float('inf'), asset_true_max)],
                'tiers_formatted': [("$0+", f"{asset_true_max}x")],
                'error': None,
                'asset_true_max': asset_true_max,
                'note': f'No margin table found, using asset maximum of {asset_true_max}x'
            }
            
        return {
            'status': 'error',
            'token': token,
            'error': f'Token {token} not found in API response',
            'max_leverage': asset_true_max,
            'min_leverage': asset_true_max,
            'num_tiers': 1,
            'tiers': [(0.0, float('inf'), asset_true_max)],
            'tiers_formatted': [("$0+", f"{asset_true_max}x")],
            'asset_true_max': asset_true_max
        }
    except Exception as e:
        return {
            'status': 'error',
            'token': token,
            'error': str(e),
            'max_leverage': 0,
            'min_leverage': 0,
            'num_tiers': 0,
            'tiers': [],
            'tiers_formatted': []
        }

def save_leverage_data(symbols: List[str], output_file: str = LEVERAGE_DATA_FILE, dex: str = "") -> None:
    """
    Fetch leverage data for multiple symbols and save to JSON file.
    
    Args:
        symbols: List of symbols to fetch leverage data for
        output_file: Output JSON filename
        dex: Optional dex parameter for API
    """
    leverage_data = {
        'timestamp': time.time(),
        'symbols': {}
    }
    
    print(f"Fetching leverage data for {len(symbols)} symbols...")
    
    for i, symbol in enumerate(symbols, 1):
        print(f"[{i}/{len(symbols)}] Fetching {symbol}...")
        try:
            data = get_leverage_info_safe(symbol, dex=dex)
            leverage_data['symbols'][symbol] = data
            
            if data['status'] == 'success':
                print(f"  ✓ Success - {data['num_tiers']} tiers, max {data['max_leverage']}x")
            elif data['status'] == 'partial_success':
                print(f"  ⚠ Partial - fallback to {data['max_leverage']}x")
            else:
                print(f"  ✗ Error - {data.get('error', 'Unknown error')}")
                
        except Exception as e:
            print(f"  ✗ Exception - {e}")
            leverage_data['symbols'][symbol] = {
                'status': 'error',
                'token': symbol,
                'error': str(e),
                'max_leverage': 0,
                'min_leverage': 0,
                'num_tiers': 0,
                'tiers': [],
                'tiers_formatted': []
            }
    
    # Save to file
    try:
        with open(output_file, 'w') as f:
            json.dump(leverage_data, f, indent=2)
        print(f"\n✓ Leverage data saved to: {output_file}")
        print(f"  Timestamp: {time.ctime(leverage_data['timestamp'])}")
        print(f"  Symbols: {len(leverage_data['symbols'])}")
        
        # Summary
        success_count = sum(1 for data in leverage_data['symbols'].values() if data['status'] == 'success')
        partial_count = sum(1 for data in leverage_data['symbols'].values() if data['status'] == 'partial_success')
        error_count = sum(1 for data in leverage_data['symbols'].values() if data['status'] == 'error')
        
        print(f"  Results: {success_count} success, {partial_count} partial, {error_count} errors")
        
    except Exception as e:
        raise SystemExit(f"Error saving leverage data: {e}")

def load_leverage_data(input_file: str = LEVERAGE_DATA_FILE) -> Dict:
    """
    Load leverage data from JSON file.
    
    Args:
        input_file: Input JSON filename
        
    Returns:
        Dict containing leverage data
    """
    try:
        with open(input_file, 'r') as f:
            data = json.load(f)
        
        if 'timestamp' in data and 'symbols' in data:
            return data
        else:
            raise ValueError("Invalid leverage data file format")
            
    except FileNotFoundError:
        raise SystemExit(f"Leverage data file not found: {input_file}")
    except json.JSONDecodeError as e:
        raise SystemExit(f"Invalid JSON in leverage data file: {e}")
    except Exception as e:
        raise SystemExit(f"Error loading leverage data: {e}")

def get_symbols_from_leverage_file(input_file: str = LEVERAGE_DATA_FILE) -> List[str]:
    """
    Get list of symbols available in leverage data file.
    
    Args:
        input_file: Input JSON filename
        
    Returns:
        List of symbol names
    """
    try:
        data = load_leverage_data(input_file)
        return list(data['symbols'].keys())
    except Exception as e:
        print(f"Warning: Could not load symbols from leverage file: {e}")
        return []

def get_leverage_info_from_file(symbol: str, input_file: str = LEVERAGE_DATA_FILE) -> Dict:
    """
    Get leverage info for a specific symbol from file.
    
    Args:
        symbol: Symbol to get leverage info for
        input_file: Input JSON filename
        
    Returns:
        Leverage info dict
    """
    try:
        data = load_leverage_data(input_file)
        
        if symbol in data['symbols']:
            return data['symbols'][symbol]
        else:
            return {
                'status': 'error',
                'token': symbol,
                'error': f'Symbol {symbol} not found in leverage data file',
                'max_leverage': 0,
                'min_leverage': 0,
                'num_tiers': 0,
                'tiers': [],
                'tiers_formatted': []
            }
    except Exception as e:
        return {
            'status': 'error',
            'token': symbol,
            'error': f'Could not load from leverage data file: {e}',
            'max_leverage': 0,
            'min_leverage': 0,
            'num_tiers': 0,
            'tiers': [],
            'tiers_formatted': []
        }

def parse_arguments():
    parser = argparse.ArgumentParser(description="Fetch and save leverage data for crypto symbols")
    parser.add_argument('symbols', nargs='*', 
                       help='Symbols to fetch leverage data for (if none provided, shows available symbols)')
    parser.add_argument('--output', '-o', default=LEVERAGE_DATA_FILE,
                       help=f'Output JSON file (default: {LEVERAGE_DATA_FILE})')
    parser.add_argument('--dex', default="",
                       help='Optional DEX parameter for API')
    parser.add_argument('--list', '-l', action='store_true',
                       help='List symbols available in existing leverage data file')
    
    return parser.parse_args()

def main():
    args = parse_arguments()
    
    if args.list:
        # List symbols in existing file
        if Path(args.output).exists():
            try:
                data = load_leverage_data(args.output)
                symbols = list(data['symbols'].keys())
                timestamp = time.ctime(data['timestamp'])
                
                print(f"Leverage data file: {args.output}")
                print(f"Last updated: {timestamp}")
                print(f"Symbols ({len(symbols)}):")
                for symbol in sorted(symbols):
                    status = data['symbols'][symbol]['status']
                    if status == 'success':
                        max_lev = data['symbols'][symbol]['max_leverage']
                        print(f"  {symbol} - ✓ {max_lev}x")
                    elif status == 'partial_success':
                        max_lev = data['symbols'][symbol]['max_leverage']
                        print(f"  {symbol} - ⚠ {max_lev}x (fallback)")
                    else:
                        print(f"  {symbol} - ✗ Error")
                        
            except Exception as e:
                print(f"Error reading leverage data file: {e}")
        else:
            print(f"No leverage data file found: {args.output}")
        return
    
    if not args.symbols:
        # No symbols provided - show usage
        print("No symbols provided. Usage examples:")
        print(f"  python {sys.argv[0]} BTC ETH SOL")
        print(f"  python {sys.argv[0]} --list")
        print(f"  python {sys.argv[0]} --help")
        return
    
    # Fetch and save leverage data
    save_leverage_data(args.symbols, args.output, args.dex)

if __name__ == "__main__":
    main()