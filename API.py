import requests
from typing import Dict, List, Tuple, Optional

INFO_URL = "https://api.hyperliquid.xyz/info"

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