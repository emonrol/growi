import requests
from typing import Dict, List, Tuple, Optional

INFO_URL = "https://api.hyperliquid.xyz/info"

class HLMetaError(Exception):
    pass

def fetch_perp_meta(dex: str = "") -> Dict:
    """
    Calls POST /info with {"type": "meta", "dex": dex} to get universe + marginTables.
    """
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
    """
    Converts [[id, {...}], [id, {...}], ...] to {id: {...}} and parses tiers.
    The tiers are returned sorted by lowerBound ascending.
    """
    out = {}
    for pair in tables_raw:
        if not isinstance(pair, list) or len(pair) != 2:
            continue
        table_id, table = pair
        tiers = table.get("marginTiers", [])
        # normalize to (lower_bound_float, max_leverage_int)
        parsed = []
        for t in tiers:
            lb = float(t["lowerBound"])
            lev = int(t["maxLeverage"])
            parsed.append((lb, lev))
        parsed.sort(key=lambda x: x[0])
        out[int(table_id)] = {
            "description": table.get("description", ""),
            "tiers": parsed
        }
    return out

def pick_margin_table_id_for_asset(asset: Dict, margin_tables: Dict[int, Dict]) -> Optional[int]:
    """
    Pick a margin table automatically:

    Strategy:
    1) If there's a single-tier table whose id == asset.maxLeverage and its only tier
       maxLeverage equals asset.maxLeverage, use it (simple markets).
    2) Otherwise, find all tables whose HIGHEST tier leverage equals asset.maxLeverage.
       - If exactly one matches, use it (typical tiered markets: e.g., BTC 'tiered 40x').
       - If multiple match, prefer a table whose description contains 'tiered'
         or with the greatest number of tiers (more specific), otherwise pick the one
         with the highest highest lowerBound (most conservative / latest).
    3) If nothing matches, return None.
    """
    asset_max = int(asset["maxLeverage"])

    # 1) Simple single-tier match by id
    if asset_max in margin_tables:
        tiers = margin_tables[asset_max]["tiers"]
        if len(tiers) == 1 and tiers[0][1] == asset_max:
            return asset_max

    # 2) Look for tiered tables whose top tier equals asset_max
    candidates = []
    for tid, info in margin_tables.items():
        tiers = info["tiers"]
        if not tiers:
            continue
        top_lev = max(lev for _, lev in tiers)
        if top_lev == asset_max:
            candidates.append((tid, info))

    if len(candidates) == 1:
        return candidates[0][0]
    elif len(candidates) > 1:
        # Prefer 'tiered' in description, or more tiers, or largest lowerBound on top tier
        def score(item):
            tid, info = item
            desc = (info.get("description") or "").lower()
            tiers = info["tiers"]
            has_tiered = "tiered" in desc
            n_tiers = len(tiers)
            top_lb = max(lb for lb, lev in tiers if lev == asset_max)
            return (has_tiered, n_tiers, top_lb)
        candidates.sort(key=score, reverse=True)
        return candidates[0][0]

    # 3) Give up
    return None


def effective_max_leverage_for_notional(
    token: str,
    notional_usdc: float,
    dex: str = "",
    override_margin_table_id: Optional[int] = None
) -> Dict:
    """
    Returns a clean dict with { 'token': str, 'tiers': List[(lowerBound, maxLeverage)] }.
    """
    meta = fetch_perp_meta(dex=dex)
    uni = _index_universe(meta["universe"])
    if token not in uni:
        raise HLMetaError(f"Token '{token}' not found in universe")
    asset = uni[token]
    tables = _index_margin_tables(meta["marginTables"])

    # Choose margin table
    table_id = override_margin_table_id or pick_margin_table_id_for_asset(asset, tables)
    if table_id is None:
        available = {tid: tables[tid]["description"] for tid in sorted(tables)}
        raise HLMetaError(
            f"Couldn't infer margin table for {token}. "
            f"Pass override_margin_table_id from one of: {available}"
        )

    tiers = tables[table_id]["tiers"]

    return {
        "token": token,
        "tiers": tiers
    }


# --- Example usage ---
if __name__ == "__main__":
    output = effective_max_leverage_for_notional("BTC", 25_000)
    print(output)

