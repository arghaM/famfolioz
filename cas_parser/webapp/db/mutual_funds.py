"""Mutual fund master management, asset allocation, and classification."""

import logging
from datetime import date, datetime
from typing import List, Optional, Tuple
from cas_parser.webapp.db.connection import get_db

logger = logging.getLogger(__name__)

__all__ = [
    'get_all_mutual_funds',
    'get_unmapped_mutual_funds',
    'get_mapped_mutual_funds',
    'add_to_mutual_fund_master',
    'map_mutual_fund_to_amfi',
    'update_fund_display_name',
    'update_fund_asset_allocation',
    'update_fund_classification',
    'get_fund_holdings',
    'get_fund_sectors',
    'update_fund_holdings',
    'update_fund_sectors',
    'get_fund_detail',
    'get_current_fy_dates',
    'get_similar_funds',
    'search_amfi_schemes',
    'get_mutual_fund_stats',
    'populate_mutual_fund_master_from_folios',
    'BUY_TX_TYPES',
    'SELL_TX_TYPES',
    'VALID_SECTORS',
]


def get_all_mutual_funds() -> List[dict]:
    """Get all mutual funds from master."""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT mf.*,
                   (SELECT COUNT(*) FROM fund_holdings fh WHERE fh.mf_id = mf.id) AS holdings_count,
                   (SELECT COUNT(*) FROM fund_sectors fs WHERE fs.mf_id = mf.id) AS sectors_count
            FROM mutual_fund_master mf
            ORDER BY mf.amc, mf.scheme_name
        """)
        return [dict(row) for row in cursor.fetchall()]


def get_unmapped_mutual_funds() -> List[dict]:
    """Get mutual funds without AMFI code mapping."""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT * FROM mutual_fund_master
            WHERE amfi_code IS NULL OR amfi_code = ''
            ORDER BY amc, scheme_name
        """)
        return [dict(row) for row in cursor.fetchall()]


def get_mapped_mutual_funds() -> List[dict]:
    """Get mutual funds with AMFI code mapping."""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT * FROM mutual_fund_master
            WHERE amfi_code IS NOT NULL AND amfi_code != ''
            ORDER BY amc, scheme_name
        """)
        return [dict(row) for row in cursor.fetchall()]


def add_to_mutual_fund_master(scheme_name: str, isin: str, amc: str) -> int:
    """Add a scheme to mutual fund master if not exists.

    Uses NULLIF to convert empty strings to NULL so COALESCE works correctly.
    This prevents empty scheme_name from overwriting existing valid names.
    """
    with get_db() as conn:
        cursor = conn.cursor()
        # Use NULLIF to treat empty strings as NULL for COALESCE
        cursor.execute("""
            INSERT INTO mutual_fund_master (scheme_name, isin, amc)
            VALUES (NULLIF(?, ''), ?, NULLIF(?, ''))
            ON CONFLICT(isin) DO UPDATE SET
                scheme_name = COALESCE(NULLIF(excluded.scheme_name, ''), mutual_fund_master.scheme_name),
                amc = COALESCE(NULLIF(excluded.amc, ''), mutual_fund_master.amc)
        """, (scheme_name, isin, amc))

        cursor.execute("SELECT id FROM mutual_fund_master WHERE isin = ?", (isin,))
        row = cursor.fetchone()
        return row['id'] if row else 0


def map_mutual_fund_to_amfi(mf_id: int, amfi_code: str, amfi_scheme_name: str = None) -> bool:
    """Map a mutual fund to an AMFI scheme code."""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            UPDATE mutual_fund_master
            SET amfi_code = ?, amfi_scheme_name = ?, updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
        """, (amfi_code, amfi_scheme_name, mf_id))
        return cursor.rowcount > 0


def update_fund_display_name(mf_id: int, display_name: str) -> bool:
    """Update the user-editable display name for a mutual fund."""
    with get_db() as conn:
        cursor = conn.cursor()
        # If display_name is empty, set to NULL (will fall back to scheme_name)
        cursor.execute("""
            UPDATE mutual_fund_master
            SET display_name = NULLIF(?, ''), updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
        """, (display_name.strip(), mf_id))
        return cursor.rowcount > 0


def update_fund_asset_allocation(mf_id: int, equity_pct: float, debt_pct: float,
                                  commodity_pct: float, cash_pct: float, others_pct: float,
                                  large_cap_pct: float = 0, mid_cap_pct: float = 0,
                                  small_cap_pct: float = 0) -> dict:
    """
    Update asset allocation percentages for a mutual fund.

    Asset class percentages should sum to 100.
    Market cap percentages (large/mid/small) apply to equity portion and should sum to 100
    when equity > 0.
    """
    total = equity_pct + debt_pct + commodity_pct + cash_pct + others_pct

    if abs(total - 100) > 0.05 and total != 0:
        return {'success': False, 'error': f'Percentages must sum to 100 (got {total})'}

    # Validate market cap split if equity has allocation
    if equity_pct > 0:
        cap_total = large_cap_pct + mid_cap_pct + small_cap_pct
        if cap_total > 0 and abs(cap_total - 100) > 0.05:
            return {'success': False, 'error': f'Market cap split must sum to 100 (got {cap_total})'}
    else:
        # No equity, zero out market cap
        large_cap_pct = mid_cap_pct = small_cap_pct = 0

    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            UPDATE mutual_fund_master
            SET equity_pct = ?, debt_pct = ?, commodity_pct = ?, cash_pct = ?, others_pct = ?,
                large_cap_pct = ?, mid_cap_pct = ?, small_cap_pct = ?,
                allocation_reviewed_at = CURRENT_TIMESTAMP,
                updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
        """, (equity_pct, debt_pct, commodity_pct, cash_pct, others_pct,
              large_cap_pct, mid_cap_pct, small_cap_pct, mf_id))

        return {'success': cursor.rowcount > 0}


def update_fund_classification(mf_id: int, fund_category: Optional[str], geography: Optional[str]) -> dict:
    """Update fund category and geography classification labels."""
    valid_categories = {None, 'equity', 'debt', 'hybrid', 'gold_commodity'}
    valid_geographies = {None, 'india', 'international'}

    if fund_category not in valid_categories:
        return {'success': False, 'error': f'Invalid fund_category: {fund_category}'}
    if geography not in valid_geographies:
        return {'success': False, 'error': f'Invalid geography: {geography}'}

    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            UPDATE mutual_fund_master
            SET fund_category = ?, geography = ?, updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
        """, (fund_category, geography, mf_id))
        return {'success': cursor.rowcount > 0}


BUY_TX_TYPES = {'purchase', 'sip', 'switch_in', 'stp_in', 'transfer_in', 'bonus', 'dividend_reinvestment'}
SELL_TX_TYPES = {'redemption', 'switch_out', 'stp_out', 'transfer_out'}

VALID_SECTORS = [
    'Financial Services', 'Information Technology', 'Healthcare', 'FMCG',
    'Automobile', 'Energy', 'Metals & Mining', 'Real Estate', 'Telecom',
    'Capital Goods', 'Consumer Discretionary', 'Utilities', 'Construction',
    'Chemicals', 'Textiles', 'Media & Entertainment', 'Others'
]


def get_fund_holdings(mf_id: int) -> List[dict]:
    """Return stock holdings for a fund, ordered by weight descending."""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT stock_name, weight_pct FROM fund_holdings
            WHERE mf_id = ? ORDER BY weight_pct DESC
        """, (mf_id,))
        return [dict(row) for row in cursor.fetchall()]


def get_fund_sectors(mf_id: int) -> List[dict]:
    """Return sector allocations for a fund, ordered by weight descending."""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT sector_name, weight_pct FROM fund_sectors
            WHERE mf_id = ? ORDER BY weight_pct DESC
        """, (mf_id,))
        return [dict(row) for row in cursor.fetchall()]


def update_fund_holdings(mf_id: int, holdings: list) -> dict:
    """Replace all holdings for a fund (delete-all + re-insert)."""
    for h in holdings:
        name = (h.get('stock_name') or '').strip()
        weight = h.get('weight_pct')
        if not name:
            return {'success': False, 'error': 'Stock name cannot be empty'}
        if weight is None or weight <= 0:
            return {'success': False, 'error': f'Weight must be > 0 for {name}'}

    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("DELETE FROM fund_holdings WHERE mf_id = ?", (mf_id,))
        for h in holdings:
            cursor.execute("""
                INSERT INTO fund_holdings (mf_id, stock_name, weight_pct)
                VALUES (?, ?, ?)
            """, (mf_id, h['stock_name'].strip(), h['weight_pct']))
        return {'success': True, 'count': len(holdings)}


def update_fund_sectors(mf_id: int, sectors: list) -> dict:
    """Replace all sector allocations for a fund (delete-all + re-insert)."""
    for s in sectors:
        name = (s.get('sector_name') or '').strip()
        weight = s.get('weight_pct')
        if not name:
            return {'success': False, 'error': 'Sector name cannot be empty'}
        if name not in VALID_SECTORS:
            return {'success': False, 'error': f'Invalid sector: {name}'}
        if weight is None or weight <= 0:
            return {'success': False, 'error': f'Weight must be > 0 for {name}'}

    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("DELETE FROM fund_sectors WHERE mf_id = ?", (mf_id,))
        for s in sectors:
            cursor.execute("""
                INSERT INTO fund_sectors (mf_id, sector_name, weight_pct)
                VALUES (?, ?, ?)
            """, (mf_id, s['sector_name'].strip(), s['weight_pct']))
        return {'success': True, 'count': len(sectors)}


def get_fund_detail(mf_id: int) -> Optional[dict]:
    """Return fund dict with holdings and sectors arrays."""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM mutual_fund_master WHERE id = ?", (mf_id,))
        row = cursor.fetchone()
        if not row:
            return None
        fund = dict(row)
        fund['holdings'] = get_fund_holdings(mf_id)
        fund['sectors'] = get_fund_sectors(mf_id)
        return fund


def get_current_fy_dates() -> Tuple[str, str]:
    """Return (fy_start, fy_end) as YYYY-MM-DD for the current Indian financial year."""
    today = date.today()
    if today.month >= 4:
        fy_start = date(today.year, 4, 1)
        fy_end = date(today.year + 1, 3, 31)
    else:
        fy_start = date(today.year - 1, 4, 1)
        fy_end = date(today.year, 3, 31)
    return fy_start.isoformat(), fy_end.isoformat()


def get_similar_funds(isin: str, limit: int = 5) -> List[dict]:
    """Find similar funds (same fund_category + geography, different ISIN).

    Scored by market cap similarity.
    """
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT fund_category, geography,
                   COALESCE(large_cap_pct, 0) as large_cap_pct,
                   COALESCE(mid_cap_pct, 0) as mid_cap_pct,
                   COALESCE(small_cap_pct, 0) as small_cap_pct
            FROM mutual_fund_master WHERE isin = ?
        """, (isin,))
        source = cursor.fetchone()
        if not source:
            return []

        cat = source['fund_category']
        geo = source['geography']
        if not cat:
            return []

        # Find funds with same category and geography
        cursor.execute("""
            SELECT id, scheme_name, display_name, amfi_scheme_name, isin, amc, current_nav,
                   fund_category, geography,
                   COALESCE(large_cap_pct, 0) as large_cap_pct,
                   COALESCE(mid_cap_pct, 0) as mid_cap_pct,
                   COALESCE(small_cap_pct, 0) as small_cap_pct,
                   COALESCE(exit_load_pct, 1.0) as exit_load_pct
            FROM mutual_fund_master
            WHERE isin != ? AND fund_category = ?
                  AND (geography = ? OR geography IS NULL OR ? IS NULL)
            ORDER BY scheme_name
        """, (isin, cat, geo, geo))
        candidates = [dict(row) for row in cursor.fetchall()]

    # Score by market cap similarity (lower = more similar)
    src_lc = source['large_cap_pct']
    src_mc = source['mid_cap_pct']
    src_sc = source['small_cap_pct']

    for c in candidates:
        c['similarity_score'] = (
            abs(c['large_cap_pct'] - src_lc) +
            abs(c['mid_cap_pct'] - src_mc) +
            abs(c['small_cap_pct'] - src_sc)
        )

    candidates.sort(key=lambda x: x['similarity_score'])
    return candidates[:limit]


def search_amfi_schemes(query: str) -> List[dict]:
    """
    Search AMFI schemes by name or code.

    Fetches from AMFI and filters by query.
    """
    import urllib.request

    url = "https://portal.amfiindia.com/spages/NAVOpen.txt"

    try:
        with urllib.request.urlopen(url, timeout=30) as response:
            content = response.read().decode('utf-8', errors='ignore')
    except Exception as e:
        logger.error(f"Failed to fetch AMFI data: {e}")
        return []

    lines = content.strip().split('\n')
    results = []
    query_lower = query.lower()
    current_amc = ""

    for line in lines:
        line = line.strip()
        if not line:
            continue

        parts = line.split(';')

        # AMC header lines (single column)
        if len(parts) == 1 and not line.startswith('Scheme Code'):
            current_amc = line
            continue

        if len(parts) >= 5:
            scheme_code = parts[0].strip()
            scheme_name = parts[3].strip() if len(parts) > 3 else ''
            nav_str = parts[4].strip() if len(parts) > 4 else ''

            # Search in scheme code or name
            if query_lower in scheme_code.lower() or query_lower in scheme_name.lower():
                try:
                    nav = float(nav_str) if nav_str else 0
                except ValueError:
                    nav = 0

                results.append({
                    'scheme_code': scheme_code,
                    'scheme_name': scheme_name,
                    'amc': current_amc,
                    'nav': nav
                })

                # Limit results
                if len(results) >= 50:
                    break

    return results


def get_mutual_fund_stats() -> dict:
    """Get statistics about mutual fund master."""
    with get_db() as conn:
        cursor = conn.cursor()

        cursor.execute("SELECT COUNT(*) as total FROM mutual_fund_master")
        total = cursor.fetchone()['total']

        cursor.execute("""
            SELECT COUNT(*) as mapped
            FROM mutual_fund_master
            WHERE amfi_code IS NOT NULL AND amfi_code != ''
        """)
        mapped = cursor.fetchone()['mapped']

        cursor.execute("""
            SELECT COUNT(*) as with_nav
            FROM mutual_fund_master
            WHERE current_nav IS NOT NULL
        """)
        with_nav = cursor.fetchone()['with_nav']

        return {
            'total': total,
            'mapped': mapped,
            'unmapped': total - mapped,
            'with_nav': with_nav
        }


def populate_mutual_fund_master_from_folios():
    """Populate mutual fund master from existing folios."""
    with get_db() as conn:
        cursor = conn.cursor()

        # Get unique schemes from folios
        cursor.execute("""
            SELECT DISTINCT scheme_name, isin, amc
            FROM folios
            WHERE isin IS NOT NULL AND isin != ''
        """)
        folios = cursor.fetchall()

        for folio in folios:
            cursor.execute("""
                INSERT INTO mutual_fund_master (scheme_name, isin, amc)
                VALUES (?, ?, ?)
                ON CONFLICT(isin) DO NOTHING
            """, (folio['scheme_name'], folio['isin'], folio['amc']))

        logger.info(f"Populated mutual fund master with {len(folios)} schemes from folios")
