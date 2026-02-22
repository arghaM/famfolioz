"""NAV fetching, history, and portfolio snapshot operations."""

import json
import logging
import urllib.request
from datetime import date, datetime
from typing import List, Optional

from cas_parser.webapp.db.connection import get_db
from cas_parser.webapp.db.mutual_funds import get_mapped_mutual_funds

logger = logging.getLogger(__name__)

__all__ = [
    "fetch_and_update_nav",
    "get_nav_for_holdings",
    "get_last_nav_update",
    "get_nav_history",
    "get_nav_history_dates",
    "take_portfolio_snapshot",
    "take_all_portfolio_snapshots",
    "get_portfolio_history",
    "get_portfolio_valuation_on_date",
]


def fetch_and_update_nav() -> dict:
    """
    Fetch NAV from AMFI for all mapped mutual funds.

    Only fetches NAV for funds that have an amfi_code mapped.
    """
    import urllib.request

    # Get all mapped funds
    mapped_funds = get_mapped_mutual_funds()
    if not mapped_funds:
        return {'success': True, 'updated': 0, 'message': 'No mapped funds to update'}

    # Create a dict of amfi_code -> mf_id for quick lookup
    amfi_to_mf = {mf['amfi_code']: mf['id'] for mf in mapped_funds}
    amfi_codes = set(amfi_to_mf.keys())

    url = "https://portal.amfiindia.com/spages/NAVOpen.txt"

    try:
        with urllib.request.urlopen(url, timeout=30) as response:
            content = response.read().decode('utf-8', errors='ignore')
    except Exception as e:
        logger.error(f"Failed to fetch NAV data: {e}")
        return {'success': False, 'error': str(e)}

    lines = content.strip().split('\n')
    updated_count = 0

    with get_db() as conn:
        cursor = conn.cursor()

        for line in lines:
            line = line.strip()
            if not line:
                continue

            parts = line.split(';')
            if len(parts) < 5:
                continue

            scheme_code = parts[0].strip()

            # Only process if this scheme code is in our mapped funds
            if scheme_code not in amfi_codes:
                continue

            try:
                nav_str = parts[4].strip()
                nav_date = parts[5].strip() if len(parts) > 5 else ''
                isin = parts[1].strip() if len(parts) > 1 else ''
                nav = float(nav_str)

                mf_id = amfi_to_mf[scheme_code]

                # Update current NAV in mutual_fund_master
                cursor.execute("""
                    UPDATE mutual_fund_master
                    SET current_nav = ?, nav_date = ?, updated_at = CURRENT_TIMESTAMP
                    WHERE id = ?
                """, (nav, nav_date, mf_id))

                if cursor.rowcount > 0:
                    updated_count += 1

                # Also store in nav_history for historical tracking
                # Get ISIN from mutual_fund_master
                cursor.execute("SELECT isin FROM mutual_fund_master WHERE id = ?", (mf_id,))
                mf_row = cursor.fetchone()
                if mf_row and mf_row['isin']:
                    cursor.execute("""
                        INSERT INTO nav_history (isin, nav_date, nav)
                        VALUES (?, ?, ?)
                        ON CONFLICT(isin, nav_date) DO UPDATE SET nav = excluded.nav
                    """, (mf_row['isin'], nav_date, nav))

            except (ValueError, IndexError) as e:
                continue

    logger.info(f"NAV update complete: {updated_count} funds updated")

    return {
        'success': True,
        'updated': updated_count,
        'total_mapped': len(mapped_funds),
        'message': f'Updated NAV for {updated_count} of {len(mapped_funds)} mapped funds'
    }


def get_nav_for_holdings(holdings: List[dict]) -> List[dict]:
    """
    Enhance holdings with current NAV from mutual fund master.

    Returns holdings with additional fields:
    - current_nav: Latest NAV from AMFI
    - current_nav_date: Date of the NAV
    - current_value_live: Recalculated value using current NAV
    - is_mapped: Whether this fund has AMFI mapping
    """
    with get_db() as conn:
        cursor = conn.cursor()

        for holding in holdings:
            isin = holding.get('isin')
            if not isin:
                holding['current_nav'] = None
                holding['current_nav_date'] = None
                holding['current_value_live'] = None
                holding['is_mapped'] = False
                continue

            cursor.execute("""
                SELECT current_nav, nav_date, amfi_code
                FROM mutual_fund_master
                WHERE isin = ?
            """, (isin,))
            row = cursor.fetchone()

            if row and row['current_nav']:
                holding['current_nav'] = row['current_nav']
                holding['current_nav_date'] = row['nav_date']
                units = holding.get('units', 0) or 0
                holding['current_value_live'] = units * row['current_nav']
                holding['is_mapped'] = bool(row['amfi_code'])
            else:
                holding['current_nav'] = None
                holding['current_nav_date'] = None
                holding['current_value_live'] = None
                holding['is_mapped'] = False

    return holdings


def get_last_nav_update() -> Optional[str]:
    """Get the timestamp of the last NAV update."""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT MAX(updated_at) as last_update
            FROM mutual_fund_master
            WHERE current_nav IS NOT NULL
        """)
        row = cursor.fetchone()
        return row['last_update'] if row else None


# ==================== Historical Valuation Operations ====================

def get_nav_history(isin: str, start_date: str = None, end_date: str = None) -> List[dict]:
    """Get historical NAV for a scheme."""
    with get_db() as conn:
        cursor = conn.cursor()

        query = "SELECT * FROM nav_history WHERE isin = ?"
        params = [isin]

        if start_date:
            query += " AND nav_date >= ?"
            params.append(start_date)
        if end_date:
            query += " AND nav_date <= ?"
            params.append(end_date)

        query += " ORDER BY nav_date ASC"

        cursor.execute(query, params)
        return [dict(row) for row in cursor.fetchall()]


def get_nav_history_dates() -> List[str]:
    """Get all unique dates with NAV history."""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT DISTINCT nav_date FROM nav_history
            ORDER BY nav_date DESC
        """)
        return [row['nav_date'] for row in cursor.fetchall()]


def take_portfolio_snapshot(investor_id: int, snapshot_date: str = None) -> dict:
    """
    Take a portfolio valuation snapshot for an investor.

    Uses NAV from nav_history for the given date, or current NAV if not available.
    """
    from datetime import date as date_type

    if not snapshot_date:
        snapshot_date = date_type.today().strftime('%d-%b-%Y')

    with get_db() as conn:
        cursor = conn.cursor()

        # Get holdings for investor
        cursor.execute("""
            SELECT h.units, f.isin, f.scheme_name
            FROM holdings h
            JOIN folios f ON f.id = h.folio_id
            WHERE f.investor_id = ?
        """, (investor_id,))
        holdings = cursor.fetchall()

        total_value = 0
        holdings_valued = 0

        for holding in holdings:
            isin = holding['isin']
            units = holding['units'] or 0

            # Try to get NAV for this date from history
            cursor.execute("""
                SELECT nav FROM nav_history
                WHERE isin = ? AND nav_date = ?
            """, (isin, snapshot_date))
            nav_row = cursor.fetchone()

            if nav_row:
                nav = nav_row['nav']
            else:
                # Fall back to current NAV from mutual_fund_master
                cursor.execute("""
                    SELECT current_nav FROM mutual_fund_master WHERE isin = ?
                """, (isin,))
                mf_row = cursor.fetchone()
                nav = mf_row['current_nav'] if mf_row and mf_row['current_nav'] else 0

            if nav:
                total_value += units * nav
                holdings_valued += 1

        # Get total invested (from active transactions)
        cursor.execute("""
            SELECT SUM(
                CASE
                    WHEN tx_type IN ('purchase', 'sip', 'switch_in') AND amount > 0 THEN amount
                    WHEN tx_type IN ('redemption', 'switch_out') AND amount < 0 THEN amount
                    ELSE 0
                END
            ) as total_invested
            FROM transactions t
            JOIN folios f ON f.id = t.folio_id
            WHERE f.investor_id = ? AND t.status = 'active'
        """, (investor_id,))
        invested_row = cursor.fetchone()
        total_invested = invested_row['total_invested'] or 0

        # Store snapshot
        cursor.execute("""
            INSERT INTO portfolio_snapshots (investor_id, snapshot_date, total_value, total_invested, holdings_count)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(investor_id, snapshot_date) DO UPDATE SET
                total_value = excluded.total_value,
                total_invested = excluded.total_invested,
                holdings_count = excluded.holdings_count
        """, (investor_id, snapshot_date, total_value, total_invested, holdings_valued))

        return {
            'investor_id': investor_id,
            'snapshot_date': snapshot_date,
            'total_value': total_value,
            'total_invested': total_invested,
            'holdings_count': holdings_valued
        }


def take_all_portfolio_snapshots(snapshot_date: str = None) -> dict:
    """Take portfolio snapshots for all investors."""
    from cas_parser.webapp.db.investors import get_all_investors

    investors = get_all_investors()
    results = []

    for investor in investors:
        result = take_portfolio_snapshot(investor['id'], snapshot_date)
        results.append(result)

    return {
        'snapshots_taken': len(results),
        'date': snapshot_date,
        'results': results
    }


def get_portfolio_history(investor_id: int, start_date: str = None, end_date: str = None) -> List[dict]:
    """Get historical portfolio valuation for an investor."""
    with get_db() as conn:
        cursor = conn.cursor()

        query = "SELECT * FROM portfolio_snapshots WHERE investor_id = ?"
        params = [investor_id]

        if start_date:
            query += " AND snapshot_date >= ?"
            params.append(start_date)
        if end_date:
            query += " AND snapshot_date <= ?"
            params.append(end_date)

        query += " ORDER BY snapshot_date ASC"

        cursor.execute(query, params)
        return [dict(row) for row in cursor.fetchall()]


def get_portfolio_valuation_on_date(investor_id: int, valuation_date: str) -> dict:
    """
    Calculate portfolio value on a specific historical date.

    Uses holdings at that date (based on transactions) and NAV from history.
    """
    with get_db() as conn:
        cursor = conn.cursor()

        # Get folios for investor
        cursor.execute("""
            SELECT f.id, f.isin, f.scheme_name, f.folio_number
            FROM folios f WHERE f.investor_id = ?
        """, (investor_id,))
        folios = cursor.fetchall()

        holdings_data = []
        total_value = 0

        for folio in folios:
            # Calculate units held on that date from transactions
            cursor.execute("""
                SELECT SUM(units) as total_units
                FROM transactions
                WHERE folio_id = ? AND tx_date <= ? AND status = 'active'
            """, (folio['id'], valuation_date))
            units_row = cursor.fetchone()
            units = units_row['total_units'] or 0

            if units <= 0:
                continue

            # Get NAV for that date
            cursor.execute("""
                SELECT nav FROM nav_history
                WHERE isin = ? AND nav_date <= ?
                ORDER BY nav_date DESC LIMIT 1
            """, (folio['isin'], valuation_date))
            nav_row = cursor.fetchone()

            nav = nav_row['nav'] if nav_row else 0
            value = units * nav

            holdings_data.append({
                'scheme_name': folio['scheme_name'],
                'folio_number': folio['folio_number'],
                'isin': folio['isin'],
                'units': units,
                'nav': nav,
                'value': value
            })
            total_value += value

        return {
            'investor_id': investor_id,
            'valuation_date': valuation_date,
            'total_value': total_value,
            'holdings': holdings_data
        }
