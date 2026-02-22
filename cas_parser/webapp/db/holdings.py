"""Holdings CRUD operations."""

import logging
from typing import List

from cas_parser.webapp.db.connection import get_db

logger = logging.getLogger(__name__)

__all__ = [
    "upsert_holding",
    "get_holdings_by_investor",
]


def upsert_holding(folio_id: int, units: float, nav: float, nav_date: str,
                   current_value: float, cost_value: float = None):
    """Insert or update a holding."""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO holdings (folio_id, units, nav, nav_date, current_value, cost_value)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(folio_id) DO UPDATE SET
                units = excluded.units,
                nav = excluded.nav,
                nav_date = excluded.nav_date,
                current_value = excluded.current_value,
                cost_value = COALESCE(excluded.cost_value, holdings.cost_value),
                updated_at = CURRENT_TIMESTAMP
        """, (folio_id, units, nav, nav_date, current_value, cost_value))


def get_holdings_by_investor(investor_id: int) -> List[dict]:
    """Get all holdings for an investor with invested amount and fund classification."""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT h.*, f.folio_number,
                   COALESCE(mfm.display_name, mfm.amfi_scheme_name, f.scheme_name) as scheme_name,
                   f.isin, f.amc, f.registrar,
                   COALESCE(h.cost_value, inv.invested_amount, 0) as invested_amount,
                   mfm.fund_category, mfm.geography
            FROM holdings h
            JOIN folios f ON f.id = h.folio_id
            LEFT JOIN mutual_fund_master mfm ON mfm.isin = f.isin
            LEFT JOIN (
                SELECT folio_id, SUM(
                    CASE
                        WHEN tx_type IN ('purchase', 'sip', 'switch_in') THEN amount
                        WHEN tx_type IN ('redemption', 'switch_out') THEN amount
                        ELSE 0
                    END
                ) as invested_amount
                FROM transactions
                WHERE status = 'active'
                GROUP BY folio_id
            ) inv ON inv.folio_id = h.folio_id
            WHERE f.investor_id = ?
            ORDER BY f.amc, h.current_value DESC
        """, (investor_id,))
        return [dict(row) for row in cursor.fetchall()]
