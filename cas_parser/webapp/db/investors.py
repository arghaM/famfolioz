"""Investor CRUD operations."""

import logging
from typing import List, Optional

from cas_parser.webapp.db.connection import get_db

logger = logging.getLogger(__name__)

__all__ = [
    "get_all_investors",
    "get_investor_by_id",
    "get_investor_by_pan",
    "create_investor",
    "update_investor",
]


def get_all_investors() -> List[dict]:
    """Get all investors with live portfolio values including NPS."""
    with get_db() as conn:
        cursor = conn.cursor()
        # Get MF data
        cursor.execute("""
            SELECT i.*,
                   COUNT(DISTINCT f.id) as folio_count,
                   COALESCE(SUM(
                       CASE
                           WHEN mf.current_nav IS NOT NULL AND mf.current_nav > 0
                           THEN h.units * mf.current_nav
                           ELSE h.current_value
                       END
                   ), 0) as mf_value
            FROM investors i
            LEFT JOIN folios f ON f.investor_id = i.id
            LEFT JOIN holdings h ON h.folio_id = f.id
            LEFT JOIN mutual_fund_master mf ON f.isin = mf.isin
            GROUP BY i.id
            ORDER BY i.name
        """)
        investors = [dict(row) for row in cursor.fetchall()]

        # Get NPS data for each investor
        for inv in investors:
            cursor.execute("""
                SELECT COUNT(*) as nps_count,
                       COALESCE(SUM(ns.total_value), 0) as nps_value
                FROM nps_subscribers ns
                WHERE ns.investor_id = ?
            """, (inv['id'],))
            nps_row = cursor.fetchone()
            inv['nps_count'] = nps_row['nps_count'] if nps_row else 0
            inv['nps_value'] = nps_row['nps_value'] if nps_row else 0
            inv['total_value'] = inv['mf_value'] + inv['nps_value']

        return investors


def get_investor_by_id(investor_id: int) -> Optional[dict]:
    """Get an investor by ID."""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM investors WHERE id = ?", (investor_id,))
        row = cursor.fetchone()
        return dict(row) if row else None


def get_investor_by_pan(pan: str) -> Optional[dict]:
    """Get an investor by PAN."""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM investors WHERE pan = ?", (pan,))
        row = cursor.fetchone()
        return dict(row) if row else None


def create_investor(name: str, pan: str = None, email: str = None, mobile: str = None,
                    last_cas_upload: str = None, statement_from_date: str = None,
                    statement_to_date: str = None) -> int:
    """Create a new investor and return their ID."""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO investors (name, pan, email, mobile, last_cas_upload,
                                   statement_from_date, statement_to_date)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (name, pan, email, mobile, last_cas_upload, statement_from_date, statement_to_date))
        return cursor.lastrowid


def update_investor(investor_id: int, name: str = None, email: str = None, mobile: str = None,
                    last_cas_upload: str = None, statement_from_date: str = None,
                    statement_to_date: str = None) -> bool:
    """Update investor details."""
    with get_db() as conn:
        cursor = conn.cursor()
        updates = []
        params = []
        if name:
            updates.append("name = ?")
            params.append(name)
        if email:
            updates.append("email = ?")
            params.append(email)
        if mobile:
            updates.append("mobile = ?")
            params.append(mobile)
        if last_cas_upload:
            updates.append("last_cas_upload = ?")
            params.append(last_cas_upload)
        if statement_from_date:
            updates.append("statement_from_date = ?")
            params.append(statement_from_date)
        if statement_to_date:
            updates.append("statement_to_date = ?")
            params.append(statement_to_date)

        if updates:
            updates.append("updated_at = CURRENT_TIMESTAMP")
            params.append(investor_id)
            cursor.execute(f"""
                UPDATE investors SET {', '.join(updates)} WHERE id = ?
            """, params)
            return cursor.rowcount > 0
        return False
