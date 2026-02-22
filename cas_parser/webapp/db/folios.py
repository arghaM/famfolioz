"""Folio management operations."""

import logging
from typing import List, Optional

from cas_parser.webapp.db.connection import get_db

logger = logging.getLogger(__name__)

__all__ = [
    "get_folio_by_number_and_isin",
    "get_folios_by_investor",
    "get_unmapped_folios",
    "get_all_folios_with_assignments",
    "create_folio",
    "map_folio_to_investor",
    "map_folios_to_investor",
    "get_folio_by_id",
    "unmap_folio",
]


def get_folio_by_number_and_isin(folio_number: str, isin: str) -> Optional[dict]:
    """Get a folio by its number and ISIN."""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT f.*, i.name as investor_name, i.pan as investor_pan
            FROM folios f
            LEFT JOIN investors i ON i.id = f.investor_id
            WHERE f.folio_number = ? AND f.isin = ?
        """, (folio_number, isin))
        row = cursor.fetchone()
        return dict(row) if row else None


def get_folios_by_investor(investor_id: int) -> List[dict]:
    """Get all folios for an investor."""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT f.*, h.units, h.nav, h.nav_date, h.current_value, h.cost_value
            FROM folios f
            LEFT JOIN holdings h ON h.folio_id = f.id
            WHERE f.investor_id = ?
            ORDER BY f.amc, f.scheme_name
        """, (investor_id,))
        return [dict(row) for row in cursor.fetchall()]


def get_unmapped_folios() -> List[dict]:
    """Get all folios not mapped to any investor."""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT f.*, h.units, h.nav, h.current_value
            FROM folios f
            LEFT JOIN holdings h ON h.folio_id = f.id
            WHERE f.investor_id IS NULL
            ORDER BY f.amc, f.scheme_name
        """)
        return [dict(row) for row in cursor.fetchall()]


def get_all_folios_with_assignments() -> List[dict]:
    """Get all folios with investor assignment info."""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT f.id, f.folio_number, f.scheme_name, f.isin, f.amc,
                   f.investor_id, i.name as investor_name,
                   h.units, h.current_value
            FROM folios f
            LEFT JOIN investors i ON i.id = f.investor_id
            LEFT JOIN holdings h ON h.folio_id = f.id
            ORDER BY CASE WHEN i.name IS NULL THEN 1 ELSE 0 END,
                     i.name, f.amc, f.scheme_name
        """)
        return [dict(row) for row in cursor.fetchall()]


def create_folio(folio_number: str, scheme_name: str, isin: str,
                 amc: str = None, registrar: str = None, investor_id: int = None) -> int:
    """Create a new folio and return its ID."""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            INSERT OR IGNORE INTO folios (folio_number, scheme_name, isin, amc, registrar, investor_id)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (folio_number, scheme_name, isin, amc, registrar, investor_id))

        if cursor.rowcount == 0:
            # Already exists, get the ID
            cursor.execute("SELECT id FROM folios WHERE folio_number = ? AND isin = ?",
                          (folio_number, isin))
            return cursor.fetchone()[0]
        return cursor.lastrowid


def map_folio_to_investor(folio_id: int, investor_id: int):
    """Map a folio to an investor."""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            UPDATE folios SET investor_id = ? WHERE id = ?
        """, (investor_id, folio_id))


def map_folios_to_investor(folio_ids: List[int], investor_id: int):
    """Map multiple folios to an investor."""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.executemany("""
            UPDATE folios SET investor_id = ? WHERE id = ?
        """, [(investor_id, fid) for fid in folio_ids])


def get_folio_by_id(folio_id: int) -> Optional[dict]:
    """Get folio with investor and holdings info, using live NAV when available."""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT f.*, i.name as investor_name, i.id as investor_id,
                   h.units,
                   COALESCE(mf.current_nav, h.nav) as nav,
                   COALESCE(mf.nav_date, h.nav_date) as nav_date,
                   h.units * COALESCE(mf.current_nav, h.nav) as current_value
            FROM folios f
            LEFT JOIN investors i ON i.id = f.investor_id
            LEFT JOIN holdings h ON h.folio_id = f.id
            LEFT JOIN mutual_fund_master mf ON mf.isin = f.isin
            WHERE f.id = ?
        """, (folio_id,))
        row = cursor.fetchone()
        return dict(row) if row else None


def unmap_folio(folio_id: int):
    """Remove investor mapping from a folio."""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            UPDATE folios SET investor_id = NULL WHERE id = ?
        """, (folio_id,))
