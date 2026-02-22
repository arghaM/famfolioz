"""Benchmark CRUD and data cache operations."""

import logging
import sqlite3
from typing import List, Optional
from cas_parser.webapp.db.connection import get_db

logger = logging.getLogger(__name__)

__all__ = [
    'get_folios_with_transactions',
    'get_category_weights',
    'get_benchmarks_by_investor',
    'add_benchmark',
    'delete_benchmark',
    'upsert_benchmark_data',
    'get_benchmark_data',
    'get_benchmark_data_latest_date',
]


def get_folios_with_transactions(investor_id: int, category: str = None) -> List[dict]:
    """Get all folios for an investor with their transactions, for benchmarking.

    Returns list of dicts with folio info + amfi_code + list of transactions.
    """
    with get_db() as conn:
        query = """
            SELECT f.id, f.isin, f.folio_number, f.scheme_name,
                   mfm.amfi_code, mfm.fund_category
            FROM folios f
            LEFT JOIN mutual_fund_master mfm ON mfm.isin = f.isin
            WHERE f.investor_id = ?
        """
        params = [investor_id]

        if category:
            query += " AND mfm.fund_category = ?"
            params.append(category)

        folios = conn.execute(query, params).fetchall()

        result = []
        for f in folios:
            folio = dict(f)
            # Get transactions for this folio
            txns = conn.execute(
                """SELECT tx_date, units, tx_type, amount, nav
                   FROM transactions
                   WHERE folio_id = ? AND status = 'active'
                   ORDER BY tx_date ASC""",
                (folio['id'],)
            ).fetchall()
            folio['transactions'] = [dict(t) for t in txns]
            result.append(folio)

        return result


def get_category_weights(investor_id: int) -> dict:
    """Get portfolio value weights by fund category for an investor.

    Returns dict like {'equity': 0.72, 'debt': 0.15, ...} where values sum to 1.
    """
    with get_db() as conn:
        rows = conn.execute(
            """SELECT mfm.fund_category, SUM(h.current_value) as total_value
               FROM holdings h
               JOIN folios f ON f.id = h.folio_id
               LEFT JOIN mutual_fund_master mfm ON mfm.isin = f.isin
               WHERE f.investor_id = ? AND h.units > 0
               GROUP BY mfm.fund_category""",
            (investor_id,)
        ).fetchall()

        weights = {}
        grand_total = sum(r['total_value'] or 0 for r in rows)

        if grand_total <= 0:
            return weights

        for r in rows:
            cat = r['fund_category'] or 'equity'
            val = r['total_value'] or 0
            weights[cat] = round(val / grand_total, 4)

        return weights


# --- Benchmark functions ---

def get_benchmarks_by_investor(investor_id: int) -> List[dict]:
    """Get all saved benchmarks for an investor."""
    with get_db() as conn:
        rows = conn.execute(
            "SELECT * FROM benchmarks WHERE investor_id = ? ORDER BY created_at",
            (investor_id,)
        ).fetchall()
        return [dict(r) for r in rows]


def add_benchmark(investor_id: int, scheme_code: int, scheme_name: str, fund_house: str = None) -> Optional[int]:
    """Add a benchmark for an investor. Returns id or None if duplicate."""
    with get_db() as conn:
        try:
            cursor = conn.execute(
                "INSERT INTO benchmarks (investor_id, scheme_code, scheme_name, fund_house) VALUES (?, ?, ?, ?)",
                (investor_id, scheme_code, scheme_name, fund_house)
            )
            return cursor.lastrowid
        except sqlite3.IntegrityError:
            row = conn.execute(
                "SELECT id FROM benchmarks WHERE investor_id = ? AND scheme_code = ?",
                (investor_id, scheme_code)
            ).fetchone()
            return row['id'] if row else None


def delete_benchmark(investor_id: int, benchmark_id: int) -> bool:
    """Delete a benchmark for an investor."""
    with get_db() as conn:
        cursor = conn.execute(
            "DELETE FROM benchmarks WHERE id = ? AND investor_id = ?",
            (benchmark_id, investor_id)
        )
        return cursor.rowcount > 0


def upsert_benchmark_data(scheme_code: int, rows: List[dict]) -> int:
    """Bulk insert or replace benchmark NAV data. rows: [{data_date, nav}]."""
    if not rows:
        return 0
    with get_db() as conn:
        conn.executemany(
            "INSERT INTO benchmark_data (scheme_code, data_date, nav) VALUES (?, ?, ?) "
            "ON CONFLICT(scheme_code, data_date) DO UPDATE SET nav = excluded.nav",
            [(scheme_code, r['data_date'], r['nav']) for r in rows]
        )
        return len(rows)


def get_benchmark_data(scheme_code: int, start_date: str = None, end_date: str = None) -> List[dict]:
    """Get benchmark NAV data points ordered by date."""
    with get_db() as conn:
        query = "SELECT data_date, nav FROM benchmark_data WHERE scheme_code = ?"
        params = [scheme_code]
        if start_date:
            query += " AND data_date >= ?"
            params.append(start_date)
        if end_date:
            query += " AND data_date <= ?"
            params.append(end_date)
        query += " ORDER BY data_date ASC"
        rows = conn.execute(query, params).fetchall()
        return [dict(r) for r in rows]


def get_benchmark_data_latest_date(scheme_code: int) -> Optional[str]:
    """Get the most recent cached date for a benchmark's data."""
    with get_db() as conn:
        row = conn.execute(
            "SELECT MAX(data_date) as latest FROM benchmark_data WHERE scheme_code = ?",
            (scheme_code,)
        ).fetchone()
        return row['latest'] if row and row['latest'] else None
