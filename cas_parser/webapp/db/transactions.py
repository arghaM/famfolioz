"""Transaction CRUD, hashing, conflict resolution, and version tracking."""

import hashlib
import logging
import re
import sqlite3
from datetime import datetime
from typing import List, Optional, Tuple

from cas_parser.webapp.db.connection import get_db

logger = logging.getLogger(__name__)

__all__ = [
    'generate_tx_hash',
    '_compute_sequence_numbers',
    'insert_transaction',
    'get_pending_conflict_groups',
    'get_conflict_group_transactions',
    'resolve_conflict',
    'get_conflict_stats',
    'get_transactions_by_folio',
    'get_transactions_by_investor',
    'get_transaction_by_id',
    'update_transaction',
    'get_transaction_versions',
    'get_transaction_version_count',
    'get_transaction_stats',
]


def generate_tx_hash(folio_number: str, tx_date: str, tx_type: str, units: float, balance: float,
                     sequence: int = 0) -> str:
    """Generate a unique hash for a transaction to prevent duplicates.

    When sequence > 0, appends |seq{sequence} to the hash input so that
    transactions with identical (folio, date, type, units, balance) get
    distinct hashes.  sequence=0 produces the same hash as before for
    backward compatibility with existing DB rows.
    """
    data = f"{folio_number}|{tx_date}|{tx_type}|{units:.4f}|{balance:.4f}"
    if sequence > 0:
        data += f"|seq{sequence}"
    return hashlib.md5(data.encode()).hexdigest()


def _compute_sequence_numbers(transactions: list) -> dict:
    """Assign sequence numbers to transactions that share the same hash fingerprint.

    Walks transactions in list order (deterministic PDF order).  For each
    (folio, date, type, units_4dp, balance_4dp) fingerprint, the first
    occurrence gets sequence=0, the second gets 1, etc.

    Returns a sparse dict {global_index: sequence} containing only non-zero
    entries so callers can use ``seq_map.get(idx, 0)``.
    """
    counts: dict = {}  # fingerprint -> next sequence number
    seq_map: dict = {}
    for idx, tx in enumerate(transactions):
        fp = (
            tx.get('folio', ''),
            tx.get('date', ''),
            tx.get('type', ''),
            f"{float(tx.get('units', 0)):.4f}",
            f"{float(tx.get('balance_units', 0)):.4f}",
        )
        seq = counts.get(fp, 0)
        if seq > 0:
            seq_map[idx] = seq
        counts[fp] = seq + 1
    return seq_map


_REVERSAL_PATTERNS = re.compile(
    r'reversal|reject|payment\s+not\s+received|cancelled|invalid\s+purchase|failed',
    re.IGNORECASE
)


def _classify_transaction_status(tx_type: str, units: float, nav: float,
                                 description: str) -> str:
    """Classify whether a transaction is active or reversed/rejected.

    Returns 'reversed' only for zero-unit informational entries (rejected/cancelled notices).
    Buy-type transactions with negative units are kept 'active' — the FIFO engine
    treats them as sell-like deductions from existing lots.
    """
    desc = (description or '').strip()

    # Zero-unit transactions with reversal keywords — informational only, no unit impact
    if abs(units) < 0.0001 and _REVERSAL_PATTERNS.search(desc):
        return 'reversed'

    return 'active'


def insert_transaction(folio_id: int, tx_date: str, tx_type: str, description: str,
                       amount: float, units: float, nav: float, balance_units: float,
                       folio_number: str, detect_conflicts: bool = True,
                       force_status: str = None, sequence: int = 0) -> Tuple[int, str]:
    """
    Insert a transaction with conflict detection.

    Returns (transaction_id, status) where status is:
    - 'inserted': New transaction inserted as active
    - 'duplicate': Transaction already exists (active), skipped
    - 'discarded': Transaction was previously discarded by user, skipped
    - 'conflict': Transaction conflicts with existing, added to pending
    - 'reversed': Transaction inserted with reversed status

    Args:
        force_status: If set (e.g. 'reversed'), skip _classify_transaction_status()
                      and conflict detection; use this status directly.
        sequence: Disambiguation ordinal for transactions with identical hash
                  fingerprints. 0 = first occurrence (default, backward compatible).
    """
    tx_hash = generate_tx_hash(folio_number, tx_date, tx_type, units, balance_units, sequence)

    with get_db() as conn:
        cursor = conn.cursor()

        # Check if this exact transaction exists (by hash)
        cursor.execute("""
            SELECT id, status FROM transactions WHERE tx_hash = ?
        """, (tx_hash,))
        existing = cursor.fetchone()

        if existing:
            status = existing['status'] if isinstance(existing, sqlite3.Row) else existing[1]
            if status == 'discarded':
                return existing[0], 'discarded'
            return existing[0], 'duplicate'

        # Check if this hash is in pending conflicts
        cursor.execute("""
            SELECT id FROM pending_conflicts WHERE tx_hash = ?
        """, (tx_hash,))
        if cursor.fetchone():
            return 0, 'pending'

        # When force_status is set, skip classification and conflict detection
        if force_status:
            cursor.execute("""
                INSERT INTO transactions (folio_id, tx_date, tx_type, description, amount, units, nav, balance_units, tx_hash, status)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (folio_id, tx_date, tx_type, description, amount, units, nav, balance_units, tx_hash, force_status))
            return cursor.lastrowid, force_status

        # Classify FIRST: detect reversals/rejections before conflict detection
        # so that rejected transactions never enter the conflict pipeline
        tx_status = _classify_transaction_status(tx_type, units, nav, description)

        # Check for conflicts: ONLY for active purchase type transactions
        # Only flag when multiple purchase transactions exist on same day for same fund
        if detect_conflicts and tx_type == 'purchase' and tx_status == 'active':
            cursor.execute("""
                SELECT COUNT(*) as cnt FROM transactions
                WHERE folio_id = ? AND tx_date = ? AND tx_type = 'purchase' AND status = 'active'
            """, (folio_id, tx_date))
            existing_count = cursor.fetchone()[0]

            # Also check pending conflicts for this folio+date (purchase only)
            cursor.execute("""
                SELECT conflict_group_id FROM pending_conflicts
                WHERE folio_id = ? AND tx_date = ? AND tx_type = 'purchase'
                LIMIT 1
            """, (folio_id, tx_date))
            pending_group = cursor.fetchone()

            if existing_count > 0 or pending_group:
                # Conflict detected - add to pending
                if pending_group:
                    conflict_group_id = pending_group[0]
                else:
                    # Create new conflict group
                    conflict_group_id = f"{folio_id}_{tx_date}_{hashlib.md5(str(datetime.now()).encode()).hexdigest()[:8]}"

                    # Move existing purchase transactions to pending
                    cursor.execute("""
                        SELECT * FROM transactions
                        WHERE folio_id = ? AND tx_date = ? AND tx_type = 'purchase' AND status = 'active'
                    """, (folio_id, tx_date))
                    existing_txs = cursor.fetchall()

                    for tx in existing_txs:
                        cursor.execute("""
                            INSERT INTO pending_conflicts
                            (conflict_group_id, folio_id, tx_date, tx_type, description, amount, units, nav, balance_units, tx_hash)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """, (conflict_group_id, tx['folio_id'], tx['tx_date'], tx['tx_type'],
                              tx['description'], tx['amount'], tx['units'], tx['nav'],
                              tx['balance_units'], tx['tx_hash']))

                        # Mark original as pending conflict
                        cursor.execute("""
                            UPDATE transactions SET status = 'pending', conflict_group_id = ?
                            WHERE id = ?
                        """, (conflict_group_id, tx['id']))

                # Add current transaction to pending
                cursor.execute("""
                    INSERT INTO pending_conflicts
                    (conflict_group_id, folio_id, tx_date, tx_type, description, amount, units, nav, balance_units, tx_hash)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (conflict_group_id, folio_id, tx_date, tx_type, description, amount, units, nav, balance_units, tx_hash))

                return 0, 'conflict'

        cursor.execute("""
            INSERT INTO transactions (folio_id, tx_date, tx_type, description, amount, units, nav, balance_units, tx_hash, status)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (folio_id, tx_date, tx_type, description, amount, units, nav, balance_units, tx_hash, tx_status))

        return cursor.lastrowid, 'inserted' if tx_status == 'active' else tx_status


def get_pending_conflict_groups() -> List[dict]:
    """Get all pending conflict groups."""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT pc.conflict_group_id, pc.tx_date, f.folio_number, f.scheme_name, f.amc,
                   COUNT(*) as tx_count
            FROM pending_conflicts pc
            JOIN folios f ON f.id = pc.folio_id
            GROUP BY pc.conflict_group_id
            ORDER BY pc.created_at DESC
        """)
        return [dict(row) for row in cursor.fetchall()]


def get_conflict_group_transactions(conflict_group_id: str) -> List[dict]:
    """Get all transactions in a conflict group."""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT pc.*, f.folio_number, f.scheme_name
            FROM pending_conflicts pc
            JOIN folios f ON f.id = pc.folio_id
            WHERE pc.conflict_group_id = ?
            ORDER BY pc.amount DESC
        """, (conflict_group_id,))
        return [dict(row) for row in cursor.fetchall()]


def resolve_conflict(conflict_group_id: str, selected_tx_hashes: List[str]) -> dict:
    """
    Resolve a conflict group by selecting which transactions to keep.

    Args:
        conflict_group_id: The conflict group to resolve
        selected_tx_hashes: List of tx_hash values to mark as active

    Returns:
        Summary of resolution
    """
    with get_db() as conn:
        cursor = conn.cursor()

        # Get all pending transactions in this group
        cursor.execute("""
            SELECT * FROM pending_conflicts WHERE conflict_group_id = ?
        """, (conflict_group_id,))
        pending_txs = cursor.fetchall()

        activated = 0
        discarded = 0

        for tx in pending_txs:
            tx_hash = tx['tx_hash']

            # Check if this transaction already exists in main table
            cursor.execute("SELECT id, status FROM transactions WHERE tx_hash = ?", (tx_hash,))
            existing = cursor.fetchone()

            if tx_hash in selected_tx_hashes:
                # User selected this - mark as active
                if existing:
                    cursor.execute("""
                        UPDATE transactions SET status = 'active', conflict_group_id = NULL
                        WHERE tx_hash = ?
                    """, (tx_hash,))
                else:
                    cursor.execute("""
                        INSERT INTO transactions
                        (folio_id, tx_date, tx_type, description, amount, units, nav, balance_units, tx_hash, status)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'active')
                    """, (tx['folio_id'], tx['tx_date'], tx['tx_type'], tx['description'],
                          tx['amount'], tx['units'], tx['nav'], tx['balance_units'], tx_hash))
                activated += 1
            else:
                # User did not select - mark as discarded
                if existing:
                    cursor.execute("""
                        UPDATE transactions SET status = 'discarded', conflict_group_id = ?
                        WHERE tx_hash = ?
                    """, (conflict_group_id, tx_hash))
                else:
                    cursor.execute("""
                        INSERT INTO transactions
                        (folio_id, tx_date, tx_type, description, amount, units, nav, balance_units, tx_hash, status, conflict_group_id)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'discarded', ?)
                    """, (tx['folio_id'], tx['tx_date'], tx['tx_type'], tx['description'],
                          tx['amount'], tx['units'], tx['nav'], tx['balance_units'], tx_hash, conflict_group_id))
                discarded += 1

        # Remove from pending
        cursor.execute("DELETE FROM pending_conflicts WHERE conflict_group_id = ?", (conflict_group_id,))

        return {
            'conflict_group_id': conflict_group_id,
            'activated': activated,
            'discarded': discarded
        }


def get_conflict_stats() -> dict:
    """Get statistics about pending conflicts."""
    with get_db() as conn:
        cursor = conn.cursor()

        cursor.execute("SELECT COUNT(DISTINCT conflict_group_id) as groups FROM pending_conflicts")
        groups = cursor.fetchone()[0]

        cursor.execute("SELECT COUNT(*) as total FROM pending_conflicts")
        total_pending = cursor.fetchone()[0]

        return {
            'pending_groups': groups,
            'pending_transactions': total_pending
        }


def get_transactions_by_folio(folio_id: int, include_discarded: bool = False) -> List[dict]:
    """Get all transactions for a folio."""
    with get_db() as conn:
        cursor = conn.cursor()
        status_filter = "" if include_discarded else "AND t.status = 'active'"
        cursor.execute(f"""
            SELECT t.*, f.folio_number, f.scheme_name
            FROM transactions t
            JOIN folios f ON f.id = t.folio_id
            WHERE t.folio_id = ? {status_filter}
            ORDER BY t.tx_date DESC, t.id DESC
        """, (folio_id,))
        return [dict(row) for row in cursor.fetchall()]


def get_transactions_by_investor(investor_id: int, limit: int = 100, include_discarded: bool = False) -> List[dict]:
    """Get recent transactions for an investor."""
    with get_db() as conn:
        cursor = conn.cursor()
        status_filter = "" if include_discarded else "AND t.status = 'active'"
        cursor.execute(f"""
            SELECT t.*, f.folio_number, f.scheme_name, f.isin
            FROM transactions t
            JOIN folios f ON f.id = t.folio_id
            WHERE f.investor_id = ? {status_filter}
            ORDER BY t.tx_date DESC, t.id DESC
            LIMIT ?
        """, (investor_id, limit))
        return [dict(row) for row in cursor.fetchall()]


def get_transaction_by_id(tx_id: int) -> Optional[dict]:
    """Get a single transaction by ID."""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT t.*, f.folio_number, f.scheme_name, f.isin
            FROM transactions t
            JOIN folios f ON f.id = t.folio_id
            WHERE t.id = ?
        """, (tx_id,))
        row = cursor.fetchone()
        return dict(row) if row else None


def update_transaction(tx_id: int, tx_date: str, tx_type: str, description: str,
                       amount: float, units: float, nav: float, balance_units: float,
                       edit_comment: str, edited_by: str = None) -> dict:
    """
    Update a transaction and create a version record.

    Args:
        tx_id: Transaction ID to update
        edit_comment: Mandatory comment explaining the edit

    Returns:
        Result with version number
    """
    if not edit_comment or not edit_comment.strip():
        return {'success': False, 'error': 'Edit comment is required'}

    with get_db() as conn:
        cursor = conn.cursor()

        # Get current transaction
        cursor.execute("SELECT * FROM transactions WHERE id = ?", (tx_id,))
        current = cursor.fetchone()

        if not current:
            return {'success': False, 'error': 'Transaction not found'}

        # Get next version number
        cursor.execute("""
            SELECT COALESCE(MAX(version), 0) + 1 as next_version
            FROM transaction_versions
            WHERE transaction_id = ?
        """, (tx_id,))
        next_version = cursor.fetchone()[0]

        # Save current state to versions table (before update)
        cursor.execute("""
            INSERT INTO transaction_versions
            (transaction_id, version, tx_date, tx_type, description, amount, units, nav, balance_units, edit_comment, edited_by)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (tx_id, next_version, current['tx_date'], current['tx_type'], current['description'],
              current['amount'], current['units'], current['nav'], current['balance_units'],
              edit_comment, edited_by))

        # Update the transaction
        cursor.execute("""
            UPDATE transactions
            SET tx_date = ?, tx_type = ?, description = ?, amount = ?, units = ?, nav = ?, balance_units = ?
            WHERE id = ?
        """, (tx_date, tx_type, description, amount, units, nav, balance_units, tx_id))

        # Sync holding units from transaction sum
        folio_id = current['folio_id']
        _sync_holding_units(cursor, folio_id)

        return {
            'success': True,
            'version': next_version,
            'message': f'Transaction updated. Version {next_version} saved.'
        }


def _sync_holding_units(cursor, folio_id: int):
    """Recalculate holding units from the sum of active transaction units.

    Updates the holdings table so that units always reflects the actual
    transaction history, not the static CAS import value.
    """
    cursor.execute("""
        SELECT COALESCE(SUM(units), 0) as total_units
        FROM transactions
        WHERE folio_id = ? AND status = 'active'
    """, (folio_id,))
    total = cursor.fetchone()[0]

    cursor.execute("""
        UPDATE holdings SET units = ?, updated_at = CURRENT_TIMESTAMP
        WHERE folio_id = ?
    """, (total, folio_id))

    # Also update current_value if we have a NAV
    cursor.execute("""
        UPDATE holdings SET current_value = units * nav
        WHERE folio_id = ? AND nav > 0
    """, (folio_id,))


def get_transaction_versions(tx_id: int) -> List[dict]:
    """Get all versions of a transaction."""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT * FROM transaction_versions
            WHERE transaction_id = ?
            ORDER BY version DESC
        """, (tx_id,))
        return [dict(row) for row in cursor.fetchall()]


def get_transaction_version_count(tx_id: int) -> int:
    """Get count of versions for a transaction."""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT COUNT(*) FROM transaction_versions WHERE transaction_id = ?
        """, (tx_id,))
        return cursor.fetchone()[0]


def get_transaction_stats(investor_id: int) -> dict:
    """Get transaction statistics for an investor."""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT
                COUNT(*) as total_transactions,
                COUNT(DISTINCT f.id) as total_folios,
                SUM(CASE WHEN t.tx_type IN ('purchase', 'sip') THEN t.amount ELSE 0 END) as total_invested,
                SUM(CASE WHEN t.tx_type = 'redemption' THEN ABS(t.amount) ELSE 0 END) as total_redeemed
            FROM transactions t
            JOIN folios f ON f.id = t.folio_id
            WHERE f.investor_id = ?
        """, (investor_id,))
        row = cursor.fetchone()
        return dict(row) if row else {}
