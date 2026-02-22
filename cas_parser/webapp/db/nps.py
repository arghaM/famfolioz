"""NPS (National Pension System) subscriber, scheme, and transaction management."""

import hashlib
import logging
from datetime import date, datetime
from typing import List, Optional
from cas_parser.webapp.db.connection import get_db

logger = logging.getLogger(__name__)

__all__ = [
    "generate_nps_tx_hash",
    "get_or_create_nps_subscriber",
    "get_nps_subscriber",
    "get_nps_subscribers_by_investor",
    "get_all_nps_subscribers",
    "upsert_nps_scheme",
    "get_nps_schemes",
    "insert_nps_transaction",
    "get_nps_transactions",
    "get_nps_transactions_by_scheme",
    "get_nps_transactions_by_contribution",
    "get_nps_portfolio_summary",
    "update_nps_statement_info",
    "save_nps_nav",
    "get_nps_nav_history",
    "get_latest_nps_nav",
    "update_nps_transaction_notes",
    "get_nps_transaction",
    "import_nps_statement",
    "get_unmapped_nps_subscribers",
    "link_nps_to_investor",
    "unlink_nps_from_investor",
]


def generate_nps_tx_hash(pran: str, tx_date: str, scheme_type: str,
                          amount: float, units: float) -> str:
    """
    Generate a deterministic hash for NPS transaction deduplication.

    Args:
        pran: PRAN number
        tx_date: Transaction date string
        scheme_type: Scheme type (E, C, G, A)
        amount: Contribution amount
        units: Units allotted

    Returns:
        MD5 hash string for the transaction
    """
    data = f"{pran}|{tx_date}|{scheme_type}|{amount:.2f}|{units:.4f}"
    return hashlib.md5(data.encode()).hexdigest()


def get_or_create_nps_subscriber(pran: str, name: str, investor_id: int = None,
                                   **kwargs) -> int:
    """
    Get existing NPS subscriber or create new one.

    Args:
        pran: PRAN number (12 digits)
        name: Subscriber name
        investor_id: Optional link to investor
        **kwargs: Additional fields (pan, dob, email, mobile, employer_name)

    Returns:
        Subscriber ID
    """
    with get_db() as conn:
        cursor = conn.cursor()

        # Check if subscriber exists
        cursor.execute("SELECT id FROM nps_subscribers WHERE pran = ?", (pran,))
        existing = cursor.fetchone()

        if existing:
            # Update existing subscriber
            updates = ["name = ?", "updated_at = CURRENT_TIMESTAMP"]
            values = [name]

            if investor_id:
                updates.append("investor_id = ?")
                values.append(investor_id)

            for field in ['pan', 'email', 'mobile', 'employer_name']:
                if kwargs.get(field):
                    updates.append(f"{field} = ?")
                    values.append(kwargs[field])

            if kwargs.get('dob'):
                updates.append("dob = ?")
                values.append(kwargs['dob'])

            values.append(existing['id'])
            cursor.execute(f"""
                UPDATE nps_subscribers SET {', '.join(updates)} WHERE id = ?
            """, values)

            return existing['id']

        # Insert new subscriber
        fields = ['pran', 'name']
        values = [pran, name]

        if investor_id:
            fields.append('investor_id')
            values.append(investor_id)

        for field in ['pan', 'email', 'mobile', 'employer_name', 'dob']:
            if kwargs.get(field):
                fields.append(field)
                values.append(kwargs[field])

        placeholders = ', '.join(['?' for _ in fields])
        cursor.execute(f"""
            INSERT INTO nps_subscribers ({', '.join(fields)})
            VALUES ({placeholders})
        """, values)

        return cursor.lastrowid


def get_nps_subscriber(subscriber_id: int = None, pran: str = None) -> Optional[dict]:
    """Get NPS subscriber by ID or PRAN."""
    with get_db() as conn:
        cursor = conn.cursor()

        if subscriber_id:
            cursor.execute("SELECT * FROM nps_subscribers WHERE id = ?", (subscriber_id,))
        elif pran:
            cursor.execute("SELECT * FROM nps_subscribers WHERE pran = ?", (pran,))
        else:
            return None

        row = cursor.fetchone()
        return dict(row) if row else None


def get_nps_subscribers_by_investor(investor_id: int) -> List[dict]:
    """Get all NPS subscribers linked to an investor."""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT * FROM nps_subscribers WHERE investor_id = ?
            ORDER BY name
        """, (investor_id,))
        return [dict(row) for row in cursor.fetchall()]


def get_all_nps_subscribers() -> List[dict]:
    """Get all NPS subscribers."""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM nps_subscribers ORDER BY name")
        return [dict(row) for row in cursor.fetchall()]


def upsert_nps_scheme(subscriber_id: int, scheme_name: str, pfm_name: str,
                       scheme_type: str, tier: str, units: float, nav: float,
                       nav_date: str, current_value: float) -> int:
    """
    Insert or update NPS scheme holding.

    Returns scheme ID.
    """
    with get_db() as conn:
        cursor = conn.cursor()

        cursor.execute("""
            INSERT INTO nps_schemes
            (subscriber_id, scheme_name, pfm_name, scheme_type, tier, units, nav, nav_date, current_value)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(subscriber_id, scheme_type, tier) DO UPDATE SET
                scheme_name = excluded.scheme_name,
                pfm_name = excluded.pfm_name,
                units = excluded.units,
                nav = excluded.nav,
                nav_date = excluded.nav_date,
                current_value = excluded.current_value,
                updated_at = CURRENT_TIMESTAMP
        """, (subscriber_id, scheme_name, pfm_name, scheme_type, tier,
              units, nav, nav_date, current_value))

        cursor.execute("""
            SELECT id FROM nps_schemes
            WHERE subscriber_id = ? AND scheme_type = ? AND tier = ?
        """, (subscriber_id, scheme_type, tier))
        row = cursor.fetchone()
        return row['id'] if row else 0


def get_nps_schemes(subscriber_id: int) -> List[dict]:
    """Get all NPS schemes for a subscriber."""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT * FROM nps_schemes WHERE subscriber_id = ?
            ORDER BY tier, scheme_type
        """, (subscriber_id,))
        return [dict(row) for row in cursor.fetchall()]


def insert_nps_transaction(subscriber_id: int, pran: str, tx_date: str,
                            contribution_type: str, scheme_type: str,
                            pfm_name: str, amount: float, units: float,
                            nav: float, description: str = "",
                            tier: str = "I") -> dict:
    """
    Insert NPS transaction with idempotency check.

    Returns dict with 'inserted' (bool) and 'id' (int).
    """
    # Generate deterministic hash
    tx_hash = generate_nps_tx_hash(pran, tx_date, scheme_type, amount, units)

    with get_db() as conn:
        cursor = conn.cursor()

        # Check if transaction already exists
        cursor.execute("""
            SELECT id FROM nps_transactions WHERE tx_hash = ?
        """, (tx_hash,))
        existing = cursor.fetchone()

        if existing:
            return {'inserted': False, 'id': existing['id'], 'duplicate': True}

        # Insert new transaction
        cursor.execute("""
            INSERT INTO nps_transactions
            (subscriber_id, tx_hash, tx_date, contribution_type, scheme_type,
             pfm_name, amount, units, nav, description, tier)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (subscriber_id, tx_hash, tx_date, contribution_type, scheme_type,
              pfm_name, amount, units, nav, description, tier))

        return {'inserted': True, 'id': cursor.lastrowid, 'duplicate': False}


def get_nps_transactions(subscriber_id: int, limit: int = 100,
                          offset: int = 0) -> List[dict]:
    """Get NPS transactions for a subscriber."""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT * FROM nps_transactions
            WHERE subscriber_id = ? AND status = 'active'
            ORDER BY tx_date DESC
            LIMIT ? OFFSET ?
        """, (subscriber_id, limit, offset))
        return [dict(row) for row in cursor.fetchall()]


def get_nps_transactions_by_scheme(subscriber_id: int, scheme_type: str) -> List[dict]:
    """Get NPS transactions for a specific scheme type."""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT * FROM nps_transactions
            WHERE subscriber_id = ? AND scheme_type = ? AND status = 'active'
            ORDER BY tx_date DESC
        """, (subscriber_id, scheme_type))
        return [dict(row) for row in cursor.fetchall()]


def get_nps_transactions_by_contribution(subscriber_id: int,
                                           contribution_type: str) -> List[dict]:
    """Get NPS transactions for a specific contribution type."""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT * FROM nps_transactions
            WHERE subscriber_id = ? AND contribution_type = ? AND status = 'active'
            ORDER BY tx_date DESC
        """, (subscriber_id, contribution_type))
        return [dict(row) for row in cursor.fetchall()]


def get_nps_portfolio_summary(subscriber_id: int) -> dict:
    """
    Get NPS portfolio summary for a subscriber.

    Returns total value, contribution breakdown, scheme-wise allocation.
    """
    with get_db() as conn:
        cursor = conn.cursor()

        # Get subscriber info including stored total_value
        cursor.execute("""
            SELECT total_value FROM nps_subscribers WHERE id = ?
        """, (subscriber_id,))
        subscriber_row = cursor.fetchone()
        stored_total_value = subscriber_row['total_value'] if subscriber_row else 0

        # Get schemes
        cursor.execute("""
            SELECT * FROM nps_schemes WHERE subscriber_id = ?
        """, (subscriber_id,))
        schemes = [dict(row) for row in cursor.fetchall()]

        # Get transaction totals by contribution type
        cursor.execute("""
            SELECT contribution_type, SUM(amount) as total_amount
            FROM nps_transactions
            WHERE subscriber_id = ? AND status = 'active'
            GROUP BY contribution_type
        """, (subscriber_id,))
        contrib_totals = {row['contribution_type']: row['total_amount']
                          for row in cursor.fetchall()}

        # Calculate totals - prefer scheme sum, fallback to stored total_value
        schemes_total = sum(s['current_value'] for s in schemes)
        total_value = schemes_total if schemes_total > 0 else (stored_total_value or 0)
        total_contribution = sum(contrib_totals.values())

        # Scheme allocation
        scheme_allocation = {}
        for s in schemes:
            key = f"{s['tier']}_{s['scheme_type']}"
            scheme_allocation[key] = {
                'units': s['units'],
                'nav': s['nav'],
                'value': s['current_value'],
                'pct': round(s['current_value'] / total_value * 100, 2) if total_value > 0 else 0
            }

        return {
            'total_value': total_value,
            'total_contribution': total_contribution,
            'gain_loss': total_value - total_contribution,
            'gain_loss_pct': round((total_value - total_contribution) / total_contribution * 100, 2) if total_contribution > 0 else 0,
            'contribution_breakdown': contrib_totals,
            'scheme_allocation': scheme_allocation,
            'schemes': schemes
        }


def update_nps_statement_info(subscriber_id: int, statement_from: str = None,
                                statement_to: str = None, total_value: float = None) -> dict:
    """Update NPS statement period info."""
    with get_db() as conn:
        cursor = conn.cursor()

        updates = ["last_statement_upload = CURRENT_TIMESTAMP"]
        values = []

        if statement_from:
            updates.append("statement_from_date = ?")
            values.append(statement_from)
        if statement_to:
            updates.append("statement_to_date = ?")
            values.append(statement_to)
        if total_value is not None and total_value > 0:
            updates.append("total_value = ?")
            values.append(total_value)

        values.append(subscriber_id)
        cursor.execute(f"""
            UPDATE nps_subscribers SET {', '.join(updates)}
            WHERE id = ?
        """, values)

        return {'success': cursor.rowcount > 0}


def save_nps_nav(pfm_name: str, scheme_type: str, nav_date: str, nav: float) -> bool:
    """Save NPS NAV to history."""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO nps_nav_history (pfm_name, scheme_type, nav_date, nav)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(pfm_name, scheme_type, nav_date) DO UPDATE SET nav = excluded.nav
        """, (pfm_name, scheme_type, nav_date, nav))
        return cursor.rowcount > 0


def get_nps_nav_history(pfm_name: str, scheme_type: str,
                         start_date: str = None, end_date: str = None) -> List[dict]:
    """Get NPS NAV history for a scheme."""
    with get_db() as conn:
        cursor = conn.cursor()

        query = """
            SELECT * FROM nps_nav_history
            WHERE pfm_name = ? AND scheme_type = ?
        """
        params = [pfm_name, scheme_type]

        if start_date:
            query += " AND nav_date >= ?"
            params.append(start_date)
        if end_date:
            query += " AND nav_date <= ?"
            params.append(end_date)

        query += " ORDER BY nav_date DESC"
        cursor.execute(query, params)
        return [dict(row) for row in cursor.fetchall()]


def get_latest_nps_nav(pfm_name: str, scheme_type: str) -> Optional[dict]:
    """Get the latest NAV for an NPS scheme."""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT * FROM nps_nav_history
            WHERE pfm_name = ? AND scheme_type = ?
            ORDER BY nav_date DESC LIMIT 1
        """, (pfm_name, scheme_type))
        row = cursor.fetchone()
        return dict(row) if row else None


def update_nps_transaction_notes(transaction_id: int, notes: str) -> dict:
    """Update notes for an NPS transaction."""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            UPDATE nps_transactions SET notes = ? WHERE id = ?
        """, (notes, transaction_id))
        return {'success': cursor.rowcount > 0}


def get_nps_transaction(transaction_id: int) -> Optional[dict]:
    """Get a single NPS transaction by ID."""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM nps_transactions WHERE id = ?", (transaction_id,))
        row = cursor.fetchone()
        return dict(row) if row else None


def import_nps_statement(statement_data: dict, investor_id: int = None) -> dict:
    """
    Import parsed NPS statement into database.

    Args:
        statement_data: Parsed NPS statement dict from nps_parser
        investor_id: Optional investor to link the subscriber to

    Returns:
        Import result with counts
    """
    subscriber_data = statement_data.get('subscriber', {})

    # Create/update subscriber
    subscriber_id = get_or_create_nps_subscriber(
        pran=subscriber_data.get('pran', ''),
        name=subscriber_data.get('name', ''),
        investor_id=investor_id,
        pan=subscriber_data.get('pan'),
        email=subscriber_data.get('email'),
        mobile=subscriber_data.get('mobile'),
        employer_name=subscriber_data.get('employer_name'),
        dob=subscriber_data.get('dob')
    )

    # Get total value from statement data
    total_value = float(statement_data.get('total_value', 0))
    if total_value == 0:
        # Calculate from schemes
        total_value = sum(float(s.get('current_value', 0)) for s in statement_data.get('schemes', []))

    # Update statement period and total value
    update_nps_statement_info(
        subscriber_id,
        statement_from=statement_data.get('statement_from_date'),
        statement_to=statement_data.get('statement_to_date'),
        total_value=total_value
    )

    # Import schemes
    schemes_imported = 0
    for scheme in statement_data.get('schemes', []):
        upsert_nps_scheme(
            subscriber_id=subscriber_id,
            scheme_name=scheme.get('scheme_name', ''),
            pfm_name=scheme.get('pfm_name', ''),
            scheme_type=scheme.get('scheme_type', ''),
            tier=scheme.get('tier', 'I'),
            units=float(scheme.get('units', 0)),
            nav=float(scheme.get('nav', 0)),
            nav_date=scheme.get('nav_date', ''),
            current_value=float(scheme.get('current_value', 0))
        )
        schemes_imported += 1

    # Import transactions
    transactions_imported = 0
    transactions_skipped = 0
    pran = subscriber_data.get('pran', '')

    for tx in statement_data.get('transactions', []):
        result = insert_nps_transaction(
            subscriber_id=subscriber_id,
            pran=pran,
            tx_date=tx.get('date', ''),
            contribution_type=tx.get('contribution_type', 'unknown'),
            scheme_type=tx.get('scheme_type', ''),
            pfm_name=tx.get('pfm_name', ''),
            amount=float(tx.get('amount', 0)),
            units=float(tx.get('units', 0)),
            nav=float(tx.get('nav', 0)),
            description=tx.get('description', ''),
            tier=tx.get('tier', 'I')
        )

        if result['inserted']:
            transactions_imported += 1
        else:
            transactions_skipped += 1

    # total_value was already calculated above before importing schemes

    return {
        'success': True,
        'subscriber_id': subscriber_id,
        'pran': pran,
        'schemes_imported': schemes_imported,
        'transactions_imported': transactions_imported,
        'transactions_skipped': transactions_skipped,
        'total_value': total_value,
        'subscriber_name': subscriber_data.get('name', '')
    }


def get_unmapped_nps_subscribers() -> List[dict]:
    """Get all NPS subscribers not linked to any investor."""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT * FROM nps_subscribers
            WHERE investor_id IS NULL
            ORDER BY name
        """)
        return [dict(row) for row in cursor.fetchall()]


def link_nps_to_investor(pran: str, investor_id: int) -> dict:
    """Link an NPS account (by PRAN) to an investor."""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            UPDATE nps_subscribers SET investor_id = ?, updated_at = CURRENT_TIMESTAMP
            WHERE pran = ?
        """, (investor_id, pran))
        return {'success': cursor.rowcount > 0}


def unlink_nps_from_investor(pran: str) -> dict:
    """Unlink an NPS account from its investor."""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            UPDATE nps_subscribers SET investor_id = NULL, updated_at = CURRENT_TIMESTAMP
            WHERE pran = ?
        """, (pran,))
        return {'success': cursor.rowcount > 0}
