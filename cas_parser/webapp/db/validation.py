"""Data validation, unit continuity checks, and quarantine management."""

import json
import logging
from typing import List, Optional
from cas_parser.webapp.db.connection import get_db

logger = logging.getLogger(__name__)

__all__ = [
    'add_to_quarantine',
    'get_quarantined_items',
    'get_quarantine_summary',
    'resolve_quarantine',
    'delete_quarantine_items',
    'get_quarantine_stats',
    'validate_folio_units',
    'validate_investor_folios',
    'validate_all_folios',
    'save_validation_issue',
    'get_validation_issues',
    'resolve_validation_issue',
    'run_post_import_validation',
]


def add_to_quarantine(partial_isin: str, scheme_name: str, amc: str,
                      folio_number: str, data_type: str, data: dict,
                      import_batch_id: str = None,
                      source_filename: str = None) -> int:
    """
    Add a holding or transaction with broken ISIN to quarantine.

    Args:
        partial_isin: The truncated/broken ISIN from PDF
        scheme_name: Scheme name from PDF
        amc: AMC name
        folio_number: Folio number
        data_type: 'holding' or 'transaction'
        data: The full data dict (holding or transaction)
        import_batch_id: Optional batch ID for grouping
        source_filename: Original PDF filename that caused the quarantine

    Returns:
        The quarantine record ID
    """
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO quarantine (partial_isin, scheme_name, amc, folio_number,
                                   data_type, data_json, import_batch_id, source_filename)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (partial_isin, scheme_name, amc, folio_number, data_type,
              json.dumps(data), import_batch_id, source_filename))
        return cursor.lastrowid


def get_quarantined_items(status: str = 'pending') -> List[dict]:
    """Get all quarantined items, grouped by partial ISIN."""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT id, partial_isin, scheme_name, amc, folio_number,
                   data_type, data_json, status, import_batch_id,
                   source_filename, created_at
            FROM quarantine
            WHERE status = ?
            ORDER BY created_at DESC
        """, (status,))
        return [dict(row) for row in cursor.fetchall()]


def get_quarantine_summary() -> List[dict]:
    """Get summary of quarantined items grouped by partial ISIN."""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT
                partial_isin,
                scheme_name,
                amc,
                COUNT(*) as item_count,
                SUM(CASE WHEN data_type = 'holding' THEN 1 ELSE 0 END) as holdings_count,
                SUM(CASE WHEN data_type = 'transaction' THEN 1 ELSE 0 END) as transactions_count,
                MIN(created_at) as first_seen,
                GROUP_CONCAT(DISTINCT source_filename) as source_files
            FROM quarantine
            WHERE status = 'pending'
            GROUP BY partial_isin, scheme_name
            ORDER BY first_seen DESC
        """)
        return [dict(row) for row in cursor.fetchall()]


def resolve_quarantine(partial_isin: str, resolved_isin: str, scheme_name: str = None) -> dict:
    """
    Resolve quarantined items by providing the correct ISIN.
    This imports the quarantined holdings and transactions with the correct ISIN.

    Args:
        partial_isin: The broken ISIN that was quarantined (can be empty)
        resolved_isin: The correct full ISIN
        scheme_name: Used as fallback identifier when partial_isin is empty

    Returns:
        Result dict with counts of imported items
    """
    from cas_parser.webapp.db.mutual_funds import add_to_mutual_fund_master

    if not resolved_isin or len(resolved_isin) != 12 or not resolved_isin.startswith('INF'):
        return {'success': False, 'error': 'Invalid ISIN format'}

    with get_db() as conn:
        cursor = conn.cursor()

        # Get all pending items - use partial_isin if available, else match by scheme_name
        if partial_isin:
            cursor.execute("""
                SELECT id, scheme_name, amc, folio_number, data_type, data_json
                FROM quarantine
                WHERE partial_isin = ? AND status = 'pending'
            """, (partial_isin,))
        elif scheme_name:
            cursor.execute("""
                SELECT id, scheme_name, amc, folio_number, data_type, data_json
                FROM quarantine
                WHERE scheme_name = ? AND status = 'pending'
            """, (scheme_name,))
        else:
            return {'success': False, 'error': 'Either partial_isin or scheme_name is required'}

        items = cursor.fetchall()
        if not items:
            return {'success': False, 'error': 'No pending items found'}

        holdings_imported = 0
        transactions_imported = 0

        # Get or create the scheme in mutual_fund_master
        item_scheme_name = items[0]['scheme_name']
        amc = items[0]['amc']
        add_to_mutual_fund_master(item_scheme_name, resolved_isin, amc)

        # CRITICAL: Add manual ISIN mapping so future imports use this ISIN
        # This teaches the parser to use the correct ISIN when it sees this scheme again
        try:
            from cas_parser.isin_resolver import add_manual_isin_mapping
            # Create a mapping pattern from the scheme name (first 30 chars should be unique enough)
            mapping_pattern = item_scheme_name[:50] if item_scheme_name else partial_isin
            if mapping_pattern:
                add_manual_isin_mapping(mapping_pattern, resolved_isin)
                logger.info(f"Added manual ISIN mapping: '{mapping_pattern}' -> {resolved_isin}")
        except Exception as e:
            logger.warning(f"Could not add manual ISIN mapping: {e}")

        item_ids = []
        for item in items:
            item_ids.append(item['id'])
            data = json.loads(item['data_json'])
            data['isin'] = resolved_isin  # Replace with correct ISIN

            if item['data_type'] == 'holding':
                holdings_imported += 1
            elif item['data_type'] == 'transaction':
                transactions_imported += 1

        # Mark all as resolved
        placeholders = ','.join('?' * len(item_ids))
        cursor.execute(f"""
            UPDATE quarantine
            SET status = 'resolved', resolved_isin = ?, resolved_at = CURRENT_TIMESTAMP
            WHERE id IN ({placeholders}) AND status = 'pending'
        """, [resolved_isin] + item_ids)

        return {
            'success': True,
            'resolved_isin': resolved_isin,
            'holdings_imported': holdings_imported,
            'transactions_imported': transactions_imported,
            'message': f'Resolved {len(items)} items. Please re-import the CAS PDF to fully import the data.'
        }


def delete_quarantine_items(partial_isin: str, scheme_name: str = None) -> dict:
    """Delete quarantined items for a partial ISIN or scheme_name."""
    with get_db() as conn:
        cursor = conn.cursor()
        if partial_isin:
            cursor.execute("""
                DELETE FROM quarantine WHERE partial_isin = ? AND status = 'pending'
            """, (partial_isin,))
        elif scheme_name:
            cursor.execute("""
                DELETE FROM quarantine WHERE scheme_name = ? AND status = 'pending'
            """, (scheme_name,))
        else:
            return {'success': False, 'error': 'Either partial_isin or scheme_name is required'}
        return {'success': True, 'deleted': cursor.rowcount}


def get_quarantine_stats() -> dict:
    """Get quarantine statistics."""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT
                COUNT(*) as total,
                SUM(CASE WHEN status = 'pending' THEN 1 ELSE 0 END) as pending,
                SUM(CASE WHEN status = 'resolved' THEN 1 ELSE 0 END) as resolved,
                COUNT(DISTINCT partial_isin) as unique_isins
            FROM quarantine
        """)
        row = cursor.fetchone()
        return dict(row) if row else {'total': 0, 'pending': 0, 'resolved': 0, 'unique_isins': 0}


def validate_folio_units(folio_id: int) -> dict:
    """
    Validate that sum of transaction units matches holding units for a folio.

    Returns validation result with detailed mismatch info if any.
    """
    with get_db() as conn:
        cursor = conn.cursor()

        # Get holding units (expected)
        cursor.execute("""
            SELECT h.units, f.scheme_name, f.folio_number, f.isin
            FROM holdings h
            JOIN folios f ON f.id = h.folio_id
            WHERE h.folio_id = ?
        """, (folio_id,))
        holding = cursor.fetchone()

        if not holding:
            return {'valid': True, 'message': 'No holding found'}

        expected_units = holding['units'] or 0

        # Sum active transaction units (calculated)
        cursor.execute("""
            SELECT COALESCE(SUM(units), 0) as total_units,
                   COUNT(*) as tx_count
            FROM transactions
            WHERE folio_id = ? AND status = 'active'
        """, (folio_id,))
        active_result = cursor.fetchone()
        active_units = active_result['total_units'] or 0
        active_tx_count = active_result['tx_count'] or 0

        # Sum pending conflict units
        cursor.execute("""
            SELECT COALESCE(SUM(units), 0) as total_units,
                   COUNT(*) as tx_count
            FROM pending_conflicts
            WHERE folio_id = ?
        """, (folio_id,))
        pending_result = cursor.fetchone()
        pending_units = pending_result['total_units'] or 0
        pending_tx_count = pending_result['tx_count'] or 0

        # Calculate difference
        difference = round(expected_units - active_units, 3)
        difference_with_pending = round(expected_units - (active_units + pending_units), 3)

        # Tolerance for floating point comparison (0.01 units)
        tolerance = 0.01

        result = {
            'folio_id': folio_id,
            'scheme_name': holding['scheme_name'],
            'folio_number': holding['folio_number'],
            'expected_units': round(expected_units, 3),
            'calculated_units': round(active_units, 3),
            'difference': difference,
            'pending_conflict_units': round(pending_units, 3),
            'pending_tx_count': pending_tx_count,
            'active_tx_count': active_tx_count,
            'valid': abs(difference) <= tolerance
        }

        if not result['valid']:
            # Determine issue type and recommendation
            if pending_tx_count > 0 and abs(difference_with_pending) <= tolerance:
                result['issue_type'] = 'pending_conflicts'
                result['description'] = f'Units mismatch by {difference:+.3f}. However, {pending_tx_count} pending conflict transactions ({pending_units:+.3f} units) would resolve this.'
                result['recommendation'] = f'ACCEPT ALL {pending_tx_count} pending transactions in conflict resolution. This will add {pending_units:+.3f} units, matching the expected {expected_units:.3f} units.'
            elif pending_tx_count > 0:
                result['issue_type'] = 'partial_conflict'
                result['description'] = f'Units mismatch by {difference:+.3f}. Pending conflicts ({pending_units:+.3f} units) partially explain this.'
                result['recommendation'] = f'Review the {pending_tx_count} pending conflict transactions. After accepting them, you may still have a discrepancy of {difference_with_pending:+.3f} units.'
            elif difference > 0:
                result['issue_type'] = 'missing_transactions'
                result['description'] = f'Missing {difference:.3f} units. Some purchase transactions may not have been imported.'
                result['recommendation'] = 'Check if there are purchase/SIP transactions missing. Re-import the CAS PDF or manually review the transaction history.'
            else:
                result['issue_type'] = 'extra_transactions'
                result['description'] = f'Extra {abs(difference):.3f} units. There may be duplicate transactions or missing redemptions.'
                result['recommendation'] = 'Check for duplicate purchase transactions or missing redemption transactions.'

        return result


def validate_investor_folios(investor_id: int) -> List[dict]:
    """Validate all folios for an investor and return issues."""
    with get_db() as conn:
        cursor = conn.cursor()

        cursor.execute("""
            SELECT f.id FROM folios f
            WHERE f.investor_id = ?
        """, (investor_id,))

        issues = []
        for row in cursor.fetchall():
            result = validate_folio_units(row['id'])
            if not result.get('valid', True):
                issues.append(result)

        return issues


def validate_all_folios() -> List[dict]:
    """Validate all folios and return issues."""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT id FROM folios")

        issues = []
        for row in cursor.fetchall():
            result = validate_folio_units(row['id'])
            if not result.get('valid', True):
                issues.append(result)

        return issues


def save_validation_issue(folio_id: int, validation_result: dict) -> int:
    """Save a validation issue to the database."""
    with get_db() as conn:
        cursor = conn.cursor()

        # Check if issue already exists for this folio
        cursor.execute("""
            SELECT id FROM validation_issues
            WHERE folio_id = ? AND status = 'open'
        """, (folio_id,))
        existing = cursor.fetchone()

        if existing:
            # Update existing issue
            cursor.execute("""
                UPDATE validation_issues SET
                    issue_type = ?,
                    expected_units = ?,
                    calculated_units = ?,
                    difference = ?,
                    pending_conflict_units = ?,
                    description = ?,
                    recommendation = ?,
                    created_at = CURRENT_TIMESTAMP
                WHERE id = ?
            """, (
                validation_result.get('issue_type', 'unit_mismatch'),
                validation_result.get('expected_units'),
                validation_result.get('calculated_units'),
                validation_result.get('difference'),
                validation_result.get('pending_conflict_units', 0),
                validation_result.get('description'),
                validation_result.get('recommendation'),
                existing['id']
            ))
            return existing['id']
        else:
            # Insert new issue
            cursor.execute("""
                INSERT INTO validation_issues
                (folio_id, issue_type, expected_units, calculated_units, difference,
                 pending_conflict_units, description, recommendation)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                folio_id,
                validation_result.get('issue_type', 'unit_mismatch'),
                validation_result.get('expected_units'),
                validation_result.get('calculated_units'),
                validation_result.get('difference'),
                validation_result.get('pending_conflict_units', 0),
                validation_result.get('description'),
                validation_result.get('recommendation')
            ))
            return cursor.lastrowid


def get_validation_issues(investor_id: int = None, status: str = 'open') -> List[dict]:
    """Get validation issues, optionally filtered by investor and status."""
    with get_db() as conn:
        cursor = conn.cursor()

        if investor_id:
            cursor.execute("""
                SELECT vi.*, f.scheme_name, f.folio_number, f.isin
                FROM validation_issues vi
                JOIN folios f ON f.id = vi.folio_id
                WHERE f.investor_id = ? AND vi.status = ?
                ORDER BY ABS(vi.difference) DESC
            """, (investor_id, status))
        else:
            cursor.execute("""
                SELECT vi.*, f.scheme_name, f.folio_number, f.isin,
                       i.name as investor_name
                FROM validation_issues vi
                JOIN folios f ON f.id = vi.folio_id
                LEFT JOIN investors i ON i.id = f.investor_id
                WHERE vi.status = ?
                ORDER BY ABS(vi.difference) DESC
            """, (status,))

        return [dict(row) for row in cursor.fetchall()]


def resolve_validation_issue(issue_id: int) -> dict:
    """Mark a validation issue as resolved."""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            UPDATE validation_issues
            SET status = 'resolved', resolved_at = CURRENT_TIMESTAMP
            WHERE id = ?
        """, (issue_id,))
        return {'success': cursor.rowcount > 0}


def run_post_import_validation(investor_id: int = None) -> dict:
    """
    Run validation after import and save any issues found.

    Returns summary of validation results.
    """
    if investor_id:
        issues = validate_investor_folios(investor_id)
    else:
        issues = validate_all_folios()

    saved_issues = 0
    for issue in issues:
        save_validation_issue(issue['folio_id'], issue)
        saved_issues += 1

    # Also clear any issues that are now resolved
    with get_db() as conn:
        cursor = conn.cursor()

        # Get all open issues
        cursor.execute("""
            SELECT id, folio_id FROM validation_issues WHERE status = 'open'
        """)
        open_issues = cursor.fetchall()

        resolved_count = 0
        # Re-validate and close if now valid
        for oi in open_issues:
            result = validate_folio_units(oi['folio_id'])
            if result.get('valid', False):
                cursor.execute("""
                    UPDATE validation_issues
                    SET status = 'resolved', resolved_at = CURRENT_TIMESTAMP
                    WHERE id = ?
                """, (oi['id'],))
                resolved_count += 1

    return {
        'issues_found': len(issues),
        'issues_saved': saved_issues,
        'auto_resolved': resolved_count
    }
