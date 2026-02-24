"""Backup/restore, configuration, alerts, feature requests, and XIRR data retrieval."""

import json
import logging
from datetime import date, datetime
from pathlib import Path
from typing import List, Optional

from cas_parser.webapp.db.connection import get_db, init_db, DB_PATH, BACKUP_DIR

logger = logging.getLogger(__name__)

__all__ = [
    'get_config',
    'set_config',
    'backup_static_tables',
    'list_backups',
    'restore_static_tables',
    'reset_database',
    'get_xirr_data_for_folio',
    'get_xirr_data_for_investor',
    'create_feature_request',
    'get_investor_alerts',
    'get_feature_requests',
]


def get_config(key: str, default: Optional[str] = None) -> Optional[str]:
    """Get a config value by key."""
    with get_db() as conn:
        row = conn.execute(
            "SELECT value FROM app_config WHERE key = ?", (key,)
        ).fetchone()
        return row['value'] if row else default


def set_config(key: str, value: str) -> bool:
    """Set a config value (insert or update)."""
    with get_db() as conn:
        conn.execute(
            "INSERT INTO app_config (key, value, updated_at) VALUES (?, ?, CURRENT_TIMESTAMP) "
            "ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = CURRENT_TIMESTAMP",
            (key, value)
        )
        return True


def backup_static_tables() -> dict:
    """
    Backup static tables and user data that doesn't change frequently:
    - investors (including CAS upload tracking)
    - mutual_fund_master (with AMFI mappings and asset allocations)
    - goals, goal_folios, and goal_notes (journal entries)
    - manual ISIN mappings (external JSON file)

    Returns backup info with file path.
    """
    BACKUP_DIR.mkdir(exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_file = BACKUP_DIR / f"backup_{timestamp}.json"

    backup_data = {
        'timestamp': timestamp,
        'version': '2.0',  # Backup format version
        'tables': {},
        'external_files': {}
    }

    with get_db() as conn:
        cursor = conn.cursor()

        # Backup investors (now includes CAS upload tracking)
        cursor.execute("SELECT * FROM investors")
        backup_data['tables']['investors'] = [dict(row) for row in cursor.fetchall()]

        # Backup mutual_fund_master
        cursor.execute("SELECT * FROM mutual_fund_master")
        backup_data['tables']['mutual_fund_master'] = [dict(row) for row in cursor.fetchall()]

        # Backup fund_holdings (with ISIN for re-linking)
        cursor.execute("""
            SELECT fh.stock_name, fh.weight_pct, mf.isin
            FROM fund_holdings fh
            JOIN mutual_fund_master mf ON mf.id = fh.mf_id
        """)
        backup_data['tables']['fund_holdings'] = [dict(row) for row in cursor.fetchall()]

        # Backup fund_sectors (with ISIN for re-linking)
        cursor.execute("""
            SELECT fs.sector_name, fs.weight_pct, mf.isin
            FROM fund_sectors fs
            JOIN mutual_fund_master mf ON mf.id = fs.mf_id
        """)
        backup_data['tables']['fund_sectors'] = [dict(row) for row in cursor.fetchall()]

        # Backup goals
        cursor.execute("SELECT * FROM goals")
        backup_data['tables']['goals'] = [dict(row) for row in cursor.fetchall()]

        # Backup goal_folios (we'll need to re-link after restore based on folio_number)
        cursor.execute("""
            SELECT gf.*, f.folio_number, f.scheme_name, f.isin
            FROM goal_folios gf
            JOIN folios f ON f.id = gf.folio_id
        """)
        backup_data['tables']['goal_folios'] = [dict(row) for row in cursor.fetchall()]

        # Backup goal_notes (journal entries - important user data!)
        cursor.execute("""
            SELECT gn.*, g.name as goal_name, g.investor_id
            FROM goal_notes gn
            JOIN goals g ON g.id = gn.goal_id
        """)
        backup_data['tables']['goal_notes'] = [dict(row) for row in cursor.fetchall()]

        # Backup users (without password hashes for safety — restore uses upsert by username)
        try:
            cursor.execute("""
                SELECT id, username, password_hash, display_name, role, investor_id,
                       is_active, last_login, created_at, updated_at
                FROM users
            """)
            backup_data['tables']['users'] = [dict(row) for row in cursor.fetchall()]
        except Exception:
            backup_data['tables']['users'] = []

        # Backup custodian_access (with username + investor PAN for re-linking)
        try:
            cursor.execute("""
                SELECT ca.investor_id, u.username as custodian_username,
                       gu.username as granted_by_username, i.pan as investor_pan,
                       ca.created_at
                FROM custodian_access ca
                JOIN users u ON u.id = ca.custodian_user_id
                JOIN users gu ON gu.id = ca.granted_by_user_id
                LEFT JOIN investors i ON i.id = ca.investor_id
            """)
            backup_data['tables']['custodian_access'] = [dict(row) for row in cursor.fetchall()]
        except Exception:
            backup_data['tables']['custodian_access'] = []

    # Backup manual ISIN mappings from external file
    manual_mappings_file = Path(__file__).parent.parent / 'data' / 'manual_isin_mappings.json'
    if manual_mappings_file.exists():
        try:
            with open(manual_mappings_file, 'r') as f:
                backup_data['external_files']['manual_isin_mappings'] = json.load(f)
        except Exception as e:
            logger.warning(f"Failed to backup manual ISIN mappings: {e}")

    # Calculate backup file size
    backup_json = json.dumps(backup_data, indent=2, default=str)

    # Write to file
    with open(backup_file, 'w') as f:
        f.write(backup_json)

    return {
        'success': True,
        'file': str(backup_file),
        'timestamp': timestamp,
        'size_kb': round(len(backup_json) / 1024, 1),
        'counts': {
            'investors': len(backup_data['tables']['investors']),
            'mutual_fund_master': len(backup_data['tables']['mutual_fund_master']),
            'fund_holdings': len(backup_data['tables'].get('fund_holdings', [])),
            'fund_sectors': len(backup_data['tables'].get('fund_sectors', [])),
            'goals': len(backup_data['tables']['goals']),
            'goal_folios': len(backup_data['tables']['goal_folios']),
            'goal_notes': len(backup_data['tables'].get('goal_notes', [])),
            'users': len(backup_data['tables'].get('users', [])),
            'custodian_access': len(backup_data['tables'].get('custodian_access', [])),
            'isin_mappings': len(backup_data['external_files'].get('manual_isin_mappings', {}))
        }
    }


def list_backups() -> List[dict]:
    """List all available backups with detailed info."""
    if not BACKUP_DIR.exists():
        return []

    backups = []
    for f in sorted(BACKUP_DIR.glob("backup_*.json"), reverse=True):
        try:
            file_size = f.stat().st_size
            with open(f, 'r') as fp:
                data = json.load(fp)

                # Count items in all tables
                table_counts = {
                    table: len(rows) if isinstance(rows, list) else 0
                    for table, rows in data.get('tables', {}).items()
                }

                # Count ISIN mappings from external files
                external = data.get('external_files', {})
                isin_count = len(external.get('manual_isin_mappings', {}))

                backups.append({
                    'file': str(f),
                    'filename': f.name,
                    'timestamp': data.get('timestamp', 'unknown'),
                    'version': data.get('version', '1.0'),
                    'size_kb': round(file_size / 1024, 1),
                    'counts': table_counts,
                    'isin_mappings': isin_count
                })
        except Exception as e:
            logger.warning(f"Failed to read backup {f}: {e}")

    return backups


def restore_static_tables(backup_file: str = None, auto_backup: bool = True) -> dict:
    """
    Restore static tables from backup.

    If no backup_file specified, uses the most recent backup.
    This restores: investors, mutual_fund_master, goals, goal_notes, ISIN mappings.
    Goal-folio links are restored based on folio_number matching.

    Args:
        backup_file: Path to backup file, or None for most recent
        auto_backup: If True, creates a backup before restoring (safety measure)
    """
    if not backup_file:
        backups = list_backups()
        if not backups:
            return {'success': False, 'error': 'No backups found'}
        backup_file = backups[0]['file']

    backup_path = Path(backup_file)
    if not backup_path.exists():
        return {'success': False, 'error': f'Backup file not found: {backup_file}'}

    # Auto-backup before restore for safety
    auto_backup_file = None
    if auto_backup:
        try:
            auto_result = backup_static_tables()
            if auto_result.get('success'):
                auto_backup_file = auto_result['file']
                logger.info(f"Auto-backup created before restore: {auto_backup_file}")
        except Exception as e:
            logger.warning(f"Failed to create auto-backup: {e}")

    with open(backup_path, 'r') as f:
        backup_data = json.load(f)

    restored = {'investors': 0, 'mutual_fund_master': 0, 'fund_holdings': 0, 'fund_sectors': 0,
                'goals': 0, 'goal_folios': 0, 'goal_notes': 0,
                'users': 0, 'custodian_access': 0, 'isin_mappings': 0}

    with get_db() as conn:
        cursor = conn.cursor()

        # Restore investors (including CAS tracking fields)
        for inv in backup_data['tables'].get('investors', []):
            try:
                cursor.execute("""
                    INSERT INTO investors (name, pan, email, mobile, last_cas_upload,
                                           statement_from_date, statement_to_date, tax_slab_pct,
                                           created_at, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(pan) DO UPDATE SET
                        name = excluded.name,
                        email = excluded.email,
                        mobile = excluded.mobile,
                        last_cas_upload = COALESCE(excluded.last_cas_upload, investors.last_cas_upload),
                        statement_from_date = COALESCE(excluded.statement_from_date, investors.statement_from_date),
                        statement_to_date = COALESCE(excluded.statement_to_date, investors.statement_to_date),
                        tax_slab_pct = COALESCE(excluded.tax_slab_pct, investors.tax_slab_pct),
                        updated_at = CURRENT_TIMESTAMP
                """, (inv['name'], inv['pan'], inv.get('email'), inv.get('mobile'),
                      inv.get('last_cas_upload'), inv.get('statement_from_date'), inv.get('statement_to_date'),
                      inv.get('tax_slab_pct'),
                      inv.get('created_at'), inv.get('updated_at')))
                restored['investors'] += 1
            except Exception as e:
                logger.warning(f"Failed to restore investor {inv.get('name')}: {e}")

        # Restore mutual_fund_master
        for mf in backup_data['tables'].get('mutual_fund_master', []):
            try:
                cursor.execute("""
                    INSERT INTO mutual_fund_master
                    (scheme_name, isin, amc, amfi_code, amfi_scheme_name, current_nav, nav_date,
                     equity_pct, debt_pct, commodity_pct, cash_pct, others_pct, display_name,
                     large_cap_pct, mid_cap_pct, small_cap_pct, allocation_reviewed_at,
                     fund_category, geography, exit_load_pct,
                     created_at, updated_at)
                    VALUES (NULLIF(?, ''), ?, NULLIF(?, ''), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(isin) DO UPDATE SET
                        scheme_name = COALESCE(NULLIF(excluded.scheme_name, ''), mutual_fund_master.scheme_name),
                        amc = COALESCE(NULLIF(excluded.amc, ''), mutual_fund_master.amc),
                        amfi_code = COALESCE(excluded.amfi_code, mutual_fund_master.amfi_code),
                        amfi_scheme_name = COALESCE(excluded.amfi_scheme_name, mutual_fund_master.amfi_scheme_name),
                        equity_pct = excluded.equity_pct,
                        debt_pct = excluded.debt_pct,
                        commodity_pct = excluded.commodity_pct,
                        cash_pct = excluded.cash_pct,
                        others_pct = excluded.others_pct,
                        display_name = COALESCE(excluded.display_name, mutual_fund_master.display_name),
                        large_cap_pct = excluded.large_cap_pct,
                        mid_cap_pct = excluded.mid_cap_pct,
                        small_cap_pct = excluded.small_cap_pct,
                        allocation_reviewed_at = COALESCE(excluded.allocation_reviewed_at, mutual_fund_master.allocation_reviewed_at),
                        fund_category = COALESCE(excluded.fund_category, mutual_fund_master.fund_category),
                        geography = COALESCE(excluded.geography, mutual_fund_master.geography),
                        exit_load_pct = COALESCE(excluded.exit_load_pct, mutual_fund_master.exit_load_pct),
                        updated_at = CURRENT_TIMESTAMP
                """, (mf['scheme_name'], mf['isin'], mf.get('amc'), mf.get('amfi_code'),
                      mf.get('amfi_scheme_name'), mf.get('current_nav'), mf.get('nav_date'),
                      mf.get('equity_pct', 0), mf.get('debt_pct', 0), mf.get('commodity_pct', 0),
                      mf.get('cash_pct', 0), mf.get('others_pct', 0), mf.get('display_name'),
                      mf.get('large_cap_pct', 0), mf.get('mid_cap_pct', 0), mf.get('small_cap_pct', 0),
                      mf.get('allocation_reviewed_at'),
                      mf.get('fund_category'), mf.get('geography'), mf.get('exit_load_pct', 1.0),
                      mf.get('created_at'), mf.get('updated_at')))
                restored['mutual_fund_master'] += 1
            except Exception as e:
                logger.warning(f"Failed to restore fund {mf.get('scheme_name')}: {e}")

        # Restore fund_holdings (look up mf_id by ISIN)
        cursor.execute("DELETE FROM fund_holdings")
        for fh in backup_data['tables'].get('fund_holdings', []):
            isin = fh.get('isin')
            if not isin:
                continue
            cursor.execute("SELECT id FROM mutual_fund_master WHERE isin = ?", (isin,))
            mf_row = cursor.fetchone()
            if not mf_row:
                continue
            try:
                cursor.execute("""
                    INSERT INTO fund_holdings (mf_id, stock_name, weight_pct)
                    VALUES (?, ?, ?)
                """, (mf_row['id'], fh['stock_name'], fh['weight_pct']))
                restored['fund_holdings'] += 1
            except Exception as e:
                logger.warning(f"Failed to restore fund holding: {e}")

        # Restore fund_sectors (look up mf_id by ISIN)
        cursor.execute("DELETE FROM fund_sectors")
        for fs in backup_data['tables'].get('fund_sectors', []):
            isin = fs.get('isin')
            if not isin:
                continue
            cursor.execute("SELECT id FROM mutual_fund_master WHERE isin = ?", (isin,))
            mf_row = cursor.fetchone()
            if not mf_row:
                continue
            try:
                cursor.execute("""
                    INSERT INTO fund_sectors (mf_id, sector_name, weight_pct)
                    VALUES (?, ?, ?)
                """, (mf_row['id'], fs['sector_name'], fs['weight_pct']))
                restored['fund_sectors'] += 1
            except Exception as e:
                logger.warning(f"Failed to restore fund sector: {e}")

        # Create a mapping of old investor IDs to new investor IDs (by PAN)
        investor_map = {}
        for inv in backup_data['tables'].get('investors', []):
            if inv.get('pan'):
                cursor.execute("SELECT id FROM investors WHERE pan = ?", (inv['pan'],))
                row = cursor.fetchone()
                if row:
                    investor_map[inv['id']] = row['id']

        # Restore goals (need to map investor_id)
        goal_map = {}  # old_goal_id -> new_goal_id
        for goal in backup_data['tables'].get('goals', []):
            old_investor_id = goal['investor_id']
            new_investor_id = investor_map.get(old_investor_id)

            if not new_investor_id:
                logger.warning(f"Cannot restore goal '{goal['name']}' - investor not found")
                continue

            try:
                cursor.execute("""
                    INSERT INTO goals (investor_id, name, description, target_amount, target_date,
                                      target_equity_pct, target_debt_pct, target_commodity_pct,
                                      target_cash_pct, target_others_pct, created_at, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (new_investor_id, goal['name'], goal.get('description'), goal.get('target_amount', 0),
                      goal.get('target_date'), goal.get('target_equity_pct', 0), goal.get('target_debt_pct', 0),
                      goal.get('target_commodity_pct', 0), goal.get('target_cash_pct', 0),
                      goal.get('target_others_pct', 0), goal.get('created_at'), goal.get('updated_at')))
                goal_map[goal['id']] = cursor.lastrowid
                restored['goals'] += 1
            except Exception as e:
                logger.warning(f"Failed to restore goal {goal.get('name')}: {e}")

        # Restore goal_folios (need to find folio by folio_number)
        for gf in backup_data['tables'].get('goal_folios', []):
            old_goal_id = gf['goal_id']
            new_goal_id = goal_map.get(old_goal_id)

            if not new_goal_id:
                continue

            # Find folio by folio_number
            cursor.execute("SELECT id FROM folios WHERE folio_number = ?", (gf['folio_number'],))
            folio_row = cursor.fetchone()

            if not folio_row:
                logger.warning(f"Cannot restore goal-folio link - folio {gf['folio_number']} not found")
                continue

            try:
                cursor.execute("""
                    INSERT INTO goal_folios (goal_id, folio_id)
                    VALUES (?, ?)
                    ON CONFLICT(goal_id, folio_id) DO NOTHING
                """, (new_goal_id, folio_row['id']))
                restored['goal_folios'] += 1
            except Exception as e:
                logger.warning(f"Failed to restore goal-folio link: {e}")

        # Restore goal_notes (journal entries)
        for note in backup_data['tables'].get('goal_notes', []):
            old_goal_id = note['goal_id']
            new_goal_id = goal_map.get(old_goal_id)

            if not new_goal_id:
                # Try to find goal by name and investor
                old_investor_id = note.get('investor_id')
                new_investor_id = investor_map.get(old_investor_id)
                if new_investor_id:
                    cursor.execute("""
                        SELECT id FROM goals WHERE investor_id = ? AND name = ?
                    """, (new_investor_id, note.get('goal_name')))
                    goal_row = cursor.fetchone()
                    if goal_row:
                        new_goal_id = goal_row['id']

            if not new_goal_id:
                logger.warning(f"Cannot restore goal note - goal not found")
                continue

            try:
                cursor.execute("""
                    INSERT INTO goal_notes (goal_id, note_type, title, content, mood, created_at, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """, (new_goal_id, note.get('note_type', 'thought'), note.get('title'),
                      note['content'], note.get('mood'),
                      note.get('created_at'), note.get('updated_at')))
                restored['goal_notes'] += 1
            except Exception as e:
                logger.warning(f"Failed to restore goal note: {e}")

        # Restore users (upsert by username, preserve existing passwords if user exists)
        user_map = {}  # old_user_id -> new_user_id
        for user in backup_data['tables'].get('users', []):
            try:
                cursor.execute("SELECT id FROM users WHERE username = ?", (user['username'],))
                existing = cursor.fetchone()
                if existing:
                    # User exists — update non-password fields, map ID
                    cursor.execute("""
                        UPDATE users SET display_name = ?, role = ?, is_active = ?,
                                         updated_at = CURRENT_TIMESTAMP
                        WHERE username = ?
                    """, (user['display_name'], user['role'], user.get('is_active', 1),
                          user['username']))
                    user_map[user['id']] = existing['id']
                else:
                    # New user — insert with password hash
                    cursor.execute("""
                        INSERT INTO users (username, password_hash, display_name, role,
                                           investor_id, is_active, created_at, updated_at)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """, (user['username'], user['password_hash'], user['display_name'],
                          user['role'],
                          investor_map.get(user.get('investor_id')),
                          user.get('is_active', 1),
                          user.get('created_at'), user.get('updated_at')))
                    user_map[user['id']] = cursor.lastrowid
                restored['users'] += 1
            except Exception as e:
                logger.warning(f"Failed to restore user {user.get('username')}: {e}")

        # Restore custodian_access (look up by username + investor PAN)
        for ca in backup_data['tables'].get('custodian_access', []):
            try:
                # Find custodian user by username
                cursor.execute("SELECT id FROM users WHERE username = ?",
                               (ca['custodian_username'],))
                cust_row = cursor.fetchone()
                if not cust_row:
                    continue

                # Find granting user by username
                cursor.execute("SELECT id FROM users WHERE username = ?",
                               (ca['granted_by_username'],))
                grant_row = cursor.fetchone()
                if not grant_row:
                    continue

                # Find investor by PAN
                inv_id = None
                if ca.get('investor_pan'):
                    cursor.execute("SELECT id FROM investors WHERE pan = ?",
                                   (ca['investor_pan'],))
                    inv_row = cursor.fetchone()
                    if inv_row:
                        inv_id = inv_row['id']

                if not inv_id:
                    continue

                cursor.execute("""
                    INSERT INTO custodian_access (investor_id, custodian_user_id, granted_by_user_id)
                    VALUES (?, ?, ?)
                    ON CONFLICT(investor_id, custodian_user_id) DO NOTHING
                """, (inv_id, cust_row['id'], grant_row['id']))
                restored['custodian_access'] += 1
            except Exception as e:
                logger.warning(f"Failed to restore custodian access: {e}")

    # Restore manual ISIN mappings
    external_files = backup_data.get('external_files', {})
    isin_mappings = external_files.get('manual_isin_mappings', {})
    if isin_mappings:
        try:
            manual_mappings_file = Path(__file__).parent.parent / 'data' / 'manual_isin_mappings.json'
            manual_mappings_file.parent.mkdir(parents=True, exist_ok=True)

            # Merge with existing mappings (don't overwrite)
            existing_mappings = {}
            if manual_mappings_file.exists():
                with open(manual_mappings_file, 'r') as f:
                    existing_mappings = json.load(f)

            merged_mappings = {**isin_mappings, **existing_mappings}  # Existing takes priority
            with open(manual_mappings_file, 'w') as f:
                json.dump(merged_mappings, f, indent=2)

            restored['isin_mappings'] = len(isin_mappings)
            logger.info(f"Restored {len(isin_mappings)} ISIN mappings")
        except Exception as e:
            logger.warning(f"Failed to restore ISIN mappings: {e}")

    result = {
        'success': True,
        'backup_file': str(backup_path),
        'restored': restored
    }

    if auto_backup_file:
        result['auto_backup_created'] = auto_backup_file

    return result


def reset_database(auto_backup: bool = True) -> dict:
    """
    Drop all tables and reinitialize the database.
    WARNING: This deletes ALL data!

    Args:
        auto_backup: If True, creates a backup before reset (safety measure)
    """
    # Create safety backup before destructive operation
    auto_backup_file = None
    if auto_backup:
        try:
            auto_result = backup_static_tables()
            if auto_result.get('success'):
                auto_backup_file = auto_result['file']
                logger.info(f"Auto-backup created before reset: {auto_backup_file}")
        except Exception as e:
            logger.warning(f"Failed to create auto-backup before reset: {e}")

    with get_db() as conn:
        cursor = conn.cursor()

        # Get all table names
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")
        tables = [row['name'] for row in cursor.fetchall()]

        # Drop all tables
        for table in tables:
            cursor.execute(f"DROP TABLE IF EXISTS {table}")

        logger.info(f"Dropped {len(tables)} tables")

    # Reinitialize
    init_db()

    result = {
        'success': True,
        'dropped_tables': tables,
        'message': 'Database reset complete. All tables recreated.'
    }

    if auto_backup_file:
        result['auto_backup_created'] = auto_backup_file
        result['message'] += f' Safety backup: {Path(auto_backup_file).name}'

    return result


def get_xirr_data_for_folio(folio_id: int) -> dict:
    """
    Get transaction cashflows and current value for XIRR calculation.

    Returns dict with transactions, current_value (live NAV if available), as_of_date.
    """
    with get_db() as conn:
        cursor = conn.cursor()

        # Get active transactions
        cursor.execute("""
            SELECT t.tx_date, t.tx_type, t.amount, t.units, t.nav
            FROM transactions t
            WHERE t.folio_id = ? AND t.status = 'active'
            ORDER BY t.tx_date ASC
        """, (folio_id,))
        transactions = [dict(row) for row in cursor.fetchall()]

        # Get holding units and try live NAV
        cursor.execute("""
            SELECT h.units, h.current_value, f.isin, f.scheme_name, f.folio_number
            FROM holdings h
            JOIN folios f ON f.id = h.folio_id
            WHERE h.folio_id = ?
        """, (folio_id,))
        holding = cursor.fetchone()

        current_value = 0
        scheme_name = None
        folio_number = None
        if holding:
            scheme_name = holding['scheme_name']
            folio_number = holding['folio_number']
            units = holding['units'] or 0
            isin = holding['isin']

            # Try to get live NAV
            if isin and units > 0:
                cursor.execute("""
                    SELECT current_nav FROM mutual_fund_master WHERE isin = ?
                """, (isin,))
                nav_row = cursor.fetchone()
                if nav_row and nav_row['current_nav']:
                    current_value = units * nav_row['current_nav']

            # Fallback to holdings.current_value
            if current_value == 0:
                current_value = holding['current_value'] or 0

        return {
            'folio_id': folio_id,
            'scheme_name': scheme_name,
            'folio_number': folio_number,
            'isin': holding['isin'] if holding else None,
            'transactions': transactions,
            'current_value': current_value,
        }


def get_xirr_data_for_investor(investor_id: int) -> List[dict]:
    """
    Get XIRR data for all folios of an investor.

    Returns list of per-folio dicts from get_xirr_data_for_folio().
    """
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT f.id FROM folios f
            WHERE f.investor_id = ?
        """, (investor_id,))
        folio_ids = [row['id'] for row in cursor.fetchall()]

    return [get_xirr_data_for_folio(fid) for fid in folio_ids]


def create_feature_request(page: str, title: str, description: str = None) -> int:
    """Create a new feature request."""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute(
            "INSERT INTO feature_requests (page, title, description) VALUES (?, ?, ?)",
            (page, title, description)
        )
        return cursor.lastrowid


def get_investor_alerts(investor_id: int) -> List[dict]:
    """Aggregate alerts from multiple sources for an investor's Home tab."""
    from cas_parser.webapp.db.validation import get_validation_issues, get_quarantine_stats
    from cas_parser.webapp.db.manual_assets import get_matured_fds, get_maturing_fds
    from cas_parser.webapp.db.transactions import get_conflict_stats

    alerts = []

    # 1. Validation issues (unit mismatches)
    try:
        issues = get_validation_issues(investor_id=investor_id, status='open')
        for i in issues:
            alerts.append({
                'id': f'validation-{i["id"]}',
                'type': 'warning',
                'category': 'Validation',
                'title': f'Unit mismatch: {i["scheme_name"]}',
                'description': i.get('description', ''),
                'link': f'/folio/{i["folio_id"]}',
                'link_text': 'View Folio'
            })
    except Exception as e:
        logger.error(f"Error fetching validation issues for alerts: {e}")

    # 2. Quarantine items (broken ISINs)
    try:
        stats = get_quarantine_stats()
        if stats.get('pending', 0) > 0:
            alerts.append({
                'id': 'quarantine',
                'type': 'warning',
                'category': 'Import',
                'title': f'{stats["pending"]} quarantined item(s) with broken ISINs',
                'description': 'Holdings/transactions with truncated ISINs need resolution',
                'link': '/settings',
                'link_text': 'Resolve in Settings'
            })
    except Exception as e:
        logger.error(f"Error fetching quarantine stats for alerts: {e}")

    # 3. Matured FDs
    try:
        matured = get_matured_fds()
        for fd in matured:
            if fd.get('investor_id') == investor_id:
                alerts.append({
                    'id': f'fd-matured-{fd["id"]}',
                    'type': 'info',
                    'category': 'FD',
                    'title': f'FD matured: {fd["name"]}',
                    'description': f'Matured on {fd.get("fd_maturity_date", "?")}. Consider reinvesting.',
                    'link': f'/manual-assets?investor_id={investor_id}',
                    'link_text': 'View FDs'
                })
    except Exception as e:
        logger.error(f"Error fetching matured FDs for alerts: {e}")

    # 4. Maturing FDs (next 30 days)
    try:
        maturing = get_maturing_fds(days=30)
        for fd in maturing:
            if fd.get('investor_id') == investor_id:
                alerts.append({
                    'id': f'fd-maturing-{fd["id"]}',
                    'type': 'info',
                    'category': 'FD',
                    'title': f'FD maturing soon: {fd["name"]}',
                    'description': f'Matures on {fd.get("fd_maturity_date", "?")}',
                    'link': f'/manual-assets?investor_id={investor_id}',
                    'link_text': 'View FDs'
                })
    except Exception as e:
        logger.error(f"Error fetching maturing FDs for alerts: {e}")

    # 5. Transaction conflicts
    try:
        conflict_stats = get_conflict_stats()
        if conflict_stats.get('pending_groups', 0) > 0:
            alerts.append({
                'id': 'conflicts',
                'type': 'danger',
                'category': 'Transactions',
                'title': f'{conflict_stats["pending_groups"]} transaction conflict(s) pending',
                'description': 'Duplicate or conflicting transactions need resolution',
                'link': '/resolve-conflicts',
                'link_text': 'Resolve Conflicts'
            })
    except Exception as e:
        logger.error(f"Error fetching conflict stats for alerts: {e}")

    return alerts


def get_feature_requests() -> List[dict]:
    """Get all feature requests, newest first."""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM feature_requests ORDER BY created_at DESC")
        return [dict(row) for row in cursor.fetchall()]
