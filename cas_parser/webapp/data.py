"""
Database models and operations for CAS Parser.

Uses SQLite for persistence with tables for:
- Investors
- Folios (portfolios)
- Holdings (current state)
- Transactions (historical)
"""

import hashlib
import json
import logging
import sqlite3
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from collections import defaultdict
from typing import List, Optional, Tuple

logger = logging.getLogger(__name__)

# Database file path
DB_PATH = Path(__file__).parent / "data.db"
BACKUP_DIR = Path(__file__).parent / "backups"


def get_connection() -> sqlite3.Connection:
    """Get a database connection with row factory."""
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    return conn


@contextmanager
def get_db():
    """Context manager for database connections."""
    conn = get_connection()
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def init_db():
    """Initialize the database schema."""
    with get_db() as conn:
        cursor = conn.cursor()

        # Investors table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS investors (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                pan TEXT UNIQUE,
                email TEXT,
                mobile TEXT,
                last_cas_upload TIMESTAMP,
                statement_from_date DATE,
                statement_to_date DATE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # Add CAS tracking columns if not exist (migration)
        for col, col_type in [('last_cas_upload', 'TIMESTAMP'),
                               ('statement_from_date', 'DATE'),
                               ('statement_to_date', 'DATE')]:
            try:
                cursor.execute(f"ALTER TABLE investors ADD COLUMN {col} {col_type}")
            except sqlite3.OperationalError:
                pass  # Column already exists

        # Add tax slab column for tax-loss harvesting
        try:
            cursor.execute("ALTER TABLE investors ADD COLUMN tax_slab_pct REAL")
        except sqlite3.OperationalError:
            pass

        # Mutual Fund Master - stores unique schemes with AMFI mapping
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS mutual_fund_master (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                scheme_name TEXT NOT NULL,
                isin TEXT,
                amc TEXT,
                amfi_code TEXT,
                amfi_scheme_name TEXT,
                current_nav REAL,
                nav_date TEXT,
                equity_pct REAL DEFAULT 0,
                debt_pct REAL DEFAULT 0,
                commodity_pct REAL DEFAULT 0,
                cash_pct REAL DEFAULT 0,
                others_pct REAL DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(isin)
            )
        """)

        # Add asset allocation columns if not exist (migration)
        for col in ['equity_pct', 'debt_pct', 'commodity_pct', 'cash_pct', 'others_pct']:
            try:
                cursor.execute(f"ALTER TABLE mutual_fund_master ADD COLUMN {col} REAL DEFAULT 0")
            except sqlite3.OperationalError:
                pass

        # Add display_name column (user-editable name, preserves original scheme_name)
        try:
            cursor.execute("ALTER TABLE mutual_fund_master ADD COLUMN display_name TEXT")
        except sqlite3.OperationalError:
            pass

        # Add market cap columns (apply to equity portion only, sum to 100%)
        for col in ['large_cap_pct', 'mid_cap_pct', 'small_cap_pct']:
            try:
                cursor.execute(f"ALTER TABLE mutual_fund_master ADD COLUMN {col} REAL DEFAULT 0")
            except sqlite3.OperationalError:
                pass

        # Add allocation review timestamp (for 30-day review reminders)
        try:
            cursor.execute("ALTER TABLE mutual_fund_master ADD COLUMN allocation_reviewed_at TIMESTAMP")
        except sqlite3.OperationalError:
            pass

        # Add fund category and geography columns (classification labels)
        for col in ['fund_category', 'geography']:
            try:
                cursor.execute(f"ALTER TABLE mutual_fund_master ADD COLUMN {col} TEXT")
            except sqlite3.OperationalError:
                pass

        # Add exit load column for tax-loss harvesting cost computation
        try:
            cursor.execute("ALTER TABLE mutual_fund_master ADD COLUMN exit_load_pct REAL DEFAULT 1.0")
        except sqlite3.OperationalError:
            pass

        # Index for faster lookups
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_mf_isin ON mutual_fund_master(isin)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_mf_amfi ON mutual_fund_master(amfi_code)")

        # Fund Holdings - individual stocks/assets a fund owns
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS fund_holdings (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                mf_id INTEGER NOT NULL,
                stock_name TEXT NOT NULL,
                weight_pct REAL NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (mf_id) REFERENCES mutual_fund_master(id)
            )
        """)
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_fh_mf_id ON fund_holdings(mf_id)")

        # Fund Sectors - sector allocation
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS fund_sectors (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                mf_id INTEGER NOT NULL,
                sector_name TEXT NOT NULL,
                weight_pct REAL NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (mf_id) REFERENCES mutual_fund_master(id)
            )
        """)
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_fs_mf_id ON fund_sectors(mf_id)")

        # Folios table (links to investor)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS folios (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                folio_number TEXT NOT NULL,
                investor_id INTEGER,
                scheme_name TEXT NOT NULL,
                isin TEXT,
                amc TEXT,
                registrar TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (investor_id) REFERENCES investors(id),
                UNIQUE(folio_number, isin)
            )
        """)

        # Holdings table (current state of each folio)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS holdings (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                folio_id INTEGER NOT NULL,
                units REAL NOT NULL,
                nav REAL NOT NULL,
                nav_date DATE,
                current_value REAL NOT NULL,
                cost_value REAL,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (folio_id) REFERENCES folios(id),
                UNIQUE(folio_id)
            )
        """)

        # Transactions table (with hash for deduplication and conflict resolution)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS transactions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                folio_id INTEGER NOT NULL,
                tx_date DATE NOT NULL,
                tx_type TEXT NOT NULL,
                description TEXT,
                amount REAL,
                units REAL NOT NULL,
                nav REAL,
                balance_units REAL,
                tx_hash TEXT UNIQUE,
                status TEXT DEFAULT 'active',
                conflict_group_id TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (folio_id) REFERENCES folios(id)
            )
        """)

        # Add status column if not exists (migration for existing DBs)
        try:
            cursor.execute("ALTER TABLE transactions ADD COLUMN status TEXT DEFAULT 'active'")
        except sqlite3.OperationalError:
            pass  # Column already exists

        try:
            cursor.execute("ALTER TABLE transactions ADD COLUMN conflict_group_id TEXT")
        except sqlite3.OperationalError:
            pass  # Column already exists

        # Pending conflicts table - stores transactions awaiting user decision
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS pending_conflicts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                conflict_group_id TEXT NOT NULL,
                folio_id INTEGER NOT NULL,
                tx_date DATE NOT NULL,
                tx_type TEXT NOT NULL,
                description TEXT,
                amount REAL,
                units REAL NOT NULL,
                nav REAL,
                balance_units REAL,
                tx_hash TEXT NOT NULL,
                source_file TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (folio_id) REFERENCES folios(id)
            )
        """)

        # NAV history table - stores daily NAV snapshots for historical valuation
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS nav_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                isin TEXT NOT NULL,
                nav_date DATE NOT NULL,
                nav REAL NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(isin, nav_date)
            )
        """)

        # Portfolio valuation snapshots - daily portfolio value per investor
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS portfolio_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                investor_id INTEGER NOT NULL,
                snapshot_date DATE NOT NULL,
                total_value REAL NOT NULL,
                total_invested REAL NOT NULL,
                holdings_count INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (investor_id) REFERENCES investors(id),
                UNIQUE(investor_id, snapshot_date)
            )
        """)

        # Transaction versions table - stores edit history
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS transaction_versions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                transaction_id INTEGER NOT NULL,
                version INTEGER NOT NULL,
                tx_date DATE NOT NULL,
                tx_type TEXT NOT NULL,
                description TEXT,
                amount REAL,
                units REAL NOT NULL,
                nav REAL,
                balance_units REAL,
                edit_comment TEXT NOT NULL,
                edited_by TEXT,
                edited_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (transaction_id) REFERENCES transactions(id)
            )
        """)

        # Goals table - investment goals with target allocation
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS goals (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                investor_id INTEGER NOT NULL,
                name TEXT NOT NULL,
                description TEXT,
                target_amount REAL NOT NULL DEFAULT 0,
                target_date DATE,
                target_equity_pct REAL DEFAULT 0,
                target_debt_pct REAL DEFAULT 0,
                target_commodity_pct REAL DEFAULT 0,
                target_cash_pct REAL DEFAULT 0,
                target_others_pct REAL DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (investor_id) REFERENCES investors(id)
            )
        """)

        # Goal-Folio linking table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS goal_folios (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                goal_id INTEGER NOT NULL,
                folio_id INTEGER NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (goal_id) REFERENCES goals(id) ON DELETE CASCADE,
                FOREIGN KEY (folio_id) REFERENCES folios(id),
                UNIQUE(goal_id, folio_id)
            )
        """)

        # Goal Notes/Journal table - for tracking thoughts over time
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS goal_notes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                goal_id INTEGER NOT NULL,
                note_type TEXT DEFAULT 'thought',
                title TEXT,
                content TEXT NOT NULL,
                mood TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (goal_id) REFERENCES goals(id) ON DELETE CASCADE
            )
        """)

        # Quarantine table - holds data with broken/truncated ISINs
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS quarantine (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                partial_isin TEXT NOT NULL,
                scheme_name TEXT,
                amc TEXT,
                folio_number TEXT,
                data_type TEXT NOT NULL,
                data_json TEXT NOT NULL,
                status TEXT DEFAULT 'pending',
                resolved_isin TEXT,
                import_batch_id TEXT,
                source_filename TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                resolved_at TIMESTAMP
            )
        """)

        # Add source_filename column to quarantine (for existing databases)
        try:
            cursor.execute("ALTER TABLE quarantine ADD COLUMN source_filename TEXT")
        except sqlite3.OperationalError:
            pass

        # Validation issues table - tracks unit mismatches between transactions and holdings
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS validation_issues (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                folio_id INTEGER NOT NULL,
                issue_type TEXT NOT NULL,
                expected_units REAL,
                calculated_units REAL,
                difference REAL,
                pending_conflict_units REAL DEFAULT 0,
                description TEXT,
                recommendation TEXT,
                status TEXT DEFAULT 'open',
                resolved_at TIMESTAMP,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (folio_id) REFERENCES folios(id)
            )
        """)

        # Manual assets table - for non-MF assets like SGB, FD, Stocks, PPF, etc.
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS manual_assets (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                investor_id INTEGER NOT NULL,
                asset_type TEXT NOT NULL,
                asset_class TEXT NOT NULL,
                name TEXT NOT NULL,
                description TEXT,

                -- Common fields
                purchase_date DATE,
                purchase_value REAL,
                units REAL DEFAULT 1,
                current_nav REAL,
                current_value REAL,

                -- FD specific fields
                fd_principal REAL,
                fd_interest_rate REAL,
                fd_tenure_months INTEGER,
                fd_maturity_date DATE,
                fd_compounding TEXT DEFAULT 'quarterly',
                fd_premature_penalty_pct REAL DEFAULT 1.0,
                fd_bank_name TEXT,

                -- SGB specific
                sgb_issue_price REAL,
                sgb_interest_rate REAL DEFAULT 2.5,
                sgb_maturity_date DATE,
                sgb_grams REAL,

                -- Stock specific
                stock_symbol TEXT,
                stock_exchange TEXT,
                stock_quantity REAL,
                stock_avg_price REAL,

                -- PPF/NPS specific
                ppf_account_number TEXT,
                ppf_maturity_date DATE,

                -- Status
                is_active INTEGER DEFAULT 1,
                matured_on DATE,

                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

                FOREIGN KEY (investor_id) REFERENCES investors(id)
            )
        """)

        # NPS Subscribers table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS nps_subscribers (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                investor_id INTEGER,
                pran TEXT UNIQUE NOT NULL,
                name TEXT NOT NULL,
                pan TEXT,
                dob DATE,
                email TEXT,
                mobile TEXT,
                employer_name TEXT,
                total_value REAL DEFAULT 0,
                last_statement_upload TIMESTAMP,
                statement_from_date DATE,
                statement_to_date DATE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (investor_id) REFERENCES investors(id)
            )
        """)

        # Add total_value column if not exists (migration)
        try:
            cursor.execute("ALTER TABLE nps_subscribers ADD COLUMN total_value REAL DEFAULT 0")
        except sqlite3.OperationalError:
            pass  # Column already exists

        # NPS Schemes/Holdings table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS nps_schemes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                subscriber_id INTEGER NOT NULL,
                scheme_name TEXT NOT NULL,
                pfm_name TEXT,
                scheme_type TEXT NOT NULL,
                tier TEXT DEFAULT 'I',
                units REAL NOT NULL,
                nav REAL NOT NULL,
                nav_date DATE,
                current_value REAL NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (subscriber_id) REFERENCES nps_subscribers(id),
                UNIQUE(subscriber_id, scheme_type, tier)
            )
        """)

        # NPS Transactions table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS nps_transactions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                subscriber_id INTEGER NOT NULL,
                tx_hash TEXT UNIQUE NOT NULL,
                tx_date DATE NOT NULL,
                contribution_type TEXT NOT NULL,
                scheme_type TEXT NOT NULL,
                pfm_name TEXT,
                amount REAL NOT NULL,
                units REAL NOT NULL,
                nav REAL NOT NULL,
                description TEXT,
                tier TEXT DEFAULT 'I',
                status TEXT DEFAULT 'active',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (subscriber_id) REFERENCES nps_subscribers(id)
            )
        """)

        # Add notes column to nps_transactions if not exists (migration)
        try:
            cursor.execute("ALTER TABLE nps_transactions ADD COLUMN notes TEXT")
        except sqlite3.OperationalError:
            pass  # Column already exists

        # Add FD status columns to manual_assets if not exist (migration)
        for col, col_type in [('fd_status', "TEXT DEFAULT 'active'"),
                               ('fd_closed_date', 'DATE'),
                               ('fd_money_received', 'INTEGER DEFAULT 0')]:
            try:
                cursor.execute(f"ALTER TABLE manual_assets ADD COLUMN {col} {col_type}")
            except sqlite3.OperationalError:
                pass  # Column already exists

        # NPS NAV History table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS nps_nav_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                pfm_name TEXT NOT NULL,
                scheme_type TEXT NOT NULL,
                nav_date DATE NOT NULL,
                nav REAL NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(pfm_name, scheme_type, nav_date)
            )
        """)

        # Feature Requests table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS feature_requests (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                page TEXT,
                title TEXT NOT NULL,
                description TEXT,
                status TEXT DEFAULT 'open',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # Create indexes
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_folios_investor ON folios(investor_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_transactions_folio ON transactions(folio_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_transactions_date ON transactions(tx_date)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_transactions_status ON transactions(status)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_tx_versions_tx ON transaction_versions(transaction_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_transactions_conflict ON transactions(conflict_group_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_pending_conflicts_group ON pending_conflicts(conflict_group_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_pending_conflicts_folio_date ON pending_conflicts(folio_id, tx_date)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_nav_history_isin ON nav_history(isin)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_nav_history_date ON nav_history(nav_date)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_nav_history_isin_date ON nav_history(isin, nav_date)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_portfolio_snapshots_investor ON portfolio_snapshots(investor_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_portfolio_snapshots_date ON portfolio_snapshots(snapshot_date)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_goals_investor ON goals(investor_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_goal_folios_goal ON goal_folios(goal_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_goal_folios_folio ON goal_folios(folio_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_goal_notes_goal ON goal_notes(goal_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_goal_notes_created ON goal_notes(created_at)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_quarantine_status ON quarantine(status)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_quarantine_partial_isin ON quarantine(partial_isin)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_validation_issues_folio ON validation_issues(folio_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_validation_issues_status ON validation_issues(status)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_manual_assets_investor ON manual_assets(investor_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_manual_assets_type ON manual_assets(asset_type)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_manual_assets_class ON manual_assets(asset_class)")

        # NPS indexes
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_nps_subscribers_pran ON nps_subscribers(pran)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_nps_subscribers_investor ON nps_subscribers(investor_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_nps_schemes_subscriber ON nps_schemes(subscriber_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_nps_transactions_subscriber ON nps_transactions(subscriber_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_nps_transactions_date ON nps_transactions(tx_date)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_nps_transactions_hash ON nps_transactions(tx_hash)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_nps_nav_history_pfm ON nps_nav_history(pfm_name, scheme_type)")


def generate_tx_hash(folio_number: str, tx_date: str, tx_type: str, units: float, balance: float) -> str:
    """Generate a unique hash for a transaction to prevent duplicates."""
    data = f"{folio_number}|{tx_date}|{tx_type}|{units:.4f}|{balance:.4f}"
    return hashlib.md5(data.encode()).hexdigest()


# ==================== Backup/Restore Operations ====================

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
                'goals': 0, 'goal_folios': 0, 'goal_notes': 0, 'isin_mappings': 0}

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


# ==================== Investor Operations ====================

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


# ==================== Folio Operations ====================

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


# ==================== Holdings Operations ====================

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


# ==================== Transaction Operations ====================

import re

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
                       folio_number: str, detect_conflicts: bool = True) -> Tuple[int, str]:
    """
    Insert a transaction with conflict detection.

    Returns (transaction_id, status) where status is:
    - 'inserted': New transaction inserted as active
    - 'duplicate': Transaction already exists (active), skipped
    - 'discarded': Transaction was previously discarded by user, skipped
    - 'conflict': Transaction conflicts with existing, added to pending
    """
    tx_hash = generate_tx_hash(folio_number, tx_date, tx_type, units, balance_units)

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

        return {
            'success': True,
            'version': next_version,
            'message': f'Transaction updated. Version {next_version} saved.'
        }


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


# ==================== Import Operations ====================


def _validate_balance_continuity(transactions: List[dict]) -> Tuple[List[dict], int]:
    """
    Two-phase transaction validation and repair:

    Phase 1 — Per-record cross-check: |amount| ≈ |units| × nav.
      If the identity fails, try reassigning the 4 raw values (amount, units,
      nav, balance) into the correct columns.

    Phase 2 — Balance-units continuity: balance[i] = balance[i-1] + units[i].
      Uses pairwise consistency to find anchors, then repairs isolated suspects
      using surrounding anchors.

    Returns:
        (transactions, repair_count) — transactions list is modified in-place.
    """
    SKIP_TYPES = {'stt', 'stamp_duty', 'charges', 'misc'}
    repair_count = 0

    # ================================================================
    # PHASE 1: Per-record amount ≈ units × nav validation
    # ================================================================
    for tx in transactions:
        if tx.get('type', '') in SKIP_TYPES:
            continue

        amount = float(tx.get('amount', 0) or 0)
        units = float(tx.get('units', 0) or 0)
        nav = float(tx.get('nav', 0) or 0)
        balance = float(tx.get('balance_units', 0) or 0)

        # Skip if any essential value is zero (can't validate)
        if nav == 0 or units == 0:
            continue

        expected_amount = abs(units) * nav
        actual_amount = abs(amount)

        # Check if amount ≈ units × nav (1% tolerance)
        if expected_amount > 0 and actual_amount > 0:
            ratio = actual_amount / expected_amount
            if 0.99 <= ratio <= 1.01:
                continue  # consistent, no repair needed

        # Cross-check failed — try all permutations of the 4 raw values
        raw = [abs(amount), abs(units), nav, balance]
        best_fit = None
        best_error = float('inf')

        for i_amt in range(4):
            for i_units in range(4):
                if i_units == i_amt:
                    continue
                for i_nav in range(4):
                    if i_nav == i_amt or i_nav == i_units:
                        continue
                    c_amount = raw[i_amt]
                    c_units = raw[i_units]
                    c_nav = raw[i_nav]
                    # NAV must be in plausible range
                    if not (1 <= c_nav <= 100000):
                        continue
                    if c_units == 0:
                        continue
                    expected = c_units * c_nav
                    if expected == 0:
                        continue
                    error = abs(c_amount - expected) / expected
                    if error < best_error:
                        best_error = error
                        # The remaining index is balance
                        i_bal = 6 - i_amt - i_units - i_nav  # sum of 0,1,2,3 = 6
                        best_fit = (c_amount, c_units, c_nav, raw[i_bal],
                                    i_amt, i_units, i_nav, i_bal)

        if best_fit and best_error < 0.01:
            c_amount, c_units, c_nav, c_balance = best_fit[:4]

            # Preserve signs from original
            if amount < 0 or units < 0:
                c_amount = -c_amount
            if units < 0:
                c_units = -c_units

            old_vals = f"amt={amount}, units={units}, nav={nav}, bal={balance}"
            tx['amount'] = str(c_amount)
            tx['units'] = str(c_units)
            tx['nav'] = str(c_nav)
            tx['balance_units'] = str(c_balance)
            new_vals = f"amt={c_amount}, units={c_units}, nav={c_nav}, bal={c_balance}"
            repair_count += 1
            logger.warning(
                f"[per-record-validation] REPAIRED tx "
                f"date={tx.get('date', '?')}, "
                f"desc={tx.get('description', '?')[:50]}: "
                f"{old_vals} → {new_vals} (error={best_error:.6f})"
            )

    # ================================================================
    # PHASE 2: Balance-units continuity
    # ================================================================
    # Group by (folio, isin) — each group has independent balance track
    groups = defaultdict(list)
    for idx, tx in enumerate(transactions):
        folio = tx.get('folio', '')
        isin = tx.get('isin', '')
        if folio and isin:
            groups[(folio, isin)].append((idx, tx))

    for (folio, isin), group_txs in groups.items():
        # Sort by date, preserving original order for same date
        group_txs.sort(key=lambda x: x[1].get('date', ''))

        # Build list of verifiable txs (skip charges; balance=0 IS valid)
        verifiable = []
        for g_idx, (_, tx) in enumerate(group_txs):
            tx_type = tx.get('type', '')
            if tx_type in SKIP_TYPES:
                tx['_anchor'] = True
                continue
            # Only skip if balance_units is truly missing (None/empty)
            bal_raw = tx.get('balance_units')
            if bal_raw is None or bal_raw == '':
                continue
            verifiable.append((g_idx, tx))

        # Pairwise consistency check
        for v_idx, (g_idx, tx) in enumerate(verifiable):
            balance_i = float(tx.get('balance_units', 0) or 0)
            units_i = float(tx.get('units', 0) or 0)

            has_forward = False
            if v_idx > 0:
                prev_tx = verifiable[v_idx - 1][1]
                prev_balance = float(prev_tx.get('balance_units', 0) or 0)
                if abs(prev_balance + units_i - balance_i) < 0.01:
                    has_forward = True

            has_backward = False
            if v_idx < len(verifiable) - 1:
                next_tx = verifiable[v_idx + 1][1]
                next_balance = float(next_tx.get('balance_units', 0) or 0)
                next_units = float(next_tx.get('units', 0) or 0)
                if abs(balance_i + next_units - next_balance) < 0.01:
                    has_backward = True

            if v_idx == 0 or v_idx == len(verifiable) - 1:
                tx['_anchor'] = True
            elif has_forward or has_backward:
                tx['_anchor'] = True
            else:
                tx['_anchor'] = False
                logger.info(
                    f"[balance-continuity] SUSPECT tx in {folio}/{isin}: "
                    f"balance={balance_i}, units={units_i}, "
                    f"date={tx.get('date', '?')}"
                )

        # Repair isolated suspects using surrounding anchors
        for g_idx, (orig_idx, tx) in enumerate(group_txs):
            if tx.get('_anchor', True):
                continue

            # Find prev anchor balance
            prev_anchor_balance = 0.0
            for j in range(g_idx - 1, -1, -1):
                if group_txs[j][1].get('_anchor', False):
                    prev_anchor_balance = float(
                        group_txs[j][1].get('balance_units', 0) or 0)
                    break

            # Find next anchor
            next_anchor_idx = None
            for j in range(g_idx + 1, len(group_txs)):
                jtx = group_txs[j][1]
                if jtx.get('_anchor', False):
                    next_anchor_idx = j
                    break

            if next_anchor_idx is None:
                logger.warning(
                    f"[balance-continuity] No next anchor for suspect in "
                    f"{folio}/{isin} date={tx.get('date', '?')} — skip")
                continue

            # Check no consecutive suspects
            has_other_suspects = any(
                not group_txs[j][1].get('_anchor', True)
                for j in range(g_idx + 1, next_anchor_idx))
            if has_other_suspects:
                logger.warning(
                    f"[balance-continuity] Consecutive suspects in "
                    f"{folio}/{isin} — skip")
                continue

            next_anchor_balance = float(
                group_txs[next_anchor_idx][1].get('balance_units', 0) or 0)

            # Compute correct balance and units
            intervening_units = sum(
                float(group_txs[j][1].get('units', 0) or 0)
                for j in range(g_idx + 1, next_anchor_idx + 1)
                if group_txs[j][1].get('type', '') not in SKIP_TYPES)

            correct_balance = next_anchor_balance - intervening_units
            correct_units = correct_balance - prev_anchor_balance

            old_units = float(tx.get('units', 0) or 0)
            old_balance = float(tx.get('balance_units', 0) or 0)

            tx['units'] = str(correct_units)
            tx['balance_units'] = str(correct_balance)

            # Try to fix NAV/amount with the corrected units
            old_amount = float(tx.get('amount', 0) or 0)
            old_nav = float(tx.get('nav', 0) or 0)
            raw_values = [abs(old_amount), abs(old_units), old_nav, old_balance]

            nav_fixed = False
            if abs(correct_units) > 0.001:
                for c_nav in raw_values:
                    if not (1 <= c_nav <= 100000):
                        continue
                    exp_amt = abs(correct_units) * c_nav
                    for c_amt in raw_values:
                        if c_amt == c_nav:
                            continue
                        if exp_amt > 0 and abs(exp_amt - c_amt) / exp_amt < 0.01:
                            tx['nav'] = str(c_nav)
                            tx['amount'] = str(
                                -c_amt if old_amount < 0 else c_amt)
                            nav_fixed = True
                            break
                    if nav_fixed:
                        break

            repair_count += 1
            logger.warning(
                f"[balance-continuity] REPAIRED tx in {folio}/{isin} "
                f"date={tx.get('date', '?')}: "
                f"units: {old_units}→{correct_units:.4f}, "
                f"balance: {old_balance}→{correct_balance:.4f}")

    # Clean up
    for tx in transactions:
        tx.pop('_anchor', None)

    return transactions, repair_count


def _validate_transaction_for_insert(
    amount: float, units: float, nav: float
) -> tuple:
    """
    Cross-validate amount, units, and NAV before persisting.

    Uses the identity: amount = |units| × nav to detect and fix
    a single corrupt value when the other two are consistent.

    Returns:
        Tuple of (amount, units, nav) — corrected if needed.
    """
    abs_units = abs(units)
    abs_amount = abs(amount)

    # Step 1: NAV range check
    if nav <= 0 or nav > 100000:
        if abs_amount > 0 and abs_units > 0:
            recomputed_nav = abs_amount / abs_units
            if 1 <= recomputed_nav <= 100000:
                logger.warning(
                    f"[persistence] Correcting NAV from {nav} to {recomputed_nav:.4f} "
                    f"(amount={amount}, units={units})"
                )
                nav = recomputed_nav
            else:
                logger.warning(
                    f"[persistence] NAV={nav} out of range, recomputed={recomputed_nav:.4f} "
                    f"also invalid — leaving as-is"
                )

    # Step 2: Cross-validate amount vs units × nav
    if nav > 0 and abs_units > 0:
        expected = abs_units * nav
        if expected > 0:
            ratio = abs_amount / expected
            if ratio >= 100:
                corrected_amount = expected
                if amount < 0:
                    corrected_amount = -corrected_amount
                logger.warning(
                    f"[persistence] Correcting amount from {amount} to {corrected_amount:.2f} "
                    f"(units={units}, nav={nav}, ratio={ratio:.1f})"
                )
                amount = corrected_amount
            elif ratio <= 0.01:
                corrected_units = abs_amount / nav
                if units < 0:
                    corrected_units = -corrected_units
                logger.warning(
                    f"[persistence] Correcting units from {units} to {corrected_units:.4f} "
                    f"(amount={amount}, nav={nav}, ratio={ratio:.6f})"
                )
                units = corrected_units

    return amount, units, nav


def import_parsed_data(parsed_data: dict, source_filename: str = None) -> dict:
    """
    Import parsed CAS data into the database.

    Args:
        parsed_data: Parsed CAS data dict
        source_filename: Original PDF filename (for quarantine tracking)

    Returns a summary of what was imported and what needs mapping.
    """
    result = {
        'new_folios': [],
        'existing_folios': [],
        'unmapped_folios': [],
        'new_transactions': 0,
        'duplicate_transactions': 0,
        'skipped_discarded': 0,
        'conflict_transactions': 0,
        'reversed_transactions': 0,
        'repaired_transactions': 0,
        'conflict_stats': {},
        'investor_id': None,
        'investor_found': False,
    }

    # Check if investor exists by PAN
    investor_data = parsed_data.get('investor', {})
    pan = investor_data.get('pan')

    # Get statement period from validation or parsed data
    validation = parsed_data.get('validation', {})
    statement_from = validation.get('statement_from') or parsed_data.get('statement_from')
    statement_to = validation.get('statement_to') or parsed_data.get('statement_to')

    if pan:
        existing_investor = get_investor_by_pan(pan)
        if existing_investor:
            result['investor_id'] = existing_investor['id']
            result['investor_found'] = True
            # Only update email/mobile if not already set - NEVER overwrite name
            # User may have set a custom name they want to keep
            # Always update CAS upload tracking
            update_investor(
                existing_investor['id'],
                name=None,  # Don't overwrite existing name
                email=investor_data.get('email') if not existing_investor.get('email') else None,
                mobile=investor_data.get('mobile') if not existing_investor.get('mobile') else None,
                last_cas_upload=datetime.now().isoformat(),
                statement_from_date=statement_from,
                statement_to_date=statement_to
            )
        # NOTE: Do NOT auto-create investor on first import
        # Admin must manually create and map investors via the Map Folios page
        # This gives admin control over investor names and prevents duplicate investors

    # Process holdings and create folios
    for holding in parsed_data.get('holdings', []):
        folio_number = holding.get('folio', '')
        isin = holding.get('isin', '')

        if not folio_number or not isin:
            continue

        # Add to mutual fund master
        add_to_mutual_fund_master(
            scheme_name=holding.get('scheme_name', ''),
            isin=isin,
            amc=holding.get('amc', '')
        )

        # Check if folio exists
        existing_folio = get_folio_by_number_and_isin(folio_number, isin)

        if existing_folio:
            folio_id = existing_folio['id']
            result['existing_folios'].append({
                'id': folio_id,
                'folio_number': folio_number,
                'scheme_name': holding.get('scheme_name', ''),
                'investor_id': existing_folio.get('investor_id')
            })
        else:
            # Create new folio
            folio_id = create_folio(
                folio_number=folio_number,
                scheme_name=holding.get('scheme_name', ''),
                isin=isin,
                amc=holding.get('amc'),
                registrar=holding.get('registrar'),
                investor_id=result['investor_id']  # May be None
            )
            result['new_folios'].append({
                'id': folio_id,
                'folio_number': folio_number,
                'scheme_name': holding.get('scheme_name', ''),
                'isin': isin,
                'amc': holding.get('amc')
            })

        # Update holding
        upsert_holding(
            folio_id=folio_id,
            units=float(holding.get('units', 0)),
            nav=float(holding.get('nav', 0)),
            nav_date=holding.get('nav_date', ''),
            current_value=float(holding.get('current_value', 0))
        )

    # Process transactions
    folio_cache = {}  # Cache folio lookups

    # Balance-units continuity validation and repair
    transactions = parsed_data.get('transactions', [])
    transactions, repair_count = _validate_balance_continuity(transactions)
    result['repaired_transactions'] = repair_count

    for tx in transactions:
        folio_number = tx.get('folio', '')
        isin = tx.get('isin', '')

        if not folio_number:
            continue

        # Get or create folio
        cache_key = f"{folio_number}|{isin}"
        if cache_key in folio_cache:
            folio_id = folio_cache[cache_key]
        else:
            folio = get_folio_by_number_and_isin(folio_number, isin)
            if folio:
                folio_id = folio['id']
            else:
                # Create folio from transaction
                folio_id = create_folio(
                    folio_number=folio_number,
                    scheme_name=tx.get('scheme_name', ''),
                    isin=isin,
                    investor_id=result['investor_id']
                )
            folio_cache[cache_key] = folio_id

        # Extract and cross-validate values before persisting
        amount = float(tx.get('amount', 0) or 0)
        units = float(tx.get('units', 0))
        nav = float(tx.get('nav', 0) or 0)
        amount, units, nav = _validate_transaction_for_insert(amount, units, nav)

        # Insert transaction with conflict detection
        _, status = insert_transaction(
            folio_id=folio_id,
            tx_date=tx.get('date', ''),
            tx_type=tx.get('type', 'unknown'),
            description=tx.get('description', ''),
            amount=amount,
            units=units,
            nav=nav,
            balance_units=float(tx.get('balance_units', 0)),
            folio_number=folio_number
        )

        if status == 'inserted':
            result['new_transactions'] += 1
        elif status == 'duplicate':
            result['duplicate_transactions'] += 1
        elif status == 'discarded':
            result['skipped_discarded'] += 1
        elif status == 'conflict':
            result['conflict_transactions'] += 1
        elif status == 'reversed':
            result['reversed_transactions'] += 1

    # Reconcile holding units with final transaction balance.
    # The CAS holdings section can be stale (e.g. not reflecting rejected purchases),
    # while transactions have the correct final balance_units.
    result['holdings_reconciled'] = 0
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT h.folio_id, h.units as holding_units, h.nav,
                   latest_tx.balance_units as tx_balance
            FROM holdings h
            JOIN (
                SELECT t.folio_id, t.balance_units
                FROM transactions t
                INNER JOIN (
                    SELECT folio_id, MAX(id) as max_id
                    FROM transactions
                    WHERE status = 'active'
                      AND tx_type IN ('purchase', 'sip', 'switch_in', 'redemption', 'switch_out')
                      AND balance_units > 0
                    GROUP BY folio_id
                ) latest ON t.id = latest.max_id
            ) latest_tx ON latest_tx.folio_id = h.folio_id
            WHERE ABS(h.units - latest_tx.balance_units) / MAX(h.units, 0.01) > 0.001
        """)
        mismatches = cursor.fetchall()

        for row in mismatches:
            fid = row['folio_id']
            old_units = row['holding_units']
            new_units = row['tx_balance']
            nav = row['nav'] or 0
            new_value = new_units * nav
            cursor.execute(
                "UPDATE holdings SET units = ?, current_value = ?, updated_at = CURRENT_TIMESTAMP WHERE folio_id = ?",
                (new_units, new_value, fid)
            )
            result['holdings_reconciled'] += 1
            logger.info(f"Reconciled folio {fid}: units {old_units} -> {new_units} (from final transaction balance)")

    # Get unmapped folios and conflict stats
    result['unmapped_folios'] = get_unmapped_folios()
    result['conflict_stats'] = get_conflict_stats()

    # Process quarantined items (items with broken ISINs)
    quarantine = parsed_data.get('quarantine', [])
    result['quarantined'] = 0
    if quarantine:
        import uuid
        import_batch_id = str(uuid.uuid4())[:8]
        for item in quarantine:
            add_to_quarantine(
                partial_isin=item.get('partial_isin', ''),
                scheme_name=item.get('scheme_name', ''),
                amc=item.get('amc', ''),
                folio_number=item.get('folio_number', ''),
                data_type=item.get('data_type', ''),
                data=item.get('data', {}),
                import_batch_id=import_batch_id,
                source_filename=source_filename
            )
            result['quarantined'] += 1
        logger.warning(f"Quarantined {result['quarantined']} items with broken ISINs (batch: {import_batch_id})")

    # Run post-import validation to check if transaction units match holdings
    validation_result = run_post_import_validation(result.get('investor_id'))
    result['validation'] = validation_result

    return result


# ==================== Mutual Fund Master Operations ====================

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


# ==================== Tax-Loss Harvesting ====================

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


def get_fund_tax_type(isin: str) -> str:
    """Return 'equity' or 'debt' based on fund_category and equity_pct.

    Equity: fund_category='equity', or hybrid with equity_pct >= 65.
    Debt: everything else (debt, gold_commodity, hybrid <65% equity).
    """
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT fund_category, COALESCE(equity_pct, 0) as equity_pct
            FROM mutual_fund_master WHERE isin = ?
        """, (isin,))
        row = cursor.fetchone()
        if not row:
            return 'equity'  # default assumption
        cat = row['fund_category']
        eq_pct = row['equity_pct'] or 0
        if cat == 'equity':
            return 'equity'
        if cat == 'hybrid' and eq_pct >= 65:
            return 'equity'
        if cat in ('debt', 'gold_commodity'):
            return 'debt'
        if cat == 'hybrid':
            return 'debt'
        # Unclassified — infer from equity_pct
        return 'equity' if eq_pct >= 65 else 'debt'


def _reverse_from_lots(lots: list, units_to_reverse: float, reversal_nav: float,
                       folio_id: int = None, tx_id: int = None) -> float:
    """Remove units from lots for a purchase reversal (not a sale).

    Searches from newest lot backwards for a matching NAV (within 1%).
    If no NAV match, removes from the newest lot.
    Returns the number of units that could not be matched.
    """
    target = units_to_reverse

    # First pass: find a lot with matching NAV (newest first)
    for i in range(len(lots) - 1, -1, -1):
        lot = lots[i]
        if lot['nav'] > 0 and reversal_nav > 0:
            if abs(lot['nav'] - reversal_nav) / lot['nav'] < 0.01:
                consumed = min(lot['units'], target)
                lot['cost'] = lot['cost'] * (lot['units'] - consumed) / lot['units'] if lot['units'] > 0 else 0
                lot['units'] -= consumed
                target -= consumed
                if lot['units'] < 0.0001:
                    lots.pop(i)
                if target < 0.0001:
                    return 0.0

    # Second pass: consume from newest lots if NAV match didn't cover everything
    for i in range(len(lots) - 1, -1, -1):
        if target < 0.0001:
            break
        lot = lots[i]
        consumed = min(lot['units'], target)
        lot['cost'] = lot['cost'] * (lot['units'] - consumed) / lot['units'] if lot['units'] > 0 else 0
        lot['units'] -= consumed
        target -= consumed
        if lot['units'] < 0.0001:
            lots.pop(i)

    if target > 0.01:
        logger.warning(f"Reversal over-consumption for folio_id={folio_id} tx_id={tx_id}: "
                       f"{target:.4f} units could not be matched")
    return target


def compute_fifo_lots(folio_id: int) -> List[dict]:
    """Build FIFO lots from buy transactions, consume on sell transactions.

    Purchase reversals (buy-type with negative units) undo lots at original cost,
    they do NOT generate realized gains.

    Returns remaining lots with positive units.
    Each lot: {tx_id, date, units, nav, cost, original_units}
    """
    skip_types = {'stt', 'stamp_duty', 'charges', 'segregated_portfolio', 'misc'}

    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT id, tx_date, tx_type, units, nav, description
            FROM transactions
            WHERE folio_id = ? AND status = 'active'
            ORDER BY tx_date ASC, id ASC
        """, (folio_id,))
        txns = [dict(row) for row in cursor.fetchall()]

    lots = []
    for txn in txns:
        tx_type = (txn['tx_type'] or '').lower().strip()
        if tx_type in skip_types:
            continue

        units = txn['units'] or 0
        nav = txn['nav'] or 0

        if tx_type in BUY_TX_TYPES and units > 0:
            cost = units * nav  # bonus: nav=0 → cost=0
            lots.append({
                'tx_id': txn['id'],
                'date': txn['tx_date'],
                'units': units,
                'nav': nav,
                'cost': cost,
                'original_units': units,
            })
        elif tx_type in BUY_TX_TYPES and units < 0:
            # Purchase reversal — undo from lots at original cost (not a sale)
            if nav < 0 or nav > 100000:
                logger.warning(f"Skipping garbled reversal tx_id={txn['id']} "
                               f"(units={units}, nav={nav}) for folio_id={folio_id}")
                continue
            _reverse_from_lots(lots, abs(units), nav, folio_id, txn['id'])
        elif tx_type in SELL_TX_TYPES:
            # Actual sale — consume from oldest lots (FIFO)
            units_to_sell = abs(units)
            while units_to_sell > 0.0001 and lots:
                lot = lots[0]
                if lot['units'] <= units_to_sell + 0.0001:
                    units_to_sell -= lot['units']
                    lots.pop(0)
                else:
                    lot['cost'] = lot['cost'] * (lot['units'] - units_to_sell) / lot['units']
                    lot['units'] -= units_to_sell
                    units_to_sell = 0
            if units_to_sell > 0.01:
                logger.warning(f"FIFO over-consumption for folio_id={folio_id}: "
                               f"{units_to_sell:.4f} units could not be matched")

    return lots


def compute_unrealized_gains(folio_id: int, current_nav: float) -> List[dict]:
    """Enrich FIFO lots with unrealized gain info.

    Returns list of lots with: current_value, unrealized_gain, holding_days,
    is_long_term, gain_type.
    """
    lots = compute_fifo_lots(folio_id)
    today = date.today()
    enriched = []
    for lot in lots:
        lot_date = datetime.strptime(lot['date'], '%Y-%m-%d').date() if isinstance(lot['date'], str) else lot['date']
        holding_days = (today - lot_date).days
        current_value = lot['units'] * current_nav
        unrealized_gain = current_value - lot['cost']
        is_long_term = holding_days >= 365
        enriched.append({
            **lot,
            'current_value': round(current_value, 2),
            'unrealized_gain': round(unrealized_gain, 2),
            'holding_days': holding_days,
            'is_long_term': is_long_term,
            'gain_type': 'LTCL' if is_long_term else 'STCL',
        })
    return enriched


def compute_realized_gains_fy(investor_id: int) -> dict:
    """Compute realized gains for current FY across all folios.

    Returns: {equity_stcg, equity_ltcg, debt_gains, total_realized,
              ltcg_exemption_used, ltcg_exemption_remaining}
    """
    fy_start, fy_end = get_current_fy_dates()
    today = date.today()

    with get_db() as conn:
        cursor = conn.cursor()
        # Get all folios for investor
        cursor.execute("""
            SELECT f.id as folio_id, f.isin, f.scheme_name, f.folio_number
            FROM folios f WHERE f.investor_id = ?
        """, (investor_id,))
        folios = [dict(row) for row in cursor.fetchall()]

    equity_stcg = 0.0
    equity_ltcg = 0.0
    debt_gains = 0.0
    equity_stcg_details = []
    equity_ltcg_details = []
    debt_gains_details = []

    for folio in folios:
        isin = folio['isin']
        tax_type = get_fund_tax_type(isin) if isin else 'equity'

        # Replay FIFO for this folio, tracking realized gains on sell txns in current FY
        skip_types = {'stt', 'stamp_duty', 'charges', 'segregated_portfolio', 'misc'}
        with get_db() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT id, tx_date, tx_type, description, units, nav
                FROM transactions
                WHERE folio_id = ? AND status = 'active'
                ORDER BY tx_date ASC, id ASC
            """, (folio['folio_id'],))
            txns = [dict(row) for row in cursor.fetchall()]

        lots = []
        for txn in txns:
            tx_type = (txn['tx_type'] or '').lower().strip()
            if tx_type in skip_types:
                continue
            units = txn['units'] or 0
            nav = txn['nav'] or 0

            if tx_type in BUY_TX_TYPES and units > 0:
                lots.append({
                    'date': txn['tx_date'],
                    'units': units,
                    'nav': nav,
                    'cost': units * nav,
                })
            elif tx_type in BUY_TX_TYPES and units < 0:
                # Purchase reversal — undo from lots at original cost, no realized gain
                if nav < 0 or nav > 100000:
                    continue
                _reverse_from_lots(lots, abs(units), nav, folio['folio_id'], txn['id'])
            elif tx_type in SELL_TX_TYPES:
                # Actual sale — FIFO consume and track realized gains
                sell_date_str = txn['tx_date']
                sell_nav = nav
                in_fy = fy_start <= sell_date_str <= fy_end
                units_to_sell = abs(units)

                while units_to_sell > 0.0001 and lots:
                    lot = lots[0]
                    consumed = min(lot['units'], units_to_sell)
                    lot_cost_per_unit = lot['cost'] / lot['units'] if lot['units'] > 0 else 0
                    realized = consumed * (sell_nav - lot_cost_per_unit)

                    if in_fy:
                        lot_date = datetime.strptime(lot['date'], '%Y-%m-%d').date() if isinstance(lot['date'], str) else lot['date']
                        sell_date = datetime.strptime(sell_date_str, '%Y-%m-%d').date() if isinstance(sell_date_str, str) else sell_date_str
                        holding_days = (sell_date - lot_date).days
                        is_lt = holding_days >= 365

                        detail = {
                            'tx_id': txn['id'],
                            'folio_id': folio['folio_id'],
                            'scheme_name': folio['scheme_name'],
                            'folio_number': folio['folio_number'],
                            'sell_date': sell_date_str,
                            'description': txn.get('description', ''),
                            'units_sold': round(consumed, 4),
                            'buy_date': lot['date'],
                            'buy_nav': round(lot_cost_per_unit, 4),
                            'sell_nav': round(sell_nav, 4),
                            'realized_gain': round(realized, 2),
                            'holding_days': holding_days,
                        }

                        if tax_type == 'equity':
                            if is_lt:
                                equity_ltcg += realized
                                equity_ltcg_details.append(detail)
                            else:
                                equity_stcg += realized
                                equity_stcg_details.append(detail)
                        else:
                            debt_gains += realized
                            debt_gains_details.append(detail)

                    lot['cost'] -= consumed * lot_cost_per_unit
                    lot['units'] -= consumed
                    units_to_sell -= consumed
                    if lot['units'] < 0.0001:
                        lots.pop(0)

    ltcg_exemption = 125000.0  # ₹1.25L annual exemption
    ltcg_exemption_used = min(max(equity_ltcg, 0), ltcg_exemption)

    return {
        'equity_stcg': round(equity_stcg, 2),
        'equity_ltcg': round(equity_ltcg, 2),
        'debt_gains': round(debt_gains, 2),
        'total_realized': round(equity_stcg + equity_ltcg + debt_gains, 2),
        'ltcg_exemption_used': round(ltcg_exemption_used, 2),
        'ltcg_exemption_remaining': round(ltcg_exemption - ltcg_exemption_used, 2),
        'equity_stcg_details': equity_stcg_details,
        'equity_ltcg_details': equity_ltcg_details,
        'debt_gains_details': debt_gains_details,
    }


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


def compute_tax_harvesting(investor_id: int, tax_slab_pct: float = None) -> dict:
    """Orchestrator: compute tax-loss harvesting opportunities for an investor.

    Returns: {summary, opportunities, realized_gains, warnings}
    """
    # Determine tax slab
    if tax_slab_pct is None:
        inv = get_investor_by_id(investor_id)
        tax_slab_pct = (inv or {}).get('tax_slab_pct') or 30.0

    today = date.today()

    with get_db() as conn:
        cursor = conn.cursor()
        # Get all folios with holdings for this investor
        cursor.execute("""
            SELECT f.id as folio_id, f.folio_number, f.scheme_name, f.isin, f.amc,
                   h.units as holding_units, h.current_value, h.cost_value,
                   COALESCE(mf.current_nav, h.nav) as current_nav,
                   COALESCE(mf.display_name, mf.amfi_scheme_name, f.scheme_name) as display_name,
                   mf.id as mf_id,
                   COALESCE(mf.fund_category, '') as fund_category,
                   COALESCE(mf.exit_load_pct, 1.0) as exit_load_pct,
                   COALESCE(mf.equity_pct, 0) as equity_pct
            FROM folios f
            JOIN holdings h ON h.folio_id = f.id
            LEFT JOIN mutual_fund_master mf ON mf.isin = f.isin
            WHERE f.investor_id = ?
        """, (investor_id,))
        folios = [dict(row) for row in cursor.fetchall()]

    opportunities = []
    total_unrealized_loss = 0.0
    total_tax_savings = 0.0
    total_net_benefit = 0.0
    warnings = []

    for folio in folios:
        current_nav = folio['current_nav'] or 0
        if current_nav <= 0:
            continue

        isin = folio['isin']
        tax_type = get_fund_tax_type(isin) if isin else 'equity'
        lots = compute_unrealized_gains(folio['folio_id'], current_nav)

        # Validate FIFO lot sum vs holding units
        lot_units_sum = sum(l['units'] for l in lots)
        holding_units = folio['holding_units'] or 0
        if holding_units > 0 and abs(lot_units_sum - holding_units) / holding_units > 0.01:
            warnings.append(
                f"{folio['display_name']}: FIFO lots ({lot_units_sum:.4f}) diverge "
                f"from holding ({holding_units:.4f}) by "
                f"{abs(lot_units_sum - holding_units) / holding_units * 100:.1f}%"
            )

        # Only process loss lots
        loss_lots = [l for l in lots if l['unrealized_gain'] < -0.01]
        if not loss_lots:
            continue

        exit_load_pct = folio['exit_load_pct']

        for lot in loss_lots:
            loss = abs(lot['unrealized_gain'])

            # Determine tax rate
            if tax_type == 'equity':
                tax_rate = 0.125 if lot['is_long_term'] else 0.20
            else:
                tax_rate = tax_slab_pct / 100.0

            tax_savings = loss * tax_rate

            # Costs
            cv = lot['current_value']
            exit_load = cv * (exit_load_pct / 100.0) if lot['holding_days'] < 365 else 0
            stt = cv * 0.001 if tax_type == 'equity' else 0
            stamp_duty = cv * 0.00005
            total_costs = exit_load + stt + stamp_duty

            net_benefit = tax_savings - total_costs
            if net_benefit <= 0:
                continue

            # Urgency: equity lot approaching 12-month mark
            urgent = False
            urgency_days_remaining = None
            if tax_type == 'equity' and not lot['is_long_term'] and lot['holding_days'] >= 300:
                urgent = True
                urgency_days_remaining = 365 - lot['holding_days']

            opportunities.append({
                'folio_id': folio['folio_id'],
                'mf_id': folio['mf_id'],
                'isin': isin,
                'fund_name': folio['display_name'],
                'amc': folio['amc'],
                'lot_date': lot['date'],
                'lot_units': round(lot['units'], 4),
                'lot_cost': round(lot['cost'], 2),
                'current_nav': current_nav,
                'current_value': cv,
                'unrealized_loss': round(-lot['unrealized_gain'], 2),
                'holding_days': lot['holding_days'],
                'is_long_term': lot['is_long_term'],
                'gain_type': lot['gain_type'],
                'tax_type': tax_type,
                'tax_rate': round(tax_rate * 100, 1),
                'tax_savings': round(tax_savings, 2),
                'exit_load': round(exit_load, 2),
                'exit_load_pct': exit_load_pct,
                'stt': round(stt, 2),
                'stamp_duty': round(stamp_duty, 2),
                'total_costs': round(total_costs, 2),
                'net_benefit': round(net_benefit, 2),
                'urgent': urgent,
                'urgency_days_remaining': urgency_days_remaining,
                'similar_funds': get_similar_funds(isin) if isin else [],
            })

            total_unrealized_loss += loss
            total_tax_savings += tax_savings
            total_net_benefit += net_benefit

    # Sort: urgent first, then by net benefit descending
    opportunities.sort(key=lambda x: (not x['urgent'], -x['net_benefit']))

    realized = compute_realized_gains_fy(investor_id)
    urgent_count = sum(1 for o in opportunities if o['urgent'])

    return {
        'summary': {
            'total_unrealized_loss': round(total_unrealized_loss, 2),
            'total_tax_savings': round(total_tax_savings, 2),
            'total_net_benefit': round(total_net_benefit, 2),
            'opportunity_count': len(opportunities),
            'urgent_count': urgent_count,
            'tax_slab_pct': tax_slab_pct,
        },
        'opportunities': opportunities,
        'realized_gains': realized,
        'warnings': warnings,
    }


def update_investor_tax_slab(investor_id: int, tax_slab_pct: float) -> dict:
    """Update an investor's income tax slab percentage."""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            UPDATE investors SET tax_slab_pct = ?, updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
        """, (tax_slab_pct, investor_id))
        if cursor.rowcount > 0:
            return {'success': True}
        return {'success': False, 'error': 'Investor not found'}


def update_fund_exit_load(mf_id: int, exit_load_pct: float) -> dict:
    """Update exit load percentage for a mutual fund."""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            UPDATE mutual_fund_master SET exit_load_pct = ?, updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
        """, (exit_load_pct, mf_id))
        if cursor.rowcount > 0:
            return {'success': True}
        return {'success': False, 'error': 'Fund not found'}


def confirm_fund_allocation_review(mf_id: int) -> bool:
    """Mark a fund's allocation as reviewed (resets the 30-day timer)."""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            UPDATE mutual_fund_master
            SET allocation_reviewed_at = CURRENT_TIMESTAMP, updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
        """, (mf_id,))
        return cursor.rowcount > 0


def get_funds_needing_review(days: int = 30) -> list:
    """Get funds whose allocation hasn't been reviewed in the given number of days."""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT id, scheme_name, display_name, amfi_scheme_name, isin, amc,
                   allocation_reviewed_at,
                   (equity_pct + debt_pct + commodity_pct + cash_pct + others_pct) as alloc_sum
            FROM mutual_fund_master
            WHERE (equity_pct + debt_pct + commodity_pct + cash_pct + others_pct) >= 1
              AND (allocation_reviewed_at IS NULL
                   OR allocation_reviewed_at < datetime('now', ? || ' days'))
            ORDER BY allocation_reviewed_at ASC NULLS FIRST
        """, (f'-{days}',))
        return [dict(row) for row in cursor.fetchall()]


def get_portfolio_asset_allocation(investor_id: int) -> dict:
    """
    Calculate portfolio-level asset allocation based on fund-level splits.

    Returns weighted allocation across all holdings.
    """
    with get_db() as conn:
        cursor = conn.cursor()

        # Get holdings with current value and fund asset allocation
        cursor.execute("""
            SELECT
                h.units,
                COALESCE(mf.current_nav, h.nav) as nav,
                h.units * COALESCE(mf.current_nav, h.nav) as value,
                COALESCE(mf.equity_pct, 0) as equity_pct,
                COALESCE(mf.debt_pct, 0) as debt_pct,
                COALESCE(mf.commodity_pct, 0) as commodity_pct,
                COALESCE(mf.cash_pct, 0) as cash_pct,
                COALESCE(mf.others_pct, 0) as others_pct,
                COALESCE(mf.large_cap_pct, 0) as large_cap_pct,
                COALESCE(mf.mid_cap_pct, 0) as mid_cap_pct,
                COALESCE(mf.small_cap_pct, 0) as small_cap_pct,
                f.scheme_name
            FROM holdings h
            JOIN folios f ON f.id = h.folio_id
            LEFT JOIN mutual_fund_master mf ON mf.isin = f.isin
            WHERE f.investor_id = ?
        """, (investor_id,))

        holdings = cursor.fetchall()

        total_value = 0
        equity_value = 0
        debt_value = 0
        commodity_value = 0
        cash_value = 0
        others_value = 0
        unallocated_value = 0
        large_cap_value = 0
        mid_cap_value = 0
        small_cap_value = 0

        holdings_detail = []

        for h in holdings:
            value = h['value'] or 0
            total_value += value

            # Check if allocation is defined (sums to ~100)
            alloc_sum = h['equity_pct'] + h['debt_pct'] + h['commodity_pct'] + h['cash_pct'] + h['others_pct']

            if alloc_sum < 1:  # Not defined
                unallocated_value += value
            else:
                fund_equity_value = value * h['equity_pct'] / 100
                equity_value += fund_equity_value
                debt_value += value * h['debt_pct'] / 100
                commodity_value += value * h['commodity_pct'] / 100
                cash_value += value * h['cash_pct'] / 100
                others_value += value * h['others_pct'] / 100

                # Market cap breakdown of equity portion
                cap_sum = h['large_cap_pct'] + h['mid_cap_pct'] + h['small_cap_pct']
                if cap_sum >= 1 and fund_equity_value > 0:
                    large_cap_value += fund_equity_value * h['large_cap_pct'] / 100
                    mid_cap_value += fund_equity_value * h['mid_cap_pct'] / 100
                    small_cap_value += fund_equity_value * h['small_cap_pct'] / 100

            holdings_detail.append({
                'scheme_name': h['scheme_name'],
                'value': value,
                'equity_pct': h['equity_pct'],
                'debt_pct': h['debt_pct'],
                'commodity_pct': h['commodity_pct'],
                'cash_pct': h['cash_pct'],
                'others_pct': h['others_pct'],
                'large_cap_pct': h['large_cap_pct'],
                'mid_cap_pct': h['mid_cap_pct'],
                'small_cap_pct': h['small_cap_pct'],
                'has_allocation': alloc_sum >= 1
            })

        # Count funds without allocation
        funds_without_allocation = len([h for h in holdings_detail if not h['has_allocation']])

        return {
            'total_value': total_value,
            'breakdown': {
                'equity': equity_value,
                'debt': debt_value,
                'commodity': commodity_value,
                'cash': cash_value,
                'others': others_value
            },
            'allocation': {
                'equity': {'value': equity_value, 'pct': (equity_value / total_value * 100) if total_value > 0 else 0},
                'debt': {'value': debt_value, 'pct': (debt_value / total_value * 100) if total_value > 0 else 0},
                'commodity': {'value': commodity_value, 'pct': (commodity_value / total_value * 100) if total_value > 0 else 0},
                'cash': {'value': cash_value, 'pct': (cash_value / total_value * 100) if total_value > 0 else 0},
                'others': {'value': others_value, 'pct': (others_value / total_value * 100) if total_value > 0 else 0},
                'unallocated': {'value': unallocated_value, 'pct': (unallocated_value / total_value * 100) if total_value > 0 else 0}
            },
            'market_cap': {
                'large': {'value': large_cap_value, 'pct': (large_cap_value / equity_value * 100) if equity_value > 0 else 0},
                'mid': {'value': mid_cap_value, 'pct': (mid_cap_value / equity_value * 100) if equity_value > 0 else 0},
                'small': {'value': small_cap_value, 'pct': (small_cap_value / equity_value * 100) if equity_value > 0 else 0},
                'total_equity': equity_value
            },
            'funds_without_allocation': funds_without_allocation,
            'holdings': holdings_detail
        }


# ==================== Goals Operations ====================

def create_goal(investor_id: int, name: str, target_amount: float = 0,
                target_date: str = None, description: str = None,
                target_equity_pct: float = 0, target_debt_pct: float = 0,
                target_commodity_pct: float = 0, target_cash_pct: float = 0,
                target_others_pct: float = 0) -> int:
    """Create a new investment goal."""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO goals (investor_id, name, description, target_amount, target_date,
                             target_equity_pct, target_debt_pct, target_commodity_pct,
                             target_cash_pct, target_others_pct)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (investor_id, name, description, target_amount, target_date,
              target_equity_pct, target_debt_pct, target_commodity_pct,
              target_cash_pct, target_others_pct))
        return cursor.lastrowid


def update_goal(goal_id: int, name: str = None, target_amount: float = None,
                target_date: str = None, description: str = None,
                target_equity_pct: float = None, target_debt_pct: float = None,
                target_commodity_pct: float = None, target_cash_pct: float = None,
                target_others_pct: float = None) -> dict:
    """Update a goal's details."""
    with get_db() as conn:
        cursor = conn.cursor()

        # Build dynamic update
        updates = []
        values = []

        if name is not None:
            updates.append("name = ?")
            values.append(name)
        if target_amount is not None:
            updates.append("target_amount = ?")
            values.append(target_amount)
        if target_date is not None:
            updates.append("target_date = ?")
            values.append(target_date if target_date else None)
        if description is not None:
            updates.append("description = ?")
            values.append(description)
        if target_equity_pct is not None:
            updates.append("target_equity_pct = ?")
            values.append(target_equity_pct)
        if target_debt_pct is not None:
            updates.append("target_debt_pct = ?")
            values.append(target_debt_pct)
        if target_commodity_pct is not None:
            updates.append("target_commodity_pct = ?")
            values.append(target_commodity_pct)
        if target_cash_pct is not None:
            updates.append("target_cash_pct = ?")
            values.append(target_cash_pct)
        if target_others_pct is not None:
            updates.append("target_others_pct = ?")
            values.append(target_others_pct)

        if not updates:
            return {'success': False, 'error': 'No updates provided'}

        updates.append("updated_at = CURRENT_TIMESTAMP")
        values.append(goal_id)

        cursor.execute(f"""
            UPDATE goals SET {', '.join(updates)} WHERE id = ?
        """, values)

        return {'success': cursor.rowcount > 0}


def delete_goal(goal_id: int) -> dict:
    """Delete a goal and its folio links."""
    with get_db() as conn:
        cursor = conn.cursor()

        # Delete folio links first
        cursor.execute("DELETE FROM goal_folios WHERE goal_id = ?", (goal_id,))

        # Delete the goal
        cursor.execute("DELETE FROM goals WHERE id = ?", (goal_id,))

        return {'success': cursor.rowcount > 0}


def get_goal_by_id(goal_id: int) -> dict:
    """Get a goal by ID with current value and allocation."""
    with get_db() as conn:
        cursor = conn.cursor()

        cursor.execute("""
            SELECT * FROM goals WHERE id = ?
        """, (goal_id,))

        row = cursor.fetchone()
        if not row:
            return None

        goal = dict(row)

        # Get linked folios and calculate current value/allocation
        goal.update(_calculate_goal_values(cursor, goal_id))

        return goal


def get_goals_by_investor(investor_id: int) -> List[dict]:
    """Get all goals for an investor with current values."""
    with get_db() as conn:
        cursor = conn.cursor()

        cursor.execute("""
            SELECT * FROM goals WHERE investor_id = ? ORDER BY created_at
        """, (investor_id,))

        goals = []
        for row in cursor.fetchall():
            goal = dict(row)
            goal.update(_calculate_goal_values(cursor, goal['id']))
            goals.append(goal)

        return goals


def _calculate_goal_values(cursor, goal_id: int) -> dict:
    """Calculate current value and allocation for a goal based on linked folios."""
    # Get linked folios with their values
    cursor.execute("""
        SELECT
            gf.folio_id,
            f.scheme_name,
            f.folio_number,
            h.units,
            COALESCE(mf.current_nav, h.nav) as nav,
            h.units * COALESCE(mf.current_nav, h.nav) as value,
            COALESCE(mf.equity_pct, 0) as equity_pct,
            COALESCE(mf.debt_pct, 0) as debt_pct,
            COALESCE(mf.commodity_pct, 0) as commodity_pct,
            COALESCE(mf.cash_pct, 0) as cash_pct,
            COALESCE(mf.others_pct, 0) as others_pct
        FROM goal_folios gf
        JOIN folios f ON f.id = gf.folio_id
        LEFT JOIN holdings h ON h.folio_id = f.id
        LEFT JOIN mutual_fund_master mf ON mf.isin = f.isin
        WHERE gf.goal_id = ?
    """, (goal_id,))

    linked_folios = []
    total_value = 0
    equity_value = 0
    debt_value = 0
    commodity_value = 0
    cash_value = 0
    others_value = 0

    for row in cursor.fetchall():
        folio = dict(row)
        value = folio['value'] or 0
        total_value += value

        # Calculate allocation
        alloc_sum = folio['equity_pct'] + folio['debt_pct'] + folio['commodity_pct'] + folio['cash_pct'] + folio['others_pct']
        if alloc_sum >= 1:
            equity_value += value * folio['equity_pct'] / 100
            debt_value += value * folio['debt_pct'] / 100
            commodity_value += value * folio['commodity_pct'] / 100
            cash_value += value * folio['cash_pct'] / 100
            others_value += value * folio['others_pct'] / 100

        linked_folios.append({
            'folio_id': folio['folio_id'],
            'scheme_name': folio['scheme_name'],
            'folio_number': folio['folio_number'],
            'units': folio['units'],
            'nav': folio['nav'],
            'value': value
        })

    return {
        'linked_folios': linked_folios,
        'linked_count': len(linked_folios),
        'current_value': total_value,
        'actual_allocation': {
            'equity': equity_value,
            'debt': debt_value,
            'commodity': commodity_value,
            'cash': cash_value,
            'others': others_value
        },
        'actual_allocation_pct': {
            'equity': (equity_value / total_value * 100) if total_value > 0 else 0,
            'debt': (debt_value / total_value * 100) if total_value > 0 else 0,
            'commodity': (commodity_value / total_value * 100) if total_value > 0 else 0,
            'cash': (cash_value / total_value * 100) if total_value > 0 else 0,
            'others': (others_value / total_value * 100) if total_value > 0 else 0
        }
    }


def link_folio_to_goal(goal_id: int, folio_id: int) -> dict:
    """Link a folio to a goal."""
    with get_db() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute("""
                INSERT INTO goal_folios (goal_id, folio_id) VALUES (?, ?)
            """, (goal_id, folio_id))
            return {'success': True}
        except sqlite3.IntegrityError:
            return {'success': False, 'error': 'Folio already linked to this goal'}


def unlink_folio_from_goal(goal_id: int, folio_id: int) -> dict:
    """Unlink a folio from a goal."""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            DELETE FROM goal_folios WHERE goal_id = ? AND folio_id = ?
        """, (goal_id, folio_id))
        return {'success': cursor.rowcount > 0}


def get_unlinked_folios_for_goal(goal_id: int, investor_id: int) -> List[dict]:
    """Get folios that are not linked to a goal."""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT
                f.id as folio_id,
                f.scheme_name,
                f.folio_number,
                h.units,
                COALESCE(mf.current_nav, h.nav) as nav,
                h.units * COALESCE(mf.current_nav, h.nav) as value
            FROM folios f
            LEFT JOIN holdings h ON h.folio_id = f.id
            LEFT JOIN mutual_fund_master mf ON mf.isin = f.isin
            WHERE f.investor_id = ?
              AND f.id NOT IN (SELECT folio_id FROM goal_folios WHERE goal_id = ?)
            ORDER BY f.scheme_name
        """, (investor_id, goal_id))

        return [dict(row) for row in cursor.fetchall()]


# ==================== Goal Notes Functions ====================

def create_goal_note(goal_id: int, content: str, title: str = None,
                     note_type: str = 'thought', mood: str = None) -> int:
    """
    Create a new note for a goal.

    Args:
        goal_id: The goal to add the note to
        content: The note content (required)
        title: Optional title for the note
        note_type: Type of note - 'thought', 'decision', 'milestone', 'review'
        mood: Optional mood indicator - 'optimistic', 'cautious', 'worried', 'confident', 'neutral'

    Returns:
        The ID of the created note
    """
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO goal_notes (goal_id, content, title, note_type, mood)
            VALUES (?, ?, ?, ?, ?)
        """, (goal_id, content, title, note_type, mood))
        return cursor.lastrowid


def get_goal_notes(goal_id: int, limit: int = 50) -> List[dict]:
    """Get all notes for a goal, ordered by creation date descending."""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT id, goal_id, note_type, title, content, mood,
                   created_at, updated_at
            FROM goal_notes
            WHERE goal_id = ?
            ORDER BY created_at DESC
            LIMIT ?
        """, (goal_id, limit))
        return [dict(row) for row in cursor.fetchall()]


def get_goal_note_by_id(note_id: int) -> dict:
    """Get a single note by ID."""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT id, goal_id, note_type, title, content, mood,
                   created_at, updated_at
            FROM goal_notes
            WHERE id = ?
        """, (note_id,))
        row = cursor.fetchone()
        return dict(row) if row else None


def update_goal_note(note_id: int, content: str = None, title: str = None,
                     note_type: str = None, mood: str = None) -> dict:
    """Update an existing goal note."""
    with get_db() as conn:
        cursor = conn.cursor()

        updates = []
        params = []

        if content is not None:
            updates.append("content = ?")
            params.append(content)
        if title is not None:
            updates.append("title = ?")
            params.append(title)
        if note_type is not None:
            updates.append("note_type = ?")
            params.append(note_type)
        if mood is not None:
            updates.append("mood = ?")
            params.append(mood)

        if not updates:
            return {'success': False, 'error': 'No fields to update'}

        updates.append("updated_at = CURRENT_TIMESTAMP")
        params.append(note_id)

        cursor.execute(f"""
            UPDATE goal_notes SET {', '.join(updates)}
            WHERE id = ?
        """, params)

        return {'success': cursor.rowcount > 0}


def delete_goal_note(note_id: int) -> dict:
    """Delete a goal note."""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("DELETE FROM goal_notes WHERE id = ?", (note_id,))
        return {'success': cursor.rowcount > 0}


def get_goal_notes_timeline(investor_id: int, limit: int = 100) -> List[dict]:
    """
    Get a timeline of all notes across all goals for an investor.
    Useful for seeing overall investment thinking evolution.
    """
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT
                gn.id, gn.goal_id, gn.note_type, gn.title, gn.content, gn.mood,
                gn.created_at, gn.updated_at,
                g.name as goal_name
            FROM goal_notes gn
            JOIN goals g ON g.id = gn.goal_id
            WHERE g.investor_id = ?
            ORDER BY gn.created_at DESC
            LIMIT ?
        """, (investor_id, limit))
        return [dict(row) for row in cursor.fetchall()]


# ==================== Quarantine Functions ====================

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


# ==================== NAV Functions ====================

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


# ==================== Unit Validation Functions ====================

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


# ==================== Manual Assets Functions ====================

def calculate_fd_value(principal: float, interest_rate: float, tenure_months: int,
                       compounding: str, start_date: str, as_of_date: str = None) -> dict:
    """
    Calculate FD current value and maturity details.

    Args:
        principal: Principal amount
        interest_rate: Annual interest rate (e.g., 7.5 for 7.5%)
        tenure_months: Tenure in months
        compounding: 'monthly', 'quarterly', 'half_yearly', 'yearly'
        start_date: FD start date (YYYY-MM-DD)
        as_of_date: Calculate value as of this date (default: today)

    Returns:
        Dict with current_value, maturity_value, interest_earned, days_completed, etc.
    """
    from datetime import datetime, date as date_type
    from dateutil.relativedelta import relativedelta

    if not as_of_date:
        as_of_date = date_type.today().strftime('%Y-%m-%d')

    start = datetime.strptime(start_date, '%Y-%m-%d').date()
    today = datetime.strptime(as_of_date, '%Y-%m-%d').date()
    maturity_date = start + relativedelta(months=tenure_months)

    days_completed = (today - start).days
    total_days = (maturity_date - start).days

    # Compounding frequency
    freq_map = {'monthly': 12, 'quarterly': 4, 'half_yearly': 2, 'yearly': 1}
    n = freq_map.get(compounding, 4)  # Default quarterly

    r = interest_rate / 100
    t_years = tenure_months / 12

    # Maturity value with compound interest: A = P(1 + r/n)^(nt)
    maturity_value = principal * ((1 + r/n) ** (n * t_years))

    # Current value (prorated based on days completed)
    if today >= maturity_date:
        current_value = maturity_value
        status = 'matured'
    else:
        elapsed_years = days_completed / 365
        current_value = principal * ((1 + r/n) ** (n * elapsed_years))
        status = 'active'

    interest_earned = current_value - principal
    maturity_interest = maturity_value - principal

    return {
        'principal': principal,
        'current_value': round(current_value, 2),
        'maturity_value': round(maturity_value, 2),
        'interest_earned': round(interest_earned, 2),
        'maturity_interest': round(maturity_interest, 2),
        'days_completed': days_completed,
        'total_days': total_days,
        'maturity_date': maturity_date.strftime('%Y-%m-%d'),
        'status': status,
        'progress_pct': min(100, round(days_completed / total_days * 100, 1)) if total_days > 0 else 0
    }


def calculate_fd_premature_value(principal: float, interest_rate: float,
                                  premature_penalty_pct: float, start_date: str,
                                  compounding: str = 'quarterly') -> dict:
    """
    Calculate FD value if broken today with premature withdrawal penalty.

    Args:
        principal: Principal amount
        interest_rate: Annual interest rate
        premature_penalty_pct: Penalty on interest rate (e.g., 1.0 for 1%)
        start_date: FD start date
        compounding: Compounding frequency

    Returns:
        Dict with premature_value, penalty_amount, effective_rate, etc.
    """
    from datetime import datetime, date as date_type

    today = date_type.today()
    start = datetime.strptime(start_date, '%Y-%m-%d').date()
    days_completed = (today - start).days

    if days_completed <= 0:
        return {
            'premature_value': principal,
            'penalty_amount': 0,
            'effective_rate': 0,
            'message': 'FD not yet started'
        }

    # Effective rate after penalty
    effective_rate = max(0, interest_rate - premature_penalty_pct)

    freq_map = {'monthly': 12, 'quarterly': 4, 'half_yearly': 2, 'yearly': 1}
    n = freq_map.get(compounding, 4)

    elapsed_years = days_completed / 365
    r = effective_rate / 100

    # Value with penalized rate
    premature_value = principal * ((1 + r/n) ** (n * elapsed_years))

    # Calculate what it would be without penalty
    r_full = interest_rate / 100
    full_value = principal * ((1 + r_full/n) ** (n * elapsed_years))

    penalty_amount = full_value - premature_value

    return {
        'premature_value': round(premature_value, 2),
        'penalty_amount': round(penalty_amount, 2),
        'effective_rate': effective_rate,
        'original_rate': interest_rate,
        'days_completed': days_completed,
        'interest_earned': round(premature_value - principal, 2)
    }


def calculate_sgb_value(issue_price: float, grams: float, interest_rate: float,
                        purchase_date: str, current_gold_price: float = None) -> dict:
    """
    Calculate SGB current value including gold appreciation and interest.

    Args:
        issue_price: SGB issue price per gram
        grams: Number of grams
        interest_rate: Annual interest rate (typically 2.5%)
        purchase_date: Purchase date
        current_gold_price: Current gold price per gram (if None, uses issue price)

    Returns:
        Dict with current_value, gold_value, interest_earned, appreciation, etc.
    """
    from datetime import datetime, date as date_type

    today = date_type.today()
    purchase = datetime.strptime(purchase_date, '%Y-%m-%d').date()
    days_held = (today - purchase).days

    if current_gold_price is None:
        current_gold_price = issue_price

    # Principal (face value)
    principal = issue_price * grams

    # Current gold value
    gold_value = current_gold_price * grams
    appreciation = gold_value - principal

    # Interest earned (simple interest, paid semi-annually)
    years_held = days_held / 365
    interest_earned = principal * (interest_rate / 100) * years_held

    # Total current value
    current_value = gold_value + interest_earned

    return {
        'principal': round(principal, 2),
        'current_value': round(current_value, 2),
        'gold_value': round(gold_value, 2),
        'appreciation': round(appreciation, 2),
        'appreciation_pct': round((appreciation / principal) * 100, 2) if principal > 0 else 0,
        'interest_earned': round(interest_earned, 2),
        'grams': grams,
        'issue_price': issue_price,
        'current_gold_price': current_gold_price,
        'days_held': days_held,
        'years_held': round(years_held, 2)
    }


def create_manual_asset(investor_id: int, asset_type: str, asset_class: str,
                        name: str, **kwargs) -> int:
    """
    Create a manual asset entry.

    Args:
        investor_id: The investor ID
        asset_type: Type of asset ('fd', 'sgb', 'stock', 'ppf', 'nps', 'other')
        asset_class: Asset class ('equity', 'debt', 'commodity', 'cash', 'others')
        name: Asset name
        **kwargs: Additional fields based on asset_type

    Returns:
        The created asset ID
    """
    with get_db() as conn:
        cursor = conn.cursor()

        # Base fields
        fields = ['investor_id', 'asset_type', 'asset_class', 'name']
        values = [investor_id, asset_type, asset_class, name]

        # Optional base fields
        optional_base = ['description', 'purchase_date', 'purchase_value', 'units',
                         'current_nav', 'current_value']
        for field in optional_base:
            if field in kwargs and kwargs[field] is not None:
                fields.append(field)
                values.append(kwargs[field])

        # FD specific fields
        fd_fields = ['fd_principal', 'fd_interest_rate', 'fd_tenure_months',
                     'fd_maturity_date', 'fd_compounding', 'fd_premature_penalty_pct',
                     'fd_bank_name']
        for field in fd_fields:
            if field in kwargs and kwargs[field] is not None:
                fields.append(field)
                values.append(kwargs[field])

        # SGB specific fields
        sgb_fields = ['sgb_issue_price', 'sgb_interest_rate', 'sgb_maturity_date', 'sgb_grams']
        for field in sgb_fields:
            if field in kwargs and kwargs[field] is not None:
                fields.append(field)
                values.append(kwargs[field])

        # Stock specific fields
        stock_fields = ['stock_symbol', 'stock_exchange', 'stock_quantity', 'stock_avg_price']
        for field in stock_fields:
            if field in kwargs and kwargs[field] is not None:
                fields.append(field)
                values.append(kwargs[field])

        # PPF/NPS fields
        ppf_fields = ['ppf_account_number', 'ppf_maturity_date']
        for field in ppf_fields:
            if field in kwargs and kwargs[field] is not None:
                fields.append(field)
                values.append(kwargs[field])

        placeholders = ', '.join(['?' for _ in fields])
        field_names = ', '.join(fields)

        cursor.execute(f"""
            INSERT INTO manual_assets ({field_names})
            VALUES ({placeholders})
        """, values)

        return cursor.lastrowid


def update_manual_asset(asset_id: int, **kwargs) -> dict:
    """Update a manual asset."""
    with get_db() as conn:
        cursor = conn.cursor()

        updates = []
        values = []

        # All updateable fields
        all_fields = [
            'name', 'description', 'asset_class', 'purchase_date', 'purchase_value',
            'units', 'current_nav', 'current_value', 'is_active', 'matured_on',
            'fd_principal', 'fd_interest_rate', 'fd_tenure_months', 'fd_maturity_date',
            'fd_compounding', 'fd_premature_penalty_pct', 'fd_bank_name',
            'sgb_issue_price', 'sgb_interest_rate', 'sgb_maturity_date', 'sgb_grams',
            'stock_symbol', 'stock_exchange', 'stock_quantity', 'stock_avg_price',
            'ppf_account_number', 'ppf_maturity_date'
        ]

        for field in all_fields:
            if field in kwargs:
                updates.append(f"{field} = ?")
                values.append(kwargs[field])

        if not updates:
            return {'success': False, 'error': 'No fields to update'}

        updates.append("updated_at = CURRENT_TIMESTAMP")
        values.append(asset_id)

        cursor.execute(f"""
            UPDATE manual_assets SET {', '.join(updates)}
            WHERE id = ?
        """, values)

        return {'success': cursor.rowcount > 0}


def delete_manual_asset(asset_id: int) -> dict:
    """Delete a manual asset."""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("DELETE FROM manual_assets WHERE id = ?", (asset_id,))
        return {'success': cursor.rowcount > 0}


def get_manual_asset(asset_id: int) -> Optional[dict]:
    """Get a manual asset by ID with calculated values."""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM manual_assets WHERE id = ?", (asset_id,))
        row = cursor.fetchone()
        if not row:
            return None

        asset = dict(row)
        return _enrich_manual_asset(asset)


def get_manual_assets_by_investor(investor_id: int, include_inactive: bool = False) -> List[dict]:
    """Get all manual assets for an investor."""
    with get_db() as conn:
        cursor = conn.cursor()

        if include_inactive:
            cursor.execute("""
                SELECT * FROM manual_assets
                WHERE investor_id = ?
                ORDER BY asset_type, name
            """, (investor_id,))
        else:
            cursor.execute("""
                SELECT * FROM manual_assets
                WHERE investor_id = ? AND is_active = 1
                ORDER BY asset_type, name
            """, (investor_id,))

        assets = []
        for row in cursor.fetchall():
            asset = dict(row)
            assets.append(_enrich_manual_asset(asset))

        return assets


def _enrich_manual_asset(asset: dict) -> dict:
    """Add calculated values to a manual asset based on its type."""
    asset_type = asset.get('asset_type', '')

    if asset_type == 'fd' and asset.get('fd_principal'):
        # Calculate FD current value
        if asset.get('purchase_date') and asset.get('fd_interest_rate') and asset.get('fd_tenure_months'):
            fd_calc = calculate_fd_value(
                principal=asset['fd_principal'],
                interest_rate=asset['fd_interest_rate'],
                tenure_months=asset['fd_tenure_months'],
                compounding=asset.get('fd_compounding', 'quarterly'),
                start_date=asset['purchase_date']
            )
            asset['calculated_value'] = fd_calc['current_value']
            asset['fd_details'] = fd_calc

            # Also calculate premature value
            if asset.get('fd_premature_penalty_pct') is not None:
                premature = calculate_fd_premature_value(
                    principal=asset['fd_principal'],
                    interest_rate=asset['fd_interest_rate'],
                    premature_penalty_pct=asset.get('fd_premature_penalty_pct', 1.0),
                    start_date=asset['purchase_date'],
                    compounding=asset.get('fd_compounding', 'quarterly')
                )
                asset['premature_details'] = premature

    elif asset_type == 'sgb' and asset.get('sgb_grams'):
        # Calculate SGB current value
        if asset.get('purchase_date') and asset.get('sgb_issue_price'):
            sgb_calc = calculate_sgb_value(
                issue_price=asset['sgb_issue_price'],
                grams=asset['sgb_grams'],
                interest_rate=asset.get('sgb_interest_rate', 2.5),
                purchase_date=asset['purchase_date'],
                current_gold_price=asset.get('current_nav')  # Use current_nav for current gold price
            )
            asset['calculated_value'] = sgb_calc['current_value']
            asset['sgb_details'] = sgb_calc

    elif asset_type == 'stock' and asset.get('stock_quantity'):
        # Calculate stock current value
        quantity = asset['stock_quantity']
        current_price = asset.get('current_nav', asset.get('stock_avg_price', 0))
        avg_price = asset.get('stock_avg_price', 0)

        current_value = quantity * current_price
        invested_value = quantity * avg_price

        asset['calculated_value'] = current_value
        asset['stock_details'] = {
            'quantity': quantity,
            'avg_price': avg_price,
            'current_price': current_price,
            'invested_value': invested_value,
            'current_value': current_value,
            'gain_loss': current_value - invested_value,
            'gain_loss_pct': ((current_value - invested_value) / invested_value * 100) if invested_value > 0 else 0
        }

    else:
        # For other types, use stored current_value or calculate from units * current_nav
        if asset.get('current_value'):
            asset['calculated_value'] = asset['current_value']
        elif asset.get('units') and asset.get('current_nav'):
            asset['calculated_value'] = asset['units'] * asset['current_nav']
        else:
            asset['calculated_value'] = asset.get('purchase_value', 0)

    return asset


def get_manual_assets_summary(investor_id: int) -> dict:
    """Get summary of manual assets by asset class."""
    assets = get_manual_assets_by_investor(investor_id)

    summary = {
        'total_value': 0,
        'by_asset_class': {
            'equity': {'count': 0, 'value': 0, 'assets': []},
            'debt': {'count': 0, 'value': 0, 'assets': []},
            'commodity': {'count': 0, 'value': 0, 'assets': []},
            'cash': {'count': 0, 'value': 0, 'assets': []},
            'others': {'count': 0, 'value': 0, 'assets': []}
        },
        'by_asset_type': {}
    }

    for asset in assets:
        value = asset.get('calculated_value', 0) or 0
        asset_class = asset.get('asset_class', 'others')
        asset_type = asset.get('asset_type', 'other')

        summary['total_value'] += value

        if asset_class in summary['by_asset_class']:
            summary['by_asset_class'][asset_class]['count'] += 1
            summary['by_asset_class'][asset_class]['value'] += value
            summary['by_asset_class'][asset_class]['assets'].append({
                'id': asset['id'],
                'name': asset['name'],
                'value': value
            })

        if asset_type not in summary['by_asset_type']:
            summary['by_asset_type'][asset_type] = {'count': 0, 'value': 0}
        summary['by_asset_type'][asset_type]['count'] += 1
        summary['by_asset_type'][asset_type]['value'] += value

    return summary


def get_maturing_fds(days: int = 30) -> list:
    """Get FDs maturing within the next N days."""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT ma.*, i.name as investor_name, i.pan as investor_pan
            FROM manual_assets ma
            JOIN investors i ON ma.investor_id = i.id
            WHERE ma.asset_type = 'fd'
              AND ma.is_active = 1
              AND (ma.fd_status IS NULL OR ma.fd_status = 'active')
              AND ma.fd_maturity_date IS NOT NULL
              AND ma.fd_maturity_date > date('now')
              AND ma.fd_maturity_date <= date('now', '+' || ? || ' days')
            ORDER BY ma.fd_maturity_date ASC
        """, (days,))
        rows = cursor.fetchall()
        return [_enrich_manual_asset({**dict(row)}) for row in rows]


def get_matured_fds() -> list:
    """Get FDs past maturity that aren't closed."""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT ma.*, i.name as investor_name, i.pan as investor_pan
            FROM manual_assets ma
            JOIN investors i ON ma.investor_id = i.id
            WHERE ma.asset_type = 'fd'
              AND ma.is_active = 1
              AND (ma.fd_status IS NULL OR ma.fd_status IN ('active', 'matured'))
              AND ma.fd_maturity_date IS NOT NULL
              AND ma.fd_maturity_date <= date('now')
            ORDER BY ma.fd_maturity_date ASC
        """)
        rows = cursor.fetchall()
        return [_enrich_manual_asset({**dict(row)}) for row in rows]


def close_fd(asset_id: int, money_received: bool = True) -> bool:
    """Mark FD as closed with money received status."""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            UPDATE manual_assets
            SET fd_status = 'closed',
                fd_closed_date = date('now'),
                fd_money_received = ?,
                is_active = 0,
                updated_at = CURRENT_TIMESTAMP
            WHERE id = ? AND asset_type = 'fd'
        """, (1 if money_received else 0, asset_id))
        return cursor.rowcount > 0


def import_fd_csv(investor_id: int, fd_rows: list) -> dict:
    """
    Import FDs from parsed CSV rows.

    Args:
        investor_id: The investor to associate FDs with
        fd_rows: List of dicts with keys: name, deposit_date, roi, tenor,
                 maturity_date, maturity_amount, balance

    Returns:
        dict with created count and any errors
    """
    created = 0
    errors = []

    for i, row in enumerate(fd_rows):
        try:
            # Calculate tenure from deposit date and maturity date
            deposit_date = row.get('deposit_date')
            maturity_date = row.get('maturity_date')
            tenure_months = 12  # default

            if deposit_date and maturity_date:
                try:
                    from datetime import datetime
                    # Parse dates
                    if isinstance(deposit_date, str):
                        dep_dt = datetime.strptime(deposit_date, '%Y-%m-%d')
                    else:
                        dep_dt = deposit_date

                    if isinstance(maturity_date, str):
                        mat_dt = datetime.strptime(maturity_date, '%Y-%m-%d')
                    else:
                        mat_dt = maturity_date

                    # Calculate months difference
                    months = (mat_dt.year - dep_dt.year) * 12 + (mat_dt.month - dep_dt.month)
                    # Add 1 if maturity day >= deposit day (round up partial months)
                    if mat_dt.day >= dep_dt.day:
                        tenure_months = max(1, months)
                    else:
                        tenure_months = max(1, months)
                except Exception:
                    tenure_months = 12  # fallback

            # Parse principal (balance field)
            principal = row.get('balance', 0)
            if isinstance(principal, str):
                principal = float(principal.replace(',', '').strip())

            # Parse interest rate
            roi = row.get('roi', 7.0)
            if isinstance(roi, str):
                roi = float(roi.replace('%', '').strip())

            # Create the FD
            create_manual_asset(
                investor_id=investor_id,
                asset_type='fd',
                asset_class='debt',
                name=row.get('name', f'FD-{i+1}'),
                description=f"FD imported from CSV",
                purchase_date=row.get('deposit_date'),
                purchase_value=principal,
                fd_principal=principal,
                fd_interest_rate=roi,
                fd_tenure_months=tenure_months,
                fd_maturity_date=row.get('maturity_date'),
                fd_compounding='quarterly',
                fd_bank_name=row.get('bank_name', '')
            )
            created += 1

        except Exception as e:
            errors.append(f"Row {i+1}: {str(e)}")

    return {
        'created': created,
        'errors': errors,
        'total_rows': len(fd_rows)
    }


def get_combined_portfolio_value(investor_id: int) -> dict:
    """
    Get combined portfolio value including mutual funds and manual assets.

    Returns breakdown by asset class across both types.
    """
    # Get mutual fund allocation
    mf_allocation = get_portfolio_asset_allocation(investor_id)

    # Get manual assets summary
    manual_summary = get_manual_assets_summary(investor_id)

    # Combine
    combined = {
        'mutual_funds_value': mf_allocation['total_value'],
        'manual_assets_value': manual_summary['total_value'],
        'total_value': mf_allocation['total_value'] + manual_summary['total_value'],
        'by_asset_class': {
            'equity': {
                'mf_value': mf_allocation['breakdown']['equity'],
                'manual_value': manual_summary['by_asset_class']['equity']['value'],
                'total': mf_allocation['breakdown']['equity'] + manual_summary['by_asset_class']['equity']['value']
            },
            'debt': {
                'mf_value': mf_allocation['breakdown']['debt'],
                'manual_value': manual_summary['by_asset_class']['debt']['value'],
                'total': mf_allocation['breakdown']['debt'] + manual_summary['by_asset_class']['debt']['value']
            },
            'commodity': {
                'mf_value': mf_allocation['breakdown']['commodity'],
                'manual_value': manual_summary['by_asset_class']['commodity']['value'],
                'total': mf_allocation['breakdown']['commodity'] + manual_summary['by_asset_class']['commodity']['value']
            },
            'cash': {
                'mf_value': mf_allocation['breakdown']['cash'],
                'manual_value': manual_summary['by_asset_class']['cash']['value'],
                'total': mf_allocation['breakdown']['cash'] + manual_summary['by_asset_class']['cash']['value']
            },
            'others': {
                'mf_value': mf_allocation['breakdown']['others'],
                'manual_value': manual_summary['by_asset_class']['others']['value'],
                'total': mf_allocation['breakdown']['others'] + manual_summary['by_asset_class']['others']['value']
            }
        },
        'manual_assets_by_type': manual_summary['by_asset_type']
    }

    # Calculate percentages
    total = combined['total_value']
    if total > 0:
        for cls in combined['by_asset_class']:
            combined['by_asset_class'][cls]['pct'] = round(
                combined['by_asset_class'][cls]['total'] / total * 100, 2
            )
    else:
        for cls in combined['by_asset_class']:
            combined['by_asset_class'][cls]['pct'] = 0

    return combined


# ==================== NPS Functions ====================

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


# ==================== XIRR Data ====================


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


# ==================== Feature Requests ====================


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


# Initialize database on module import
init_db()
populate_mutual_fund_master_from_folios()
