"""Database connection, schema initialization, and context manager."""

import logging
import os
import sqlite3
from contextlib import contextmanager
from pathlib import Path

logger = logging.getLogger(__name__)

__all__ = ['DB_PATH', 'BACKUP_DIR', 'get_connection', 'get_db', 'init_db']

# Database file path — override with FAMFOLIOZ_DATA_DIR env var (used by Docker)
_data_dir = Path(os.environ.get('FAMFOLIOZ_DATA_DIR', str(Path(__file__).parent.parent)))
DB_PATH = _data_dir / "data.db"
BACKUP_DIR = _data_dir / "backups"


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

        # Add equity sub-category for goal-level sub-allocation analysis
        try:
            cursor.execute("ALTER TABLE mutual_fund_master ADD COLUMN equity_sub_category TEXT")
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

        # Goal Phases table - phased asset allocation targets over time
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS goal_phases (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                goal_id INTEGER NOT NULL,
                phase_name TEXT NOT NULL,
                start_date DATE,
                end_date DATE,
                equity_pct REAL DEFAULT 0,
                debt_pct REAL DEFAULT 0,
                commodity_pct REAL DEFAULT 0,
                sort_order INTEGER DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (goal_id) REFERENCES goals(id) ON DELETE CASCADE
            )
        """)

        # Goal Phase Equity Sub-Allocation table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS goal_phase_equity_sub (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                phase_id INTEGER NOT NULL,
                india_large_cap_pct REAL DEFAULT 0,
                india_mid_small_pct REAL DEFAULT 0,
                india_flexi_pct REAL DEFAULT 0,
                intl_us_global_pct REAL DEFAULT 0,
                intl_emerging_pct REAL DEFAULT 0,
                sectoral_thematic_pct REAL DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (phase_id) REFERENCES goal_phases(id) ON DELETE CASCADE
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

        # App Config table (key-value store for settings like family name)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS app_config (
                key TEXT PRIMARY KEY,
                value TEXT,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # Benchmark tables for performance comparison
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS benchmarks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                investor_id INTEGER NOT NULL,
                scheme_code INTEGER NOT NULL,
                scheme_name TEXT NOT NULL,
                fund_house TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (investor_id) REFERENCES investors(id),
                UNIQUE(investor_id, scheme_code)
            )
        """)

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS benchmark_data (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                scheme_code INTEGER NOT NULL,
                data_date DATE NOT NULL,
                nav REAL NOT NULL,
                UNIQUE(scheme_code, data_date)
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
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_goal_phases_goal ON goal_phases(goal_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_goal_phase_equity_sub_phase ON goal_phase_equity_sub(phase_id)")
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

        # Benchmark indexes
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_benchmarks_investor ON benchmarks(investor_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_benchmark_data_scheme ON benchmark_data(scheme_code)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_benchmark_data_date ON benchmark_data(scheme_code, data_date)")
