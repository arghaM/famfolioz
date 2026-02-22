"""
Database access layer for the CAS Parser webapp.

Re-exports all public functions from domain-specific sub-modules.
Import from here or from individual sub-modules:

    from cas_parser.webapp.db import get_all_investors
    from cas_parser.webapp.db.investors import get_all_investors  # same thing
"""

# Explicit __all__ so `from db import *` re-exports underscore names too
__all__ = [
    # connection
    "DB_PATH", "BACKUP_DIR", "get_connection", "get_db", "init_db",
    # investors
    "get_all_investors", "get_investor_by_id", "get_investor_by_pan",
    "create_investor", "update_investor",
    # folios
    "get_folio_by_number_and_isin", "get_folios_by_investor", "get_unmapped_folios",
    "get_all_folios_with_assignments", "create_folio", "map_folio_to_investor",
    "map_folios_to_investor", "get_folio_by_id", "unmap_folio",
    # holdings
    "upsert_holding", "get_holdings_by_investor",
    # benchmarks
    "get_folios_with_transactions", "get_category_weights",
    "get_benchmarks_by_investor", "add_benchmark", "delete_benchmark",
    "upsert_benchmark_data", "get_benchmark_data", "get_benchmark_data_latest_date",
    # goals
    "create_goal", "update_goal", "delete_goal", "get_goal_by_id",
    "get_goals_by_investor", "link_folio_to_goal", "unlink_folio_from_goal",
    "get_unlinked_folios_for_goal", "create_goal_note", "get_goal_notes",
    "get_goal_note_by_id", "update_goal_note", "delete_goal_note",
    "get_goal_notes_timeline",
    # transactions
    "generate_tx_hash", "_compute_sequence_numbers", "insert_transaction",
    "get_pending_conflict_groups", "get_conflict_group_transactions",
    "resolve_conflict", "get_conflict_stats", "get_transactions_by_folio",
    "get_transactions_by_investor", "get_transaction_by_id", "update_transaction",
    "get_transaction_versions", "get_transaction_version_count", "get_transaction_stats",
    # mutual_funds
    "get_all_mutual_funds", "get_unmapped_mutual_funds", "get_mapped_mutual_funds",
    "add_to_mutual_fund_master", "map_mutual_fund_to_amfi", "update_fund_display_name",
    "update_fund_asset_allocation", "update_fund_classification", "get_fund_holdings",
    "get_fund_sectors", "update_fund_holdings", "update_fund_sectors", "get_fund_detail",
    "get_current_fy_dates", "get_similar_funds", "search_amfi_schemes",
    "get_mutual_fund_stats", "populate_mutual_fund_master_from_folios",
    "BUY_TX_TYPES", "SELL_TX_TYPES", "VALID_SECTORS",
    # nps
    "generate_nps_tx_hash", "get_or_create_nps_subscriber", "get_nps_subscriber",
    "get_nps_subscribers_by_investor", "get_all_nps_subscribers", "upsert_nps_scheme",
    "get_nps_schemes", "insert_nps_transaction", "get_nps_transactions",
    "get_nps_transactions_by_scheme", "get_nps_transactions_by_contribution",
    "get_nps_portfolio_summary", "update_nps_statement_info", "save_nps_nav",
    "get_nps_nav_history", "get_latest_nps_nav", "update_nps_transaction_notes",
    "get_nps_transaction", "import_nps_statement", "get_unmapped_nps_subscribers",
    "link_nps_to_investor", "unlink_nps_from_investor",
    # nav
    "fetch_and_update_nav", "get_nav_for_holdings", "get_last_nav_update",
    "get_nav_history", "get_nav_history_dates", "take_portfolio_snapshot",
    "take_all_portfolio_snapshots", "get_portfolio_history", "get_portfolio_valuation_on_date",
    # tax
    "get_fund_tax_type", "_reverse_from_lots", "compute_fifo_lots",
    "compute_unrealized_gains", "compute_realized_gains_fy", "compute_tax_harvesting",
    "update_investor_tax_slab", "update_fund_exit_load", "confirm_fund_allocation_review",
    "get_funds_needing_review", "get_portfolio_asset_allocation",
    # validation
    "add_to_quarantine", "get_quarantined_items", "get_quarantine_summary",
    "resolve_quarantine", "delete_quarantine_items", "get_quarantine_stats",
    "validate_folio_units", "validate_investor_folios", "validate_all_folios",
    "save_validation_issue", "get_validation_issues", "resolve_validation_issue",
    "run_post_import_validation",
    # manual_assets
    "calculate_fd_value", "calculate_fd_premature_value", "calculate_sgb_value",
    "create_manual_asset", "update_manual_asset", "delete_manual_asset",
    "get_manual_asset", "get_manual_assets_by_investor", "get_manual_assets_summary",
    "get_maturing_fds", "get_matured_fds", "close_fd", "import_fd_csv",
    "get_combined_portfolio_value",
    # import_engine
    "_validate_balance_continuity", "_detect_reversal_pairs", "_find_excess_transactions",
    "_stage_and_analyze_transactions", "_validate_transaction_for_insert",
    "import_parsed_data",
    # admin
    "get_config", "set_config", "backup_static_tables", "list_backups",
    "restore_static_tables", "reset_database", "get_xirr_data_for_folio",
    "get_xirr_data_for_investor", "create_feature_request", "get_investor_alerts",
    "get_feature_requests",
]

# === Leaf modules (no cross-deps) ===
from cas_parser.webapp.db.connection import *  # noqa: F401,F403
from cas_parser.webapp.db.investors import *  # noqa: F401,F403
from cas_parser.webapp.db.folios import *  # noqa: F401,F403
from cas_parser.webapp.db.holdings import *  # noqa: F401,F403
from cas_parser.webapp.db.benchmarks import *  # noqa: F401,F403
from cas_parser.webapp.db.goals import *  # noqa: F401,F403

# === Modules with one dep ===
from cas_parser.webapp.db.transactions import *  # noqa: F401,F403
from cas_parser.webapp.db.mutual_funds import *  # noqa: F401,F403
from cas_parser.webapp.db.nps import *  # noqa: F401,F403

# === Modules with multiple deps ===
from cas_parser.webapp.db.nav import *  # noqa: F401,F403
from cas_parser.webapp.db.tax import *  # noqa: F401,F403
from cas_parser.webapp.db.validation import *  # noqa: F401,F403
from cas_parser.webapp.db.manual_assets import *  # noqa: F401,F403

# === Most connected modules (import last) ===
from cas_parser.webapp.db.import_engine import *  # noqa: F401,F403
from cas_parser.webapp.db.admin import *  # noqa: F401,F403


# Initialize database on package import (same as original data.py lines 6514-6516)
init_db()
populate_mutual_fund_master_from_folios()
