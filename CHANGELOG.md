# Changelog

All notable changes to Famfolioz will be documented in this file.

Format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).
Versioning follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- Home tab on investor page with portfolio summary, growth chart, asset allocation doughnut, and alerts panel
- Alerts API aggregating validation issues, quarantine, FD maturity, and transaction conflicts
- Source filename tracking in quarantine for traceability

### Fixed
- Holdings now show scheme name from MF Master (display_name > amfi_scheme_name > folio name) instead of raw CAS PDF text
- Consolidated .gitignore — removed gaps, added coverage/IDE/egg-info patterns

### Removed
- Redundant setup.py (pyproject.toml is now the single build config)

## [1.0.0] - 2025-02-15

### Added
- CAS PDF parser with FSM-based section detection
- Mutual fund holdings and transaction tracking
- NPS statement parsing and portfolio tracking
- Manual asset management (FD, SGB, PPF, stocks)
- FD maturity tracking and CSV import
- NAV refresh from AMFI with portfolio snapshots
- XIRR calculation (per-folio, per-ISIN, portfolio-level)
- Asset allocation analysis with market cap breakdown
- Tax-loss harvesting analysis
- Goal-based investing with folio linking
- Transaction conflict detection and resolution
- ISIN resolver with AMFI database and manual mappings
- Quarantine system for broken ISINs
- Post-import validation (unit mismatch detection)
- Backup/restore for static configuration
- Web UI with Bootstrap 5
