# Changelog

All notable changes to Famfolioz will be documented in this file.

Format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).
Versioning follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- **Role-based authentication** with admin and member roles
  - Setup wizard on first launch to create admin account
  - Login/logout with 7-day persistent sessions
  - `@admin_required` decorator on admin-only endpoints
  - `check_investor_access()` for investor-scoped access control
- **Custodian access model** for family portfolio delegation
  - Grant/revoke read+manage access to other investors' portfolios
  - Members see only own + custodian-granted portfolios
- **User management UI** in Settings page
  - Create, edit, disable users
  - Reset passwords
  - Manage custodian access per user
- **CLI management tool** (`python -m cas_parser.webapp.manage`)
  - `list-users`, `create-admin`, `reset-password` commands
  - Escape hatch for lockout recovery
- Upload page shows **last sync date per investor** with color-coded staleness badges
- "Private by Design" message on login page
- Auth nav bar across all pages (user display + logout)
- Home tab on investor page with portfolio summary, growth chart, asset allocation doughnut, and alerts panel
- Alerts API aggregating validation issues, quarantine, FD maturity, and transaction conflicts
- Source filename tracking in quarantine for traceability
- Backup/restore now includes users and custodian_access tables

### Fixed
- Holdings now show scheme name from MF Master (display_name > amfi_scheme_name > folio name) instead of raw CAS PDF text
- Member dashboard no longer crashes when admin-only APIs return 403
- Password hashing uses pbkdf2:sha256 explicitly (compatible with Python 3.8-3.12)
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
- Docker support with docker-compose
- Smart SIP planner with goal-based projections
