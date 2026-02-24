# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Famfolioz — a self-hosted family portfolio tracker. Parses CDSL CAS PDFs and provides a web UI for tracking mutual funds, NPS, FDs, and other assets across multiple family members with role-based access control.

## Build and Development Commands

```bash
# Install dependencies
pip install -r requirements.txt

# Install dev dependencies
pip install -r requirements-dev.txt

# Install in development mode
pip install -e .

# Run all tests
pytest cas_parser/tests/

# Run specific test file
pytest cas_parser/tests/test_validator.py

# Run specific test
pytest cas_parser/tests/test_validator.py::TestValidateISIN::test_valid_isin

# Run with coverage
pytest cas_parser/tests/ --cov=cas_parser --cov-report=html

# Lint
flake8 cas_parser/
black --check cas_parser/
isort --check cas_parser/

# Type check
mypy cas_parser/
```

## Web Application

```bash
# Start the web app
python -m cas_parser.webapp.app

# User management CLI
python -m cas_parser.webapp.manage list-users
python -m cas_parser.webapp.manage create-admin <username>
python -m cas_parser.webapp.manage reset-password <username>
```

### App Architecture

Flask application with Blueprint-based routing:

```
cas_parser/webapp/
  app.py              # create_app() factory, registers blueprints
  auth.py             # init_auth(), @admin_required, check_investor_access()
  manage.py           # CLI: list-users, create-admin, reset-password

  db/                 # Database layer — each file is a domain module
    connection.py     # get_db(), init_db() with full schema
    auth.py           # users + custodian_access CRUD
    investors.py      # Investor profiles
    folios.py         # Folio management and mapping
    holdings.py       # Current holdings
    transactions.py   # Transaction history
    mutual_funds.py   # MF master data, ISIN mapping, classification
    goals.py          # Goal-based investing and notes
    nav.py            # NAV refresh, portfolio snapshots
    nps.py            # NPS subscribers, transactions, NAV
    manual_assets.py  # FDs, SGBs, PPF, stocks, gold
    tax.py            # Tax-loss harvesting analysis
    benchmarks.py     # Benchmark indices
    import_engine.py  # CAS PDF import with deduplication
    admin.py          # Backup, restore, config, validation
    validation.py     # Post-import data validation
    __init__.py       # Re-exports all functions via __all__

  routes/             # 11 Blueprints
    auth.py           # Login, logout, setup wizard, user CRUD, custodian API
    pages.py          # HTML page routes (14 pages)
    investors.py      # Investor CRUD, sync status
    folios.py         # Folio mapping
    transactions.py   # Transaction APIs, CAS parsing
    performance.py    # XIRR, snapshots, benchmarks
    mutual_funds.py   # MF master, classification, allocation
    goals.py          # Goals and notes
    nps.py            # NPS portfolio
    manual_assets.py  # Manual asset CRUD
    admin.py          # Backup, restore, validation, config
    __init__.py       # register_routes() wires all blueprints + auth

  templates/          # 20 Bootstrap 5 Jinja2 templates
```

### Authentication Model

- `before_request` hook enforces login on all routes (except `/login`, `/setup`, `/health`, `/static`)
- First-run: no users exist -> redirects to `/setup` wizard
- `@admin_required` decorator on admin-only endpoints (returns 403)
- `check_investor_access(investor_id)` helper on investor-scoped endpoints
- Indirect ownership resolvers: `get_investor_id_for_goal()`, `get_investor_id_for_folio()`, etc.
- Two roles: `admin` (full access) and `member` (own + custodian portfolios)
- `custodian_access` table grants cross-family portfolio access
- `get_accessible_investor_ids(user_id)` returns union of own + custodian investor IDs
- Password hashing: `werkzeug.security` with explicit `method='pbkdf2:sha256'` (Python 3.8+ compatible)

### Database

Single SQLite file at `cas_parser/webapp/data.db`. All db functions use `get_db()` context manager from `db/connection.py`. The `db/__init__.py` re-exports everything so routes import via `from cas_parser.webapp import data as db`.

### Important Conventions

- All monetary values use Python `Decimal` (no floats)
- Templates use `{% include '_auth_nav.html' %}` for the auth nav bar
- Templates include a fetch interceptor script that redirects to `/login` on 401
- Admin-only JS in templates is wrapped in `{% if current_user and current_user.role == 'admin' %}` blocks
- API endpoints return JSON; page routes return rendered templates

## CAS Parser (CLI)

```bash
python -m cas_parser.main statement.pdf
python -m cas_parser.main statement.pdf -o output.json
python -m cas_parser.main statement.pdf -p mypassword
python -m cas_parser.main statement.pdf --validate-only
python -m cas_parser.main statement.pdf -v
```

### Parser Architecture

Uses a **Finite State Machine (FSM)** for section detection:

```
INITIAL -> INVESTOR_INFO -> HOLDINGS_SUMMARY -> TRANSACTION_DETAILS -> END
```

Key modules:
- `models.py` — Dataclasses: `Investor`, `Holding`, `Transaction`, `CASStatement`
- `extractor.py` — PDF text extraction using pdfplumber
- `section_detector.py` — FSM for detecting CAS sections using semantic markers
- `holdings_parser.py` — Parse mutual fund holdings (handles multi-line scheme names)
- `transactions_parser.py` — Parse transaction history with type detection
- `validator.py` — Validation rules (value calculations, ISIN/PAN format, consistency)
- `main.py` — CLI interface and orchestration

## Key Design Decisions

1. **Semantic Markers**: Uses regex patterns to detect sections (not fixed positions) for format drift tolerance
2. **Decimal for Money**: All monetary values use `Decimal` to avoid floating-point errors
3. **Transaction Type Detection**: `TransactionTypeDetector` class with keyword pattern matching
4. **Validation Tolerance**: 1% tolerance for value calculations (units x NAV ~ current_value)
5. **Domain-split DB**: Each `db/*.py` owns its tables; `__init__.py` re-exports for backward compat

## Edge Cases Handled

- Segregated portfolio entries (detected by "Segregated" keyword)
- Switch in/out transactions (detect and pair)
- Dividend reinvestment (units positive, not redemption)
- STT and charges (do not reduce units)
- Multi-line scheme names
- Multiple page continuation

## Important Patterns

ISIN: `INF[A-Z0-9]{9}` (12 characters starting with INF)
PAN: `[A-Z]{5}[0-9]{4}[A-Z]` (10 characters)
Date formats: `DD-Mon-YYYY`, `DD/MM/YYYY`
