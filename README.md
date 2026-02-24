# Famfolioz

Private, self-hosted family portfolio tracker. Parse CDSL CAS PDFs, track mutual funds, NPS, FDs, and more — all on your own machine.

**Private by Design** — your financial data never leaves your device.

## Features

### Portfolio Tracking
- Import CDSL Consolidated Account Statement (CAS) PDFs with automatic parsing
- Track mutual fund holdings with live NAV from AMFI
- NPS (National Pension System) portfolio tracking
- Manual asset management — FDs, SGBs, PPF, stocks, gold (with CSV bulk import for FDs)
- Portfolio snapshots and growth charts over time

### Analysis
- XIRR returns calculation (per-folio, per-ISIN, and portfolio-level)
- Asset allocation breakdown with market cap analysis (large/mid/small/hybrid)
- Tax-loss harvesting recommendations
- FD maturity tracking and alerts
- SIP planner with goal-based projections

### Goal-Based Investing
- Create financial goals and link folios to them
- Track goal progress against target amounts
- Notes and timeline per goal

### Multi-Investor Support
- Manage portfolios for multiple family members
- Role-based access control (admin and member roles)
- Custodian access — delegate portfolio management across family members

### Data Management
- Transaction conflict detection and resolution
- ISIN resolution with AMFI database and manual mappings
- Quarantine system for unresolvable ISINs
- Post-import validation (unit mismatch detection)
- JSON backup and restore
- Database reset with safety checks

### CAS PDF Parser
- Extract investor details (name, PAN, email, mobile)
- Parse mutual fund holdings (scheme name, ISIN, folio, units, NAV, current value)
- Parse transaction history (purchase, redemption, SIP, switch, STP, dividends, charges)
- Handle edge cases: segregated portfolios, multi-line scheme names, page continuations
- Validation rules with anomaly detection

## Quick Start

See **[SETUP.md](SETUP.md)** for detailed step-by-step instructions.

```bash
git clone https://github.com/arghaM/famfolioz.git
cd famfolioz
bash setup_app.sh      # one-time setup
./start.sh             # start the app (or double-click start.command on macOS)
```

Then open http://127.0.0.1:5000 in your browser. On first launch, you'll be prompted to create an admin account.

### Docker

```bash
docker compose up -d
```

Data persists in a Docker volume. Access at http://localhost:5000.

## Architecture

### CAS Parser (FSM)

The PDF parser uses a Finite State Machine for section detection:

```
INITIAL -> INVESTOR_INFO -> HOLDINGS_SUMMARY -> TRANSACTION_DETAILS -> END
```

### Web Application

```
cas_parser/webapp/
  app.py              # Flask application factory
  auth.py             # Authentication (before_request, decorators, access control)
  manage.py           # CLI tool for user management

  db/                 # Database layer (15 domain modules)
    connection.py     # SQLite connection + schema initialization
    auth.py           # User and custodian access CRUD
    investors.py      # Investor profiles
    folios.py         # Folio management
    holdings.py       # Current holdings
    transactions.py   # Transaction history
    mutual_funds.py   # MF master data, ISIN mapping, classification
    goals.py          # Goal-based investing
    nav.py            # NAV refresh and portfolio snapshots
    nps.py            # NPS subscribers, transactions, NAV
    manual_assets.py  # FDs, SGBs, PPF, stocks, gold
    tax.py            # Tax-loss harvesting analysis
    benchmarks.py     # Benchmark indices and returns
    import_engine.py  # CAS PDF import with deduplication
    admin.py          # Backup, restore, config, validation
    validation.py     # Post-import data validation

  routes/             # API + page routes (11 blueprints)
    auth.py           # Login, logout, setup, user management
    pages.py          # HTML page routes
    investors.py      # Investor CRUD + sync status
    folios.py         # Folio mapping and management
    transactions.py   # Transaction APIs + CAS parsing
    performance.py    # XIRR, snapshots, benchmarks
    mutual_funds.py   # MF master, classification, allocation
    goals.py          # Goals and notes CRUD
    nps.py            # NPS portfolio management
    manual_assets.py  # Manual asset CRUD
    admin.py          # Backup, restore, validation, config

  templates/          # 20 Bootstrap 5 templates
```

### Authentication

- **Admin** — full access to all investors, settings, backup, user management
- **Member** — access to own portfolio + any custodian-granted portfolios
- Setup wizard on first launch creates the initial admin account
- Session-based auth with 7-day persistent cookies

### Key Modules (Parser)

- `models.py` — Dataclasses: `Investor`, `Holding`, `Transaction`, `CASStatement`
- `extractor.py` — PDF text extraction using pdfplumber
- `section_detector.py` — FSM for section detection using semantic markers
- `holdings_parser.py` — Holdings parsing (handles multi-line scheme names)
- `transactions_parser.py` — Transaction parsing with type detection
- `validator.py` — Validation rules (value calculations, ISIN/PAN format, consistency)
- `main.py` — CLI interface and orchestration

## CLI Usage

### Parse a CAS PDF

```bash
python -m cas_parser.main statement.pdf
python -m cas_parser.main statement.pdf -o output.json
python -m cas_parser.main statement.pdf -p mypassword
python -m cas_parser.main statement.pdf --validate-only
```

### User Management

```bash
python -m cas_parser.webapp.manage list-users
python -m cas_parser.webapp.manage create-admin <username>
python -m cas_parser.webapp.manage reset-password <username>
```

## Python API

```python
from cas_parser import parse_cas_pdf

statement = parse_cas_pdf("statement.pdf")

# Investor info
print(f"Name: {statement.investor.name}")
print(f"PAN: {statement.investor.pan}")

# Holdings
for holding in statement.holdings:
    print(f"{holding.scheme_name}: {holding.units} units @ {holding.nav}")

# Transactions
for tx in statement.transactions:
    print(f"{tx.date}: {tx.transaction_type.value} - {tx.units} units")

# Validation
if not statement.validation.is_valid:
    for error in statement.validation.errors:
        print(f"Error: {error}")
```

## Testing

```bash
pytest cas_parser/tests/                                    # all tests
pytest cas_parser/tests/test_validator.py                   # specific file
pytest cas_parser/tests/ --cov=cas_parser --cov-report=html # with coverage
```

## Data Storage

| Item | Location | In git? |
|------|----------|---------|
| Financial data | `cas_parser/webapp/data.db` | No (gitignored) |
| Backup JSONs | `cas_parser/webapp/backups/` | No (gitignored) |
| App code | Everything else | Yes |
| Dependencies | `venv/` | No (recreated by setup) |

All data stays local. Nothing is sent to any external server.

## License

MIT License
